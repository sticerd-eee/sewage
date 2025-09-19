############################################################
# Create Site-Level Spill Statistics (Monthly & Quarterly)
# Project: Sewage
# Date: 29/05/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads pre-aggregated monthly and quarterly spill data
#' from DuckDB, calculates various site-level spill metrics including
#' cross-site thresholds and treatment indicators, and exports the
#' combined results to a partitioned parquet dataset.

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }
  required_packages <- c(
    "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "lubridate", "DBI", "duckdb", "dplyr", "dbplyr", "arrow", "conflicted"
  )
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
    library(pkg, character.only = TRUE)
  }))
  conflicts_prefer(dbplyr::sql)
  conflicts_prefer(dplyr::filter)
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "compute_spill_stats.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  log_appender(appender_file(log_path))
  log_layout(layout_glue_colors)
  log_threshold(DEBUG)
  log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed"),
  db_path = here::here("data", "duckdb.duckdb"),
  output_dir = here::here("data", "processed", "agg_spill_stats")
)

# Database Functions
############################################################

#' Connect to the DuckDB database
#' @return DuckDB connection object
connect_to_db <- function() {
  log_info("Connecting to DuckDB at {CONFIG$db_path}")
  tryCatch(
    {
      con <- dbConnect(duckdb::duckdb(), dbdir = CONFIG$db_path)
      dbExecute(con, "PRAGMA memory_limit='16GB'")
      dbExecute(con, "PRAGMA max_memory='16GB'")
      dbExecute(con, "PRAGMA max_temp_directory_size='500GiB'")
      log_info("Database connection established")
      return(con)
    },
    error = function(e) {
      log_error("Failed to connect to database: {e$message}")
      stop(glue("Database connection failed: {e$message}"))
    }
  )
}

#' Load required spill datasets into DuckDB if not already present
#' Load datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  logger::log_info("Loading datasets into DuckDB")

  # Check if tables already exist
  existing_tables <- DBI::dbListTables(con)

  # Load monthly spill data if needed
  if (!"dat_mo" %in% existing_tables) {
    logger::log_info("Loading monthly spill data")
    dat_mo <- import(
      file.path(
        CONFIG$processed_dir, "agg_spill_stats", "agg_spill_dry_mo.parquet"
      ),
      trust = TRUE
    )

    dat_mo <- dat_mo %>%
      select(
        water_company,
        site_id,
        month_id,
        spill_count_mo,
        spill_hrs_mo,
        dry_spill_count_mo_r1_d01_weak,
        dry_spill_hrs_mo_r1_d01_weak
      ) %>%
      arrange(site_id, month_id)
    copy_to(con, dat_mo, "dat_mo", temporary = FALSE)
    rm(dat_mo)
    logger::log_info("Monthly spill data loaded")
  }

  # Load quarterly spill data if needed
  if (!"dat_qtr" %in% existing_tables) {
    logger::log_info("Loading quarterly spill data")
    dat_qtr <- import(
      file.path(
        CONFIG$processed_dir, "agg_spill_stats", "agg_spill_dry_qtr.parquet"
      ),
      trust = TRUE
    )

    dat_qtr <- dat_qtr %>%
      select(
        water_company,
        site_id,
        qtr_id,
        spill_count_qt,
        spill_hrs_qt,
        dry_spill_count_qt_r1_d01_weak,
        dry_spill_hrs_qt_r1_d01_weak
      ) %>%
      arrange(site_id, qtr_id)

    copy_to(con, dat_qtr, "dat_qtr", temporary = FALSE)
    rm(dat_qtr)
    logger::log_info("Quarterly spill data loaded")
  }
}

#' Prepare and calculate spill statistics for a given period (monthly/quarterly)
#' @param con DuckDB connection
#' @param period_type Character string: "monthly" or "quarterly"
#' @return A lazy tibble (dbplyr) with calculated spill statistics for the period
calculate_period_spill_stats <- function(con, period_type) {
  log_info("Calculating {period_type} spill statistics")

  # Monthly aggregation
  if (period_type == "monthly") {
    spill_tbl <- tbl(con, "dat_mo") %>%
      rename(
        spill_count = spill_count_mo,
        spill_hrs = spill_hrs_mo,
        dry_spill_count = dry_spill_count_mo_r1_d01_weak,
        dry_spill_hrs = dry_spill_hrs_mo_r1_d01_weak
      ) %>%
      mutate(
        year = (month_id - 1) %/% 12 + 1,
        spill_date = month_id
      ) %>%
      select(site_id, month_id, spill_date, year, spill_count, spill_hrs, dry_spill_count, dry_spill_hrs)

    period_suffix <- "mo"
  }

  # Quarterly Aggregation
  else if (period_type == "quarterly") {
    spill_tbl <- tbl(con, "dat_qtr") %>%
      rename(
        spill_count = spill_count_qt,
        spill_hrs = spill_hrs_qt,
        dry_spill_count = dry_spill_count_qt_r1_d01_weak,
        dry_spill_hrs = dry_spill_hrs_qt_r1_d01_weak
      ) %>%
      mutate(
        year = (qtr_id - 1) %/% 4 + 1,
        spill_date = qtr_id,
      ) %>%
      select(site_id, qtr_id, spill_date, year, spill_count, spill_hrs, dry_spill_count, dry_spill_hrs)

    period_suffix <- "qtr"
  } else {
    stop("Invalid period_type specified. Must be 'monthly' or 'quarterly'.")
  }

  # Base metrics with non-NA values for threshold calculation
  metrics_base <- spill_tbl %>%
    filter(!is.na(spill_count) & !is.na(spill_hrs))

  # Calculate period-specific thresholds
  log_info("Calculating {period_type} thresholds")
  period_thresholds <- metrics_base %>%
    group_by(spill_date) %>%
    summarise(
      across(c(spill_count, spill_hrs, dry_spill_count, dry_spill_hrs),
        list(
          p50 = ~ median(., na.rm = TRUE),
          p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
          p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
          max = ~ max(., na.rm = TRUE),
          mean = ~ mean(., na.rm = TRUE)
        ),
        .names = "thr_{.fn}_{.col}_{period_suffix}"
      ),
      n_sites_spills = n(),
      .groups = "drop"
    ) %>%
    rename(!!paste0("n_sites_spills_", period_suffix) := n_sites_spills)

  # Calculate yearly thresholds based on the current period's data
  log_info("Calculating yearly thresholds based on {period_type} data")
  yearly_thresholds <- metrics_base %>%
    group_by(year) %>%
    summarise(
      across(c(spill_count, spill_hrs, dry_spill_count, dry_spill_hrs),
        list(
          p50 = ~ median(., na.rm = TRUE),
          p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
          p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
          max = ~ max(., na.rm = TRUE),
          mean = ~ mean(., na.rm = TRUE)
        ),
        .names = "thr_{.fn}_{.col}_yr"
      ),
      n_sites_spills = n(),
      .groups = "drop"
    ) %>%
    rename(!!paste0("n_sites_spills_yr") := n_sites_spills)

  # Calculate all-time thresholds across the entire dataset
  log_info("Calculating all-time thresholds")
  all_time_thresholds <- metrics_base %>%
    summarise(
      across(c(spill_count, spill_hrs, dry_spill_count, dry_spill_hrs),
        list(
          p50 = ~ median(., na.rm = TRUE),
          p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
          p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
          max = ~ max(., na.rm = TRUE),
          mean = ~ mean(., na.rm = TRUE)
        ),
        .names = "thr_{.fn}_{.col}_all"
      ),
      n_sites_spills = n()
    ) %>%
    rename(n_sites_spills_all = n_sites_spills)

  # Combine base data with thresholds and calculate final metrics
  log_info("Joining data and calculating final {period_type} metrics")
  final_stats <- spill_tbl %>%
    left_join(period_thresholds, by = "spill_date") %>%
    left_join(yearly_thresholds, by = "year") %>%
    cross_join(all_time_thresholds) %>%
    mutate(
      log_spill_count = log(1 + spill_count),
      log_spill_hrs = log(1 + spill_hrs),
      log_dry_spill_count = log(1 + dry_spill_count),
      log_dry_spill_hrs = log(1 + dry_spill_hrs),
      period_type = !!period_type
    ) %>%
    # Create binary indicators
    mutate(
      # Period-specific count indicators
      across(
        starts_with("thr_") & contains("_spill_count_") & ends_with(paste0("_", period_suffix)),
        ~ as.integer(spill_count > .x),
        .names = "d_{.col}"
      )
    ) %>%
    mutate(
      # Period-specific hours indicators
      across(
        starts_with("thr_") & contains("_spill_hrs_") & ends_with(paste0("_", period_suffix)),
        ~ as.integer(spill_hrs > .x),
        .names = "d_{.col}"
      )
    ) %>%
    # Yearly count indicators
    mutate(
      across(
        starts_with("thr_") & contains("_spill_count_") & ends_with("_yr"),
        ~ as.integer(spill_count > .x),
        .names = "d_{.col}"
      )
    ) %>%
    # Yearly hours indicators
    mutate(
      across(
        starts_with("thr_") & contains("_spill_hrs_") & ends_with("_yr"),
        ~ as.integer(spill_hrs > .x),
        .names = "d_{.col}"
      )
    ) %>%
    # All-time count indicators
    mutate(
      across(
        starts_with("thr_") & contains("_spill_count_") & ends_with("_all"),
        ~ as.integer(spill_count > .x),
        .names = "d_{.col}"
      )
    ) %>%
    # All-time hours indicators
    mutate(
      across(
        starts_with("thr_") & contains("_spill_hrs_") & ends_with("_all"),
        ~ as.integer(spill_hrs > .x),
        .names = "d_{.col}"
      )
    ) %>%
    rename_with(~ sub("^d_thr_", "d_", .x), starts_with("d_thr_"))

  log_info("Finished calculating {period_type} spill statistics")
  return(final_stats)
}

#' Export spill statistics as separate parquet files
#' @param monthly_stats Tibble/Lazy Tibble of monthly stats
#' @param quarterly_stats Tibble/Lazy Tibble of quarterly stats
#' @return NULL
export_spill_data <- function(monthly_stats, quarterly_stats) {
  # Ensure base directory exists
  log_info("Exporting spill statistics as separate parquet files")
  output_dir <- CONFIG$output_dir
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  # Prepare monthly data
  log_info("Preparing monthly spill statistics")
  monthly_data <- monthly_stats %>%
    select(
      # Identifiers
      site_id, month_id, 
      # Base metrics
      spill_count, spill_hrs,
      dry_spill_count, dry_spill_hrs,
      # Log metrics
      log_spill_count, log_spill_hrs,
      log_dry_spill_count, log_dry_spill_hrs,
      # Threshold values
      starts_with("thr_"),
      # Indicator flags
      starts_with("d_"),
      # Site counts per period
      starts_with("n_sites_")
    ) %>%
    arrange(site_id, month_id) %>%
    collect()

  # Prepare quarterly data
  log_info("Preparing quarterly spill statistics")
  quarterly_data <- quarterly_stats %>%
    select(
      # Identifiers
      site_id, qtr_id,
      # Base metrics
      spill_count, spill_hrs,
      dry_spill_count, dry_spill_hrs,
      # Log metrics
      log_spill_count, log_spill_hrs,
      log_dry_spill_count, log_dry_spill_hrs,
      # Threshold values
      starts_with("thr_"),
      # Indicator flags
      starts_with("d_"),
      # Site counts per period
      starts_with("n_sites_")
    ) %>%
    arrange(site_id, qtr_id) %>%
    collect()

  # Write monthly data
  monthly_path <- file.path(output_dir, "agg_spill_stats_mo.parquet")
  log_info("Writing monthly data to: {monthly_path}")
  tryCatch(
    {
      arrow::write_parquet(monthly_data, monthly_path)
      log_info("Monthly data: {nrow(monthly_data)} records, {ncol(monthly_data)} columns")
    },
    error = function(e) {
      log_error("Failed to write monthly data: {e$message}")
      stop(e)
    }
  )

  # Write quarterly data
  quarterly_path <- file.path(output_dir, "agg_spill_stats_qtr.parquet")
  log_info("Writing quarterly data to: {quarterly_path}")
  tryCatch(
    {
      arrow::write_parquet(quarterly_data, quarterly_path)
      log_info("Quarterly data: {nrow(quarterly_data)} records, {ncol(quarterly_data)} columns")
    },
    error = function(e) {
      log_error("Failed to write quarterly data: {e$message}")
      stop(e)
    }
  )

  # Clean up
  rm(monthly_data, quarterly_data)
  gc(full = TRUE)

  log_info("Finished exporting spill statistics to separate files")
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_db Boolean indicating whether to refresh the database tables
#' @return NULL
main <- function(refresh_db = FALSE) {

  initialise_environment()
  setup_logging()
  con <- connect_to_db()
  on.exit({
    log_info("Disconnecting from database")
    dbDisconnect(con, shutdown = TRUE)
  })

  if (refresh_db) {
    logger::log_info("Refresh requested – reloading data")
    tables <- DBI::dbListTables(con)
    for (table in c("dat_mo", "dat_qtr")) {
      if (table %in% tables) {
        logger::log_info("Dropping table: {table}")
        DBI::dbRemoveTable(con, table)
      }
    }
  }

  load_data_to_db(con)

  # Calculate stats for both periods
  monthly_stats <- calculate_period_spill_stats(con, "monthly")
  quarterly_stats <- calculate_period_spill_stats(con, "quarterly")

  # Export the combined dataset
  export_spill_data(monthly_stats, quarterly_stats)

  log_info("Spill statistics aggregation completed successfully")
}

# Execute when sourced/run directly
if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
