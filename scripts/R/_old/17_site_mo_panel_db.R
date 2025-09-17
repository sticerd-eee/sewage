############################################################
# Create Monthly Regression Panel Dataset
# Project: Sewage
# Date: 05/03/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates a monthly regression panel dataset based on radius
#' from the DuckDB database. The resulting dataset is exported in parquet
#' format for downstream analysis.

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c(
    "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "lubridate", "DBI", "duckdb", "dbplyr", "arrow", "dtplyr",
    "data.table", "matrixStats"
  )

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "17_site_mo_panel_db.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed"),
  db_path = here::here("data", "duckdb.duckdb"),
  radius_thresholds = c(250, 500, 1000, 2000)
)

# Database Functions
############################################################

#' Connect to the DuckDB database
#' @return DuckDB connection object
connect_to_db <- function() {
  logger::log_info("Connecting to DuckDB at {CONFIG$db_path}")

  tryCatch(
    {
      con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CONFIG$db_path)
      dbExecute(con, "PRAGMA memory_limit='14GB'")
      dbExecute(con, "PRAGMA max_memory='13GB'")
      dbExecute(con, "PRAGMA max_temp_directory_size='500GiB'")
      logger::log_info("Database connection established")
      return(con)
    },
    error = function(e) {
      logger::log_error("Failed to connect to database: {e$message}")
      stop(glue::glue("Database connection failed: {e$message}"))
    }
  )
}


#' Load datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  logger::log_info("Loading datasets into DuckDB")

  # Check if tables already exist
  existing_tables <- DBI::dbListTables(con)

  # Load house price data if needed
  if (!"house_price_data" %in% existing_tables) {
    logger::log_info("Loading house price data")
    house_price_data <- import(
      file.path(CONFIG$processed_dir, "house_price.parquet"),
      trust = TRUE
    )
    copy_to(con, house_price_data, "house_price_data", temporary = FALSE)
    rm(house_price_data)
    logger::log_info("House price data loaded")
  }

  # Load spill lookup data if needed
  if (!"spill_lookup" %in% existing_tables) {
    logger::log_info("Loading spill lookup data")
    spill_lookup <- import(
      file.path(CONFIG$processed_dir, "spill_house_lookup.parquet"),
      trust = TRUE
    )
    copy_to(con, spill_lookup, "spill_lookup", temporary = FALSE)
    rm(spill_lookup)
    logger::log_info("Spill lookup data loaded")
  }

  # Load monthly spill data if needed
  if (!"dat_mo" %in% existing_tables) {
    logger::log_info("Loading monthly spill data")
    dat_mo <- import(
      file.path(
        CONFIG$processed_dir, "agg_spill_stats", "agg_spill_mo.parquet"
      ),
      trust = TRUE
    )

    dat_mo <- dat_mo %>%
      select(water_company, site_id, year, month, spill_count_mo, spill_hrs_mo) %>%
      arrange(site_id, year, month)
    copy_to(con, dat_mo, "dat_mo", temporary = FALSE)
    rm(dat_mo)
    logger::log_info("Monthly spill data loaded")
  }
}

#' Prepare tables for the monthly regression panel
#' @param con DuckDB connection
#' @return A list containing prepared database tables
prepare_tables <- function(con) {
  logger::log_info("Preparing tables for monthly regression panel")

  # Create lazy table references
  house_tbl <- tbl(con, "house_price_data")
  spill_lookup_tbl <- tbl(con, "spill_lookup")
  dat_mo_tbl <- tbl(con, "dat_mo")

  # Prepare house price data
  house_tbl <- house_tbl %>%
    select(house_id, price, date_of_transfer) %>%
    rename(transfer_date = date_of_transfer) %>%
    mutate(
      transfer_date = sql("DATE_TRUNC('month', transfer_date)")
    )

  # Prepare spill lookup data
  spill_lookup_tbl <- spill_lookup_tbl %>%
    select(house_id, site_id, distance_m)

  # Prepare monthly spill data
  dat_mo_tbl <- dat_mo_tbl %>%
    mutate(
      year = as.integer(year),
      month = as.integer(month),
      spill_date = as.Date(paste0(year, "-", month, "-1"))
    ) %>%
    rename(spill_count = spill_count_mo, spill_hrs = spill_hrs_mo) %>%
    select(site_id, spill_date, spill_count, spill_hrs)

  # Return prepared tables
  return(list(
    house_tbl = house_tbl,
    spill_lookup_tbl = spill_lookup_tbl,
    dat_mo_tbl = dat_mo_tbl
  ))
}

#' Create monthly panel data at site level
#' @param prepared_tables List containing prepared database tables
#' @param con DuckDB connection
#' @return NULL - data is saved directly to parquet files per radius
create_monthly_panel <- function(prepared_tables, con) {
  logger::log_info("Creating monthly panel dataset")

  # Extract prepared tables
  house_tbl <- prepared_tables$house_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl
  dat_mo_tbl <- prepared_tables$dat_mo_tbl

  # Calculate monthly metrics from spill data (outside radius loop)
  logger::log_info("Calculating monthly spill metrics across all sites")
  monthly_metrics_base <- dat_mo_tbl %>%
    filter(!is.na(spill_count) & !is.na(spill_hrs))

  monthly_thresholds <- monthly_metrics_base %>%
    group_by(spill_date) %>%
    summarise(
      across(c(spill_count, spill_hrs),
        list(
          p50 = ~ median(., na.rm = TRUE),
          p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
          p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
          max = ~ max(., na.rm = TRUE),
          mean = ~ mean(., na.rm = TRUE)
        ),
        .names = "treated_{.col}_{.fn}_mo"
      ),
      n_sites_spills_mo = n(),
      .groups = "drop"
    )

  # Calculate yearly metrics
  logger::log_info("Calculating yearly spillage metrics across all sites")
  yearly_thresholds <- monthly_metrics_base %>%
    mutate(year = year(spill_date)) %>%
    group_by(year) %>%
    summarise(
      across(c(spill_count, spill_hrs),
        list(
          p50 = ~ median(., na.rm = TRUE),
          p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
          p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
          max = ~ max(., na.rm = TRUE),
          mean = ~ mean(., na.rm = TRUE)
        ),
        .names = "treated_{.col}_{.fn}_yr"
      ),
      n_sites_spills_yr = n(),
      .groups = "drop"
    )


  # Combine base data with monthly and yearly thresholds
  monthly_metrics <- dat_mo_tbl %>%
    mutate(year = year(spill_date)) %>%
    left_join(monthly_thresholds, by = join_by(spill_date)) %>%
    left_join(yearly_thresholds, by = join_by(year)) %>%
    mutate(
      across(c(spill_count, spill_hrs), ~ log(1 + .x), .names = "log_{.col}"),
      # Ensure threshold columns are NA if the site's spill data is NA for that month
      across(
        contains("treated_"),
        ~ if_else(is.na(spill_count) | is.na(spill_hrs), NA_real_, .)
      ),
      month_id = (year(spill_date) - 2021) * 12 + month(spill_date)
    )

  # Process each radius threshold separately to conserve memory
  for (radius in CONFIG$radius_thresholds) {
    logger::log_info("Processing radius {radius}m")

    # 1. Calculate house price statistics by site and month, filtering by radius
    logger::log_info("Aggregating house price data at site level for radius {radius}m")
    gc(full = TRUE)

    house_agg <- house_tbl %>%
      left_join(spill_lookup_tbl, by = "house_id") %>%
      group_by(site_id, transfer_date) %>%
      filter(distance_m <= radius) %>%
      summarise(
        price_median = median(price, na.rm = TRUE),
        price_mean = mean(price, na.rm = TRUE),
        n_houses = n_distinct(house_id),
        mean_distance = mean(distance_m, na.rm = TRUE),
        .groups = "drop"
      )

    unique_dates <- house_tbl %>%
      left_join(spill_lookup_tbl, by = "house_id") %>%
      filter(distance_m <= radius) %>%
      distinct(transfer_date) %>%
      collect()

    house_agg_w_list <- lapply(unique_dates$transfer_date, function(date) {
      date_data <- house_tbl %>%
        left_join(spill_lookup_tbl, by = "house_id") %>%
        filter(
          distance_m <= radius,
          transfer_date == date
        ) %>%
        select(site_id, transfer_date, price, distance_m) %>%
        collect() %>%
        as.data.table()

      date_data[, .(
        price_median_w = matrixStats::weightedMedian(price, w = 1 / distance_m, na.rm = TRUE),
        price_mean_w = matrixStats::weightedMean(price, w = 1 / distance_m, na.rm = TRUE)
      ), by = .(site_id, transfer_date)]
    })

    # Combine results
    house_agg_w <- rbindlist(house_agg_w_list)
    rm(house_agg_w_list, unique_dates)
    gc(full = TRUE)

    house_price_panel <- house_agg %>%
      left_join(house_agg_w,
        by = c("site_id", "transfer_date"),
        copy = TRUE
      ) %>%
      mutate(
        log_price_median = log(price_median),
        log_price_mean = log(price_mean),
        log_price_median_w = log(price_median_w),
        log_price_mean_w = log(price_mean_w),
        n_houses = ifelse(is.na(n_houses), 0, n_houses),
        radius = as.integer(radius)
      )
    rm(house_agg, house_agg_w)
    gc(full = TRUE)

    # 2. Join with monthly spill metrics and create treatment indicators
    logger::log_info("Joining spill metrics with house data for radius {radius}m")
    final_panel <- monthly_metrics %>%
      rename(transfer_date = spill_date) %>%
      left_join(house_price_panel, by = join_by(transfer_date, site_id)) %>%
      mutate(
        # Replace all monthly spill_count thresholds with binary indicators
        across(
          contains("treated_spill_count") & contains("_mo"),
          ~ if_else(!is.na(spill_count) & spill_count > ., 1, 0),
          .names = "{.col}"
        ),
        # Replace all monthly spill_hrs thresholds with binary indicators
        across(
          contains("treated_spill_hrs") & contains("_mo"),
          ~ if_else(!is.na(spill_hrs) & spill_hrs > ., 1, 0),
          .names = "{.col}"
        ),
        # Replace all yearly spill_count thresholds with binary indicators
        across(
          contains("treated_spill_count") & contains("_yr"),
          ~ if_else(!is.na(spill_count) & spill_count > ., 1, 0),
          .names = "{.col}"
        ),
        # Replace all yearly spill_hrs thresholds with binary indicators
        across(
          contains("treated_spill_hrs") & contains("_yr"),
          ~ if_else(!is.na(spill_hrs) & spill_hrs > ., 1, 0),
          .names = "{.col}"
        ),
        # Ensure binary indicators are NA if original spill data was NA
        across(
          contains("treated_"),
          ~ if_else(is.na(spill_hrs) & is.na(spill_count), NA_integer_, as.integer(.))
        )
      ) %>%
      select(
        site_id,
        transfer_date, month_id, year,
        mean_distance, radius,
        price_median, log_price_median, price_median_w, log_price_median_w,
        price_mean, log_price_mean, price_mean_w, log_price_mean_w,
        spill_count, log_spill_count, spill_hrs, log_spill_hrs,
        contains("treated"),
        everything()
      ) %>%
      arrange(site_id, transfer_date)

    # 3. Export the processed data
    export_panel_data(final_panel, radius)

    logger::log_info("Completed processing for radius {radius}m")

    # Free memory before next iteration
    rm(final_panel, house_price_panel)
    Sys.sleep(1)
    gc(full = TRUE)
  }

  # Free memory for monthly metrics at the end
  rm(monthly_metrics, monthly_metrics_base, monthly_thresholds, yearly_thresholds)
  gc()

  logger::log_info("All radius thresholds processed successfully")
}

#' Export monthly panel dataset to parquet format
#' @param panel_data Monthly panel dataset to export
#' @param radius The radius threshold for this dataset
#' @return NULL
export_panel_data <- function(panel_data, radius) {
  export_path <- here::here("data", "processed", "dat_panel_site_mo")

  # Create the radius subfolder
  radius_dir <- file.path(export_path, paste0("radius=", radius))
  dir.create(radius_dir, recursive = TRUE, showWarnings = FALSE)

  # Write parquet file
  parquet_path <- file.path(radius_dir, "data.parquet")
  panel_data %>%
    to_arrow() %>%
    write_parquet(parquet_path)

  logger::log_info("Exported panel data for radius {radius}m to: {parquet_path}")
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_db Boolean indicating whether to refresh the database
#' @return NULL
main <- function(refresh_db = FALSE) {
  tryCatch({
    # Setup
    initialise_environment()
    setup_logging()

    # Connect to database
    con <- connect_to_db()

    # Clean up on exit
    on.exit({
      logger::log_info("Disconnecting from database")
      DBI::dbDisconnect(con, shutdown = TRUE)
    })

    # Load data to database if needed or if refresh requested
    if (refresh_db) {
      logger::log_info("Refresh requested, reloading all data")
      tables <- DBI::dbListTables(con)
      for (table in c("house_price_data", "spill_lookup", "dat_mo")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    # Prepare tables
    prepared_tables <- prepare_tables(con)

    # Create monthly regression panel (data is saved directly)
    create_monthly_panel(prepared_tables, con)

    logger::log_info("Monthly regression panel dataset creation completed successfully")
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
