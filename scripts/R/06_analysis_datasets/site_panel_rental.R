############################################################
# Create Site-Level Rental Price Statistics by Radius (Monthly & Quarterly)
# Project: Sewage
# Date: 16/09/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads Zoopla rental data and the spill-to-rental lookup from
#' DuckDB, aggregates rental price statistics (median, mean, weighted, counts)
#' for each site at monthly and quarterly intervals, iterating through
#' specified distance radii. Results are exported to a partitioned
#' parquet dataset.

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
    "lubridate", "DBI", "duckdb", "dbplyr", "arrow", "dtplyr",
    "data.table", "matrixStats"
  )
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "18_site_panel_rentals.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  log_appender(appender_file(log_path))
  log_layout(layout_glue_colors)
  log_threshold(DEBUG)
  log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  db_path = here::here("data", "duckdb.duckdb"),
  radius_thresholds = c(250, 500, 1000, 2000),
  base_year = 2021,
  output_dir = here::here("data", "processed", "dat_panel_site", "rentals"),
  rental_data_file = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  lookup_file = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
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
      dbExecute(con, "PRAGMA memory_limit='14GB'")
      dbExecute(con, "PRAGMA max_memory='14GB'")
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

#' Load rental datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  existing_tables <- DBI::dbListTables(con)

  if (!"rental_price_data" %in% existing_tables) {
    logger::log_info("Loading Zoopla rental listings")
    rental_data <- import(CONFIG$rental_data_file, trust = TRUE)
    copy_to(con, rental_data, "rental_price_data", temporary = FALSE)
    rm(rental_data)
    logger::log_info("Rental listings loaded")
  }

  if (!"spill_rental_lookup" %in% existing_tables) {
    logger::log_info("Loading spill-to-rental lookup data")
    rental_lookup <- import(CONFIG$lookup_file, trust = TRUE)
    copy_to(con, rental_lookup, "spill_rental_lookup", temporary = FALSE)
    rm(rental_lookup)
    logger::log_info("Lookup data loaded")
  }
}

#' Prepare base rental and lookup tables
#' @param con DuckDB connection
#' @return A list containing prepared lazy database tables
prepare_tables <- function(con) {
  log_info("Preparing base tables")

  rental_tbl <- tbl(con, "rental_price_data") %>%
    filter(!is.na(rented_est)) %>%
    select(rental_id, listing_price, rented_est) %>%
    mutate(
      year = year(rented_est),
      month = month(rented_est),
      quarter = quarter(rented_est),
      rent_date_mo = sql("DATE_TRUNC('month', rented_est)"),
      rent_date_qtr = sql("DATE_TRUNC('quarter', rented_est)")
    )

  lookup_tbl <- tbl(con, "spill_rental_lookup") %>%
    select(rental_id, site_id, distance_m)

  return(list(
    rental_tbl = rental_tbl,
    lookup_tbl = lookup_tbl
  ))
}

#' Calculate weighted rental price statistics
#' @param dt data.table with site_id, date_col, listing_price, distance_m.
#' @param date_col Character name of the date column to group by.
#' @return data.table with weighted stats by site_id and date_col.
calculate_weighted_rent_stats <- function(dt, date_col) {
  dt_filtered <- dt[!is.na(listing_price) & !is.na(distance_m)]
  if (nrow(dt_filtered) == 0) {
    empty_dt <- data.table(
      site_id = integer(0),
      rent_median_w = numeric(0),
      rent_mean_w = numeric(0)
    )
    empty_dt[, (date_col) := as.Date(character(0))]
    return(empty_dt)
  }

  dt_filtered[, weight := 1 / distance_m]

  weighted_stats <- dt_filtered[, .(
    rent_median_w = matrixStats::weightedMedian(listing_price, w = weight, na.rm = TRUE),
    rent_mean_w = matrixStats::weightedMean(listing_price, w = weight, na.rm = TRUE)
  ), by = .(site_id, get(date_col))]

  setnames(weighted_stats, "get", date_col)
  return(weighted_stats)
}

#' Create site-level rental panel data for a specific radius.
#' @param prepared_tables List containing prepared lazy tables (`rental_tbl`, `lookup_tbl`).
#' @param radius_m Numeric radius threshold in meters for this run.
#' @return data.table containing aggregated monthly and quarterly stats for the given radius.
create_rental_panel_for_radius <- function(prepared_tables, radius_m) {
  log_info("Processing radius: {radius_m}m")

  rental_tbl <- prepared_tables$rental_tbl
  lookup_tbl <- prepared_tables$lookup_tbl
  base_year <- CONFIG$base_year

  rentals_in_radius_tbl <- lookup_tbl %>%
    filter(distance_m <= .env$radius_m) %>%
    inner_join(rental_tbl, by = "rental_id") %>%
    select(
      site_id, rental_id,
      rent_date_mo, rent_date_qtr,
      year, month, quarter,
      listing_price, distance_m
    )

  log_debug("Collecting data for weighted stats (radius {radius_m}m)")
  rentals_in_radius_dt <- rentals_in_radius_tbl %>%
    select(site_id, rent_date_mo, rent_date_qtr, listing_price, distance_m) %>%
    collect() %>%
    as.data.table()
  log_info(
    "Collected {nrow(rentals_in_radius_dt)} rows for weighted calculation (radius {radius_m}m)"
  )

  # Monthly aggregation
  log_info("Aggregating monthly rental prices for radius {radius_m}m")
  monthly_unweighted_agg <- rentals_in_radius_tbl %>%
    group_by(site_id, rent_date_mo, year, month) %>%
    summarise(
      rent_median = median(listing_price, na.rm = TRUE),
      rent_mean = mean(listing_price, na.rm = TRUE),
      n_rentals = n_distinct(rental_id),
      mean_distance = mean(distance_m, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(date = rent_date_mo) %>%
    collect() %>%
    as.data.table()

  log_debug("Calculating weighted monthly stats (radius {radius_m}m)")
  monthly_weighted_agg <- calculate_weighted_rent_stats(
    rentals_in_radius_dt, "rent_date_mo"
  )
  setnames(monthly_weighted_agg, "rent_date_mo", "date")

  monthly_agg <- merge(
    monthly_unweighted_agg, monthly_weighted_agg,
    by = c("site_id", "date")
  )

  # Quarterly aggregation
  log_info("Aggregating quarterly rental prices for radius {radius_m}m")
  quarterly_unweighted_agg <- rentals_in_radius_tbl %>%
    group_by(site_id, rent_date_qtr, year, quarter) %>%
    summarise(
      rent_median = median(listing_price, na.rm = TRUE),
      rent_mean = mean(listing_price, na.rm = TRUE),
      n_rentals = n_distinct(rental_id),
      mean_distance = mean(distance_m, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(date = rent_date_qtr) %>%
    collect() %>%
    as.data.table()

  log_debug("Calculating weighted quarterly stats (radius {radius_m}m)")
  quarterly_weighted_agg <- calculate_weighted_rent_stats(
    rentals_in_radius_dt, "rent_date_qtr"
  )
  setnames(quarterly_weighted_agg, "rent_date_qtr", "date")

  quarterly_agg <- merge(
    quarterly_unweighted_agg, quarterly_weighted_agg,
    by = c("site_id", "date"), all = TRUE
  )

  rm(rentals_in_radius_dt)
  gc(full = TRUE)

  log_debug("Completing monthly data for radius {radius_m}m")
  radius_sites_mo <- unique(monthly_agg$site_id)
  all_dates_mo <- seq.Date(
    min(monthly_agg$date), max(monthly_agg$date),
    by = "month"
  )
  monthly_template <- CJ(
    site_id = radius_sites_mo, date = all_dates_mo, sorted = FALSE
  )

  monthly_panel <- merge(
    monthly_template, monthly_agg,
    by = c("site_id", "date"), all.x = TRUE
  )

  monthly_panel[, `:=`(
    year = data.table::year(date),
    month = data.table::month(date),
    month_id = (data.table::year(date) - base_year) * 12
      + data.table::month(date),
    n_rentals = fifelse(is.na(n_rentals), 0L, n_rentals),
    radius = as.integer(radius_m),
    period_type = "monthly",
    log_rent_median = log(rent_median),
    log_rent_mean = log(rent_mean),
    log_rent_median_w = log(rent_median_w),
    log_rent_mean_w = log(rent_mean_w)
  )]
  if (!"quarter" %in% names(monthly_panel)) monthly_panel[, quarter := NA_integer_]
  if (!"qtr_id" %in% names(monthly_panel)) monthly_panel[, qtr_id := NA_integer_]

  log_debug("Completing quarterly data for radius {radius_m}m")
  radius_sites_qtr <- unique(quarterly_agg$site_id)
  all_dates_qtr <- seq.Date(
    min(quarterly_agg$date), max(quarterly_agg$date),
    by = "quarter"
  )
  quarterly_template <- CJ(
    site_id = radius_sites_qtr, date = all_dates_qtr, sorted = FALSE
  )

  quarterly_panel <- merge(
    quarterly_template, quarterly_agg,
    by = c("site_id", "date"), all.x = TRUE
  )

  quarterly_panel[, `:=`(
    year = data.table::year(date),
    quarter = data.table::quarter(date),
    qtr_id = (data.table::year(date) - base_year) * 4
      + data.table::quarter(date),
    n_rentals = fifelse(is.na(n_rentals), 0L, n_rentals),
    radius = as.integer(radius_m),
    period_type = "quarterly",
    log_rent_median = log(rent_median),
    log_rent_mean = log(rent_mean),
    log_rent_median_w = log(rent_median_w),
    log_rent_mean_w = log(rent_mean_w)
  )]
  if (!"month" %in% names(quarterly_panel)) quarterly_panel[, month := NA_integer_]
  if (!"month_id" %in% names(quarterly_panel)) quarterly_panel[, month_id := NA_integer_]

  radius_data <- rbindlist(
    list(monthly_panel, quarterly_panel),
    use.names = TRUE,
    fill = TRUE
  )

  final_cols <- c(
    "site_id", "date", "period_type", "radius",
    "year", "month", "quarter", "month_id", "qtr_id",
    "n_rentals", "mean_distance",
    "rent_median", "log_rent_median",
    "rent_mean", "log_rent_mean",
    "rent_median_w", "log_rent_median_w",
    "rent_mean_w", "log_rent_mean_w"
  )

  radius_data <- select(radius_data, any_of(final_cols))
  setorderv(radius_data, c("site_id", "date", "period_type"))

  log_info("Completed processing for radius {radius_m}m")
  return(radius_data)
}

#' Export aggregated rental price data to partitioned parquet dataset.
#' @param data_list List of data.tables for each radius.
#' @return NULL
export_rental_data <- function(data_list) {
  log_info("Exporting aggregated rental price statistics")

  combined_data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  rm(data_list)
  gc()

  log_info("Writing partitioned parquet dataset to: {CONFIG$output_dir}")
  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  arrow::write_dataset(
    dataset = combined_data,
    path = CONFIG$output_dir,
    partitioning = c("radius", "period_type"),
    format = "parquet",
    existing_data_behavior = "overwrite"
  )

  log_info("Export complete. Data written to {CONFIG$output_dir}")
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_db Boolean indicating whether to refresh the database tables
#' @return NULL
main <- function(refresh_db = FALSE) {
  tryCatch({
    initialise_environment()
    setup_logging()

    con <- connect_to_db()
    on.exit({
      log_info("Disconnecting from database")
      dbDisconnect(con, shutdown = TRUE)
    })

    if (refresh_db) {
      log_info("Refresh requested – reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c("rental_price_data", "spill_rental_lookup")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    prepared_tables <- prepare_tables(con)

    all_radius_results <- list()
    log_info("Starting rental price aggregation across radii")
    for (rad in CONFIG$radius_thresholds) {
      radius_result_dt <- create_rental_panel_for_radius(
        prepared_tables = prepared_tables,
        radius_m = rad
      )

      all_radius_results[[as.character(rad)]] <- radius_result_dt

      Sys.sleep(1)
      gc(full = TRUE)
    }
    log_info("Finished processing all radii.")

    export_rental_data(all_radius_results)
    log_info("Rental price statistics aggregation completed successfully")
  }, error = function(e) {
    log_error("Fatal error during script execution: {e$message}")
    stop(e)
  }, finally = {
    log_info("Script finished at {Sys.time()}")
  })
}

if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
