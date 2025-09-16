############################################################
# Create Site-Level House Price Statistics by Radius (Monthly & Quarterly)
# Project: Sewage
# Date: 06/03/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads house price data and the site-house lookup from
#' DuckDB, aggregates house price statistics (median, mean, weighted, counts)
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
  log_path <- here::here("output", "log", "18_site_panel_sales.log")
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
  radius_thresholds = c(250, 500, 1000, 2000),
  base_year = 2021,
  output_dir = here::here("data", "processed", "dat_panel_site", "sales")
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

#' Load house datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  # Check if tables already exist
  existing_tables <- DBI::dbListTables(con)

  # House prices
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

  # Spill lookup
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
}

#' Prepare base house price and lookup tables
#' @param con DuckDB connection
#' @return A list containing prepared lazy database tables
prepare_tables <- function(con) {
  log_info("Preparing base tables")

  house_tbl <- tbl(con, "house_price_data") %>%
    select(house_id, price, date_of_transfer) %>%
    mutate(
      year = year(date_of_transfer),
      month = month(date_of_transfer),
      quarter = quarter(date_of_transfer),
      transfer_date_mo = sql("DATE_TRUNC('month', date_of_transfer)"),
      transfer_date_qtr = sql("DATE_TRUNC('quarter', date_of_transfer)")
    )

  spill_lookup_tbl <- tbl(con, "spill_lookup") %>%
    select(house_id, site_id, distance_m)

  return(list(
    house_tbl = house_tbl,
    spill_lookup_tbl = spill_lookup_tbl
  ))
}

#' Calculate weighted house price statistics
#' @param dt data.table with site_id, date_col, price, distance_m.
#' @param date_col Character name of the date column to group by.
#' @return data.table with site_id, date_col, price_median_w, price_mean_w.
calculate_weighted_stats <- function(dt, date_col) {
  # Filter data for valid weights and prices
  dt_filtered <- dt[!is.na(price) & !is.na(distance_m)]
  if (nrow(dt_filtered) == 0) {
    empty_dt <- data.table(
      site_id = integer(0),
      price_median_w = numeric(0),
      price_mean_w = numeric(0)
    )
    empty_dt[, (date_col) := as.Date(character(0))]
    return(empty_dt)
  }

  # Calculate inverse distance weight
  dt_filtered[, weight := 1 / distance_m]

  # Aggregate using matrixStats functions
  weighted_stats <- dt_filtered[, .(
    price_median_w = matrixStats::weightedMedian(price, w = weight, na.rm = TRUE),
    price_mean_w = matrixStats::weightedMean(price, w = weight, na.rm = TRUE)
  ), by = .(site_id, get(date_col))]

  # Rename the date column back dynamically
  setnames(weighted_stats, "get", date_col)
  return(weighted_stats)
}

#' Create site-level panel data by aggregating house prices for a specific radius.
#' @param prepared_tables List containing prepared lazy tables (`house_tbl`, `spill_lookup_tbl`).
#' @param radius_m Numeric radius threshold in meters for this run.
#' @param con DuckDB connection (optional, if tables are already prepared).
#' @return data.table containing aggregated monthly and quarterly stats for the given radius.
create_site_panel_for_radius <- function(prepared_tables, radius_m, con = NULL) {
  log_info("Processing radius: {radius_m}m")

  house_tbl <- prepared_tables$house_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl
  base_year <- CONFIG$base_year

  # 1. Filter houses within the current radius
  houses_in_radius_tbl <- spill_lookup_tbl %>%
    filter(distance_m <= .env$radius_m) %>%
    inner_join(house_tbl, by = "house_id") %>%
    select(
      site_id, house_id,
      transfer_date_mo, transfer_date_qtr,
      year, month, quarter,
      price, distance_m
    )

  # 2. Collect data needed for weighted stats
  log_debug("Collecting data for weighted stats (radius {radius_m}m)")
  houses_in_radius_dt <- houses_in_radius_tbl %>%
    select(site_id, transfer_date_mo, transfer_date_qtr, price, distance_m) %>%
    collect() %>%
    as.data.table()
  log_info(
    "Collected {nrow(houses_in_radius_dt)} rows for weighted calculation
    (radius {radius_m}m)"
  )

  # --- Monthly Aggregation ---
  log_info("Aggregating monthly house prices for radius {radius_m}m")

  # 3a. Calculate unweighted monthly stats using dbplyr
  monthly_unweighted_agg <- houses_in_radius_tbl %>%
    group_by(site_id, transfer_date_mo, year, month) %>%
    summarise(
      price_median = median(price, na.rm = TRUE),
      price_mean = mean(price, na.rm = TRUE),
      n_houses = n_distinct(house_id),
      mean_distance = mean(distance_m, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(date = transfer_date_mo) %>%
    collect() %>%
    as.data.table()

  # 3b. Calculate weighted monthly stats
  log_debug("Calculating weighted monthly stats (radius {radius_m}m)")
  monthly_weighted_agg <- calculate_weighted_stats(
    houses_in_radius_dt, "transfer_date_mo"
  )
  setnames(monthly_weighted_agg, "transfer_date_mo", "date")

  # 3c. Merge monthly weighted and unweighted stats
  monthly_agg <- merge(
    monthly_unweighted_agg, monthly_weighted_agg,
    by = c("site_id", "date")
  )

  # --- Quarterly Aggregation ---
  log_info("Aggregating quarterly house prices for radius {radius_m}m")

  # 4a. Calculate unweighted quarterly stats
  quarterly_unweighted_agg <- houses_in_radius_tbl %>%
    group_by(site_id, transfer_date_qtr, year, quarter) %>%
    summarise(
      price_median = median(price, na.rm = TRUE),
      price_mean = mean(price, na.rm = TRUE),
      n_houses = n_distinct(house_id),
      mean_distance = mean(distance_m, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(date = transfer_date_qtr) %>%
    collect() %>%
    as.data.table()


  # 4b. Calculate weighted quarterly stats
  log_debug("Calculating weighted quarterly stats (radius {radius_m}m)")
  quarterly_weighted_agg <- calculate_weighted_stats(
    houses_in_radius_dt, "transfer_date_qtr"
  )
  setnames(quarterly_weighted_agg, "transfer_date_qtr", "date")

  # 4c. Merge quarterly weighted and unweighted stats
  quarterly_agg <- merge(
    quarterly_unweighted_agg, quarterly_weighted_agg,
    by = c("site_id", "date"), all = TRUE
  )


  # 5. Clean up large intermediate data.table for the radius
  rm(houses_in_radius_dt)
  gc(full = TRUE)


  # 6. Data completion

  # Monthly
  # Create monthly template
  log_debug("Completing monthly data for radius {radius_m}m")
  radius_sites_mo <- unique(monthly_agg$site_id)
  all_dates_mo <- seq.Date(
    min(monthly_agg$date), max(monthly_agg$date),
    by = "month"
  )
  monthly_template <- CJ(
    site_id = radius_sites_mo, date = all_dates_mo, sorted = FALSE
  )

  # Merge monthly data onto template
  monthly_panel <- merge(
    monthly_template, monthly_agg,
    by = c("site_id", "date"), all.x = TRUE
  )

  # Add date components, IDs, fill NAs, calculate logs
  monthly_panel[, `:=`(
    year = data.table::year(date),
    month = data.table::month(date),
    month_id = (data.table::year(date) - base_year) * 12
      + data.table::month(date),
    n_houses = fifelse(is.na(n_houses), 0L, n_houses),
    radius = as.integer(radius_m),
    period_type = "monthly",
    log_price_median = log(price_median),
    log_price_mean = log(price_mean),
    log_price_median_w = log(price_median_w),
    log_price_mean_w = log(price_mean_w)
  )]
  if (!"quarter" %in% names(monthly_panel)) monthly_panel[, quarter := NA_integer_]
  if (!"qtr_id" %in% names(monthly_panel)) monthly_panel[, qtr_id := NA_integer_]

  # Quarterly
  # Create quarterly template
  log_debug("Completing quarterly data for radius {radius_m}m")
  radius_sites_qtr <- unique(quarterly_agg$site_id)
  all_dates_qtr <- seq.Date(
    min(quarterly_agg$date), max(quarterly_agg$date),
    by = "quarter"
  )
  quarterly_template <- CJ(
    site_id = radius_sites_qtr, date = all_dates_qtr, sorted = FALSE
  )

  # Merge quarterly data onto template
  quarterly_panel <- merge(
    quarterly_template, quarterly_agg,
    by = c("site_id", "date"), all.x = TRUE
  )

  # Add date components, IDs, fill NAs, calculate logs
  quarterly_panel[, `:=`(
    year = data.table::year(date),
    quarter = data.table::quarter(date),
    qtr_id = (data.table::year(date) - base_year) * 4
      + data.table::quarter(date),
    n_houses = fifelse(is.na(n_houses), 0L, n_houses),
    radius = as.integer(radius_m),
    period_type = "quarterly",
    log_price_median = log(price_median),
    log_price_mean = log(price_mean),
    log_price_median_w = log(price_median_w),
    log_price_mean_w = log(price_mean_w)
  )]
  if (!"month" %in% names(quarterly_panel)) quarterly_panel[, month := NA_integer_]
  if (!"month_id" %in% names(quarterly_panel)) quarterly_panel[, month_id := NA_integer_]

  # 6. Combine monthly and quarterly data for this radius
  radius_data <- rbindlist(
    list(monthly_panel, quarterly_panel),
    use.names = TRUE,
    fill = TRUE
  )

  # 7. Select and order final columns for consistency
  final_cols <- c(
    # Identifiers
    "site_id", "date", "period_type", "radius",
    "year", "month", "quarter", "month_id", "qtr_id",
    # Stats
    "n_houses", "mean_distance",
    "price_median", "log_price_median",
    "price_mean", "log_price_mean",
    "price_median_w", "log_price_median_w",
    "price_mean_w", "log_price_mean_w"
  )

  radius_data <- select(radius_data, any_of(final_cols))
  setorderv(radius_data, c("site_id", "date", "period_type"))

  log_info("Completed processing for radius {radius_m}m")

  # 8. Return the combined data for this radius
  return(radius_data)
}

#' Export aggregated house price data to partitioned parquet dataset.
#' @param data_list List of data.tables, each containing combined monthly/quarterly data for one radius.
#' @return NULL
export_house_data <- function(data_list) {
  log_info("Exporting aggregated house price statistics")

  # Combine data from all radii
  combined_data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
  rm(data_list)
  gc()

  log_info("Writing partitioned parquet dataset to: {CONFIG$output_dir}")
  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  # Write dataset partitioned by radius and period type
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
    # Setup
    initialise_environment()
    setup_logging()

    # Connect to duckdb
    con <- connect_to_db()
    on.exit({
      log_info("Disconnecting from database")
      dbDisconnect(con, shutdown = TRUE)
    })

    # Load data
    if (refresh_db) {
      log_info("Refresh requested – reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c("house_price_data", "spill_lookup")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    # Prepare data
    prepared_tables <- prepare_tables(con)

    # Aggregate data for each radius
    all_radius_results <- list()
    log_info("Starting house price aggregation across radii")
    for (rad in CONFIG$radius_thresholds) {
      radius_result_dt <- create_site_panel_for_radius(
        prepared_tables = prepared_tables,
        radius_m = rad,
        con = con
      )

      all_radius_results[[as.character(rad)]] <- radius_result_dt

      Sys.sleep(1)
      gc(full = TRUE)
    }
    log_info("Finished processing all radii.")

    # Export data
    export_house_data(all_radius_results)
    log_info("House price statistics aggregation completed successfully")
  }, error = function(e) {
    # Log fatal errors
    log_error("Fatal error during script execution: {e$message}")
    stop(e)
  }, finally = {
    log_info("Script finished at {Sys.time()}")
  })
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
