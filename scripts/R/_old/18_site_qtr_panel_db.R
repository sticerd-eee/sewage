############################################################
# Create Quarterly Regression Panel Dataset
# Project: Sewage
# Date: 23/04/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates a quarterly regression panel dataset based on radius
#' from the DuckDB database. The resulting dataset is exported in parquet
#' format for downstream analysis.

# Setup Functions
############################################################

#' Initialise the R environment with required packages and settings
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
  log_path <- here::here("output", "log", "18_site_qtr_panel_db.log")
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
  
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CONFIG$db_path)
    dbExecute(con, "PRAGMA memory_limit='16GB'")
    dbExecute(con, "PRAGMA max_memory='16GB'")
    dbExecute(con, "PRAGMA max_temp_directory_size='500GiB'")
    logger::log_info("Database connection established")
    return(con)
  }, error = function(e) {
    logger::log_error("Failed to connect to database: {e$message}")
    stop(glue::glue("Database connection failed: {e$message}"))
  })
}


#' Load datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  logger::log_info("Loading datasets into DuckDB")
  
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
  
  # Quarterly spill data
  if (!"dat_qtr" %in% existing_tables) {
    logger::log_info("Loading quarterly spill data")
    dat_qtr <- import(
      file.path(
        CONFIG$processed_dir, "spill_aggregated", "agg_spill_qtr.parquet"),
      trust = TRUE
    )
    
    dat_qtr <- dat_qtr %>%
      select(water_company, site_id, year, quarter, spill_count_qt, spill_hrs_qt) %>%
      arrange(site_id, year, quarter)
    
    copy_to(con, dat_qtr, "dat_qtr", temporary = FALSE)
    rm(dat_qtr)
    logger::log_info("Quarterly spill data loaded")
  }
}

#' Prepare tables for the quarterly regression panel
#' @param con DuckDB connection
#' @return A list containing prepared database tables
prepare_tables <- function(con) {
  logger::log_info("Preparing tables for quarterly regression panel")
  
  # Lazy tables
  house_tbl <- tbl(con, "house_price_data")
  spill_lookup_tbl <- tbl(con, "spill_lookup")
  dat_qtr_tbl <- tbl(con, "dat_qtr")
  
  # House price – truncate to quarter
  house_tbl <- house_tbl %>%
    select(house_id, price, date_of_transfer) %>%
    mutate(
      transfer_qtr = sql("DATE_TRUNC('quarter', date_of_transfer)")
    )
  
  # Spill lookup
  spill_lookup_tbl <- spill_lookup_tbl %>%
    select(house_id, site_id, distance_m)
  
  # Quarterly spill metrics
  dat_qtr_tbl <- dat_qtr_tbl %>%
    mutate(
      spill_qtr_date = case_when(
        quarter == 1 ~ as.Date(paste0(year, "-01-01")),
        quarter == 2 ~ as.Date(paste0(year, "-04-01")),
        quarter == 3 ~ as.Date(paste0(year, "-07-01")),
        quarter == 4 ~ as.Date(paste0(year, "-10-01"))
      )
    ) %>%
    rename(spill_count = spill_count_qt, spill_hrs = spill_hrs_qt) %>%
    select(site_id, spill_qtr_date, year, quarter, spill_count, spill_hrs)
  
  return(list(
    house_tbl = house_tbl,
    spill_lookup_tbl = spill_lookup_tbl,
    dat_qtr_tbl = dat_qtr_tbl
  ))
}

#' Create quarterly panel data at site level
#' @param prepared_tables List containing prepared database tables
#' @param con DuckDB connection
#' @return NULL – data is saved directly to parquet files per radius
create_quarterly_panel <- function(prepared_tables, con) {
  logger::log_info("Creating quarterly panel dataset")
  
  house_tbl <- prepared_tables$house_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl
  dat_qtr_tbl <- prepared_tables$dat_qtr_tbl
  
  # Calculate quarterly metrics from spill data (outside radius loop)
  logger::log_info("Calculating quarterly spill metrics across all sites")
  qtr_metrics_base <- dat_qtr_tbl %>%
    filter(!is.na(spill_count) & !is.na(spill_hrs))
  
  # Quarterly thresholds
  qtr_thresholds <- qtr_metrics_base %>%
    group_by(spill_qtr_date) %>%
    summarise(
      across(c(spill_count, spill_hrs),
             list(
               p50 = ~ median(., na.rm = TRUE),
               p75 = ~ quantile(., probs = 0.75, na.rm = TRUE),
               p90 = ~ quantile(., probs = 0.90, na.rm = TRUE),
               max = ~ max(., na.rm = TRUE),
               mean = ~ mean(., na.rm = TRUE)
             ),
             .names = "treated_{.col}_{.fn}_qt"
      ),
      n_sites_spills_qt = n(),
      .groups = "drop"
    )
  
  # Calculate yearly metrics
  logger::log_info("Calculating yearly spillage metrics across all sites")
  yr_thresholds <- qtr_metrics_base %>%
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
  
  # Combine thresholds & augment log vars
  qtr_metrics <- dat_qtr_tbl %>%
    left_join(qtr_thresholds, by = join_by(spill_qtr_date)) %>%
    left_join(yr_thresholds, by = join_by(year)) %>%
    mutate(
      across(c(spill_count, spill_hrs), ~ log(1 + .x), .names = "log_{.col}"),
      qtr_id = (year - 2021) * 4 + quarter
    )
  
  # Process each radius threshold separately to conserve memory
  for (radius in CONFIG$radius_thresholds) {
    logger::log_info("Processing radius {radius}m")
    
    # 1. Calculate house price statistics by site and qtr, filtering by radius
    logger::log_info("Aggregating house price data at site level for radius {radius}m")
    house_qtr_tbl <- house_tbl %>%
      left_join(spill_lookup_tbl, by = "house_id") %>%
      filter(distance_m <= radius) %>%
      group_by(site_id, transfer_qtr) %>%
      summarise(
        price_median = median(price, na.rm = TRUE),
        price_mean = mean(price, na.rm = TRUE),
        n_houses = n_distinct(house_id),
        mean_distance = mean(distance_m, na.rm = TRUE),
        .groups = "drop"
      )
    
    # Weighted statistics – compute in chunks per quarter
    unique_qtrs <- house_tbl %>%
      mutate(transfer_qtr = sql("DATE_TRUNC('quarter', date_of_transfer)")) %>%
      left_join(spill_lookup_tbl, by = "house_id") %>%
      filter(distance_m <= radius) %>%
      distinct(transfer_qtr) %>%
      collect()
    
    house_qtr_w_list <- lapply(unique_qtrs$transfer_qtr, function(qdate) {
      dt <- house_tbl %>%
        left_join(spill_lookup_tbl, by = "house_id") %>%
        filter(
          distance_m <= radius, 
          transfer_qtr == qdate) %>%
        select(site_id, price, distance_m) %>%
        collect() %>%
        as.data.table()
      
      dt[, .(
        price_median_w = matrixStats::weightedMedian(price, w = 1 / distance_m, na.rm = TRUE),
        price_mean_w   = matrixStats::weightedMean(price,   w = 1 / distance_m, na.rm = TRUE)
      ), by = .(site_id)][, transfer_qtr := qdate]
    })
    
    # Combine results
    house_qtr_w <- rbindlist(house_qtr_w_list)
    rm(house_qtr_w_list, unique_qtrs)
    gc(full = TRUE)
    
    # Merge weighted & unweighted
    house_price_panel <- house_qtr_tbl %>%
      left_join(house_qtr_w, by = c("site_id", "transfer_qtr"), copy = TRUE) %>%
      mutate(
        log_price_median = log(price_median),
        log_price_mean = log(price_mean),
        log_price_median_w = log(price_median_w),
        log_price_mean_w = log(price_mean_w),
        n_houses = coalesce(n_houses, 0L),
        radius = as.integer(radius)
      )
    
    # 2. Join with monthly spill metrics and create treatment indicators
    final_panel <- qtr_metrics %>%
      rename(transfer_qtr = spill_qtr_date) %>%
      left_join(house_price_panel, by = join_by(transfer_qtr, site_id)) %>%
      mutate(
        across(
          contains("treated_spill_count") & contains("_qt"),
          ~ if_else(!is.na(spill_count) & spill_count > ., 1L, 0L),
          .names = "{.col}"
        ),
        across(
          contains("treated_spill_hrs") & contains("_qt"),
          ~ if_else(!is.na(spill_hrs) & spill_hrs > ., 1L, 0L),
          .names = "{.col}"
        ),
        across(
          contains("treated_spill_count") & contains("_yr"),
          ~ if_else(!is.na(spill_count) & spill_count > ., 1L, 0L),
          .names = "{.col}"
        ),
        across(
          contains("treated_spill_hrs") & contains("_yr"),
          ~ if_else(!is.na(spill_hrs) & spill_hrs > ., 1L, 0L),
          .names = "{.col}"
        ),
        # Ensure binary indicators are NA where spill data is missing
        across(
          contains("treated_"),
          ~ if_else(is.na(spill_count) & is.na(spill_hrs), NA_integer_, as.integer(.))
        )
      ) %>%
      select(
        site_id,
        transfer_qtr, qtr_id, year, quarter,
        mean_distance, radius,
        price_median, log_price_median, price_median_w, log_price_median_w,
        price_mean, log_price_mean, price_mean_w, log_price_mean_w,
        spill_count, log_spill_count, spill_hrs, log_spill_hrs,
        contains("treated"),
        everything()
      ) %>%
      arrange(site_id, transfer_qtr)
    
    # -------------------------------------------------------
    # 3. Export
    # -------------------------------------------------------
    export_panel_data(final_panel, radius)
    logger::log_info("Completed processing for radius {radius}m")
    
    # Clean-up
    rm(final_panel, house_price_panel, house_qtr_w, house_qtr_tbl)
    gc(full = TRUE)
  }
  
  rm(qtr_metrics, qtr_metrics_base, qtr_thresholds, yr_thresholds)
  gc()
  logger::log_info("All radius thresholds processed successfully (quarterly panel)")
}

#' Export quarterly panel dataset to parquet format
#' @param panel_data Quarterly panel dataset to export
#' @param radius The radius threshold for this dataset
#' @return NULL
export_panel_data <- function(panel_data, radius) {
  export_path <- here::here("data", "processed", "dat_panel_site_qtr")
  radius_dir <- file.path(export_path, paste0("radius=", radius))
  dir.create(radius_dir, recursive = TRUE, showWarnings = FALSE)
  
  parquet_path <- file.path(radius_dir, "data.parquet")
  panel_data %>%
    to_arrow() %>%
    write_parquet(parquet_path)
  
  logger::log_info("Exported quarterly panel for radius {radius}m to: {parquet_path}")
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_db Boolean indicating whether to refresh the database
#' @return NULL
main <- function(refresh_db = FALSE) {
  tryCatch({
    initialise_environment()
    setup_logging()
    
    con <- connect_to_db()
    on.exit({
      logger::log_info("Disconnecting from database")
      DBI::dbDisconnect(con, shutdown = TRUE)
    })
    
    if (refresh_db) {
      logger::log_info("Refresh requested – reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c("house_price_data", "spill_lookup", "dat_qtr")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    
    load_data_to_db(con)
    prepared_tables <- prepare_tables(con)
    create_quarterly_panel(prepared_tables, con)
    
    logger::log_info("Quarterly regression panel dataset creation completed successfully")
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Execute when sourced/run directly
if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
