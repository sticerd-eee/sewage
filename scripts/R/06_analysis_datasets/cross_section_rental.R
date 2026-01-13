############################################################
# Create Cross-sectional Databases (Zoopla Rentals)
# Project: Sewage
# Date: 15/09/2025
# Author: Jacopo Olivieri (rental adaptation)
############################################################

#' This script creates two cross-sectional databases using DuckDB for Zoopla rentals:
#' 1. One that aggregates sewage spill data across all years
#' 2. One that aggregates data from the 12 months prior to rental date
#' Both are then exported in parquet format for downstream analysis.

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  # Define required packages
  required_packages <- c(
    "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "lubridate", "DBI", "duckdb", "dbplyr", "arrow"
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
  log_path <- here::here("output", "log", "cross_section_rental.log")
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
  radius_thresholds = c(5000, 2000, 1000, 500, 250),
  base_year = 2021,
  # Input paths specific to rentals
  rentals_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  rental_lookup_path = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
  monthly_spill_path = here::here("data", "processed", "agg_spill_stats", "agg_spill_mo.parquet")
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
      dbExecute(con, "PRAGMA memory_limit='16GB'")
      dbExecute(con, "PRAGMA max_memory='16GB'")
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
  logger::log_info("Loading datasets into DuckDB (rentals)")
  
  # Check if tables already exist
  existing_tables <- DBI::dbListTables(con)
  
  # Load rental data if needed
  if (!"rental_data" %in% existing_tables) {
    logger::log_info("Loading rental data from Zoopla parquet")
    rental_data <- import(CONFIG$rentals_path, trust = TRUE)
    copy_to(con, rental_data, "rental_data", temporary = FALSE)
    rm(rental_data)
    logger::log_info("Rental data loaded")
  }
  
  # Load rental spill lookup data if needed
  if (!"spill_rental_lookup" %in% existing_tables) {
    logger::log_info("Loading rental spill lookup data")
    spill_rental_lookup <- import(CONFIG$rental_lookup_path, trust = TRUE)
    copy_to(con, spill_rental_lookup, "spill_rental_lookup", temporary = FALSE)
    rm(spill_rental_lookup)
    logger::log_info("Rental spill lookup data loaded")
  }
  
  # Load monthly spill data if needed
  if (!"dat_mo" %in% existing_tables) {
    logger::log_info("Loading monthly spill data")
    dat_mo <- import(CONFIG$monthly_spill_path, trust = TRUE)
    dat_mo <- dat_mo %>%
      select(water_company, site_id, month_id, spill_count_mo, spill_hrs_mo) %>%
      arrange(site_id, month_id)
    copy_to(con, dat_mo, "dat_mo", temporary = FALSE)
    rm(dat_mo)
    logger::log_info("Monthly spill data loaded")
  }
}

#' Prepare and join datasets for analysis (rentals)
#' @param con DuckDB connection
#' @return A list containing the joined data and radius table
prepare_data <- function(con) {
  logger::log_info("Preparing rental datasets for analysis")

  # Create lazy table references
  rental_tbl <- tbl(con, "rental_data")
  spill_lookup_tbl <- tbl(con, "spill_rental_lookup")
  dat_mo_tbl <- tbl(con, "dat_mo")

  # Prepare rental data
  base_year <- CONFIG$base_year
  rental_tbl <- rental_tbl %>%
    select(rental_id, listing_price, rented_est) %>%
    rename(rent = listing_price, rented_date = rented_est) %>%
    mutate(
      rented_month_id = (year(rented_date) - base_year) * 12 + month(rented_date)
    ) %>%
    select(rental_id, rent, rented_month_id)

  # Prepare spill lookup data
  spill_lookup_tbl <- spill_lookup_tbl %>%
    select(rental_id, site_id, distance_m)

  # Prepare monthly spill data
  dat_mo_tbl <- dat_mo_tbl %>%
    select(site_id, month_id, spill_count_mo, spill_hrs_mo)

  # Merge data
  rental_spill_data_tbl <- rental_tbl %>%
    left_join(spill_lookup_tbl, by = "rental_id") %>%
    left_join(dat_mo_tbl, by = "site_id")

  # Define radius thresholds
  radius_tbl <- tibble(radius = CONFIG$radius_thresholds)
  gc(full = TRUE)

  logger::log_info("Data preparation complete")
  return(list(
    rental_spill_data = rental_spill_data_tbl,
    radius_tbl = radius_tbl,
    rental_tbl = rental_tbl
  ))
}

#' Create cross-sectional database for all years (rentals)
#' @param prepared_data List containing prepared data
#' @param con DuckDB connection
#' @return Aggregated data for all years
create_all_years_db <- function(prepared_data, con) {
  logger::log_info("Creating rental cross-sectional database for all years")

  rental_spill_data_tbl <- prepared_data$rental_spill_data
  radius_tbl <- prepared_data$radius_tbl
  rental_tbl <- prepared_data$rental_tbl

  # 1. Spill statistics: aggregate across all years
  dat_agg_all <- rental_spill_data_tbl %>%
    cross_join(radius_tbl, copy = TRUE) %>%
    group_by(rental_id, radius) %>%
    summarise(
      # Spill metrics
      spill_count = sum(
        if_else(distance_m <= radius, spill_count_mo, 0),
        na.rm = TRUE
      ),
      spill_hrs = sum(
        if_else(distance_m <= radius, spill_hrs_mo, 0),
        na.rm = TRUE
      ),
      n_spill_sites = n_distinct(
        if_else(distance_m <= radius, site_id, NA_integer_),
        na.rm = TRUE
      ),
      # Quality metrics
      n_site_months = sum(if_else(distance_m <= radius, 1, 0)),
      n_spill_count_present = sum(
        if_else(distance_m <= radius, as.integer(!is.na(spill_count_mo)), 0L),
        na.rm = TRUE
      ),
      n_spill_hrs_present = sum(
        if_else(distance_m <= radius, as.integer(!is.na(spill_hrs_mo)), 0L),
        na.rm = TRUE
      ),
      .groups = "drop"
    ) %>%
    # Calculate quality proportions
    mutate(
      prop_spill_count_na = if_else(
        n_site_months > 0,
        1 - (n_spill_count_present / n_site_months), 0 
      ),
      prop_spill_hrs_na = if_else(
        n_site_months > 0,
        1 - (n_spill_hrs_present / n_site_months), 0 
      )
    ) %>% 
    select(-n_spill_count_present, -n_spill_hrs_present) %>% 
    filter(!(prop_spill_count_na == 1 & n_site_months > 0),
           !(prop_spill_hrs_na == 1 & n_site_months > 0))
  gc(full = TRUE)

  # 2. Mean distance: using distinct rental-site pairs
  agg_all_years_distances <- rental_spill_data_tbl %>%
    distinct(rental_id, site_id, distance_m) %>%
    cross_join(radius_tbl, copy = TRUE) %>%
    group_by(rental_id, radius) %>%
    summarise(
      mean_distance = mean(
        if_else(distance_m <= radius, distance_m, NA_real_),
        na.rm = TRUE
      ),
      min_distance = min(
        if_else(distance_m <= radius, distance_m, NA_real_),
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  gc(full = TRUE)

  # 3. Join the spill, distance and rent data
  dat_agg_all <- dat_agg_all %>%
    left_join(agg_all_years_distances, by = c("rental_id", "radius")) %>%
    left_join(select(rental_tbl, rental_id, rent), by = "rental_id") %>%
    arrange(rental_id, radius) %>%
    collect()
  gc(full = TRUE)

  logger::log_info("All years rental cross-sectional database created")
  return(dat_agg_all)
}

#' Create cross-sectional database for prior 12 months (rentals)
#' @param prepared_data List containing prepared data
#' @param con DuckDB connection
#' @return Aggregated data for prior 12 months
create_prior_12mo_db <- function(prepared_data, con) {
  logger::log_info("Creating rental cross-sectional database for prior 12 months")

  rental_spill_data_tbl <- prepared_data$rental_spill_data
  radius_tbl <- prepared_data$radius_tbl
  rental_tbl <- prepared_data$rental_tbl

  # 1. Spill statistics for the trailing 12 months
  dat_agg_12mo <- rental_spill_data_tbl %>%
    filter(
      rented_month_id >= 13,  # January 2022 onwards (month_id = 13)
      month_id >= (rented_month_id - 11),  # 11 months before rental
      month_id <= rented_month_id  # Up to and including rental month
    ) %>%
    cross_join(radius_tbl, copy = TRUE) %>%
    group_by(rental_id, radius) %>%
    summarise(
      # Spill metrics
      spill_count = sum(
        if_else(distance_m <= radius, spill_count_mo, 0),
        na.rm = TRUE
      ),
      spill_hrs = sum(
        if_else(distance_m <= radius, spill_hrs_mo, 0),
        na.rm = TRUE
      ),
      n_spill_sites = n_distinct(
        if_else(distance_m <= radius, site_id, NA_integer_),
        na.rm = TRUE
      ),
      # Quality metrics
      n_site_months = sum(if_else(distance_m <= radius, 1, 0)),
      n_spill_count_present = sum(
        if_else(distance_m <= radius, as.integer(!is.na(spill_count_mo)), 0L),
        na.rm = TRUE
      ),
      n_spill_hrs_present = sum(
        if_else(distance_m <= radius, as.integer(!is.na(spill_hrs_mo)), 0L),
        na.rm = TRUE
      ),
      .groups = "drop"
    ) %>%
    # Calculate quality proportions
    mutate(
      prop_spill_count_na = if_else(
        n_site_months > 0,
        1 - (n_spill_count_present / n_site_months), 0 
      ),
      prop_spill_hrs_na = if_else(
        n_site_months > 0,
        1 - (n_spill_hrs_present / n_site_months), 0 
      )
    ) %>% 
    select(-n_spill_count_present, -n_spill_hrs_present) %>% 
    filter(!(prop_spill_count_na == 1 & n_site_months > 0),
           !(prop_spill_hrs_na == 1 & n_site_months > 0))
  
  gc(full = TRUE)

  # 2. Mean distance: unique rental-site pairs, then aggregate
  agg_12mo_distances <- rental_spill_data_tbl %>%
    filter(
      rented_month_id >= 13,  
      month_id >= (rented_month_id - 11),
      month_id <= rented_month_id
    ) %>%
    distinct(rental_id, site_id, distance_m) %>%
    cross_join(radius_tbl, copy = TRUE) %>%
    group_by(rental_id, radius) %>%
    summarise(
      mean_distance = mean(
        if_else(distance_m <= radius, distance_m, NA_real_),
        na.rm = TRUE
      ),
      min_distance = min(
        if_else(distance_m <= radius, distance_m, NA_real_),
        na.rm = TRUE
      ),
      .groups = "drop"
    )
  gc(full = TRUE)

  # 3. Join the spill, distance and rent data
  dat_agg_12mo <- dat_agg_12mo %>%
    left_join(agg_12mo_distances, by = c("rental_id", "radius")) %>%
    left_join(select(rental_tbl, rental_id, rent), by = "rental_id") %>%
    arrange(rental_id, radius) %>%
    collect()
  gc(full = TRUE)

  logger::log_info("Prior 12 months rental cross-sectional database created")
  return(dat_agg_12mo)
}

#' Export data to Parquet format (partitioned by radius)
#' @param all_years_data Data frame for all years
#' @param prior_12mo_data Data frame for prior 12 months
#' @return NULL
export_data <- function(all_years_data, prior_12mo_data) {
  tryCatch(
    {
      # Export paths (directories for partitioned datasets)
      all_years_path <- here::here("data", "processed", "cross_section", "rentals", "all_years")
      prior_12mo_path <- here::here("data", "processed", "cross_section", "rentals", "prior_12mo")

      # Export data to parquet format
      logger::log_info("Exporting all years rental data to parquet")
      all_years_data %>%
        group_by(radius) %>%
        write_dataset(path = all_years_path, format = "parquet")

      logger::log_info("Exporting prior 12 months rental data to parquet")
      prior_12mo_data %>%
        group_by(radius) %>%
        write_dataset(path = prior_12mo_path, format = "parquet")

      logger::log_info("Data export complete")
      logger::log_info("All years rental data saved to: {all_years_path}")
      logger::log_info("Prior 12 months rental data saved to: {prior_12mo_path}")
    },
    error = function(e) {
      logger::log_error("Data export failed: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
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
      DBI::dbDisconnect(con, shutdown = FALSE)
    })

    # Load data to database if needed or if refresh requested
    if (refresh_db) {
      logger::log_info("Refresh requested, reloading rental-related tables")
      tables <- DBI::dbListTables(con)
      for (table in c("rental_data", "spill_rental_lookup", "dat_mo")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    # Prepare data
    prepared_data <- prepare_data(con)

    # Create cross-sectional databases
    all_years_data <- create_all_years_db(prepared_data, con)
    prior_12mo_data <- create_prior_12mo_db(prepared_data, con)

    # Export data
    export_data(all_years_data, prior_12mo_data)

    logger::log_info("Rental cross-sectional database creation completed successfully")
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
