############################################################
# Create Rental-Level Panel Data by Radius
# Project: Sewage
# Date: 21/03/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads Zoopla rental data and the site-rental lookup from
#' DuckDB. For each specified radius, it creates a dataset containing
#' individual rental listing records, linking each let to any site(s)
#' within that radius. The panel only includes rentals that fall within the
#' specified radius of sites. The final output is a partitioned parquet dataset
#' containing all listings across all radii.

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
    "lubridate", "DBI", "duckdb", "dbplyr", "arrow", "dtplyr"
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
  log_path <- here::here("output", "log", "20_rental_panel_within_radius.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed", "zoopla"),
  db_path = here::here("data", "duckdb.duckdb"),
  radius_thresholds = c(250, 500, 1000),
  output_dir = here::here("data", "processed", "within_radius_panel", "rentals"),
  base_year = 2021
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

#' Load rental datasets into DuckDB if not already present
#' @param con DuckDB connection
#' @return NULL
load_data_to_db <- function(con) {
  # Check if tables already exist
  existing_tables <- DBI::dbListTables(con)

  # Rental listings
  if (!"rental_data" %in% existing_tables) {
    logger::log_info("Loading rental listing data")
    rental_data <- import(
      file.path(CONFIG$processed_dir, "zoopla_rentals.parquet"),
      trust = TRUE
    )
    copy_to(con, rental_data, "rental_data", temporary = FALSE)
    rm(rental_data)
    logger::log_info("Rental listing data loaded")
  }

  # Spill lookup
  if (!"rental_spill_lookup" %in% existing_tables) {
    logger::log_info("Loading spill lookup data")
    spill_lookup <- import(
      file.path(CONFIG$processed_dir, "spill_rental_lookup.parquet"),
      trust = TRUE
    )
    copy_to(con, spill_lookup, "rental_spill_lookup", temporary = FALSE)
    rm(spill_lookup)
    logger::log_info("Spill lookup data loaded")
  }
}

#' Prepare base rental and lookup tables with necessary columns
#' @param con DuckDB connection
#' @return A list containing prepared lazy database tables
prepare_tables <- function(con) {
  log_info("Preparing base tables")

  rental_tbl <- tbl(con, "rental_data") %>%
    select(rental_id, listing_price, rented_est) %>%
    mutate(
      year = year(rented_est),
      month = month(rented_est),
      quarter = quarter(rented_est),
      rental_date_mo = sql("DATE_TRUNC('month', rented_est)"),
      rental_date_qtr = sql("DATE_TRUNC('quarter', rented_est)")
    )

  spill_lookup_tbl <- tbl(con, "rental_spill_lookup") %>%
    select(rental_id, site_id, distance_m)

  return(list(
    rental_tbl = rental_tbl,
    spill_lookup_tbl = spill_lookup_tbl
  ))
}

#' Create rental-level panel data for a specific radius.
#' This function filters for rentals within the specified radius of sites and creates panel data.
#' @param prepared_tables List containing prepared lazy tables (`rental_tbl`, `spill_lookup_tbl`).
#' @param radius_m Numeric radius threshold in meters for this run.
#' @param con DuckDB connection.
#' @return A dplyr table containing the rental panel for the radius.
create_rental_panel_for_radius <- function(prepared_tables, radius_m, con) {
  log_info("Processing radius: {radius_m}m")

  rental_tbl <- prepared_tables$rental_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl
  base_year <- CONFIG$base_year

  # 1. Filter rentals within the current radius and join with rental listing data
  log_info("Filtering sites within {radius_m}m radius")
  rentals_in_radius_tbl <- spill_lookup_tbl %>%
    filter(distance_m <= .env$radius_m) %>%
    inner_join(rental_tbl, by = "rental_id") %>%
    select(
      site_id, rental_id,
      rental_date_mo, rental_date_qtr,
      month, quarter,
      listing_price, distance_m
    )

  # 2. Monthly closest rental panel
  log_info("Building monthly panel for radius {radius_m}m")
  monthly <- rentals_in_radius_tbl %>%
    select(site_id, rental_date_mo, rental_id, listing_price, distance_m) %>%
    rename(date = rental_date_mo, price = listing_price) %>%
    # complete panel data
    complete(site_id, date) %>%
    # add metadata
    mutate(
      period_type = "monthly",
      month_id = (lubridate::year(date) - base_year) * 12 + lubridate::month(date),
      radius = as.integer(radius_m)
    )

  # 3. Quarterly closest rental panel
  log_info("Building quarterly panel for radius {radius_m}m")
  quarterly <- rentals_in_radius_tbl %>%
    select(site_id, rental_date_qtr, rental_id, listing_price, distance_m) %>%
    rename(date = rental_date_qtr, price = listing_price) %>%
    # complete panel data
    complete(site_id, date) %>%
    mutate(
      period_type = "quarterly",
      qtr_id = (lubridate::year(date) - base_year) * 4 + lubridate::quarter(date),
      radius = as.integer(radius_m)
    )

  # 4. Combine, order variables, and sort
  panel <- union_all(monthly, quarterly) %>%
    # order columns and sort
    select(
      # identifiers
      site_id, rental_id,
      # date ids
      month_id, qtr_id,
      # metadata
      distance_m, radius, period_type,
    ) %>%
    arrange(site_id, month_id, qtr_id)

  # 5. Clean up intermediate data for the radius
  rm(rentals_in_radius_tbl, monthly, quarterly)
  gc(full = TRUE)

  # 6. Return datasets for this radius
  return(panel)
}

#' Export panel data to partitioned parquet datasets.
#' The exported data only includes rentals within the specified radius thresholds.
#' @param duckdb_panels List of duckdb_tbl objects with panel data.
#' @return NULL
export_rental_panel <- function(duckdb_panels) {
  log_info("Exporting rental panel data to partitioned parquet")

  # Create output directory if it doesn't exist
  log_info("Setting up output directory: {CONFIG$output_dir}")
  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  # Process each radius separately to manage memory better
  for (radius_name in names(duckdb_panels)) {
    radius_value <- as.integer(radius_name)
    log_info("Processing radius {radius_value}m for export")

    # Get the dplyr table for this radius
    radius_tbl <- duckdb_panels[[radius_name]]

    # Convert to Arrow table directly from duckdb
    log_info("Converting radius {radius_value}m data to Arrow format")
    arrow_tbl <- radius_tbl %>%
      arrow::to_arrow()

    # Write this radius data to disk with partitioning
    log_info("Writing radius {radius_value}m data to disk")
    arrow::write_dataset(
      dataset = arrow_tbl,
      path = CONFIG$output_dir,
      partitioning = c("radius", "period_type"),
      format = "parquet",
      existing_data_behavior = "overwrite"
    )

    # Clean up
    rm(arrow_tbl, radius_tbl)
    gc(full = TRUE)
    log_info("Completed export for radius {radius_value}m")
  }

  log_info("Export complete for all radii. Data written to {CONFIG$output_dir}")
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
    on.exit(
      {
        log_info("Disconnecting from database")
        dbDisconnect(con, shutdown = TRUE)
      },
      add = TRUE
    ) # Ensure disconnection even on error

    # Load data
    if (refresh_db) {
      log_info("Refresh requested – reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c("rental_data", "rental_spill_lookup")) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    # Prepare data tables (lazy references)
    prepared_tables <- prepare_tables(con)

    # Process data for each radius
    all_radius_results <- list()
    log_info(
      "Starting rental panel creation across radii:
      {paste(CONFIG$radius_thresholds, collapse=', ')}m"
    )
    for (rad in CONFIG$radius_thresholds) {
      radius_results <- create_rental_panel_for_radius(
        prepared_tables = prepared_tables,
        radius_m = rad,
        con = con
      )

      all_radius_results[[as.character(rad)]] <- radius_results

      Sys.sleep(1)
      gc(full = TRUE)
    }
    log_info("Finished processing all radii.")

    # Export data
    export_rental_panel(all_radius_results)
    log_info("Rental-level panel creation completed successfully")
  }, error = function(e) {
    # Log fatal errors
    log_error("Fatal error during script execution: {conditionMessage(e)}")
    log_error("Traceback: {paste(capture.output(traceback()), collapse = '\n')}")
    stop(e)
  }, finally = {
    log_info("Script finished at {Sys.time()}")
  })
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
