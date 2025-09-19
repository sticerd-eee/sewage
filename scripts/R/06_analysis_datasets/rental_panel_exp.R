############################################################
# Create Rental Listing-Level Panel Data by Radius
# Project: Sewage
# Date: 16/09/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads Zoopla rental data and the site-rental lookup from
#' DuckDB. For each specified radius, it creates a general dataset containing
#' all rental listing records, both within and outside the specified radius of sites.
#' Rentals are tagged with their relationship to sites (if any) and include metadata
#' about whether they fall within the radius threshold. This mirrors the housing panel
#' export, producing a general panel that can be tailored for different analyses. The
#' final output is a partitioned parquet dataset stored under
#' `data/processed/general_panel/rentals`.

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
  log_path <- here::here("output", "log", "20_rental_panel_exp.log")
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
  radius_thresholds = c(250, 500, 1000),
  output_dir = here::here("data", "processed", "general_panel", "rentals"),
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
      file.path(CONFIG$processed_dir, "zoopla", "zoopla_rentals.parquet"),
      trust = TRUE
    )
    copy_to(con, rental_data, "rental_data", temporary = FALSE)
    rm(rental_data)
    logger::log_info("Rental listing data loaded")
  }

  # Spill lookup
  if (!"rental_spill_lookup" %in% existing_tables) {
    logger::log_info("Loading rental spill lookup data")
    spill_lookup <- import(
      file.path(CONFIG$processed_dir, "zoopla", "spill_rental_lookup.parquet"),
      trust = TRUE
    )
    copy_to(con, spill_lookup, "rental_spill_lookup", temporary = FALSE)
    rm(spill_lookup)
    logger::log_info("Rental spill lookup data loaded")
  }

}

#' Prepare base rental, lookup, and quarterly spill statistics tables
#' @param con DuckDB connection
#' @return A list containing prepared lazy database tables
prepare_tables <- function(con) {
  log_info("Preparing base tables")

  rental_tbl <- tbl(con, "rental_data") %>%
    select(rental_id, listing_price, qtr_id)

  spill_lookup_tbl <- tbl(con, "rental_spill_lookup") %>%
    select(rental_id, site_id, distance_m)

  return(list(
    rental_tbl = rental_tbl,
    spill_lookup_tbl = spill_lookup_tbl
  ))
}

#' Create rental-level panel data for a specific radius.
#' @param prepared_tables List containing prepared lazy tables.
#' @param radius_m Numeric radius threshold in meters for this run.
#' @param con DuckDB connection.
#' @return A dplyr table containing the rental panel for the radius.
create_rental_panel_for_radius <- function(prepared_tables, radius_m, con) {
  log_info("Processing radius: {radius_m}m")

  rental_tbl <- prepared_tables$rental_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl

  # 1. Get unique rental-site pairs within radius
  log_info("Filtering rentals within {radius_m}m radius")
  rental_site_pairs <- spill_lookup_tbl %>%
    filter(distance_m <= .env$radius_m) %>%
    select(rental_id, site_id, distance_m) %>%
    distinct()

  # 2. Get rental listings and create transfer quarter indicator
  rental_listings <- rental_tbl %>%
    select(rental_id, qtr_id) %>%
    rename(qtr_id_transfer = qtr_id)

  # 3. Get all unique quarters from the data
  all_quarters <- rental_tbl %>%
    select(qtr_id) %>%
    distinct()

  # 4. Create complete panel for rentals within radius
  log_info("Building quarterly rental panel for radius {radius_m}m")
  rentals_in_radius_tbl_complete <- rental_site_pairs %>%
    # Cross join with all quarters to create rental-site-quarter panel
    cross_join(all_quarters) %>%
    # Add transfer quarter information
    left_join(rental_listings, by = "rental_id") %>%
    # Add within_radius flag
    mutate(within_radius = TRUE)

  # 5. Handle rentals outside radius
  rentals_outside_radius_tbl <- rental_tbl %>%
    anti_join(
      rental_site_pairs %>% select(rental_id) %>% distinct(),
      by = "rental_id"
    ) %>%
    mutate(
      qtr_id_transfer = qtr_id,
      within_radius = FALSE,
      distance_m = NA,
      site_id = NA
    )

  # 6. Combine and finalise
  panel <- union_all(rentals_in_radius_tbl_complete, rentals_outside_radius_tbl) %>%
    mutate(radius = as.integer(.env$radius_m)) %>%
    select(
      rental_id, site_id,
      qtr_id, qtr_id_transfer,
      distance_m, radius, within_radius
    ) %>%
    arrange(rental_id, site_id, qtr_id)

  # Clean up
  rm(rental_site_pairs, rental_listings, all_quarters,
     rentals_in_radius_tbl_complete, rentals_outside_radius_tbl)
  gc(full = TRUE)

  return(panel)
}

#' Export rental panel data to partitioned parquet datasets.
#' @param duckdb_panels List of duckdb_tbl objects with panel data.
#' @return NULL
export_rental_panel <- function(duckdb_panels) {
  log_info("Exporting rental panel data to partitioned parquet")

  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  for (radius_name in names(duckdb_panels)) {
    radius_value <- as.integer(radius_name)
    log_info("Processing radius {radius_value}m for export")

    radius_tbl <- duckdb_panels[[radius_name]]

    log_info("Converting radius {radius_value}m data to Arrow format")
    arrow_tbl <- radius_tbl %>%
      arrow::to_arrow()

    log_info("Writing radius {radius_value}m data to disk")
    arrow::write_dataset(
      dataset = arrow_tbl,
      path = CONFIG$output_dir,
      partitioning = "radius",
      format = "parquet",
      existing_data_behavior = "overwrite"
    )

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
    initialise_environment()
    setup_logging()

    con <- connect_to_db()
    on.exit(
      {
        log_info("Disconnecting from database")
        dbDisconnect(con, shutdown = TRUE)
      },
      add = TRUE
    )

    if (refresh_db) {
      log_info("Refresh requested – reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c(
        "rental_data", "rental_spill_lookup"
      )) {
        if (table %in% tables) {
          logger::log_info("Dropping table: {table}")
          DBI::dbRemoveTable(con, table)
        }
      }
    }
    load_data_to_db(con)

    prepared_tables <- prepare_tables(con)

    all_radius_results <- list()
    log_info(
      "Starting rental panel creation across radii: {paste(CONFIG$radius_thresholds, collapse=', ')}m"
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

    export_rental_panel(all_radius_results)
    log_info("Rental-level panel creation completed successfully")
  }, error = function(e) {
    log_error("Fatal error during script execution: {conditionMessage(e)}")
    log_error("Traceback: {paste(capture.output(traceback()), collapse = '\n')}")
    stop(e)
  }, finally = {
    log_info("Script finished at {Sys.time()}")
  })
}

if (sys.nframe() == 0) {
  main(refresh_db = FALSE)
}
