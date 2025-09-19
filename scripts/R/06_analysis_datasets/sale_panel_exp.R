############################################################
# Create House Sale-Level Panel Data by Radius
# Project: Sewage
# Date: 09/05/2025
# Author: Jacopo Olivieri
############################################################

#' This script reads house price data and the site-house lookup from
#' DuckDB. For each specified radius, it creates a general dataset containing
#' all house sale records, both within and outside the specified radius of sites.
#' Houses are tagged with their relationship to sites (if any) and include metadata
#' about whether they fall within the radius threshold. This creates a general panel
#' that will require further processing depending on the specific analysis requirements.
#' The final output is a partitioned parquet dataset.

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
  log_path <- here::here("output", "log", "20_sale_panel_exp.log")
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
  output_dir = here::here("data", "processed", "general_panel", "sales"),
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

#' Prepare base house price, lookup, and quarterly spill statistics tables
#' @param con DuckDB connection
#' @return A list containing prepared lazy database tables
prepare_tables <- function(con) {
  log_info("Preparing base tables")
  
  base_year <- CONFIG$base_year
  
  # House price data
  house_tbl <- tbl(con, "house_price_data") 
  house_tbl <- house_tbl %>%
    select(house_id, price, qtr_id) 
  
  # Spill lookup
  spill_lookup_tbl <- tbl(con, "spill_lookup") 
  spill_lookup_tbl <- spill_lookup_tbl %>%
    select(house_id, site_id, distance_m)
  
  return(list(
    house_tbl = house_tbl,
    spill_lookup_tbl = spill_lookup_tbl
  ))
}


#' Create house-sale-level panel data for a specific radius.
#' @param prepared_tables List containing prepared lazy tables (`house_tbl`, `spill_lookup_tbl`).
#' @param radius_m Numeric radius threshold in meters for this run.
#' @param con DuckDB connection.
#' @return A list containing two data.tables: `nearest_dt` and `summary_dt`.
create_house_panel_for_radius <- function(prepared_tables, radius_m, con) {
  log_info("Processing radius: {radius_m}m")

  house_tbl <- prepared_tables$house_tbl
  spill_lookup_tbl <- prepared_tables$spill_lookup_tbl

  # 1. Get unique house-site pairs within radius
  log_info("Filtering sites within {radius_m}m radius")
  house_site_pairs <- spill_lookup_tbl %>%
    filter(distance_m <= .env$radius_m) %>%
    select(house_id, site_id, distance_m) %>%
    distinct()

  # 2. Get house sales and create transfer quarter indicator
  house_sales <- house_tbl %>%
    select(house_id, qtr_id) %>%
    rename(qtr_id_transfer = qtr_id)

  # 3. Get all unique quarters from the data
  all_quarters <- house_tbl %>%
    select(qtr_id) %>%
    distinct()

  # 4. Create complete panel for houses within radius
  log_info("Building quarterly panel for radius {radius_m}m")
  houses_in_radius_tbl_complete <- house_site_pairs %>%
    # Cross join with all quarters to create house-site-quarter panel
    cross_join(all_quarters) %>%
    # Add transfer quarter information
    left_join(house_sales, by = "house_id") %>%
    # Add within_radius flag
    mutate(within_radius = TRUE)

  # 5. Handle houses outside radius
  houses_outside_radius_tbl <- house_tbl %>%
    anti_join(
      house_site_pairs %>% select(house_id) %>% distinct(),
      by = "house_id"
    ) %>%
    mutate(
      qtr_id_transfer = qtr_id,
      within_radius = FALSE,
      distance_m = NA,
      site_id = NA
    )

  # 6. Combine and finalise
  panel <- union_all(houses_in_radius_tbl_complete, houses_outside_radius_tbl) %>%
    mutate(radius = as.integer(.env$radius_m)) %>%
    select(
      house_id, site_id,
      qtr_id, qtr_id_transfer,
      distance_m, radius, within_radius
    ) %>%
    arrange(house_id, site_id, qtr_id)

  # Clean up
  rm(house_site_pairs, house_sales, all_quarters,
     houses_in_radius_tbl_complete, houses_outside_radius_tbl)
  gc(full = TRUE)

  return(panel)
}

#' Export panel data to partitioned parquet datasets.
#' The exported data includes all houses (both within and outside the radius),
#' providing a general dataset that can be further processed for specific analyses.
#' @param duckdb_panels List of duckdb_tbl objects with panel data.
#' @return NULL
export_house_panel <- function(duckdb_panels) {
  log_info("Exporting house panel data to partitioned parquet")

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
      partitioning = "radius",
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
    )

    # Load data
    if (refresh_db) {
      log_info("Refresh requested - reloading data")
      tables <- DBI::dbListTables(con)
      for (table in c(
        "house_price_data", "spill_lookup"
      )) {
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
      "Starting house panel creation across radii:
      {paste(CONFIG$radius_thresholds, collapse=', ')}m"
    )
    for (rad in CONFIG$radius_thresholds) {
      radius_results <- create_house_panel_for_radius(
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
    export_house_panel(all_radius_results)
    log_info("House-level panel creation completed successfully")
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
