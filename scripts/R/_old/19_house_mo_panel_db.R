############################################################
# Create House-Level Monthly Panel Dataset
# Project: Sewage
# Date: 12/03/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates a house-level monthly panel dataset based on radius
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
    "data.table"
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
  log_path <- here::here("output", "log", "19_house_mo_panel_db.log")
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
        CONFIG$processed_dir, "spill_aggregated", "agg_spill_mo.parquet"
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

#' Create house-level monthly panel dataset for a specific radius
#' @param con DuckDB connection
#' @param rad Numeric radius threshold in metres
#' @return NULL - data is exported directly to parquet format
create_house_panel_for_radius <- function(con, rad) {
  logger::log_info("Creating house-level panel for radius {rad}m")

  # Create references to tables
  house_tbl <- tbl(con, "house_price_data")
  spill_lookup_tbl <- tbl(con, "spill_lookup")

  # Load site-level panel data for this radius
  logger::log_info("Loading site-level panel data for radius {rad}m")
  dat_panel_site <- open_dataset(
    here::here("data", "processed", "dat_panel_site_mo")
  ) %>%
    filter(radius == rad) %>%
    select(site_id, month_id, year, spill_count, spill_hrs) %>%
    to_duckdb(con = con)

  # Prepare house data
  logger::log_info("Preparing house data with spill site lookup")
  house_dat <- house_tbl %>%
    rename(transfer_date = date_of_transfer) %>%
    mutate(
      transfer_date = sql("DATE_TRUNC('month', transfer_date)"),
      month_id = (year(transfer_date) - 2021) * 12 + month(transfer_date),
      radius = as.integer(rad)
    ) %>%
    left_join(spill_lookup_tbl, by = "house_id") %>%
    select(house_id, site_id, month_id, price, distance_m)

  # Group houses into those within radius and those outside
  logger::log_info("Separating houses by radius boundary")
  house_dat_above_rad <- house_dat %>%
    filter(distance_m > rad) %>%
    distinct(house_id, .keep_all = TRUE) %>%
    left_join(dat_panel_site,
      by = join_by(site_id, month_id)
    ) %>%
    rename(month_id_transfer = month_id) %>%
    mutate(
      month_id_spill = month_id_transfer
    ) %>%
    relocate(month_id_spill, .after = month_id_transfer) %>%
    select(
      house_id, site_id, month_id_transfer, month_id_spill,
      price, distance_m, spill_count, spill_hrs
    )
  gc(full = TRUE)

  house_dat_below_rad <- house_dat %>%
    filter(distance_m <= rad) %>%
    inner_join(dat_panel_site,
      by = join_by(site_id),
      suffix = c("_transfer", "_spill")
    ) %>%
    filter(
      month_id_spill <= month_id_transfer,
      month_id_spill >= month_id_transfer - 12
    ) %>%
    relocate(month_id_spill, .after = month_id_transfer) %>%
    select(
      house_id, site_id, month_id_transfer, month_id_spill,
      price, distance_m, spill_count, spill_hrs
    )
  gc(full = TRUE)

  # Create house-level panel by combining both groups and joining with site-level data
  logger::log_info("Creating unified panel with site-level spill data")
  dat_panel_house <- union_all(house_dat_below_rad, house_dat_above_rad) %>%
    mutate(
      radius = as.integer(rad)
    )
  rm(house_dat_above_rad, house_dat_below_rad)
  gc(full = TRUE)


  # Export aggregated data
  export_panel_data(dat_panel_house, rad)
}

#' Create the nearest site summary from the panel data
#' @param panel_data Panel dataset to summarise
#' @return Summarised nearest site dataset
nearest_site_panel <- function(panel_data) {
  logger::log_info("Creating nearest site summary")

  panel_data %>%
    group_by(house_id) %>%
    filter(!all(is.na(spill_count))) %>%
    ungroup() %>%
    group_by(house_id, month_id_transfer, month_id_spill) %>%
    filter(!all(is.na(spill_count)) & !all(is.na(distance_m))) %>%
    slice_min(distance_m, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    arrange(house_id, site_id, month_id_transfer, month_id_spill)
}

#' Create the weighted summary from the panel data
#' @param panel_data Panel dataset to summarise
#' @return Summarised weighted dataset
distance_weighted_panel <- function(panel_data) {
  logger::log_info("Creating weighted summary")

  panel_data %>%
    group_by(house_id) %>%
    filter(!all(is.na(spill_count))) %>%
    ungroup() %>%
    group_by(house_id, month_id_transfer, month_id_spill) %>%
    filter(!all(is.na(spill_count)) & !all(is.na(distance_m))) %>%
    dplyr::summarise(
      price = first(price),
      distance_m = mean(distance_m, na.rm = TRUE),
      spill_count = sum(spill_count / distance_m * 1000, na.rm = TRUE),
      spill_duration = sum(spill_hrs / distance_m * 1000, na.rm = TRUE),
      radius = first(radius),
      .groups = "keep"
    ) %>%
    ungroup() %>%
    mutate(
      across(
        c(spill_count, spill_duration),
        ~ log(.x + 1),
        .names = "log_{.col}"
      )
    ) %>%
    arrange(house_id, month_id_transfer, month_id_spill)
}

#' Export panel dataset to parquet format
#' @param panel_data Panel dataset to export
#' @param rad The radius threshold for this dataset
#' @return NULL
export_panel_data <- function(panel_data, rad) {
  export_path <- here::here("data", "processed")

  # Create base directory
  dir.create(export_path, recursive = TRUE, showWarnings = FALSE)
  logger::log_info("Creating and exporting summarized datasets for radius {rad}m")

  # Create and export nearest site summary
  logger::log_info("Processing nearest site panel")
  dat_panel_house_nearest <- nearest_site_panel(panel_data)
  nearest_dir <- file.path(export_path, "dat_panel_house_nearest", paste0("radius=", rad))
  dir.create(nearest_dir, recursive = TRUE, showWarnings = FALSE)
  nearest_path <- file.path(nearest_dir, "data.parquet")
  logger::log_info("Exporting nearest site panel to {nearest_path}")
  dat_panel_house_nearest %>%
    to_arrow() %>%
    write_parquet(nearest_path)
  rm(dat_panel_house_nearest)
  gc(full = TRUE)

  # Create and export weighted summary
  logger::log_info("Processing distance weighted panel")
  dat_panel_house_weighted <- distance_weighted_panel(panel_data)
  weighted_dir <- file.path(export_path, "dat_panel_house_weighted", paste0("radius=", rad))
  dir.create(weighted_dir, recursive = TRUE, showWarnings = FALSE)
  weighted_path <- file.path(weighted_dir, "data.parquet")
  logger::log_info("Exporting distance weighted panel to {weighted_path}")
  dat_panel_house_weighted %>%
    to_arrow() %>%
    write_parquet(weighted_path)
  rm(dat_panel_house_weighted)
  gc(full = TRUE)

  logger::log_info("Exported summarized data for radius {rad}m")
}

#' Create house-level panel datasets for all configured radius thresholds
#' @param con DuckDB connection
#' @return NULL
create_all_house_panels <- function(con) {
  logger::log_info("Creating house-level panel datasets for all radius thresholds")

  # Process each radius threshold
  for (rad in CONFIG$radius_thresholds) {
    create_house_panel_for_radius(con, rad)
    # Free memory
    gc(full = TRUE)
  }

  logger::log_info("All house-level summarized panel datasets created successfully")
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

    # Create house-level panel datasets for all radius thresholds
    create_all_house_panels(con)

    logger::log_info("House-level monthly panel dataset creation completed successfully")
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
