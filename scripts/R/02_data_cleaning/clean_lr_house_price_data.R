############################################################
# Clean the HM Land Registry UK property sales data
# Project: Sewage
# Date: 14/01/2025
# Author: Jacopo Olivieri
############################################################

#' This script cleans and combines the Price Paid Data for the years
#' 2021-2023, published by HM Land Registry, regarding all UK property sales.
#' It's part of the data preparation pipeline for analysing sewage discharge
#' @source https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  base_year = 2021,
  input_dir = here::here("data", "raw", "lr_house_price"),
  output_dir = here::here("data", "processed"),
  # Shared cache path for postcode processing (used across scripts)
  postcode_cache_path = here::here("data", "cache", "postcodes", "lr_house_price_postcodes.rds"),
  column_name_mapping = c(
    "transaction_id", # V1  - Transaction unique identifier
    "price", # V2  - Sale price
    "date_of_transfer", # V3  - Date when sale was completed
    "postcode", # V4  - Postcode at time of transaction
    "property_type", # V5  - D/S/T/F/O
    "old_new", # V6  - Y/N for new build or established
    "duration", # V7  - F/L for Freehold/Leasehold
    "paon", # V8  - Primary address (house number/name)
    "saon", # V9  - Secondary address (flat number etc)
    "street", # V10 - Street name
    "locality", # V11 - Locality
    "town_city", # V12 - Town/City
    "district", # V13 - District
    "county", # V14 - County
    "ppd_category", # V15 - A/B for transaction type
    "record_status" # V16 - A/C/D for record status
  ),
  postcode_vars = c(
    "postcode", # Full UK postcode
    "quality", # Positional quality (1-9)
    "eastings", # OS grid reference Easting
    "northings", # OS grid reference Northing
    "country", # Constituent country of UK/Channel Islands/Isle of Man
    "nhs_ha", # Strategic Health Authority code
    "longitude", # WGS84 longitude
    "latitude", # WGS84 latitude
    # "european_electoral_region", # European Electoral Region code
    # "primary_care_trust",    # Primary Care areas code
    "region", # Region code (formerly GOR)
    "lsoa", # 2011 Census lower layer super output area
    "msoa", # 2011 Census middle layer super output area
    # "incode",                # 3-character inward code
    # "outcode",               # 2-4 character outward code
    # "parliamentary_constituency", # Westminster Parliamentary Constituency code
    # "admin_district",        # District/unitary authority
    # "parish",                # Parish (England)/community (Wales)
    "admin_county", # County
    "admin_ward" # Administrative/electoral ward
    # "ccg",                   # Clinical Commissioning Group
    # "nuts",                  # NUTS/LAU areas code
    # "_code"                  # ONS/GSS Code (9 characters)
  )
)

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Packages used in this script (primary role per line)
  required_packages <- c(
    "rio",        # Data import/export
    "tidyverse",  # Data manipulation and piping
    "purrr",      # Iteration helpers
    "here",       # Project-rooted paths
    "logger",     # Structured logging
    "glue",       # String interpolation
    "fs",         # File-system ops
    "lubridate",  # Date handling
    "arrow"       # Parquet I/O
  )

  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Sequential execution (no parallel plan) to avoid API rate/ordering issues
setup_parallel <- function() {
  logger::log_info("Running sequentially (no future::multisession)")
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "14_clean_lr_house_price_data.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Data Loading Functions
############################################################

#' Load data for a specific year
#' @param year Integer representing the year
#' @return Tibble containing the raw data
#' @throws error if file not found
load_year_data <- function(year) {
  file_path <- file.path(CONFIG$input_dir, glue::glue("pp-{year}.csv"))

  if (!file.exists(file_path)) {
    err_msg <- glue::glue("File not found: {file_path}")
    logger::log_error(err_msg)
    stop(err_msg)
  }

  logger::log_info("Loading data for year {year}")
  rio::import(file_path, setclass = "tbl")
}

#' Load data for all configured years
#' @return Combined tibble of all years' data
load_all_years <- function() {
  logger::log_info("Starting data loading for years {paste(CONFIG$years, collapse=', ')}")

  map_dfr(CONFIG$years, function(year) {
    load_year_data(year) %>%
      clean_data(year)
  })
}

# Data Processing Functions
############################################################

#' Clean and standardize data for a single year
#' @param df Data frame to clean
#' @param year Year of the data
#' @return Cleaned data frame
clean_data <- function(df, year) {
  colnames(df) <- CONFIG$column_name_mapping
  base_year <- CONFIG$base_year

  df %>%
    mutate(
      postcode = str_remove_all(postcode, fixed(" ")),     # Normalise for joins
      date_of_transfer = ymd_hm(date_of_transfer),           # Parse timestamp
      qtr_id = (lubridate::year(date_of_transfer) - base_year) * 4 +
        lubridate::quarter(date_of_transfer),               # Quarter index
      month_id = (lubridate::year(date_of_transfer) - base_year) * 12 +
        lubridate::month(date_of_transfer)                  # Month index
    )
}

# Postcode processing utilities (shared across scripts)
source(here::here("scripts", "R", "utils", "postcode_processing_utils.R"))

# Export Functions
############################################################

#' Export processed data to a single Parquet file
#' @param df Data frame to export
#' @return NULL
export_data <- function(df) {
  parquet_path <- file.path(CONFIG$output_dir, "house_price.parquet")
  
  tryCatch(
    {
      arrow::write_parquet(df, parquet_path)
      logger::log_info("Data exported successfully to Parquet file at {parquet_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export data to Parquet: {e$message}")
      logger::log_error(err_msg)
      stop(err_msg)
    }
  )
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_postcodes Boolean indicating whether to refresh postcode data
#' @return NULL
main <- function(refresh_postcodes = FALSE) {
  # Setup
  initialise_environment()
  setup_logging()
  setup_parallel()

  # Load and clean data
  raw_data <- load_all_years()

  # Process postcode data (cached unless refresh = TRUE)
  postcode_data <- get_postcode_data(
    raw_data,
    refresh = refresh_postcodes,
    cache_path = CONFIG$postcode_cache_path
  )

  # Merge and add identifier
  final_data <- left_join(raw_data, postcode_data, by = join_by(postcode)) %>%
    mutate(house_id = row_number()) %>%
    relocate(house_id, .before = transaction_id)

  # Export results
  export_data(final_data)
  logger::log_info("Script finished at {Sys.time()}")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
