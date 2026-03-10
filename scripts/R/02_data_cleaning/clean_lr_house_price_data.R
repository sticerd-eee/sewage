# ==============================================================================
# HM Land Registry Price Paid Data Cleaner
# ==============================================================================
#
# Purpose: Clean and combine HM Land Registry property sales data for 2021-2023,
#          enrich with cached/geocoded postcode attributes, and write the
#          canonical `house_price.parquet` output used by downstream sales
#          analysis scripts.
#
# Author: Jacopo Olivieri
# Date: 2025-01-14
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/raw/lr_house_price/pp-{year}.csv
#   - data/raw/uk_postcodes/2602_uk_postcodes.csv
#   - scripts/R/utils/postcode_processing_utils.R
#
# Outputs:
#   - data/processed/house_price.parquet
#   - output/log/clean_lr_house_price_data.log
#
# Source:
#   - https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "glue",
  "logger",
  "lubridate",
  "purrr",
  "rio",
  "stringr",
  "tibble"
)

LOG_FILE <- here::here("output", "log", "clean_lr_house_price_data.log")

check_required_packages(REQUIRED_PACKAGES)

# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  base_year = 2021,
  input_dir = here::here("data", "raw", "lr_house_price"),
  canonical_output_path = here::here("data", "processed", "house_price.parquet"),
  local_postcode_lookup_path = here::here(
    "data", "raw", "uk_postcodes", "2602_uk_postcodes.csv"
  ),
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
  )
)

# Setup Functions
############################################################

#' Attach the required packages after fail-fast dependency checks
#' @return NULL
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

#' Sequential execution (no parallel plan) to avoid API rate/ordering issues
setup_parallel <- function() {
  logger::log_info("Running sequentially (no future::multisession)")
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
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
#' @param output_path Destination parquet path
#' @return NULL
export_data <- function(df, output_path) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  tryCatch(
    {
      arrow::write_parquet(df, output_path)
      logger::log_info("Data exported successfully to Parquet file at {output_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export data to Parquet at {output_path}: {e$message}")
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
  initialise_logging()
  setup_parallel()

  logger::log_info(
    "`refresh_postcodes` is ignored for LR builds; local postcode files are used instead."
  )

  # Load and clean data
  raw_data <- load_all_years()

  # Build postcode enrichment using the local lookup keyed on postcode only
  postcode_result <- get_local_postcode_data_for_sales(
    raw_data,
    lookup_path = CONFIG$local_postcode_lookup_path
  )

  # Merge postcode attributes and add a stable house identifier
  final_data <- left_join(
    raw_data,
    postcode_result$postcode_data,
    by = "postcode"
  ) %>%
    mutate(house_id = row_number()) %>%
    relocate(house_id, .before = transaction_id)

  export_data(final_data, CONFIG$canonical_output_path)

  logger::log_info("Script finished at {Sys.time()}")

  invisible(
    list(
      output_path = CONFIG$canonical_output_path,
      diagnostics = postcode_result$diagnostics
    )
  )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
