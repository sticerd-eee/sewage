# ==============================================================================
# CDRC Zoopla Rental Transactions Data Cleaner
# ==============================================================================
#
# Purpose: Clean and combine safeguarded Zoopla rental listings for 2021-2023,
#          enrich them with postcode attributes, and export the canonical
#          rental panel used by downstream sewage and housing analyses.
#
# Author: Jacopo Olivieri
# Date: 2025-09-02
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/raw/zoopla/rentals_safeguarded_2014-2022.csv
#   - data/raw/zoopla/rentals_safeguarded_2023.csv
#   - scripts/R/utils/postcode_processing_utils.R
#
# Outputs:
#   - data/processed/zoopla/zoopla_rental.parquet
#   - output/log/clean_zoopla_data.log
#
# Source:
#   - WhenFresh / Zoopla Property Transactions
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
  "data.table",
  "dplyr",
  "fs",
  "glue",
  "here",
  "logger",
  "lubridate",
  "PostcodesioR",
  "purrr",
  "rio",
  "stringr",
  "tibble",
  "tidyverse"
)

LOG_FILE <- here::here("output", "log", "clean_zoopla_data.log")

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "utils", "postcode_processing_utils.R"),
  local = TRUE
)

# Configuration
############################################################

CONFIG <- list(
  # Time-frame
  years = 2021:2023,
  base_year = 2021,
  # Data retention options
  keep_address_line_1 = TRUE,  # Toggle to FALSE to exclude address data
  # Input file paths
  input_dir = here::here("data", "raw", "zoopla"),
  
  # Output file paths
  output_file = here::here(
    "data", "processed", "zoopla", "zoopla_rental.parquet"
  ),
  # Column renaming map: old_name = new_name
  column_name_mapping = c(
    "zp.Address1" = "address_line_01",      # First line of address
    "zp.Address2" = "address_line_02",      # Second line of address
    "zp.Address3" = "address_line_03",      # Third line of address
    "zp.Postcode" = "postcode",             # Full postcode (spaces removed later)
    "zp.PropertyType" = "property_type",     # Zoopla property type
    "zp.Bedrooms" = "bedrooms",             # Listed bedrooms
    "zp.Bathrooms" = "bathrooms",           # Listed bathrooms
    "zp.Receptions" = "receptions",         # Listed receptions
    "zp.Floors" = "floors",                 # Listed floors
    "zp.ListingCreated" = "listing_created", # Listing creation date
    "zp.ListingPageViews" = "listing_page_views", # Listing page views
    "zp.ListingPrice" = "listing_price",     # Advertised rent
    "zp.LatestToRent" = "latest_to_rent",    # Last day shown "to rent"
    "zp.Rented" = "rented",                  # Date set to rented/let agreed
    "epc.EnergyEfficiency" = "epc_energy_efficiency", # EPC score
    "epc.EnergyRating" = "epc_energy_rating"        # EPC letter
  )
)

# Functions
############################################################

#' Attach the packages used unqualified in this script
#' @return NULL
initialise_environment <- function() {
  invisible(lapply("dplyr", function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

#' Load Zoopla Rental Data
#'
#' Reads the safeguarded Zoopla rental CSV files for 2014-2022 and 2023
#' from the raw input directory and combines them into a single tibble.
#'
#' @param file_path Character. Path to raw Zoopla input directory.
#' @return A tibble with all records combined.
load_data <- function(file_path = CONFIG$input_dir) {
  logger::log_info("Loading Zoopla data from: {basename(file_path)}")

  # Expected input files
  files <- c(
    fs::path(file_path, "rentals_safeguarded_2014-2022.csv"),
    fs::path(file_path, "rentals_safeguarded_2023.csv")
  )

  # Import separately for file-specific tidying
  logger::log_info("Reading files: {paste(basename(files), collapse = ' + ')}")
  df_2014_2022 <- rio::import(files[[1]]) %>% 
    select(-cdrc.File)
  df_2023 <- rio::import(files[[2]]) %>% 
    select(-V1, -cdrc.File)

  # Combine and return
  df <- dplyr::bind_rows(df_2014_2022, df_2023)
  logger::log_info("Loaded {nrow(df)} rows x {ncol(df)} cols")
  tibble::as_tibble(df)
}

#' Export Zoopla Rental Data to Parquet
#'
#' Builds a year-ranged filename when possible; writes to processed/zoopla.
#'
#' @param df Cleaned tibble from `clean_zoopla_data()`
#' @param output_file Optional override of the default output path.
#'   Defaults to `CONFIG$output_file`.
#' @return Full output file path (returned invisibly).
export_zoopla_data <- function(
  df,
  output_file = CONFIG$output_file
) {
  # Ensure output directory exists
  dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

  logger::log_info("Exporting data to: {output_file}")
  arrow::write_parquet(df, output_file)

  invisible(output_file)
}

#' Clean Zoopla Rental Data
#'
#' - Filters observations to CONFIG$years when either `zp.LatestToRent` or
#'   `zp.Rented` falls within those years (robust to date/year formats).
#' - Renames variables to snake_case, aligning with LR naming where possible.
#'
#' @param df Tibble returned by `load_data()`
#' @return Cleaned tibble
clean_zoopla_data <- function(df) {
  df %>%
    # Filter to study years by either rented or latest_to_rent
    filter(
      lubridate::year(`zp.LatestToRent`) %in% CONFIG$years |
        lubridate::year(`zp.Rented`) %in% CONFIG$years
    ) %>%
    # Drop address lines (postcode retained, optionally keep Address1)
    {if (CONFIG$keep_address_line_1) {
      select(., -matches("zp\\.Address[4-9]"))  # Keep Address1, remove others
    } else {
      select(., -contains("zp.Address"))         # Remove all address fields
    }} %>%
    # Standardise names using CONFIG map
    rename_with(
      ~ if_else(
        .x %in% names(CONFIG$column_name_mapping),
        CONFIG$column_name_mapping[.x],
        .x
      )
    ) %>%
    # Drop observations without price data
    filter(!is.na(listing_price)) %>% 
    # Postcode normalisation; prefer rented date, else latest_to_rent
    mutate(
      postcode = stringr::str_remove_all(postcode, stringr::fixed(" ")),
      rented_est = dplyr::coalesce(rented, latest_to_rent)
    ) %>%
    # LR‑aligned time IDs from rented_est
    mutate(
      qtr_id = (lubridate::year(rented_est) - CONFIG$base_year) * 4 + lubridate::quarter(rented_est),
      month_id = (lubridate::year(rented_est) - CONFIG$base_year) * 12 + lubridate::month(rented_est)
    ) %>%
    # Map property types to codes (keep bungalows as "B")
    mutate(
      property_type = stringr::str_to_upper(stringr::str_trim(property_type)),
      property_type = dplyr::case_when(
        is.na(property_type) | property_type == "" ~ NA_character_,
        property_type == "DETACHED" ~ "D",
        property_type == "SEMI-DETACHED" ~ "S",
        property_type == "TERRACED" ~ "T",
        property_type == "FLAT" ~ "F",
        property_type == "BUNGALOW" ~ "B"
      )
    )
}

# Main Workflow
############################################################

#' Main Zoopla Data Processing Pipeline
#'
#' Orchestrates the workflow: initialises environment, sets up logging,
#' loads and cleans the Zoopla rental data, processes postcodes, and exports.
#'
#' @param refresh_postcodes Boolean indicating whether to refresh postcode data (default: FALSE)
#' @return The combined tibble (invisibly) for interactive use.
main <- function(refresh_postcodes = FALSE) {
  tryCatch({
    # Setup
    initialise_environment()
    initialise_logging()

    # Load raw data
    df_raw <- load_data()

    # Clean data
    df <- clean_zoopla_data(df_raw)

    # Process postcode data
    postcode_data <- get_postcode_data(df, refresh = refresh_postcodes)

    # Merge data
    final_data <- left_join(df, postcode_data, by = join_by(postcode)) %>%
      mutate(rental_id = row_number()) %>%
      relocate(rental_id, .before = 1)

    # Export final data
    export_path <- export_zoopla_data(final_data)
    logger::log_info("Export complete: {export_path}")
    
    # Cleanup cache after successful completion
    cleanup_postcode_cache()
    
    invisible(final_data)
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Run pipeline when script is executed directly
if (sys.nframe() == 0) {
  main()
}
