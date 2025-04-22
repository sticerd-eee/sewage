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
  input_dir = here::here("data", "raw", "lr_house_price"),
  output_dir = here::here("data", "processed"),
  postcode_cache_path = here::here("data", "raw", "lr_house_price", "postcode_data.rds"),
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
  required_packages <- c(
    "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "lubridate", "PostcodesioR", "furrr", "future", "memoise", 
    "data.table", "arrow"
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

#' Set up parallel processing configuration
#' @return NULL
setup_parallel <- function() {
  n_workers <- parallelly::availableCores() - 1
  machine_ram <- memuse::Sys.meminfo()$totalram
  ram_limit <- as.numeric(0.8 * machine_ram)

  options(future.globals.maxSize = ram_limit)
  future::plan(future::multisession, workers = n_workers)
  logger::log_info("Parallel processing initialized with {n_workers} workers")
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "12_clean_lr_house_price_data.log")
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

  df %>%
    mutate(
      year = year,
      postcode = str_remove_all(postcode, fixed(" ")),
      date_of_transfer = ymd_hm(date_of_transfer)
    )
}

#' Process postcode data using the PostcodesioR API with retry logic
#' @param postcodes Vector of postcodes to process
#' @param batch_size Number of postcodes to process in each batch
#' @param max_tries Maximum number of retry attempts
#' @param initial_backoff Initial backoff time in seconds
#' @param resume_from Optional path to a previously saved intermediate result to resume from
#' @return Tibble containing postcode data with coordinates
process_postcodes <- function(postcodes, batch_size = 50, max_tries = 5, initial_backoff = 1, resume_from = NULL) {
  # Create retry function with exponential backoff
  retry_with_backoff <- function(expr, tries = max_tries, backoff = initial_backoff) {
    attempt <- 1
    while (attempt <= tries) {
      result <- tryCatch(
        {
          expr()
        },
        error = function(e) {
          if (attempt == tries) {
            logger::log_error("All retry attempts failed. Last error: {e$message}")
            stop(e)
          }
          backoff_time <- backoff * 2^(attempt - 1)
          logger::log_warn("API request failed: {e$message}. Retrying in {backoff_time} seconds. Attempt {attempt}/{tries}")
          Sys.sleep(backoff_time)
          NULL
        }
      )
      if (!is.null(result)) {
        return(result)
      }
      attempt <- attempt + 1
    }
  }

  # Handle resuming from previous run if specified
  if (!is.null(resume_from) && file.exists(resume_from)) {
    logger::log_info("Resuming from {resume_from}")
    processed_data <- readRDS(resume_from)
    processed_postcodes <- processed_data$postcode
    postcodes <- postcodes[!postcodes %in% processed_postcodes]

    if (length(postcodes) == 0) {
      logger::log_info("All postcodes already processed")
      return(processed_data)
    }
    logger::log_info("{length(postcodes)} postcodes remain to be processed")
  }

  n_batches <- ceiling(length(postcodes) / batch_size)
  # Split postcodes into batches
  batch_indices <- split(
    seq_along(postcodes),
    ceiling(seq_along(postcodes) / batch_size)
  )

  # Process batches sequentially with retry logic
  logger::log_info(
    "Processing {length(postcodes)} postcodes in {n_batches} batches"
  )

  results <- map(
    seq_along(batch_indices),
    function(i) {
      idx <- batch_indices[[i]] # Fixed batch indexing
      logger::log_info("Processing batch {i}/{n_batches} (postcodes {min(idx)}-{max(idx)})")

      # Add small delay between batches to avoid hammering the API
      Sys.sleep(0.2)

      batch_postcodes <- list(postcodes = postcodes[idx])

      # Use retry logic for the API call
      batch_result <- retry_with_backoff(function() {
        bulk_postcode_lookup(batch_postcodes)
      })

      # Process successful results, explicitly handling NULLs
      results <- map(batch_result, function(x) {
        if (is.null(x) || is.null(x$result)) {
          logger::log_warn("Empty result for a postcode in batch {i}")
          return(NULL)
        }
        res <- x$result
        res <- res[names(res) %in% CONFIG$postcode_vars]
        res
      })

      # Remove NULL results
      results <- Filter(Negate(is.null), results)

      return(results)
    },
    .progress = TRUE
  )

  # Save intermediate results every 10 batches
  save_interval <- 10
  for (i in seq_along(results)) {
    if (i %% save_interval == 0 || i == length(results)) {
      logger::log_info("Saving intermediate results after batch {i}/{length(results)}")
      intermediate_path <- here::here("data", "temp", glue::glue("postcode_data_intermediate_{i}.rds"))

      # Ensure temp directory exists
      temp_dir <- dirname(intermediate_path)
      if (!dir.exists(temp_dir)) {
        dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
      }

      # Combine results so far
      intermediate_results <- data.table::rbindlist(
        unlist(results[1:i], recursive = FALSE),
        fill = TRUE,
        use.names = TRUE
      )

      saveRDS(intermediate_results, intermediate_path)
    }
  }

  # Combine all results using data.table
  logger::log_info("Combining results into single data frame")
  final_results <- data.table::rbindlist(
    unlist(results, recursive = FALSE),
    fill = TRUE,
    use.names = TRUE
  ) %>%
    as_tibble() %>%
    suppressWarnings() %>%
    filter(!is.na(eastings) & !is.na(northings)) %>%
    mutate(
      postcode = str_remove_all(postcode, fixed(" "))
    ) %>%
    rename(
      northing = northings,
      easting = eastings
    )

  # If resuming, combine with previous results
  if (!is.null(resume_from) && file.exists(resume_from)) {
    processed_data <- readRDS(resume_from)
    final_results <- bind_rows(processed_data, final_results)
  }

  logger::log_info("Postcode processing complete. Successfully processed {nrow(final_results)} postcodes")
  return(final_results)
}

#' Get postcode data, either from cache or by fetching new data
#' @param df Data frame containing postcodes
#' @param refresh Boolean indicating whether to force refresh of postcode data
#' @return Tibble containing postcode data
get_postcode_data <- function(df, refresh = FALSE) {
  if (!refresh && file.exists(CONFIG$postcode_cache_path)) {
    logger::log_info("Loading cached postcode data")
    return(readRDS(CONFIG$postcode_cache_path))
  }

  logger::log_info("Fetching new postcode data")
  unique_postcodes <- unique(df$postcode)

  postcode_data <- process_postcodes(unique_postcodes)
  saveRDS(postcode_data, CONFIG$postcode_cache_path)

  postcode_data
}

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
main <- function(refresh_postcodes = TRUE) {
  tryCatch({
    # Setup
    initialise_environment()
    setup_logging()
    setup_parallel()

    # Load and clean data
    raw_data <- load_all_years()

    # Process postcode data
    postcode_data <- get_postcode_data(raw_data, refresh = refresh_postcodes)

    # Merge data
    final_data <- left_join(raw_data, postcode_data, by = join_by(postcode)) %>%
      mutate(house_id = row_number()) %>%
      relocate(house_id, .before = transaction_id)

    # Export results
    export_data(final_data)
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
