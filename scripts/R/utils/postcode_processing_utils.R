############################################################
# Postcode Processing Utilities
# Project: Sewage
# Date: 02/09/2025
# Author: Jacopo Olivieri
############################################################

#' Shared functions for processing UK postcodes using the PostcodesioR API.
#' Contains robust postcode geocoding functionality with retry logic, batch
#' processing, and caching for use across multiple data cleaning scripts.

# Dependencies
############################################################

# Lightweight, defensive attachment of required packages for utilities only.
required_packages <- c(
  "tidyverse",   # tibble/dplyr utilities
  "logger",      # logging
  "glue",        # string interpolation
  "here",        # paths
  "fs",          # file ops
  "PostcodesioR",# postcode API client
  "data.table"   # efficient rbindlist
)

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  if (!paste0("package:", pkg) %in% search()) library(pkg, character.only = TRUE)
}))

# Configuration
############################################################

# Standard postcode variables to extract from PostcodesioR API
POSTCODE_VARS <- c(
  "postcode", # Full UK postcode
  "quality", # Positional quality (1-9)
  "eastings", # OS grid reference Easting
  "northings", # OS grid reference Northing
  "country", # Constituent country of UK/Channel Islands/Isle of Man
  "nhs_ha", # Strategic Health Authority code
  "longitude", # WGS84 longitude
  "latitude", # WGS84 latitude
  "region", # Region code (formerly GOR)
  "lsoa", # 2011 Census lower layer super output area
  "msoa", # 2011 Census middle layer super output area
  "admin_county", # County
  "admin_ward" # Administrative/electoral ward
)

# Functions
############################################################

#' Process postcode data using the PostcodesioR API with retry logic
#'
#' Robust function for geocoding UK postcodes using the PostcodesioR API.
#' Includes exponential backoff retry logic, batch processing, intermediate
#' saves, and resume capability for handling large postcode datasets.
#'
#' @param postcodes Vector of postcodes to process
#' @param batch_size Number of postcodes to process in each batch (default: 50)
#' @param max_tries Maximum number of retry attempts (default: 5)
#' @param initial_backoff Initial backoff time in seconds (default: 1)
#' @param resume_from Optional path to a previously saved intermediate result to resume from
#' @return Tibble containing postcode data with coordinates and geographic variables
#' @export
process_postcodes <- function(postcodes, batch_size = 50, max_tries = 5, initial_backoff = 1, resume_from = NULL) {
  # Retry wrapper with exponential backoff to handle transient API errors
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

  # Optional resume from a previous partial run
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
  # Split into batches to keep payload modest
  batch_indices <- split(
    seq_along(postcodes),
    ceiling(seq_along(postcodes) / batch_size)
  )

  # Process batches sequentially with retry logic
  logger::log_info(
    "Processing {length(postcodes)} postcodes in {n_batches} batches"
  )

  results <- purrr::map(
    seq_along(batch_indices),
    function(i) {
      idx <- batch_indices[[i]] # Fixed batch indexing
      logger::log_info("Processing batch {i}/{n_batches} (postcodes {min(idx)}-{max(idx)})")

      # Small inter-batch delay to be gentle to the API
      Sys.sleep(0.2)

      batch_postcodes <- list(postcodes = postcodes[idx])

      # Use retry logic for the API call
      batch_result <- retry_with_backoff(function() {
        PostcodesioR::bulk_postcode_lookup(batch_postcodes)
      })

      # Keep requested fields and drop NULLs
      results <- purrr::map(batch_result, function(x) {
        if (is.null(x) || is.null(x$result)) {
          logger::log_warn("Empty result for a postcode in batch {i}")
          return(NULL)
        }
        res <- x$result
        res <- res[names(res) %in% POSTCODE_VARS]
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

  # Combine results and standardise minimal fields
  logger::log_info("Combining results into single data frame")
  final_results <- data.table::rbindlist(
    unlist(results, recursive = FALSE),
    fill = TRUE,
    use.names = TRUE
  ) %>%
    tibble::as_tibble() %>%
    suppressWarnings() %>%
    dplyr::filter(!is.na(eastings) & !is.na(northings)) %>%
    dplyr::mutate(
      postcode = stringr::str_remove_all(postcode, stringr::fixed(" "))
    ) %>%
    dplyr::rename(
      northing = northings,
      easting = eastings
    )

  # If resuming, combine with previous results
  if (!is.null(resume_from) && file.exists(resume_from)) {
    processed_data <- readRDS(resume_from)
    final_results <- dplyr::bind_rows(processed_data, final_results)
  }

  logger::log_info("Postcode processing complete. Successfully processed {nrow(final_results)} postcodes")
  return(final_results)
}

#' Get postcode data, either from cache or by fetching new data
#'
#' High-level function for obtaining postcode geocoding data with cache management.
#' Uses a shared cache file that can be utilized across multiple scripts.
#'
#' @param df Data frame containing postcodes
#' @param refresh Boolean indicating whether to force refresh of postcode data (default: FALSE)
#' @param cache_path Optional custom cache path (defaults to shared location)
#' @return Tibble containing postcode data with geographic variables
#' @export
get_postcode_data <- function(df, refresh = FALSE, cache_path = NULL) {
  # Use shared cache in data/cache/postcodes if not specified
  if (is.null(cache_path)) {
    cache_path <- here::here("data", "cache", "postcodes", "postcode_data.rds")
  }
  
  if (!refresh && file.exists(cache_path)) {
    logger::log_info("Loading cached postcode data from {cache_path}")
    return(readRDS(cache_path))
  }

  logger::log_info("Fetching new postcode data")
  unique_postcodes <- unique(df$postcode)

  postcode_data <- process_postcodes(unique_postcodes)
  
  # Ensure cache directory exists
  cache_dir <- dirname(cache_path)
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  saveRDS(postcode_data, cache_path)
  logger::log_info("Postcode data cached to {cache_path}")

  postcode_data
}

#' Clean up postcode processing cache and temporary files
#'
#' Utility function to remove cache files and temporary intermediate results
#' after successful completion of postcode processing.
#'
#' @param cache_path Path to cache file to remove (optional)
#' @param remove_temp Boolean indicating whether to remove temp directory (default: TRUE)
#' @return NULL (invisible)
#' @export
cleanup_postcode_cache <- function(cache_path = NULL, remove_temp = TRUE) {
  # Remove main cache file
  if (is.null(cache_path)) {
    cache_path <- here::here("data", "cache", "postcodes", "postcode_data.rds")
  }
  
  if (file.exists(cache_path)) {
    file.remove(cache_path)
    logger::log_info("Removed postcode cache: {cache_path}")
  }
  
  # Remove temporary intermediate files
  if (remove_temp) {
    temp_dir <- here::here("data", "temp")
    if (dir.exists(temp_dir)) {
      temp_files <- list.files(temp_dir, pattern = "postcode_data_intermediate_", full.names = TRUE)
      if (length(temp_files) > 0) {
        file.remove(temp_files)
        logger::log_info("Removed {length(temp_files)} temporary postcode files")
      }
    }
  }
  
  invisible(NULL)
}
