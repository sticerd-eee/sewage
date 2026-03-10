# ==============================================================================
# EDM API Data Batch Downloader
# ==============================================================================
#
# Purpose: Download raw EDM data from ArcGIS Feature/Map Server endpoints for
#          2024 onwards, handle pagination, reconstruct complete JSON payloads,
#          and save timestamped compressed responses for downstream processing.
#
# Author: Jacopo Olivieri
# Date: 2025-04-05
# Date Modified: 2026-03-10
#
# Inputs:
#   - scripts/config/api_config.R - API endpoint definitions and query params
#
# Outputs:
#   - data/raw/edm_data/raw_api_responses/{company}/{timestamp}_{company}.json.gz
#   - output/log/edm_api_download_log.csv
#   - output/log/fetch_edm_api_data_2024_onwards.log
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

REQUIRED_PACKAGES <- c("dplyr", "fs", "httr", "jsonlite", "logger", "lubridate")
LOG_FILE <- here::here("output", "log", "fetch_edm_api_data_2024_onwards.log")

check_required_packages(REQUIRED_PACKAGES)

# Configuration
############################################################

CONFIG <- list(
  # Base directory for raw downloaded API data
  base_save_directory = here::here("data", "raw", "edm_data", "raw_api_responses"),

  # Path to CSV file used for logging download attempts
  metadata_log_file = here::here("output", "log", "edm_api_download_log.csv"),

  # Default maximum records per request for paginated APIs
  default_max_records_per_request = 1000,

  # Path to API configuration file
  api_config_path = here::here("scripts", "config", "api_config.R")
)

# API Download Functions
############################################################

#' Normalize ArcGIS service URLs to the layer root
#'
#' @param base_url ArcGIS service URL, optionally including `/query` and params
#' @return Service-root URL without a query string
normalise_arcgis_service_url <- function(base_url) {
  if (!is.character(base_url) || length(base_url) != 1 || is.na(base_url) || !nzchar(base_url)) {
    stop("`base_url` must be a single non-empty character string.")
  }

  parsed_url <- httr::parse_url(base_url)

  if (is.null(parsed_url$scheme) || is.null(parsed_url$hostname) || is.null(parsed_url$path)) {
    stop("`base_url` is not a valid URL: ", base_url)
  }

  parsed_url$query <- NULL
  parsed_url$params <- NULL
  parsed_url$fragment <- NULL
  parsed_url$path <- sub("/query/?$", "", parsed_url$path)

  httr::build_url(parsed_url)
}

#' Build the ArcGIS query endpoint from a service URL
#'
#' @param base_url ArcGIS service URL
#' @return Query endpoint URL
build_query_url <- function(base_url) {
  paste0(normalise_arcgis_service_url(base_url), "/query")
}

#' Return the first value unless it is NULL
#'
#' @param value Primary value
#' @param default Default used when `value` is NULL
#' @return `value` or `default`
coalesce_null <- function(value, default) {
  if (is.null(value)) {
    return(default)
  }

  value
}

#' Format ArcGIS error objects into a readable string
#'
#' @param error_object Parsed ArcGIS error object
#' @return Character string describing the error
format_arcgis_error <- function(error_object) {
  if (is.null(error_object)) {
    return("Unknown ArcGIS error")
  }

  details <- error_object$details
  detail_text <- NULL
  if (!is.null(details)) {
    details <- as.character(unlist(details, use.names = FALSE))
    details <- details[nzchar(details)]
    if (length(details) > 0) {
      detail_text <- paste(details, collapse = " | ")
    }
  }

  parts <- c(
    if (!is.null(error_object$code)) paste0("code ", error_object$code),
    coalesce_null(error_object$message, "Unknown ArcGIS error"),
    detail_text
  )

  paste(parts[nzchar(parts)], collapse = " - ")
}

#' Validate that a parsed response matches the ArcGIS feature-query contract
#'
#' @param response_data Parsed ArcGIS response
#' @param api_name Human-readable API name for errors
#' @param offset Pagination offset for errors
validate_arcgis_response <- function(response_data, api_name, offset) {
  if (!is.list(response_data)) {
    stop("ArcGIS response for ", api_name, " at offset ", offset, " was not a JSON object.")
  }

  if (!is.null(response_data$error)) {
    stop(
      "ArcGIS returned an error for ", api_name, " at offset ", offset, ": ",
      format_arcgis_error(response_data$error)
    )
  }

  if (!"features" %in% names(response_data)) {
    stop(
      "ArcGIS response for ", api_name, " at offset ", offset,
      " did not contain a `features` element."
    )
  }

  invisible(TRUE)
}

#' Coerce ArcGIS `features` payloads into a data frame
#'
#' @param page_features Parsed `features` element from an ArcGIS response
#' @param api_name Human-readable API name for errors
#' @param offset Pagination offset for errors
#' @return Data frame of features
coerce_arcgis_features <- function(page_features, api_name, offset) {
  if (is.null(page_features)) {
    stop("ArcGIS response for ", api_name, " at offset ", offset, " had `features = NULL`.")
  }

  if (is.data.frame(page_features)) {
    return(page_features)
  }

  if (is.list(page_features) && length(page_features) == 0) {
    return(data.frame())
  }

  if (is.list(page_features)) {
    return(dplyr::bind_rows(page_features))
  }

  stop(
    "ArcGIS response for ", api_name, " at offset ", offset,
    " returned an unsupported `features` structure."
  )
}

#' Fetch data using pagination from ArcGIS Server endpoints
#'
#' @param api_config List containing API endpoint details
#' @param default_max_records Maximum records per request
#' @return List with success status, features, and metadata
fetch_paginated_data <- function(api_config, default_max_records) {
  logger::log_info("Starting download for: {api_config$name}")

  base_url <- api_config$base_url
  query_url <- build_query_url(base_url)
  base_params <- api_config$query_params
  max_records <- coalesce_null(api_config$max_records_per_request, default_max_records)

  # Initialize pagination variables
  offset <- 0
  all_features <- list()
  total_records_fetched <- 0
  limit_exceeded_final <- FALSE
  first_response_metadata <- NULL

  # Paginate through all available data
  while (TRUE) {
    current_params <- c(base_params, list(
      resultOffset = offset,
      resultRecordCount = max_records
    ))

    logger::log_debug("Requesting records starting at offset: {offset}")

    tryCatch(
      {
        response <- httr::GET(query_url, query = current_params, httr::timeout(120))
        httr::stop_for_status(response, task = paste("fetch data from", api_config$name))

        response_text <- httr::content(response, "text", encoding = "UTF-8")
        response_data <- jsonlite::fromJSON(response_text, flatten = TRUE)
        validate_arcgis_response(response_data, api_config$name, offset)

        page_features <- coerce_arcgis_features(
          response_data$features,
          api_config$name,
          offset
        )

        # Preserve metadata structure from first page
        if (is.null(first_response_metadata)) {
          first_response_metadata <- response_data
          first_response_metadata$features <- NULL
        }

        if (nrow(page_features) > 0) {
          num_received <- nrow(page_features)
          logger::log_debug("Received {num_received} features")
          all_features[[length(all_features) + 1]] <- page_features
          total_records_fetched <- total_records_fetched + num_received
          offset <- offset + num_received
        } else {
          logger::log_debug("Received 0 features. Assuming end of data")
          break
        }

        # Check if more pages exist
        limit_exceeded_page <- isTRUE(response_data$exceededTransferLimit)
        limit_exceeded_final <- limit_exceeded_final || limit_exceeded_page

        if (!limit_exceeded_page) {
          logger::log_debug("Server indicated no more data")
          break
        }

        Sys.sleep(0.5) # Pause to be respectful of server load
      },
      error = function(e) {
        logger::log_error("Error during request for {api_config$name} at offset {offset}: {e$message}")
        stop(e)
      }
    )
  }

  logger::log_info("Finished download for: {api_config$name}, total records: {total_records_fetched}")
  if (limit_exceeded_final) {
    logger::log_warn("The server's transfer limit was hit on at least one page during download")
  }

  if (length(all_features) > 0) {
    combined_features_df <- dplyr::bind_rows(all_features)
    return(list(
      success = TRUE,
      combined_features = combined_features_df,
      total_records = total_records_fetched,
      limit_exceeded = limit_exceeded_final,
      metadata_template = first_response_metadata
    ))
  } else if (total_records_fetched == 0 && !limit_exceeded_final) {
    logger::log_info("Query returned 0 features successfully")
    return(list(
      success = TRUE,
      combined_features = data.frame(),
      total_records = 0,
      limit_exceeded = FALSE,
      metadata_template = coalesce_null(first_response_metadata, list())
    ))
  } else {
    logger::log_warn("No features collected, and loop finished unexpectedly")
    return(list(success = FALSE, error_message = "No features collected or initial request failed"))
  }
}

#' Log API download attempts to CSV for tracking
#'
#' @param log_file_path Path to the CSV log file
#' @param log_data Named list with data for the log entry
log_download_attempt <- function(log_file_path, log_data) {
  log_data$TimestampUTC <- format(lubridate::now(tzone = "UTC"), "%Y-%m-%dT%H:%M:%SZ")

  # Define standard log columns
  log_header <- c(
    "TimestampUTC", "ApiID", "ApiName", "Status", "RawDataFilename",
    "RecordsFetched", "PaginationLimitHit", "FileSizeKB", "ErrorMessage"
  )
  log_data_df <- data.frame(log_data)

  # Ensure consistent column structure
  for (col in log_header) {
    if (!col %in% names(log_data_df)) {
      log_data_df[[col]] <- NA
    }
  }
  log_data_df <- log_data_df[, log_header]

  write_header <- !file.exists(log_file_path)

  tryCatch(
    {
      write.table(log_data_df, log_file_path,
        append = !write_header, sep = ",",
        row.names = FALSE, col.names = write_header, quote = TRUE, na = ""
      )
      logger::log_debug("Log entry written to {log_file_path}")
    },
    error = function(e) {
      logger::log_error("Critical Error: Failed to write to log file: {log_file_path} - {e$message}")
    }
  )
}

#' Download, process and save data from a single API endpoint
#'
#' @param api_id API identifier from configuration
#' @param api_config Configuration for the specific API
#' @return Log entry with download attempt details
process_single_api <- function(api_id, api_config) {
  logger::log_info("Processing API: {api_config$name} (ID: {api_id})")

  # Initialize log entry
  log_entry <- list(
    ApiID = api_id,
    ApiName = api_config$name,
    Status = "Failed"
  )

  tryCatch(
    {
      # Fetch data with pagination
      fetch_result <- fetch_paginated_data(api_config, CONFIG$default_max_records_per_request)

      if (!fetch_result$success) {
        stop(coalesce_null(fetch_result$error_message, "Unknown error during data fetch."))
      }

      # Prepare file location and name
      current_timestamp <- Sys.time()
      datetime_str_filename <- format(current_timestamp, "%y%m%d_%H%M")

      safe_company_name_filename <- gsub("[^a-zA-Z0-9_-]+", "_", api_config$name)
      safe_company_name_filename <- gsub("^_|_$", "", safe_company_name_filename)

      company_subdir <- file.path(CONFIG$base_save_directory, safe_company_name_filename)
      fs::dir_create(company_subdir, recurse = TRUE)

      output_filename_base <- paste0(
        datetime_str_filename, "_",
        safe_company_name_filename,
        ".json"
      )
      output_filename_base <- tolower(output_filename_base)
      output_filename_gz <- file.path(company_subdir, paste0(output_filename_base, ".gz"))

      # Reconstruct complete JSON object
      final_json_structure <- fetch_result$metadata_template
      if (is.null(final_json_structure)) {
        logger::log_warn("Metadata template missing for API: {api_id} - JSON structure might be incomplete")
        final_json_structure <- list()
      }
      final_json_structure$features <- fetch_result$combined_features
      final_json_structure$exceededTransferLimit <- fetch_result$limit_exceeded

      # Convert to JSON and save compressed
      json_output_string <- jsonlite::toJSON(
        final_json_structure,
        auto_unbox = TRUE,
        pretty = FALSE,
        digits = NA
      )

      tryCatch(
        {
          gz_conn <- gzfile(output_filename_gz, "wt", encoding = "UTF-8")
          writeLines(json_output_string, gz_conn)
          close(gz_conn)
          file_size_kb <- round(file.info(output_filename_gz)$size / 1024, 2)
          logger::log_info(
            "Raw compressed JSON saved to: {output_filename_gz} ({file_size_kb} KB)"
          )

          # Update log with success info
          log_entry$Status <- "Success"
          log_entry$RawDataFilename <- here::here(output_filename_gz)
          log_entry$RecordsFetched <- fetch_result$total_records
          log_entry$PaginationLimitHit <- fetch_result$limit_exceeded
          log_entry$FileSizeKB <- file_size_kb
          log_entry$ErrorMessage <- NA
        },
        error = function(e_save) {
          logger::log_error("Error saving compressed JSON data: {e_save$message}")
          if (exists("gz_conn") && isOpen(gz_conn)) {
            try(close(gz_conn), silent = TRUE)
          }
          log_entry$ErrorMessage <- paste("File Save Error:", e_save$message)
        }
      )
    },
    error = function(e_main) {
      logger::log_error("Processing failed for API: {api_config$name} - {e_main$message}")
      log_entry$ErrorMessage <- e_main$message
    }
  )

  return(log_entry)
}

# Main Execution
############################################################

#' Main execution function
#' @param refresh_config Whether to reload the API configuration
main <- function(refresh_config = FALSE) {
  tryCatch({
    # Setup
    setup_logging(LOG_FILE)
    logger::log_info("Script started at {Sys.time()}")

    logger::log_info("===== Starting EDM Batch Download Process =====")

    # Load API configurations
    api_config_list <- NULL
    source(CONFIG$api_config_path, local = TRUE)

    if (is.null(api_config_list)) {
      stop("Failed to load API configuration or api_config_list is NULL")
    }

    # Ensure directories exist
    fs::dir_create(dirname(CONFIG$metadata_log_file), recurse = TRUE)
    fs::dir_create(CONFIG$base_save_directory, recurse = TRUE)

    # Process each API endpoint
    for (api_id in names(api_config_list)) {
      current_api_config <- api_config_list[[api_id]]
      log_entry <- process_single_api(api_id, current_api_config)
      log_download_attempt(CONFIG$metadata_log_file, log_entry)
    }

    logger::log_info("===== EDM Batch Download Process Finished =====")
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Script Execution Control
############################################################

# Execute main function only if script is run directly
if (sys.nframe() == 0) {
  tryCatch(
    {
      main(refresh_config = FALSE)
    },
    error = function(e) {
      cat("\n--- Fatal Error during script execution ---\n")
      message("Error: ", e$message)
      cat("--------------------------------------------\n")
      stop(e)
    }
  )
}
