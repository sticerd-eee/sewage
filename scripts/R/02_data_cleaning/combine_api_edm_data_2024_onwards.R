# ==============================================================================
# EDM API Parquet Combiner
# ==============================================================================
#
# Purpose: Read the per-company 2024+ EDM API Parquet files, combine and clean
#          them, and write the consolidated dataset used by downstream analysis
#          steps.
#
# Author: Jacopo Olivieri
# Date: 2025-04-06
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/processed/edm_api_data/{company_id}.parquet
#   - scripts/config/api_config.R
#
# Outputs:
#   - data/processed/edm_api_data/combined_api_data.parquet
#   - output/log/06_combine_api_edm_data_2024_onwards.log
#
# ==============================================================================

# Setup
############################################################

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
source(here::here("scripts", "config", "api_config.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow", "dplyr", "fs",
  "logger", "lubridate", "purrr"
)
LOG_FILE <- here::here(
  "output",
  "log",
  "06_combine_api_edm_data_2024_onwards.log"
)
EDM_API_CONTRACT <- get_edm_api_contract()

check_required_packages(REQUIRED_PACKAGES)


# Configuration
############################################################

CONFIG <- list(
  input_dir = EDM_API_CONTRACT$processed_directory,
  output_file = EDM_API_CONTRACT$combined_file,
  input_file_pattern = "\\.parquet$",
  exclude_pattern = "combined_api_data\\.parquet$",
  company_name_mapping = EDM_API_CONTRACT$company_id_to_name
)
REQUIRED_INPUT_COLUMNS <- c(
  "water_company",
  "attributes_id",
  "attributes_latest_event_start",
  "attributes_latest_event_end",
  "attributes_latitude",
  "attributes_longitude",
  "attributes_receiving_water_course"
)
EXPECTED_OUTPUT_COLUMNS <- c(
  "water_company",
  "unique_id",
  "latitude",
  "longitude",
  "receiving_water_course",
  "start_time",
  "end_time",
  "year"
)


# Data Processing Functions
############################################################

#' Resolve in-scope per-company Parquet files.
#'
#' @param input_dir Directory containing per-company Parquet files
#' @param file_pattern Regex pattern to identify input Parquet files
#' @param exclude_pattern Regex pattern for files to exclude
#' @param contract EDM API contract list from `get_edm_api_contract()`
#' @return List with valid Parquet files and contract status metadata
resolve_input_parquet_files <- function(
    input_dir,
    file_pattern,
    exclude_pattern,
    contract = EDM_API_CONTRACT) {
  if (!fs::dir_exists(input_dir)) {
    return(list(
      parquet_files = character(),
      status = compare_edm_api_company_ids(character(), contract)
    ))
  }

  parquet_files <- fs::dir_ls(
    input_dir,
    regexp = file_pattern,
    type = "file",
    recurse = FALSE
  )
  parquet_files <- parquet_files[!grepl(exclude_pattern, basename(parquet_files))]

  actual_company_ids <- sub("\\.parquet$", "", basename(parquet_files))
  status <- compare_edm_api_company_ids(actual_company_ids, contract)

  list(
    parquet_files = parquet_files[match(status$valid_company_ids, actual_company_ids)],
    status = status
  )
}

#' Log missing or unexpected processed API Parquet files.
#'
#' @param status Output of `compare_edm_api_company_ids()`
#' @param input_dir Directory inspected for per-company Parquet files
log_parquet_contract <- function(status, input_dir) {
  if (length(status$missing_company_ids) > 0) {
    logger::log_warn(
      "Expected processed API Parquet files missing from {input_dir} for the {EDM_API_CONTRACT$scope_label}: {paste(status$missing_company_ids, collapse = ', ')}"
    )
  }

  if (length(status$unexpected_company_ids) > 0) {
    logger::log_warn(
      "Ignoring out-of-scope or stale processed API Parquet files in {input_dir}: {paste(status$unexpected_company_ids, collapse = ', ')}"
    )
  }
}

#' Read only the required columns from a Parquet file.
#'
#' @param parquet_file Full path to a Parquet file
#' @param required_columns Character vector of required column names
#' @return Tibble containing only the required columns
read_required_parquet_file <- function(
    parquet_file,
    required_columns = REQUIRED_INPUT_COLUMNS) {
  parquet_data <- tryCatch(
    arrow::read_parquet(
      parquet_file,
      col_select = dplyr::all_of(required_columns)
    ),
    error = function(e) {
      stop(
        paste0(
          "Failed to read required columns from ",
          basename(parquet_file),
          ": ",
          conditionMessage(e)
        ),
        call. = FALSE
      )
    }
  )

  missing_columns <- setdiff(required_columns, names(parquet_data))
  if (length(missing_columns) > 0) {
    stop(
      paste0(
        "Required columns missing from ",
        basename(parquet_file),
        ": ",
        paste(missing_columns, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  parquet_data
}

#' Read and combine in-scope Parquet files from a directory.
#'
#' @param input_dir Directory containing the Parquet files
#' @param file_pattern Regex pattern to identify input Parquet files
#' @param exclude_pattern Regex pattern for files to exclude
#' @param contract EDM API contract list from `get_edm_api_contract()`
#' @return A single tibble containing data from all input files, or NULL on error
read_and_combine_parquet_files <- function(
    input_dir,
    file_pattern,
    exclude_pattern,
    contract = EDM_API_CONTRACT) {
  logger::log_info("--- Reading and combining Parquet files from: {input_dir} ---")

  if (!fs::dir_exists(input_dir)) {
    logger::log_error("Input directory does not exist: {input_dir}")
    return(NULL)
  }

  resolved_files <- resolve_input_parquet_files(
    input_dir = input_dir,
    file_pattern = file_pattern,
    exclude_pattern = exclude_pattern,
    contract = contract
  )
  log_parquet_contract(resolved_files$status, input_dir)

  parquet_files <- resolved_files$parquet_files
  if (length(parquet_files) == 0) {
    logger::log_warn(
      "No in-scope Parquet files found matching pattern '{file_pattern}' (excluding '{exclude_pattern}') in {input_dir}."
    )
    return(dplyr::tibble())
  }

  logger::log_info(
    "Found {length(parquet_files)} in-scope Parquet files to combine."
  )

  tryCatch(
    {
      parquet_data_list <- purrr::map(
        parquet_files,
        read_required_parquet_file,
        required_columns = REQUIRED_INPUT_COLUMNS
      )
      combined_data <- dplyr::bind_rows(parquet_data_list)

      logger::log_info(
        "Successfully read and combined {nrow(combined_data)} rows from {length(parquet_files)} files."
      )
      combined_data
    },
    error = function(e) {
      logger::log_error(
        "Schema/read error while combining required Parquet columns: {conditionMessage(e)}"
      )
      NULL
    }
  )
}

#' Parse EDM API event timestamps and log any dropped values.
#'
#' @param values Raw timestamp values from the EDM API parquet files
#' @param column_name Column name for logging
#' @return List containing parsed values and parse/drop counts
parse_api_timestamp_column <- function(values, column_name) {
  character_values <- as.character(values)
  missing_input <- is.na(values) | is.na(character_values) | trimws(character_values) == ""
  missing_input[is.na(missing_input)] <- TRUE

  numeric_values <- suppressWarnings(as.numeric(values))
  parsed_values <- as.POSIXct(
    numeric_values / 1000,
    origin = "1970-01-01",
    tz = "UTC"
  )
  unparseable_values <- !missing_input & is.na(parsed_values)

  missing_input_count <- sum(missing_input)
  unparseable_count <- sum(unparseable_values)

  if (missing_input_count > 0 || unparseable_count > 0) {
    logger::log_warn(
      "Timestamp parse summary for {column_name}: {missing_input_count} missing values, {unparseable_count} unparseable values."
    )
  }

  list(
    values = parsed_values,
    missing_input_count = missing_input_count,
    unparseable_count = unparseable_count
  )
}

#' Check whether a cleaned dataset is safe to publish.
#'
#' @param data_to_write Cleaned tibble intended for the canonical combined output
#' @param output_path Output Parquet path for logging
#' @param expected_columns Expected output schema
#' @return Logical indicating whether the dataset is publishable
validate_publishable_combined_data <- function(
    data_to_write,
    output_path,
    expected_columns = EXPECTED_OUTPUT_COLUMNS) {
  if (is.null(data_to_write)) {
    logger::log_error(
      "Cannot publish combined Parquet file to {output_path}: input data is NULL."
    )
    return(FALSE)
  }

  missing_columns <- setdiff(expected_columns, names(data_to_write))
  if (length(missing_columns) > 0) {
    logger::log_error(
      "Cannot publish combined Parquet file to {output_path}: missing expected output columns: {paste(missing_columns, collapse = ', ')}"
    )
    return(FALSE)
  }

  if (nrow(data_to_write) == 0) {
    logger::log_error(
      "Cannot publish combined Parquet file to {output_path}: combined dataset has 0 rows."
    )
    return(FALSE)
  }

  TRUE
}

#' Clean and standardise the combined EDM API data.
#'
#' @param combined_data Tibble containing the raw combined data
#' @return Cleaned tibble with standardised data
clean_combined_data <- function(combined_data) {
  if (is.null(combined_data) || nrow(combined_data) == 0) {
    logger::log_info("Skipping cleaning step as input data is NULL or empty.")
    return(combined_data)
  }

  logger::log_info("Starting data cleaning with {nrow(combined_data)} rows")

  snake_case_names <- names(CONFIG$company_name_mapping)
  contract_start <- as.POSIXct("2024-01-01", tz = "UTC")
  start_time_parse <- parse_api_timestamp_column(
    combined_data$attributes_latest_event_start,
    "attributes_latest_event_start"
  )
  end_time_parse <- parse_api_timestamp_column(
    combined_data$attributes_latest_event_end,
    "attributes_latest_event_end"
  )

  cleaned_data <- combined_data |>
    dplyr::transmute(
      water_company = dplyr::if_else(
        water_company %in% snake_case_names,
        unname(CONFIG$company_name_mapping[water_company]),
        water_company
      ),
      unique_id = attributes_id,
      latitude = attributes_latitude,
      longitude = attributes_longitude,
      receiving_water_course = attributes_receiving_water_course,
      start_time = start_time_parse$values,
      end_time = end_time_parse$values
    )

  invalid_timestamp_rows <- sum(
    is.na(cleaned_data$start_time) | is.na(cleaned_data$end_time)
  )
  pre_contract_rows <- sum(
    !is.na(cleaned_data$end_time) & cleaned_data$end_time < contract_start
  )

  if (invalid_timestamp_rows > 0 || pre_contract_rows > 0) {
    logger::log_warn(
      "Dropping {invalid_timestamp_rows} rows with missing or unparseable timestamps and {pre_contract_rows} rows ending before {format(contract_start, tz = 'UTC', usetz = TRUE)}."
    )
  }

  cleaned_data <- cleaned_data |>
    dplyr::filter(
      end_time >= contract_start,
      !is.na(start_time),
      !is.na(end_time)
    ) |>
    dplyr::mutate(
      start_time = dplyr::if_else(
        start_time < contract_start,
        contract_start,
        start_time
      ),
      year = as.integer(lubridate::year(start_time))
    ) |>
    dplyr::distinct()

  logger::log_info("Data cleaning finished with {nrow(cleaned_data)} rows")
  cleaned_data
}

#' Write the combined and cleaned data to a single Parquet file.
#'
#' @param data_to_write The final tibble to write
#' @param output_path The full path for the output Parquet file
#' @return Logical indicating success
write_combined_parquet <- function(data_to_write, output_path) {
  if (!validate_publishable_combined_data(data_to_write, output_path)) {
    return(FALSE)
  }

  logger::log_info("--- Writing combined data to Parquet ---")
  logger::log_info("Output path: {output_path}")

  tryCatch(
    {
      output_dir <- dirname(output_path)
      if (!fs::dir_exists(output_dir)) {
        logger::log_info("Creating output directory: {output_dir}")
        fs::dir_create(output_dir, recurse = TRUE)
      }

      arrow::write_parquet(data_to_write, output_path)

      if (!fs::file_exists(output_path)) {
        logger::log_error(
          "File writing seemed complete but file not found or inaccessible at {output_path}."
        )
        return(FALSE)
      }

      rows_written <- nrow(data_to_write)
      file_size_kb <- round(fs::file_info(output_path)$size / 1024, 2)
      logger::log_info(
        "Successfully wrote {rows_written} rows to {output_path} ({file_size_kb} KB)."
      )

      TRUE
    },
    error = function(e) {
      logger::log_error("Failed to write Parquet file to {output_path}: {e$message}")
      FALSE
    }
  )
}


# Main Execution
############################################################

main <- function() {
  start_time <- Sys.time()

  tryCatch(
    {
      setup_logging(LOG_FILE, threshold = "INFO")

      logger::log_info("===== Starting Combined API Data Processing =====")

      combined_data <- read_and_combine_parquet_files(
        CONFIG$input_dir,
        CONFIG$input_file_pattern,
        CONFIG$exclude_pattern
      )

      if (is.null(combined_data)) {
        stop("Failed to read and combine Parquet files. See logs for details.")
      }

      if (nrow(combined_data) == 0) {
        stop("No in-scope Parquet data available to combine. Refusing to publish an empty canonical output.")
      }

      cleaned_data <- clean_combined_data(combined_data)
      if (is.null(cleaned_data)) {
        stop("Data cleaning step failed. See logs for details.")
      }
      if (!validate_publishable_combined_data(cleaned_data, CONFIG$output_file)) {
        stop("Refusing to publish an empty or invalid combined Parquet file.")
      }

      write_success <- write_combined_parquet(cleaned_data, CONFIG$output_file)
      if (!write_success) {
        stop("Failed to write the final combined Parquet file.")
      }

      logger::log_info(
        "===== Combined API Data Processing Finished Successfully ====="
      )
    },
    error = function(e) {
      logger::log_error("Fatal error during main execution: {conditionMessage(e)}")
      stop(conditionMessage(e), call. = FALSE)
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info(
        "===== Script execution finished in {formatted_duration} ====="
      )
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

if (sys.nframe() == 0) {
  main()
}
