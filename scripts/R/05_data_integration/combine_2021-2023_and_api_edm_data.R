############################################################
# Combine 2021-2023 and API 2024+ EDM Data
# Project: Sewage
# Date: 2025-04-07
# Author: Jacopo Olivieri
############################################################

#' Reads, harmonises, and combines the processed 2021-2023 EDM data with the
#' processed API-sourced EDM data (2024 onwards) into a single consolidated
#' Parquet file for analysis.

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

REQUIRED_PACKAGES <- c("arrow", "dplyr", "fs", "here", "logger")
LOG_FILE <- here::here(
  "output",
  "log",
  "07_combine_2021-2023_and_api_edm_data.log"
)
EDM_API_CONTRACT <- get_edm_api_contract()

check_required_packages(REQUIRED_PACKAGES)


# Configuration
############################################################

CONFIG <- list(
  data_file_2021_2023 = here::here("data", "processed", "combined_edm_data.parquet"),
  api_data_file = EDM_API_CONTRACT$combined_file,
  output_file = here::here("data", "processed", "combined_edm_data.parquet"),
  final_columns = c(
    "water_company", "year", "site_name_ea", "unique_id",
    "start_time", "end_time", "site_name_wa_sc", "latitude",
    "longitude", "receiving_water_course", "data_source"
  )
)


# Data Processing Functions
############################################################

#' Read EDM data and add a data source identifier.
#'
#' @param file_path Path to the input Parquet file
#' @param source_name Character string identifying the data source
#' @return Tibble with loaded data and a `data_source` column, or NULL on error
load_data <- function(file_path, source_name) {
  logger::log_info("Reading {source_name} data from: {file_path}")
  if (!fs::file_exists(file_path)) {
    logger::log_error("{source_name} data file not found: {file_path}")
    return(NULL)
  }

  tryCatch(
    {
      df <- arrow::read_parquet(file_path) |>
        dplyr::mutate(data_source = source_name)

      logger::log_info("Successfully read {nrow(df)} rows from {source_name} data.")
      df
    },
    error = function(e) {
      logger::log_error(
        "Error reading {source_name} data from {file_path}: {e$message}"
      )
      NULL
    }
  )
}

#' Combine and harmonise 2021-2023 and 2024+ EDM dataframes.
#'
#' @param df1 First dataframe (e.g., 2021-2023 data)
#' @param df2 Second dataframe (e.g., 2024 onwards)
#' @return A single combined tibble, or NULL if inputs are invalid
combine_data <- function(df1, df2) {
  logger::log_info("Combining 2021-2023 data and 2024 onwards data.")
  if (is.null(df1) || is.null(df2)) {
    logger::log_error("One or both input dataframes for combination are NULL.")
    return(NULL)
  }

  tryCatch(
    {
      if (!identical(class(df1$start_time), class(df2$start_time))) {
        logger::log_warn(
          "Attempting to coerce start_time columns to POSIXct for binding."
        )
        df1$start_time <- as.POSIXct(df1$start_time)
        df2$start_time <- as.POSIXct(df2$start_time)
      }

      if (!identical(class(df1$end_time), class(df2$end_time))) {
        logger::log_warn(
          "Attempting to coerce end_time columns to POSIXct for binding."
        )
        df1$end_time <- as.POSIXct(df1$end_time)
        df2$end_time <- as.POSIXct(df2$end_time)
      }

      if (!identical(class(df1$year), class(df2$year))) {
        logger::log_warn("Attempting to coerce year columns to integer for binding.")
        df1$year <- as.integer(df1$year)
        df2$year <- as.integer(df2$year)
      }

      combined_df <- dplyr::bind_rows(df1, df2)
      logger::log_info("Successfully combined data. Total rows: {nrow(combined_df)}")
      combined_df
    },
    error = function(e) {
      logger::log_error("Error combining dataframes: {e$message}")
      NULL
    }
  )
}

#' Write the final combined data to a Parquet file.
#'
#' @param df The combined dataframe
#' @param output_path Path for the output Parquet file
#' @return Logical TRUE for success, FALSE for failure
export_data <- function(df, output_path) {
  logger::log_info("Writing final combined data to: {output_path}")
  if (is.null(df)) {
    logger::log_error("Cannot write output: Input dataframe is NULL.")
    return(FALSE)
  }

  if (nrow(df) == 0) {
    logger::log_warn("Input dataframe has 0 rows. Writing an empty Parquet file.")
  }

  tryCatch(
    {
      output_dir <- dirname(output_path)
      if (!fs::dir_exists(output_dir)) {
        logger::log_info("Creating output directory: {output_dir}")
        fs::dir_create(output_dir, recurse = TRUE)
      }

      arrow::write_parquet(df, output_path)
      logger::log_info("Successfully wrote {nrow(df)} rows to {output_path}.")
      TRUE
    },
    error = function(e) {
      logger::log_error(
        "Error writing output Parquet file to {output_path}: {e$message}"
      )
      FALSE
    }
  )
}


# Main Execution
############################################################

#' Main function to orchestrate the data combination process.
main <- function() {
  start_time <- Sys.time()

  setup_logging(LOG_FILE, threshold = "INFO")
  logger::log_info(
    "===== Starting 2021-2023 data and 2024 onwards EDM Data Combination ====="
  )

  tryCatch(
    {
      data_2021_2023 <- load_data(CONFIG$data_file_2021_2023, "2021-2023 data")
      data_2024_onwards <- load_data(CONFIG$api_data_file, "2024 onwards")

      combined_data <- combine_data(data_2021_2023, data_2024_onwards)

      if (!is.null(combined_data)) {
        write_success <- export_data(combined_data, CONFIG$output_file)
        if (!write_success) {
          stop("Failed to write the final combined Parquet file.")
        }
      } else {
        stop("Data combination failed. Cannot write output.")
      }

      logger::log_info("===== Data Combination Finished Successfully =====")
    },
    error = function(e) {
      logger::log_fatal("Fatal error during script execution: {e$message}")
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
