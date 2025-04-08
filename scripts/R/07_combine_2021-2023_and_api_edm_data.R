############################################################
# Combine 2021-2023 and API 2024+ EDM Data
# Project: Sewage
# Date: 2025-04-07
# Author: Jacopo Olivieri
############################################################

#' Reads, harmonizes, and combines the processed 2021-2023 EDM data
#' with the processed API-sourced EDM data (2024 onwards) into a single
#' consolidated Parquet file for analysis.

# Setup Functions
############################################################

#' Initialize the R environment with required packages
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  # Define required packages
  required_packages <- c(
    "here", "fs", "arrow", "dplyr", "purrr",
    "logger", "glue", "lubridate", "janitor"
  )

  # Install and load packages
  install_if_missing <- function(packages) {
    new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
    if (length(new_packages) > 0) {
      message(
        "Installing missing packages: ",
        paste(new_packages, collapse = ", ")
      )
      install.packages(new_packages)
    }
    invisible(sapply(packages, library, character.only = TRUE))
  }

  install_if_missing(required_packages)
}

#' Configure logging for the script
#' @return NULL
setup_logging <- function() {
  log_dir <- here::here("output", "log")
  fs::dir_create(log_dir, recurse = TRUE)
  log_path <- file.path(log_dir, "07_combine_2021-2023_and_api_edm_data.log")

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::INFO)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  data_file_2021_2023 = here::here("data", "processed", "combined_edm_data.parquet"),
  api_data_file = here::here("data", "processed", "edm_api_data", "combined_api_data.parquet"),
  output_file = here::here("data", "processed", "combined_edm_data.parquet"),
  final_columns = c(
    "water_company", "year", "site_name_ea", "unique_id", "start_time", "end_time", "site_name_wa_sc", "latitude", "longitude",
    "receiving_water_course", "data_source"
  )
)

# Data Processing Functions
############################################################

#' Reads individual EDM and adds a data source identifier.
#' @param file_path Path to the input Parquet file.
#' @param source_name Character string identifying the data source (e.g., "2021-2023 data", "2024 onwards").
#' @return A tibble with the loaded data and an added 'data_source' column, or NULL on error.
load_data <- function(file_path, source_name) {
  logger::log_info("Reading {source_name} data from: {file_path}")
  if (!fs::file_exists(file_path)) {
    logger::log_error("{source_name} data file not found: {file_path}")
    return(NULL)
  }

  tryCatch(
    {
      df <- arrow::read_parquet(file_path) %>%
        dplyr::mutate(data_source = source_name)
      logger::log_info("Successfully read {nrow(df)} rows from {source_name} data.")
      return(df)
    },
    error = function(e) {
      logger::log_error("Error reading {source_name} data from {file_path}: {e$message}")
      return(NULL)
    }
  )
}

#' Combines and harmonises dataframes.
#' @param df1 First dataframe (e.g., 2021-2023 data).
#' @param df2 Second dataframe (e.g., 2024 onwards).
#' @return A single combined tibble, or NULL if inputs are invalid.
combine_data <- function(df1, df2) {
  logger::log_info("Combining 2021-2023 data and 2024 onwards data.")
  if (is.null(df1) || is.null(df2)) {
    logger::log_error("One or both input dataframes for combination are NULL.")
    return(NULL)
  }

  tryCatch(
    {
      # Ensure column types are compatible before binding - check major ones
      if (!identical(class(df1$start_time), class(df2$start_time))) {
        logger::log_warn("Attempting to coerce start_time columns to POSIXct for binding.")
        df1$start_time <- as.POSIXct(df1$start_time)
        df2$start_time <- as.POSIXct(df2$start_time)
      }
      if (!identical(class(df1$end_time), class(df2$end_time))) {
        logger::log_warn("Attempting to coerce end_time columns to POSIXct for binding.")
        df1$end_time <- as.POSIXct(df1$end_time)
        df2$end_time <- as.POSIXct(df2$end_time)
      }
      if (class(df1$year) != class(df2$year)) {
        logger::log_warn("Attempting to coerce year columns to integer for binding.")
        df1$year <- as.integer(df1$year)
        df2$year <- as.integer(df2$year)
      }


      combined_df <- dplyr::bind_rows(df1, df2)
      logger::log_info("Successfully combined data. Total rows: {nrow(combined_df)}")
      return(combined_df)
    },
    error = function(e) {
      logger::log_error("Error combining dataframes: {e$message}")
      return(NULL)
    }
  )
}


#' Writes the final combined data to a Parquet file.
#' @param df The combined dataframe.
#' @param output_path Path for the output Parquet file.
#' @return Logical TRUE for success, FALSE for failure.
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
      return(TRUE)
    },
    error = function(e) {
      logger::log_error("Error writing output Parquet file to {output_path}: {e$message}")
      return(FALSE)
    }
  )
}

# Main Execution
############################################################

#' Main function to orchestrate the data combination process.
main <- function() {
  start_time <- Sys.time()

  # Setup
  initialise_environment()
  setup_logging()

  logger::log_info("===== Starting 2021-2023 data and 2024 onwards EDM Data Combination =====")

  tryCatch(
    {
      # Read data
      data_2021_2023 <- load_data(CONFIG$`data_file_2021_2023`, "2021-2023 data")
      data_2024_onwards <- load_data(CONFIG$api_data_file, "2024 onwards")

      # Combine data
      combined_data <- combine_data(data_2021_2023, data_2024_onwards)

      # Write output
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
      # Stop execution implicitly due to error
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info("===== Script execution finished in {formatted_duration} =====")
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}
