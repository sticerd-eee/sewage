############################################################
# Combine EDM API Parquet Data (2024 Onwards)
# Project: Sewage
# Date: 2025-04-06
# Author: Jacopo Olivieri
#' Note: This script cleans and combines the individual sewage spill data from 
#' UK water companies for the years 2021-2023 (generated from API data,
#' 2024 onwards) into a single consolidated Parquet file.

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
    "here", "fs", "arrow", "dplyr", "purrr", "logger", "glue", "janitor"
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

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here(
    "output", "log",
    "06_combine_api_edm_data_2024_onwards.log-2023.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  input_dir = here::here("data", "processed", "edm_api_data"),
  output_file = here::here("data", "processed", "edm_api_data", "combined_api_data.parquet"),
  # Exclude the potential output file from the list of inputs
  input_file_pattern = "\.parquet$",
  exclude_pattern = "combined_api_data\.parquet$"
)

# Data Processing Functions
############################################################

#' Reads and combines multiple Parquet files from a directory.
#' @param input_dir Directory containing the Parquet files.
#' @param file_pattern Regex pattern to identify input Parquet files.
#' @param exclude_pattern Regex pattern for files to exclude (e.g., the output file).
#' @return A single tibble containing data from all input files, or NULL on error.
read_and_combine_parquet_files <- function(input_dir, file_pattern, exclude_pattern) {
  logger::log_info("--- Reading and combining Parquet files from: {input_dir} ---")

  if (!fs::dir_exists(input_dir)) {
    logger::log_error("Input directory does not exist: {input_dir}")
    return(NULL)
  }

  # Find files, excluding the output file itself
  parquet_files <- fs::dir_ls(
    input_dir,
    regexp = file_pattern,
    type = "file",
    recurse = FALSE
  )

  # Further filter based on exclude pattern
  parquet_files <- parquet_files[!grepl(exclude_pattern, basename(parquet_files))]


  if (length(parquet_files) == 0) {
    logger::log_warn("No input Parquet files found matching pattern '{file_pattern}' (excluding '{exclude_pattern}') in {input_dir}.")
    return(dplyr::tibble()) # Return empty tibble if no files found
  }

  logger::log_info("Found {length(parquet_files)} Parquet files to combine.")
  # Log filenames being combined (optional, can be verbose)
  # logger::log_debug("Files: {paste(basename(parquet_files), collapse=', ')}")

  tryCatch(
    {
      # Read and bind rows using purrr::map_dfr for efficiency
      combined_data <- purrr::map_dfr(parquet_files, arrow::read_parquet, .id = "source_file") %>%
        dplyr::mutate(source_file = basename(source_file)) # Keep only filename

      logger::log_info("Successfully read and combined {nrow(combined_data)} rows from {length(parquet_files)} files.")
      return(combined_data)
    },
    error = function(e) {
      logger::log_error("Error reading or combining Parquet files: {e$message}")
      # Log which file might have caused issues if possible (not directly available here)
      return(NULL)
    }
  )
}

#' Placeholder for cleaning the combined data.
#' Add specific cleaning steps here later (e.g., type conversions,
#' datetime parsing, filtering) similar to script 03.
#' @param combined_data The tibble containing combined data.
#' @return The cleaned tibble.
clean_combined_data <- function(combined_data) {
  if (is.null(combined_data) || nrow(combined_data) == 0) {
    logger::log_info("Skipping cleaning step as input data is NULL or empty.")
    return(combined_data)
  }

  logger::log_info("--- Starting data cleaning (currently a placeholder) ---")

  # <<<< Add cleaning steps here >>>>
  # Example: Standardize column names
  # cleaned_data <- combined_data %>% janitor::clean_names()

  # Example: Parse datetimes (assuming columns like 'start_time', 'end_time')
  # cleaned_data <- cleaned_data %>%
  #   mutate(across(matches("time|date"), ~ lubridate::parse_date_time(.x, orders = c("Ymd HMS", "Y-m-d H:M:S"), tz = "UTC")))

  # Example: Deduplication (already done in script 05 per file, but maybe needed across files)
  # rows_before <- nrow(cleaned_data)
  # cleaned_data <- dplyr::distinct(cleaned_data)
  # rows_after <- nrow(cleaned_data)
  # if (rows_before > rows_after) {
  #   logger::log_info("Removed {rows_before - rows_after} duplicate rows during combined cleaning.")
  # }

  # For now, just return the input data
  cleaned_data <- combined_data

  logger::log_info("--- Data cleaning step finished ---")
  return(cleaned_data)
}

#' Writes the combined and cleaned data to a single Parquet file.
#' @param data_to_write The final tibble to write.
#' @param output_path The full path for the output Parquet file.
#' @return Logical indicating success (TRUE) or failure (FALSE).
write_combined_parquet <- function(data_to_write, output_path) {
  if (is.null(data_to_write)) {
      logger::log_error("Cannot write Parquet file: Input data is NULL.")
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

      rows_written <- nrow(data_to_write)
      if (fs::file_exists(output_path) && rows_written > 0) {
        file_size_kb <- round(fs::file_info(output_path)$size / 1024, 2)
        logger::log_info(
          "Successfully wrote {rows_written} rows to {output_path} ({file_size_kb} KB)."
        )
        return(TRUE)
      } else if (rows_written == 0) {
         logger::log_info("Wrote an empty Parquet file (0 rows) to {output_path}.")
         return(TRUE)
      }
       else {
        logger::log_error("File writing seemed complete but file not found or inaccessible at {output_path}.")
        return(FALSE)
      }
    },
    error = function(e) {
      logger::log_error("Failed to write Parquet file to {output_path}: {e$message}")
      return(FALSE)
    }
  )
}

# Main Execution
############################################################

#' Main pipeline orchestration function
main <- function() {
  start_time <- Sys.time()

  tryCatch({
    # Initialize environment and logging
    initialise_environment()
    setup_logging()

    logger::log_info("===== Starting Combined API Data Processing =====")

    # 1. Read and Combine
    combined_data <- read_and_combine_parquet_files(
      CONFIG$input_dir,
      CONFIG$input_file_pattern,
      CONFIG$exclude_pattern
      )

    # Check if combination was successful
    if (is.null(combined_data)) {
       stop("Failed to read and combine Parquet files. See logs for details.")
    }
     if (nrow(combined_data) == 0) {
        logger::log_warn("No data combined from input files. Writing an empty output file.")
     }


    # 2. Clean Data (Placeholder Step)
    cleaned_data <- clean_combined_data(combined_data)
     if (is.null(cleaned_data)) {
       stop("Data cleaning step failed. See logs for details.")
    }


    # 3. Write Combined File
    write_success <- write_combined_parquet(cleaned_data, CONFIG$output_file)

    if (!write_success) {
      stop("Failed to write the final combined Parquet file.")
    }

    logger::log_info("===== Combined API Data Processing Finished Successfully =====")

  }, error = function(e) {
    logger::log_error("Fatal error during main execution: {e$message}")
    # Optionally re-throw the error to stop execution completely if desired
    # stop(e)
  }, finally = {
    end_time <- Sys.time()
    duration <- end_time - start_time
    formatted_duration <- format(duration)
    logger::log_info(
      "===== Script execution finished in {formatted_duration} ====="
    )
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}
