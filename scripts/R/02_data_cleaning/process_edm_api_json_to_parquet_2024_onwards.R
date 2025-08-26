############################################################
# EDM API Data to Parquet Conversion
# Project: Sewage
# Date: 2025-04-06
# Author: Jacopo Olivieri
############################################################

#' Processes raw water company API JSON data into standardised Parquet files
#' Extracts features data from latest API responses and handles incremental updates

# Setup Functions
############################################################

#' Initialize required packages for data processing
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  # Define required packages
  required_packages <- c(
    "here", "fs", "jsonlite", "dplyr", "purrr", "tidyr",
    "janitor", "arrow", "logger", "glue", "lubridate",
    "tibble"
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

#' Configure logging with appropriate file and format
setup_logging <- function() {
  log_path <- here::here(
    "output", "log", "05_process_edm_api_json_to_parquet_2024_onwards.log"
  )
  fs::dir_create(dirname(log_path), recurse = TRUE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::INFO)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  input_dir = here::here("data", "raw", "edm_data", "raw_api_responses"),
  output_dir = here::here("data", "processed", "edm_api_data"),
  json_file_pattern = "\\d{6}_\\d{4}_.*\\.json\\.gz$"
)

# Data Processing Functions
############################################################

#' Extract features from compressed JSON API response
#' @param json_gz_path Path to the compressed JSON file
#' @return Tibble with extracted features or NULL/empty tibble on error
read_and_extract_features <- function(json_gz_path) {
  if (is.null(json_gz_path) || !fs::file_exists(json_gz_path)) {
    logger::log_error(
      "Input file path is NULL or does not exist: {json_gz_path}"
    )
    return(NULL)
  }

  logger::log_info("Reading and processing: {json_gz_path}")

  tryCatch(
    {
      # Open connection and ensure it closes on exit
      con <- gzfile(json_gz_path, "rt")
      on.exit(close(con), add = TRUE)

      json_content <- readLines(con, warn = FALSE)
      json_string <- paste(json_content, collapse = "\\n")

      if (length(json_string) == 0 || nchar(json_string) == 0) {
        logger::log_warn(
          "File was empty or could not be read properly: {json_gz_path}"
        )
        return(tibble::tibble())
      }

      parsed_data <- jsonlite::fromJSON(
        txt = json_string,
        simplifyVector = TRUE
      )

      if (!"features" %in% names(parsed_data)) {
        logger::log_warn(
          "No 'features' element found in JSON file: {json_gz_path}"
        )
        return(tibble::tibble())
      }

      features_df <- parsed_data$features

      if (is.null(features_df) || nrow(features_df) == 0) {
        logger::log_info("JSON file contained 0 features: {json_gz_path}")
        return(tibble::tibble())
      }

      features_df <- tibble::as_tibble(features_df)

      logger::log_debug("Successfully extracted {nrow(features_df)} features
                        from {basename(json_gz_path)}")
      return(features_df)
    },
    error = function(e) {
      logger::log_error(
        "Error reading or parsing JSON file {json_gz_path}: {e$message}"
      )
      return(NULL)
    }
  )
}

#' Process data for a single water company, extracting and merging features
#' @param company_name Name of the water company
#' @param company_input_dir Directory containing company's raw JSON files
#' @param output_dir Directory for output Parquet files
#' @param file_pattern Regex pattern to identify JSON files
#' @return Logical indicating success or failure
process_company_data <- function(
    company_name, company_input_dir, output_dir, file_pattern) {
  logger::log_info("--- Processing company: {company_name} ---")

  processed_subdir <- fs::path(company_input_dir, "processed")
  fs::dir_create(processed_subdir)

  # Find only unprocessed JSON files in main directory
  json_files_to_process <- fs::dir_ls(company_input_dir,
    regexp = file_pattern,
    type = "file",
    recurse = FALSE
  )

  if (length(json_files_to_process) == 0) {
    logger::log_info("No new '.json.gz' files found to process for {company_name}.")
    return(TRUE)
  }

  logger::log_info("Found {length(json_files_to_process)} new JSON files for
                   {company_name}.")

  all_new_features_list <- list()
  processed_json_paths <- character()

  # Process each JSON file
  for (json_file in json_files_to_process) {
    features_data <- read_and_extract_features(json_file)

    if (!is.null(features_data) && nrow(features_data) > 0) {
      # Clean names and add company identifier
      features_data <- features_data %>%
        janitor::clean_names() %>%
        dplyr::mutate(water_company = company_name, .before = 1)

      all_new_features_list[[length(all_new_features_list) + 1]] <- features_data
      processed_json_paths <- c(processed_json_paths, json_file)
    } else if (is.null(features_data)) {
      logger::log_warn("Skipping file due to read error: {basename(json_file)}")
    } else {
      logger::log_info("Skipping empty file: {basename(json_file)}")
      processed_json_paths <- c(processed_json_paths, json_file)
    }
  }

  # Handle case where no features were extracted
  if (length(all_new_features_list) == 0) {
    logger::log_info(
      "No features extracted from any new JSON files for {company_name}."
    )

    # Move processed files even if empty
    if (length(processed_json_paths) > 0) {
      tryCatch(
        {
          target_paths <- fs::path(processed_subdir, basename(processed_json_paths))
          fs::file_move(processed_json_paths, target_paths)
          logger::log_info("Moved {length(processed_json_paths)} processed
                           (empty/error) JSON files to '{processed_subdir}'.")
        },
        error = function(e_move) {
          logger::log_error(
            "Failed to move processed JSON files for {company_name} after
            empty extraction: {e_move$message}"
          )
        }
      )
    }
    return(TRUE)
  }

  # Combine all extracted features
  all_new_features_data <- dplyr::bind_rows(all_new_features_list)
  logger::log_info("Combined {nrow(all_new_features_data)} new records from
                   {length(processed_json_paths)} files for {company_name}.")

  # Determine output path and handle existing data
  output_parquet_path <- fs::path(
    output_dir, paste0(janitor::make_clean_names(company_name), ".parquet")
  )

  final_data_to_write <- all_new_features_data
  is_append <- FALSE

  if (fs::file_exists(output_parquet_path)) {
    logger::log_info("Existing Parquet file found: {output_parquet_path}.
                     Reading and appending.")
    is_append <- TRUE
    tryCatch(
      {
        existing_data <- arrow::read_parquet(output_parquet_path)
        logger::log_info("Read {nrow(existing_data)} records from existing
                         Parquet file.")

        # Check schema compatibility
        common_cols <- intersect(names(existing_data), names(all_new_features_data))
        existing_subset <- existing_data[, common_cols, drop = FALSE]
        new_subset <- all_new_features_data[, common_cols, drop = FALSE]
        if (!isTRUE(all.equal(sapply(existing_subset, class), sapply(new_subset, class)))) {
          logger::log_warn(
            "Schema differences detected between existing and new data for
            {company_name}. Attempting bind, but review may be needed."
          )
        }

        # Combine and deduplicate
        combined_data <- dplyr::bind_rows(existing_data, all_new_features_data)
        rows_before_dedup <- nrow(combined_data)
        final_data_to_write <- dplyr::distinct(combined_data)
        rows_after_dedup <- nrow(final_data_to_write)

        if (rows_before_dedup > rows_after_dedup) {
          logger::log_info(
            "Combined {rows_before_dedup} rows total. Removed
            {rows_before_dedup - rows_after_dedup} duplicate rows for
            {company_name}."
          )
        } else {
          logger::log_info("Combined {rows_before_dedup} rows total.
                           No duplicates found.")
        }
      },
      error = function(e) {
        logger::log_error("Error reading/combining existing Parquet file
                          {output_parquet_path}: {e$message}")
        logger::log_warn("Overwriting existing Parquet file for {company_name}
                         with only new data due to read/combine error.")
        is_append <- FALSE
      }
    )
  } else {
    logger::log_info("No existing Parquet file found for {company_name}.
                     Writing new file.")

    # Deduplicate initial data
    rows_before_dedup <- nrow(final_data_to_write)
    final_data_to_write <- dplyr::distinct(final_data_to_write)
    rows_after_dedup <- nrow(final_data_to_write)
    if (rows_before_dedup > rows_after_dedup) {
      logger::log_info("Removed {rows_before_dedup - rows_after_dedup}
                       duplicate rows from initial data for {company_name}.")
    }
  }

  # Write output and move processed files
  tryCatch(
    {
      fs::dir_create(dirname(output_parquet_path))
      arrow::write_parquet(final_data_to_write, output_parquet_path)

      rows_written <- nrow(final_data_to_write)
      if (rows_written > 0) {
        file_size_kb <- round(fs::file_info(output_parquet_path)$size / 1024, 2)
        action_word <- if (is_append) "Appended/updated" else "Wrote"
        logger::log_info(
          "{action_word} {rows_written} total records for {company_name} to:
          {output_parquet_path} ({file_size_kb} KB)"
        )
      } else {
        logger::log_info("Wrote empty Parquet file (schema only) for
                         {company_name} to: {output_parquet_path}")
      }

      # Move processed JSON files after successful write
      if (length(processed_json_paths) > 0) {
        target_paths <- fs::path(processed_subdir, basename(processed_json_paths))
        fs::file_move(processed_json_paths, target_paths)
        logger::log_info("Successfully moved {length(processed_json_paths)}
                         processed JSON source files to '{processed_subdir}'.")
      } else {
        logger::log_info("No source JSON files to move for {company_name}
                         (perhaps all had errors or were empty).")
      }

      return(TRUE)
    },
    error = function(e) {
      logger::log_error("Failed to write Parquet file for {company_name} to
                        {output_parquet_path}: {e$message}")
      logger::log_warn(
        "Processed JSON files for {company_name} were NOT moved due to Parquet
        write error. They will be retried on next run."
      )
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

    logger::log_info("===== Starting API Data Processing to Parquet (Append Mode) =====")

    fs::dir_create(CONFIG$output_dir, recurse = TRUE)

    # Find company directories excluding processed subdirectories
    all_entries <- fs::dir_ls(CONFIG$input_dir, type = "directory")
    company_dirs <- all_entries[!endsWith(basename(all_entries), "processed")]

    if (length(company_dirs) == 0) {
      logger::log_warn("No company subdirectories found in: {CONFIG$input_dir}")
      logger::log_info("===== Script finished: No data to process =====")
      return()
    }

    company_names <- basename(company_dirs)
    logger::log_info("Found {length(company_names)} companies to process:
                     {paste(company_names, collapse=', ')}")

    # Process each company
    results <- purrr::map2_lgl(
      company_names,
      company_dirs,
      ~ process_company_data(
        company_name = .x,
        company_input_dir = .y,
        output_dir = CONFIG$output_dir,
        file_pattern = CONFIG$json_file_pattern
      )
    )

    # Summarize results
    success_count <- sum(results)
    failure_count <- length(results) - success_count
    logger::log_info(
      "Processing summary: {success_count} companies processed successfully,
      {failure_count} failed."
    )
  }, error = function(e) {
    logger::log_error("Fatal error during main execution: {e$message}")
    stop(e)
  }, finally = {
    end_time <- Sys.time()
    duration <- end_time - start_time
    formatted_duration <- format(duration)
    logger::log_info(
      "===== API Data Processing Finished in {formatted_duration} ====="
    )
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}
