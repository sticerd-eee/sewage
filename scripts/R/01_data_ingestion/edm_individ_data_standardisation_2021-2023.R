############################################################
# EDM Spill Data File Standardisation Script
# Project: Sewage
# Date: 01/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script standardises the raw Event Duration Monitoring (EDM) data files
#' from UK water companies for the years 2021-2023. It's part of the
#' data preparation pipeline for analysing sewage discharge events.

# Setup Functions
############################################################

#' Initialise required packages for data processing
#' Installs missing packages and loads all required ones
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  # Define required packages
  required_packages <- c("tidyverse", "fs", "stringr", "here", "utils", "logger")

  # Install and load packages
  install_if_missing <- function(packages) {
    new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
    if (length(new_packages) > 0) {
      install.packages(new_packages)
    }
    invisible(sapply(packages, library, character.only = TRUE))
  }

  install_if_missing(required_packages)
}

#' Configure logging with appropriate file and format
setup_logging <- function() {
  log_path <- here::here(
    "output", "log",
    "01_edm_individ_data_standardisation_2021-2023.log"
  )
  if (!dir.exists(dirname(log_path))) {
    dir.create(dirname(log_path), recursive = TRUE)
  }

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
}

# Configuration
############################################################

#' Define global configuration settings
initialise_config <- function() {
  list(
    VALID_EXTENSIONS = c("csv", "xlsx", "xlsb"),
    WATER_COMPANIES = list(
      "anglian" = "anglian_water",
      "northumbrian|nwl" = "northumbrian_water",
      "severn" = "severn_trent_water",
      "south\\s*west" = "south_west_water",
      "southern" = "southern_water",
      "thames" = "thames_water",
      "uu|united" = "united_utilities",
      "welsh" = "welsh_water",
      "wessex" = "wessex_water",
      "yorkshire" = "yorkshire_water",
      "all water and sewerage companies" = "annual_return"
    )
  )
}

# File Processing Functions
############################################################

#' Unzip EDM files and return extracted directories
#' @param zip_path Path to directory containing zip files
#' @return Character vector of extracted directory paths
#' @throws error if no valid files found or extraction fails
unzip_edm_files <- function(zip_path = here()) {
  log_info("Starting file extraction from: {zip_path}")

  if (!dir_exists(zip_path)) {
    log_error("Directory does not exist: {zip_path}")
    stop("Directory does not exist: ", zip_path)
  }

  zip_files <- dir_ls(zip_path, pattern = "EDM.*\\.zip$")
  if (length(zip_files) == 0) {
    log_error("No EDM zip files found in: {zip_path}")
    stop("No EDM zip files found in: ", zip_path)
  }

  temp_dir <- path(zip_path, "temp_edm_extract")
  if (dir_exists(temp_dir)) {
    dir_delete(temp_dir)
  }
  dir_create(temp_dir)

  extracted_dirs <- process_zip_files(zip_files, temp_dir)

  if (length(extracted_dirs) == 0) {
    log_error("No directories were successfully extracted")
    stop("No directories were successfully extracted")
  }

  log_info("Successfully extracted {length(extracted_dirs)} directories")
  return(extracted_dirs)
}


#' Process multiple zip files
#' @param zip_files Vector of zip file paths
#' @param temp_dir Temporary directory for extraction
#' @return Character vector of extracted directory paths
process_zip_files <- function(zip_files, temp_dir) {
  extracted_dirs <- character()

  for (zip_file in zip_files) {
    tryCatch(
      {
        log_info("Processing {path_file(zip_file)}")

        zip_contents <- unzip(zip_file, list = TRUE)
        root_dir <- unique(dirname(zip_contents$Name))[1]

        unzip(zip_file, exdir = temp_dir)

        if (root_dir != ".") {
          extracted_dirs <- c(extracted_dirs, path(temp_dir, root_dir))
        } else {
          extracted_dirs <- c(extracted_dirs, temp_dir)
        }
      },
      error = function(e) {
        log_warn("Error extracting {path_file(zip_file)}: {e$message}")
        warning("Error extracting ", path_file(zip_file), ": ", e$message)
      }
    )
  }

  return(extracted_dirs)
}


#' Clean company names based on predefined mapping
#' @param filename Filename to extract company name from
#' @param company_patterns Named list of patterns to company names
#' @return Standardised company name or NA
clean_company_name <- function(filename, company_patterns) {
  filename_lower <- tolower(filename)

  # Create a named vector for pattern matching
  pattern_matches <- sapply(names(company_patterns), function(pattern) {
    str_detect(filename_lower, fixed(pattern))
  })

  # Find the first matching pattern
  matched_pattern <- names(company_patterns)[which(pattern_matches)[1]]

  # Return the corresponding company name or NA if no match
  if (length(matched_pattern) > 0) {
    return(company_patterns[[matched_pattern]])
  } else {
    return(NA_character_)
  }
}


#' Create a data frame mapping original files to their standardised names
#' @param base_path Base directory path
#' @param config Configuration settings
#' @return Data frame with file mapping information
create_file_mapping <- function(base_path, config) {
  log_info("Creating file mappings from {base_path}")

  pattern <- paste0("\\.(", paste(config$VALID_EXTENSIONS, collapse = "|"), ")$")
  files <- dir_ls(base_path, recurse = TRUE, type = "file", regexp = pattern)

  if (length(files) == 0) {
    log_error("No valid files found in {base_path}")
    stop("No valid files found to process")
  }

  mappings <- create_mappings_dataframe(files, config$WATER_COMPANIES)
  validate_mappings(mappings)

  return(mappings)
}


#' Create mappings dataframe from file list
#' @param files Vector of file paths
#' @param water_companies List of water company name patterns
#' @return Data frame with mapping information
create_mappings_dataframe <- function(files, water_companies) {
  mappings <- tibble(original_path = files) %>%
    mutate(
      year = str_extract(basename(original_path), "(?<!\\d)\\d{4}(?!\\d)"),
      filename_lower = tolower(path_file(original_path)),
      company = case_when(
        str_detect(filename_lower, "anglian") ~ water_companies[["anglian"]],
        str_detect(filename_lower, "northumbrian|nwl") ~ water_companies[["northumbrian|nwl"]],
        str_detect(filename_lower, "severn") ~ water_companies[["severn"]],
        str_detect(filename_lower, "south\\s*west") ~ water_companies[["south\\s*west"]],
        str_detect(filename_lower, "southern") ~ water_companies[["southern"]],
        str_detect(filename_lower, "thames") ~ water_companies[["thames"]],
        str_detect(filename_lower, "uu|united") ~ water_companies[["uu|united"]],
        str_detect(filename_lower, "welsh") ~ water_companies[["welsh"]],
        str_detect(filename_lower, "wessex") ~ water_companies[["wessex"]],
        str_detect(filename_lower, "yorkshire") ~ water_companies[["yorkshire"]],
        str_detect(filename_lower, "all water and sewerage companies") ~
          water_companies[["all water and sewerage companies"]],
        TRUE ~ NA_character_
      ),
      extension = path_ext(original_path)
    ) %>%
    filter(!is.na(year) & !is.na(company)) %>%
    mutate(
      new_name = paste0(year, "_", company, "_edm.", extension)
    ) %>%
    group_by(year, company) %>%
    mutate(
      possible_duplicate = n() > 1,
      file_count = n()
    ) %>%
    ungroup()

  return(mappings)
}


#' Validate and report on file mappings
#' @param mappings Data frame of file mappings
validate_mappings <- function(mappings) {
  log_info("Validating file mappings")

  # Log summary statistics
  log_info("Total files found: {nrow(mappings)}")
  log_info("Files by year: {deparse(table(mappings$year))}")
  log_info("Files by company: {deparse(table(mappings$company))}")

  # Check for duplicates
  if (any(mappings$possible_duplicate)) {
    duplicates <- mappings[mappings$possible_duplicate, ]
    log_warn("Found {nrow(duplicates)} possible duplicate files")
    print(duplicates)
  }
}


#' Process files according to mappings
#' @param mappings Data frame of file mappings
#' @param data_path Base data path
process_files <- function(mappings, data_path) {
  walk2(
    mappings$original_path,
    mappings$new_name,
    ~ file_move(.x, path(data_path, .y))
  )

  cleanup_temp_directory(data_path)
  log_info("Operations completed successfully!")
  log_info("Files standardised: {nrow(mappings)}")
}

#' Clean up temporary directory
#' @param data_path Base data path
cleanup_temp_directory <- function(data_path) {
  dir_delete(path(data_path, "temp_edm_extract"))
}

# Main Execution
############################################################

#' Main function to orchestrate EDM file standardisation process
#' @param data_path Path to data directory
main <- function(data_path = here()) {
  start_time <- Sys.time()

  tryCatch(
    {
      # Initialise environment and settings
      initialise_environment()
      setup_logging()
      config <- initialise_config()

      log_info("Starting EDM file standardisation process")

      extracted_dirs <- unzip_edm_files(data_path)
      mappings <- create_file_mapping(path(data_path, "temp_edm_extract"), config)
      process_files(mappings, data_path)
    },
    error = function(e) {
      log_error("Fatal error: {e$message}")
      cleanup_temp_directory(data_path)
      stop(e$message)
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info(
        "===== EDM File Standardisation Finished in {formatted_duration} ====="
      )
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Execute main function
if (sys.nframe() == 0) {
  data_path <- here("data", "raw", "edm_data")
  main(data_path)
}
############################################################
