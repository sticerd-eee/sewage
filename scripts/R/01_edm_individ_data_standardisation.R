############################################################
# EDM Spill Data File Standardization Script
# Project: Sewage
# Date: 01/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script standardises the raw Event Duration Monitoring (EDM) data files 
#' from UK water companies for the years 2021-2023. It's part of the 
#' data preparation pipeline for analysing sewage discharge events.


# Set Up
############################################################

# Initialize packages with version control
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}


# Load required packages
required_packages <- c("tidyverse", "fs", "stringr", "here", "utils", "logger")
install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)


# Initialize logging
log_path <- here::here("output", "log", "edm_individ_data_standardisation.log")
if (!dir.exists(dirname(log_path))) {
  dir.create(dirname(log_path), recursive = TRUE)
}
logger::log_appender(logger::appender_file(log_path))
logger::log_layout(logger::layout_glue_colors)
############################################################



# Configuration
############################################################

VALID_EXTENSIONS <- c("csv", "xlsx", "xlsb")
############################################################



# Functions
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
    tryCatch({
      log_info("Processing {path_file(zip_file)}")
      
      zip_contents <- unzip(zip_file, list = TRUE)
      root_dir <- unique(dirname(zip_contents$Name))[1]
      
      unzip(zip_file, exdir = temp_dir)
      
      if (root_dir != ".") {
        extracted_dirs <- c(extracted_dirs, path(temp_dir, root_dir))
      } else {
        extracted_dirs <- c(extracted_dirs, temp_dir)
      }
      
    }, error = function(e) {
      log_warn("Error extracting {path_file(zip_file)}: {e$message}")
      warning("Error extracting ", path_file(zip_file), ": ", e$message)
    })
  }
  
  return(extracted_dirs)
}


#' Clean company names based on predefined mapping
#' @param filename Filename to extract company name from
#' @return Standardized company name or NA
clean_company_name <- function(filename) {
  filename_lower <- tolower(filename)
  
  # Create a named vector for pattern matching
  pattern_matches <- sapply(names(WATER_COMPANIES), function(pattern) {
    str_detect(filename_lower, fixed(pattern))
  })
  
  # Find the first matching pattern
  matched_pattern <- names(WATER_COMPANIES)[which(pattern_matches)[1]]
  
  # Return the corresponding company name or NA if no match
  if (length(matched_pattern) > 0) {
    return(WATER_COMPANIES[[matched_pattern]])
  } else {
    return(NA_character_)
  }
}


#' Create a data frame mapping original files to their standardized names
#' @param base_path Base directory path
#' @return Data frame with file mapping information
create_file_mapping <- function(base_path) {
  log_info("Creating file mappings from {base_path}")
  
  pattern <- paste0("\\.(", paste(VALID_EXTENSIONS, collapse = "|"), ")$")
  files <- dir_ls(base_path, recurse = TRUE, type = "file", regexp = pattern)
  
  if (length(files) == 0) {
    log_error("No valid files found in {base_path}")
    stop("No valid files found to process")
  }
  
  mappings <- create_mappings_dataframe(files)
  validate_mappings(mappings)
  
  return(mappings)
}


#' Create mappings dataframe from file list
#' @param files Vector of file paths
#' @return Data frame with mapping information
create_mappings_dataframe <- function(files) {
  mappings <- tibble(original_path = files) %>%
    mutate(
      year = str_extract(basename(original_path), "(?<!\\d)\\d{4}(?!\\d)"),
      filename_lower = tolower(path_file(original_path)),
      company = case_when(
        str_detect(filename_lower, "anglian") ~ "anglian_water",
        str_detect(filename_lower, "northumbrian|nwl") ~ "northumbrian_water",
        str_detect(filename_lower, "severn") ~ "severn_trent_water",
        str_detect(filename_lower, "south\\s*west") ~ "south_west_water",
        str_detect(filename_lower, "southern") ~ "southern_water",
        str_detect(filename_lower, "thames") ~ "thames_water",
        str_detect(filename_lower, "uu|united") ~ "united_utilities",
        str_detect(filename_lower, "welsh") ~ "welsh_water",
        str_detect(filename_lower, "wessex") ~ "wessex_water",
        str_detect(filename_lower, "yorkshire") ~ "yorkshire_water",
        str_detect(filename_lower, "all water and sewerage companies") ~ 
          "annual_return",
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
    ~file_move(.x, path(data_path, .y))
  )
  cleanup_temp_directory(data_path)
  log_info("Operations completed successfully!")
  log_info("Files standardized: {nrow(mappings)}")
}

#' Clean up temporary directory
#' @param data_path Base data path
cleanup_temp_directory <- function(data_path) {
  dir_delete(path(data_path, "temp_edm_extract"))
}



# Main execution
############################################################

#' Main function to process EDM files
#' @param data_path Path to data directory
main <- function(data_path = here()) {
  log_info("Starting EDM file standardization process")
  
  tryCatch({
    extracted_dirs <- unzip_edm_files(data_path)
    mappings <- create_file_mapping(path(data_path, "temp_edm_extract"))
    process_files(mappings, data_path)
  }, error = function(e) {
    log_error("Fatal error: {e$message}")
    cleanup_temp_directory(data_path)
    stop(e$message)
  })
}


# Run the main function
data_path <- here("data", "raw", "edm_data")
main(data_path)
############################################################