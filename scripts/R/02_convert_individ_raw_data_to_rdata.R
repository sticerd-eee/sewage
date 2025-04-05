############################################################
# Convert raw sewage spill data to RData format to speed up data cleaning
# Project: Sewage
# Date: 05/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script cleans and combines the individual sewage spill data
#' from UK water companies for the years 2021-2023. It's part of the 
#' data preparation pipeline for analysing sewage discharge events.


# Set Up
############################################################

# Initialise packages with version control
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }
}

# Run manual renv commands if needed
# renv::activate() # Activate the project library
# renv::restore()  # Restore the environment if running for first time
# renv::snapshot()  # After adding/updating packages, snapshot the state


# Load required packages
load_packages <- function() {
  required_packages <- c("rmarkdown", "rio", "tidyverse", "purrr", "here",
                         "janitor", "logger", "glue", "openxlsx2", "fs", 
                         "furrr", "future", "readxlsb")  
  
  install_if_missing <- function(packages) {
    new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
    if (length(new_packages) > 0) {
      message("Installing missing packages: ", paste(new_packages, collapse = ", "))
      install.packages(new_packages)
    }
    invisible(sapply(packages, library, character.only = TRUE))
  }
  
  install_if_missing(required_packages)
}

# Parallel Processing
setup_parallel <- function() {
  
  # Get number of CPU cores
  n_cores <- parallelly::availableCores()
  percentage_cores <- 0.8 # Increase to 1 if needed
  n_workers <- max(1, n_cores * percentage_cores) 
  
  # Memory options
  machine_ram <- memuse::Sys.meminfo()$totalram
  percentage_ram <- 0.7
  ram_limit <- as.numeric(percentage_ram*machine_ram)
  options(future.globals.maxSize = ram_limit)
  
  # Set up parallel processing
  future::plan(future::multisession, workers = n_workers)
  logger::log_info("Parallel processing initialized with {n_workers} workers")
}


# Initialise logging
setup_logging <- function() {
  tryCatch({
    log_path <- here::here("output", "log", 
                           "convert_individ_raw_data_to_rdata.log")
    dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
    
    logger::log_appender(logger::appender_file(log_path))
    logger::log_layout(logger::layout_glue_colors)
    logger::log_info("Script started at {Sys.time()}")
  }, 
  error = function(e) {
    warning(glue::glue("Failed to set up logging: {e$message}"))
  })
}
############################################################


# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  input_dir = here::here("data", "raw", "edm_data"),
  output_dir = here::here("data", "processed"),
  water_company_names = c(
    "Anglian Water",
    "Welsh Water",
    "Northumbrian Water",
    "Severn Trent Water",
    "South West Water",
    "Southern Water",
    "Thames Water",
    "United Utilities",
    "Wessex Water",
    "Yorkshire Water"))
############################################################

# Functions
############################################################

#' Load data for a specific year for all water companies
#' @param year Integer representing the year
#' @return Tibble containing the raw data
#' @throws error if file not found
load_data <- function(year) {
  
  # Validate input
  if (!year %in% CONFIG$years) {
    err_msg <- glue::glue("Invalid year: {year}")
    logger::log_error(err_msg)
    stop(err_msg)
  }
  
  # Convert water company name to filename format
  company_filenames <- make_clean_names(CONFIG$water_company_names)
  
  # Find matching file using regex
  patterns <- glue::glue("{year}_{company_filenames}_edm\\.\\w+$")
  matched_files <- purrr::map(patterns, 
                              ~ fs::dir_ls(CONFIG$input_dir, 
                                           regexp = .x))
  
  # Extract file path
  file_paths <- map2(matched_files, CONFIG$water_company_names, ~ {
    files_found <- .x
    company <- .y
    
    if (length(files_found) == 0) {
      err_msg <- glue("No data file found for {company} in {year}.")
      logger::log_error(err_msg)
      stop(err_msg)
    } else if (length(files_found) > 1) {
      err_msg <- glue("Multiple files found for {company} in {year}: {paste(files_found, collapse = ', ')}.")
      logger::log_error(err_msg)
      stop(err_msg)
    }
    
    # Return the single file path
    files_found[[1]]
  }) %>%
    set_names(CONFIG$water_company_names)
  
  # Import data
  import_data <- function(file_path) {
    
    logger::log_info("Loading file: {file_path}")
    # For united utilities in 2021-2022 (only files in xlsb format)
    if (grepl("\\.xlsb$", file_path)) {
      sheet_names <- c("Jan to April", "May to August", "September to December")
      furrr::future_map(sheet_names, function(sheet_name) {
        readxlsb::read_xlsb(file_path,, 
                            sheet = sheet_name,
                            col_names=TRUE)}, 
        .options = furrr::furrr_options(seed = TRUE)) %>% 
        bind_rows() %>% 
        as_tibble() %>%
        filter(!is.na(.[[1]]))
      # For all other files (xlsx and csv format)
    } else {
      # For south west water in 2021 (data in second sheet)
      if (grepl("2021_south_west_water", file_path)) {
        rio::import(file_path, sheet = "2021 StartStop data", setclass = "tbl") 
      } else {
        # For all other files
        rio::import(file_path, setclass = "tbl")
      }}
  }
  
  # Return a list of data frames for all water companies for a given year
  furrr::future_map(file_paths, import_data, 
                    .options = furrr::furrr_options(seed = TRUE))
}

#' Export data
#' @param data List of data frames by company to export
export_to_rdata <- function(data) {
  tryCatch({
    # File path
    rdata_file <- file.path(CONFIG$output_dir, "individual_edm_by_company.Rdata")
    dir.create(dirname(rdata_file), recursive = TRUE, showWarnings = FALSE)
    
    # Save data 
    edm_data <- data
    save(edm_data, file = rdata_file)
    logger::log_info("Data exported to {rdata_file}")
    
  }, error = function(e) {
    logger::log_error("Error exporting data: {e$message}")
    stop(glue::glue("Failed to export data: {e$message}"))
  })
}

# Main execution
############################################################

# Main function
main <- function() {
  tryCatch({
    initialise_environment()
    setup_parallel()
    load_packages()
    setup_logging()
    
    # Process all years
    edm_data <- furrr::future_map(
      CONFIG$years, 
      load_data,
      .options = furrr::furrr_options(seed = TRUE)) %>% 
      set_names(CONFIG$years)
    
    # Export the data
    export_to_rdata(edm_data)
    
    logger::log_info("Processing completed successfully")
  }, 
  error = function(e) {
    logger::log_error("Fatal error in main execution: {e$message}")
    stop(glue::glue("Script execution failed: {e$message}"))
  })
}

# Execute main function
if (sys.nframe() == 0) { 
  main() 
}