############################################################
# Clean the EA's consented discharges database
# Project: Sewage
# Date: 10/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script cleans the Consented Discharges to Controlled
#' Waters with Conditions database, published by the Environment Agency
#' here: https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109/consented-discharges-to-controlled-waters-with-conditions
#' It's part of the data preparation pipeline for analysing sewage discharge
#' events.

# Set Up
############################################################

# Initialise packages with version control with renv
#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here",
    "janitor", "logger", "glue", "fs"
  )

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here(
    "output", "log",
    "clean_consented_discharges_database.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}
############################################################


# Configuration
############################################################

CONFIG <- list(
  raw_dir = here::here("data", "raw", "ea_consents"),
  processed_dir = here::here("data", "processed"),
  water_company_mapping = list(
    "ANGLIAN WATER SERVICES LIMITED" = "Anglian Water",
    "DWR CYMRU CYFYNGEDIG" = "Welsh Water",
    "NORTHUMBRIAN WATER LIMITED (ESSEX & SUFFOLK WATER)" = "Northumbrian Water",
    "SEVERN TRENT WATER LIMITED" = "Severn Trent",
    "SOUTH WEST WATER LIMITED" = "South West Water",
    "SOUTHERN WATER SERVICES LIMITED" = "Southern Water",
    "THAMES WATER UTILITIES LTD" = "Thames Water",
    "UNITED UTILITIES WATER LIMITED" = "United Utilities",
    "WESSEX WATER SERVICES LIMITED" = "Wessex Water",
    "YORKSHIRE WATER SERVICES LIMITED" = "Yorkshire Water"
  ),
  column_name_mapping = c(
    "company_name" = "water_company",
    "discharge_site_name" = "site_name_ea",
    "discharge_site_type_code" = "activity_reference",
    "permit_number" = "permit_reference_ea",
    "outlet_grid_ref" = "outlet_discharge_ngr"
  )
)
############################################################

# Functions
############################################################

#' Load EA consented discharges database
#' @param file_name Name of the csv file to load
#' @return List of named dataframes for each water company for each year
load_data <- function(file_name = "consents_all.csv") {
  file_path <- fs::path(CONFIG$raw_dir, file_name)
  logger::log_info("Loading data: {file_path}")

  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }

  tryCatch(
    {
      df <- rio::import(file_path)
      logger::log_info("Loaded data")
      return(df)
    },
    error = function(e) {
      error_msg <- glue::glue("Failed to load data: {e$message}")
      logger::log_error(error_msg)
      stop(error_msg)
    }
  )
}


#' Clean data
#' @param df Data frame to clean
#' @return Cleaned data frame
clean_data <- function(df) {
  tryCatch(
    {
      cleaned <- df %>%
        # clean variable names
        janitor::clean_names() %>%
        rename_with(
          ~ if_else(.x %in% names(CONFIG$column_name_mapping),
            CONFIG$column_name_mapping[.x],
            .x
          )
        ) %>%
        # clean water company names
        mutate(
          water_company = recode(
            water_company,
            !!!CONFIG$water_company_mapping
          )
        ) %>%
        filter(water_company %in% unlist(CONFIG$water_company_mapping))

      logger::log_info("Cleaned {nrow(cleaned)} records")
      return(cleaned)
    },
    error = function(e) {
      logger::log_error("Data cleaning failed: {e$message}")
      stop(e)
    }
  )
}


#' Export processed data
#' @param df Processed data frame
#' @return NULL
export_data <- function(df) {
  tryCatch(
    {
      # Export as RData
      rdata_file <- file.path(CONFIG$processed_dir, "consent_discharges_db.RData")
      save(df, file = rdata_file)

      # Export as CSV for easy viewing
      csv_file <- file.path(CONFIG$processed_dir, "consent_discharges_db.csv")
      rio::export(df, csv_file)

      logger::log_info("Data exported successfully to {rdata_file} and {csv_file}")
    },
    error = function(e) {
      msg <- glue::glue("Error exporting data: {e$message}")
      logger::log_error(msg)
      stop(msg)
    }
  )
}


# Main execution
############################################################

# Main function
main <- function() {
  tryCatch(
    {
      # Setup
      initialise_environment()
      setup_logging()

      # Load data
      start_time <- Sys.time()
      logger::log_info("Starting data processing")
      raw_data <- load_data()

      # Clean data
      processed_data <- clean_data(raw_data)

      # Export data
      export_data(processed_data)
      execution_time <- difftime(Sys.time(), start_time, units = "mins")
      logger::log_info("Processing completed in {round(execution_time, 2)} minutes")
    },
    error = function(e) {
      logger::log_error("Fatal error: {e$message}")
      stop(e)
    }, finally = {
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Execute main function
if (sys.nframe() == 0) {
  main()
}
