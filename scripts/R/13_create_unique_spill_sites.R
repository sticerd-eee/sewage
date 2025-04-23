############################################################
# Create Unique Spill Sites
# Project: Sewage
# Date: 03/02/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates unique spill sites from the merged EDM data
#' by cleaning location data and extracting distinct sites.

# Set Up Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }
  
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", 
    "fs", "rnrfa", "arrow"
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
  log_path <- here::here("output", "log", "13_create_unique_spill_sites.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}


# Configuration
############################################################

CONFIG <- list(
  spill_data_path = here::here(
    "data", "processed", "matched_events_annual_data", 
    "matched_events_annual_data.parquet"),
  unique_spills_parquet = here::here(
    "data", "processed", "unique_spill_sites.parquet"),
  unique_spills_xlsx = here::here(
    "data", "processed", "unique_spill_sites.xlsx")
)


# Functions
############################################################

#' Load individual sewage spill data
#' @return A data frame containing individual sewage spill records
load_data <- function() {
  file_path <- fs::path(CONFIG$spill_data_path)
  logger::log_info("Loading data: {file_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  
  tryCatch(
    {
      df <- rio::import(file_path, trust = TRUE)
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


#' Standardize grid references (NGR)
#' @param x Character vector of raw NGR strings
#' @return Cleaned NGR strings or NA
clean_ngr <- function(x) {
  x <- dplyr::case_when(
    toupper(x) %in% c("UNABLE TO MATCH TO CONSENTS DATABASE",
                      "NOT IN CONSENTS DATABASE") ~ NA_character_,
    TRUE ~ x
  )
  
  # Extract content after 'and', drop trailing notes
  x <- ifelse(stringr::str_detect(x, "(?i)and"),
              stringr::str_extract(x, "(?i)(?<=and).*"), x)
  x <- stringr::str_replace(x, "(?i)Not.*", "")
  
  # Keep only leading alphanumeric block
  x <- stringr::str_extract(x, "^[^,&/\\(]+")
  x <- stringr::str_remove_all(x, "\\s+")
  
  trimws(x)
}


#' Derive unique spill sites with OSGB coordinates
#' @param data Raw spill dataframe
#' @return Tidy table: one row per site, availability per year, BNG coords
clean_location <- function(data) {
  data %>%
    # clean ngr
    mutate(ngr = clean_ngr(ngr)) %>% 
    # deduplicate on cleaned ref
    distinct(water_company, year, site_id, ngr) %>%
    # pivot availability by year
    mutate(available = TRUE) %>%
    pivot_wider(
      id_cols     = c(water_company, site_id, ngr),
      names_from  = year,
      values_from = available,
      names_prefix= "available_year_",
      values_fill = FALSE
    ) %>%
    # parse British National Grid coords
    mutate(
      coords = map(ngr, ~ tryCatch(
        rnrfa::osg_parse(.x, coord_system = "BNG"),
        error = function(e) list(easting=NA_real_, northing=NA_real_)
      ))
    ) %>%
    mutate(
      easting  = map_dbl(coords, "easting"),
      northing = map_dbl(coords, "northing")
    ) %>%
    select(-coords)
}


#' Export unique spills data to parquet and Excel
#' @param unique_spills_df The unique spills dataframe
#' @param excel_output Path to Excel output file
#' @param parquet_output Path to parquet output file
#' @return NULL
export_data <- function(
    unique_spills_df,
    excel_output = CONFIG$unique_spills_xlsx,
    parquet_output = CONFIG$unique_spills_parquet) {
  tryCatch(
    {
      
      # Export to parquet files
      arrow::write_parquet(unique_spills_df, parquet_output)
      logger::log_info("Exported to parquet: {parquet_output}")
      
      # Export to Excel 
      rio::export(unique_spills_df, excel_output)
      logger::log_info("Exported data to Excel file: {excel_output}")
    },
    error = function(e) {
      logger::log_error("Data export failed: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

# Main execution
############################################################

main <- function() {
  tryCatch(
    {
      # Setup
      initialise_environment()
      setup_logging()
      
      # Load and process data
      logger::log_info("Loading processed merged data")
      spill_data <- load_data()
      
      logger::log_info("Creating unique spill sites")
      unique_sites <- spill_data %>%
        clean_location() %>%
        relocate(site_id, everything())
      
      # Export results
      logger::log_info("Exporting unique spill sites")
      export_data(unique_spills_df = unique_sites)
      logger::log_info("Processing completed successfully")
    },
    error = function(e) {
      logger::log_error("Fatal error: {e$message}")
      stop(e)
    }
  )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}