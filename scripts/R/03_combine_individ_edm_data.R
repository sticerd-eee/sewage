############################################################
# Combine the individual sewage spill data of all water companies
# Project: Sewage
# Date: 09/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script cleans and combines the individual sewage spill data
#' from UK water companies for the years 2021-2023. It's part of the 
#' data preparation pipeline for analysing sewage discharge events.
#' The data input of this script is the RData output of the 
#' convert_raw_to_rdata.R script.

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
    "janitor", "logger", "glue", "openxlsx2", "fs", 
    "furrr", "future", "readxlsb", "lubridate"
  )
  
  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Configure parallel processing settings
#' @return NULL
setup_parallel <- function() {
  n_workers <- parallelly::availableCores() - 1
  machine_ram <- memuse::Sys.meminfo()$totalram
  ram_limit <- as.numeric(0.7 * machine_ram)
  
  options(future.globals.maxSize = ram_limit)
  future::plan(future::multisession, workers = n_workers)
  logger::log_info("Parallel processing initialized with {n_workers} workers")
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "combine_individ_edm_data.log")
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
  years = 2021:2023,
  raw_dir = here::here("data", "raw", "edm_data"),
  processed_dir = here::here("data", "processed"),
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
    "Yorkshire Water"
    ),
  datetime_formats = c(
    "%Y-%m-%dT%H:%M:%SZ",      
    "%Y-%m-%dT%H:%M:%OSZ",  
    "%d-%m-%YT%H:%M:%SZ",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%OS%z"
  ),
  column_name_mapping = c(
    "activity_reference_on_permit" = "activity_reference",
    "wa_sc_supplementary_permit_ref_optional" = "permit_reference_wa_sc",
    "ea_permit_reference_ea_consents_database" = "permit_reference_ea",
    "yw_discharge_urn" = "unique_id",
    "srn" = "unique_id"
    ))

############################################################

# Functions
############################################################

#' Load Rdata containing a list of dataframes for each water company for each 
#' year. The data is returned from the convert_raw_to_rdata.R script.
#' @param file_name Name of the RData file to load 
#' @return List of named dataframes for each water company for each year
load_data <- function(file_name = "individual_edm_by_company.RData") {
  
  file_path <- fs::path(CONFIG$processed_dir, file_name)
  logger::log_info("Loading data: {file_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  
  tryCatch({
    df <- rio::import(file_path, trust= TRUE)
    logger::log_info("Loaded data")
    return(df)
  }, error = function(e) {
    error_msg <- glue::glue("Failed to load data: {e$message}")
    logger::log_error(error_msg)
    stop(error_msg)
  })
  
}


#' Clean variable names, remove NA rows, add year and company name columns
#' @param df Data frame to clean
#' @param df_name Name of the dataframe
#' @return Cleaned data frame
clean_data <- function(df, df_name) {
  tryCatch({
    # Get year and water company name
    year <- as.numeric(stringr::str_extract(df_name, "\\d{4}$"))
    water_company <- stringr::str_trim(
      stringr::str_replace(df_name, "\\d{4}$", ""))
    
    cleaned <- df %>%
      # remove duplicate rows
      unique() %>% 
      # clean variable names
      janitor::clean_names() %>%
      rename_with(
        ~case_when(
          grepl("site", tolower(.)) & grepl("ea", tolower(.)) ~ "site_name_ea",
          grepl("site", tolower(.)) & grepl("wa_sc", tolower(.)) ~ "site_name_wa_sc",
          grepl("site", tolower(.)) & grepl("name", tolower(.)) ~ "site_name_ea",
          grepl("start|begin", tolower(.)) ~ "start_time",
          grepl("end|stop", tolower(.)) ~ "end_time",
          . %in% names(CONFIG$column_name_mapping) ~ CONFIG$column_name_mapping[.],
          TRUE ~ .)) %>%
      # remove rows with NA in event duration monitoring data
      filter(if_any(.cols = c(start_time, end_time), 
                    ~!is.na(.) & !is.null(.))) %>%
      # add year and water company columns
      mutate(
        year = year,
        water_company = water_company,
        .before = everything()
      ) 
    
    # Company specific variable renaming 
    ## Southern Water, South West Water 2022, Thames Water 2023
    if (water_company == "Southern Water" | 
        (water_company == "South West Water" & year == 2022) |
        (water_company == "Thames Water" & year == 2023)) {
      cleaned <- cleaned %>% 
        rename(site_name_wa_sc = site_name_ea)
    }
    
    logger::log_info("Successfully cleaned data for {df_name}")
    return(cleaned)
  }, error = function(e) {
    msg <- glue::glue("Error cleaning data for {df_name}: {e$message}")
    logger::log_error(msg)
    stop(msg)
  })
}


#' Parse datetime columns using multiple methods
#' @param df Data frame containing datetime columns
#' @return Data frame with parsed datetime columns
clean_datetime_columns <- function(df) {
  
  # Setup parsing configuration
  time_cols = c("start_time", "end_time")
  parsed_cols <- paste0("parsed_", time_cols)
  formats = CONFIG$datetime_formats
  tz = "UTC"
  
  # Initialize parsed columns
  df[parsed_cols] <- NA_POSIXct_
  
  # Logging function for NA counts
  log_na_counts <- function(stage = "") {
    na_counts <- sapply(parsed_cols, function(col) sum(is.na(df[[col]])))
    if (stage != "") {
      counts_str <- paste(names(na_counts), na_counts, collapse = ", ")
      if (stage == "FINAL") {
        logger::log_info("Final parsing results: {counts_str}")
      } else {
        logger::log_debug("After {stage}: {counts_str}")
      }
    }
    return(na_counts)
  }
  # Initial state
  logger::log_debug("Starting datetime parsing")
  initial_nas <- log_na_counts()
  
  # Parse dates using multiple methods
  for (i in seq_along(time_cols)) {
    col <- time_cols[i]
    parsed_col <- parsed_cols[i]
    original_values <- df[[col]]
    
    logger::log_debug("Processing column: {col}")
    
    # 1. Handle existing POSIXct values
    if (inherits(original_values, "POSIXct")) {
      df[[parsed_col]] <- original_values
      logger::log_debug("Column {col} already in POSIXct format")
      next
    }
    
    # 2. Try parsing ISO format dates
    still_na <- is.na(df[[parsed_col]])
    if (inherits(original_values, "POSIXct") || 
        inherits(original_values, "character")) {
      if (any(still_na)) {
        for (fmt in formats) {
          if (!any(still_na)) break
          
          temp_times <- suppressWarnings(
            as.POSIXct(original_values[still_na], 
                       format = fmt, 
                       tz = tz)
          )
          
          good_parse <- !is.na(temp_times)
          if (any(good_parse)) {
            df[[parsed_col]][still_na][good_parse] <- temp_times[good_parse]
            still_na[still_na][good_parse] <- FALSE
            logger::log_debug("Successfully parsed {sum(good_parse)} values with format: {fmt}")
          }
        }
      }
    }
    
    # 3. Try Excel date conversion for remaining NAs
    if (any(still_na)) {
      excel_vals <- suppressWarnings(as.numeric(original_values[still_na]))
      valid_excel <- !is.na(excel_vals) & excel_vals > 1 & excel_vals < 73050
      
      if (any(valid_excel)) {
        excel_dates <- as.POSIXct(
          (excel_vals[valid_excel] - 25569) * 86400,
          origin = "1970-01-01",
          tz = tz
        )
        df[[parsed_col]][still_na][valid_excel] <- excel_dates
        logger::log_debug("Successfully parsed {sum(valid_excel)} Excel date values")
      }
    }
    
    # 4. Try lubridate ymd_hms for remaining unparsed values
    still_na <- is.na(df[[parsed_col]])  # Refresh NA check
    if (any(still_na)) {
      temp_times <- suppressWarnings(
        lubridate::ymd_hms(original_values[still_na], tz = tz)
      )
      good_parse <- !is.na(temp_times)
      if (any(good_parse)) {
        df[[parsed_col]][still_na][good_parse] <- temp_times[good_parse]
        logger::log_debug("Successfully parsed {sum(good_parse)} values with ymd_hms")
      }
    }
  }
  
  # Final state and warnings
  final_nas <- log_na_counts("FINAL")
  total_nas <- sum(final_nas)
  if (total_nas > 0) {
    na_details <- paste(names(final_nas), final_nas, collapse = ", ")
    logger::log_warn("Unable to parse {total_nas} datetime values ({na_details})")
  }
  logger::log_info("Datetime parsing completed")
  
  # Clean variable names and prepare for merge
  df %>% 
    mutate(across(c(start_time, end_time), as.character)) %>% 
    rename(
      start_time_og = start_time,
      end_time_og = end_time,
      start_time = parsed_start_time,
      end_time = parsed_end_time
    ) %>% 
    return()
}


clean_time_range <- function(df) {
  tryCatch({
    clean_df <- df %>%
      filter(
        # If start_time is before the year, end_time must be in that year
        !(lubridate::year(start_time) < year & lubridate::year(end_time) != year) &
          # If end_time is after the year, start_time must be in that year
          !(lubridate::year(end_time) > year & lubridate::year(start_time) != year) &
          # end_time is strictly after year start
          end_time > as.POSIXct(paste0(year, "-01-01 00:00:00"), tz = "UTC")) %>% 
      # Rectify spills that should actually start one day later
      mutate(
        end_time = if_else(
          start_time > end_time & 
          (end_time + days(1)) > start_time & 
          format(end_time, "%H:%M:%S") == "00:00:00",
        end_time + days(1),
        end_time
      )) %>%
      # Eliminate spills that have negative duration
      filter(end_time > start_time) 
    
    logger::log_info("Successfully cleaned time ranges")
    return(clean_df)
    
  }, error = function(e) {
    msg <- glue::glue("Error filtering time ranges")
    logger::log_error(msg)
    stop(msg)
  })
}


#' Combine and export data
#' @param df List of dataframes to combine 
#' @return NULL
export_data <- function(df) {
  
  tryCatch({
    # Combine all dataframes
    vars <- c("water_company", "year", "site_name_ea", "site_name_wa_sc",
              "start_time", "end_time", "start_time_og", "end_time_og")
    combined_df <- bind_rows(df) %>%
      select(
        any_of(vars),
        everything()
      )
    
    # Export as RData
    rdata_file <- file.path(CONFIG$processed_dir, "combined_edm_data.RData")
    save(combined_df, file = rdata_file)
    
    # Export as CSV for easy viewing
    csv_file <- file.path(CONFIG$processed_dir, "combined_edm_data.csv")
    rio::export(combined_df, csv_file)
    
    logger::log_info("Data exported successfully to {rdata_file} and {csv_file}")
  }, error = function(e) {
    msg <- glue::glue("Error exporting data: {e$message}")
    logger::log_error(msg)
    stop(msg)
  })
}
  

# Main execution
############################################################

# Main function
main <- function() {
  
  tryCatch({
    # Setup
    initialise_environment()
    setup_logging()
    #setup_parallel()
    
    # Load data
    logger::log_info("Starting data processing")
    raw_data <- load_data() 
    
    # Process data
    processed_data <- raw_data %>%
      imap(~setNames(.x, paste(names(.x), .y))) %>%
      flatten() %>% 
      purrr::map2(.x = ., .y = names(.), clean_data) %>%
      purrr::map(.x = ., .f = clean_datetime_columns) %>% 
      purrr::map(.x = ., .f = clean_time_range)
    
    # Export data
    export_data(processed_data)
    
    logger::log_info("Processing completed successfully")
  }, 
  error = function(e) {
    msg <- glue::glue("Fatal error in main execution: {e$message}")
    logger::log_error(msg)
    stop(msg)
  })
}

# Execute main function
if (sys.nframe() == 0) { 
  main() 
}
