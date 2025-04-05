############################################################
# Combine the annual sewage spill data of all water companies
# Project: Sewage
# Date: 02/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script cleans and combines the annual sewage spill data
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
                         "janitor", "logger", "glue")
  
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


# Initialise logging
setup_logging <- function() {
  tryCatch({
    log_path <- here::here("output", "log", "combine_annual_return_data.log")
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
  column_name_mapping = c(
  "activity_reference_on_permit_if_1_discharge_on_permit" = "activity_reference",
  "activity_reference_on_permit" = "activity_reference",
  "bathing_water_only_populate_for_storm_overflow_with_a_bathing_water_edm_requirement" = "bathing_water",
  "counted_spills_using_12_24h_count_method" = "counted_spills",
  "counted_spills_using_12_24hr_counting_method" = "counted_spills",
  "ea_permit_reference_ea_consents_database" = "permit_reference_ea",
  "edm_operation_action_taken_planned_status_timeframe" = "edm_operation_action",
  "edm_operation_percent_of_reporting_period_edm_operational" = "edm_operation_percent",
  "edm_operation_reporting_percent_primary_reason_90_percent" = "edm_operation_reason",
  "edm_operation_reporting_percentage_primary_reason_90_percent" = "edm_operation_reason",
  "high_spill_frequency_action_taken_planned_status_timeframe" = "high_spill_action",
  "high_spill_frequency_environmental_enhancement_planning_position_hydraulic_capacity" = "high_spill_enhancement",
  "high_spill_frequency_operational_review_primary_reason" = "high_spill_operational_review",
  "high_spill_frequeny_action_taken_planned_status_timeframe" = "high_spill_action",
  "initial_edm_commission_date" = "edm_commission_date",
  "no_full_years_edm_data_years" = "no_full_years_edm_data",
  "outlet_discharge_ngr_ea_consents_database" = "outlet_discharge_ngr",
  "receiving_water_environment_common_name_ea_consents_database" = "receiving_water_name",
  "shellfish_water_only_populate_for_storm_overflow_with_a_shellfish_water_edm_requirement" = "shellfish_water",
  "site_name_ea_consents_database" = "site_name_ea",
  "site_name_wa_sc_operational_name_optional" = "site_name_wa_sc",
  "site_name_wa_sc_operational_optional" = "site_name_wa_sc",
  "storm_discharge_asset_type" = "asset_type",
  "total_duration_hours_of_all_spills_prior_to_processing_through_12_24_hour_counting_method" = "total_duration_all_spills_hrs",
  "total_duration_hrs_all_spills_prior_to_processing_through_12_24h_count_method" = "total_duration_all_spills_hrs",
  "treatment_method_over_above_storm_tank_settlement_screening" = "treatment_type",
  "wa_sc_supplementary_permit_ref_optional" = "permit_reference_wa_sc",
  "water_company_name" = "water_company",
  "wfd_waterbody_catchment_name_cycle_2_discharge_outlet" = "wfd_waterbody_name",
  "wfd_waterbody_id_cycle_2_discharge_outlet" = "wfd_waterbody_id"))

############################################################


# Define Functions
############################################################

#' Load data for a specific year
#' @param year Integer representing the year
#' @return Tibble containing the raw data
#' @throws error if file not found
load_data <- function(year) {
  logger::log_info("Loading data for year {year}")
  
  # File path
  file_path <- file.path(
    CONFIG$input_dir,
    glue::glue("{year}_annual_return_edm.xlsx")
  )
  
  if (!file.exists(file_path)) {
    err_msg <- glue::glue("File not found: {file_path}")
    logger::log_error(err_msg)
    stop(err_msg)
  }
  
  # Import data
  logger::log_info("Loading data for year {year}")
  rio::import_list(file_path, skip = 1, setclass = "tbl")
}


#' Clean and standardise data
#' @param df Data frame to clean
#' @return Cleaned data frame
clean_data <- function(df) {
  df %>%
    # Replace various NA representations with actual NA
    mutate(across(where(is.character),
                  ~ if_else(. == "-" | grepl("n/a|#n/a|#na", tolower(.)), 
                           NA, .))) %>%
    # Convert data types
    mutate(across(where(is.character),
                  ~ type.convert(.x, as.is = TRUE))) %>% 
    mutate(across(starts_with(c("Shellfish", "Bathing")),
                  ~ as.character(.x))) %>% 
    # Clean column names
    janitor::clean_names() %>% 
    rename_with(
      ~ if_else(.x %in% names(CONFIG$column_name_mapping),
                CONFIG$column_name_mapping[.x],
                .x)
    ) %>% 
    # Remove empty water company rows
    filter(!is.na(water_company)) %>% 
    # Clean location data
    mutate(
      outlet_discharge_ngr_og = outlet_discharge_ngr,
      outlet_discharge_ngr = case_when(
        outlet_discharge_ngr == "Unable to match to Consents Database" ~ NA_character_,
        outlet_discharge_ngr == "Not in Consents Database" ~ NA_character_,
        TRUE ~ outlet_discharge_ngr
      ),
      outlet_discharge_ngr = str_replace(outlet_discharge_ngr, "(?i)Not.*", ""),
      outlet_discharge_ngr = str_extract(outlet_discharge_ngr, "^[^,&/\\(]+"),
      outlet_discharge_ngr = str_remove_all(outlet_discharge_ngr, "\\s+")
    ) %>% 
    return()
}


#' Process data for a specific year
#' @param year Integer representing the year
#' @return Processed data frame for the year
process_year <- function(year) {
  tryCatch({
    logger::log_info("Processing data for year {year}")
    
    data <- load_data(year) %>%
      lapply(clean_data) %>%
      bind_rows() %>%
      mutate(year = year)
    
    logger::log_info("Processed {nrow(data)} rows for year {year}")
    data
  }, 
  error = function(e) {
    logger::log_error("Error processing year {year}: {e$message}")
    stop(glue::glue("Failed to process year {year}: {e$message}"))
  })
}


#' Export processed data
#' @param data Combined data frame to export
export_data <- function(data) {
  tryCatch({
    # File path
    rds_file <- file.path(CONFIG$output_dir, "annual_return_edm.rds")
    csv_file <- file.path(CONFIG$output_dir, "annual_return_edm.csv")
    dir.create(dirname(rds_file), recursive = TRUE, showWarnings = FALSE)
    
    # Save data
    saveRDS(data, rds_file)
    logger::log_info("Data exported to {rds_file}")
    write_csv(data, csv_file)
    logger::log_info("CSV backup saved to {csv_file}")
  }, error = function(e) {
    logger::log_error("Error exporting data: {e$message}")
    stop(glue::glue("Failed to export data: {e$message}"))
  })
}
############################################################


# Main execution
############################################################

# Main function
main <- function() {
  tryCatch({
    initialise_environment()
    load_packages()
    setup_logging()
    
    # Process all years and combine
    combined_data <- map_df(CONFIG$years, process_year)
    
    # Remove empty column
    combined_data <- select(combined_data, -x26)
    
    # Rename Welsh Water
    combined_data <- combined_data %>%
      mutate(water_company = if_else(
        water_company=="Dwr Cymru Welsh Water", 
        "Welsh Water", 
        water_company)) 
    
    # Export data
    export_data(combined_data)
    
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

