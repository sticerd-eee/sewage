############################################################
# Combine Annual Sewage Spill Return Data
# Project: Sewage
# Date: 02/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script cleans and combines the annual sewage spill data
#' from UK water companies for the years 2021-2024. It's part of the
#' data preparation pipeline for analysing sewage discharge events.

# Setup Functions
############################################################

#' Initialize the R environment with required packages
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }
}

#' Load all required packages for data processing
#' @return NULL
load_packages <- function() {
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here",
    "janitor", "logger", "glue", "hms", "lubridate", "arrow", "conflicted"
  )

  install_if_missing <- function(packages) {
    new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
    if (length(new_packages) > 0) {
      message("Installing missing packages: ", paste(new_packages, collapse = ", "))
      install.packages(new_packages)
    }
    invisible(sapply(packages, library, character.only = TRUE))
  }

  install_if_missing(required_packages)
  conflict_prefer("select", "dplyr")
  conflict_prefer("filter", "dplyr")
}

#' Configure logging for the script
#' @return NULL
setup_logging <- function() {
  log_dir <- here::here("output", "log")
  fs::dir_create(log_dir, recurse = TRUE)
  log_path <- file.path(log_dir, "08_combine_annual_return_data.log")

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::INFO)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

#' Configuration settings for paths and data mappings
CONFIG <- list(
  years = 2021:2024,
  input_dir = here::here("data", "raw", "edm_data"),
  output_dir = here::here("data", "processed"),
  column_name_mapping = c(
    # 2021
    "water_company_name" = "water_company",
    "site_name_ea_consents_database" = "site_name_ea",
    "site_name_wa_sc_operational_optional" = "site_name_wa_sc", # Variant 1
    "ea_permit_reference_ea_consents_database" = "permit_reference_ea",
    "wa_sc_supplementary_permit_ref_optional" = "permit_reference_wa_sc",
    "activity_reference_on_permit_if_1_discharge_on_permit" = "activity_reference", # Variant 1
    "storm_discharge_asset_type" = "asset_type",
    "outlet_discharge_ngr_ea_consents_database" = "outlet_discharge_ngr",
    "wfd_waterbody_id_cycle_2_discharge_outlet" = "wfd_waterbody_id_cycle_2",
    "wfd_waterbody_catchment_name_cycle_2_discharge_outlet" = "wfd_waterbody_name_cycle_2",
    "receiving_water_environment_common_name_ea_consents_database" = "receiving_water_name",
    "shellfish_water_only_populate_for_storm_overflow_with_a_shellfish_water_edm_requirement" = "shellfish_water", # Variant 1
    "bathing_water_only_populate_for_storm_overflow_with_a_bathing_water_edm_requirement" = "bathing_water", # Variant 1
    "initial_edm_commission_date" = "edm_commission_date",
    "total_duration_hrs_all_spills_prior_to_processing_through_12_24h_count_method" = "total_duration_all_spills_hrs", # Variant 1 (Numeric hours)
    "counted_spills_using_12_24h_count_method" = "counted_spills",
    "edm_operation_percent_of_reporting_period_edm_operational" = "edm_operation_percent",
    "edm_operation_reporting_percent_primary_reason_90_percent" = "edm_operation_reason",
    "edm_operation_action_taken_planned_status_timeframe" = "edm_operation_action",
    "high_spill_frequency_operational_review_primary_reason" = "high_spill_operational_review_single_year_reason", # Variant 1
    "high_spill_frequency_action_taken_planned_status_timeframe" = "high_spill_action",
    "high_spill_frequency_environmental_enhancement_planning_position_hydraulic_capacity" = "high_spill_enhancement",
    # 2022
    "treatment_method_over_above_storm_tank_settlement_screening" = "treatment_type", # Variant 1
    "long_term_average_spill_count" = "long_term_avg_spill_count",
    "no_full_years_edm_data_years" = "no_full_years_edm_data",
    # 2023
    "activity_reference_on_permit" = "activity_reference", # Variant 2
    "unique_id" = "unique_id",
    # 2024
    "wfd_waterbody_catchment_name_cycle_3_discharge_outlet" = "wfd_waterbody_name_cycle_3",
    "shellfish_water_s_only_populate_for_storm_overflow_with_a_shellfish_water_edm_requirement" = "shellfish_water", # Variant 2
    "bathing_water_s_only_populate_for_storm_overflow_with_a_bathing_water_edm_requirement" = "bathing_water", # Variant 2
    "treatment_method_on_permit_over_above_storm_tank_settlement_screening" = "treatment_type", # Variant 2
    "total_duration_hh_mm_ss_all_spills_prior_to_processing_through_12_24h_count_method" = "total_duration_all_spills_hrs", # Variant 2 (HH:MM:SS)
    "data_start_calendar_year" = "year_data_start",
    "high_spill_frequency_operational_review_single_year_reason" = "high_spill_operational_review_single_year_reason", # Variant 2
    "high_spill_frequency_operational_review_long_term_average_reason" = "high_spill_operational_review_long_term_average_reason",
    "investigation_activity_for_the_reporting_period" = "investigation_activity",
    "improvement_activity_for_the_reporting_period" = "improvement_activity",
    "uwwtr_soaf_investigation_status" = "uwwtr_soaf_investigation_status",
    "old_format_unique_id_pre_2024_annual_return" = "unique_id_2023"
  )
)

# Data Processing Functions
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
    mutate(across(
      where(is.character),
      ~ if_else(. == "-" | grepl("n/a|#n/a|#na", tolower(.)),
        NA, .
      )
    )) %>%
    # Convert data types
    mutate(across(
      where(is.character),
      ~ type.convert(.x, as.is = TRUE)
    )) %>%
    mutate(across(
      starts_with(c("Shellfish", "Bathing")),
      ~ as.character(.x)
    )) %>%
    # Clean column names
    janitor::clean_names() %>%
    rename_with(
      ~ if_else(.x %in% names(CONFIG$column_name_mapping),
        CONFIG$column_name_mapping[.x],
        .x
      )
    ) %>%
    # Remove empty water company rows
    filter(!is.na(water_company)) %>%
    # Convert hours to numeric type
    mutate(across(
      any_of("total_duration_all_spills_hrs"),
      ~ {
        if (inherits(.x, "POSIXct")) {
          as.numeric(as_hms(.x)) / 3600
        } else if (is.numeric(.x)) {
          .x
        } else {
          NA_real_
        }
      }
    )) %>%
    # Convert data start year to integer
    mutate(across(
      any_of("year_data_start"),
      ~ {
        year_match <- str_extract(.x, "\\d{4}")
        if_else(!is.na(year_match), as.integer(year_match), NA_integer_)
      }
    )) %>%
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
  tryCatch(
    {
      logger::log_info("Processing data for year {year}")

      data <- load_data(year) %>%
        lapply(clean_data) %>%
        bind_rows() %>%
        mutate(year = year) %>% 
        dplyr::mutate(!!paste0("site_id_", year) := row_number())

      logger::log_info("Processed {nrow(data)} rows for year {year}")
      data
    },
    error = function(e) {
      logger::log_error("Error processing year {year}: {e$message}")
      stop(glue::glue("Failed to process year {year}: {e$message}"))
    }
  )
}

#' Export processed data to parquet format
#' @param data Combined data frame to export
#' @return NULL
export_data <- function(data) {
  tryCatch(
    {
      # File path
      parquet_file <- file.path(CONFIG$output_dir, "annual_return_edm.parquet")
      dir.create(dirname(parquet_file), recursive = TRUE, showWarnings = FALSE)

      # Save data
      arrow::write_parquet(data, parquet_file)
      logger::log_info("Data exported to {parquet_file}")
    },
    error = function(e) {
      logger::log_error("Error exporting data: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

# Main Execution
############################################################

#' Main function to orchestrate the entire data processing pipeline
#' @return NULL
main <- function() {
  start_time <- Sys.time()

  tryCatch(
    {
      logger::log_info("===== Starting Annual Return EDM Data Processing =====")

      initialise_environment()
      load_packages()
      setup_logging()

      # Process all years and combine
      logger::log_info("Processing data for years {paste(CONFIG$years, collapse=', ')}")
      combined_data <- map(CONFIG$years, process_year) %>%
        list_rbind()

      logger::log_info("Successfully combined data for all years, total rows: {nrow(combined_data)}")

      # Remove empty column
      combined_data <- select(combined_data, -x26)
      logger::log_info("Removed empty column x26")

      # Rename Welsh Water
      combined_data <- combined_data %>%
        mutate(water_company = if_else(
          water_company == "Dwr Cymru Welsh Water",
          "Welsh Water",
          water_company
        ))
      logger::log_info("Standardised water company names")

      # Export data
      export_data(combined_data)

      logger::log_info("===== Annual Return EDM Data Processing Completed Successfully =====")
    },
    error = function(e) {
      logger::log_error("Fatal error in main execution: {e$message}")
      stop(glue::glue("Script execution failed: {e$message}"))
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info("Script execution completed in {formatted_duration}")
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
