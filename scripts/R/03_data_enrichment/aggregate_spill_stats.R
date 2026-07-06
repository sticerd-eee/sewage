############################################################
# Aggregate Spill Statistics
# Project: Sewage
# Date: 28/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script individual sewage overflow spills into total spill counts and
#' durations based on data affecting the same catchment area.

# Set Up Functions
############################################################

# Initialise packages from the rv-managed project library
#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management is handled by rv
  
  # Define required packages
  required_packages <- c(
    "rmarkdown", "arrow", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "data.table"
  )
  
  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
  
  # Source shared utilities
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here(
    "output", "log", "12_aggregate_spill_stats.log"
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
  merged_data_path = here::here(
    "data", "processed", "matched_events_annual_data", 
    "matched_events_annual_data.parquet"),
  crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_works_crosswalk.parquet"),
  data_path_annual = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  output_dir = here::here("data", "processed", "agg_spill_stats"),
  years = 2021:2024,
  base_year = 2021
)

# Functions
############################################################

#' Load merged individual sewage spill data
#' @return A list with two elements:
#' \describe{
#'   \item{spill_data}{A data frame with site_id, start_time, and end_time}
#'   \item{metadata}{Works-year crosswalk metadata with EA fallback totals}
#' }
load_data <- function() {
  file_path <- CONFIG$merged_data_path
  crosswalk_path <- CONFIG$crosswalk_path
  logger::log_info("Loading data: {file_path}")
  logger::log_info("Loading works-year crosswalk: {crosswalk_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  if (!file.exists(crosswalk_path)) {
    stop(glue::glue("File not found: {crosswalk_path}"))
  }
  
  tryCatch(
    {
      data <- arrow::read_parquet(file_path) %>%
        select(site_id, year, water_company, start_time, end_time)
      crosswalk <- arrow::read_parquet(crosswalk_path) %>%
        select(
          site_id, year, water_company, annual_status,
          spill_hrs_ea, spill_count_ea
        ) %>%
        rename(
          spill_hrs_ea_crosswalk = spill_hrs_ea,
          spill_count_ea_crosswalk = spill_count_ea
        ) %>%
        distinct()

      list(
        spill_data = data,
        metadata = crosswalk
      ) %>% 
        return()
    },
    error = function(e) {
      error_msg <- glue::glue("Failed to load data: {e$message}")
      logger::log_error(error_msg)
      stop(error_msg)
    }
  )
}

#' Aggregate spill data by water company and year/month
#' @param data Input dataframe containing individual spill data
#' @return List with aggregated yearly and monthly spill statistics
aggregate_spills <- function(data) {
  prepared_data <- prepare_spill_data(data)
  dt_yearly    <- prepared_data$yearly
  dt_monthly   <- prepared_data$monthly
  dt_monthly[, month_id := (year - CONFIG$base_year) * 12 + month]
  dt_monthly[, qtr_id := (year - CONFIG$base_year) * 4 + quarter]
  
  # ---- Yearly ----------------------------------------------------
  yearly_result <- dt_yearly[
    ,
    .(
      spill_count_yr = count_spills(start_time, end_time),
      spill_hrs_yr = calculate_spill_hours(start_time, end_time)
    ),
    by = .(water_company, site_id, year)
  ]
  
  # ---- Monthly ---------------------------------------------------
  monthly_result <- dt_monthly[
    ,
    .(
      spill_count_mo = count_spills(start_time, end_time),
      spill_hrs_mo = calculate_spill_hours(start_time, end_time)
    ),
    by = .(water_company, site_id, year, month, month_id)
  ]

  # ---- Quarterly (D13: uses quarter-split data) ----------------
  quarterly_result <- dt_monthly[
    ,
    .(
      spill_count_qt = count_spills(start_time, end_time),
      spill_hrs_qt = calculate_spill_hours(start_time, end_time)
    ),
    by = .(water_company, site_id, year, quarter, qtr_id)
  ]
  
  list(
    yearly = as_tibble(yearly_result),
    monthly = as_tibble(monthly_result),
    quarterly = as_tibble(quarterly_result)
  )
}

#' Complete and align spill data across time
#' @param data List with components:
#'   \itemize{
#'     \item yearly: annual spill data (columns: water_company, site_id, year, spill_count_yr, spill_hrs_yr).
#'     \item monthly: monthly spill data (columns: water_company, site_id, year, month, spill_count_mo, spill_hrs_mo).
#'   }
#' @param metadata site metadata with annual spill totals
#' @return List with:
#'   \itemize{
#'     \item yearly: completed yearly data.
#'     \item monthly: completed monthly data.
#'   }
complete_data_observations <- function(
    data, metadata = data$metadata) {
  metadata <- metadata %>%
    select(
      site_id, year, water_company, annual_status,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    ) %>%
    distinct()

  reporting_sites <- metadata %>%
    filter(.data$annual_status != "absent") %>%
    distinct(site_id, water_company)

  event_sites <- bind_rows(
    distinct(data$yearly, site_id, water_company),
    distinct(data$monthly, site_id, water_company),
    distinct(data$quarterly, site_id, water_company)
  )

  all_sites <- bind_rows(reporting_sites, event_sites) %>%
    filter(!is.na(.data$site_id), !is.na(.data$water_company)) %>%
    distinct()
  
  # Yearly 
  logger::log_info("Completing yearly data observations")
  yearly_grid <- tidyr::crossing(all_sites, year = CONFIG$years)
  
  completed_yearly <- yearly_grid %>%
    left_join(
      data$yearly,
      by = c("site_id", "year", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    mutate(
      spill_count_yr = dplyr::coalesce(
        as.numeric(.data$spill_count_yr),
        .data$spill_count_ea_crosswalk
      ),
      spill_hrs_yr = dplyr::coalesce(
        as.numeric(.data$spill_hrs_yr),
        .data$spill_hrs_ea_crosswalk
      )
    ) %>%
    select(
      site_id, water_company, year, annual_status,
      spill_count_yr, spill_hrs_yr,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    )
  
  # Monthly 
  logger::log_info("Completing monthly data observations")
  monthly_grid <- tidyr::crossing(
    all_sites, year = CONFIG$years, month = 1:12) %>%
    mutate(month_id = (year - CONFIG$base_year) * 12 + month)
  
  completed_monthly <- monthly_grid %>%
    left_join(
      data$monthly,
      by = c("site_id", "year", "month", "month_id", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    mutate(
      spill_count_mo = dplyr::coalesce(
        as.numeric(.data$spill_count_mo),
        .data$spill_count_ea_crosswalk
      ),
      spill_hrs_mo = dplyr::coalesce(
        as.numeric(.data$spill_hrs_mo),
        .data$spill_hrs_ea_crosswalk
      )
    ) %>%
    select(
      site_id, water_company, month_id, annual_status,
      spill_count_mo, spill_hrs_mo,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    )
  
  
  # Quarterly 
  logger::log_info("Completing quarterly data observations")
  quarterly_grid <- tidyr::crossing(
    all_sites, year = CONFIG$years, quarter = 1:4) %>%
    mutate(qtr_id = (year - CONFIG$base_year) * 4 + quarter)
  
  completed_quarterly <- quarterly_grid %>%
    left_join(
      data$quarterly,
      by = c("site_id", "year", "quarter", "qtr_id", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    mutate(
      spill_count_qt = dplyr::coalesce(
        as.numeric(.data$spill_count_qt),
        .data$spill_count_ea_crosswalk
      ),
      spill_hrs_qt = dplyr::coalesce(
        as.numeric(.data$spill_hrs_qt),
        .data$spill_hrs_ea_crosswalk
      )
    ) %>%
    select(
      site_id, water_company, qtr_id, annual_status,
      spill_count_qt, spill_hrs_qt,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    )
  
  list(
    yearly    = completed_yearly,
    monthly   = completed_monthly,
    quarterly = completed_quarterly    
  )
}

#' Export function to save aggregated results
#' @param final_results List containing components $yearly and $monthly
#' @return NULL (invisibly)
export_results <- function(final_results) {
  output_dir <- CONFIG$output_dir
  
  if (!dir.exists(output_dir)) {
    fs::dir_create(output_dir, recurse = TRUE)
    logger::log_info("Created output directory: {output_dir}")
  }
  
  yr_path <- file.path(output_dir, "agg_spill_yr.parquet")
  mo_path <- file.path(output_dir, "agg_spill_mo.parquet")
  qt_path  <- file.path(output_dir, "agg_spill_qtr.parquet")
  
  # Yearly data
  logger::log_info("Saving yearly data to {yr_path}")
  arrow::write_parquet(final_results$yearly, yr_path)
  
  # Monthly data
  logger::log_info("Saving monthly data to {mo_path}")
  arrow::write_parquet(final_results$monthly, mo_path)
  
  # Quarterly data
  logger::log_info("Saving quarterly data to {qt_path}")
  arrow::write_parquet(final_results$quarterly, qt_path) 
  
  logger::log_info("Export completed successfully")
  invisible(NULL)
}


# Main execution
############################################################

# Note: split_monthly_records() function is imported from shared utilities
# Note: prepare_spill_data() function is imported from shared utilities
# Note: count_spills() function is imported from shared utilities

main <- function() {
  # Setup
  initialise_environment()
  setup_logging()
  
  # Load and process data
  logger::log_info("Starting data processing pipeline")
  data <- load_data()
  
  # Aggregate spills
  logger::log_info("Aggregating spill statistics")
  aggregated_data <- aggregate_spills(data$spill_data)
  
  # Complete time series observations
  logger::log_info("Completing time series observations")
  completed_data <- complete_data_observations(
    aggregated_data,
    metadata = data$metadata)
  
  # Export results
  logger::log_info("Exporting results")
  export_results(completed_data)
  
  logger::log_info("Processing completed successfully")
}

# Execute main function
if (sys.nframe() == 0) {
  main()
}
