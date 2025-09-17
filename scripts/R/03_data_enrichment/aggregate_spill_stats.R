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
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
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
  data_path_annual = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  output_dir = here::here("data", "processed", "agg_spill_stats"),
  years = 2021:2023,
  base_year = 2021
)

# Functions
############################################################

#' Load merged individual sewage spill data
#' @return A list with two elements:
#' \describe{
#'   \item{spill_data}{A data frame with site_id, start_time, and end_time}
#'   \item{metadata}{A data frame with all other variables excluding start_time and end_time}
#' }
load_data <- function() {
  file_path <- CONFIG$merged_data_path
  logger::log_info("Loading data: {file_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  
  tryCatch(
    {
      data <- rio::import(file_path, trust = TRUE)
      list(
        spill_data = data %>% 
          select(site_id, year, water_company, start_time, end_time),
        metadata = data %>% 
          select(-start_time, -end_time, -unique_id) %>% 
          distinct()
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
  dt_yearly  <- prepared_data$yearly
  dt_monthly <- prepared_data$monthly
  dt_monthly[, month_id := (year - CONFIG$base_year) * 12 + month]
  dt_monthly[, qtr_id := (year - CONFIG$base_year) * 4 + quarter]

  # Combinations to ensure zero-row outputs are created where needed
  yearly_combinations <- expand_grid(
    water_company = unique(dt_yearly$water_company),
    year          = CONFIG$years
  )

  monthly_combinations <- expand_grid(
    water_company = unique(dt_monthly$water_company),
    year          = CONFIG$years,
    month         = 1:12
  ) %>%
    mutate(month_id = (year - CONFIG$base_year) * 12 + month)
  
  # ---- Yearly ----------------------------------------------------
  yearly_result <- map2(
    yearly_combinations$water_company,
    yearly_combinations$year,
    ~ {
      current_data <- dt_yearly[
        water_company == .x & year == .y
      ]
      
      if (nrow(current_data) == 0) {
        return(data.table(
          water_company      = .x,
          year               = .y,
          site_id            = NA_character_,
          spill_count_yr     = 0L,
          spill_hrs_yr       = 0L,
          spill_count_yr_ea  = 0L,
          spill_hrs_yr_ea    = 0L
        ))
      }
      
      current_data[,
                   .(
                     spill_count_yr    = count_spills(start_time, end_time),
                     spill_hrs_yr      = calculate_spill_hours(start_time, end_time)
                   ),
                   by = .(water_company, site_id, year)
      ]
    }
  )
  
  # ---- Monthly ---------------------------------------------------
  monthly_result <- pmap(
    list(
      monthly_combinations$water_company,
      monthly_combinations$year,
      monthly_combinations$month,
      monthly_combinations$month_id
    ),
    function(wc, yr, mo, mid) {
      current_data <- dt_monthly[
        water_company == wc & year == yr & month == mo
      ]
      
      if (nrow(current_data) == 0) {
        return(data.table(
          water_company   = wc,
          year            = yr,
          month           = mo,
          month_id        = mid,
          site_id         = NA_character_,
          spill_count_mo  = 0L,
          spill_hrs_mo    = 0L
        ))
      }
      
      current_data[,
                   .(
                     spill_count_mo = count_spills(start_time, end_time),
                     spill_hrs_mo   = calculate_spill_hours(start_time, end_time)
                   ),
                   by = .(water_company, site_id, year, month, month_id)
      ]
    }
  )

  # ---- Quarterly -------------------------------------------------
  quarterly_combinations <- expand_grid(
    water_company = unique(dt_monthly$water_company),
    year          = CONFIG$years,
    quarter       = 1:4
  ) %>%
    mutate(qtr_id = (year - CONFIG$base_year) * 4 + quarter)
  
  quarterly_result <- pmap(
    list(
      quarterly_combinations$water_company,
      quarterly_combinations$year,
      quarterly_combinations$quarter,
      quarterly_combinations$qtr_id
    ),
    function(wc, yr, qt, qid) {
      cur <- dt_monthly[water_company == wc & year == yr & quarter == qt]
      if (nrow(cur) == 0) {
        return(data.table(
          water_company   = wc,
          year            = yr,
          quarter         = qt,
          qtr_id          = qid,
          site_id         = NA_character_,
          spill_count_qt  = 0L,
          spill_hrs_qt    = 0
        ))
      }
      cur[,
          .(
            spill_count_qt = count_spills(start_time, end_time),
            spill_hrs_qt   = calculate_spill_hours(start_time, end_time)
          ),
          by = .(water_company, site_id, year, quarter, qtr_id)
      ]
    }
  )
  
  list(
    yearly   = bind_rows(yearly_result),
    monthly  = bind_rows(monthly_result),
    quarterly = bind_rows(quarterly_result) 
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
  site_metadata_cols <- c(
    "site_name_ea", "site_name_wa_sc", 
    "permit_reference_ea", "permit_reference_wa_sc",
    "activity_reference", "asset_type",
    "ngr",
    "unique_id",
    "wfd_waterbody_id_cycle_2", "receiving_water_name",
    "shellfish_water", "bathing_water", 
    "edm_commission_date"
  )
  
  # Yearly 
  logger::log_info("Completing yearly data observations")
  yearly_sites <- bind_rows(
    distinct(data$yearly, site_id, water_company),
    distinct(metadata, site_id, water_company)
  ) %>% 
    distinct()
  
  yearly_grid <- tidyr::crossing(yearly_sites, year = CONFIG$years)
  
  completed_yearly <- yearly_grid %>%
    left_join(
      data$yearly,
      by = c("site_id", "year", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    group_by(site_id) %>%      
    fill(all_of(intersect(site_metadata_cols, names(.))),
         .direction = "downup") %>%
    ungroup() %>%    
    mutate(
      spill_count_yr = if_else(is.na(spill_count_yr), spill_count_ea, spill_count_yr),
      spill_hrs_yr   = if_else(is.na(spill_hrs_yr),   spill_hrs_ea,   spill_hrs_yr)
    )
  
  # Monthly 
  logger::log_info("Completing monthly data observations")
  monthly_sites <- bind_rows(
    distinct(data$monthly, site_id, water_company),
    distinct(metadata, site_id, water_company)
  ) %>% 
    distinct()

  monthly_grid <- tidyr::crossing(
    monthly_sites, year = CONFIG$years, month = 1:12)
  
  completed_monthly <- monthly_grid %>%
    left_join(
      data$monthly,
      by = c("site_id", "year", "month", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    group_by(site_id) %>%      
    fill(all_of(intersect(site_metadata_cols, names(.))),
         .direction = "downup") %>%
    ungroup() %>%    
    mutate(
      spill_count_mo = if_else(is.na(spill_count_mo), spill_count_ea, spill_count_mo),
      spill_hrs_mo   = if_else(is.na(spill_hrs_mo),   spill_hrs_ea,   spill_hrs_mo),
      month_id       = dplyr::coalesce(month_id, (year - CONFIG$base_year) * 12 + month)
    )
  
  
  # Quarterly 
  logger::log_info("Completing quarterly data observations")
  quarterly_sites <- bind_rows(
    distinct(data$quarterly, site_id, water_company),
    distinct(metadata, site_id, water_company)
  ) %>% 
    distinct()
  
  quarterly_grid <- tidyr::crossing(
    quarterly_sites, year = CONFIG$years, quarter = 1:4)
  
  completed_quarterly <- quarterly_grid %>%
    left_join(
      data$quarterly,
      by = c("site_id", "year", "quarter", "water_company")
    ) %>%
    left_join(
      metadata,
      by = c("site_id", "year", "water_company")
    ) %>%
    group_by(site_id) %>%      
    fill(all_of(intersect(site_metadata_cols, names(.))),
         .direction = "downup") %>%
    ungroup() %>%    
    mutate(
      spill_count_qt = if_else(is.na(spill_count_qt), spill_count_ea, spill_count_qt),
      spill_hrs_qt   = if_else(is.na(spill_hrs_qt),   spill_hrs_ea,   spill_hrs_qt),
      qtr_id         = dplyr::coalesce(qtr_id, (year - CONFIG$base_year) * 4 + quarter)
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
