# ==============================================================================
# Aggregate Spill Statistics
# ==============================================================================
#
# Purpose: Aggregate matched event-level discharges to completed yearly, monthly,
#          and quarterly Works panels using the Environment Agency 12/24 method.
#
# Author: Jacopo Olivieri
# Date: 2024-12-28
# Date Modified: 2026-08-10
#
# Inputs:
#   - data/processed/matched_events_annual_data/matched_events_annual_data.parquet
#   - data/processed/matched_events_annual_data/site_works_crosswalk.parquet
#
# Outputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet
#   - data/processed/agg_spill_stats/agg_spill_mo.parquet
#   - data/processed/agg_spill_stats/agg_spill_qtr.parquet
#   - output/log/aggregate_spill_stats.log
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "data.table",
  "dplyr",
  "fs",
  "glue",
  "here",
  "logger",
  "lubridate",
  "tibble",
  "tidyr"
)

LOG_FILE <- here::here("output", "log", "aggregate_spill_stats.log")

check_required_packages(REQUIRED_PACKAGES)
source(
  here::here("scripts", "R", "utils", "spill_aggregation_utils.R"),
  local = TRUE
)

# Setup Functions
############################################################

#' Attach packages used unqualified by the aggregation functions
#' @return NULL invisibly
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

#' Initialise persistent logging for this script
#' @return NULL invisibly
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
  invisible(NULL)
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
  output_dir = here::here("data", "processed", "agg_spill_stats"),
  years = 2021:2024,
  base_year = 2021
)

INPUT_CONTRACT <- list(
  events = c(
    "site_id", "year", "water_company", "start_time", "end_time"
  ),
  crosswalk = c(
    "site_id", "year", "water_company", "annual_status",
    "spill_hrs_ea", "spill_count_ea"
  )
)

# Functions
############################################################

#' Return the column names stored in a Parquet file
#' @param path Path to a Parquet file
#' @return Character vector of column names
parquet_names <- function(path) {
  arrow::open_dataset(path, format = "parquet")$schema$names
}

#' Validate a Parquet input against its required columns
#' @param path Path to a Parquet file
#' @param required_columns Character vector of required columns
#' @param label Human-readable input label used in errors
#' @return TRUE invisibly
assert_parquet_contract <- function(path, required_columns, label) {
  if (!file.exists(path)) {
    stop(glue::glue("{label} input not found: {path}"), call. = FALSE)
  }

  missing_columns <- setdiff(required_columns, parquet_names(path))
  if (length(missing_columns) > 0) {
    stop(
      glue::glue(
        "{label} input is missing required columns: ",
        "{paste(missing_columns, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

#' Validate both aggregation inputs before loading full data
#' @param config Aggregation configuration containing both input paths
#' @return TRUE invisibly
preflight_inputs <- function(config = CONFIG) {
  assert_parquet_contract(
    config$merged_data_path,
    INPUT_CONTRACT$events,
    "Event"
  )
  assert_parquet_contract(
    config$crosswalk_path,
    INPUT_CONTRACT$crosswalk,
    "Works-year crosswalk"
  )
  invisible(TRUE)
}

#' Assert that a data frame contains one row per declared key
#' @param data Data frame to validate
#' @param keys Character vector naming the key columns
#' @param label Human-readable dataset label used in errors
#' @return TRUE invisibly
assert_unique_keys <- function(data, keys, label) {
  missing_keys <- setdiff(keys, names(data))
  if (length(missing_keys) > 0) {
    stop(
      glue::glue(
        "{label} is missing key columns: ",
        "{paste(missing_keys, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }

  if (anyDuplicated(data[keys]) > 0L) {
    stop(
      glue::glue("{label} must be unique on: {paste(keys, collapse = ', ')}"),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

#' Load event-level discharges and Works-year metadata
#'
#' Reads only the declared input-contract columns. Event rows retain
#' `site_id`, `year`, `water_company`, `start_time`, and `end_time`; metadata
#' retains the Works-year Annual Status and EA outlet totals.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{spill_data}{Event-level discharge rows assigned to Works.}
#'   \item{metadata}{Unique Works-year-company metadata with Annual Status and EA totals.}
#' }
load_data <- function() {
  file_path <- CONFIG$merged_data_path
  crosswalk_path <- CONFIG$crosswalk_path
  logger::log_info("Loading data: {file_path}")
  logger::log_info("Loading works-year crosswalk: {crosswalk_path}")
  
  tryCatch(
    {
      preflight_inputs()

      # Upstream, every monitored outlet is mapped to a sewage works. Outlets with the
      # same company and normalised EA site name are grouped when corroborated by either
      # a shared permit or locations within 250 m. A works-level site_id may therefore
      # cover several outlets. spill_hrs sums outlet durations (outlet-hours), even when
      # timestamps match; spill_count applies the 12/24 method once to the combined
      # works-level event stream.
      data <- arrow::read_parquet(
        file_path,
        col_select = dplyr::all_of(INPUT_CONTRACT$events)
      )
      crosswalk <- arrow::read_parquet(
        crosswalk_path,
        col_select = dplyr::all_of(INPUT_CONTRACT$crosswalk)
      ) %>%
        rename(
          spill_hrs_ea_crosswalk = spill_hrs_ea,
          spill_count_ea_crosswalk = spill_count_ea
        )

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

#' Aggregate event-level discharges to Works-period statistics
#'
#' Spill counts apply the 12/24 method once to each combined Works event stream.
#' Spill hours remain additive outlet-hours, including simultaneous outlets.
#'
#' @param data Event-level discharge data assigned to Works
#' @return List with yearly, monthly, and quarterly Works-period statistics
aggregate_spills <- function(data) {
  prepared_data <- prepare_spill_data(data, CONFIG$base_year)
  dt_yearly    <- prepared_data$yearly
  dt_monthly   <- prepared_data$monthly
  
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

  # ---- Quarterly (month slices grouped into calendar quarters) --
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

#' Complete Works observations across yearly, monthly, and quarterly grids
#' @param data List with components:
#'   \itemize{
#'     \item yearly: Works-year spill counts and outlet-hours.
#'     \item monthly: Works-month spill counts and outlet-hours.
#'     \item quarterly: Works-quarter spill counts and outlet-hours.
#'   }
#' @param metadata Unique Works-year metadata with Annual Status and EA totals
#' @return List with:
#'   \itemize{
#'     \item yearly: completed Works-year observations.
#'     \item monthly: completed Works-month observations with calendar columns and month_id.
#'     \item quarterly: completed Works-quarter observations with calendar columns and qtr_id.
#'   }
complete_data_observations <- function(data, metadata) {
  metadata <- metadata %>%
    select(
      site_id, year, water_company, annual_status,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    )

  assert_unique_keys(
    metadata,
    c("site_id", "year", "water_company"),
    "Works-year metadata"
  )

  reporting_sites <- metadata %>%
    filter(.data$annual_status != "absent") %>%
    distinct(site_id, water_company)

  event_sites <- bind_rows(
    distinct(data$yearly, site_id, water_company),
    distinct(data$monthly, site_id, water_company),
    distinct(data$quarterly, site_id, water_company)
  )

  event_site_years <- data$yearly %>%
    distinct(site_id, year, water_company) %>%
    mutate(has_event_data = TRUE)

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
      spill_count_yr = dplyr::case_when(
        !is.na(.data$spill_count_yr) ~ as.numeric(.data$spill_count_yr),
        .data$annual_status == "reported_zero" ~ 0,
        TRUE ~ NA_real_
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
    left_join(
      event_site_years,
      by = c("site_id", "year", "water_company")
    ) %>%
    mutate(
      can_zero_fill = dplyr::coalesce(
        .data$annual_status == "reported_zero" |
          (.data$annual_status == "reported_positive" & .data$has_event_data),
        FALSE
      ),
      spill_count_mo = dplyr::if_else(
        is.na(.data$spill_count_mo) & .data$can_zero_fill,
        0,
        as.numeric(.data$spill_count_mo)
      ),
      spill_hrs_mo = dplyr::if_else(
        is.na(.data$spill_hrs_mo) & .data$can_zero_fill,
        0,
        as.numeric(.data$spill_hrs_mo)
      )
    ) %>%
    select(
      site_id, water_company, year, month, month_id, annual_status,
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
    left_join(
      event_site_years,
      by = c("site_id", "year", "water_company")
    ) %>%
    mutate(
      can_zero_fill = dplyr::coalesce(
        .data$annual_status == "reported_zero" |
          (.data$annual_status == "reported_positive" & .data$has_event_data),
        FALSE
      ),
      spill_count_qt = dplyr::if_else(
        is.na(.data$spill_count_qt) & .data$can_zero_fill,
        0,
        as.numeric(.data$spill_count_qt)
      ),
      spill_hrs_qt = dplyr::if_else(
        is.na(.data$spill_hrs_qt) & .data$can_zero_fill,
        0,
        as.numeric(.data$spill_hrs_qt)
      )
    ) %>%
    select(
      site_id, water_company, year, quarter, qtr_id, annual_status,
      spill_count_qt, spill_hrs_qt,
      spill_count_ea_crosswalk, spill_hrs_ea_crosswalk
    )

  assert_unique_keys(
    completed_yearly,
    c("site_id", "water_company", "year"),
    "Completed yearly output"
  )
  assert_unique_keys(
    completed_monthly,
    c("site_id", "water_company", "month_id"),
    "Completed monthly output"
  )
  assert_unique_keys(
    completed_quarterly,
    c("site_id", "water_company", "qtr_id"),
    "Completed quarterly output"
  )
  
  list(
    yearly    = completed_yearly,
    monthly   = completed_monthly,
    quarterly = completed_quarterly    
  )
}

#' Export the completed Works-period aggregates
#' @param final_results List containing yearly, monthly, and quarterly components
#' @return NULL (invisibly)
export_results <- function(final_results) {
  output_dir <- CONFIG$output_dir

  assert_unique_keys(
    final_results$yearly,
    c("site_id", "water_company", "year"),
    "Yearly export"
  )
  assert_unique_keys(
    final_results$monthly,
    c("site_id", "water_company", "month_id"),
    "Monthly export"
  )
  assert_unique_keys(
    final_results$quarterly,
    c("site_id", "water_company", "qtr_id"),
    "Quarterly export"
  )
  
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

main <- function() {
  # Setup
  initialise_environment()
  initialise_logging()
  
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
