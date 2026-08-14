# ==============================================================================
# Prior-to-Rental Radius Exposure Builder
# ==============================================================================
#
# Purpose: Build and publish spill exposure before rental at rental-radius grain
#          through the shared prior-exposure engine.
#
# Author: Jacopo Olivieri
# Date: 2025-12-18
# Date Modified: 2026-08-13
#
# Inputs:
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - data/processed/matched_events_annual_data/matched_events_annual_data.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/cross_section/rentals/prior_to_rental/
#   - output/log/cross_section_prior_to_rental.log
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
  "here",
  "logger",
  "lubridate",
  "tibble",
  "tidyr"
)

LOG_FILE <- here::here("output", "log", "cross_section_prior_to_rental.log")

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "utils", "spill_aggregation_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "site_group_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "prior_exposure_utils.R"),
  local = TRUE
)

CONFIG <- list(
  market = "rental",
  grain = "radius",
  processed_dir = here::here("data", "processed"),
  output_path = here::here(
    "data", "processed", "cross_section", "rentals", "prior_to_rental"
  ),
  radius_thresholds = c(250, 500, 1000),
  base_year = 2021,
  window_start = as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
  chunk_size = 100000,
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  log_file = LOG_FILE
)

initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

load_data <- function() {
  prior_exposure_load_data(CONFIG, CONFIG$market, CONFIG$grain)
}

create_joined_events <- function(rental_ids, data) {
  prior_exposure_join_events(rental_ids, data)
}

get_rental_metadata <- function(rental_dt) {
  rental_dt[, .(rental_id, listing_price, n_days_in_window)]
}

process_chunk <- function(rental_ids, data) {
  prior_exposure_process_chunk(rental_ids, data)
}

create_prior_to_rental_db <- function(data) {
  prior_exposure_stream(data, CONFIG$output_path)
}

main <- function() {
  initialise_logging()
  data <- load_data()
  create_prior_to_rental_db(data)
  logger::log_info("Script completed successfully: {CONFIG$output_path}")
}

if (sys.nframe() == 0) main()
