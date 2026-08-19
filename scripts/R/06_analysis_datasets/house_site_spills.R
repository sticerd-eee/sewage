# ==============================================================================
# Sales Measurement-Table Builder (house_site_spills)
# ==============================================================================
#
# Purpose: Publish the unmasked measurement layer for the house market: one
#          row per eligible transaction by nearby Site Group within the maximum
#          radius threshold, carrying the window-clipped spill measures and the
#          four atomic evidence flags.
#
#          This is the intermediate the prior-exposure engine already computed
#          and immediately masked. Materializing it lets the four published
#          prior datasets be derived from one measurement pass rather than each
#          recomputing it. No verdict is stored: each derivation ORs its own
#          subset of the flags.
#
# Author: Jacopo Olivieri
# Date: 2026-08-18
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/matched_events_annual_data/matched_events_annual_data.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/cross_section/sales/house_site_spills/
#   - output/log/house_site_spills.log
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

LOG_FILE <- here::here("output", "log", "house_site_spills.log")

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
  market = "sale",
  grain = "measurement",
  processed_dir = here::here("data", "processed"),
  output_path = here::here(
    "data", "processed", "cross_section", "sales", "house_site_spills"
  ),
  radius_thresholds = c(250, 500, 1000),
  base_year = 2021,
  window_start = as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
  chunk_size = 10000,
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
  prior_exposure_load_data(
    CONFIG, CONFIG$market, CONFIG$grain,
    prior_exposure_measurement_contract(CONFIG$market)
  )
}

build_measurement_table <- function(data) {
  prior_exposure_stream(
    data, CONFIG$output_path,
    profile = prior_exposure_measurement_stream_profile()
  )
}

main <- function() {
  initialise_logging()
  data <- load_data()
  build_measurement_table(data)
  logger::log_info("Script completed successfully: {CONFIG$output_path}")
}

if (sys.nframe() == 0) main()
