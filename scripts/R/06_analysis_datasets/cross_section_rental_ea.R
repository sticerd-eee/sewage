# ==============================================================================
# Study-Period Rental Cross-Section Builder (Annual Returns)
# ==============================================================================
#
# Purpose: Build the rental cross-section at the rental listing level. For each
#          rental listing and radius, sum spill counts and hours across all sites
#          within that radius over the 2021–2023 rental study period.
#
#          Exposure comes from the EA Annual Returns, the annual spill figures
#          published by the Environment Agency and carried on the Site Group
#          crosswalk as spill_count_ea and spill_hrs_ea. "EA" and "Annual
#          Returns" name the same source throughout this project. The
#          event-based counterpart is cross_section_rental.R.
#
# Author: Jacopo Olivieri
# Date: 2025-04-05
# Date Modified: 2026-08-17
#
# Inputs:
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/cross_section/rentals/study_period_ea/
#   - output/log/cross_section_rental_ea.log
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required. Install project dependencies with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c("arrow", "data.table", "dplyr", "here", "logger")
LOG_FILE <- here::here("output", "log", "cross_section_rental_ea.log")

check_required_packages(REQUIRED_PACKAGES)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "site_group_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "cross_section_study_period_utils.R"),
  local = TRUE
)

CONFIG <- list(
  market = "rental",
  exposure_source = "annual_returns",
  source_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals.parquet"
  ),
  lookup_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  output_path = here::here(
    "data", "processed", "cross_section", "rentals", "study_period_ea"
  ),
  start_date = as.Date("2021-01-01"),
  end_date = as.Date("2023-12-31"),
  radii = c(250L, 500L, 1000L),
  ineligible_chunk_size = 100000L,
  output_batch_size = 20L
)

initialise_logging <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Study-period rental Annual-Returns builder started at {Sys.time()}.")
}

run_study_period_cross_section <- function(
    config = CONFIG, build = build_study_period_cross_section) {
  build(config)
}

main <- function(build = build_study_period_cross_section) {
  initialise_logging()
  tryCatch(
    {
      result <- run_study_period_cross_section(CONFIG, build)
      logger::log_info(
        "Study-period rental Annual-Returns builder completed: {result$output_path}."
      )
      invisible(result)
    },
    error = function(error) {
      logger::log_error(
        "Study-period rental Annual-Returns builder failed: {conditionMessage(error)}"
      )
      stop(conditionMessage(error), call. = FALSE)
    }
  )
}

if (sys.nframe() == 0L) main()
