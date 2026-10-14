# ==============================================================================
# Study-Period Rental Cross-Section Builder (Individual EDM Events)
# ==============================================================================
#
# Purpose: Build the rental cross-section at the rental listing level. For each
#          rental listing and radius, sum spill counts and hours across all sites
#          within that radius over the 2021–2023 rental study period.
#
#          Exposure comes from the matched individual EDM events. Events are
#          clipped to the study window, spill hours are the summed clipped
#          durations, and spill counts are recomputed with the EA 12/24 rule
#          (count_spills()) rather than taken from the companies' reported
#          annual figures. This is the same measurement the prior-exposure
#          datasets use, so the two families now differ only in window.
#
#          The Annual Returns remain the evidence oracle: the event feed carries
#          positives only, so a Site Group whose window contains a reported_na or
#          absent annual status still yields NA exposure. The Annual-Returns
#          counterpart is cross_section_rental_ea.R.
#
# Author: Jacopo Olivieri
# Date: 2026-08-17
# Date Modified: 2026-08-17
#
# Inputs:
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/processed/matched_events_annual_data/matched_events_annual_data.parquet
#
# Outputs:
#   - data/processed/cross_section/rentals/study_period/
#   - output/log/cross_section_rental.log
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
LOG_FILE <- here::here("output", "log", "cross_section_rental.log")

check_required_packages(REQUIRED_PACKAGES)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "spill_aggregation_utils.R"),
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
  exposure_source = "events",
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
  events_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "matched_events_annual_data.parquet"
  ),
  output_path = here::here(
    "data", "processed", "cross_section", "rentals", "study_period"
  ),
  start_date = as.Date("2021-01-01"),
  end_date = as.Date("2023-12-31"),
  radii = c(250L, 500L, 1000L),
  ineligible_chunk_size = 100000L,
  output_batch_size = 20L
)

initialise_logging <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Study-period rental event builder started at {Sys.time()}.")
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
        "Study-period rental event builder completed: {result$output_path}."
      )
      invisible(result)
    },
    error = function(error) {
      logger::log_error(
        "Study-period rental event builder failed: {conditionMessage(error)}"
      )
      stop(conditionMessage(error), call. = FALSE)
    }
  )
}

if (sys.nframe() == 0L) main()
