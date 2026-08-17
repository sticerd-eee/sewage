# ==============================================================================
# Study-Period Sales Cross-Section Builder (Annual Returns)
# ==============================================================================
#
# Purpose: Build the sales cross-section at the property-transaction level. For
#          each property transaction and radius, sum spill counts and hours
#          across all sites within that radius over the sales 2021–2024 study
#          period.
#
#          Exposure comes from the EA Annual Returns, the annual spill figures
#          published by the Environment Agency and carried on the Site Group
#          crosswalk as spill_count_ea and spill_hrs_ea. "EA" and "Annual
#          Returns" name the same source throughout this project. The
#          event-based counterpart is cross_section_sales.R.
#
# Author: Jacopo Olivieri
# Date: 2025-04-05
# Date Modified: 2026-08-17
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/cross_section/sales/study_period_ea/
#   - output/log/cross_section_sales_ea.log
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
LOG_FILE <- here::here("output", "log", "cross_section_sales_ea.log")

check_required_packages(REQUIRED_PACKAGES)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "cross_section_study_period_utils.R"),
  local = TRUE
)

CONFIG <- list(
  market = "sale",
  exposure_source = "annual_returns",
  source_path = here::here("data", "processed", "house_price.parquet"),
  lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  output_path = here::here(
    "data", "processed", "cross_section", "sales", "study_period_ea"
  ),
  start_date = as.Date("2021-01-01"),
  end_date = as.Date("2024-12-31"),
  radii = c(250L, 500L, 1000L),
  ineligible_chunk_size = 100000L,
  output_batch_size = 20L
)

initialise_logging <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Study-period sales Annual-Returns builder started at {Sys.time()}.")
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
        "Study-period sales Annual-Returns builder completed: {result$output_path}."
      )
      invisible(result)
    },
    error = function(error) {
      logger::log_error(
        "Study-period sales Annual-Returns builder failed: {conditionMessage(error)}"
      )
      stop(conditionMessage(error), call. = FALSE)
    }
  )
}

if (sys.nframe() == 0L) main()
