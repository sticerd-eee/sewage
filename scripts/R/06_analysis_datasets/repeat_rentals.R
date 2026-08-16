# ==============================================================================
# Repeat Rentals Mapping
# ==============================================================================
#
# Purpose: Link rental records at the same address to identify repeat rentals
#          between 2014–2023.
#
# Author: Jacopo Olivieri
# Date: 2026-08-14
# Date Modified: 2026-08-15
#
# Inputs:
#   - data/processed/zoopla/zoopla_rentals_long_run.parquet (2014–2023)
#
# Outputs:
#   - data/processed/repeated_transactions/repeated_rentals_candidate.parquet
#   - data/processed/repeated_transactions/
#       repeated_rentals_large_groups_candidate.parquet
#   - data/processed/repeated_transactions/
#       repeated_rentals_price_ratios_candidate.parquet
#   - data/processed/repeated_transactions/
#       repeated_rentals_same_day_candidate.parquet
#   - output/log/repeat_rentals.log
#
# Notes:
#   - repeat_count describes the full 2014–2023 window.
#   - Window-restricted consumers must regroup after filtering.
#   - Rentals sharing an address and a date are data errors: every conflicting
#     row is excluded from the mapping and routed to the same-day review.
#   - Candidate outputs are promoted only after validation succeeds.
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(
  here::here("scripts", "R", "utils", "script_setup.R"),
  local = TRUE
)

REQUIRED_PACKAGES <- c(
  "arrow",
  "data.table",
  "digest",
  "here",
  "logger",
  "lubridate",
  "tidyselect"
)

LOG_FILE <- here::here("output", "log", "repeat_rentals.log")

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "utils", "hash_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "repeat_transactions_utils.R"),
  local = TRUE
)

repeat_rentals_config <- function(output_dir = here::here(
  "data", "processed", "repeated_transactions"
)) {
  list(
    id_col = "rental_id",
    date_col = "rented_est",
    price_col = "listing_price",
    postcode_col = "postcode",
    address_cols = c(
      "postcode", "address_line_01", "address_line_02", "address_line_03"
    ),
    primary_address_col = "address_line_01",
    property_type_col = "property_type",
    duplicate_check_cols = RENTAL_IDENTITY_FIELDS,
    input_path = here::here(
      "data", "processed", "zoopla", "zoopla_rentals_long_run.parquet"
    ),
    log_file = LOG_FILE,
    output_path = file.path(output_dir, "repeated_rentals_candidate.parquet"),
    previous_manifest_path = file.path(output_dir, "repeated_rentals.parquet"),
    large_group_review_path = file.path(output_dir, "repeated_rentals_large_groups_candidate.parquet"),
    price_ratio_review_path = file.path(output_dir, "repeated_rentals_price_ratios_candidate.parquet"),
    same_day_review_path = file.path(output_dir, "repeated_rentals_same_day_candidate.parquet"),
    market = "rentals",
    year_min = 2014L,
    year_max = 2023L,
    # Observed 2026-08-15 baselines: coverage 1.00000000, repeat share 0.74848970.
    # Unchanged from 2026-08-14: this market contains no same-day conflicts, so
    # the same-day exclusion removes nothing and its review file is empty.
    key_coverage_floor = 0.99,
    repeat_share_floor = 0.70,
    large_group_size = 12L,
    extreme_annualized_price_ratio = 4
  )
}

main <- function(config = repeat_rentals_config(), data = NULL) {
  setup_logging(config$log_file, console = interactive(), threshold = "INFO")
  logger::log_info("Building repeat-rentals mapping from the long-run superset.")
  run_repeat_transactions(config, data = data)
}

if (sys.nframe() == 0L) main()
