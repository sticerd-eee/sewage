# ==============================================================================
# Repeat Sales Mapping
# ==============================================================================
#
# Purpose: Link sales made at the same address to identify repeat sales between
#          2014–2024.
#
# Author: Jacopo Olivieri
# Date: 2026-08-14
# Date Modified: 2026-08-15
#
# Inputs:
#   - data/processed/house_price_long_run.parquet (2014–2024)
#
# Outputs:
#   - data/processed/repeated_transactions/repeated_sales.parquet
#   - data/processed/repeated_transactions/repeated_sales_large_groups.parquet
#   - data/processed/repeated_transactions/repeated_sales_price_ratios.parquet
#   - data/processed/repeated_transactions/repeated_sales_same_day.parquet
#   - output/log/repeat_sales.log
#
# Notes:
#   - repeat_count describes the full 2014–2024 window.
#   - Window-restricted consumers must regroup after filtering.
#   - Sales sharing an address and a date are data errors: every conflicting row
#     is excluded from the mapping and routed to the same-day review.
#   - Outputs are staged as `*_candidate.parquet` and promoted onto the paths
#     above only once the run's checks pass; a failed run leaves the previous
#     generation in place. Publication replaces it without keeping a backup.
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

LOG_FILE <- here::here("output", "log", "repeat_sales.log")

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "utils", "hash_utils.R"),
  local = TRUE
)
source(
  here::here("scripts", "R", "utils", "repeat_transactions_utils.R"),
  local = TRUE
)

repeat_sales_config <- function(output_dir = here::here(
  "data", "processed", "repeated_transactions"
)) {
  list(
    id_col = "house_id",
    date_col = "date_of_transfer",
    price_col = "price",
    postcode_col = "postcode",
    address_cols = c("postcode", "paon", "saon", "street"),
    primary_address_col = "paon",
    property_type_col = "property_type",
    duplicate_check_cols = character(),
    input_path = here::here("data", "processed", "house_price_long_run.parquet"),
    log_file = LOG_FILE,
    output_path = file.path(output_dir, "repeated_sales_candidate.parquet"),
    previous_manifest_path = file.path(output_dir, "repeated_sales.parquet"),
    large_group_review_path = file.path(output_dir, "repeated_sales_large_groups_candidate.parquet"),
    price_ratio_review_path = file.path(output_dir, "repeated_sales_price_ratios_candidate.parquet"),
    same_day_review_path = file.path(output_dir, "repeated_sales_same_day_candidate.parquet"),
    market = "sales",
    publish = TRUE,
    year_min = 2014L,
    year_max = 2024L,
    # Observed 2026-08-15 baselines: coverage 0.99681926, repeat share 0.36233545.
    # Coverage measures address-key completeness only and is unchanged from
    # 2026-08-14. Repeat share fell from 0.36939131 because the 102699 rows in
    # 50327 same-day conflicts now leave the mapping; largest group fell 20 -> 11.
    key_coverage_floor = 0.99,
    repeat_share_floor = 0.35,
    large_group_size = 12L,
    extreme_annualized_price_ratio = 4
  )
}

main <- function(config = repeat_sales_config(), data = NULL) {
  setup_logging(config$log_file, console = interactive(), threshold = "INFO")
  logger::log_info("Building repeat-sales mapping from the long-run superset.")
  run_repeat_transactions(config, data = data)
}

if (sys.nframe() == 0L) main()
