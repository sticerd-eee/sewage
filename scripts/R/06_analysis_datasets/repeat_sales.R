# ==============================================================================
# Repeat Sales Mapping
# ==============================================================================
#
# Input:
#   - data/processed/house_price_long_run.parquet (2014-2024)
# Output candidate:
#   - data/processed/repeated_transactions/repeated_sales_candidate.parquet
#
# repeat_count describes the full long-run window. Window-restricted consumers
# must regroup after filtering.
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install dependencies with `rv sync`.", call. = FALSE)
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow", "data.table", "digest", "logger", "lubridate", "tidyselect"
)
check_required_packages(REQUIRED_PACKAGES)

source(here::here("scripts", "R", "utils", "hash_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "repeat_transactions_utils.R"), local = TRUE)

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
    log_file = here::here("output", "log", "repeat_sales.log"),
    output_path = file.path(output_dir, "repeated_sales_candidate.parquet"),
    previous_manifest_path = file.path(output_dir, "repeated_sales.parquet"),
    large_group_review_path = file.path(output_dir, "repeated_sales_large_groups_candidate.parquet"),
    price_ratio_review_path = file.path(output_dir, "repeated_sales_price_ratios_candidate.parquet"),
    market = "sales",
    log_name = "repeat_sales",
    year_min = 2014L,
    year_max = 2024L,
    # Observed 2026-08-14 baselines: coverage 0.99681926, repeat share 0.36939131.
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
