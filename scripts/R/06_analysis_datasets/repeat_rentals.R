# ==============================================================================
# Repeat Rentals Mapping
# ==============================================================================
#
# Input:
#   - data/processed/zoopla/zoopla_rentals_long_run.parquet (2014-2023)
# Output candidate:
#   - data/processed/repeated_transactions/repeated_rentals_candidate.parquet
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
    duplicate_check_cols = c(
      "postcode", "address_line_01", "address_line_02", "address_line_03",
      "listing_price", "latest_to_rent", "rented"
    ),
    input_path = here::here(
      "data", "processed", "zoopla", "zoopla_rentals_long_run.parquet"
    ),
    log_file = here::here("output", "log", "repeat_rentals.log"),
    output_path = file.path(output_dir, "repeated_rentals_candidate.parquet"),
    previous_manifest_path = file.path(output_dir, "repeated_rentals.parquet"),
    large_group_review_path = file.path(output_dir, "repeated_rentals_large_groups_candidate.parquet"),
    price_ratio_review_path = file.path(output_dir, "repeated_rentals_price_ratios_candidate.parquet"),
    market = "rentals",
    log_name = "repeat_rentals",
    year_min = 2014L,
    year_max = 2023L,
    key_coverage_floor = 0,
    repeat_share_floor = 0,
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
