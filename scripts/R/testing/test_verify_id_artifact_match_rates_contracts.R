# ==============================================================================
# ID Artifact Verifier Specification Contracts
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this test.", call. = FALSE)
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(message, call. = FALSE)
  }
}

verifier_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "testing", "verify_id_artifact_match_rates.R"),
  envir = verifier_env
)

specs <- verifier_env$default_id_artifact_specs()
spec_by_name <- stats::setNames(specs, vapply(specs, `[[`, character(1), "name"))

assert_identical(
  sort(names(spec_by_name)),
  sort(c(
    "spill_house_lookup", "spill_rental_lookup",
    "repeated_sales", "repeated_rentals",
    "study_period_sales", "study_period_rentals",
    "prior_to_sale", "prior_to_rental",
    "prior_to_sale_house_site", "prior_to_rental_rental_site",
    "within_radius_sales", "within_radius_rentals",
    "general_panel_sales", "general_panel_rentals"
  )),
  "The verifier inventory must contain the exact 14 declared ID-keyed artifacts."
)

assert_identical(
  normalizePath(spec_by_name$repeated_sales$source_path, mustWork = FALSE),
  normalizePath(
    here::here("data", "processed", "house_price_long_run.parquet"),
    mustWork = FALSE
  ),
  "Repeated-sales verification must use the long-run sales source."
)
assert_identical(
  normalizePath(spec_by_name$repeated_rentals$source_path, mustWork = FALSE),
  normalizePath(
    here::here(
      "data", "processed", "zoopla", "zoopla_rentals_long_run.parquet"
    ),
    mustWork = FALSE
  ),
  "Repeated-rentals verification must use the long-run rental source."
)
assert_identical(
  normalizePath(spec_by_name$study_period_sales$source_path, mustWork = FALSE),
  normalizePath(
    here::here("data", "processed", "house_price.parquet"),
    mustWork = FALSE
  ),
  "Study-period sales verification must continue to use the study source."
)
assert_identical(
  normalizePath(spec_by_name$study_period_rentals$source_path, mustWork = FALSE),
  normalizePath(
    here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    mustWork = FALSE
  ),
  "Study-period rental verification must continue to use the study source."
)

message("ID artifact verifier specification contract tests passed.")
