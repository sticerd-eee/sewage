# ==============================================================================
# ID Artifact Verifier Specification Contracts
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this test.", call. = FALSE)
}
if (!requireNamespace("arrow", quietly = TRUE)) {
  stop("Package `arrow` is required to run this test.", call. = FALSE)
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
    "study_period_ea_sales", "study_period_ea_rentals",
    "prior_to_sale", "prior_to_rental",
    "prior_to_sale_house_site", "prior_to_rental_rental_site",
    "within_radius_sales", "within_radius_rentals",
    "general_panel_sales", "general_panel_rentals"
  )),
  "The verifier inventory must contain the exact 16 declared ID-keyed artifacts."
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
assert_identical(
  normalizePath(spec_by_name$study_period_ea_sales$source_path, mustWork = FALSE),
  normalizePath(
    here::here("data", "processed", "house_price.parquet"),
    mustWork = FALSE
  ),
  "Study-period EA sales verification must use the same source as study-period sales."
)
assert_identical(
  normalizePath(
    spec_by_name$study_period_ea_rentals$source_path,
    mustWork = FALSE
  ),
  normalizePath(
    here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    mustWork = FALSE
  ),
  "Study-period EA rental verification must use the same source as study-period rentals."
)
assert_identical(
  c(spec_by_name$study_period_ea_sales$id, spec_by_name$study_period_ea_rentals$id),
  c("house_id", "rental_id"),
  "Study-period EA specs must key on the same identifiers as study-period specs."
)

# The engine-side scan must count exactly what the row-wise scan counted:
# NA and empty IDs are missing, present IDs are matched against the cleaned
# source, and unmatched IDs are reported with examples.
fixture_root <- tempfile("id-match-rate-counting-")
dir.create(fixture_root, recursive = TRUE)
on.exit(unlink(fixture_root, recursive = TRUE), add = TRUE)
fixture_source <- file.path(fixture_root, "source.parquet")
fixture_artifact <- file.path(fixture_root, "artifact.parquet")
arrow::write_parquet(
  data.frame(house_id = c("01leadingzero", "02stablehash"), stringsAsFactors = FALSE),
  fixture_source
)
arrow::write_parquet(
  data.frame(
    house_id = c(
      "01leadingzero", "01leadingzero", "02stablehash",
      NA_character_, "", "stale-id"
    ),
    stringsAsFactors = FALSE
  ),
  fixture_artifact
)
counted <- verifier_env$verify_id_artifact(
  "fixture-counting", fixture_artifact, "house_id",
  verifier_env$read_unique_source_ids(fixture_source, "house_id")
)
assert_identical(counted$total_rows, 6, "The verifier must count every artifact row.")
assert_identical(counted$missing_ids, 2, "NA and empty IDs must count as missing.")
assert_identical(counted$nonmissing_ids, 4, "Present IDs must exclude NA and empty IDs.")
assert_identical(counted$matched_ids, 3, "Every present ID found in the source must match.")
assert_identical(counted$unmatched_ids, 1, "Present IDs absent from the source must be unmatched.")
assert_identical(counted$match_rate, 0.75, "The match rate must be matched over nonmissing IDs.")
assert_identical(
  counted$unmatched_examples,
  "stale-id",
  "The verifier must report the unmatched IDs it found."
)

message("ID artifact verifier specification contract tests passed.")
