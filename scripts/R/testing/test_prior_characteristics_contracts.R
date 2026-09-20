# ==============================================================================
# Property-Radius Characteristics Contract Tests
# ==============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(here)
  library(tibble)
})

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      message,
      "\nActual: ", paste(capture.output(str(actual)), collapse = " "),
      "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
      call. = FALSE
    )
  }
}

assert_error_contains <- function(expression, expected, message) {
  error_message <- tryCatch(
    {
      force(expression)
      NA_character_
    },
    error = function(error) conditionMessage(error)
  )
  if (is.na(error_message) || !grepl(expected, error_message, fixed = TRUE)) {
    stop(message, "\nActual error: ", error_message, call. = FALSE)
  }
}

producer_env <- new.env(parent = globalenv())
sys.source(
  here::here(
    "scripts", "R", "06_analysis_datasets", "build_prior_characteristics.R"
  ),
  envir = producer_env
)

universe <- tibble(
  house_id = rep(c("zero", "one", "multi", "unknown"), each = 3L),
  radius = rep(c(250L, 500L, 1000L), 4L),
  n_spill_sites = c(
    0L, 0L, 0L,
    1L, 1L, 1L,
    1L, 2L, 3L,
    1L, 1L, 1L
  ),
  spill_count_weekly_avg = c(
    0, 0, 0,
    0, 1, 2,
    1, 2, 4,
    NA, NA, NA
  ),
  spill_hrs_weekly_avg = c(
    0, 0, 0,
    0, 10, 20,
    5, 10, 40,
    NA, NA, NA
  )
)
pairs <- tibble(
  house_id = c("one", "multi", "multi", "multi", "unknown"),
  site_id = c(1L, 1L, 2L, 3L, 4L),
  distance_m = c(100, 100, 400, 900, 100)
)
site_characteristics <- tibble(
  site_id = 1:4,
  distance_to_coast_m = c(1000, 5000, 10000, 2000),
  bath_ever_2124 = c(TRUE, FALSE, FALSE, FALSE),
  bath_unknown_2124 = c(FALSE, FALSE, FALSE, TRUE),
  shell_ever_2124 = c(FALSE, TRUE, FALSE, FALSE),
  shell_unknown_2124 = c(FALSE, FALSE, FALSE, TRUE)
)

output <- producer_env$build_market_prior_characteristics(
  source_data = universe,
  lookup_data = pairs,
  site_characteristics = site_characteristics,
  id_column = "house_id",
  market = "sales",
  radii = c(250L, 500L, 1000L)
)

assert_identical(
  output[c("house_id", "radius")],
  universe[c("house_id", "radius")],
  "The property companion must conserve the source row universe and order."
)
assert_true(
  !anyDuplicated(output[c("house_id", "radius")]),
  "Property companion keys must be unique."
)
assert_identical(
  names(output),
  producer_env$prior_characteristics_columns("house_id"),
  "The property companion schema and order must be exact."
)

zero <- output |> filter(house_id == "zero", radius == 1000L)
assert_identical(zero$n_spill_sites, 0L, "No-site source counts must be retained.")
assert_true(is.na(zero$min_coast_dist_m) && is.na(zero$max_coast_dist_m), "No-site coast ranges must be NA.")
assert_identical(zero$any_bath_2124, FALSE, "No-site any must be FALSE.")
assert_true(is.na(zero$all_bath_2124), "No-site all must be NA.")
assert_identical(zero$mixed_bath_2124, FALSE, "No-site mixed must be FALSE.")
assert_identical(zero$bath_unknown_2124, FALSE, "No-site unknown must be FALSE.")
assert_identical(zero$n_bath_2124, 0L, "No-site counts must be zero.")
assert_identical(zero$spill_count_band, "no_site", "No-site rows must have the no_site band.")

multi_500 <- output |> filter(house_id == "multi", radius == 500L)
assert_identical(multi_500$min_coast_dist_m, 1000, "Coast minimum must cover nearby sites.")
assert_identical(multi_500$max_coast_dist_m, 5000, "Coast maximum must cover nearby sites.")
assert_identical(multi_500$any_bath_2124, TRUE, "Any designated site must set any.")
assert_identical(multi_500$all_bath_2124, FALSE, "A known negative site must clear all.")
assert_identical(multi_500$mixed_bath_2124, TRUE, "Positive and known negative sites must be mixed.")
assert_identical(multi_500$n_bath_2124, 1L, "Designated site counts must be exact.")

unknown <- output |> filter(house_id == "unknown", radius == 250L)
assert_identical(unknown$bath_unknown_2124, TRUE, "Uncertain nearby evidence must be retained.")
assert_identical(unknown$all_bath_2124, FALSE, "Unknown evidence must prevent an all-designated verdict.")
assert_identical(unknown$spill_count_band, "unknown", "Missing exposure with sites must be unknown.")

# Positive p50 is computed separately by radius and measure. At radius 500,
# positive spill counts are 1 and 2, so the cutoff is 1.5. At radius 1000 the
# positive counts 2 and 4 split at 3. Hours use their own medians.
audit <- producer_env$build_intensity_cutoff_audit(
  bind_rows(
    transmute(universe, market = "sales", radius, n_spill_sites,
      spill_count_weekly_avg, spill_hrs_weekly_avg),
    transmute(universe, market = "rentals", radius, n_spill_sites,
      spill_count_weekly_avg, spill_hrs_weekly_avg)
  )
)
sales_count_500 <- audit |>
  filter(market == "sales", radius == 500L, measure == "spill_count_weekly_avg")
sales_hours_500 <- audit |>
  filter(market == "sales", radius == 500L, measure == "spill_hrs_weekly_avg")
assert_identical(sales_count_500$p50, 1.5, "Count p50 must use only positive rows.")
assert_identical(sales_hours_500$p50, 10, "Hours p50 must be measure-specific.")
assert_identical(
  sales_count_500$n_total,
  sales_count_500$n_no_site + sales_count_500$n_unknown +
    sales_count_500$n_zero + sales_count_500$n_positive,
  "Audit total categories must reconcile."
)
assert_identical(
  sales_count_500$n_positive,
  sales_count_500$n_le_p50 + sales_count_500$n_gt_p50,
  "Audit positive bands must reconcile."
)

tied <- tibble(
  house_id = c("a", "b", "c"),
  radius = 250L,
  n_spill_sites = 1L,
  spill_count_weekly_avg = c(1, 1, 2),
  spill_hrs_weekly_avg = c(5, 5, 10)
)
tied_output <- producer_env$add_intensity_bands(tied, market = "sales")
assert_identical(
  tied_output$spill_count_band,
  c("spill_le_p50", "spill_le_p50", "spill_gt_p50"),
  "All exact median ties must enter spill_le_p50."
)

assert_error_contains(
  producer_env$build_market_prior_characteristics(
    universe,
    bind_rows(pairs, tibble(house_id = "one", site_id = 999L, distance_m = 50)),
    site_characteristics,
    "house_id", "sales", c(250L, 500L, 1000L)
  ),
  "characteristic",
  "A real property-Site Group pair without characteristics must fail closed."
)
assert_error_contains(
  producer_env$add_intensity_bands(
    mutate(tied, spill_count_weekly_avg = -1), market = "sales"
  ),
  "non-negative",
  "Known negative exposures must be rejected."
)
assert_error_contains(
  producer_env$add_intensity_bands(
    mutate(tied, spill_count_weekly_avg = 0, spill_hrs_weekly_avg = 0),
    market = "sales"
  ),
  "positive",
  "A market-radius-measure without positive rows must fail closed."
)
assert_error_contains(
  producer_env$validate_prior_characteristics(
    mutate(output, spill_count_band = "invalid"), universe, "house_id"
  ),
  "allowed",
  "Unexpected band enums must be rejected."
)

# Optional canonical read-back. Before the first production build these checks
# are skipped; afterwards they enforce all six market-radius partitions and
# the combined cutoff audit.
if (all(vapply(producer_env$MARKET_SPECS, function(spec) {
    dir.exists(spec$output_path)
  }, logical(1)))) {
  for (spec in producer_env$MARKET_SPECS) {
    dataset <- arrow::open_dataset(spec$output_path)
    refined <- "site_generation" %in% dataset$schema$names
    expected_columns <- producer_env$prior_characteristics_columns(spec$id, refined)
    assert_true(
      setequal(dataset$schema$names, expected_columns) &&
        length(dataset$schema$names) == length(expected_columns),
      paste(spec$market, "canonical schema must be exact.")
    )
    expected_signature <- producer_env$arrow_schema_signature(
      producer_env$prior_characteristics_schema(spec$id, include_radius = TRUE, refined = refined)
    )
    observed_signature <- producer_env$arrow_schema_signature(dataset$schema)
    assert_identical(
      observed_signature[names(expected_signature)],
      expected_signature,
      paste(spec$market, "canonical physical types must be exact.")
    )
    radii <- dataset |>
      select("radius") |>
      distinct() |>
      collect() |>
      pull("radius") |>
      sort()
    assert_identical(
      as.integer(radii),
      c(250L, 500L, 1000L),
      paste(spec$market, "must publish all three Hive radius partitions.")
    )
    for (radius_value in c(250L, 500L, 1000L)) {
      canonical <- dataset |>
        filter(.data$radius == !!radius_value) |>
        collect() |>
        select(all_of(expected_columns))
      source <- producer_env$read_source_radius(spec, radius_value)
      producer_env$validate_prior_characteristics(canonical, source, spec$id, refined)
    }
  }

  cutoff_path <- producer_env$CUTOFF_OUTPUT_PATH
  assert_true(file.exists(cutoff_path), "The combined cutoff audit must exist.")
  cutoff <- arrow::read_parquet(cutoff_path)
  producer_env$validate_cutoff_audit(cutoff)
  assert_identical(
    producer_env$arrow_schema_signature(arrow::open_dataset(cutoff_path)$schema),
    producer_env$arrow_schema_signature(producer_env$cutoff_audit_schema()),
    "The cutoff audit must retain the exact physical types."
  )
  assert_identical(nrow(cutoff), 12L, "The cutoff audit must contain 2 markets × 3 radii × 2 measures.")
}

cat("All property-radius characteristics contract tests passed.\n")
