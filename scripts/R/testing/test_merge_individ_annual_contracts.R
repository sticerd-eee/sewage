# ==============================================================================
# Individual and Annual EDM Merge Contract Runner
# ==============================================================================
#
# Runnable standalone via plain Rscript; exits non-zero on the first failure.
#
# ==============================================================================

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

assert_identical <- function(actual, expected, message) {
  if (!identical(actual, expected)) {
    stop(
      paste0(
        message,
        "\nExpected: ", paste(capture.output(str(expected)), collapse = " "),
        "\nActual: ", paste(capture.output(str(actual)), collapse = " ")
      ),
      call. = FALSE
    )
  }
}

assert_schema_type <- function(schema, field_name, expected_type) {
  field <- schema$GetFieldByName(field_name)
  assert_true(!is.null(field), paste0("Matched schema is missing ", field_name, "."))
  assert_identical(
    field$type$ToString(),
    expected_type,
    paste0("Matched schema type drift for ", field_name, ".")
  )
}

run_contract_module <- function(path) {
  cat("Running ", path, "\n", sep = "")
  source(here::here(path), local = new.env(parent = globalenv()))
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tibble)
})

run_contract_module("scripts/R/testing/test_merge_works_register_contracts.R")
run_contract_module("scripts/R/testing/test_merge_matching_contracts.R")
run_contract_module("scripts/R/testing/test_merge_outputs_contracts.R")

merge_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "05_data_integration", "merge_individ_annual_location.R"),
  local = merge_env
)
merge_env$initialise_environment()

# ------------------------------------------------------------------------------
# Real-input integration smoke: one small company-year through the full path
# ------------------------------------------------------------------------------

smoke_company <- "Welsh Water"
smoke_year <- 2022L
smoke_config <- merge_env$CONFIG
smoke_config$years <- smoke_year

events <- arrow::open_dataset(smoke_config$data_path_individual) %>%
  dplyr::filter(
    .data$water_company == !!smoke_company,
    .data$year == !!smoke_year
  ) %>%
  dplyr::collect()

annual_returns <- arrow::open_dataset(smoke_config$data_path_annual) %>%
  dplyr::filter(.data$water_company == !!smoke_company) %>%
  dplyr::collect()

lookup <- arrow::read_parquet(smoke_config$data_path_lookup)

assert_true(nrow(events) > 0, "Smoke company-year should have event rows.")
assert_true(nrow(annual_returns) > 0, "Smoke company should have annual rows.")

smoke_result <- merge_env$build_merge_outputs_from_data(
  events = events,
  annual_returns = annual_returns,
  lookup = lookup,
  manual_overrides = tibble::tibble(),
  config = smoke_config
)

assert_identical(
  nrow(smoke_result$outputs$matched_events) + nrow(smoke_result$outputs$events_unmatched),
  nrow(events),
  "Smoke outputs should account for every event row."
)
assert_true(
  nrow(smoke_result$outputs$matched_events) > 0,
  "Smoke run should produce matched event rows."
)
assert_true(
  nrow(smoke_result$outputs$site_works_crosswalk) > 0,
  "Smoke run should produce a non-empty works-year crosswalk."
)

smoke_output_dir <- tempfile("merge-individ-smoke-")
on.exit(unlink(smoke_output_dir, recursive = TRUE, force = TRUE), add = TRUE)
merge_env$write_merge_outputs(smoke_result$outputs, smoke_output_dir)

matched_path <- file.path(
  smoke_output_dir,
  merge_env$MERGE_OUTPUT_FILES[["matched_events"]]
)
matched_table <- arrow::read_parquet(matched_path, as_data_frame = FALSE)
matched_schema <- matched_table$schema

assert_identical(
  names(matched_table),
  names(merge_env$MATCHED_EVENTS_PROTOTYPE),
  "Matched output column names should match the production prototype."
)

assert_schema_type(matched_schema, "site_id", "int32")
assert_schema_type(matched_schema, "start_time", "timestamp[us, tz=UTC]")
assert_schema_type(matched_schema, "end_time", "timestamp[us, tz=UTC]")
assert_schema_type(matched_schema, "spill_hrs_ea", "double")
assert_schema_type(matched_schema, "spill_count_ea", "double")
assert_schema_type(matched_schema, "unique_id", "string")
assert_schema_type(matched_schema, "ngr", "string")
assert_schema_type(matched_schema, "water_company", "string")
assert_schema_type(matched_schema, "year", "int32")

# ------------------------------------------------------------------------------
# Sabotaged CONFIG subprocess: missing input exits non-zero, output untouched
# ------------------------------------------------------------------------------

sabotage_root <- tempfile("merge-individ-sabotage-")
sabotage_canonical <- file.path(sabotage_root, "matched_events_annual_data")
dir.create(sabotage_root, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(sabotage_root, recursive = TRUE, force = TRUE), add = TRUE)

merge_env$write_merge_outputs(smoke_result$outputs, sabotage_canonical)
before_hashes <- tools::md5sum(file.path(
  sabotage_canonical,
  unname(merge_env$MERGE_OUTPUT_FILES)
))

sabotage_script <- tempfile("merge-individ-sabotage-script-", fileext = ".R")
writeLines(
  c(
    paste0("setwd(", dQuote(here::here()), ")"),
    "env <- new.env(parent = globalenv())",
    "source('scripts/R/05_data_integration/merge_individ_annual_location.R', local = env)",
    "cfg <- env$CONFIG",
    "cfg$data_path_individual <- file.path(tempdir(), 'missing-combined-edm.parquet')",
    paste0("cfg$output_dir <- ", dQuote(sabotage_canonical)),
    "cfg$manual_overrides_path <- file.path(tempdir(), 'missing-overrides.csv')",
    "env$LOG_FILE <- file.path(tempdir(), 'sabotaged-merge.log')",
    "env$main(cfg)"
  ),
  sabotage_script
)

sabotage_run <- suppressWarnings(system2("Rscript", sabotage_script, stdout = TRUE, stderr = TRUE))
sabotage_status <- attr(sabotage_run, "status")
assert_true(
  !is.null(sabotage_status) && sabotage_status != 0L,
  "Sabotaged CONFIG subprocess should exit non-zero."
)
assert_true(
  merge_env$complete_merge_output_set(sabotage_canonical),
  "Sabotaged CONFIG should leave the pre-existing canonical output set complete."
)
assert_identical(
  tools::md5sum(file.path(sabotage_canonical, unname(merge_env$MERGE_OUTPUT_FILES))),
  before_hashes,
  "Sabotaged CONFIG should leave existing canonical output files untouched."
)

cat("All merge_individ_annual contract tests passed.\n")
