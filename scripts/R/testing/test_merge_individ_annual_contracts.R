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
# Synthetic micro-fixtures: works-grain agreement hours, key_spans_works,
# and agreement rejection evidence
# ------------------------------------------------------------------------------

fixture_resolver_row <- function(site_id, year = 2024L,
                                 company = "Fixture Water",
                                 site_name_ea = NA_character_,
                                 permit_reference_ea = NA_character_,
                                 unique_id = NA_character_,
                                 spill_hrs_ea = NA_real_) {
  tibble::tibble(
    site_id = as.integer(site_id),
    water_company = company,
    year = as.integer(year),
    site_name_ea = site_name_ea,
    site_name_wa_sc = NA_character_,
    permit_reference_ea = permit_reference_ea,
    permit_reference_wa_sc = NA_character_,
    activity_reference = NA_character_,
    unique_id = unique_id,
    spill_hrs_ea = spill_hrs_ea,
    spill_count_ea = NA_real_
  )
}

fixture_event_row <- function(year = 2024L, company = "Fixture Water",
                              site_name_ea = NA_character_,
                              permit_reference_ea = NA_character_,
                              event_hours = 0) {
  tibble::tibble(
    water_company = company,
    year = as.integer(year),
    site_name_ea = site_name_ea,
    site_name_wa_sc = NA_character_,
    permit_reference_ea = permit_reference_ea,
    permit_reference_wa_sc = NA_character_,
    activity_reference = NA_character_,
    unique_id = NA_character_,
    event_hours = event_hours
  )
}

fixture_events <- dplyr::bind_rows(
  # Works 101 files under two member-outlet name variants (60 + 40 hours);
  # the event carries the works TOTAL, so acceptance requires works-year
  # hours summed over ALL resolver rows of the works, not just name-rung rows.
  fixture_event_row(site_name_ea = "Main Works", event_hours = 100),
  # Only usable key is a permit spanning two works; no name key.
  fixture_event_row(permit_reference_ea = "P-SPAN"),
  # Agreement rejection: candidates too close to separate (90 vs 85 for 100).
  fixture_event_row(site_name_ea = "Close Works", event_hours = 100)
)

fixture_resolver <- dplyr::bind_rows(
  fixture_resolver_row(101L, site_name_ea = "Main Works", spill_hrs_ea = 60),
  fixture_resolver_row(101L, site_name_ea = "Main Works STW", spill_hrs_ea = 40),
  fixture_resolver_row(102L, site_name_ea = "Main Works", spill_hrs_ea = 60),
  fixture_resolver_row(201L, permit_reference_ea = "P-SPAN"),
  fixture_resolver_row(202L, permit_reference_ea = "P-SPAN"),
  fixture_resolver_row(301L, site_name_ea = "Close Works", spill_hrs_ea = 90),
  fixture_resolver_row(302L, site_name_ea = "Close Works", spill_hrs_ea = 85)
)

fixture_decisions <- merge_env$resolve_merge_matches(
  fixture_events,
  fixture_resolver
)

works_grain_decision <- fixture_decisions %>%
  dplyr::filter(.data$site_name_ea == "Main Works")
assert_identical(
  works_grain_decision$site_id,
  101L,
  "Agreement should accept on works-year TOTAL hours across all member outlets."
)
assert_identical(
  works_grain_decision$match_method,
  "agreement",
  "Works-grain agreement acceptance should record the agreement method."
)

key_spans_decision <- fixture_decisions %>%
  dplyr::filter(.data$permit_reference_ea == "P-SPAN")
assert_true(
  is.na(key_spans_decision$site_id),
  "A permit key spanning two works should not match."
)
assert_identical(
  key_spans_decision$reason,
  "key_spans_works",
  "A spanning non-name key should be reason-coded key_spans_works."
)

reject_decision <- fixture_decisions %>%
  dplyr::filter(.data$site_name_ea == "Close Works")
assert_identical(
  reject_decision$reason,
  "agreement_failed",
  "Near-equal agreement candidates should still be reason-coded agreement_failed."
)

agreement_near_misses <- attr(fixture_decisions, "agreement_near_misses")
assert_true(
  !is.null(agreement_near_misses),
  "Driver should expose agreement_near_misses on its return value."
)
assert_identical(
  names(agreement_near_misses),
  names(merge_env$MERGE_AGREEMENT_NEAR_MISS_PROTOTYPE),
  "agreement_near_misses should follow the agreement near-miss prototype."
)
assert_identical(
  nrow(agreement_near_misses),
  2L,
  "The rejected agreement tuple should contribute one evidence row per candidate."
)
assert_identical(
  sort(agreement_near_misses$candidate_site_id),
  c(301L, 302L),
  "Agreement evidence should list every scored candidate works."
)
assert_true(
  all(agreement_near_misses$reason == "agreement_failed"),
  "Agreement evidence rows should carry the rejection reason."
)
assert_true(
  all(agreement_near_misses$site_name_ea == "CLOSE WORKS"),
  "Agreement evidence should carry the tuple's normalised name-key value."
)
assert_true(
  all(agreement_near_misses$event_hours == 100),
  "Agreement evidence should carry the tuple's event hours."
)
assert_true(
  all(abs(sort(agreement_near_misses$relative_error) - c(0.10, 0.15)) < 1e-9),
  "Agreement evidence should carry the candidates' relative errors."
)

fixture_near_miss_report <- merge_env$assemble_near_miss_report(
  agreement_near_misses = agreement_near_misses
)
assert_identical(
  nrow(fixture_near_miss_report),
  2L,
  "agreement_near_misses should flow through assemble_near_miss_report."
)
assert_true(
  all(fixture_near_miss_report$report_type == "agreement_rejected"),
  "Assembled agreement near-misses should be report-typed agreement_rejected."
)

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

# Mirror the production load path (load_merge_inputs filters annual rows to
# config$years): the register util now derives its year-specific lookup
# columns from the years threaded in from CONFIG rather than a hardcoded
# 2021:2024, so out-of-config annual rows would be unlinkable here.
annual_returns <- arrow::open_dataset(smoke_config$data_path_annual) %>%
  dplyr::filter(
    .data$water_company == !!smoke_company,
    .data$year %in% smoke_config$years
  ) %>%
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

# ------------------------------------------------------------------------------
# String-near name pass among unmatched tuples (plan D7 output 5)
# ------------------------------------------------------------------------------

string_near_decisions <- tibble::tibble(
  water_company = "Fixture Water",
  year = 2024L,
  site_name_ea = c("Broadford Works", "Spanner Works", "ABC", NA_character_, "Matched Works"),
  site_name_wa_sc = c(NA_character_, NA_character_, NA_character_, "Crossfield STW", NA_character_),
  permit_reference_ea = NA_character_,
  permit_reference_wa_sc = NA_character_,
  activity_reference = NA_character_,
  unique_id = NA_character_,
  site_id = c(NA_integer_, NA_integer_, NA_integer_, NA_integer_, 703L),
  match_method = c(NA, NA, NA, NA, "site_name_ea"),
  match_quality = c(NA, NA, NA, NA, 1),
  reason = c("no_usable_key", "name_spans_works", "no_usable_key", "no_usable_key", NA_character_),
  annual_status_hint = NA_character_
)

string_near_resolver <- tibble::tibble(
  water_company = "Fixture Water",
  site_id = c(701L, 702L, 703L, 704L),
  site_name_ea_norm = c(
    "BRODFORD WORKS", "CROSSFIELD STW", "SPANNER WORKS", "TOTALLY DIFFERENT"
  ),
  site_name_wa_sc_norm = NA_character_
)

string_near_report <- merge_env$assemble_string_near_name_report(
  string_near_decisions,
  string_near_resolver,
  max_edit_distance = 2,
  min_name_chars = 6
)

assert_true(
  all(string_near_report$report_type == "string_near_name"),
  "String-near rows should be report-typed string_near_name."
)
edit_distance_hit <- string_near_report %>%
  dplyr::filter(.data$site_name_ea == "BROADFORD WORKS")
assert_identical(
  nrow(edit_distance_hit),
  1L,
  "An unmatched name within the edit-distance threshold should be reported once."
)
assert_identical(
  edit_distance_hit$reason,
  "edit_distance_1",
  "String-near reason should encode the edit distance."
)
assert_identical(
  edit_distance_hit$site_id,
  701L,
  "String-near site_id should be the annual candidate's works site_id."
)
assert_identical(
  edit_distance_hit$candidate_site_id,
  701L,
  "String-near candidate_site_id should equal the annual works site_id."
)
assert_identical(
  edit_distance_hit$year,
  2024L,
  "String-near rows should carry the unmatched tuple's year."
)

cross_field_hit <- string_near_report %>%
  dplyr::filter(.data$site_name_ea == "CROSSFIELD STW")
assert_identical(
  nrow(cross_field_hit),
  1L,
  "A cross-field exact-equal name should be reported with distance 0."
)
assert_identical(
  cross_field_hit$reason,
  "edit_distance_0",
  "Cross-field exact-equal names should be report-coded edit_distance_0."
)

assert_true(
  !"SPANNER WORKS" %in% string_near_report$site_name_ea,
  "Same-field exact-equal names should be skipped (candidate-domain cases)."
)
assert_true(
  !any(grepl("ABC", string_near_report$site_name_ea)),
  "Names shorter than the minimum length should be skipped."
)
assert_true(
  !"MATCHED WORKS" %in% string_near_report$site_name_ea,
  "Matched tuples should not enter the string-near pass."
)

combined_near_miss_report <- merge_env$assemble_near_miss_report(
  agreement_near_misses = agreement_near_misses,
  string_near_name_misses = string_near_report
)
assert_identical(
  sort(unique(combined_near_miss_report$report_type)),
  c("agreement_rejected", "string_near_name"),
  "Agreement rejects and string-near rows should both appear in the near-miss report."
)
assert_identical(
  nrow(combined_near_miss_report),
  nrow(agreement_near_misses) + nrow(string_near_report),
  "Near-miss assembly should keep every agreement and string-near row."
)

# ------------------------------------------------------------------------------
# KTD-10 carry-forward: years before the first return take the NEAREST future
# location; unparseable NGRs never shadow parseable ones; all-NA commission
# dates stay NA (not Inf)
# ------------------------------------------------------------------------------

ktd10_membership <- tibble::tibble(
  site_id = 900L,
  member_site_id = 900L,
  water_company = "Fixture Water",
  component_id = 1L,
  n_members = 1L,
  site_id_members = "900"
)

ktd10_resolver <- tibble::tibble(
  site_id = 900L,
  member_site_id = 900L,
  water_company = "Fixture Water",
  year = c(2022L, 2024L),
  ngr = c("TQ3000080000", "TQ4000090000"),
  easting = c(530000, 540000),
  northing = c(180000, 190000),
  spill_hrs_ea = c(1, 2),
  spill_count_ea = c(1, 1),
  site_name_ea_norm = "KTD TEN WORKS",
  site_name_wa_sc_norm = NA_character_
)

ktd10_crosswalk <- merge_env$build_site_works_crosswalk(
  membership = ktd10_membership,
  resolver = ktd10_resolver,
  decisions = merge_env$MERGE_MATCHING_DECISION_PROTOTYPE,
  years = 2021:2024
)

assert_identical(
  ktd10_crosswalk %>% dplyr::filter(.data$year == 2021L) %>% dplyr::pull(.data$ngr),
  "TQ3000080000",
  "A year before the works' first return should carry the NEAREST future location (2022), not the farthest (2024)."
)
assert_identical(
  ktd10_crosswalk %>% dplyr::filter(.data$year == 2023L) %>% dplyr::pull(.data$ngr),
  "TQ3000080000",
  "A gap year should carry the most recent past location."
)
assert_identical(
  ktd10_crosswalk %>% dplyr::filter(.data$year == 2024L) %>% dplyr::pull(.data$ngr),
  "TQ4000090000",
  "A filed year should carry its own location."
)

assert_true(
  all(is.na(ktd10_crosswalk$edm_commission_date)),
  "All-NA commission-date groups should stay NA, not become Inf."
)
assert_true(
  inherits(ktd10_crosswalk$edm_commission_date, "Date"),
  "All-NA commission dates should stay typed as Date."
)

unparseable_resolver <- tibble::tibble(
  site_id = 901L,
  member_site_id = 901L,
  year = c(2022L, 2023L),
  ngr = c("TQ3000080000", "NOT A GRID REF"),
  easting = c(530000, NA_real_),
  northing = c(180000, NA_real_)
)
preferred_location <- merge_env$most_recent_representative_location(
  unparseable_resolver,
  901L,
  2023L
)
assert_identical(
  preferred_location$ngr,
  "TQ3000080000",
  "A non-NA but unparseable NGR should not shadow an older parseable one."
)

# ------------------------------------------------------------------------------
# Publish gate: a staged file lost before promote stops the publish and leaves
# canonical untouched
# ------------------------------------------------------------------------------

publish_guard_root <- tempfile("merge-individ-publish-guard-")
publish_guard_canonical <- file.path(publish_guard_root, "matched_events_annual_data")
dir.create(publish_guard_root, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(publish_guard_root, recursive = TRUE, force = TRUE), add = TRUE)

merge_env$publish_merge_outputs(smoke_result$outputs, publish_guard_canonical)
publish_guard_hashes <- tools::md5sum(file.path(
  publish_guard_canonical,
  unname(merge_env$MERGE_OUTPUT_FILES)
))

publish_guard_error <- tryCatch(
  {
    merge_env$publish_merge_outputs(
      smoke_result$outputs,
      publish_guard_canonical,
      drop_staged_file = merge_env$MERGE_OUTPUT_FILES[["near_miss_report"]]
    )
    NULL
  },
  error = identity
)
assert_true(
  inherits(publish_guard_error, "error") &&
    grepl("incomplete", conditionMessage(publish_guard_error)),
  "A staged file deleted before promote should stop the publish."
)
assert_true(
  merge_env$complete_merge_output_set(publish_guard_canonical),
  "A failed staged validation should leave the canonical output set complete."
)
assert_identical(
  tools::md5sum(file.path(
    publish_guard_canonical,
    unname(merge_env$MERGE_OUTPUT_FILES)
  )),
  publish_guard_hashes,
  "A failed staged validation should leave canonical files untouched."
)

staged_mismatch_dir <- tempfile("merge-individ-staged-mismatch-")
on.exit(unlink(staged_mismatch_dir, recursive = TRUE, force = TRUE), add = TRUE)
merge_env$write_merge_outputs(smoke_result$outputs, staged_mismatch_dir)
arrow::write_parquet(
  merge_env$MATCHED_EVENTS_PROTOTYPE,
  file.path(staged_mismatch_dir, merge_env$MERGE_OUTPUT_FILES[["matched_events"]])
)
staged_mismatch_error <- tryCatch(
  {
    merge_env$validate_staged_merge_outputs(staged_mismatch_dir, smoke_result$outputs)
    NULL
  },
  error = identity
)
assert_true(
  inherits(staged_mismatch_error, "error") &&
    grepl("row\\(s\\) on disk", conditionMessage(staged_mismatch_error)),
  "A staged file whose row count drifted from memory should fail validation."
)

# ------------------------------------------------------------------------------
# Year-filter guard: NA years hard-stop; real out-of-range years are dropped
# with a warning only
# ------------------------------------------------------------------------------

na_year_error <- tryCatch(
  {
    merge_env$filter_years_with_guard(
      tibble::tibble(year = c(2022L, NA_integer_), unique_id = c("E1", "E2")),
      2021:2024,
      "Event"
    )
    NULL
  },
  error = identity
)
assert_true(
  inherits(na_year_error, "error") &&
    grepl("NA year", conditionMessage(na_year_error)),
  "Rows with NA year should hard-stop the load path."
)

out_of_range_filtered <- merge_env$filter_years_with_guard(
  tibble::tibble(year = c(2022L, 1999L), unique_id = c("E1", "E2")),
  2021:2024,
  "Event"
)
assert_identical(
  out_of_range_filtered$unique_id,
  "E1",
  "Out-of-range non-NA years should be dropped without stopping."
)

cat("All merge_individ_annual contract tests passed.\n")
