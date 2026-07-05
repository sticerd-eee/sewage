# ==============================================================================
# Merge Output Assembly Contract Tests
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

assert_error_matching <- function(expr, pattern, message) {
  err <- tryCatch(
    {
      force(expr)
      NULL
    },
    error = identity
  )
  assert_true(
    inherits(err, "error") && grepl(pattern, conditionMessage(err)),
    paste0(
      message,
      if (inherits(err, "error")) {
        paste0("\nGot error: ", conditionMessage(err))
      } else {
        "\nNo error was raised."
      }
    )
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tibble)
})

source(here::here("scripts", "R", "utils", "merge_outputs_utils.R"))

membership <- tibble(
  site_id = c(1L, 2L),
  member_site_id = c(1L, 2L),
  water_company = "Test Water",
  component_id = c(1L, 2L),
  n_members = c(1L, 1L),
  site_id_members = c("1", "2")
)

resolver <- tibble(
  annual_row_id = 1:4,
  site_id = c(1L, 1L, 1L, 2L),
  member_site_id = c(1L, 1L, 1L, 2L),
  canonical_site_id = c(1L, 1L, 1L, 2L),
  water_company = "Test Water",
  year = c(2021L, 2022L, 2023L, 2021L),
  annual_site_id = 1:4,
  site_name_ea = c("Alpha", "Alpha", "Alpha", "Beta"),
  site_name_ea_norm = c("ALPHA", "ALPHA", "ALPHA", "BETA"),
  site_name_wa_sc = NA_character_,
  permit_reference_ea = NA_character_,
  permit_reference_ea_norm = NA_character_,
  permit_reference_wa_sc = NA_character_,
  activity_reference = NA_character_,
  outlet_discharge_ngr = c("TQ3000080000", "TQ3000080000", "TQ3000080000", "TQ3100080000"),
  ngr = c("TQ3000080000", "TQ3000080000", "TQ3000080000", "TQ3100080000"),
  easting = c(530000, 530000, 530000, 531000),
  northing = c(180000, 180000, 180000, 180000),
  spill_hrs_ea = c(0, 12, NA, 24),
  spill_count_ea = c(0, 3, NA, 4),
  edm_operation_percent = c(100, 95, NA, 90),
  no_full_years_edm_data = c(1, 2, NA, 1),
  edm_commission_date = as.Date(c("2020-01-01", "2020-01-01", "2020-01-01", "2021-01-01"))
)

events <- tibble(
  water_company = "Test Water",
  year = c(2021L, 2022L, 2023L),
  site_name_ea = c("Alpha", "Alpha", "Missing"),
  site_name_wa_sc = NA_character_,
  permit_reference_ea = NA_character_,
  permit_reference_wa_sc = NA_character_,
  activity_reference = NA_character_,
  unique_id = c("E1", "E2", "E3"),
  start_time_og = c(
    "2021-01-01T00:00:00Z",
    "2022-01-01T00:00:00Z",
    "2023-01-01T00:00:00Z"
  ),
  end_time_og = c(
    "2021-01-01T01:00:00Z",
    "2022-01-01T01:00:00Z",
    "2023-01-01T01:00:00Z"
  ),
  start_time = as.POSIXct(c(
    "2021-01-01 00:00:00",
    "2022-01-01 00:00:00",
    "2023-01-01 00:00:00"
  ), tz = "UTC"),
  end_time = as.POSIXct(c(
    "2021-01-01 01:00:00",
    "2022-01-01 01:00:00",
    "2023-01-01 01:00:00"
  ), tz = "UTC")
)

decisions <- tibble(
  water_company = "Test Water",
  year = c(2021L, 2022L, 2023L),
  site_name_ea = c("Alpha", "Alpha", "Missing"),
  site_name_wa_sc = NA_character_,
  permit_reference_ea = NA_character_,
  permit_reference_wa_sc = NA_character_,
  activity_reference = NA_character_,
  unique_id = c("E1", "E2", "E3"),
  site_id = c(1L, 1L, NA_integer_),
  match_method = c("site_name_ea", "agreement", NA_character_),
  match_quality = c(1, 0.1, NA_real_),
  reason = c(NA_character_, NA_character_, "no_usable_key"),
  annual_status_hint = c(NA_character_, NA_character_, NA_character_)
)

register_near_misses <- tibble(
  water_company = "Test Water",
  site_name_ea_norm = "ALPHA",
  site_id_from = 1L,
  site_id_to = 2L,
  distance_m = 400,
  reason = "same_name_distance_250m_to_1km"
)

outputs <- assemble_merge_outputs(
  events = events,
  membership = membership,
  resolver = resolver,
  decisions = decisions,
  register_near_misses = register_near_misses,
  years = 2021:2024
)

# ------------------------------------------------------------------------------
# Crosswalk status taxonomy and full works x years grid
# ------------------------------------------------------------------------------

crosswalk <- outputs$site_works_crosswalk
assert_identical(
  nrow(crosswalk),
  8L,
  "Crosswalk should contain one row per works x year."
)
assert_identical(
  crosswalk %>% filter(site_id == 1L, year == 2021L) %>% pull(annual_status),
  "reported_zero",
  "0/0 annual metrics should produce reported_zero."
)
assert_identical(
  crosswalk %>% filter(site_id == 1L, year == 2022L) %>% pull(annual_status),
  "reported_positive",
  "Positive annual metrics should produce reported_positive."
)
assert_identical(
  crosswalk %>% filter(site_id == 1L, year == 2023L) %>% pull(annual_status),
  "reported_na",
  "NA annual metrics should produce reported_na."
)
assert_identical(
  crosswalk %>% filter(site_id == 1L, year == 2024L) %>% pull(annual_status),
  "absent",
  "Missing annual rows should produce absent."
)
assert_identical(
  crosswalk %>% filter(site_id == 1L, year == 2024L) %>% pull(ngr),
  "TQ3000080000",
  "Absent years should carry the representative member's most recent non-NA NGR."
)

# ------------------------------------------------------------------------------
# Row accounting and output semantics
# ------------------------------------------------------------------------------

assert_identical(
  nrow(outputs$matched_events),
  2L,
  "Matched-events output should contain only real matched event rows."
)
assert_identical(
  nrow(outputs$events_unmatched),
  1L,
  "Events-unmatched output should contain the unmatched event rows."
)
assert_identical(
  outputs$events_unmatched$reason,
  "no_usable_key",
  "Unmatched events should carry their decision reason."
)
assert_true(
  !("site_metadata.parquet" %in% unname(MERGE_OUTPUT_FILES)),
  "site_metadata.parquet should not be one of the declared CH5 outputs."
)

assert_true(
  nrow(outputs$annual_unmatched) >= 1L,
  "Reported-positive works-years with no matched events should appear in annual_unmatched."
)
assert_identical(
  outputs$site_works_crosswalk %>%
    filter(site_id == 1L, year == 2023L) %>%
    pull(spill_hrs_ea),
  NA_real_,
  "reported_na hours should remain NA rather than being coerced to zero."
)
assert_identical(
  outputs$matched_events$start_time_og,
  c("2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z"),
  "Native *_og timestamp columns should remain character strings."
)
assert_identical(
  nrow(outputs$near_miss_report),
  1L,
  "Register near misses should flow into near_miss_report."
)

assert_error_matching(
  assemble_merge_outputs(
    events = events,
    membership = membership,
    resolver = resolver,
    decisions = bind_rows(decisions, decisions[1, ]),
    years = 2021:2024
  ),
  "events != matched",
  "Duplicating a tuple decision before assembly should trip row accounting."
)

# ------------------------------------------------------------------------------
# Schema parity between empty and populated outputs
# ------------------------------------------------------------------------------

empty_outputs <- empty_merge_outputs()
for (name in names(empty_outputs)) {
  assert_identical(
    names(empty_outputs[[name]]),
    names(outputs[[name]]),
    paste0("Empty and populated ", name, " outputs should have identical columns.")
  )
  assert_identical(
    vapply(empty_outputs[[name]], typeof, character(1)),
    vapply(outputs[[name]], typeof, character(1)),
    paste0("Empty and populated ", name, " outputs should have identical column types.")
  )
}

# ------------------------------------------------------------------------------
# Publish validation and atomic promotion invariant
# ------------------------------------------------------------------------------

null_outputs <- outputs
null_outputs["matched_events"] <- list(NULL)
assert_error_matching(
  validate_publishable_merge_outputs(null_outputs),
  "NULL output",
  "Publish validation should reject NULL outputs."
)
assert_error_matching(
  {
    empty_matched_outputs <- outputs
    empty_matched_outputs$matched_events <- outputs$matched_events[0, ]
    validate_publishable_merge_outputs(empty_matched_outputs)
  },
  "empty crosswalk or matched-events",
  "Publish validation should reject an empty matched-events output."
)

publish_root <- tempfile("merge-output-publish-")
canonical_dir <- file.path(publish_root, "matched_events_annual_data")
dir.create(publish_root, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(publish_root, recursive = TRUE, force = TRUE), add = TRUE)

publish_merge_outputs(outputs, canonical_dir)
assert_true(
  complete_merge_output_set(canonical_dir),
  "A normal publish should leave a complete canonical output set."
)
assert_true(
  !file.exists(file.path(canonical_dir, "site_metadata.parquet")),
  "A normal publish should not retain the retired site_metadata.parquet output."
)

crash_root <- tempfile("merge-output-crash-")
crash_canonical <- file.path(crash_root, "matched_events_annual_data")
dir.create(crash_root, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(crash_root, recursive = TRUE, force = TRUE), add = TRUE)
write_merge_outputs(outputs, crash_canonical)

crash_script <- tempfile("merge-output-crash-script-", fileext = ".R")
writeLines(
  c(
    "suppressPackageStartupMessages({library(arrow); library(dplyr); library(tibble)})",
    paste0(
      "source(",
      dQuote(here::here("scripts", "R", "utils", "merge_outputs_utils.R")),
      ")"
    ),
    paste0("staging <- ", dQuote(file.path(crash_root, "staging"))),
    paste0("canonical <- ", dQuote(crash_canonical)),
    "outputs <- empty_merge_outputs()",
    "outputs$site_works_crosswalk <- tibble(site_id=1L, year=2024L, water_company='Test Water', site_id_members='1', n_outlets=1L, n_outlets_reporting=1L, annual_status='reported_positive', spill_hrs_ea=1, spill_count_ea=1, ngr='TQ3000080000', easting=530000, northing=180000, edm_operation_percent=100, no_full_years_edm_data=1, edm_commission_date=as.Date('2024-01-01'), matched_event_count=1L, match_methods='site_name_ea')",
    "outputs$matched_events <- tibble(water_company='Test Water', year=2024L, site_name_ea='Alpha', site_name_wa_sc=NA_character_, permit_reference_ea=NA_character_, start_time_og='2024-01-01T00:00:00Z', end_time_og='2024-01-01T01:00:00Z', permit_reference_wa_sc=NA_character_, activity_reference=NA_character_, site_code=NA_character_, asset_type=NA_character_, unique_id='E1', event_duration_in_hours=1, new_unqiue_id=NA_character_, start_time=as.POSIXct('2024-01-01 00:00:00', tz='UTC'), end_time=as.POSIXct('2024-01-01 01:00:00', tz='UTC'), site_id=1L, match_method='site_name_ea', match_quality=1, annual_status='reported_positive', spill_hrs_ea=1, spill_count_ea=1, ngr='TQ3000080000')",
    "outputs$events_unmatched <- EVENTS_UNMATCHED_PROTOTYPE",
    "outputs$annual_unmatched <- ANNUAL_UNMATCHED_PROTOTYPE",
    "outputs$near_miss_report <- NEAR_MISS_REPORT_PROTOTYPE",
    "validate_publishable_merge_outputs(outputs)",
    "write_merge_outputs(outputs, staging)",
    "promote_staged_merge_outputs(staging, canonical, crash_at='after_backup')"
  ),
  crash_script
)

crash_run <- suppressWarnings(system2("Rscript", crash_script, stdout = TRUE, stderr = TRUE))
crash_status <- attr(crash_run, "status")
assert_true(
  !is.null(crash_status) && crash_status != 0L,
  "The subprocess promotion crash should exit non-zero."
)
assert_true(
  complete_merge_output_set(crash_canonical) ||
    complete_merge_output_set(paste0(crash_canonical, ".prev")),
  "After a mid-promotion crash, a complete output set should exist at canonical or .prev."
)
if (dir.exists(crash_canonical)) {
  assert_true(
    complete_merge_output_set(crash_canonical),
    "If canonical exists after a crash, it should be a complete output set."
  )
}

cat("All merge output contract tests passed.\n")
