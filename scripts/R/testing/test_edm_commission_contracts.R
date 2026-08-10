# ==============================================================================
# EDM Commission Evidence Contract Tests
# ==============================================================================
#
# Runnable standalone via plain Rscript; exits non-zero on the first failure.
#
# ==============================================================================

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
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

assert_error <- function(code, pattern, message) {
  error <- tryCatch({
    force(code)
    NULL
  }, error = identity)
  if (is.null(error) || !grepl(pattern, conditionMessage(error), fixed = TRUE)) {
    stop(message, call. = FALSE)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(glue)
  library(here)
  library(logger)
  library(readr)
  library(rnrfa)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source(here::here("scripts", "R", "utils", "edm_commission_utils.R"))

assert_identical(
  EDM_COMMISSION_OBSERVATION_CLASSES,
  c(
    "missing", "actual", "future_deadline", "before_2016",
    "not_commissioned_as_of_report", "not_feasible",
    "invalid_placeholder", "unparseable"
  ),
  "Observation classes should remain a closed vocabulary."
)
assert_identical(
  EDM_COMMISSION_RESOLUTION_STATUSES,
  c(
    "resolved", "missing", "future_only", "not_commissioned",
    "not_feasible", "actual_state_conflict", "conflicting_actual_dates",
    "before_2016", "invalid_placeholder", "unparseable"
  ),
  "Canonical statuses should remain a closed vocabulary."
)
assert_identical(
  EDM_COMMISSION_PRECISIONS,
  c("day", "month", "year", "vague", "unknown", "conflict"),
  "Canonical precision should remain a closed vocabulary."
)

fixture_path <- here::here(
  "scripts", "R", "testing", "fixtures",
  "edm_commission_text_expectations.csv"
)
fixture <- read.csv(fixture_path, stringsAsFactors = FALSE, na.strings = "")

# The fixture is reviewed evidence, not parser-derived expected output.
assert_identical(nrow(fixture), 63L, "The reviewed fixture should contain all 63 current forms.")
assert_true(
  !anyDuplicated(fixture$normalized_text),
  "Each normalized non-empty form should appear exactly once in the fixture."
)
assert_identical(
  fixture$normalized_text,
  normalise_commission_text(fixture$source_text),
  "Fixture normalized text should match the public normalization contract."
)

classified <- classify_commission_observations(
  fixture$source_text,
  fixture$report_year
)
actual_fixture_view <- data.frame(
  normalized_text = classified$normalized_text,
  observation_class = classified$observation_class,
  candidate_start = as.character(classified$candidate_start),
  candidate_end = as.character(classified$candidate_end),
  candidate_precision = classified$candidate_precision,
  stringsAsFactors = FALSE
)
expected_fixture_view <- fixture[c(
  "normalized_text", "observation_class", "candidate_start",
  "candidate_end", "candidate_precision"
)]
expected_fixture_view$candidate_start[
  is.na(expected_fixture_view$candidate_start)
] <- NA_character_
expected_fixture_view$candidate_end[
  is.na(expected_fixture_view$candidate_end)
] <- NA_character_
assert_identical(
  actual_fixture_view,
  expected_fixture_view,
  "Every reviewed current source-text form should retain its declared semantics."
)

# Locale independence, encoded dates, placeholders, and temporal classification.
may <- classify_commission_observations("May 2021", 2021L)
assert_identical(as.character(may$candidate_start), "2021-05-01", "May should parse explicitly.")
assert_identical(may$candidate_precision, "month", "May should retain month precision.")

serial <- classify_commission_observations("44531", 2021L)
assert_identical(as.character(serial$candidate_start), "2021-12-01", "Excel serial 44531 should decode explicitly.")
assert_identical(serial$candidate_precision, "day", "Excel serials should carry day precision.")

special <- classify_commission_observations(
  c(
    "0", "an unsupported commission label", "December 2023", "2023",
    "asset note 2021", "Commissioned in 1800", "99999"
  ),
  c(2021L, 2021L, 2022L, 2022L, 2021L, 2021L, 2021L)
)
assert_identical(
  special$observation_class,
  c(
    "invalid_placeholder", "unparseable", "future_deadline",
    "future_deadline", "unparseable", "unparseable", "unparseable"
  ),
  paste(
    "Invalid placeholders, unsupported substrings, unbounded serials, and",
    "bare future dates should remain distinct."
  )
)

# Compatible actual evidence refines; incompatible actual evidence fails closed.
compatible <- resolve_commission_history(
  c("Commissioned in 2021", "Mar 2021"),
  c(2021L, 2022L)
)
assert_identical(as.character(compatible$edm_commission_date), "2021-03-01", "Month evidence should refine its containing year.")
assert_identical(compatible$edm_commission_date_precision, "month", "Compatible refinement should retain month precision.")
assert_identical(compatible$edm_commission_resolution_status, "resolved", "Compatible actual evidence should resolve.")

for (actual_conflict in list(
  list(c("Mar 2021", "Apr 2021"), c(2021L, 2022L)),
  list(c("Commissioned in 2020", "Commissioned in 2021"), c(2021L, 2022L))
)) {
  result <- resolve_commission_history(actual_conflict[[1]], actual_conflict[[2]])
  assert_true(is.na(result$edm_commission_date), "Conflicting actual dates should remain dateless.")
  assert_identical(result$edm_commission_date_precision, "conflict", "Conflicting actual dates should carry conflict precision.")
  assert_identical(result$edm_commission_resolution_status, "conflicting_actual_dates", "Incompatible actual intervals should be explicit.")
}

# Chronology: later actual evidence supersedes earlier state; later/same-year state conflicts.
later_actual_cases <- list(
  list(c("To be installed by Dec 2023", "Dec 2023"), c(2021L, 2023L), "2023-12-01"),
  list(c("To be installed by Dec 2023", "Apr 2022"), c(2021L, 2022L), "2022-04-01"),
  list(c("To be installed by Dec 2022", "To be installed by Dec 2023", "Apr 2024"), c(2021L, 2022L, 2024L), "2024-04-01"),
  list(c("Installed but not yet commissioned", "Apr 2022"), c(2021L, 2022L), "2022-04-01"),
  list(c("EDM not technically feasible at this overflow", "Apr 2022"), c(2021L, 2022L), "2022-04-01")
)
for (case in later_actual_cases) {
  result <- resolve_commission_history(case[[1]], case[[2]])
  assert_identical(as.character(result$edm_commission_date), case[[3]], "Later actual evidence should supersede every earlier state.")
  assert_identical(result$edm_commission_resolution_status, "resolved", "Later actual evidence should resolve.")
}

actual_state_cases <- list(
  list(c("Mar 2021", "To be installed by Dec 2023"), c(2021L, 2022L)),
  list(c("Mar 2021", "To be installed by Dec 2023"), c(2021L, 2021L)),
  list(c("Mar 2021", "Installed but not yet commissioned"), c(2021L, 2022L)),
  list(c("Mar 2021", "EDM not technically feasible at this overflow"), c(2021L, 2022L))
)
for (case in actual_state_cases) {
  result <- resolve_commission_history(case[[1]], case[[2]])
  assert_true(is.na(result$edm_commission_date), "Actual/state contradictions should remain dateless.")
  assert_identical(result$edm_commission_date_precision, "conflict", "Actual/state contradictions should carry conflict precision.")
  assert_identical(result$edm_commission_resolution_status, "actual_state_conflict", "Actual/state chronology should fail closed.")
}

# Non-date status vocabulary and pre-2016 compatibility.
status_cases <- list(
  list(c("To be installed by Dec 2023"), 2021L, "future_only", "unknown"),
  list(c(NA_character_, "  "), c(2021L, 2022L), "missing", "unknown"),
  list("Commissioned pre-2016", 2021L, "before_2016", "vague"),
  list("Installed but not yet commissioned", 2021L, "not_commissioned", "unknown"),
  list("EDM not technically feasible at this overflow", 2021L, "not_feasible", "unknown"),
  list("0", 2021L, "invalid_placeholder", "unknown"),
  list("an unsupported commission label", 2021L, "unparseable", "unknown")
)
for (case in status_cases) {
  result <- resolve_commission_history(case[[1]], case[[2]])
  assert_true(is.na(result$edm_commission_date), "Unresolved histories should remain dateless.")
  assert_identical(result$edm_commission_resolution_status, case[[3]], "The canonical unresolved status should be preserved.")
  assert_identical(result$edm_commission_date_precision, case[[4]], "The canonical unresolved precision should be valid.")
}

same_year_states <- resolve_commission_history(
  c(
    "To be installed by Dec 2023",
    "Installed but not yet commissioned",
    "EDM not technically feasible at this overflow"
  ),
  rep(2021L, 3L)
)
assert_identical(
  same_year_states$edm_commission_resolution_status,
  "not_feasible",
  "Same-year non-actual states should use the conservative order-independent precedence."
)

pre_compatible <- resolve_commission_history(
  c("Commissioned pre-2016", "Commissioned in 2015"),
  c(2021L, 2022L)
)
assert_identical(as.character(pre_compatible$edm_commission_date), "2015-01-01", "Pre-2016 should be compatible with dated pre-2016 actual evidence.")
pre_conflict <- resolve_commission_history(
  c("Commissioned pre-2016", "Commissioned in 2016"),
  c(2021L, 2022L)
)
assert_identical(pre_conflict$edm_commission_resolution_status, "conflicting_actual_dates", "Pre-2016 should conflict with actual evidence from 2016 onward.")

# Resolution is row-order independent and invariant validation fails closed.
texts <- c("To be installed by Dec 2023", "Commissioned in 2021", "Mar 2021")
years <- c(2020L, 2021L, 2022L)
assert_identical(
  resolve_commission_history(texts, years),
  resolve_commission_history(rev(texts), rev(years)),
  "History resolution should not depend on input row order."
)

assert_error(
  validate_commission_observations(data.frame(
    observation_class = "actual",
    candidate_start = as.Date(NA),
    candidate_end = as.Date(NA),
    candidate_precision = "unknown"
  )),
  "Invalid commission observation combination",
  "Actual evidence without a complete dated interval should fail validation."
)
assert_error(
  validate_commission_resolution(data.frame(
    edm_commission_date = as.Date("2021-01-01"),
    edm_commission_date_precision = "unknown",
    edm_commission_resolution_status = "resolved"
  )),
  "Invalid commission resolution combination",
  "A resolved date with unknown precision should fail validation."
)
assert_error(
  validate_commission_resolution(data.frame(
    edm_commission_date = as.Date(NA),
    edm_commission_date_precision = "unknown",
    edm_commission_resolution_status = "invented_status"
  )),
  "Unknown commission resolution status",
  "Statuses outside the closed vocabulary should fail validation."
)

# The production entry seam delegates without collapsing same-year evidence.
script_env <- new.env(parent = globalenv())
source(
  here::here(
    "scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"
  ),
  local = script_env
)
entry_data <- tibble(
  site_id = c(1L, 1L, 2L, 2L),
  year = c(2021L, 2022L, 2021L, 2021L),
  water_company = "Test Water",
  outlet_discharge_ngr = "TQ3000080000",
  edm_commission_date = c(
    "Commissioned in 2021", "Mar 2021",
    "Mar 2021", "To be installed by Dec 2023"
  ),
  edm_operation_percent = NA_real_,
  edm_operation_reason = NA_character_
)
entry_summary <- script_env$summarise_site_metadata(entry_data)
entry_resolved <- entry_summary[entry_summary$site_id == 1L, ]
entry_conflict <- entry_summary[entry_summary$site_id == 2L, ]
assert_identical(
  as.character(entry_resolved$edm_commission_date),
  "2021-03-01",
  "The entry seam should expose the classified resolver date."
)
assert_identical(
  entry_resolved$edm_commission_date_precision,
  "month",
  "The entry seam should expose independent precision."
)
assert_identical(
  entry_resolved$edm_commission_resolution_status,
  "resolved",
  "The entry seam should expose independent resolution status."
)
assert_identical(
  entry_conflict$edm_commission_resolution_status,
  "actual_state_conflict",
  "The entry seam should preserve same-year actual/state conflicts before metadata collapse."
)

# Runtime source coverage is reconstructed from annual returns and the lookup.
annual_data <- arrow::read_parquet(
  here::here("data", "processed", "annual_return_edm.parquet")
)
lookup_data <- arrow::read_parquet(
  here::here("data", "processed", "annual_return_lookup.parquet")
)
runtime <- build_commission_runtime_enumeration(annual_data, lookup_data)
assert_commission_fixture_coverage(runtime, fixture)
assert_identical(
  sort(unique(runtime$normalized_text)),
  sort(fixture$normalized_text),
  "Runtime enumeration should contain no unreviewed or silently dropped forms."
)
assert_true(
  all(runtime$n_canonical_sites > 0L) && all(nzchar(runtime$canonical_site_examples)),
  "Runtime evidence should include mapped canonical counts and examples."
)

cat("All EDM commission contract tests passed under LC_TIME=", Sys.getlocale("LC_TIME"), ".\n", sep = "")
