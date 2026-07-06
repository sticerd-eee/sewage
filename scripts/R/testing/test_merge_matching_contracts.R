# ==============================================================================
# Merge Matching Contract Tests
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
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(stringr)
  library(purrr)
})

source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
source(here::here("scripts", "R", "utils", "merge_matching_utils.R"))

resolver_row <- function(site_id, year = 2024L, company = "Test Water",
                         site_name_ea = NA_character_,
                         site_name_wa_sc = NA_character_,
                         permit_reference_ea = NA_character_,
                         permit_reference_wa_sc = NA_character_,
                         activity_reference = NA_character_,
                         unique_id = NA_character_,
                         spill_hrs_ea = NA_real_,
                         spill_count_ea = NA_real_) {
  tibble(
    site_id = as.integer(site_id),
    member_site_id = as.integer(site_id),
    canonical_site_id = as.integer(site_id),
    water_company = company,
    year = as.integer(year),
    annual_site_id = as.integer(site_id),
    site_name_ea = site_name_ea,
    site_name_wa_sc = site_name_wa_sc,
    permit_reference_ea = permit_reference_ea,
    permit_reference_wa_sc = permit_reference_wa_sc,
    activity_reference = activity_reference,
    unique_id = unique_id,
    spill_hrs_ea = spill_hrs_ea,
    spill_count_ea = spill_count_ea
  )
}

event_row <- function(year = 2024L, company = "Test Water",
                      site_name_ea = NA_character_,
                      site_name_wa_sc = NA_character_,
                      permit_reference_ea = NA_character_,
                      permit_reference_wa_sc = NA_character_,
                      activity_reference = NA_character_,
                      unique_id = NA_character_,
                      event_hours = 0) {
  tibble(
    water_company = company,
    year = as.integer(year),
    site_name_ea = site_name_ea,
    site_name_wa_sc = site_name_wa_sc,
    permit_reference_ea = permit_reference_ea,
    permit_reference_wa_sc = permit_reference_wa_sc,
    activity_reference = activity_reference,
    unique_id = unique_id,
    event_hours = event_hours
  )
}

first_decision <- function(events, resolver, manual_overrides = tibble()) {
  resolve_merge_matches(events, resolver, manual_overrides = manual_overrides)[1, ]
}

# ------------------------------------------------------------------------------
# Ladder discipline and normalisation-only variants
# ------------------------------------------------------------------------------

normalisation_decision <- first_decision(
  event_row(site_name_ea = "  alpha   works "),
  resolver_row(1L, site_name_ea = "ALPHA WORKS")
)
assert_identical(
  normalisation_decision$site_id,
  1L,
  "Trim/case/whitespace normalisation should allow exact name matches."
)
assert_identical(
  normalisation_decision$match_method,
  "site_name_ea",
  "Normalised name match should record the site_name_ea rung."
)

unrelated_decision <- first_decision(
  event_row(site_name_ea = "Gamma Works"),
  resolver_row(2L, site_name_ea = "Delta Works")
)
assert_true(
  is.na(unrelated_decision$site_id),
  "Unrelated same-company-year records should not match."
)
assert_identical(
  unrelated_decision$reason,
  "no_usable_key",
  "Unrelated records should be reason-coded no_usable_key."
)

# ------------------------------------------------------------------------------
# unique_id is same-year only
# ------------------------------------------------------------------------------

cross_year_uid <- first_decision(
  event_row(year = 2024L, unique_id = "UID-1"),
  resolver_row(3L, year = 2023L, unique_id = "UID-1")
)
assert_true(
  is.na(cross_year_uid$site_id),
  "A unique_id present only in another year should not match via rung 1."
)

# ------------------------------------------------------------------------------
# Two-step permit rung
# ------------------------------------------------------------------------------

permit_only_decision <- first_decision(
  event_row(permit_reference_ea = "P-1", activity_reference = "ACT-1"),
  resolver_row(4L, permit_reference_ea = "P-1", activity_reference = NA_character_)
)
assert_identical(
  permit_only_decision$site_id,
  4L,
  "If permit+activity finds zero annual rows, permit-only should retry and resolve one works."
)
assert_identical(
  permit_only_decision$match_method,
  "permit_only",
  "Permit fallback should record permit_only."
)

permit_spans <- first_decision(
  event_row(permit_reference_ea = "P-2", activity_reference = "ACT-2"),
  bind_rows(
    resolver_row(5L, permit_reference_ea = "P-2", activity_reference = NA_character_),
    resolver_row(6L, permit_reference_ea = "P-2", activity_reference = NA_character_)
  )
)
assert_true(
  is.na(permit_spans$site_id),
  "Permit-only candidates spanning multiple works should not match."
)

# ------------------------------------------------------------------------------
# key_conflict only between uniquely resolving rungs
# ------------------------------------------------------------------------------

conflict_decision <- first_decision(
  event_row(permit_reference_ea = "P-3", unique_id = "UID-3"),
  bind_rows(
    resolver_row(7L, unique_id = "UID-3"),
    resolver_row(8L, permit_reference_ea = "P-3")
  )
)
assert_true(
  is.na(conflict_decision$site_id),
  "Conflicting unique resolutions should not match."
)
assert_identical(
  conflict_decision$reason,
  "key_conflict",
  "Conflicting unique resolutions should be reason-coded key_conflict."
)

span_does_not_conflict <- first_decision(
  event_row(site_name_ea = "Span Works", unique_id = "UID-4"),
  bind_rows(
    resolver_row(9L, site_name_ea = "Span Works", unique_id = "UID-4"),
    resolver_row(10L, site_name_ea = "Span Works")
  )
)
assert_identical(
  span_does_not_conflict$site_id,
  9L,
  "A spanning name rung should not veto an earlier unique_id match."
)
assert_true(
  is.na(span_does_not_conflict$reason),
  "A spanning lower rung should not create key_conflict."
)

# ------------------------------------------------------------------------------
# Match-to-absent
# ------------------------------------------------------------------------------

absent_decision <- first_decision(
  event_row(year = 2024L, permit_reference_ea = "P-4"),
  resolver_row(11L, year = 2023L, permit_reference_ea = "P-4")
)
assert_identical(
  absent_decision$site_id,
  11L,
  "A key resolving one works with no same-year annual row should still match."
)
assert_identical(
  absent_decision$annual_status_hint,
  "absent",
  "Match-to-absent should mark an absent annual status hint."
)

# ------------------------------------------------------------------------------
# Agreement tier
# ------------------------------------------------------------------------------

agreement_accept <- first_decision(
  event_row(site_name_ea = "Collision Works", event_hours = 100),
  bind_rows(
    resolver_row(12L, site_name_ea = "Collision Works", spill_hrs_ea = 90),
    resolver_row(13L, site_name_ea = "Collision Works", spill_hrs_ea = 50)
  )
)
assert_identical(
  agreement_accept$site_id,
  12L,
  "Agreement should accept the candidate within tolerance with a separated runner-up."
)
assert_identical(
  agreement_accept$match_method,
  "agreement",
  "Agreement matches should record the agreement method."
)
assert_true(
  abs(agreement_accept$match_quality - 0.1) < 1e-9,
  "Agreement match_quality should be the best relative error."
)

agreement_failed <- first_decision(
  event_row(site_name_ea = "Close Collision", event_hours = 100),
  bind_rows(
    resolver_row(14L, site_name_ea = "Close Collision", spill_hrs_ea = 90),
    resolver_row(15L, site_name_ea = "Close Collision", spill_hrs_ea = 85)
  )
)
assert_true(
  is.na(agreement_failed$site_id),
  "Near-equal agreement candidates should not match."
)
assert_identical(
  agreement_failed$reason,
  "agreement_failed",
  "Near-equal agreement candidates should be reason-coded agreement_failed."
)

agreement_uninformative <- first_decision(
  event_row(site_name_ea = "Zero Collision", event_hours = 0),
  bind_rows(
    resolver_row(16L, site_name_ea = "Zero Collision", spill_hrs_ea = 0),
    resolver_row(17L, site_name_ea = "Zero Collision", spill_hrs_ea = 0)
  )
)
assert_identical(
  agreement_uninformative$reason,
  "agreement_uninformative",
  "All-zero agreement candidates should be reason-coded agreement_uninformative."
)

agreement_na <- first_decision(
  event_row(site_name_ea = "NA Collision", event_hours = 10),
  bind_rows(
    resolver_row(18L, site_name_ea = "NA Collision", spill_hrs_ea = NA_real_),
    resolver_row(19L, site_name_ea = "NA Collision", spill_hrs_ea = NA_real_)
  )
)
assert_true(
  is.na(agreement_na$site_id),
  "NA candidate metrics should not match."
)
assert_true(
  agreement_na$reason %in% c("agreement_failed", "name_spans_works"),
  "NA candidate metrics should be reason-coded without crashing."
)

# ------------------------------------------------------------------------------
# Manual overrides
# ------------------------------------------------------------------------------

override_events <- event_row(site_name_ea = "Manual Needed")
override_resolver <- resolver_row(20L, site_name_ea = "Known Works")
manual_override <- override_events %>%
  mutate(site_id = 20L)
override_decision <- first_decision(
  override_events,
  override_resolver,
  manual_overrides = manual_override
)
assert_identical(
  override_decision$site_id,
  20L,
  "Manual override should apply to an otherwise-unmatched tuple."
)
assert_identical(
  override_decision$match_method,
  "manual_override",
  "Manual override matches should record manual_override."
)

assert_error_matching(
  resolve_merge_matches(
    override_events,
    override_resolver,
    manual_overrides = override_events %>% mutate(site_id = 999L)
  ),
  "unknown works site_id",
  "Manual overrides naming a site_id absent from the register should fail preflight."
)

# ------------------------------------------------------------------------------
# Row accounting
# ------------------------------------------------------------------------------

row_accounting_events <- bind_rows(
  event_row(site_name_ea = "Alpha", event_hours = 1),
  event_row(site_name_ea = "Alpha", event_hours = 2),
  event_row(site_name_ea = "Beta", event_hours = 3)
)
row_accounting_resolver <- bind_rows(
  resolver_row(30L, site_name_ea = "Alpha"),
  resolver_row(31L, site_name_ea = "Beta")
)
row_accounting_decisions <- resolve_merge_matches(
  row_accounting_events,
  row_accounting_resolver
)
assert_identical(
  nrow(row_accounting_decisions),
  2L,
  "Every distinct input tuple should appear exactly once in the decisions output."
)

cat("All merge matching contract tests passed.\n")
