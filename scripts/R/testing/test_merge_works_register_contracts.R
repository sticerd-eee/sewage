# ==============================================================================
# Merge Works Register Contract Tests
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

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

source(here::here("scripts", "R", "utils", "ngr_utils.R"))
source(here::here("scripts", "R", "utils", "merge_works_register_utils.R"))

make_annual_row <- function(site_id, year = 2024L, company = "Test Water",
                            name = "Alpha Works", permit = NA_character_,
                            ngr = NA_character_, activity = NA_character_,
                            annual_site_id = site_id) {
  tibble(
    canonical_site_id = as.integer(site_id),
    annual_site_id = as.integer(annual_site_id),
    year = as.integer(year),
    water_company = company,
    site_name_ea = name,
    site_name_wa_sc = NA_character_,
    permit_reference_ea = permit,
    permit_reference_wa_sc = NA_character_,
    activity_reference = activity,
    outlet_discharge_ngr = ngr
  )
}

membership_pairs <- function(register) {
  register$membership %>%
    select(site_id, member_site_id, water_company) %>%
    arrange(site_id, member_site_id, water_company)
}

member_representative <- function(register, member_site_id) {
  register$membership %>%
    filter(.data$member_site_id == !!member_site_id) %>%
    pull(.data$site_id)
}

# ------------------------------------------------------------------------------
# Same company + same name + identical permit merges two site IDs
# ------------------------------------------------------------------------------

permit_fixture <- bind_rows(
  make_annual_row(10L, permit = "EA/PERMIT/1"),
  make_annual_row(20L, permit = "EA/PERMIT/1")
)
permit_register <- build_works_register(permit_fixture)

assert_identical(
  member_representative(permit_register, 20L),
  10L,
  "Same company + same normalised name + identical permit should merge to the smallest representative site_id."
)
assert_true(
  "permit_reference_ea" %in% permit_register$edges$justification,
  "Permit-corroborated edges should record permit_reference_ea justification."
)

# ------------------------------------------------------------------------------
# Same-name NGR thresholds: 200m merge, 400m near-miss, 1.2km ignored
# ------------------------------------------------------------------------------

ngr_merge <- build_works_register(bind_rows(
  make_annual_row(30L, permit = NA_character_, ngr = "TQ3000080000"),
  make_annual_row(31L, permit = NA_character_, ngr = "TQ3020080000")
))
assert_identical(
  member_representative(ngr_merge, 31L),
  30L,
  "Same-name NGRs 200m apart should merge."
)
assert_true(
  "ngr_distance" %in% ngr_merge$edges$justification,
  "NGR-corroborated edges should record ngr_distance justification."
)

ngr_near_miss <- build_works_register(bind_rows(
  make_annual_row(40L, permit = NA_character_, ngr = "TQ3000080000"),
  make_annual_row(41L, permit = NA_character_, ngr = "TQ3040080000")
))
assert_identical(
  member_representative(ngr_near_miss, 41L),
  41L,
  "Same-name NGRs 400m apart should not merge."
)
assert_identical(
  nrow(ngr_near_miss$near_misses),
  1L,
  "Same-name NGRs 400m apart should land in the near-miss report."
)

ngr_far <- build_works_register(bind_rows(
  make_annual_row(50L, permit = NA_character_, ngr = "TQ3000080000"),
  make_annual_row(51L, permit = NA_character_, ngr = "TQ3120080000")
))
assert_identical(
  member_representative(ngr_far, 51L),
  51L,
  "Same-name NGRs 1.2km apart should not merge."
)
assert_identical(
  nrow(ngr_far$near_misses),
  0L,
  "Same-name NGRs 1.2km apart should not appear in the near-miss report."
)

# ------------------------------------------------------------------------------
# Identical permit + different names does not merge: no permit-only edges
# ------------------------------------------------------------------------------

permit_only_fixture <- bind_rows(
  make_annual_row(60L, name = "Alpha Works", permit = "EA/PERMIT/2"),
  make_annual_row(61L, name = "Beta Works", permit = "EA/PERMIT/2")
)
permit_only_register <- build_works_register(permit_only_fixture)
assert_identical(
  member_representative(permit_only_register, 61L),
  61L,
  "Identical permit_reference_ea with different site_name_ea should not create a permit-only edge."
)
assert_identical(
  nrow(permit_only_register$edges),
  0L,
  "Permit-only evidence should not create an edge."
)

# ------------------------------------------------------------------------------
# One-year name variant cannot split a works once a corroborated edge exists
# ------------------------------------------------------------------------------

variant_fixture <- bind_rows(
  make_annual_row(70L, year = 2021L, name = "Alpha Works", permit = "EA/PERMIT/3"),
  make_annual_row(71L, year = 2021L, name = "Alpha Works", permit = "EA/PERMIT/3"),
  make_annual_row(70L, year = 2022L, name = "Alpha Works", permit = "EA/PERMIT/3"),
  make_annual_row(71L, year = 2022L, name = "Alpha Outfall", permit = "EA/PERMIT/3")
)
variant_all <- build_works_register(variant_fixture)
variant_one_year <- build_works_register(filter(variant_fixture, year == 2021L))

assert_identical(
  membership_pairs(variant_all),
  membership_pairs(variant_one_year),
  "A one-year name variant should not split a works after an earlier corroborated same-name edge exists."
)

# ------------------------------------------------------------------------------
# Representative is deterministic under shuffled input row order
# ------------------------------------------------------------------------------

shuffle_fixture <- bind_rows(
  make_annual_row(90L, permit = "EA/PERMIT/4"),
  make_annual_row(88L, permit = "EA/PERMIT/4"),
  make_annual_row(89L, permit = "EA/PERMIT/4")
)
set.seed(20260705)
shuffle_a <- build_works_register(shuffle_fixture)
shuffle_b <- build_works_register(slice_sample(shuffle_fixture, prop = 1))
assert_identical(
  membership_pairs(shuffle_a),
  membership_pairs(shuffle_b),
  "Works membership and representative site_id should be deterministic under shuffled input."
)
assert_true(
  all(shuffle_a$membership$site_id == 88L),
  "The representative site_id should be the smallest member site_id."
)

# ------------------------------------------------------------------------------
# Monitor-multiple rows collapse without error
# ------------------------------------------------------------------------------

monitor_multiple <- build_works_register(bind_rows(
  make_annual_row(100L, permit = "EA/PERMIT/5", ngr = "TQ3000080000"),
  make_annual_row(101L, permit = "EA/PERMIT/5", ngr = "TQ3000080000")
))
assert_identical(
  member_representative(monitor_multiple, 101L),
  100L,
  "Identifier-identical monitor-multiple rows should collapse into one works."
)

# ------------------------------------------------------------------------------
# Unparseable or NA NGR only contributes via the permit corroborator
# ------------------------------------------------------------------------------

bad_ngr_no_permit <- build_works_register(bind_rows(
  make_annual_row(110L, permit = NA_character_, ngr = "NOT IN CONSENTS DATABASE"),
  make_annual_row(111L, permit = NA_character_, ngr = NA_character_)
))
assert_identical(
  member_representative(bad_ngr_no_permit, 111L),
  111L,
  "Unparseable or NA NGRs without permit corroboration should not merge."
)

bad_ngr_with_permit <- build_works_register(bind_rows(
  make_annual_row(120L, permit = "EA/PERMIT/6", ngr = "NOT IN CONSENTS DATABASE"),
  make_annual_row(121L, permit = "EA/PERMIT/6", ngr = NA_character_)
))
assert_identical(
  member_representative(bad_ngr_with_permit, 121L),
  120L,
  "Unparseable or NA NGRs should still merge through permit corroboration."
)

# ------------------------------------------------------------------------------
# Empty input yields schema parity with populated output
# ------------------------------------------------------------------------------

empty_register <- build_works_register(permit_fixture[0, ])
populated_register <- build_works_register(permit_fixture)

for (name in c("membership", "resolver", "edges", "near_misses")) {
  assert_identical(
    names(empty_register[[name]]),
    names(populated_register[[name]]),
    paste0("Empty and populated ", name, " outputs should have identical columns.")
  )
  assert_identical(
    vapply(empty_register[[name]], typeof, character(1)),
    vapply(populated_register[[name]], typeof, character(1)),
    paste0("Empty and populated ", name, " outputs should have identical column types.")
  )
}

# ------------------------------------------------------------------------------
# Real-input smoke: edge counts by justification on regenerated lookup inputs
# ------------------------------------------------------------------------------

annual_path <- here::here("data", "processed", "annual_return_edm.parquet")
lookup_path <- here::here("data", "processed", "annual_return_lookup.parquet")

if (file.exists(annual_path) && file.exists(lookup_path)) {
  real_register <- build_works_register(
    arrow::read_parquet(annual_path),
    arrow::read_parquet(lookup_path)
  )
  assert_identical(
    nrow(real_register$membership),
    15430L,
    "Real-input register should contain one membership row per canonical lookup site_id."
  )
  assert_true(
    nrow(real_register$edges) > 0,
    "Real-input register should produce at least one corroborated works edge."
  )
  cat("Real-input works-register edge counts by justification:\n")
  print(real_register$edge_summary)
  cat("Real-input works-register near-miss rows: ", nrow(real_register$near_misses), "\n", sep = "")
}

cat("All merge works-register contract tests passed.\n")
