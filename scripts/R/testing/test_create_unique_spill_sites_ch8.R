# ==============================================================================
# Create Unique Spill Sites CH8 Contract Tests
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
  library(dplyr)
  library(glue)
  library(here)
  library(logger)
  library(purrr)
  library(readr)
  library(rnrfa)
  library(stringr)
  library(tibble)
  library(tidyr)
})

script_env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"),
  local = script_env
)

make_crosswalk <- function(site_id,
                           annual_status,
                           spill_hrs_ea,
                           spill_count_ea,
                           water_company = "Test Water",
                           ngr = "TQ 30000 80000",
                           easting = 530000,
                           northing = 180000) {
  years <- 2021:2024
  tibble(
    site_id = as.integer(site_id),
    year = years,
    water_company = water_company,
    site_id_members = as.character(site_id),
    n_outlets = 1L,
    n_outlets_reporting = if_else(annual_status == "absent", 0L, 1L),
    annual_status = annual_status,
    spill_hrs_ea = spill_hrs_ea,
    spill_count_ea = spill_count_ea,
    ngr = ngr,
    easting = easting,
    northing = northing,
    edm_operation_percent = NA_real_,
    no_full_years_edm_data = NA_real_,
    edm_commission_date = as.Date(rep(NA_character_, length(years))),
    matched_event_count = 0L,
    match_methods = NA_character_
  )
}

# ------------------------------------------------------------------------------
# Status taxonomy drives availability: only absent is unavailable
# ------------------------------------------------------------------------------

status_crosswalk <- make_crosswalk(
  site_id = 101L,
  annual_status = c("reported_zero", "reported_positive", "reported_na", "absent"),
  spill_hrs_ea = c(0, 12, NA, NA),
  spill_count_ea = c(0, 3, NA, NA)
)

status_sites <- script_env$build_matched_site_data(status_crosswalk)
status_row <- status_sites %>% filter(site_id == 101L)

assert_identical(
  as.logical(status_row$available_year_2021),
  TRUE,
  "reported_zero works-years should be available."
)
assert_identical(
  as.logical(status_row$available_year_2022),
  TRUE,
  "reported_positive works-years should be available."
)
assert_identical(
  as.logical(status_row$available_year_2023),
  TRUE,
  "reported_na works-years should be available."
)
assert_identical(
  as.logical(status_row$available_year_2024),
  FALSE,
  "absent works-years should be unavailable."
)

# ------------------------------------------------------------------------------
# Crosswalk supplies fallback metadata even with zero matched events
# ------------------------------------------------------------------------------

fallback_crosswalk <- make_crosswalk(
  site_id = 202L,
  annual_status = c("reported_zero", "absent", "absent", "absent"),
  spill_hrs_ea = c(0, NA, NA, NA),
  spill_count_ea = c(0, NA, NA, NA),
  water_company = "Zero Events Water",
  ngr = "TQ 30000 80000"
)

fallback_sites <- script_env$build_matched_site_data(fallback_crosswalk)
fallback_row <- fallback_sites %>% filter(site_id == 202L)

assert_identical(
  fallback_row$water_company_matched,
  "Zero Events Water",
  "A reporting works-year with zero matched events should still populate fallback water_company from the crosswalk."
)
assert_identical(
  fallback_row$ngr_matched,
  "TQ3000080000",
  "A reporting works-year with zero matched events should still populate fallback NGR from the crosswalk."
)
assert_true(
  !is.na(fallback_row$easting_matched) && !is.na(fallback_row$northing_matched),
  "Crosswalk fallback coordinates should be retained for zero-event works."
)

# ------------------------------------------------------------------------------
# NLO carryforward stops before the first post-NLO positive crosswalk works-year
# ------------------------------------------------------------------------------

nlo_unique_sites <- tibble(
  site_id = 303L,
  available_year_2021 = TRUE,
  available_year_2022 = FALSE,
  available_year_2023 = TRUE,
  available_year_2024 = FALSE,
  edm_operation_reason_2021 = NA_character_,
  edm_operation_reason_2022 = "No longer operational",
  edm_operation_reason_2023 = NA_character_,
  edm_operation_reason_2024 = NA_character_
)

nlo_crosswalk <- make_crosswalk(
  site_id = 303L,
  annual_status = c("reported_zero", "absent", "reported_positive", "absent"),
  spill_hrs_ea = c(0, NA, 5, NA),
  spill_count_ea = c(0, NA, 1, NA)
)

nlo_result <- script_env$apply_nlo_carryforward(
  unique_sites = nlo_unique_sites,
  crosswalk_data = nlo_crosswalk
)

assert_identical(
  as.logical(nlo_result$available_year_2022),
  TRUE,
  "NLO carryforward should fill absent years between the NLO reason and the next positive works-year."
)
assert_identical(
  as.logical(nlo_result$available_year_2023),
  TRUE,
  "The positive works-year remains available from crosswalk evidence."
)
assert_identical(
  as.logical(nlo_result$available_year_2024),
  FALSE,
  "NLO carryforward should not continue after the first post-NLO positive works-year."
)
assert_identical(
  as.integer(nlo_result$nlo_carryforward_year),
  2022L,
  "nlo_carryforward_year should record the first year added by carryforward."
)

cat("All create_unique_spill_sites CH8 contract tests passed.\n")
