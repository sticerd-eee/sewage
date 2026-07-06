############################################################
# Aggregate Spill Statistics Crosswalk Contracts
# Project: Sewage
# Date: 06/07/2026
############################################################

required_packages <- c("dplyr", "here", "lubridate", "tibble")

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}))

source(here::here("scripts", "R", "03_data_enrichment", "aggregate_spill_stats.R"))
initialise_environment()

CONFIG$years <- 2021:2022
CONFIG$base_year <- 2021

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

assert_equal <- function(actual, expected, message, tolerance = 1e-9) {
  if (length(actual) != length(expected) ||
      any(abs(as.numeric(actual) - as.numeric(expected)) > tolerance, na.rm = TRUE) ||
      any(is.na(actual) != is.na(expected))) {
    stop(
      sprintf(
        "%s\nActual: %s\nExpected: %s",
        message,
        paste(actual, collapse = ", "),
        paste(expected, collapse = ", ")
      ),
      call. = FALSE
    )
  }
}

events <- tibble::tibble(
  site_id = 1L,
  year = 2021L,
  water_company = "Fixture Water",
  start_time = as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
  end_time = as.POSIXct("2021-01-01 02:00:00", tz = "UTC")
)

crosswalk <- tibble::tribble(
  ~site_id, ~year, ~water_company, ~annual_status, ~spill_count_ea_crosswalk, ~spill_hrs_ea_crosswalk,
  1L, 2021L, "Fixture Water", "reported_positive", 99, 99,
  1L, 2022L, "Fixture Water", "absent", NA_real_, NA_real_,
  2L, 2021L, "Fixture Water", "reported_zero", 0, 0,
  2L, 2022L, "Fixture Water", "absent", NA_real_, NA_real_,
  3L, 2021L, "Fixture Water", "reported_na", NA_real_, NA_real_,
  3L, 2022L, "Fixture Water", "absent", NA_real_, NA_real_,
  4L, 2021L, "Fixture Water", "reported_positive", 5, 5,
  4L, 2022L, "Fixture Water", "absent", NA_real_, NA_real_
)

aggregated <- aggregate_spills(events)
completed <- complete_data_observations(aggregated, crosswalk)

event_year <- completed$yearly %>%
  filter(.data$site_id == 1L, .data$year == 2021L)
assert_equal(event_year$spill_count_yr, 1, "Event-computed yearly count should beat crosswalk fallback.")
assert_equal(event_year$spill_hrs_yr, 2, "Event-computed yearly hours should beat crosswalk fallback.")

reported_zero_year <- completed$yearly %>%
  filter(.data$site_id == 2L, .data$year == 2021L)
assert_equal(reported_zero_year$spill_count_yr, 0, "reported_zero site-year should enter yearly grid with zero count.")
assert_equal(reported_zero_year$spill_hrs_yr, 0, "reported_zero site-year should enter yearly grid with zero hours.")

reported_zero_months <- completed$monthly %>%
  filter(.data$site_id == 2L, .data$month_id %in% 1:12)
assert_true(
  nrow(reported_zero_months) == 12L &&
    all(reported_zero_months$spill_count_mo == 0) &&
    all(reported_zero_months$spill_hrs_mo == 0),
  "reported_zero site-year should enter all monthly grid cells with zero totals."
)

reported_zero_quarters <- completed$quarterly %>%
  filter(.data$site_id == 2L, .data$qtr_id %in% 1:4)
assert_true(
  nrow(reported_zero_quarters) == 4L &&
    all(reported_zero_quarters$spill_count_qt == 0) &&
    all(reported_zero_quarters$spill_hrs_qt == 0),
  "reported_zero site-year should enter all quarterly grid cells with zero totals."
)

reported_na_year <- completed$yearly %>%
  filter(.data$site_id == 3L, .data$year == 2021L)
assert_true(
  is.na(reported_na_year$spill_count_yr) && is.na(reported_na_year$spill_hrs_yr),
  "reported_na site-year should propagate NA totals rather than zero."
)

absent_year <- completed$yearly %>%
  filter(.data$site_id == 4L, .data$year == 2022L)
assert_true(
  absent_year$annual_status == "absent" &&
    is.na(absent_year$spill_count_yr) &&
    is.na(absent_year$spill_hrs_yr) &&
    is.na(absent_year$spill_count_ea_crosswalk) &&
    is.na(absent_year$spill_hrs_ea_crosswalk),
  "Absent works-years should be present only through site-level crossing, with no EA fallback."
)

descriptive_cols <- c(
  "site_name_ea", "site_name_wa_sc",
  "permit_reference_ea", "permit_reference_wa_sc",
  "activity_reference", "asset_type", "ngr", "unique_id",
  "wfd_waterbody_id_cycle_2", "receiving_water_name",
  "shellfish_water", "bathing_water", "edm_commission_date"
)

for (period in c("yearly", "monthly", "quarterly")) {
  assert_true(
    length(intersect(descriptive_cols, names(completed[[period]]))) == 0L,
    sprintf("Descriptive pseudo-row columns should be absent from %s output.", period)
  )
}

cat("aggregate_spill_stats crosswalk contracts passed\n")
