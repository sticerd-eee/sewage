# ==============================================================================
# Study-Period Cross-Section Contract Tests
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(here)
  library(tibble)
})

source(here::here(
  "scripts", "R", "utils", "cross_section_study_period_utils.R"
))

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

schema_signature <- function(schema) {
  stats::setNames(
    vapply(schema$fields, function(field) field$type$ToString(), character(1)),
    schema$names
  )
}

# U1: whole-year authority and literal public schemas ---------------------------

window_2021_2024 <- study_period_window(
  as.Date("2021-01-01"), as.Date("2024-12-31")
)
assert_identical(
  window_2021_2024$years,
  2021:2024,
  "The production window must derive the contiguous 2021-2024 sequence."
)
assert_identical(
  window_2021_2024$n_days_in_window,
  1461L,
  "The production window must include all 1,461 calendar days."
)

window_2022_2024 <- study_period_window(
  as.Date("2022-01-01"), as.Date("2024-12-31")
)
assert_identical(window_2022_2024$years, 2022:2024, "Years must follow dates.")
assert_identical(window_2022_2024$n_days_in_window, 1096L, "Leap days matter.")

assert_error_contains(
  study_period_window(as.Date("2021-02-01"), as.Date("2024-12-31")),
  "1 January",
  "A partial first year must fail."
)
assert_error_contains(
  study_period_window(as.Date("2021-01-01"), as.Date("2024-11-30")),
  "31 December",
  "A partial final year must fail."
)
assert_error_contains(
  study_period_window(as.Date("2024-01-01"), as.Date("2023-12-31")),
  "ordered",
  "Reversed study-period bounds must fail."
)
assert_error_contains(
  study_period_window("2021-01-01", as.Date("2024-12-31")),
  "Date",
  "Character date bounds must not be accepted implicitly."
)

sales_schema <- study_period_public_schema("sale")
rental_schema <- study_period_public_schema("rental")
assert_identical(
  schema_signature(sales_schema),
  c(
    house_id = "int32", price = "int32", ppd_category = "string",
    n_days_in_window = "int32", spill_hrs = "double",
    n_spill_sites = "int32", spill_count = "double",
    mean_distance = "double", min_distance = "double",
    spatially_eligible = "bool", has_missing_site = "bool",
    spill_count_daily_avg = "double", spill_hrs_daily_avg = "double",
    spill_count_weekly_avg = "double", spill_hrs_weekly_avg = "double",
    radius = "int32"
  ),
  "The sales schema must remain literal and ordered."
)
assert_identical(
  schema_signature(rental_schema),
  c(
    rental_id = "int32", listing_price = "double",
    n_days_in_window = "int32", spill_hrs = "double",
    n_spill_sites = "int32", spill_count = "double",
    mean_distance = "double", min_distance = "double",
    spatially_eligible = "bool", has_missing_site = "bool",
    spill_count_daily_avg = "double", spill_hrs_daily_avg = "double",
    spill_count_weekly_avg = "double", spill_hrs_weekly_avg = "double",
    radius = "int32"
  ),
  "The rental schema must expose listing_price and reject rent drift."
)
assert_true(!"rent" %in% rental_schema$names, "rent must not be public.")

# U1: annual-return truth table and collapse -----------------------------------

annual_fixture <- rbindlist(list(
  data.table(
    site_id = 10L,
    year = 2021:2024,
    annual_status = c(
      "reported_zero", "reported_positive", "reported_positive",
      "reported_zero"
    ),
    spill_count_ea = c(0, 1, 2, 0),
    spill_hrs_ea = c(0, 3, 4, 0)
  ),
  data.table(
    site_id = 20L,
    year = 2021:2024,
    annual_status = c(
      "reported_zero", "reported_na", "absent", "reported_zero"
    ),
    spill_count_ea = c(0, NA, NA, 0),
    spill_hrs_ea = c(0, NA, NA, 0)
  ),
  data.table(
    site_id = 30L,
    year = 2021:2023,
    annual_status = "reported_zero",
    spill_count_ea = 0,
    spill_hrs_ea = 0
  ),
  data.table(
    site_id = 40L,
    year = 2021:2025,
    annual_status = "reported_zero",
    spill_count_ea = 0,
    spill_hrs_ea = 0
  )
))

collapsed <- collapse_study_period_annual_returns(
  annual_fixture,
  window_2021_2024
)
setkey(collapsed, site_id)
assert_identical(
  collapsed[.(10L), .(spill_count, spill_hrs, has_missing_evidence)],
  data.table(spill_count = 3, spill_hrs = 7, has_missing_evidence = FALSE),
  "Complete annual evidence must sum across the derived years."
)
assert_true(
  collapsed[.(20L), has_missing_evidence] &&
    is.na(collapsed[.(20L), spill_count]) &&
    is.na(collapsed[.(20L), spill_hrs]),
  "reported_na or absent evidence must make the period unknown."
)
assert_true(
  collapsed[.(30L), has_missing_evidence],
  "A missing Site Group-year must make the period unknown."
)
assert_true(
  !collapsed[.(40L), has_missing_evidence],
  "Years outside the configured period must be ignored."
)

missing_global_year <- annual_fixture[year != 2024L]
assert_error_contains(
  collapse_study_period_annual_returns(missing_global_year, window_2021_2024),
  "derived study year",
  "A year absent from the crosswalk as a whole must fail."
)

duplicate_year <- rbind(annual_fixture, annual_fixture[1L])
assert_error_contains(
  collapse_study_period_annual_returns(duplicate_year, window_2021_2024),
  "duplicate Site Group-year",
  "Duplicate Site Group-year evidence must fail."
)

invalid_cases <- list(
  list(status = "reported_zero", count = 1, hours = 0, error = "reported_zero"),
  list(status = "reported_positive", count = NA, hours = 1, error = "reported_positive"),
  list(status = "reported_positive", count = 0, hours = 0, error = "reported_positive"),
  list(status = "reported_na", count = 0, hours = NA, error = "reported_na"),
  list(status = "absent", count = NA, hours = 0, error = "absent"),
  list(status = "unexpected", count = NA, hours = NA, error = "annual_status"),
  list(status = "reported_positive", count = -1, hours = 1, error = "nonnegative"),
  list(status = "reported_positive", count = Inf, hours = 1, error = "finite")
)
for (case in invalid_cases) {
  invalid <- copy(annual_fixture)
  invalid[site_id == 10L & year == 2021L, `:=`(
    annual_status = case$status,
    spill_count_ea = case$count,
    spill_hrs_ea = case$hours
  )]
  assert_error_contains(
    collapse_study_period_annual_returns(invalid, window_2021_2024),
    case$error,
    paste("Invalid annual-return state must fail:", case$status)
  )
}

cat("Study-period cross-section contract tests passed (U1).\n")
