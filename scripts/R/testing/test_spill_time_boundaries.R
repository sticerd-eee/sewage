############################################################
# Spill Time Boundary Contracts
# Project: Sewage
# Date: 10/08/2026
############################################################

required_packages <- c("data.table", "here")

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}))

source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
source(here::here(
  "scripts", "R", "03_data_enrichment", "aggregate_daily_spill_rainfall.R"
))

test_base_year <- 2021L

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

make_event <- function(start_time, end_time, year = 2021L) {
  data.table::data.table(
    site_id = 1L,
    year = year,
    water_company = "Fixture Water",
    start_time = as.POSIXct(start_time, tz = "UTC"),
    end_time = as.POSIXct(end_time, tz = "UTC")
  )
}

count_fixture_spills <- function(second_start, second_end) {
  count_spills(
    as.POSIXct(
      c("2021-01-01 00:00:00", second_start),
      tz = "UTC"
    ),
    as.POSIXct(
      c("2021-01-01 01:00:00", second_end),
      tz = "UTC"
    )
  )
}

assert_equal(
  count_fixture_spills(
    "2021-01-02 11:59:59",
    "2021-01-02 12:00:00"
  ),
  2,
  "A spill beginning one second before the active block end should remain in the active sequence."
)
assert_equal(
  count_fixture_spills(
    "2021-01-02 12:00:00",
    "2021-01-02 13:00:00"
  ),
  2,
  "A spill beginning exactly at the excluded block end should start a new 12-hour block."
)
assert_equal(
  count_fixture_spills(
    "2021-01-02 12:00:01",
    "2021-01-02 13:00:01"
  ),
  2,
  "A spill beginning after the active block end should start a new 12-hour block."
)
assert_equal(
  count_spills(
    as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
    as.POSIXct("2021-01-03 12:00:00", tz = "UTC")
  ),
  3,
  "A 60-hour spill should retain its existing 12-hour plus two 24-hour block count."
)

run_new_year_fixture <- function(timezone) {
  previous_timezone <- Sys.getenv("TZ", unset = NA_character_)
  on.exit({
    if (is.na(previous_timezone)) {
      Sys.unsetenv("TZ")
    } else {
      Sys.setenv(TZ = previous_timezone)
    }
  }, add = TRUE)

  Sys.setenv(TZ = timezone)

  event <- make_event(
    "2021-12-31 23:00:00",
    "2022-01-01 01:00:00"
  )
  prepared <- prepare_spill_data(event, test_base_year)
  daily <- aggregate_daily_spills(event)

  list(yearly = prepared$yearly, monthly = prepared$monthly, daily = daily)
}

utc_result <- run_new_year_fixture("UTC")
rome_result <- run_new_year_fixture("Europe/Rome")
expected_year_end <- as.POSIXct("2022-01-01 00:00:00", tz = "UTC")

assert_equal(
  utc_result$yearly$end_time,
  expected_year_end,
  "New Year clamp should end at the exact UTC year boundary."
)
assert_true(
  nrow(utc_result$monthly) == 1L &&
    utc_result$monthly$month == 12L &&
    utc_result$monthly$quarter == 4L,
  "A prior-year record should retain only its positive-duration December slice."
)
assert_equal(
  calculate_spill_hours(
    utc_result$monthly$start_time,
    utc_result$monthly$end_time
  ),
  1,
  "The retained December slice should preserve the full boundary hour."
)
assert_true(
  all(utc_result$monthly$end_time > utc_result$monthly$start_time),
  "Monthly preparation should never return zero-duration slices."
)

for (period in c("yearly", "monthly")) {
  assert_equal(
    utc_result[[period]]$start_time,
    rome_result[[period]]$start_time,
    sprintf("%s start times should not depend on the machine timezone.", period)
  )
  assert_equal(
    utc_result[[period]]$end_time,
    rome_result[[period]]$end_time,
    sprintf("%s end times should not depend on the machine timezone.", period)
  )
}

assert_equal(
  utc_result$daily$date,
  as.Date("2021-12-31"),
  "The daily caller should use the shared UTC year clamp without a phantom January day."
)
assert_equal(
  utc_result$daily$spill_hrs,
  rome_result$daily$spill_hrs,
  "Daily aggregation should not depend on the machine timezone."
)

month_crossing <- make_event(
  "2021-02-28 23:30:00",
  "2021-03-01 00:30:00"
)
month_slices <- prepare_spill_data(
  month_crossing,
  test_base_year
)$monthly[order(month)]

assert_equal(
  month_slices$month,
  c(2, 3),
  "A month-crossing spill should produce one positive slice in each month."
)
assert_true(
  all(month_slices$end_time > month_slices$start_time),
  "Every month-crossing slice should have positive duration."
)
assert_equal(
  calculate_spill_hours(month_slices$start_time, month_slices$end_time),
  1,
  "Monthly slices should reconcile exactly to the original spill duration."
)

alternative_base_slices <- prepare_spill_data(
  month_crossing,
  base_year = 2020L
)$monthly[order(month)]

assert_equal(
  alternative_base_slices$month_id,
  c(14, 15),
  "Month IDs should be derived from the supplied base year."
)
assert_equal(
  alternative_base_slices$qtr_id,
  c(5, 5),
  "Quarter IDs should be derived from the supplied base year."
)

month_boundary <- make_event(
  "2021-01-31 23:00:00",
  "2021-02-01 00:00:00"
)
month_boundary_slices <- prepare_spill_data(
  month_boundary,
  test_base_year
)$monthly

assert_true(
  nrow(month_boundary_slices) == 1L && month_boundary_slices$month == 1L,
  "A spill ending exactly at a month boundary should belong only to the preceding month."
)
assert_equal(
  calculate_spill_hours(
    month_boundary_slices$start_time,
    month_boundary_slices$end_time
  ),
  1,
  "A spill ending at a month boundary should retain its full duration."
)

year_boundary <- make_event(
  "2021-12-31 23:00:00",
  "2022-01-01 00:00:00"
)
year_boundary_slices <- prepare_spill_data(
  year_boundary,
  test_base_year
)$monthly

assert_true(
  nrow(year_boundary_slices) == 1L &&
    year_boundary_slices$month == 12L &&
    year_boundary_slices$quarter == 4L,
  "A spill ending exactly at year end should belong only to December and Q4."
)

cat("spill time boundary contracts passed\n")
