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

# U2: row-group ownership and spatial status semantics -------------------------

sales_contract <- study_period_market_contract("sale")
source_fixture <- data.table(
  house_id = 1:4,
  price = c(100000L, 200000L, 300000L, 400000L),
  ppd_category = c("A", "B", "A", "B"),
  easting = c(500000, 500100, NA, 500200),
  northing = c(200000, 200100, 200200, 200200)
)
ledger <- study_period_source_ledger(source_fixture, sales_contract)
assert_identical(
  ledger$spatially_eligible,
  c(TRUE, TRUE, FALSE, TRUE),
  "Eligibility must be the exact finite-coordinate predicate."
)

site_totals <- data.table(
  site_id = c(10L, 20L, 30L, 40L, 50L),
  spill_count = c(3, NA, 1, 2, 4),
  spill_hrs = c(7, NA, 2, 3, 5),
  has_missing_evidence = c(FALSE, TRUE, FALSE, FALSE, FALSE)
)

lookup_group_one <- data.table(
  house_id = c(1L, 2L, 2L),
  site_id = c(NA_integer_, 10L, 20L),
  distance_m = c(NA, 300, 750),
  distance_km = c(NA, 0.3, 0.75),
  n_site_groups = c(0L, 2L, 2L)
)
lookup_group_two <- data.table(
  house_id = c(4L, 4L, 4L),
  site_id = c(30L, 40L, 50L),
  distance_m = c(250, 500, 1000),
  distance_km = c(0.25, 0.5, 1),
  n_site_groups = 3L
)

reduced_one <- study_period_reduce_lookup_row_group(
  lookup_group_one,
  ledger,
  site_totals,
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
setkey(reduced_one, house_id, radius)

assert_true(
  reduced_one[.(1L, 250L),
    n_spill_sites == 0L && spatially_eligible && !has_missing_site &&
      spill_count == 0 && spill_hrs == 0 && is.na(min_distance)],
  "An eligible sentinel must produce a known true zero at every radius."
)
assert_true(
  reduced_one[.(2L, 250L),
    n_spill_sites == 0L && spill_count == 0 && is.na(mean_distance)],
  "A site outside 250 m must not destroy the nested-radius zero."
)
assert_true(
  reduced_one[.(2L, 500L),
    n_spill_sites == 1L && spill_count == 3 && spill_hrs == 7 &&
      min_distance == 300 && !has_missing_site],
  "The 500 m row must include only the 300 m complete-evidence site."
)
assert_true(
  reduced_one[.(2L, 1000L),
    n_spill_sites == 2L && min_distance == 300 && mean_distance == 525 &&
      has_missing_site && is.na(spill_count) && is.na(spill_hrs)],
  "Unknown evidence must preserve known geography while masking exposure."
)
assert_true(
  reduced_one[.(2L, 500L),
    spill_count_daily_avg == 3 / 1461 &&
      spill_count_weekly_avg == 3 / 1461 * 7],
  "Daily and weekly rates must derive from the validated inclusive day count."
)

reduced_two <- study_period_reduce_lookup_row_group(
  lookup_group_two,
  ledger,
  site_totals,
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
setkey(reduced_two, house_id, radius)
assert_identical(
  reduced_two[.(4L, c(250L, 500L, 1000L)), n_spill_sites],
  c(1L, 2L, 3L),
  "Sites exactly on each boundary must enter that radius and every larger one."
)

rental_contract <- study_period_market_contract("rental")
rental_ledger <- study_period_source_ledger(
  source_fixture[, .(
    rental_id = house_id,
    listing_price = as.double(price) / 100,
    easting,
    northing
  )],
  rental_contract
)
rental_group_one <- copy(lookup_group_one)
setnames(rental_group_one, "house_id", "rental_id")
reduced_rental <- study_period_reduce_lookup_row_group(
  rental_group_one,
  rental_ledger,
  site_totals,
  rental_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
scientific_fields <- c(
  "n_days_in_window", "spill_hrs", "n_spill_sites", "spill_count",
  "mean_distance", "min_distance", "spatially_eligible", "has_missing_site",
  "spill_count_daily_avg", "spill_hrs_daily_avg",
  "spill_count_weekly_avg", "spill_hrs_weekly_avg", "radius"
)
assert_identical(
  as.data.frame(reduced_one[order(house_id), ..scientific_fields]),
  as.data.frame(reduced_rental[order(rental_id), ..scientific_fields]),
  "Isomorphic sales and rental inputs must produce identical scientific fields."
)

ineligible <- study_period_ineligible_rows(
  ledger[spatially_eligible == FALSE],
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
assert_true(
  nrow(ineligible) == 3L && all(!ineligible$spatially_eligible) &&
    all(!ineligible$has_missing_site) && all(is.na(ineligible$n_spill_sites)) &&
    all(is.na(ineligible$spill_count)) && all(is.na(ineligible$min_distance)),
  "Coordinate-ineligible transactions must retain three explicitly unknown rows."
)

assert_error_contains(
  study_period_reduce_lookup_row_group(
    copy(lookup_group_one)[house_id == 2L, n_site_groups := 3L],
    ledger, site_totals, sales_contract, c(250L, 500L, 1000L), 1461L
  ),
  "declared n_site_groups",
  "A declared count larger than the contained rows must fail."
)
assert_error_contains(
  study_period_reduce_lookup_row_group(
    rbind(lookup_group_one, lookup_group_one[2L]),
    ledger, site_totals, sales_contract, c(250L, 500L, 1000L), 1461L
  ),
  "duplicate transaction-Site Group",
  "Duplicate lookup pairs must fail."
)
assert_error_contains(
  study_period_reduce_lookup_row_group(
    copy(lookup_group_one)[house_id == 1L, distance_m := 0],
    ledger, site_totals, sales_contract, c(250L, 500L, 1000L), 1461L
  ),
  "sentinel",
  "A mixed null-site/non-null-distance sentinel must fail."
)
assert_error_contains(
  study_period_reduce_lookup_row_group(
    copy(lookup_group_one)[house_id == 2L & site_id == 10L, distance_m := -1],
    ledger, site_totals, sales_contract, c(250L, 500L, 1000L), 1461L
  ),
  "finite nonnegative distances",
  "Negative lookup distances must fail."
)

write_lookup_fixture <- function(path, groups, contract) {
  schema <- arrow::schema(
    house_id = arrow::int32(),
    site_id = arrow::int32(),
    distance_m = arrow::float64(),
    distance_km = arrow::float64(),
    n_site_groups = arrow::int32()
  )
  stream <- arrow::FileOutputStream$create(path)
  properties <- arrow::ParquetWriterProperties$create(names(schema))
  writer <- arrow::ParquetFileWriter$create(schema, stream, properties = properties)
  for (group in groups) {
    batch <- arrow::RecordBatch$create(group, schema = schema)
    writer$WriteBatch(batch, chunk_size = batch$num_rows)
  }
  writer$Close()
  stream$close()
  invisible(path)
}

lookup_path <- tempfile(fileext = ".parquet")
write_lookup_fixture(lookup_path, list(lookup_group_one, lookup_group_two), sales_contract)
captured_chunks <- list()
stream_result <- study_period_stream_lookup(
  lookup_path = lookup_path,
  ledger = ledger,
  site_totals = site_totals,
  contract = sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L,
  ineligible_chunk_size = 1L,
  write_fragment = function(chunk, fragment_index) {
    captured_chunks[[fragment_index]] <<- copy(chunk)
  }
)
assert_identical(stream_result$row_groups, 2L, "Both physical row groups must stream.")
assert_identical(
  stream_result$eligible_transactions,
  3L,
  "Every eligible transaction must be owned by exactly one physical row group."
)
assert_identical(
  stream_result$ineligible_transactions,
  1L,
  "Source-only ineligible transactions must be emitted separately."
)
assert_identical(
  length(captured_chunks),
  3L,
  "Each row group and bounded ineligible chunk must be written immediately."
)
streamed <- rbindlist(captured_chunks, use.names = TRUE)
assert_identical(nrow(streamed), 12L, "Every source ID needs exactly three rows.")
assert_true(
  !anyDuplicated(streamed[, .(house_id, radius)]),
  "Streamed fragments must have collision-free public keys."
)

split_lookup_path <- tempfile(fileext = ".parquet")
write_lookup_fixture(
  split_lookup_path,
  list(rbind(lookup_group_one, lookup_group_two), lookup_group_two),
  sales_contract
)
assert_error_contains(
  study_period_stream_lookup(
    split_lookup_path, ledger, site_totals, sales_contract,
    c(250L, 500L, 1000L), 1461L,
    write_fragment = function(chunk, fragment_index) invisible(NULL)
  ),
  "more than one physical row group",
  "A transaction split across physical row groups must fail."
)

missing_lookup_path <- tempfile(fileext = ".parquet")
write_lookup_fixture(missing_lookup_path, list(lookup_group_one), sales_contract)
assert_error_contains(
  study_period_stream_lookup(
    missing_lookup_path, ledger, site_totals, sales_contract,
    c(250L, 500L, 1000L), 1461L,
    write_fragment = function(chunk, fragment_index) invisible(NULL)
  ),
  "missing coordinate-eligible",
  "A coordinate-eligible source ID absent from the lookup must fail."
)

stage_path <- tempfile("study-period-stage-")
for (fragment_index in seq_along(captured_chunks)) {
  study_period_write_fragment(captured_chunks[[fragment_index]], stage_path, fragment_index)
}
validation <- study_period_validate_dataset(
  stage_path,
  ledger,
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
assert_identical(validation$rows, 12, "The bounded validator must conserve rows.")
assert_identical(
  validation$transactions,
  4L,
  "The bounded validator must reconcile every source transaction."
)
assert_identical(
  schema_signature(arrow::open_dataset(stage_path)$schema),
  schema_signature(sales_contract$schema),
  "A reopened stage must have the exact literal sales schema."
)

tampered_value <- copy(streamed)
tampered_value[house_id == 2L & radius == 500L, price := price + 1L]
tampered_value_path <- tempfile("study-period-tampered-value-")
study_period_write_fragment(tampered_value, tampered_value_path, 1L)
assert_error_contains(
  study_period_validate_dataset(
    tampered_value_path, ledger, sales_contract,
    c(250L, 500L, 1000L), 1461L
  ),
  "source field price",
  "A staged source-value mutation must be rejected."
)

tampered_eligibility <- copy(streamed)
tampered_eligibility[house_id == 1L & radius == 250L,
  spatially_eligible := FALSE]
tampered_eligibility_path <- tempfile("study-period-tampered-eligibility-")
study_period_write_fragment(tampered_eligibility, tampered_eligibility_path, 1L)
assert_error_contains(
  study_period_validate_dataset(
    tampered_eligibility_path, ledger, sales_contract,
    c(250L, 500L, 1000L), 1461L
  ),
  "source-derived eligibility",
  "A staged eligibility mutation must be rejected."
)

duplicate_key_path <- tempfile("study-period-duplicate-key-")
study_period_write_fragment(rbind(streamed, streamed[1L]), duplicate_key_path, 1L)
assert_error_contains(
  study_period_validate_dataset(
    duplicate_key_path, ledger, sales_contract,
    c(250L, 500L, 1000L), 1461L
  ),
  "duplicate public keys",
  "A duplicate staged public key must be rejected."
)

cat("Study-period cross-section contract tests passed (U1-U2).\n")
