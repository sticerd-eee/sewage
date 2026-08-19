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
  "scripts", "R", "utils", "dataset_publication_utils.R"
))
source(here::here(
  "scripts", "R", "utils", "spill_aggregation_utils.R"
))
source(here::here(
  "scripts", "R", "utils", "site_group_utils.R"
))
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
    house_id = "string", price = "int32", ppd_category = "string",
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
    rental_id = "string", listing_price = "double",
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
    spill_hrs_ea = c(0, 3, 4, 0),
    matched_event_count = c(0L, 1L, 2L, 0L)
  ),
  data.table(
    site_id = 20L,
    year = 2021:2024,
    annual_status = c(
      "reported_zero", "reported_na", "absent", "reported_zero"
    ),
    spill_count_ea = c(0, NA, NA, 0),
    spill_hrs_ea = c(0, NA, NA, 0),
    matched_event_count = 0L
  ),
  data.table(
    site_id = 30L,
    year = 2021:2023,
    annual_status = "reported_zero",
    spill_count_ea = 0,
    spill_hrs_ea = 0,
    matched_event_count = 0L
  ),
  data.table(
    site_id = 40L,
    year = 2021:2025,
    annual_status = "reported_zero",
    spill_count_ea = 0,
    spill_hrs_ea = 0,
    matched_event_count = 0L
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

# U1: the Annual-Returns collapse is unchanged by the source dispatch ----------

assert_identical(
  as.data.frame(collapse_study_period_annual_returns(
    annual_fixture, window_2021_2024
  )),
  as.data.frame(data.table(
    site_id = c(10L, 20L, 30L, 40L),
    spill_count = c(3, NA, NA, 0),
    spill_hrs = c(7, NA, NA, 0),
    has_missing_evidence = c(FALSE, TRUE, TRUE, FALSE)
  )),
  "The Annual-Returns collapse must survive the pluggable-source refactor intact."
)

# U1: the evidence grid rides the shared truth table and window reducer --------

evidence_grid <- study_period_annual_evidence_grid(
  annual_fixture, window_2021_2024
)
assert_identical(
  names(evidence_grid$site_evidence),
  c(
    "site_id", "annual_returns_absent", "annual_returns_na",
    "reported_positive_without_matched_events", "has_missing_evidence"
  ),
  "The grid must expose the three atomic flags plus the Stage-1 verdict."
)
assert_identical(
  evidence_grid$site_evidence$site_id,
  c(10L, 20L, 30L, 40L),
  "The window reducer must return one verdict per Site Group, in site order."
)
assert_identical(
  evidence_grid$site_evidence$annual_returns_absent,
  c(FALSE, TRUE, TRUE, FALSE),
  paste(
    "Site 20's absent year and site 30's missing 2024 row must both raise",
    "annual_returns_absent through the shared gap fill."
  )
)
assert_identical(
  evidence_grid$site_evidence$annual_returns_na,
  c(FALSE, TRUE, FALSE, FALSE),
  "Only site 20's reported_na year may raise annual_returns_na."
)
assert_identical(
  evidence_grid$site_evidence$has_missing_evidence,
  c(FALSE, TRUE, TRUE, FALSE),
  "The Stage-1 verdict must reproduce today's two-flag masks exactly."
)
assert_identical(
  evidence_grid$site_evidence$reported_positive_without_matched_events,
  c(FALSE, FALSE, FALSE, FALSE),
  "reported_positive years with matched events must not raise the third flag."
)

# The third atomic flag stays outside the Stage-1 verdict: a reported_positive
# year without matched events raises it, yet the exposure stays unmasked.
stage1_fixture <- data.table(
  site_id = 80L,
  year = 2021:2024,
  annual_status = c(
    "reported_positive", "reported_zero", "reported_zero", "reported_zero"
  ),
  spill_count_ea = c(2, 0, 0, 0),
  spill_hrs_ea = c(5, 0, 0, 0),
  matched_event_count = 0L
)
stage1_grid <- study_period_annual_evidence_grid(
  stage1_fixture, window_2021_2024
)
assert_true(
  stage1_grid$site_evidence$reported_positive_without_matched_events &&
    !stage1_grid$site_evidence$has_missing_evidence,
  paste(
    "A reported_positive year without matched events must raise the atomic",
    "flag without entering the Stage-1 verdict."
  )
)
assert_identical(
  collapse_study_period_annual_returns(stage1_fixture, window_2021_2024)[
    , .(spill_count, spill_hrs, has_missing_evidence)
  ],
  data.table(spill_count = 2, spill_hrs = 5, has_missing_evidence = FALSE),
  "Stage 1 must keep exposure unmasked when only the third flag is raised."
)

invalid_matched <- copy(annual_fixture)
invalid_matched[site_id == 10L & year == 2021L, matched_event_count := NA_integer_]
assert_error_contains(
  collapse_study_period_annual_returns(invalid_matched, window_2021_2024),
  "matched_event_count",
  "A missing matched_event_count must fail loudly, not gap-fill to zero."
)

# U1: event-based collapse ------------------------------------------------------

utc_time <- function(text) as.POSIXct(text, tz = "UTC")

# Sites 10 and 40 carry complete Annual-Returns evidence, site 20 turns unknown
# mid-window, and site 30 is missing its 2024 row from the crosswalk grid.
events_fixture <- data.table(
  site_id = c(10L, 20L, 30L),
  start_time = utc_time(c(
    "2021-06-01 00:00:00", "2021-06-01 00:00:00", "2021-06-01 00:00:00"
  )),
  end_time = utc_time(c(
    "2021-06-02 01:00:00", "2021-06-01 05:00:00", "2021-06-01 05:00:00"
  )),
  year = 2021L
)

events_collapsed <- collapse_study_period_events(
  annual_fixture, events_fixture, window_2021_2024
)
setkey(events_collapsed, site_id)

assert_identical(
  events_collapsed$site_id,
  c(10L, 20L, 30L, 40L),
  "The events collapse must cover exactly the Annual-Returns Site Group grid."
)
assert_identical(
  events_collapsed[.(10L), .(spill_hrs, spill_count)],
  data.table(spill_hrs = 25, spill_count = 2),
  paste(
    "A 25-hour in-window event must yield 25 spill hours and 2 spills under",
    "the 12/24 rule."
  )
)
assert_true(
  events_collapsed[.(20L), has_missing_evidence] &&
    is.na(events_collapsed[.(20L), spill_count]) &&
    is.na(events_collapsed[.(20L), spill_hrs]),
  "Events cannot override reported_na or absent Annual-Returns evidence."
)
assert_true(
  events_collapsed[.(30L), has_missing_evidence] &&
    is.na(events_collapsed[.(30L), spill_hrs]),
  "A Site Group-year missing from the crosswalk grid must mask event exposure."
)
assert_identical(
  events_collapsed[.(40L), .(spill_hrs, spill_count, has_missing_evidence)],
  data.table(spill_hrs = 0, spill_count = 0, has_missing_evidence = FALSE),
  "A fully reported Site Group with no events must retain a true zero."
)
assert_identical(
  names(events_collapsed),
  names(collapse_study_period_annual_returns(annual_fixture, window_2021_2024)),
  "Both collapses must emit the same intermediate columns."
)

clipping_annual_fixture <- rbindlist(lapply(
  c(51L, 52L, 53L, 54L),
  function(site) data.table(
    site_id = site,
    year = 2021:2024,
    annual_status = "reported_zero",
    spill_count_ea = 0,
    spill_hrs_ea = 0
  )
))
clipping_events_fixture <- data.table(
  site_id = c(51L, 52L, 53L, 54L, 54L),
  start_time = utc_time(c(
    "2020-12-31 18:00:00", "2024-12-31 20:00:00", "2020-01-01 00:00:00",
    "2021-01-01 00:00:00", "2021-03-01 00:00:00"
  )),
  end_time = utc_time(c(
    "2021-01-01 06:00:00", "2025-01-02 00:00:00", "2020-01-02 00:00:00",
    "2021-01-01 02:00:00", "2021-03-01 03:00:00"
  )),
  year = c(2020L, 2024L, 2020L, 2021L, 2021L)
)
clipped <- collapse_study_period_events(
  clipping_annual_fixture, clipping_events_fixture, window_2021_2024
)
setkey(clipped, site_id)

assert_identical(
  clipped[.(51L), .(spill_hrs, spill_count)],
  data.table(spill_hrs = 6, spill_count = 1),
  "An event straddling the window start must contribute only its in-window hours."
)
assert_identical(
  clipped[.(52L), .(spill_hrs, spill_count)],
  data.table(spill_hrs = 4, spill_count = 1),
  "An event straddling the window end must contribute only its in-window hours."
)
assert_identical(
  clipped[.(53L), .(spill_hrs, spill_count, has_missing_evidence)],
  data.table(spill_hrs = 0, spill_count = 0, has_missing_evidence = FALSE),
  "An event entirely outside the window must contribute nothing."
)
assert_identical(
  clipped[.(54L), .(spill_hrs, spill_count)],
  data.table(spill_hrs = 5, spill_count = 2),
  "Separate events at one Site Group must sum hours and count independently."
)

events_missing_column <- copy(events_fixture)[, end_time := NULL]
assert_error_contains(
  collapse_study_period_events(
    annual_fixture, events_missing_column, window_2021_2024
  ),
  "end_time",
  "The events collapse must reject a feed without its required columns."
)
assert_error_contains(
  collapse_study_period_events(
    annual_fixture,
    copy(events_fixture)[1L, end_time := utc_time("2021-05-01 00:00:00")],
    window_2021_2024
  ),
  "start_time",
  "The events collapse must reject an event ending before it starts."
)

# U1: event evidence flows through the shared radius aggregation ----------------

events_reduced <- study_period_reduce_lookup_row_group(
  data.table(
    house_id = c("2", "2"),
    site_id = c(10L, 20L),
    distance_m = c(300, 750),
    distance_km = c(0.3, 0.75),
    n_site_groups = 2L
  ),
  study_period_source_ledger(
    data.table(
      house_id = as.character(1:4),
      price = c(100000L, 200000L, 300000L, 400000L),
      ppd_category = c("A", "B", "A", "B"),
      easting = c(500000, 500100, NA, 500200),
      northing = c(200000, 200100, 200200, 200200)
    ),
    study_period_market_contract("sale")
  ),
  events_collapsed,
  study_period_market_contract("sale"),
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
setkey(events_reduced, house_id, radius)
assert_true(
  events_reduced[.("2", 500L),
    n_spill_sites == 1L && !has_missing_site && spill_hrs == 25 &&
      spill_count == 2],
  "Event exposure must aggregate to the transaction-radius grain unchanged."
)
assert_true(
  events_reduced[.("2", 1000L),
    has_missing_site && is.na(spill_count) && is.na(spill_hrs)],
  paste(
    "A radius containing a Site Group with events but unknown Annual-Returns",
    "evidence must report NA exposure and has_missing_site."
  )
)

# U2: row-group ownership and spatial status semantics -------------------------

sales_contract <- study_period_market_contract("sale")
source_fixture <- data.table(
  house_id = as.character(1:4),
  price = c(100000L, 200000L, 300000L, 400000L),
  ppd_category = c("A", "B", "A", "B"),
  easting = c(500000, 500100, NA, 500200),
  northing = c(200000, 200100, 200200, 200200)
)
assert_error_contains(
  study_period_source_ledger(
    copy(source_fixture)[, house_id := seq_len(.N)],
    sales_contract
  ),
  "character transaction identifiers",
  "Study-period source ledgers must reject stale positional integer IDs."
)
leading_zero_source <- copy(source_fixture[1:2])
leading_zero_source[, house_id := c("01leadingzero", "02stablehash")]
leading_zero_ledger <- study_period_source_ledger(
  leading_zero_source,
  sales_contract
)
assert_identical(
  leading_zero_ledger$house_id,
  c("01leadingzero", "02stablehash"),
  "The source ledger must preserve character IDs including a leading zero."
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
  house_id = c("1", "2", "2"),
  site_id = c(NA_integer_, 10L, 20L),
  distance_m = c(NA, 300, 750),
  distance_km = c(NA, 0.3, 0.75),
  n_site_groups = c(0L, 2L, 2L)
)
lookup_group_two <- data.table(
  house_id = c("4", "4", "4"),
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
  reduced_one[.("1", 250L),
    n_spill_sites == 0L && spatially_eligible && !has_missing_site &&
      spill_count == 0 && spill_hrs == 0 && is.na(min_distance)],
  "An eligible sentinel must produce a known true zero at every radius."
)
assert_true(
  reduced_one[.("2", 250L),
    n_spill_sites == 0L && spill_count == 0 && is.na(mean_distance)],
  "A site outside 250 m must not destroy the nested-radius zero."
)
assert_true(
  reduced_one[.("2", 500L),
    n_spill_sites == 1L && spill_count == 3 && spill_hrs == 7 &&
      min_distance == 300 && !has_missing_site],
  "The 500 m row must include only the 300 m complete-evidence site."
)
assert_true(
  reduced_one[.("2", 1000L),
    n_spill_sites == 2L && min_distance == 300 && mean_distance == 525 &&
      has_missing_site && is.na(spill_count) && is.na(spill_hrs)],
  "Unknown evidence must preserve known geography while masking exposure."
)
assert_true(
  reduced_one[.("2", 500L),
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
  reduced_two[.("4", c(250L, 500L, 1000L)), n_spill_sites],
  c(1L, 2L, 3L),
  "Sites exactly on each boundary must enter that radius and every larger one."
)

lookup_group_all_outside <- data.table(
  house_id = "4",
  site_id = 30L,
  distance_m = 1000.01,
  distance_km = 1000.01 / 1000,
  n_site_groups = 1L
)
reduced_all_outside <- study_period_reduce_lookup_row_group(
  lookup_group_all_outside,
  ledger,
  site_totals,
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
assert_true(
  nrow(reduced_all_outside) == 3L &&
    all(reduced_all_outside$n_spill_sites == 0L) &&
    all(reduced_all_outside$spill_count == 0) &&
    all(reduced_all_outside$spill_hrs == 0) &&
    all(is.na(reduced_all_outside$mean_distance)) &&
    all(is.na(reduced_all_outside$min_distance)) &&
    all(!reduced_all_outside$has_missing_site),
  paste0(
    "A row group whose valid matches all exceed the maximum radius must ",
    "produce known true-zero exposure at every radius."
  )
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

negative_site_count <- copy(reduced_two)
negative_site_count[radius == 250L, n_spill_sites := -1L]
assert_error_contains(
  study_period_validate_and_cast_public(negative_site_count, sales_contract),
  "nonnegative site count",
  "A negative published site count must fail."
)

negative_min_distance <- copy(reduced_two)
negative_min_distance[radius == 250L, min_distance := -1]
assert_error_contains(
  study_period_validate_and_cast_public(negative_min_distance, sales_contract),
  "distance semantics",
  "A negative published minimum distance must fail."
)

negative_mean_distance <- copy(reduced_two)
negative_mean_distance[
  radius == 250L,
  `:=`(mean_distance = -1, min_distance = -2)
]
assert_error_contains(
  study_period_validate_and_cast_public(negative_mean_distance, sales_contract),
  "distance semantics",
  "A negative published mean distance must fail."
)

write_lookup_fixture <- function(path, groups, contract) {
  schema <- arrow::schema(
    house_id = arrow::utf8(),
    site_id = arrow::int32(),
    distance_m = arrow::float64(),
    distance_km = arrow::float64(),
    n_site_groups = arrow::int32()
  )
  stream <- arrow::FileOutputStream$create(path)
  properties <- arrow::ParquetWriterProperties$create(names(schema))
  writer <- arrow::ParquetFileWriter$create(schema, stream, properties = properties)
  for (group in groups) {
    group <- copy(group)
    group[[contract$id]] <- as.character(group[[contract$id]])
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

buffered_stage_path <- tempfile("study-period-buffered-stage-")
buffered_writer <- study_period_buffered_writer(
  buffered_stage_path,
  batch_size = 2L
)
buffered_writer$write(captured_chunks[[1L]], 1L)
assert_true(
  !dir.exists(buffered_stage_path),
  "The buffered writer must keep a bounded partial batch in memory."
)
buffered_writer$write(captured_chunks[[2L]], 2L)
assert_identical(
  buffered_writer$fragments(),
  1L,
  "A full output batch must produce one physical fragment namespace."
)
buffered_writer$write(captured_chunks[[3L]], 3L)
buffered_writer$flush()
assert_identical(
  buffered_writer$fragments(),
  2L,
  "The final partial output batch must flush exactly once."
)
buffered_validation <- study_period_validate_dataset(
  buffered_stage_path,
  ledger,
  sales_contract,
  radii = c(250L, 500L, 1000L),
  n_days_in_window = 1461L
)
assert_identical(
  buffered_validation$rows,
  12,
  "Output batching must preserve every source ID-radius row."
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

cross_fragment_duplicate_path <- tempfile(
  "study-period-cross-fragment-duplicate-"
)
study_period_write_fragment(streamed, cross_fragment_duplicate_path, 1L)
study_period_write_fragment(streamed[1L], cross_fragment_duplicate_path, 2L)
assert_error_contains(
  study_period_validate_dataset(
    cross_fragment_duplicate_path, ledger, sales_contract,
    c(250L, 500L, 1000L), 1461L
  ),
  "duplicate public keys",
  "A public key repeated across physical fragments must be rejected."
)

# U3: shared staged publication lifecycle -------------------------------------

write_generation <- function(path, value) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  writeLines(value, file.path(path, "generation.txt"))
  invisible(path)
}
read_generation <- function(path) {
  readLines(file.path(path, "generation.txt"), warn = FALSE)
}
validate_generation <- function(path) {
  if (!dir.exists(path) || !file.exists(file.path(path, "generation.txt"))) {
    stop("invalid generation", call. = FALSE)
  }
  invisible(path)
}

publication_root <- tempfile("dataset-publication-")
dir.create(publication_root)
canonical <- file.path(publication_root, "canonical")
stage_one <- file.path(publication_root, ".canonical.stage-one")
write_generation(stage_one, "one")
validation_paths <- character()
publish_validated_dataset(
  stage_one,
  canonical,
  validate = function(path) {
    validation_paths <<- c(validation_paths, path)
    validate_generation(path)
  }
)
assert_identical(read_generation(canonical), "one", "First publication must promote.")
assert_identical(
  validation_paths,
  c(stage_one, canonical),
  "The same product validator must run before and after promotion."
)

stage_two <- file.path(publication_root, ".canonical.stage-two")
write_generation(stage_two, "two")
publish_validated_dataset(stage_two, canonical, validate_generation)
assert_identical(read_generation(canonical), "two", "Replacement must promote.")
assert_true(
  !dir.exists(paste0(canonical, ".prev")),
  "Successful replacement must remove only its temporary backup."
)

interrupted <- file.path(publication_root, "interrupted")
write_generation(paste0(interrupted, ".prev"), "recoverable")
interrupted_stage <- file.path(publication_root, ".interrupted.stage")
write_generation(interrupted_stage, "candidate")
assert_error_contains(
  publish_validated_dataset(interrupted_stage, interrupted, validate_generation),
  "recoverable",
  "Absent canonical with present .prev must stop without mutation."
)
assert_identical(
  read_generation(paste0(interrupted, ".prev")),
  "recoverable",
  "Interrupted-state preflight must preserve the recovery generation."
)

ambiguous <- file.path(publication_root, "ambiguous")
write_generation(ambiguous, "canonical")
write_generation(paste0(ambiguous, ".prev"), "previous")
ambiguous_stage <- file.path(publication_root, ".ambiguous.stage")
write_generation(ambiguous_stage, "candidate")
assert_error_contains(
  publish_validated_dataset(ambiguous_stage, ambiguous, validate_generation),
  "ambiguous",
  "Present canonical with present .prev must fail closed."
)
assert_identical(read_generation(ambiguous), "canonical", "Canonical must remain.")
assert_identical(
  read_generation(paste0(ambiguous, ".prev")),
  "previous",
  "Ambiguous .prev must remain."
)

restored <- file.path(publication_root, "restored")
write_generation(restored, "old")
restored_stage <- file.path(publication_root, ".restored.stage")
write_generation(restored_stage, "new")
promotion_failure <- function(from, to) {
  if (identical(from, restored_stage) && identical(to, restored)) return(FALSE)
  file.rename(from, to)
}
assert_error_contains(
  publish_validated_dataset(
    restored_stage, restored, validate_generation,
    rename_path = promotion_failure
  ),
  "restored",
  "A failed stage promotion must report restoration."
)
assert_identical(read_generation(restored), "old", "Promotion failure must restore old.")

post_validation <- file.path(publication_root, "post-validation")
write_generation(post_validation, "old")
post_validation_stage <- file.path(publication_root, ".post-validation.stage")
write_generation(post_validation_stage, "new")
assert_error_contains(
  publish_validated_dataset(
    post_validation_stage,
    post_validation,
    validate = function(path) {
      validate_generation(path)
      if (identical(path, post_validation)) {
        stop("injected final validation failure", call. = FALSE)
      }
    }
  ),
  "restored",
  "Post-promotion validation failure must restore the prior generation."
)
assert_identical(
  read_generation(post_validation),
  "old",
  "Final validation failure must restore the exact prior generation."
)

first_invalid <- file.path(publication_root, "first-invalid")
first_invalid_stage <- file.path(publication_root, ".first-invalid.stage")
write_generation(first_invalid_stage, "invalid")
assert_error_contains(
  publish_validated_dataset(
    first_invalid_stage,
    first_invalid,
    validate = function(path) {
      validate_generation(path)
      if (identical(path, first_invalid)) stop("invalid final", call. = FALSE)
    }
  ),
  "first generation",
  "A first-generation final validation failure must stop."
)
assert_true(
  !dir.exists(first_invalid),
  "A rejected first generation must not remain canonical."
)

cleanup_incomplete <- file.path(publication_root, "cleanup-incomplete")
write_generation(cleanup_incomplete, "old")
cleanup_stage <- file.path(publication_root, ".cleanup-incomplete.stage")
write_generation(cleanup_stage, "new")
assert_error_contains(
  publish_validated_dataset(
    cleanup_stage,
    cleanup_incomplete,
    validate_generation,
    remove_path = function(path) 1L
  ),
  "cleanup incomplete",
  "Failed successful-backup cleanup must return nonzero context."
)
assert_identical(
  read_generation(cleanup_incomplete),
  "new",
  "Cleanup failure must keep the validated replacement canonical."
)
assert_identical(
  read_generation(paste0(cleanup_incomplete, ".prev")),
  "old",
  "Cleanup failure must retain the readable temporary backup."
)

# U4: thin production adapters -------------------------------------------------

source_adapter <- function(path) {
  environment <- new.env(parent = globalenv())
  sys.source(here::here(path), envir = environment)
  environment
}

sale_spec <- list(
  market = "sale",
  id = "house_id",
  value = "price",
  provenance = "ppd_category",
  source_suffix = "house_price.parquet",
  lookup_suffix = "spill_house_lookup.parquet",
  end_date = as.Date("2024-12-31"),
  output_parent = file.path("cross_section", "sales")
)
rental_spec <- list(
  market = "rental",
  id = "rental_id",
  value = "listing_price",
  provenance = NULL,
  source_suffix = file.path("zoopla", "zoopla_rentals.parquet"),
  lookup_suffix = file.path("zoopla", "spill_rental_lookup.parquet"),
  end_date = as.Date("2023-12-31"),
  output_parent = file.path("cross_section", "rentals")
)

# Every market now ships an event-based builder on the unsuffixed path and an
# Annual-Returns builder on the _ea sibling.
adapter_specs <- list(
  modifyList(sale_spec, list(
    script = "cross_section_sales.R",
    exposure_source = "events",
    output_suffix = file.path(sale_spec$output_parent, "study_period")
  )),
  modifyList(sale_spec, list(
    script = "cross_section_sales_ea.R",
    exposure_source = "annual_returns",
    output_suffix = file.path(sale_spec$output_parent, "study_period_ea")
  )),
  modifyList(rental_spec, list(
    script = "cross_section_rental.R",
    exposure_source = "events",
    output_suffix = file.path(rental_spec$output_parent, "study_period")
  )),
  modifyList(rental_spec, list(
    script = "cross_section_rental_ea.R",
    exposure_source = "annual_returns",
    output_suffix = file.path(rental_spec$output_parent, "study_period_ea")
  ))
)

for (spec in adapter_specs) {
  market <- spec$market
  spec$path <- file.path("scripts", "R", "06_analysis_datasets", spec$script)
  adapter <- source_adapter(spec$path)
  assert_identical(
    adapter$CONFIG$exposure_source,
    spec$exposure_source,
    paste(spec$script, "must declare its exposure source explicitly.")
  )
  assert_true(
    endsWith(adapter$LOG_FILE, sub("[.]R$", ".log", spec$script)),
    paste(spec$script, "must log to the file named after it.")
  )
  if (spec$exposure_source == "events") {
    assert_true(
      is.character(adapter$CONFIG$events_path) &&
        endsWith(
          adapter$CONFIG$events_path, "matched_events_annual_data.parquet"
        ),
      paste(spec$script, "must read the matched individual EDM event feed.")
    )
  } else {
    assert_true(
      is.null(adapter$CONFIG$events_path),
      paste(spec$script, "must not configure an event feed.")
    )
  }
  assert_true(
    exists("run_study_period_cross_section", envir = adapter, mode = "function"),
    paste(market, "adapter must expose the shared orchestration seam.")
  )
  assert_identical(adapter$CONFIG$market, market, paste(market, "market must match."))
  assert_identical(
    adapter$CONFIG$start_date,
    as.Date("2021-01-01"),
    paste(market, "must configure the settled start date.")
  )
  assert_identical(
    adapter$CONFIG$end_date,
    spec$end_date,
    paste(market, "must configure the settled end date.")
  )
  assert_identical(
    adapter$CONFIG$radii,
    c(250L, 500L, 1000L),
    paste(market, "must configure only supported radii.")
  )
  assert_true(
    endsWith(adapter$CONFIG$source_path, spec$source_suffix) &&
      endsWith(adapter$CONFIG$lookup_path, spec$lookup_suffix) &&
      endsWith(adapter$CONFIG$output_path, spec$output_suffix),
    paste(market, "must pass its exact source, lookup, and study_period paths.")
  )
  resolved_contract <- study_period_market_contract(market)
  assert_identical(resolved_contract$id, spec$id, paste(market, "ID must match."))
  assert_identical(resolved_contract$value, spec$value, paste(market, "value must match."))
  assert_identical(
    resolved_contract$provenance,
    spec$provenance,
    paste(market, "provenance must match.")
  )

  injected_error <- tryCatch(
    adapter$run_study_period_cross_section(
      build = function(config) stop("injected shared-engine failure", call. = FALSE)
    ),
    error = identity
  )
  assert_true(
    inherits(injected_error, "error") &&
      grepl("injected shared-engine failure", conditionMessage(injected_error), fixed = TRUE),
    paste(market, "must rethrow a fatal shared-engine failure.")
  )

  adapter_text <- paste(readLines(here::here(spec$path), warn = FALSE), collapse = "\n")
  for (obsolete in c("duckdb", "dat_mo", "all_years", "prior_12mo", "install.packages")) {
    assert_true(
      !grepl(obsolete, adapter_text, fixed = TRUE),
      paste(market, "adapter must not retain obsolete execution text:", obsolete)
    )
  }
  # Adapters run standalone under Rscript, where the shared measurement core is
  # only available if the adapter sources it itself; this suite's environment
  # would mask that omission because the core is already attached here.
  for (required_source in c(
    "spill_aggregation_utils.R", "site_group_utils.R",
    "cross_section_study_period_utils.R"
  )) {
    assert_true(
      grepl(required_source, adapter_text, fixed = TRUE),
      paste(market, "adapter must source the shared core file:", required_source)
    )
  }
}

build_root <- tempfile("study-period-build-")
dir.create(build_root, recursive = TRUE)
build_source_path <- file.path(build_root, "source.parquet")
build_lookup_path <- file.path(build_root, "lookup.parquet")
build_crosswalk_path <- file.path(build_root, "crosswalk.parquet")
build_output_path <- file.path(build_root, "study_period")
arrow::write_parquet(source_fixture, build_source_path)
write_lookup_fixture(
  build_lookup_path,
  list(lookup_group_one, lookup_group_two),
  sales_contract
)
arrow::write_parquet(annual_fixture, build_crosswalk_path)
build_config <- list(
  market = "sale",
  source_path = build_source_path,
  lookup_path = build_lookup_path,
  crosswalk_path = build_crosswalk_path,
  output_path = build_output_path,
  start_date = as.Date("2021-01-01"),
  end_date = as.Date("2024-12-31"),
  radii = c(250L, 500L, 1000L),
  ineligible_chunk_size = 1L
)
first_build <- build_study_period_cross_section(build_config)
assert_identical(
  first_build$source_transactions,
  4L,
  "The shared build must use the source ledger as its cardinality authority."
)
assert_true(dir.exists(build_output_path), "The shared build must publish canonical output.")
second_build <- build_study_period_cross_section(build_config)
assert_identical(
  second_build$stream$output_rows,
  12,
  "A replacement build must conserve the exact source-by-radius row total."
)
assert_true(
  !dir.exists(paste0(build_output_path, ".prev")),
  "A successful replacement build must clean its temporary backup."
)

assert_error_contains(
  study_period_validate_radii(c(250L, 500L, 2000L)),
  "exactly 250, 500, and 1000",
  "Adapters must not accept a radius outside the supported contract."
)

# U4: exposure-source configuration --------------------------------------------

assert_identical(
  study_period_validate_config(build_config)$exposure_source,
  "annual_returns",
  "A configuration without an exposure source must keep the Annual-Returns default."
)
assert_error_contains(
  study_period_validate_config(
    modifyList(build_config, list(exposure_source = "guesswork"))
  ),
  "exposure_source",
  "An unknown exposure source must fail configuration validation."
)
assert_error_contains(
  study_period_validate_config(
    modifyList(build_config, list(exposure_source = "events"))
  ),
  "events_path",
  "An events configuration without an events path must fail validation."
)
assert_error_contains(
  study_period_validate_config(modifyList(build_config, list(
    exposure_source = "events",
    events_path = file.path(build_root, "absent-events.parquet")
  ))),
  "events_path",
  "An events configuration pointing at a missing feed must fail validation."
)

build_events_path <- file.path(build_root, "events.parquet")
arrow::write_parquet(
  data.table(
    site_id = c(10L, 30L, 40L, 50L),
    start_time = as.POSIXct(rep("2021-06-01 00:00:00", 4L), tz = "UTC"),
    end_time = as.POSIXct(
      c(
        "2021-06-02 01:00:00", "2021-06-01 04:00:00",
        "2021-06-01 02:00:00", "2021-06-01 06:00:00"
      ),
      tz = "UTC"
    ),
    year = 2021L
  ),
  build_events_path
)
events_build_config <- modifyList(build_config, list(
  exposure_source = "events",
  events_path = build_events_path,
  output_path = file.path(build_root, "study_period_events")
))
events_build <- build_study_period_cross_section(events_build_config)
assert_identical(
  events_build$source_transactions,
  4L,
  "The events build must use the same source ledger cardinality authority."
)
assert_identical(
  events_build$exposure_source,
  "events",
  "The events build must report the exposure source it consumed."
)

events_published <- as.data.table(
  arrow::open_dataset(events_build_config$output_path) |> dplyr::collect()
)
annual_published <- as.data.table(
  arrow::open_dataset(build_output_path) |> dplyr::collect()
)
setkey(events_published, house_id, radius)
setkey(annual_published, house_id, radius)
assert_identical(
  events_published[, .(house_id, radius)],
  annual_published[, .(house_id, radius)],
  "Both exposure sources must publish exactly the same public key set."
)
assert_identical(
  is.na(events_published$spill_hrs),
  is.na(annual_published$spill_hrs),
  "Both exposure sources must share an identical missingness pattern."
)
assert_true(
  events_published[.("2", 500L), spill_hrs == 25 && spill_count == 2],
  "The published events dataset must carry the recomputed 12/24 exposure."
)

# U4: exposure-source comparison invariants ------------------------------------

comparison <- new.env(parent = globalenv())
sys.source(
  here::here(
    "scripts", "R", "testing", "verify_study_period_exposure_sources.R"
  ),
  envir = comparison
)

comparison_fixture <- function(spill_count, spill_hrs, has_missing_site = NULL) {
  n <- length(spill_count)
  if (is.null(has_missing_site)) has_missing_site <- is.na(spill_count)
  fixture <- data.table(
    transaction_id = rep(as.character(seq_len(n / 3L)), each = 3L),
    radius = rep(c(250L, 500L, 1000L), times = n / 3L),
    spatially_eligible = TRUE,
    has_missing_site = has_missing_site,
    spill_count = as.double(spill_count),
    spill_hrs = as.double(spill_hrs)
  )
  setkey(fixture, transaction_id, radius)
  fixture
}

paired_events <- comparison_fixture(
  spill_count = c(1, 2, 3, NA, 0, 6),
  spill_hrs = c(2, 4, 6, NA, 0, 12)
)
paired_annual <- comparison_fixture(
  spill_count = c(2, 3, 4, NA, 0, 5),
  spill_hrs = c(3, 5, 7, NA, 0, 11)
)

paired_report <- comparison$compare_study_period_sources(
  paired_events, paired_annual, "sale"
)
assert_identical(
  sort(unique(paired_report$radius)),
  c(250L, 500L, 1000L),
  "The comparison must report one section per supported radius."
)
assert_true(
  all(paired_report[measure == "spill_count", n_rows] == 2L),
  "The comparison must attribute every row to exactly one radius section."
)
assert_true(
  paired_report[measure == "spill_count" & radius == 250L, n_comparable] == 1L &&
    paired_report[measure == "spill_count" & radius == 250L,
      n_missing_exposure] == 1L,
  "A shared NA row must be excluded from the comparable subset and counted."
)
assert_true(
  paired_report[measure == "spill_hrs" & radius == 500L, diff_mean] == -0.5,
  "The comparison must report the mean events-minus-EA level difference."
)

assert_error_contains(
  comparison$compare_study_period_sources(
    paired_events, paired_annual[-1L], "sale"
  ),
  "row count",
  "A row-count mismatch between sources must fail loudly."
)
mismatched_key <- copy(paired_annual)
mismatched_key[1L, transaction_id := "fabricated"]
assert_error_contains(
  comparison$compare_study_period_sources(paired_events, mismatched_key, "sale"),
  "identical transaction-radius key set",
  "A fabricated key mismatch between sources must fail loudly."
)
duplicate_key <- rbind(paired_annual, paired_annual[1L])
setkey(duplicate_key, transaction_id, radius)
assert_error_contains(
  comparison$compare_study_period_sources(paired_events, duplicate_key, "sale"),
  "duplicate transaction-radius key",
  "A duplicated public key must fail loudly."
)
extra_na <- copy(paired_annual)
extra_na[2L, `:=`(spill_count = NA_real_, spill_hrs = NA_real_)]
assert_error_contains(
  comparison$compare_study_period_sources(paired_events, extra_na, "sale"),
  "missingness pattern",
  "An NA present under one source but not the other must fail loudly."
)
divergent_flag <- copy(paired_annual)
divergent_flag[3L, has_missing_site := TRUE]
assert_error_contains(
  comparison$compare_study_period_sources(
    paired_events, divergent_flag, "sale"
  ),
  "has_missing_site",
  "A divergent missing-site flag must fail loudly."
)

all_na_events <- comparison_fixture(
  spill_count = rep(NA_real_, 6L), spill_hrs = rep(NA_real_, 6L)
)
all_na_report <- comparison$compare_study_period_sources(
  all_na_events, copy(all_na_events), "sale"
)
assert_true(
  all(is.na(all_na_report$pearson)) && all(is.na(all_na_report$spearman)) &&
    all(all_na_report$n_comparable == 0L),
  "An all-NA radius subset must report undefined correlations without erroring."
)
constant_events <- comparison_fixture(
  spill_count = rep(1, 6L), spill_hrs = rep(2, 6L)
)
assert_true(
  all(is.na(comparison$compare_study_period_sources(
    constant_events, copy(constant_events), "sale"
  )$pearson)),
  "A zero-variance subset must report an undefined correlation, not an error."
)

# U5: direct consumer and supported documentation -----------------------------

plot_script_path <- here::here(
  "scripts", "R", "09_analysis", "01_descriptive", "cross_sectional_plots.R"
)
plot_script_text <- paste(readLines(plot_script_path, warn = FALSE), collapse = "\n")
assert_true(
  grepl("prepare_cross_section_sales <- function", plot_script_text, fixed = TRUE) &&
    grepl("prepare_cross_section_rentals <- function", plot_script_text, fixed = TRUE),
  "The plot consumer must expose exactly the two local preparation seams."
)
assert_true(
  grepl("study_period", plot_script_text, fixed = TRUE) &&
    !grepl("all_years", plot_script_text, fixed = TRUE) &&
    !grepl("listing_price = rent", plot_script_text, fixed = TRUE) &&
    !grepl("install.packages", plot_script_text, fixed = TRUE),
  "The live plot consumer must use study_period/listing_price without runtime installs."
)
assert_true(
  grepl("if (sys.nframe() == 0L) main()", plot_script_text, fixed = TRUE),
  "The plot consumer must guard its production entry point."
)

figure_root <- here::here("output", "figures")
figure_snapshot <- if (dir.exists(figure_root)) {
  file.info(list.files(figure_root, recursive = TRUE, full.names = TRUE))$mtime
} else {
  structure(numeric(), names = character())
}
plot_environment <- new.env(parent = globalenv())
sys.source(plot_script_path, envir = plot_environment)
figure_snapshot_after <- if (dir.exists(figure_root)) {
  file.info(list.files(figure_root, recursive = TRUE, full.names = TRUE))$mtime
} else {
  structure(numeric(), names = character())
}
assert_identical(
  figure_snapshot_after,
  figure_snapshot,
  "Sourcing the plot consumer must not create or rewrite figures."
)

consumer_fixture <- CJ(
  transaction_id = 1:4,
  radius = c(250L, 500L, 1000L),
  unique = TRUE
)
consumer_fixture[, `:=`(
  price = as.integer(c(100L, 200L, 300L, 400L)[transaction_id]),
  ppd_category = c("A", "B", "A", "B")[transaction_id],
  listing_price = as.double(c(500, 1000, 1500, 2000)[transaction_id]),
  spill_count = ifelse(transaction_id == 2L, 0, ifelse(transaction_id == 3L, NA, 2)),
  spill_hrs = ifelse(transaction_id == 2L, 0, ifelse(transaction_id == 3L, NA, 4)),
  min_distance = ifelse(transaction_id %in% c(2L, 3L), NA, 100),
  spatially_eligible = transaction_id != 3L,
  has_missing_site = transaction_id == 3L
)]

sales_consumer <- plot_environment$prepare_cross_section_sales(
  consumer_fixture[, .(
    house_id = transaction_id, price, ppd_category, spill_count, spill_hrs,
    min_distance, spatially_eligible, has_missing_site, radius
  )],
  radii = c(250L, 500L, 1000L),
  sample_size = NULL
)
assert_identical(
  sort(unique(as.character(sales_consumer$ppd_category))),
  c("A", "B"),
  "Both Land Registry categories must survive primary plot preparation."
)
assert_true(
  all(sales_consumer$spill_count[sales_consumer$house_id == 2L] == 0) &&
    all(is.na(sales_consumer$inverse_spill_count[sales_consumer$house_id == 2L])) &&
    all(is.na(sales_consumer$spill_count[sales_consumer$house_id == 3L])) &&
    all(is.na(sales_consumer$inverse_spill_count[sales_consumer$house_id == 3L])),
  "Plot preparation must preserve eligible zeros and unknown exposure distinctly."
)

rental_consumer <- plot_environment$prepare_cross_section_rentals(
  consumer_fixture[, .(
    rental_id = transaction_id, listing_price, spill_count, spill_hrs,
    min_distance, spatially_eligible, has_missing_site, radius
  )],
  radii = c(250L, 500L, 1000L),
  sample_size = NULL
)
assert_true(
  "listing_price" %in% names(rental_consumer) &&
    all(rental_consumer$log_price == log(rental_consumer$listing_price)),
  "Rental preparation must trim and transform listing_price directly."
)

quarto_text <- paste(
  readLines(here::here("book", "_quarto.yml"), warn = FALSE),
  collapse = "\n"
)
assert_true(
  !grepl("house_data_exploration.qmd", quarto_text, fixed = TRUE) &&
    !grepl("zoopla_data_exploration.qmd", quarto_text, fixed = TRUE),
  "The supported book render must exclude both archival exploration chapters."
)
assert_true(
  file.exists(here::here("book", "house_data_exploration.qmd")) &&
    file.exists(here::here("book", "zoopla_data_exploration.qmd")),
  "The excluded exploration chapters must remain as archival source files."
)

documentation_text <- paste(
  readLines(here::here("docs", "pipeline_documentation.md"), warn = FALSE),
  readLines(
    here::here("book", "data_clean_documentation", "01_pipeline.qmd"),
    warn = FALSE
  ),
  collapse = "\n"
)
assert_true(
  grepl("study_period", documentation_text, fixed = TRUE) &&
    grepl("annual-return", documentation_text, fixed = TRUE) &&
    grepl("prior-to-transaction", documentation_text, fixed = TRUE),
  "Supported documentation must distinguish fixed-period and prior exposure."
)

# U2: shared measurement core, study-family seams ------------------------------

# The window reducer is `any` per Site Group across the window's years. It is
# exercised directly here; the two collapse functions consume it through
# study_period_annual_evidence_grid() since ticket 05.
core_window_universe <- expand_site_year_universe(
  data.table(
    site_id = c(50L, 50L, 60L, 60L, 60L),
    year = c(2021L, 2022L, 2021L, 2022L, 2023L),
    annual_status = c(
      "reported_zero", "reported_zero",
      "reported_zero", "reported_na", "reported_positive"
    ),
    matched_event_count = c(0L, 0L, 0L, 0L, 2L)
  ),
  site_ids = c(50L, 60L),
  years = 2021:2023
)
core_window_flags <- reduce_evidence_flags_over_window(
  classify_annual_returns_evidence(core_window_universe)
)
assert_identical(
  core_window_flags$site_id,
  c(50L, 60L),
  "The window reducer must return one row per Site Group, in site order."
)
assert_identical(
  core_window_flags$annual_returns_absent,
  c(TRUE, FALSE),
  paste(
    "Site 50's missing 2023 row must raise annual_returns_absent, and site",
    "60's complete coverage must not."
  )
)
assert_identical(
  core_window_flags$annual_returns_na,
  c(FALSE, TRUE),
  "One reported_na year anywhere in the window must raise the NA flag."
)
assert_identical(
  core_window_flags$reported_positive_without_matched_events,
  c(FALSE, FALSE),
  "A reported_positive year with matched events must raise no flag."
)

# Clip equivalence: the shared clip with the study window's two constants must
# reproduce study_period_clip_events() on a fixture straddling both edges.
core_study_events <- data.table(
  site_id = c(70L, 70L, 70L, 70L, 70L),
  start_time = utc_time(c(
    "2020-12-31 12:00:00", "2022-06-01 00:00:00", "2024-12-31 12:00:00",
    "2020-01-01 00:00:00", "2020-12-30 00:00:00"
  )),
  end_time = utc_time(c(
    "2021-01-01 06:00:00", "2022-06-02 00:00:00", "2025-01-02 00:00:00",
    "2020-06-01 00:00:00", "2021-01-01 00:00:00"
  ))
)
core_study_clipped <- clip_events_to_window(
  core_study_events,
  utc_time("2021-01-01 00:00:00"),
  utc_time("2025-01-01 00:00:00")
)
assert_identical(
  core_study_clipped[, .(site_id, clipped_start, clipped_end, event_hours)],
  study_period_clip_events(core_study_events, window_2021_2024),
  "The shared clip must reproduce the study engine's clipping exactly."
)
assert_identical(
  core_study_clipped$event_hours,
  c(6, 24, 12),
  paste(
    "Clipping must keep only the in-window overlap and drop both the event",
    "wholly before the window and the one that merely touches its start."
  )
)

# Collapse equivalence: the shared collapse grouped by site alone must
# reproduce the totals collapse_study_period_events() computes today.
core_study_collapsed <- collapse_events_by_group(
  clip_events_to_window(
    events_fixture,
    utc_time("2021-01-01 00:00:00"),
    utc_time("2025-01-01 00:00:00")
  ),
  by = "site_id"
)
setkey(core_study_collapsed, site_id)
assert_identical(
  core_study_collapsed[.(10L), .(spill_hrs, spill_count)],
  events_collapsed[.(10L), .(spill_hrs, spill_count)],
  "The shared collapse must reproduce the study engine's per-site totals."
)
assert_identical(
  core_study_collapsed[.(20L), .(spill_hrs, spill_count)],
  data.table(spill_hrs = 5, spill_count = 1),
  paste(
    "The shared collapse must total a Site Group the engine later masks for",
    "missing evidence, since masking is not the collapse's job."
  )
)
assert_identical(
  core_study_collapsed$site_id,
  c(10L, 20L, 30L),
  paste(
    "The shared collapse must cover only Site Groups with events; the",
    "evidence grid is what restores the true zeros."
  )
)

# The study engine orders by site and clipped start alone, and sums without
# na.rm so an unexpected NA reaches its own guard. Both are the defaults, so
# the engine inherits today's behaviour by calling the core plainly.
assert_identical(
  formals(collapse_events_by_group)$order_by,
  "clipped_start",
  "The default sort key must match the study engine's existing row order."
)
assert_identical(
  formals(collapse_events_by_group)$na.rm,
  FALSE,
  "The default must not absorb a missing event hour the study engine guards on."
)

# An event feed with nothing in the window must survive the whole core without
# the study engine's separate empty-input branch.
core_empty_clipped <- clip_events_to_window(
  events_fixture,
  utc_time("2030-01-01 00:00:00"),
  utc_time("2031-01-01 00:00:00")
)
assert_identical(
  names(core_empty_clipped),
  c(names(events_fixture), "clipped_start", "clipped_end", "event_hours"),
  "An empty clip must still carry the clipped columns, correctly typed."
)
assert_identical(
  nrow(collapse_events_by_group(core_empty_clipped, by = "site_id")),
  0L,
  "Collapsing an empty clip must yield no rows rather than failing."
)

cat("Study-period cross-section contract tests passed (U1-U5).\n")
