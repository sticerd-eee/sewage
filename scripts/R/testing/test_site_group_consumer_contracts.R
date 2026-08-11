############################################################
# Site Group Consumer Contract Tests
# Project: Sewage
############################################################

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(glue)
  library(logger)
  library(purrr)
  library(sf)
  library(tibble)
})

source(here::here("scripts", "R", "utils", "site_group_utils.R"))

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

crosswalk_fixture <- tibble(
  site_id = rep(c(10L, 20L), each = 4L),
  year = rep(2021:2024, times = 2L),
  water_company = rep(c("Example Water", "Other Water"), each = 4L),
  site_id_canonical_members = rep(c("10;11", "20"), each = 4L),
  annual_status = c(
    "reported_zero", "reported_positive", "reported_na", "absent",
    "absent", "reported_zero", "reported_positive", "reported_na"
  ),
  ngr = c(
    "SU1000010000", "SU1000010000", "NOT AN NGR", "NOT AN NGR",
    NA, "SU2000020000", "SU2000020000", "SU2000020000"
  ),
  easting = c(410000, 410000, NA, NA, NA, 420000, 420000, 420000),
  northing = c(110000, 110000, NA, NA, NA, 120000, 120000, 120000)
)

projection <- derive_site_group_projection(
  crosswalk_fixture,
  years = 2021:2024,
  include_availability = TRUE
)

assert_identical(
  projection$site_id,
  c(10L, 20L),
  "A multi-member Site Group must yield one projection row, not one canonical-member row."
)
assert_identical(
  projection[projection$site_id == 10L, c("ngr", "easting", "northing")],
  tibble(ngr = "SU1000010000", easting = 410000, northing = 110000),
  "A newer unparseable location must not shadow the most recent valid group location."
)
assert_identical(
  projection[projection$site_id == 10L, paste0("available_year_", 2021:2024)] |>
    unlist(use.names = FALSE),
  c(TRUE, TRUE, TRUE, FALSE),
  "Reported zero, positive, and NA group-years must be available; absent must be unavailable."
)
assert_identical(
  projection[projection$site_id == 20L, "available_year_2024"] |>
    unlist(use.names = FALSE),
  TRUE,
  "Configured 2024 availability must be projected rather than omitted by a hardcoded year list."
)
assert_true(
  !anyDuplicated(projection$site_id),
  "The Site Group projection key must be unique."
)

duplicate_fixture <- bind_rows(crosswalk_fixture, slice(crosswalk_fixture, 1L))
assert_error_contains(
  derive_site_group_projection(duplicate_fixture, 2021:2024),
  "unique on site_id, year, water_company",
  "Duplicate group-year-company rows must fail before a consumer join."
)

company_conflict_fixture <- crosswalk_fixture
company_conflict_fixture$water_company[4] <- "Changed Water"
assert_error_contains(
  derive_site_group_projection(company_conflict_fixture, 2021:2024),
  "exactly one water_company",
  "Cross-company Site Group histories must fail instead of selecting by row order."
)

missing_year_fixture <- filter(crosswalk_fixture, !(site_id == 10L & year == 2024L))
assert_error_contains(
  derive_site_group_projection(missing_year_fixture, 2021:2024),
  "one row for every configured year",
  "Missing configured group-years must fail instead of silently manufacturing availability."
)

left_fixture <- tibble(event_id = 1:3, site_id = c(10L, 10L, 20L))
joined_fixture <- left_join_site_group_projection(
  left_fixture,
  select(projection, site_id, water_company, ngr),
  context = "event fixture"
)
assert_identical(
  nrow(joined_fixture),
  nrow(left_fixture),
  "A group metadata join must conserve the left-side event row count."
)
assert_identical(
  joined_fixture$site_id,
  left_fixture$site_id,
  "A group metadata join must preserve group IDs and left-side ordering."
)
assert_error_contains(
  left_join_site_group_projection(
    left_fixture,
    bind_rows(projection, slice(projection, 1L)),
    context = "duplicate projection fixture"
  ),
  "must be unique on site_id",
  "A duplicate Site Group projection must fail before joining."
)

# Real crosswalk integration: one group row and configured 2024 availability.
real_crosswalk_path <- here::here(
  "data", "processed", "matched_events_annual_data",
  "site_group_crosswalk.parquet"
)
real_crosswalk <- arrow::read_parquet(real_crosswalk_path)
real_projection <- derive_site_group_projection(
  real_crosswalk,
  years = 2021:2024,
  include_availability = TRUE
)
assert_identical(
  nrow(real_projection),
  dplyr::n_distinct(real_crosswalk$site_id),
  "The production crosswalk must project to exactly one row per Site Group."
)
assert_true(
  "available_year_2024" %in% names(real_projection),
  "The production projection must include configured 2024 availability."
)

# Rainfall-grid integration: a multi-member group remains one spatial point.
clean_rainfall_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "02_data_cleaning", "clean_rainfall_data.R"),
  envir = clean_rainfall_env
)
grid_bounds <- list(
  xbound = rbind(seq(400000, 430000, by = 1000), seq(401000, 431000, by = 1000)),
  ybound = rbind(seq(100000, 130000, by = 1000), seq(101000, 131000, by = 1000))
)
grid_fixture <- projection |>
  filter(!is.na(easting), !is.na(northing)) |>
  select(site_id, ngr, easting, northing) |>
  as.data.table()
grid_lookup <- clean_rainfall_env$create_spill_site_lookup(
  grid_fixture,
  grid_bounds,
  radius = 1L
)
assert_identical(
  nrow(grid_lookup),
  9L * nrow(grid_fixture),
  "The rainfall-grid fixture must create nine cells per Site Group, not per canonical member."
)
assert_true(
  !anyDuplicated(grid_lookup[, .(site_id, x_idx, y_idx)]),
  "The rainfall-grid lookup key must remain unique."
)

# Daily-panel integration: the consumer selects availability from configured years.
daily_panel_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "03_data_enrichment", "aggregate_daily_spill_rainfall.R"),
  envir = daily_panel_env
)
daily_crosswalk_path <- tempfile("site-group-daily-crosswalk-", fileext = ".parquet")
arrow::write_parquet(crosswalk_fixture, daily_crosswalk_path)
daily_panel_env$CONFIG$site_group_crosswalk_path <- daily_crosswalk_path
daily_panel_env$CONFIG$site_group_years <- 2021:2024
daily_sites <- daily_panel_env$load_spill_sites()
assert_true(
  "available_year_2024" %in% names(daily_sites),
  "The daily-panel consumer must select 2024 through configured Site Group years."
)
assert_identical(
  nrow(daily_sites),
  nrow(projection),
  "The daily-panel consumer must load one row per Site Group."
)

# Rainfall aggregation integration: metadata attachment conserves the site-day grid.
rainfall_aggregation_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "03_data_enrichment", "aggregate_rainfall_stats.R"),
  envir = rainfall_aggregation_env
)
rainfall_aggregation_env$CONFIG$start_date <- as.Date("2024-01-01")
rainfall_aggregation_env$CONFIG$end_date <- as.Date("2024-01-02")
rainfall_grid <- rainfall_aggregation_env$create_chunk_site_day_grid(
  projection |>
    select(site_id, water_company, ngr) |>
    as.data.table()
)
assert_identical(
  nrow(rainfall_grid),
  2L * nrow(projection),
  "Rainfall Site Group metadata attachment must conserve the two-day fixture grid."
)

# Dry-spill integration: event rows survive the crosswalk projection join.
dry_spill_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "03_data_enrichment", "identify_dry_spills.R"),
  envir = dry_spill_env
)
dry_temp_dir <- tempfile("site-group-dry-contract-")
dir.create(dry_temp_dir)
dry_events_path <- file.path(dry_temp_dir, "events.parquet")
dry_crosswalk_path <- file.path(dry_temp_dir, "crosswalk.parquet")
dry_events <- tibble(
  site_id = c(10L, 10L, 20L),
  year = c(2021L, 2022L, 2024L),
  water_company = c("Example Water", "Example Water", "Other Water"),
  start_time = as.POSIXct(c("2021-01-01", "2022-01-01", "2024-01-01"), tz = "UTC"),
  end_time = as.POSIXct(c("2021-01-01 01:00:00", "2022-01-01 01:00:00", "2024-01-01 01:00:00"), tz = "UTC")
)
arrow::write_parquet(dry_events, dry_events_path)
arrow::write_parquet(crosswalk_fixture, dry_crosswalk_path)
dry_spill_env$CONFIG$spills_file <- dry_events_path
dry_spill_env$CONFIG$site_group_crosswalk_file <- dry_crosswalk_path
dry_spill_env$CONFIG$site_group_years <- 2021:2024
dry_joined <- dry_spill_env$load_spill_data()
assert_identical(
  nrow(dry_joined),
  nrow(dry_events),
  "Dry-spill Site Group attachment must conserve event rows."
)
assert_identical(
  dry_joined$site_id,
  dry_events$site_id,
  "Dry-spill Site Group attachment must preserve group IDs and ordering."
)

# Property integration: each group is one point and the renamed count is unchanged.
property_projection <- projection |>
  filter(!is.na(easting), !is.na(northing))
house_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "04_feature_engineering", "10km_site_house_sale_match.R"),
  envir = house_env
)
house_spills <- house_env$prepare_spill_sites(property_projection)
house_points <- house_env$prepare_house_data(tibble(
  house_id = c("h1", "h2"),
  easting = property_projection$easting,
  northing = property_projection$northing
))
house_lookup <- house_env$perform_spatial_join(
  house_points,
  house_spills$spill_sf,
  house_spills$lookup,
  radius_km = 0.1
)
assert_identical(
  nrow(house_lookup),
  nrow(house_points),
  "House fixtures must retain one row per property when each has one nearby Site Group."
)
assert_identical(
  house_lookup$n_site_groups,
  c(1L, 1L),
  "The renamed house count must preserve the pre-migration one-group values."
)
assert_true(
  !"n_discharge_outlet" %in% names(house_lookup),
  "House lookups must omit the legacy n_discharge_outlet count."
)
assert_true(
  !anyDuplicated(house_lookup[c("house_id", "site_id")]),
  "House lookup keys must remain unique."
)

rental_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "04_feature_engineering", "10km_site_rental_match.R"),
  envir = rental_env
)
rental_spills <- rental_env$prepare_spill_sites(property_projection)
rental_points <- rental_env$prepare_rental_data(tibble(
  rental_id = c("r1", "r2"),
  easting = property_projection$easting,
  northing = property_projection$northing
))
rental_lookup <- rental_env$perform_spatial_join(
  rental_points,
  rental_spills$spill_sf,
  rental_spills$lookup,
  radius_km = 0.1,
  chunk_size = 1L
)
assert_identical(
  nrow(rental_lookup),
  nrow(rental_points),
  "Rental fixtures must retain one row per property when each has one nearby Site Group."
)
assert_identical(
  rental_lookup$n_site_groups,
  c(1L, 1L),
  "The renamed rental count must preserve the pre-migration one-group values."
)
assert_true(
  !"n_discharge_outlet" %in% names(rental_lookup),
  "Rental lookups must omit the legacy n_discharge_outlet count."
)
assert_true(
  !anyDuplicated(rental_lookup[c("rental_id", "site_id")]),
  "Rental lookup keys must remain unique."
)

cat("All Site Group consumer contract tests passed.\n")
