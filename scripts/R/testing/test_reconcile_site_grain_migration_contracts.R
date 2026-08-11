############################################################
# Site Grain Migration Reconciliation Contract Tests
############################################################

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
  library(readr)
  library(tibble)
})

reconciliation_env <- new.env(parent = globalenv())
sys.source(
  here::here("scripts", "R", "testing", "reconcile_site_grain_migration.R"),
  envir = reconciliation_env
)

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
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

write_fixture_pair <- function(data, artifact, legacy_property_baseline = FALSE) {
  baseline_path <- file.path(test_dir, paste0(artifact, "-baseline.parquet"))
  candidate_path <- file.path(test_dir, paste0(artifact, "-candidate.csv"))
  baseline <- data
  if (legacy_property_baseline) {
    baseline <- baseline |>
      rename(n_discharge_outlet = "n_site_groups")
  }
  arrow::write_parquet(baseline, baseline_path)
  readr::write_csv(data, candidate_path, na = "")
  list(baseline = baseline_path, candidate = candidate_path)
}

test_dir <- tempfile("site-grain-reconciliation-", tmpdir = "/private/tmp")
dir.create(test_dir)
on.exit(unlink(test_dir, recursive = TRUE), add = TRUE)

baseline_agg_dir <- file.path(test_dir, "aggregate-baseline")
candidate_agg_dir <- file.path(test_dir, "aggregate-candidate")
dir.create(baseline_agg_dir)
dir.create(candidate_agg_dir)
aggregate_fixtures <- list(
  agg_spill_yr.parquet = tibble(
    site_id = 10L, water_company = "A", year = 2023L,
    spill_count_yr = 1L, spill_hrs_yr = 2, annual_status = "reported_positive"
  ),
  agg_spill_mo.parquet = tibble(
    site_id = 10L, water_company = "A", month_id = "2023-01",
    spill_count_mo = 1L, spill_hrs_mo = 2, annual_status = "reported_positive"
  ),
  agg_spill_qtr.parquet = tibble(
    site_id = 10L, water_company = "A", qtr_id = "2023-Q1",
    spill_count_qt = 1L, spill_hrs_qt = 2, annual_status = "reported_positive"
  )
)
for (file in names(aggregate_fixtures)) {
  arrow::write_parquet(aggregate_fixtures[[file]], file.path(baseline_agg_dir, file))
  arrow::write_parquet(aggregate_fixtures[[file]], file.path(candidate_agg_dir, file))
}
aggregate_checks <- reconciliation_env$reconcile_aggregate_outputs(
  baseline_agg_dir, candidate_agg_dir
)
assert_true(
  nrow(aggregate_checks) == 3L && all(aggregate_checks$status == "passed"),
  "Real aggregate contracts must compare their vector keys and values."
)

fixtures <- list(
  house_property = tibble(
    house_id = c("h1", "h1", "h2", "h3", "h4"),
    site_id = c(10L, 20L, 10L, 20L, -1L),
    distance_m = c(100, 125, 200, 300, NA_real_),
    distance_km = c(0.1, 0.125, 0.2, 0.3, NA_real_),
    n_site_groups = c(2L, 2L, 1L, 1L, 0L)
  ),
  rental_property = tibble(
    rental_id = c("r1", "r2"), site_id = c(10L, 20L),
    distance_m = c(80, 140), distance_km = c(0.08, 0.14),
    n_site_groups = c(1L, 1L)
  ),
  rainfall = tibble(
    site_id = c(10L, 20L), water_company = c("A", "B"), year = 2023L,
    rainfall_r1_yr = c(100, 120), rainfall_r9_yr = c(140, 160)
  ),
  dry_spill = tibble(
    site_id = c(10L, 10L, 20L), year = 2023L,
    water_company = c("A", "A", "B"),
    start_time = as.POSIXct(
      c("2023-01-01", "2023-01-02", "2023-02-01"), tz = "UTC"
    ),
    end_time = as.POSIXct(
      c(
        "2023-01-01 01:00:00", "2023-01-02 01:00:00",
        "2023-02-01 01:00:00"
      ),
      tz = "UTC"
    ),
    ngr = c("SU1000010000", "SU1000010000", "SU2000020000"),
    rainfall_1cell_d01_na_rm = c(0, 0.5, 1),
    rainfall_1cell_d01_strict = c(0, 0.5, 1),
    rainfall_max_9cell_d01_na_rm = c(0, 0.5, 1),
    rainfall_max_9cell_d01_strict = c(0, 0.5, 1),
    rainfall_max_9cell_d0123_na_rm = c(0, 0.5, 1),
    rainfall_max_9cell_d0123_strict = c(0, 0.5, 1)
  ),
  exposure = tibble(
    category = rep(c("Any Spill", "Dry Spills", "Zero Spills"), each = 5L),
    distance_m = rep(c(50, 100, 250, 500, 1000), 3L),
    n_sites = rep(c(2L, 1L, 1L), each = 5L),
    population = c(
      10000, 20000, 30000, 40000, 50000,
      5000, 10000, 15000, 20000, 25000,
      3000, 6000, 9000, 12000, 15000
    )
  ),
  map_support = tibble(
    site_id = c(10L, 20L), period = 2023L,
    easting = c(410000, 420000), northing = c(110000, 120000),
    spill_total = c(3, 0)
  )
)

paths <- lapply(names(fixtures), function(artifact) {
  write_fixture_pair(
    fixtures[[artifact]],
    artifact,
    legacy_property_baseline = artifact %in% c("house_property", "rental_property")
  )
})
names(paths) <- names(fixtures)

authoritative_locations <- tribble(
  ~site_id, ~ngr, ~easting, ~northing,
  10L, "SU1000010000", 410000, 110000,
  20L, "SU2000020000", 420000, 120000
)
location_paths <- list(
  baseline = file.path(test_dir, "site-group-locations-baseline.parquet"),
  candidate = file.path(test_dir, "site-group-locations-candidate.csv")
)
arrow::write_parquet(authoritative_locations, location_paths$baseline)
readr::write_csv(authoritative_locations, location_paths$candidate, na = "")

contracts <- reconciliation_env$consumer_artifact_contracts()
assert_true(
  identical(names(contracts), names(fixtures)),
  "Reconciliation must declare the six real consumer artifact contracts explicitly."
)
assert_true(
  all(vapply(contracts, function(contract) {
    !is.null(contract$key) && is.null(contract$key_options)
  }, logical(1))),
  "Every consumer artifact must have one unambiguous supported key."
)
assert_true(
  identical(contracts$exposure$key, c("category", "distance_m")) &&
    identical(contracts$exposure$values, c("n_sites", "population")),
  "Exposure reconciliation must use the real population-table schema."
)

env_values <- character()
for (artifact in names(contracts)) {
  env_values[contracts[[artifact]]$baseline_env] <- paths[[artifact]]$baseline
  env_values[contracts[[artifact]]$candidate_env] <- paths[[artifact]]$candidate
}
do.call(Sys.setenv, as.list(env_values))
Sys.setenv(
  SITE_GRAIN_BASELINE_SITE_GROUP_PROJECTION = location_paths$baseline,
  SITE_GRAIN_NEW_SITE_GROUP_PROJECTION = location_paths$candidate
)
configured_paths <- reconciliation_env$consumer_artifact_paths_from_env()
assert_true(
  identical(configured_paths, paths),
  "Consumer paths must be read from their artifact-specific environment variables."
)
assert_true(
  identical(
    reconciliation_env$authoritative_location_paths_from_env(), location_paths
  ),
  "Authoritative baseline/candidate Site Group projections must use explicit paths."
)

reconcile <- function(
    artifact_paths = paths,
    location_paths_override = location_paths,
    expected_candidate = fixtures$exposure,
    expected_baseline = fixtures$exposure) {
  reconciliation_env$reconcile_consumer_artifacts(
    artifact_paths,
    location_paths = location_paths_override,
    exposure_expectations = list(
      baseline = expected_baseline,
      candidate = expected_candidate
    )
  )
}

identical_checks <- reconcile()
expected_checks <- paste0("consumer_", names(fixtures), "_artifact")
assert_true(
  identical(identical_checks$check, expected_checks),
  "Every required real consumer artifact pair must produce a named gate."
)
assert_true(
  all(identical_checks$status == "passed"),
  "Matching on-disk Parquet/CSV consumer artifacts must pass."
)
assert_true(
  all(grepl("candidate rows read from", identical_checks$detail, fixed = TRUE)),
  "Passing checks must record that candidate artifacts were read from disk."
)

exposure_input_root <- file.path(test_dir, "exposure-inputs")
baseline_exposure_agg <- file.path(exposure_input_root, "baseline")
candidate_exposure_agg <- file.path(exposure_input_root, "candidate")
dir.create(baseline_exposure_agg, recursive = TRUE)
dir.create(candidate_exposure_agg, recursive = TRUE)
for (directory in c(baseline_exposure_agg, candidate_exposure_agg)) {
  invisible(file.create(file.path(
    directory, c("agg_spill_mo.parquet", "agg_spill_dry_mo.parquet")
  )))
}
population_raster <- file.path(exposure_input_root, "population.tif")
invisible(file.create(population_raster))
original_recompute <- reconciliation_env$recompute_exposure_expectations
reconciliation_env$recompute_exposure_expectations <- function(...) {
  list(baseline = fixtures$exposure, candidate = fixtures$exposure)
}
configured_exposure_checks <- reconciliation_env$reconcile_consumer_artifacts(
  paths,
  location_paths = location_paths,
  exposure_inputs = list(
    baseline_agg_dir = baseline_exposure_agg,
    candidate_agg_dir = candidate_exposure_agg,
    population_raster = population_raster
  )
)
reconciliation_env$recompute_exposure_expectations <- original_recompute
assert_true(
  configured_exposure_checks$status[
    configured_exposure_checks$check == "consumer_exposure_artifact"
  ] == "passed",
  "Existing exposure recomputation inputs must not be reported as unavailable."
)

# A value change is unexplained drift.
drifted_house <- fixtures$house_property |>
  mutate(
    distance_m = replace(.data$distance_m, 1L, 101),
    distance_km = replace(.data$distance_km, 1L, 0.101)
  )
readr::write_csv(drifted_house, paths$house_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "house_property consumer artifact unrelated drift",
  "A changed property-sidecar value must fail real consumer reconciliation."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

invalid_house_distance <- fixtures$house_property |>
  mutate(
    distance_km = if_else(
      .data$house_id == "h1" & .data$site_id == 10L,
      -0.1,
      .data$distance_km
    )
  )
readr::write_csv(invalid_house_distance, paths$house_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "matched rows must have non-negative, internally consistent distances",
  "Negative or inconsistent property distances must fail closed."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

# The map sidecar defines the exact Site Groups eligible for spatial-policy drift.
spatial_candidates <- fixtures
spatial_candidates$map_support <- fixtures$map_support |>
  mutate(easting = if_else(.data$site_id == 10L, .data$easting + 25, .data$easting))
spatial_candidate_locations <- authoritative_locations |>
  mutate(easting = if_else(.data$site_id == 10L, .data$easting + 25, .data$easting))
spatial_candidates$rainfall <- fixtures$rainfall |>
  mutate(
    rainfall_r1_yr = if_else(
      .data$site_id == 10L, .data$rainfall_r1_yr + 1, .data$rainfall_r1_yr
    )
  )
spatial_candidates$dry_spill <- fixtures$dry_spill |>
  filter(!(.data$site_id == 10L & .data$start_time == as.POSIXct(
    "2023-01-01", tz = "UTC"
  ))) |>
  mutate(
    rainfall_1cell_d01_na_rm = if_else(
      .data$site_id == 10L,
      .data$rainfall_1cell_d01_na_rm + 0.1,
      .data$rainfall_1cell_d01_na_rm
    )
  ) |>
  bind_rows(
    fixtures$dry_spill |>
      slice(1L) |>
      mutate(
        start_time = as.POSIXct("2023-01-03", tz = "UTC"),
        end_time = as.POSIXct("2023-01-03 01:00:00", tz = "UTC")
      )
  )
spatial_candidates$house_property <- tribble(
  ~house_id, ~site_id, ~distance_m, ~distance_km, ~n_site_groups,
  "h1", 10L, 110, 0.11, 2L,
  "h1", 20L, 125, 0.125, 2L,
  "h3", 20L, 300, 0.3, 2L,
  "h3", 10L, 250, 0.25, 2L,
  "h4", -1L, NA_real_, NA_real_, 0L
)
spatial_candidates$exposure <- fixtures$exposure |>
  mutate(
    n_sites = if_else(
      .data$category == "Dry Spills", .data$n_sites + 1L, .data$n_sites
    ),
    population = case_when(
      .data$category == "Any Spill" ~ .data$population + 3000,
      .data$category == "Dry Spills" ~ .data$population + 2000,
      TRUE ~ .data$population
    )
  )
for (artifact in names(spatial_candidates)) {
  readr::write_csv(
    spatial_candidates[[artifact]], paths[[artifact]]$candidate, na = ""
  )
}
readr::write_csv(spatial_candidate_locations, location_paths$candidate, na = "")
spatial_checks <- reconcile(
  paths, expected_candidate = spatial_candidates$exposure
)
assert_true(
  all(spatial_checks$status == "passed"),
  "Every change attributable to the representative-location policy must pass."
)
assert_true(
  grepl(
    "1 Site Group(s) classified", spatial_checks$detail[
      spatial_checks$check == "consumer_map_support_artifact"
    ], fixed = TRUE
  ) &&
    grepl(
      "1 removed key(s), 1 added key(s), 1 changed value row(s)",
      spatial_checks$detail[spatial_checks$check == "consumer_dry_spill_artifact"],
      fixed = TRUE
    ) &&
    grepl(
      "1 count row(s)", spatial_checks$detail[
        spatial_checks$check == "consumer_house_property_artifact"
      ], fixed = TRUE
    ) &&
    grepl(
      "10 aggregate row(s) and 15 value cell(s)",
      spatial_checks$detail[spatial_checks$check == "consumer_exposure_artifact"],
      fixed = TRUE
    ) &&
    grepl(
      "categories Any Spill, Dry Spills; n_sites category deltas Dry Spills +1",
      spatial_checks$detail[spatial_checks$check == "consumer_exposure_artifact"],
      fixed = TRUE
    ),
  "Passing spatial-policy gates must report explicit classified counts."
)
for (artifact in names(fixtures)) {
  readr::write_csv(fixtures[[artifact]], paths[[artifact]]$candidate, na = "")
}
readr::write_csv(authoritative_locations, location_paths$candidate, na = "")

# An independently observed representative NGR change can explain NGR-only
# dry-spill drift even when the parsed coordinates are unchanged.
ngr_candidate_locations <- authoritative_locations |>
  mutate(ngr = if_else(.data$site_id == 10L, "SU10001000", .data$ngr))
ngr_candidate_dry <- fixtures$dry_spill |>
  mutate(ngr = if_else(.data$site_id == 10L, "SU10001000", .data$ngr))
readr::write_csv(ngr_candidate_locations, location_paths$candidate, na = "")
readr::write_csv(ngr_candidate_dry, paths$dry_spill$candidate, na = "")
ngr_checks <- reconcile(paths)
assert_true(
  all(ngr_checks$status == "passed"),
  paste0(
    "An authoritative NGR-only representative-location change must explain ",
    "dry-spill NGR drift."
  )
)
readr::write_csv(fixtures$dry_spill, paths$dry_spill$candidate, na = "")
readr::write_csv(authoritative_locations, location_paths$candidate, na = "")

# Unrelated map totals, rainfall values, dry keys, and property distances fail closed.
unrelated_map <- fixtures$map_support |>
  mutate(spill_total = replace(.data$spill_total, 1L, 4))
readr::write_csv(unrelated_map, paths$map_support$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "map_support consumer artifact unrelated drift",
  "Map spill-total drift must not be excused as a location-policy change."
)
readr::write_csv(fixtures$map_support, paths$map_support$candidate, na = "")

arbitrary_map_location <- fixtures$map_support |>
  mutate(easting = replace(.data$easting, 1L, .data$easting[1L] + 25))
readr::write_csv(arbitrary_map_location, paths$map_support$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "map_support candidate coordinates do not match authoritative Site Group projection",
  "Map coordinate drift must not circularly authorize its own downstream changes."
)

out_of_domain_map <- fixtures$map_support |>
  mutate(easting = replace(.data$easting, 1L, 900000))
out_of_domain_locations <- authoritative_locations |>
  mutate(easting = replace(.data$easting, 1L, 900000))
readr::write_csv(out_of_domain_map, paths$map_support$candidate, na = "")
readr::write_csv(out_of_domain_locations, location_paths$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "outside the British National Grid domain",
  "Corrupt coordinates must fail even when map and projection sidecars agree."
)
readr::write_csv(spatial_candidates$map_support, paths$map_support$candidate, na = "")
readr::write_csv(spatial_candidate_locations, location_paths$candidate, na = "")

unrelated_rainfall <- fixtures$rainfall |>
  mutate(rainfall_r1_yr = if_else(
    .data$site_id == 20L, .data$rainfall_r1_yr + 1, .data$rainfall_r1_yr
  ))
readr::write_csv(unrelated_rainfall, paths$rainfall$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "rainfall consumer artifact unrelated drift",
  "Rainfall drift outside changed-location Site Groups must fail."
)
readr::write_csv(fixtures$rainfall, paths$rainfall$candidate, na = "")

unrelated_dry <- fixtures$dry_spill |>
  bind_rows(
    fixtures$dry_spill |>
      filter(.data$site_id == 20L) |>
      mutate(
        start_time = as.POSIXct("2023-02-02", tz = "UTC"),
        end_time = as.POSIXct("2023-02-02 01:00:00", tz = "UTC")
      )
  )
readr::write_csv(unrelated_dry, paths$dry_spill$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "dry_spill consumer artifact unrelated drift",
  "Dry-spill membership drift outside changed-location Site Groups must fail."
)
readr::write_csv(fixtures$dry_spill, paths$dry_spill$candidate, na = "")

unrelated_property <- fixtures$house_property |>
  mutate(
    distance_m = if_else(
      .data$house_id == "h1" & .data$site_id == 20L,
      .data$distance_m + 1,
      .data$distance_m
    ),
    distance_km = if_else(
      .data$house_id == "h1" & .data$site_id == 20L,
      .data$distance_km + 0.001,
      .data$distance_km
    )
  )
readr::write_csv(unrelated_property, paths$house_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "house_property consumer artifact unrelated drift",
  paste0(
    "An affected property neighborhood must not excuse distance drift for an ",
    "unchanged Site Group."
  )
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")
readr::write_csv(fixtures$map_support, paths$map_support$candidate, na = "")
readr::write_csv(authoritative_locations, location_paths$candidate, na = "")

unattributed_exposure <- spatial_candidates$exposure |>
  mutate(population = replace(.data$population, 1L, .data$population[1L] + 1000))
readr::write_csv(spatial_candidates$map_support, paths$map_support$candidate, na = "")
readr::write_csv(spatial_candidate_locations, location_paths$candidate, na = "")
readr::write_csv(unattributed_exposure, paths$exposure$candidate, na = "")
assert_error_contains(
  reconcile(paths, expected_candidate = spatial_candidates$exposure),
  "exposure candidate sidecar does not equal independent recomputation",
  "Exposure drift must match independently recomputed candidate evidence exactly."
)
readr::write_csv(fixtures$exposure, paths$exposure$candidate, na = "")
readr::write_csv(fixtures$map_support, paths$map_support$candidate, na = "")
readr::write_csv(authoritative_locations, location_paths$candidate, na = "")

invalid_recomputed_exposure <- fixtures$exposure |>
  mutate(population = replace(.data$population, 1L, .data$population[1L] + 500))
assert_error_contains(
  reconcile(paths, expected_candidate = invalid_recomputed_exposure),
  "population must use non-negative nearest-1000 precision",
  "Injected recomputation evidence must satisfy publication-domain constraints."
)

# Missing configured paths remain named publication gates.
missing_paths <- paths
missing_paths$rainfall$baseline <- ""
missing_paths$map_support$candidate <- file.path(test_dir, "not-published.parquet")
missing_checks <- reconcile(missing_paths)
assert_true(
  missing_checks$status[missing_checks$check == "consumer_rainfall_artifact"] ==
    "pending_publication",
  "A missing rainfall baseline path must be a named pending-publication gate."
)
assert_true(
  missing_checks$status[missing_checks$check == "consumer_map_support_artifact"] ==
    "pending_publication",
  "An unavailable map-support sidecar must be a named pending-publication gate."
)
missing_location_paths <- location_paths
missing_location_paths$candidate <- ""
missing_location_checks <- reconcile(
  paths, location_paths_override = missing_location_paths
)
assert_true(
  missing_location_checks$status[
    missing_location_checks$check == "consumer_map_support_artifact"
  ] == "pending_publication",
  "Missing authoritative projections must leave the map gate pending."
)

unconfigured_paths <- lapply(contracts, function(contract) {
  list(baseline = "", candidate = "")
})
all_pending_checks <- reconcile(
  unconfigured_paths
)
assert_true(
  identical(all_pending_checks$check, expected_checks) &&
    all(all_pending_checks$status == "pending_publication"),
  "Every unconfigured required consumer pair must remain a named pending gate."
)
assert_true(
  all(c(
    "consumer_house_property_artifact", "consumer_rental_property_artifact"
  ) %in% all_pending_checks$check),
  "House and rental property sidecars must have separate pending gates."
)

# Only legacy baseline property sidecars receive the count-column rename.
legacy_candidate <- fixtures$house_property |>
  rename(n_discharge_outlet = "n_site_groups")
readr::write_csv(legacy_candidate, paths$house_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "house_property candidate schema mismatch",
  "The legacy property count name must not be normalized on candidate sidecars."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

# Sidecars must contain complete, unique declared keys.
missing_property_key <- fixtures$house_property |>
  mutate(site_id = replace(.data$site_id, 1L, NA_integer_))
readr::write_csv(missing_property_key, paths$house_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "house_property candidate has missing key values",
  "A property sidecar with missing site_id must fail before reconciliation."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

duplicated_rental_key <- bind_rows(
  fixtures$rental_property,
  slice(fixtures$rental_property, 1L)
)
readr::write_csv(duplicated_rental_key, paths$rental_property$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "rental_property candidate is not unique",
  "A duplicate property-sidecar key must fail before comparison."
)
readr::write_csv(fixtures$rental_property, paths$rental_property$candidate, na = "")

# The declared schema is exact: neither missing nor invented columns are accepted.
wrong_exposure_schema <- fixtures$exposure |>
  rename(group = "category")
readr::write_csv(wrong_exposure_schema, paths$exposure$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "exposure candidate schema mismatch",
  "A fictitious exposure schema must fail the exact schema gate."
)
readr::write_csv(fixtures$exposure, paths$exposure$candidate, na = "")

extra_rainfall_column <- fixtures$rainfall |>
  mutate(period = .data$year)
readr::write_csv(extra_rainfall_column, paths$rainfall$candidate, na = "")
assert_error_contains(
  reconcile(paths),
  "rainfall candidate schema mismatch",
  "Unexpected columns must fail a direct production-artifact contract."
)
readr::write_csv(fixtures$rainfall, paths$rainfall$candidate, na = "")

assert_error_contains(
  reconciliation_env$reconciliation_gate_mode(
    missing_checks, allow_pending_publication = FALSE
  ),
  "consumer_rainfall_artifact",
  "The strict migration gate must fail when a required consumer pair is pending."
)
assert_true(
  identical(
    reconciliation_env$reconciliation_gate_mode(
      missing_checks, allow_pending_publication = TRUE
    ),
    "pending_publication"
  ),
  paste0(
    "SITE_GRAIN_ALLOW_PENDING_PUBLICATION=true may permit the documented ",
    "pre-publication mode but must not report migration completion."
  )
)
assert_true(
  identical(
    reconciliation_env$reconciliation_gate_mode(
      identical_checks, allow_pending_publication = FALSE
    ),
    "complete"
  ),
  "The migration can report complete only when every real consumer pair passes."
)

cat("All Site Grain migration reconciliation contract tests passed.\n")
