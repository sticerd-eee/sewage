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

fixtures <- list(
  house_property = tibble(
    house_id = c("h1", "h2"), site_id = c(10L, 20L),
    distance_m = c(100, 125), distance_km = c(0.1, 0.125),
    n_site_groups = c(1L, 1L)
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
    site_id = c(10L, 20L), year = 2023L, water_company = c("A", "B"),
    start_time = as.POSIXct(c("2023-01-01", "2023-02-01"), tz = "UTC"),
    end_time = as.POSIXct(
      c("2023-01-01 01:00:00", "2023-02-01 01:00:00"), tz = "UTC"
    ),
    ngr = c("SU1000010000", "SU2000020000"),
    rainfall_1cell_d01_na_rm = c(0, 1), rainfall_1cell_d01_strict = c(0, 1),
    rainfall_max_9cell_d01_na_rm = c(0, 1), rainfall_max_9cell_d01_strict = c(0, 1),
    rainfall_max_9cell_d0123_na_rm = c(0, 1), rainfall_max_9cell_d0123_strict = c(0, 1)
  ),
  exposure = tibble(
    category = rep(c("Any Spill", "Dry Spills"), each = 2L),
    distance_m = rep(c(50, 100), 2L),
    n_sites = rep(c(2L, 1L), each = 2L),
    population = c(1000, 1800, 400, 750)
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
configured_paths <- reconciliation_env$consumer_artifact_paths_from_env()
assert_true(
  identical(configured_paths, paths),
  "Consumer paths must be read from their artifact-specific environment variables."
)

identical_checks <- reconciliation_env$reconcile_consumer_artifacts()
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

# A value change is unexplained drift.
drifted_house <- fixtures$house_property |>
  mutate(distance_m = replace(.data$distance_m, 1L, 101))
readr::write_csv(drifted_house, paths$house_property$candidate, na = "")
assert_error_contains(
  reconciliation_env$reconcile_consumer_artifacts(paths),
  "house_property consumer artifact drift",
  "A changed property-sidecar value must fail real consumer reconciliation."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

# Missing configured paths remain named publication gates.
missing_paths <- paths
missing_paths$rainfall$baseline <- ""
missing_paths$map_support$candidate <- file.path(test_dir, "not-published.parquet")
missing_checks <- reconciliation_env$reconcile_consumer_artifacts(missing_paths)
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

unconfigured_paths <- lapply(contracts, function(contract) {
  list(baseline = "", candidate = "")
})
all_pending_checks <- reconciliation_env$reconcile_consumer_artifacts(
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
  reconciliation_env$reconcile_consumer_artifacts(paths),
  "house_property candidate schema mismatch",
  "The legacy property count name must not be normalized on candidate sidecars."
)
readr::write_csv(fixtures$house_property, paths$house_property$candidate, na = "")

# Sidecars must contain complete, unique declared keys.
missing_property_key <- fixtures$house_property |>
  mutate(site_id = replace(.data$site_id, 1L, NA_integer_))
readr::write_csv(missing_property_key, paths$house_property$candidate, na = "")
assert_error_contains(
  reconciliation_env$reconcile_consumer_artifacts(paths),
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
  reconciliation_env$reconcile_consumer_artifacts(paths),
  "rental_property candidate is not unique",
  "A duplicate property-sidecar key must fail before comparison."
)
readr::write_csv(fixtures$rental_property, paths$rental_property$candidate, na = "")

# The declared schema is exact: neither missing nor invented columns are accepted.
wrong_exposure_schema <- fixtures$exposure |>
  rename(group = "category")
readr::write_csv(wrong_exposure_schema, paths$exposure$candidate, na = "")
assert_error_contains(
  reconciliation_env$reconcile_consumer_artifacts(paths),
  "exposure candidate schema mismatch",
  "A fictitious exposure schema must fail the exact schema gate."
)
readr::write_csv(fixtures$exposure, paths$exposure$candidate, na = "")

extra_rainfall_column <- fixtures$rainfall |>
  mutate(period = .data$year)
readr::write_csv(extra_rainfall_column, paths$rainfall$candidate, na = "")
assert_error_contains(
  reconciliation_env$reconcile_consumer_artifacts(paths),
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
