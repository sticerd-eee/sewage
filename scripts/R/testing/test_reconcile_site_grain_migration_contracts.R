############################################################
# Site Grain Migration Reconciliation Contract Tests
############################################################

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
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

test_dir <- tempfile("site-grain-reconciliation-", tmpdir = "/private/tmp")
dir.create(test_dir)
on.exit(unlink(test_dir, recursive = TRUE), add = TRUE)

fixtures <- list(
  property = tibble(
    house_id = c("h1", "h2"), site_id = c(10L, 20L),
    distance_m = c(100, 125), distance_km = c(0.1, 0.125),
    n_site_groups = c(1L, 1L)
  ),
  rainfall = tibble(
    site_id = c(10L, 20L), water_company = c("A", "B"), year = 2023L,
    rainfall_r1_yr = c(100, 120), rainfall_r9_yr = c(140, 160)
  ),
  dry_spill = tibble(
    site_id = c(10L, 20L), year = 2023L, water_company = c("A", "B"),
    start_time = as.POSIXct(c("2023-01-01", "2023-02-01"), tz = "UTC"),
    end_time = as.POSIXct(c("2023-01-01 01:00:00", "2023-02-01 01:00:00"), tz = "UTC"),
    rainfall_1cell_d01_na_rm = c(0, 1), rainfall_1cell_d01_strict = c(0, 1),
    rainfall_max_9cell_d01_na_rm = c(0, 1), rainfall_max_9cell_d01_strict = c(0, 1),
    rainfall_max_9cell_d0123_na_rm = c(0, 1), rainfall_max_9cell_d0123_strict = c(0, 1)
  ),
  exposure = tibble(
    site_id = c(10L, 20L), period = 2023L,
    population = c(1000, 500), spill_total = c(3, 0)
  ),
  map_support = tibble(
    site_id = c(10L, 20L), period = 2023L,
    easting = c(410000, 420000), northing = c(110000, 120000),
    spill_total = c(3, 0)
  )
)

paths <- lapply(names(fixtures), function(artifact) {
  baseline_path <- file.path(test_dir, paste0(artifact, "-baseline.parquet"))
  candidate_path <- file.path(test_dir, paste0(artifact, "-candidate.parquet"))
  baseline <- fixtures[[artifact]]
  if (artifact == "property") {
    baseline <- baseline |>
      rename(n_discharge_outlet = "n_site_groups")
  }
  arrow::write_parquet(baseline, baseline_path)
  arrow::write_parquet(fixtures[[artifact]], candidate_path)
  list(baseline = baseline_path, candidate = candidate_path)
})
names(paths) <- names(fixtures)

contracts <- reconciliation_env$consumer_artifact_contracts()
env_values <- character()
for (artifact in names(contracts)) {
  env_values[contracts[[artifact]]$baseline_env] <- paths[[artifact]]$baseline
  env_values[contracts[[artifact]]$candidate_env] <- paths[[artifact]]$candidate
}
do.call(Sys.setenv, as.list(env_values))
configured_paths <- reconciliation_env$consumer_artifact_paths_from_env()
assert_true(
  identical(configured_paths, paths),
  "Consumer baseline/candidate paths must be read from their documented environment variables."
)

identical_checks <- reconciliation_env$reconcile_consumer_artifacts()
assert_true(
  identical(identical_checks$check, paste0("consumer_", names(fixtures), "_artifact")),
  "Every required real consumer artifact pair must produce a named gate."
)
assert_true(
  all(identical_checks$status == "passed"),
  "Identical on-disk baseline/candidate consumer artifacts must pass."
)
assert_true(
  all(grepl("candidate rows read from", identical_checks$detail, fixed = TRUE)),
  "Passing checks must record that candidate artifacts were read from disk."
)

drifted_property <- fixtures$property |>
  mutate(distance_m = replace(.data$distance_m, 1L, 101))
arrow::write_parquet(drifted_property, paths$property$candidate)
assert_error_contains(
  reconciliation_env$reconcile_consumer_artifacts(),
  "property consumer artifact drift",
  "A changed candidate value must fail real consumer reconciliation."
)
arrow::write_parquet(fixtures$property, paths$property$candidate)

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
  "An unavailable map-support candidate must be a named pending-publication gate."
)

unconfigured_paths <- lapply(contracts, function(contract) {
  list(baseline = "", candidate = "")
})
all_pending_checks <- reconciliation_env$reconcile_consumer_artifacts(
  unconfigured_paths
)
assert_true(
  identical(
    all_pending_checks$check,
    paste0("consumer_", names(contracts), "_artifact")
  ) && all(all_pending_checks$status == "pending_publication"),
  "Every unconfigured required consumer pair must remain a named pending-publication gate."
)

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
