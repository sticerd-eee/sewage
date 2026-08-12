# ==============================================================================
# Prior-Exposure Producer Contract Tests
# ==============================================================================
#
# Focused, deterministic contracts shared by the sale and rental builders.
# Source each producer in its own environment because the scripts intentionally
# use the same CONFIG and function names.
# Run from the repository root with plain Rscript.
#
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(here)
  library(tibble)
})

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

source_prior_exposure_producer <- function(path) {
  producer_env <- new.env(parent = globalenv())
  sys.source(here::here(path), envir = producer_env)
  sys.source(
    here::here("scripts", "R", "utils", "site_group_utils.R"),
    envir = producer_env
  )
  producer_env
}

write_rental_fixture <- function(root) {
  zoopla_dir <- file.path(root, "zoopla")
  event_dir <- file.path(root, "matched_events_annual_data")
  dir.create(zoopla_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(event_dir, recursive = TRUE, showWarnings = FALSE)

  arrow::write_parquet(
    tibble(
      rental_id = 1L,
      listing_price = 1200,
      rented_est = as.Date("2024-03-15")
    ),
    file.path(zoopla_dir, "zoopla_rentals.parquet")
  )
  arrow::write_parquet(
    tibble(rental_id = 1L, site_id = 10L, distance_m = 100),
    file.path(zoopla_dir, "spill_rental_lookup.parquet")
  )
  arrow::write_parquet(
    tibble(
      site_id = c(10L, 10L),
      start_time = as.POSIXct(
        c("2024-03-14 18:00:00", "2024-03-15 00:00:00"),
        tz = "UTC"
      ),
      end_time = as.POSIXct(
        c("2024-03-15 06:00:00", "2024-03-15 01:00:00"),
        tz = "UTC"
      ),
      year = c(2024L, 2024L)
    ),
    file.path(event_dir, "matched_events_annual_data.parquet")
  )
  arrow::write_parquet(
    tidyr::expand_grid(site_id = 10L, year = 2021:2024) |>
      mutate(
        water_company = "Test Water",
        annual_status = "reported_positive"
      ),
    file.path(event_dir, "site_group_crosswalk.parquet")
  )

  invisible(root)
}

assert_rental_time_contract <- function(producer_env, fixture_root, label) {
  producer_env$CONFIG$processed_dir <- fixture_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    fixture_root,
    "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  )

  data <- producer_env$load_data()
  rented_est <- data$rental_dt$rented_est
  expected_endpoint <- as.POSIXct("2024-03-15 00:00:00", tz = "UTC")

  assert_true(
    inherits(rented_est, "POSIXct"),
    paste(label, "must normalize collected rented_est to POSIXct")
  )
  assert_identical(
    attr(rented_est, "tzone"),
    "UTC",
    paste(label, "must normalize collected rented_est to UTC")
  )
  assert_identical(
    rented_est,
    expected_endpoint,
    paste(label, "must preserve rental Date as UTC midnight")
  )

  joined <- producer_env$create_joined_events(1L, data)
  assert_identical(
    nrow(joined$events_dt),
    1L,
    paste(label, "must retain only the event overlapping the exclusive cutoff")
  )
  assert_identical(
    joined$events_dt$clamped_end,
    expected_endpoint,
    paste(label, "must clamp the overlapping event to rental midnight UTC")
  )
  assert_true(
    all(joined$events_dt$start_time < expected_endpoint),
    paste(label, "must exclude events starting exactly at the endpoint")
  )

  sale_timestamp <- as.POSIXct("2024-03-15 00:00:00", tz = "UTC")
  expected_days <- as.integer(difftime(
    sale_timestamp,
    producer_env$CONFIG$window_start,
    units = "days"
  ))
  rental_days <- producer_env$get_rental_metadata(data$rental_dt)$n_days_in_window
  assert_identical(
    rental_days,
    expected_days,
    paste(label, "rental and isomorphic UTC sale windows must have equal length")
  )
}

assert_true(
  !"package:lubridate" %in% search(),
  "The clean-namespace fixture must run without lubridate attached"
)

fixture_root <- tempfile("prior-exposure-contract-")
dir.create(fixture_root, recursive = TRUE)
write_rental_fixture(fixture_root)

producer_paths <- c(
  rental_site = file.path(
    "scripts", "R", "06_analysis_datasets",
    "rental_spill_prior_to_rental.R"
  ),
  rental_radius = file.path(
    "scripts", "R", "06_analysis_datasets",
    "cross_section_prior_to_rental.R"
  )
)

producer_envs <- lapply(producer_paths, source_prior_exposure_producer)
for (label in names(producer_envs)) {
  assert_rental_time_contract(producer_envs[[label]], fixture_root, label)
}

message("Prior-exposure producer contract tests passed")
