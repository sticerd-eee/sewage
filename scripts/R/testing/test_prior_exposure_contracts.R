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
  assert_true(
    !"cutoff_year" %in% names(joined$events_dt),
    paste(label, "must remove cutoff_year before event reducers")
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

write_prefix_fixture <- function(root, transaction_times) {
  zoopla_dir <- file.path(root, "zoopla")
  event_dir <- file.path(root, "matched_events_annual_data")
  dir.create(zoopla_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(event_dir, recursive = TRUE, showWarnings = FALSE)

  transaction_ids <- seq_along(transaction_times)
  transaction_times <- as.POSIXct(transaction_times, tz = "UTC")
  arrow::write_parquet(
    tibble(
      house_id = as.integer(transaction_ids),
      price = as.double(transaction_ids * 100000),
      date_of_transfer = transaction_times
    ),
    file.path(root, "house_price.parquet")
  )
  arrow::write_parquet(
    tibble(
      rental_id = as.integer(transaction_ids),
      listing_price = as.double(transaction_ids * 1000),
      rented_est = as.Date(transaction_times)
    ),
    file.path(zoopla_dir, "zoopla_rentals.parquet")
  )

  lookup_rows <- tibble(
    transaction_id = c(transaction_ids, 2L, 2L),
    site_id = c(rep(10L, length(transaction_ids)), 20L, 30L),
    distance_m = c(rep(100, length(transaction_ids)), 400, 700)
  )
  arrow::write_parquet(
    transmute(
      lookup_rows,
      house_id = as.integer(.data$transaction_id),
      site_id = as.integer(.data$site_id),
      distance_m = as.double(.data$distance_m)
    ),
    file.path(root, "spill_house_lookup.parquet")
  )
  arrow::write_parquet(
    transmute(
      lookup_rows,
      rental_id = as.integer(.data$transaction_id),
      site_id = as.integer(.data$site_id),
      distance_m = as.double(.data$distance_m)
    ),
    file.path(zoopla_dir, "spill_rental_lookup.parquet")
  )

  supported_sites <- tidyr::expand_grid(site_id = c(10L, 30L), year = 2021:2024) |>
    mutate(
      water_company = "Test Water",
      annual_status = if_else(
        .data$site_id == 10L & .data$year == 2023L,
        "absent",
        "reported_positive"
      )
    )
  arrow::write_parquet(
    supported_sites,
    file.path(event_dir, "site_group_crosswalk.parquet")
  )
  arrow::write_parquet(
    tibble(
      site_id = integer(),
      start_time = as.POSIXct(character(), tz = "UTC"),
      end_time = as.POSIXct(character(), tz = "UTC"),
      year = integer()
    ),
    file.path(event_dir, "matched_events_annual_data.parquet")
  )

  invisible(root)
}

producer_specs <- list(
  sale_site = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "house_spill_prior_to_sale.R"),
    id = "house_id", transaction = "house_dt", result = "create_prior_to_sale_db",
    missing = "site_missing", grain = c("house_id", "site_id", "radius")
  ),
  rental_site = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "rental_spill_prior_to_rental.R"),
    id = "rental_id", transaction = "rental_dt", result = "create_prior_to_rental_db",
    missing = "site_missing", grain = c("rental_id", "site_id", "radius")
  ),
  sale_radius = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "cross_section_prior_to_sale.R"),
    id = "house_id", transaction = "house_dt", result = "create_prior_to_sale_db",
    missing = "has_missing_site", grain = c("house_id", "radius")
  ),
  rental_radius = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "cross_section_prior_to_rental.R"),
    id = "rental_id", transaction = "rental_dt", result = "create_prior_to_rental_db",
    missing = "has_missing_site", grain = c("rental_id", "radius")
  )
)

prefix_root <- tempfile("prior-exposure-prefix-")
dir.create(prefix_root, recursive = TRUE)
write_prefix_fixture(
  prefix_root,
  c(
    "2021-01-01 00:00:00",
    "2022-06-01 00:00:00",
    "2024-06-01 00:00:00",
    "2025-01-01 00:00:00"
  )
)

prefix_results <- list()
for (label in names(producer_specs)) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- prefix_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    prefix_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  data <- producer_env$load_data()

  assert_identical(
    data[[spec$transaction]]$cutoff_year,
    c(2020L, 2022L, 2024L, 2024L),
    paste(label, "must derive the integer exclusive cutoff year")
  )
  assert_identical(
    key(data$site_missing_dt),
    c("site_id", "cutoff_year"),
    paste(label, "must key prefix flags on both join columns")
  )
  assert_identical(
    sort(unique(data$site_missing_dt$cutoff_year)),
    2020:2024,
    paste(label, "must request every prefix through the maximum needed cutoff")
  )
  assert_true(
    !data$site_missing_dt[site_id == 10L & cutoff_year == 2022L, site_missing] &&
      data$site_missing_dt[site_id == 10L & cutoff_year == 2024L, site_missing],
    paste(label, "must keep early prefixes observed and propagate the later gap")
  )

  joined <- producer_env$create_joined_events(1:4, data)
  expected_pairs <- data$spill_lookup_dt[
    get(spec$id) %in% 1:4,
    c(spec$id, "site_id", "distance_m"),
    with = FALSE
  ]
  actual_pairs <- joined$lookup_chunk[, c(spec$id, "site_id", "distance_m"), with = FALSE]
  setorderv(expected_pairs, c(spec$id, "site_id", "distance_m"))
  setorderv(actual_pairs, c(spec$id, "site_id", "distance_m"))
  setkey(expected_pairs, NULL)
  setkey(actual_pairs, NULL)
  assert_identical(
    actual_pairs,
    expected_pairs,
    paste(label, "must conserve the exact transaction-Site Group lookup pairs")
  )
  assert_true(
    !"cutoff_year" %in% names(joined$lookup_chunk),
    paste(label, "must remove cutoff_year before reducers")
  )

  result <- producer_env[[spec$result]](data)
  prefix_results[[label]] <- result
  price_column <- if (grepl("^sale", label)) "price" else "listing_price"
  expected_columns <- if (grepl("site$", label)) {
    c(
      spec$id, price_column, "n_days_in_window", "site_id", "radius",
      "distance_m", "spill_hrs", "spill_count", "site_missing",
      "spill_count_daily_avg", "spill_hrs_daily_avg",
      "spill_count_weekly_avg", "spill_hrs_weekly_avg"
    )
  } else {
    c(
      spec$id, price_column, "n_days_in_window", "radius", "spill_hrs",
      "n_spill_sites", "spill_count", "mean_distance", "min_distance",
      "has_missing_site", "spill_count_daily_avg", "spill_hrs_daily_avg",
      "spill_count_weekly_avg", "spill_hrs_weekly_avg"
    )
  }
  expected_types <- if (grepl("site$", label)) {
    c(
      "integer", "double", "integer", "integer", "double", "double",
      "double", "double", "logical", rep("double", 4L)
    )
  } else {
    c(
      "integer", "double", "integer", "double", "double", "integer",
      "double", "double", "double", "logical", rep("double", 4L)
    )
  }
  assert_identical(names(result), expected_columns, paste(label, "must preserve its exact schema"))
  assert_identical(
    unname(vapply(result, typeof, character(1))),
    expected_types,
    paste(label, "must preserve its exact column types")
  )
  assert_true(
    !"cutoff_year" %in% names(result),
    paste(label, "must not publish cutoff_year")
  )
  assert_true(
    !anyDuplicated(result[, spec$grain, with = FALSE]),
    paste(label, "must preserve its unique public grain")
  )
  assert_true(
    all(is.nan(result[get(spec$id) == 1L, spill_count_daily_avg])),
    paste(label, "must preserve the existing zero-day NaN policy at window start")
  )

  if (grepl("site$", label)) {
    assert_true(
      all(!result[get(spec$id) == 2L & site_id == 10L, get(spec$missing)]),
      paste(label, "must not let the future 2023 gap mask a 2022 transaction")
    )
    assert_true(
      all(result[get(spec$id) == 3L & site_id == 10L, get(spec$missing)]),
      paste(label, "must mark a 2024 transaction after the 2023 gap")
    )
    assert_true(
      all(result[get(spec$id) == 2L & site_id == 20L, get(spec$missing)]),
      paste(label, "must conservatively mark an entirely absent Site Group")
    )
    assert_true(
      all(!result[get(spec$id) == 1L & site_id == 10L, get(spec$missing)]),
      paste(label, "must represent the window-start empty prefix as complete")
    )
  } else {
    assert_true(
      !result[get(spec$id) == 2L & radius == 250, get(spec$missing)],
      paste(label, "must keep missingness outside 250m from invalidating 250m")
    )
    assert_true(
      result[get(spec$id) == 2L & radius == 500, get(spec$missing)] &&
        result[get(spec$id) == 2L & radius == 1000, get(spec$missing)],
      paste(label, "must propagate missingness only through containing radii")
    )
  }
}

assert_identical(
  prefix_results$sale_site$site_missing,
  prefix_results$rental_site$site_missing,
  "Isomorphic sale and rental site-level fixtures must have missingness parity"
)
assert_identical(
  prefix_results$sale_radius$has_missing_site,
  prefix_results$rental_radius$has_missing_site,
  "Isomorphic sale and rental radius-level fixtures must have missingness parity"
)

unsupported_root <- tempfile("prior-exposure-unsupported-")
dir.create(unsupported_root, recursive = TRUE)
write_prefix_fixture(unsupported_root, "2025-06-01 00:00:00")
for (label in names(producer_specs)) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- unsupported_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    unsupported_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  error <- tryCatch(producer_env$load_data(), error = identity)
  assert_true(inherits(error, "error"), paste(label, "must reject an unsupported later year"))
  assert_true(grepl("2025", conditionMessage(error)), paste(label, "must name unsupported 2025"))
}

message("Prior-exposure producer contract tests passed")
