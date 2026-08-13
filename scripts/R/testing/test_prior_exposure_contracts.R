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

source(here::here("scripts", "R", "utils", "site_group_utils.R"))
source(here::here("scripts", "R", "utils", "prior_exposure_utils.R"))

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

source_prior_exposure_producer <- function(path) {
  producer_env <- new.env(parent = globalenv())
  sys.source(here::here(path), envir = producer_env)
  sys.source(
    here::here("scripts", "R", "utils", "site_group_utils.R"),
    envir = producer_env
  )
  sys.source(
    here::here("scripts", "R", "utils", "spill_aggregation_utils.R"),
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
        annual_status = "reported_positive",
        matched_event_count = 1L
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
      ),
      matched_event_count = if_else(.data$annual_status == "absent", 0L, 1L)
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

# Exposure-evidence regression scope -------------------------------------------

evidence_state_fixture <- tibble(
  site_id = 10:15,
  year = 2023L,
  water_company = "Test Water",
  annual_status = c(
    "reported_zero", "reported_zero", "reported_positive",
    "reported_positive", "reported_na", "absent"
  ),
  matched_event_count = c(0L, 1L, 1L, 0L, 1L, 1L)
)
evidence_messages <- capture.output(
  evidence_state_result <- derive_site_group_prefix_missing_flags(
    evidence_state_fixture,
    base_year = 2023L,
    cutoff_years = 2023L,
    include_event_evidence = TRUE
  ),
  type = "message"
)
assert_identical(
  evidence_state_result$has_unknown_event_evidence,
  c(FALSE, FALSE, FALSE, TRUE, TRUE, TRUE),
  "Annual Status and matched events must distinguish observed evidence from unknown evidence."
)
for (status in c("reported_zero", "reported_na", "absent")) {
  assert_true(
    any(grepl(status, evidence_messages, fixed = TRUE) &
      grepl("1", evidence_messages, fixed = TRUE)),
    paste("The evidence helper must log one event-bearing", status, "Site Group-year.")
  )
}

cutoff_evidence_fixture <- tibble(
  site_id = 20L,
  year = 2022:2023,
  water_company = "Test Water",
  annual_status = "reported_positive",
  matched_event_count = c(1L, 0L)
)
cutoff_evidence_result <- suppressMessages(derive_site_group_prefix_missing_flags(
  cutoff_evidence_fixture,
  base_year = 2022L,
  cutoff_years = 2022:2023,
  include_event_evidence = TRUE
))
assert_identical(
  cutoff_evidence_result$has_unknown_event_evidence,
  c(FALSE, TRUE),
  "A positive year without matched events must affect its own and later cutoffs only."
)

for (invalid_count in list(NA_real_, -1, 1.5, "invalid")) {
  invalid_evidence_fixture <- evidence_state_fixture
  invalid_evidence_fixture$matched_event_count[1] <- invalid_count
  assert_error_contains(
    derive_site_group_prefix_missing_flags(
      invalid_evidence_fixture,
      base_year = 2023L,
      cutoff_years = 2023L
    ),
    "matched_event_count",
    "Invalid matched_event_count values must fail before evidence classification."
  )
}

write_evidence_output_fixture <- function(root) {
  zoopla_dir <- file.path(root, "zoopla")
  event_dir <- file.path(root, "matched_events_annual_data")
  dir.create(zoopla_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(event_dir, recursive = TRUE, showWarnings = FALSE)

  transaction_times <- as.POSIXct(
    c("2023-01-01 00:00:00", "2024-01-01 00:00:00"),
    tz = "UTC"
  )
  arrow::write_parquet(
    tibble(
      house_id = 1:2,
      price = c(100000L, 200000L),
      date_of_transfer = transaction_times
    ),
    file.path(root, "house_price.parquet")
  )
  arrow::write_parquet(
    tibble(
      rental_id = 1:2,
      listing_price = c(1000, 2000),
      rented_est = as.Date(transaction_times)
    ),
    file.path(zoopla_dir, "zoopla_rentals.parquet")
  )

  lookup <- tidyr::expand_grid(transaction_id = 1:2, site_id = 10:15) |>
    mutate(distance_m = 100)
  arrow::write_parquet(
    transmute(
      lookup,
      house_id = as.integer(.data$transaction_id),
      site_id = as.integer(.data$site_id),
      distance_m = as.double(.data$distance_m)
    ),
    file.path(root, "spill_house_lookup.parquet")
  )
  arrow::write_parquet(
    transmute(
      lookup,
      rental_id = as.integer(.data$transaction_id),
      site_id = as.integer(.data$site_id),
      distance_m = as.double(.data$distance_m)
    ),
    file.path(zoopla_dir, "spill_rental_lookup.parquet")
  )

  crosswalk <- tidyr::expand_grid(site_id = 10:15, year = 2021:2024) |>
    mutate(
      water_company = "Test Water",
      annual_status = case_when(
        .data$year < 2023L ~ "reported_zero",
        .data$site_id %in% c(10L, 11L) ~ "reported_zero",
        .data$site_id %in% c(12L, 13L) ~ "reported_positive",
        .data$site_id == 14L ~ "reported_na",
        TRUE ~ "absent"
      ),
      matched_event_count = case_when(
        .data$year < 2023L ~ 0L,
        .data$site_id %in% c(11L, 12L, 14L, 15L) ~ 1L,
        TRUE ~ 0L
      )
    )
  arrow::write_parquet(
    crosswalk,
    file.path(event_dir, "site_group_crosswalk.parquet")
  )

  event_sites <- c(11L, 12L, 14L, 15L)
  arrow::write_parquet(
    tibble(
      site_id = event_sites,
      start_time = as.POSIXct("2023-01-10 00:00:00", tz = "UTC"),
      end_time = as.POSIXct("2023-01-10 02:00:00", tz = "UTC"),
      year = 2023L
    ),
    file.path(event_dir, "matched_events_annual_data.parquet")
  )

  invisible(root)
}

evidence_output_root <- tempfile("prior-exposure-evidence-output-")
dir.create(evidence_output_root, recursive = TRUE)
write_evidence_output_fixture(evidence_output_root)
evidence_output_results <- list()
evidence_producer_specs <- list(
  sale_site = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "house_spill_prior_to_sale.R"),
    id = "house_id", result = "create_prior_to_sale_db"
  ),
  rental_site = list(
    path = file.path("scripts", "R", "06_analysis_datasets", "rental_spill_prior_to_rental.R"),
    id = "rental_id", result = "create_prior_to_rental_db"
  )
)
for (label in c("sale_site", "rental_site")) {
  spec <- evidence_producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- evidence_output_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    evidence_output_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  producer_env$CONFIG$radius_thresholds <- 250L
  data <- producer_env$load_data()
  result <- producer_env[[spec$result]](data)
  evidence_output_results[[label]] <- result

  raw_metrics <- c("spill_count", "spill_hrs")
  rate_metrics <- c(
    "spill_count_daily_avg", "spill_hrs_daily_avg",
    "spill_count_weekly_avg", "spill_hrs_weekly_avg"
  )
  all_metrics <- c(raw_metrics, rate_metrics)
  assert_true(
    all(result[get(spec$id) == 1L, !is.na(spill_count)]),
    paste(label, "must not let a future 2023 evidence gap mask a 2022 cutoff")
  )
  assert_true(
    all(result[get(spec$id) == 2L & site_id == 10L, spill_count] == 0),
    paste(label, "must retain reported_zero without events as observed zero")
  )
  assert_true(
    all(result[get(spec$id) == 2L & site_id %in% c(11L, 12L), spill_count] == 1),
    paste(label, "must use detailed events for event-bearing zero and positive years")
  )
  unknown_rows <- result[get(spec$id) == 2L & site_id %in% c(13L, 14L, 15L)]
  assert_true(
    all(vapply(unknown_rows[, ..all_metrics], function(column) all(is.na(column)), logical(1))),
    paste(label, "must keep all six final metrics unknown after zero-fill and rate calculation")
  )
  assert_true(
    all(!unknown_rows[site_id %in% c(13L, 14L), site_missing]) &&
      all(unknown_rows[site_id == 15L, site_missing]),
    paste(label, "must keep site_missing independent from evidence completeness")
  )
  assert_true(
    !"has_unknown_event_evidence" %in% names(result) && ncol(result) == 13L,
    paste(label, "must keep the internal evidence flag out of the public schema")
  )
}
assert_identical(
  evidence_output_results$sale_site$spill_count,
  evidence_output_results$rental_site$spill_count,
  "The isomorphic sale and rental evidence fixtures must have spill-count parity."
)

# Shared safe-publication regression scope -------------------------------------

publisher_schema <- arrow::schema(
  id = arrow::int32(),
  generation = arrow::int32(),
  site_missing = arrow::bool(),
  radius = arrow::int32()
)
publication_candidate <- function(ids, generation, radii) {
  tibble(
    id = as.integer(ids),
    generation = as.integer(generation),
    site_missing = FALSE,
    radius = as.integer(radii)
  )
}
read_publication <- function(path) {
  arrow::open_dataset(path) |>
    collect() |>
    arrange(.data$id, .data$radius)
}

publication_root <- tempfile("prior-exposure-publication-")
dir.create(publication_root, recursive = TRUE)
canonical_path <- file.path(publication_root, "canonical")
first_generation <- publication_candidate(1:2, 1L, c(250L, 500L))
second_generation <- publication_candidate(3L, 2L, 250L)
publish_prior_exposure_dataset(
  first_generation, canonical_path, publisher_schema, c(250L, 500L)
)
publish_prior_exposure_dataset(
  second_generation, canonical_path, publisher_schema, 250L
)
assert_identical(
  read_publication(canonical_path),
  second_generation,
  "A second publication must replace the complete generation and remove stale radii."
)
assert_identical(
  sort(list.dirs(canonical_path, recursive = FALSE, full.names = FALSE)),
  "radius=250",
  "The canonical dataset must contain only the configured Hive radius partition."
)
assert_identical(
  read_publication(paste0(canonical_path, ".prev")),
  first_generation,
  "Successful replacement must preserve the prior generation as .prev."
)

restored_path <- file.path(publication_root, "restored")
publish_prior_exposure_dataset(
  first_generation, restored_path, publisher_schema, c(250L, 500L)
)
promotion_failure <- function(from, to) {
  if (grepl(".stage-", basename(from), fixed = TRUE) &&
      identical(to, restored_path)) {
    return(FALSE)
  }
  file.rename(from, to)
}
restored_error <- tryCatch(
  publish_prior_exposure_dataset(
    second_generation, restored_path, publisher_schema, 250L,
    rename_path = promotion_failure
  ),
  error = identity
)
assert_true(inherits(restored_error, "error"), "Injected promotion failure must stop publication.")
assert_identical(
  read_publication(restored_path),
  first_generation,
  "A failed promotion must restore the exact prior canonical generation."
)

recoverable_path <- file.path(publication_root, "recoverable")
publish_prior_exposure_dataset(
  first_generation, recoverable_path, publisher_schema, c(250L, 500L)
)
promotion_and_restore_failure <- function(from, to) {
  if (identical(to, recoverable_path)) return(FALSE)
  file.rename(from, to)
}
recoverable_error <- tryCatch(
  publish_prior_exposure_dataset(
    second_generation, recoverable_path, publisher_schema, 250L,
    rename_path = promotion_and_restore_failure
  ),
  error = identity
)
recoverable_prev <- paste0(recoverable_path, ".prev")
assert_true(
  inherits(recoverable_error, "error") &&
    grepl(recoverable_prev, conditionMessage(recoverable_error), fixed = TRUE),
  "Failed promotion and restoration must report the exact recoverable .prev path."
)
assert_identical(
  read_publication(recoverable_prev),
  first_generation,
  "Failed restoration must leave the prior generation readable at .prev."
)

interrupted_path <- file.path(publication_root, "interrupted")
interrupted_prev <- paste0(interrupted_path, ".prev")
arrow::write_dataset(first_generation, interrupted_prev, partitioning = "radius")
interrupted_error <- tryCatch(
  publish_prior_exposure_dataset(
    second_generation, interrupted_path, publisher_schema, 250L
  ),
  error = identity
)
assert_true(
  inherits(interrupted_error, "error") &&
    grepl(interrupted_prev, conditionMessage(interrupted_error), fixed = TRUE),
  "Canonical-absent/.prev-present state must stop and report the recoverable path."
)
assert_identical(
  read_publication(interrupted_prev),
  first_generation,
  "Interrupted-state detection must not delete or move the recoverable generation."
)

empty_error <- tryCatch(
  publish_prior_exposure_dataset(
    first_generation[0, ], file.path(publication_root, "empty"),
    publisher_schema, c(250L, 500L)
  ),
  error = identity
)
assert_true(
  inherits(empty_error, "error") &&
    grepl("empty", conditionMessage(empty_error), ignore.case = TRUE),
  "The publisher must reject an empty candidate before writing."
)

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
    c(2020L, 2022L, 2024L),
    paste(label, "must return only transaction-relevant completeness prefixes")
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
