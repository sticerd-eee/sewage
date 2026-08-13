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

assert_streaming_seam_exists <- exists("prior_exposure_stream", mode = "function")

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

expected_contract_failures <- character()
record_expected_contract_failure <- function(label, expression) {
  tryCatch(
    force(expression),
    error = function(error) {
      expected_contract_failures <<- c(
        expected_contract_failures,
        paste0(label, ": ", conditionMessage(error))
      )
    }
  )
  invisible(NULL)
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

collect_prior_exposure_fixture <- function(producer_env, data) {
  transaction_ids <- data$transaction_dt$transaction_id
  starts <- seq.int(1L, length(transaction_ids), by = data$config$chunk_size)
  chunks <- lapply(starts, function(start) {
    end <- min(start + data$config$chunk_size - 1L, length(transaction_ids))
    producer_env$process_chunk(transaction_ids[start:end], data)
  })
  data.table::rbindlist(chunks, use.names = TRUE)
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
source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))

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
  result <- collect_prior_exposure_fixture(producer_env, data)
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
    c(2022L, 2024L, 2024L),
    paste(label, "must derive the integer exclusive cutoff year")
  )
  assert_identical(
    key(data$site_missing_dt),
    c("site_id", "cutoff_year"),
    paste(label, "must key prefix flags on both join columns")
  )
  assert_identical(
    sort(unique(data$site_missing_dt$cutoff_year)),
    c(2022L, 2024L),
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

  result <- collect_prior_exposure_fixture(producer_env, data)
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

# Shared eligibility characterization -----------------------------------------

variant_schema_signatures <- list(
  sale_site = c(
    house_id = "int32", price = "int32", n_days_in_window = "int32",
    site_id = "int32", distance_m = "double", spill_hrs = "double",
    spill_count = "double", site_missing = "bool",
    spill_count_daily_avg = "double", spill_hrs_daily_avg = "double",
    spill_count_weekly_avg = "double", spill_hrs_weekly_avg = "double",
    radius = "int32"
  ),
  rental_site = c(
    rental_id = "int32", listing_price = "double",
    n_days_in_window = "int32", site_id = "int32", distance_m = "double",
    spill_hrs = "double", spill_count = "double", site_missing = "bool",
    spill_count_daily_avg = "double", spill_hrs_daily_avg = "double",
    spill_count_weekly_avg = "double", spill_hrs_weekly_avg = "double",
    radius = "int32"
  ),
  sale_radius = c(
    house_id = "int32", price = "int32", n_days_in_window = "int32",
    spill_hrs = "double", n_spill_sites = "int32", spill_count = "double",
    mean_distance = "double", min_distance = "double",
    has_missing_site = "bool", spill_count_daily_avg = "double",
    spill_hrs_daily_avg = "double", spill_count_weekly_avg = "double",
    spill_hrs_weekly_avg = "double", radius = "int32"
  ),
  rental_radius = c(
    rental_id = "int32", listing_price = "double",
    n_days_in_window = "int32", spill_hrs = "double",
    n_spill_sites = "int32", spill_count = "double", mean_distance = "double",
    min_distance = "double", has_missing_site = "bool",
    spill_count_daily_avg = "double", spill_hrs_daily_avg = "double",
    spill_count_weekly_avg = "double", spill_hrs_weekly_avg = "double",
    radius = "int32"
  )
)

for (label in names(variant_schema_signatures)) {
  axes <- strsplit(label, "_", fixed = TRUE)[[1]]
  record_expected_contract_failure(
    paste(label, "literal public schema"),
    assert_identical(
      prior_exposure_schema_signature(prior_exposure_public_schema(axes[1], axes[2])),
      variant_schema_signatures[[label]],
      paste(label, "must resolve to its exact literal reopened Arrow schema")
    )
  )
}
assert_error_contains(
  prior_exposure_public_schema("other", "site"),
  "sale, rental",
  "The schema resolver must reject an unsupported market."
)
assert_error_contains(
  prior_exposure_public_schema("sale", "other"),
  "site, radius",
  "The schema resolver must reject an unsupported grain."
)
assert_error_contains(
  prior_exposure_load_data(
    list(processed_dir = "must-not-be-read"), "other", "site"
  ),
  "sale, rental",
  "An unsupported engine variant must fail before attempting input loading."
)

prior_utility_lines <- readLines(here::here(
  "scripts", "R", "utils", "prior_exposure_utils.R"
))
assert_true(
  !any(grepl("^count_spills <- function", prior_utility_lines)),
  "The shared prior-exposure module must not reimplement count_spills()."
)
count_boundary <- as.POSIXct("2024-01-01 00:00:00", tz = "UTC")
assert_identical(
  count_spills(
    c(count_boundary, count_boundary + 12 * 60 * 60),
    c(count_boundary + 60, count_boundary + 12 * 60 * 60 + 60)
  ),
  2,
  "The existing count_spills() interface must retain its excluded 12-hour endpoint boundary."
)

write_eligibility_fixture <- function(root, transaction_times) {
  zoopla_dir <- file.path(root, "zoopla")
  event_dir <- file.path(root, "matched_events_annual_data")
  dir.create(zoopla_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(event_dir, recursive = TRUE, showWarnings = FALSE)

  transaction_times <- as.POSIXct(transaction_times, tz = "UTC")
  transaction_ids <- seq_along(transaction_times)
  arrow::write_parquet(
    tibble(
      house_id = as.integer(transaction_ids),
      price = as.integer(transaction_ids * 100000),
      date_of_transfer = transaction_times
    ),
    file.path(root, "house_price.parquet")
  )
  arrow::write_parquet(
    tibble(
      rental_id = as.integer(transaction_ids),
      listing_price = as.double(transaction_ids * 1000),
      rented_est = transaction_times
    ),
    file.path(zoopla_dir, "zoopla_rentals.parquet")
  )
  arrow::write_parquet(
    tibble(house_id = integer(), site_id = integer(), distance_m = double()),
    file.path(root, "spill_house_lookup.parquet")
  )
  arrow::write_parquet(
    tibble(rental_id = integer(), site_id = integer(), distance_m = double()),
    file.path(zoopla_dir, "spill_rental_lookup.parquet")
  )
  arrow::write_parquet(
    tibble(
      site_id = 10L,
      year = 2021L,
      water_company = "Test Water",
      annual_status = "reported_zero",
      matched_event_count = 0L
    ),
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

eligibility_root <- tempfile("prior-exposure-eligibility-")
dir.create(eligibility_root, recursive = TRUE)
write_eligibility_fixture(
  eligibility_root,
  c("2021-01-30 23:59:00", "2021-01-31 00:00:00")
)
for (label in names(producer_specs)) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- eligibility_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    eligibility_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  data <- producer_env$load_data()
  transaction_dt <- data[[spec$transaction]]
  metadata <- if (identical(spec$id, "house_id")) {
    producer_env$get_house_metadata(transaction_dt)
  } else {
    producer_env$get_rental_metadata(transaction_dt)
  }
  record_expected_contract_failure(
    paste(label, "30-complete-day eligibility"),
    {
      assert_identical(
        metadata[[spec$id]],
        2L,
        paste(label, "must exclude 29d23h59m and retain exactly 30 complete days")
      )
      assert_identical(
        metadata$n_days_in_window,
        30L,
        paste(label, "must expose the exact 30-day integer denominator")
      )
    }
  )
}

# Empty eligible cohorts must fail before any output stage exists.
ineligible_root <- tempfile("prior-exposure-ineligible-")
dir.create(ineligible_root, recursive = TRUE)
write_eligibility_fixture(
  ineligible_root,
  c("2021-01-02 00:00:00", "2021-01-30 23:59:00")
)
for (label in names(producer_specs)) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- ineligible_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    ineligible_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  stage_sentinel <- file.path(ineligible_root, paste0(label, ".stage"))
  error <- tryCatch(producer_env$load_data(), error = identity)
  input_name <- if (identical(spec$id, "house_id")) {
    "house_price.parquet"
  } else {
    "zoopla_rentals.parquet"
  }
  record_expected_contract_failure(
    paste(label, "empty eligible cohort"),
    {
      assert_true(inherits(error, "error"), paste(label, "must reject an all-ineligible input"))
      error_message <- conditionMessage(error)
      for (term in c(input_name, "2021-01-01", "30", "2")) {
        assert_true(
          grepl(term, error_message, fixed = TRUE),
          paste(label, "empty-cohort error must name", term)
        )
      }
      assert_true(
        !dir.exists(stage_sentinel),
        paste(label, "must reject the cohort before staging")
      )
    }
  )
}

# Transaction identifiers must fail closed before chunk construction.
rewrite_transaction_ids <- function(root, market, ids, value_name = NULL) {
  times <- as.POSIXct(rep("2021-03-01 00:00:00", length(ids)), tz = "UTC")
  if (identical(market, "sale")) {
    if (is.null(value_name)) value_name <- "price"
    values <- list(
      house_id = ids,
      value = as.integer(seq_along(ids) * 100000),
      date_of_transfer = times
    )
    names(values)[2] <- value_name
    arrow::write_parquet(tibble::as_tibble(values), file.path(root, "house_price.parquet"))
  } else {
    if (is.null(value_name)) value_name <- "listing_price"
    values <- list(
      rental_id = ids,
      value = as.double(seq_along(ids) * 1000),
      rented_est = times
    )
    names(values)[2] <- value_name
    arrow::write_parquet(
      tibble::as_tibble(values),
      file.path(root, "zoopla", "zoopla_rentals.parquet")
    )
  }
}

for (id_case in c("missing", "duplicate", "character")) {
  for (label in names(producer_specs)) {
    spec <- producer_specs[[label]]
    market <- if (identical(spec$id, "house_id")) "sale" else "rental"
    id_root <- tempfile(paste0("prior-exposure-id-", id_case, "-"))
    dir.create(id_root, recursive = TRUE)
    write_eligibility_fixture(id_root, rep("2021-03-01 00:00:00", 2L))
    ids <- switch(
      id_case,
      missing = c(1L, NA_integer_),
      duplicate = c(1L, 1L),
      character = c("1", "2")
    )
    rewrite_transaction_ids(id_root, market, ids)
    producer_env <- source_prior_exposure_producer(spec$path)
    producer_env$CONFIG$processed_dir <- id_root
    producer_env$CONFIG$site_group_crosswalk_path <- file.path(
      id_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
    )
    error <- tryCatch(producer_env$load_data(), error = identity)
    record_expected_contract_failure(
      paste(label, id_case, "transaction identifier"),
      {
        assert_true(inherits(error, "error"), paste(label, "must reject", id_case, "IDs"))
        assert_true(
          grepl(spec$id, conditionMessage(error), fixed = TRUE),
          paste(label, "identifier error must name", spec$id)
        )
      }
    )
  }
}

# Rental inputs must use the established value name, never a phantom `price`.
wrong_rental_value_root <- tempfile("prior-exposure-rental-value-")
dir.create(wrong_rental_value_root, recursive = TRUE)
write_eligibility_fixture(wrong_rental_value_root, "2021-03-01 00:00:00")
rewrite_transaction_ids(wrong_rental_value_root, "rental", 1L, value_name = "price")
for (label in c("rental_site", "rental_radius")) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- wrong_rental_value_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    wrong_rental_value_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  assert_error_contains(
    producer_env$load_data(),
    "listing_price",
    paste(label, "must reject a rental input with `price` instead of `listing_price`")
  )
}

# Site-empty prototypes must bind losslessly with a later populated chunk.
empty_then_populated_root <- tempfile("prior-exposure-empty-then-populated-")
dir.create(empty_then_populated_root, recursive = TRUE)
write_eligibility_fixture(
  empty_then_populated_root,
  c("2021-03-01 00:00:00", "2021-03-02 00:00:00")
)
arrow::write_parquet(
  tibble(house_id = 2L, site_id = 10L, distance_m = 100),
  file.path(empty_then_populated_root, "spill_house_lookup.parquet")
)
arrow::write_parquet(
  tibble(rental_id = 2L, site_id = 10L, distance_m = 100),
  file.path(empty_then_populated_root, "zoopla", "spill_rental_lookup.parquet")
)
for (label in c("sale_site", "rental_site")) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- empty_then_populated_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    empty_then_populated_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  data <- producer_env$load_data()
  empty_chunk <- producer_env$process_chunk(1L, data)
  populated_chunk <- producer_env$process_chunk(2L, data)
  record_expected_contract_failure(
    paste(label, "typed empty site chunk"),
    {
      assert_identical(nrow(empty_chunk), 0L, paste(label, "first site chunk must be empty"))
      assert_identical(
        names(empty_chunk), names(populated_chunk),
        paste(label, "empty and populated site chunks must have identical fields")
      )
      assert_identical(
        unname(vapply(empty_chunk, typeof, character(1))),
        unname(vapply(populated_chunk, typeof, character(1))),
        paste(label, "empty and populated site chunks must have identical R types")
      )
    }
  )
}

# Radius grain retains a complete zero-site grid for the empty first chunk.
for (label in c("sale_radius", "rental_radius")) {
  spec <- producer_specs[[label]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- empty_then_populated_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    empty_then_populated_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  producer_env$CONFIG$radius_thresholds <- c(250L, 500L, 1000L)
  producer_env$CONFIG$chunk_size <- 1L
  data <- producer_env$load_data()
  result <- collect_prior_exposure_fixture(producer_env, data)
  empty_rows <- result[get(spec$id) == 1L]
  assert_identical(
    empty_rows$radius,
    c(250L, 500L, 1000L),
    paste(label, "no-site transaction must retain every configured radius")
  )
  assert_true(
    all(empty_rows$spill_count == 0) && all(empty_rows$spill_hrs == 0) &&
      all(empty_rows$n_spill_sites == 0L),
    paste(label, "no-site radius rows must contain zero metrics")
  )
  assert_true(
    all(is.na(empty_rows$mean_distance)) && all(is.na(empty_rows$min_distance)) &&
      all(!empty_rows$has_missing_site),
    paste(label, "no-site radius rows must have missing distances and observed false missingness")
  )
}

# Public candidates must reopen with the literal schemas and reject drift.
public_contract_candidate <- function(label) {
  radii <- c(250L, 500L, 1000L)
  common_rates <- list(
    spill_count_daily_avg = c(0, 0.5, 1),
    spill_hrs_daily_avg = c(0, 1, 2),
    spill_count_weekly_avg = c(0, 3.5, 7),
    spill_hrs_weekly_avg = c(0, 7, 14),
    radius = radii
  )
  candidate <- switch(
    label,
    sale_site = c(list(
      house_id = rep(1L, 3L), price = rep(100000L, 3L),
      n_days_in_window = rep(60L, 3L), site_id = rep(10L, 3L),
      distance_m = rep(100, 3L), spill_hrs = c(0, 60, 120),
      spill_count = c(0, 30, 60), site_missing = rep(FALSE, 3L)
    ), common_rates),
    rental_site = c(list(
      rental_id = rep(1L, 3L), listing_price = rep(1200, 3L),
      n_days_in_window = rep(60L, 3L), site_id = rep(10L, 3L),
      distance_m = rep(100, 3L), spill_hrs = c(0, 60, 120),
      spill_count = c(0, 30, 60), site_missing = rep(FALSE, 3L)
    ), common_rates),
    sale_radius = c(list(
      house_id = rep(1L, 3L), price = rep(100000L, 3L),
      n_days_in_window = rep(60L, 3L), spill_hrs = c(0, 60, 120),
      n_spill_sites = c(0L, 1L, 2L), spill_count = c(0, 30, 60),
      mean_distance = c(NA_real_, 100, 200), min_distance = c(NA_real_, 100, 100),
      has_missing_site = rep(FALSE, 3L)
    ), common_rates),
    rental_radius = c(list(
      rental_id = rep(1L, 3L), listing_price = rep(1200, 3L),
      n_days_in_window = rep(60L, 3L), spill_hrs = c(0, 60, 120),
      n_spill_sites = c(0L, 1L, 2L), spill_count = c(0, 30, 60),
      mean_distance = c(NA_real_, 100, 200), min_distance = c(NA_real_, 100, 100),
      has_missing_site = rep(FALSE, 3L)
    ), common_rates)
  )
  data.table::as.data.table(candidate)
}

for (label in names(variant_schema_signatures)) {
  axes <- strsplit(label, "_", fixed = TRUE)[[1]]
  schema <- prior_exposure_public_schema(axes[1], axes[2])
  candidate <- public_contract_candidate(label)
  path <- tempfile(paste0("prior-exposure-public-", label, "-"))
  publish_prior_exposure_dataset(candidate, path, schema, c(250L, 500L, 1000L))
  reopened <- arrow::open_dataset(path)
  assert_identical(
    prior_exposure_schema_signature(reopened$schema),
    variant_schema_signatures[[label]],
    paste(label, "must reopen with its exact schema")
  )

  character_identifier <- data.table::copy(candidate)
  character_identifier[[names(candidate)[1]]] <- rep("1", nrow(candidate))
  drift_cases <- list(
    missing_field = candidate[, setdiff(names(candidate), "spill_hrs"), with = FALSE],
    extra_field = data.table::copy(candidate)[, unexpected := 1],
    reordered_fields = candidate[, rev(names(candidate)), with = FALSE],
    character_identifier = character_identifier
  )
  if (grepl("^rental", label)) {
    wrong_value <- data.table::copy(candidate)
    data.table::setnames(wrong_value, "listing_price", "price")
    drift_cases$wrong_rental_value_name <- wrong_value
  }
  for (drift_label in names(drift_cases)) {
    drift_path <- tempfile(paste0("prior-exposure-drift-", label, "-"))
    drift_error <- tryCatch(
      publish_prior_exposure_dataset(
        drift_cases[[drift_label]], drift_path, schema, c(250L, 500L, 1000L)
      ),
      error = identity
    )
    assert_true(
      inherits(drift_error, "error"),
      paste(label, "must reject", drift_label)
    )
  }

  key_columns <- if (grepl("site$", label)) {
    c(names(candidate)[1], "site_id", "radius")
  } else {
    c(names(candidate)[1], "radius")
  }
  duplicate_candidate <- data.table::rbindlist(list(candidate, candidate[1]))
  duplicate_path <- tempfile(paste0("prior-exposure-duplicate-", label, "-"))
  duplicate_error <- tryCatch(
    publish_prior_exposure_dataset(
      duplicate_candidate, duplicate_path, schema, c(250L, 500L, 1000L)
    ),
    error = identity
  )
  record_expected_contract_failure(
    paste(label, "duplicate public key"),
    {
      assert_true(
        inherits(duplicate_error, "error"),
        paste(label, "must reject duplicate key", paste(key_columns, collapse = "/"))
      )
    }
  )

  missing_id_candidate <- data.table::copy(candidate)
  missing_id_candidate[1, (names(candidate)[1]) := NA_integer_]
  missing_id_path <- tempfile(paste0("prior-exposure-missing-id-", label, "-"))
  missing_id_error <- tryCatch(
    publish_prior_exposure_dataset(
      missing_id_candidate, missing_id_path, schema, c(250L, 500L, 1000L)
    ),
    error = identity
  )
  record_expected_contract_failure(
    paste(label, "missing public transaction identifier"),
    {
      assert_true(
        inherits(missing_id_error, "error"),
        paste(label, "must reject a missing public transaction identifier")
      )
    }
  )
}

# Streaming stages must preserve chunk-local contracts without accumulation.
assert_true(
  assert_streaming_seam_exists,
  "The shared utility must expose the streaming orchestration seam."
)
streaming_results <- list()
for (label in names(producer_specs)) {
  spec <- producer_specs[[label]]
  axes <- strsplit(label, "_", fixed = TRUE)[[1]]
  producer_env <- source_prior_exposure_producer(spec$path)
  producer_env$CONFIG$processed_dir <- empty_then_populated_root
  producer_env$CONFIG$site_group_crosswalk_path <- file.path(
    empty_then_populated_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  producer_env$CONFIG$radius_thresholds <- c(250L, 500L)
  producer_env$CONFIG$chunk_size <- 1L
  data <- producer_env$load_data()
  output_path <- tempfile(paste0("prior-exposure-stream-", label, "-"))
  written_chunks <- integer()
  observing_writer <- function(chunk, stage_path, chunk_index) {
    written_chunks <<- c(written_chunks, as.integer(chunk_index))
    prior_exposure_write_chunk(chunk, stage_path, chunk_index)
  }
  prior_exposure_stream(data, output_path, write_chunk = observing_writer)
  reopened <- arrow::open_dataset(output_path)
  result <- reopened |>
    dplyr::collect() |>
    data.table::as.data.table()
  data.table::setorderv(result, spec$grain)
  streaming_results[[label]] <- result

  expected_written_chunks <- if (grepl("site$", label)) 2L else c(1L, 2L)
  assert_identical(
    written_chunks,
    expected_written_chunks,
    paste(label, "must write no fragment for an empty site chunk and every radius chunk")
  )
  assert_identical(
    prior_exposure_schema_signature(reopened$schema),
    variant_schema_signatures[[label]],
    paste(label, "streaming output must reopen with the literal public schema")
  )
  assert_identical(
    sort(unique(result$radius)),
    c(250L, 500L),
    paste(label, "streaming output must preserve exact integer radii")
  )
  expected_rows <- if (grepl("site$", label)) 2L else 4L
  assert_identical(
    nrow(result), expected_rows,
    paste(label, "streaming output must conserve its exact row total")
  )
  if (grepl("site$", label)) {
    assert_identical(
      unique(result[[spec$id]]), 2L,
      paste(label, "must preserve the populated chunk after an empty first chunk")
    )
  } else {
    assert_identical(
      sort(unique(result[[spec$id]])), c(1L, 2L),
      paste(label, "must preserve two same-radius chunk key sets without overwrite")
    )
    zero_rows <- result[get(spec$id) == 1L]
    assert_true(
      all(zero_rows$spill_count == 0) && all(zero_rows$spill_hrs == 0) &&
        all(zero_rows$n_spill_sites == 0L) &&
        all(is.na(zero_rows$mean_distance)) && all(is.na(zero_rows$min_distance)) &&
        all(!zero_rows$has_missing_site),
      paste(label, "no-site first chunk must write its complete zero grid")
    )
  }
}

local({
  diagnostic_lines <- character()
  logger::log_appender(function(lines) {
    diagnostic_lines <<- c(diagnostic_lines, as.character(lines))
  })
  on.exit(logger::log_appender(logger::appender_console), add = TRUE)
  diagnostic_env <- source_prior_exposure_producer(producer_specs$sale_radius$path)
  diagnostic_env$CONFIG$processed_dir <- empty_then_populated_root
  diagnostic_env$CONFIG$site_group_crosswalk_path <- file.path(
    empty_then_populated_root,
    "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  )
  diagnostic_env$CONFIG$radius_thresholds <- c(250L, 500L)
  diagnostic_env$CONFIG$chunk_size <- 1L
  diagnostic_data <- diagnostic_env$load_data()
  prior_exposure_stream(
    diagnostic_data,
    tempfile("prior-exposure-stream-diagnostics-")
  )
  diagnostics <- paste(diagnostic_lines, collapse = "\n")
  for (term in c(
      "transactions=", "lookup_pairs=", "joined_events=", "output_rows=",
      "elapsed_seconds=", "stage="
  )) {
    assert_true(
      grepl(term, diagnostics, fixed = TRUE),
      paste("Streaming diagnostics must include", term)
    )
  }
  assert_true(
    !any(vapply(
      c("transaction_id=", "house_id=", "rental_id=", "site_id="),
      grepl, logical(1), x = diagnostics, fixed = TRUE
    )),
    "Streaming diagnostics must not log row-level identifiers."
  )
})

# Failures after a completed write and during stage validation must leave the
# last-known-good canonical generation untouched and avoid the promotion seam.
failure_env <- source_prior_exposure_producer(producer_specs$sale_radius$path)
failure_env$CONFIG$processed_dir <- empty_then_populated_root
failure_env$CONFIG$site_group_crosswalk_path <- file.path(
  empty_then_populated_root, "matched_events_annual_data", "site_group_crosswalk.parquet"
)
failure_env$CONFIG$radius_thresholds <- c(250L, 500L)
failure_env$CONFIG$chunk_size <- 1L
failure_data <- failure_env$load_data()
failure_canonical <- tempfile("prior-exposure-stream-failure-")
prior_exposure_stream(failure_data, failure_canonical)
read_failure_publication <- function(path) {
  arrow::open_dataset(path) |>
    dplyr::collect() |>
    dplyr::arrange(.data$house_id, .data$radius)
}
failure_baseline <- read_failure_publication(failure_canonical)

failed_stage <- NA_character_
publisher_called <- FALSE
observing_first_write <- function(chunk, stage_path, chunk_index) {
  failed_stage <<- stage_path
  prior_exposure_write_chunk(chunk, stage_path, chunk_index)
}
fail_second_chunk <- function(transaction_ids, data, joined) {
  if (identical(transaction_ids, 2L)) stop("injected second-chunk failure", call. = FALSE)
  prior_exposure_process_joined_chunk(transaction_ids, data, joined)
}
unreached_publisher <- function(...) {
  publisher_called <<- TRUE
  stop("publisher must not be reached", call. = FALSE)
}
after_write_error <- tryCatch(
  prior_exposure_stream(
    failure_data, failure_canonical,
    process_joined = fail_second_chunk,
    write_chunk = observing_first_write,
    publish_dataset = unreached_publisher
  ),
  error = identity
)
assert_true(
  inherits(after_write_error, "error") &&
    grepl("second-chunk", conditionMessage(after_write_error), fixed = TRUE) &&
    !publisher_called && !dir.exists(failed_stage),
  "A failure after the first write must clean the stage and never invoke publication."
)
assert_identical(
  read_failure_publication(failure_canonical), failure_baseline,
  "A failure after the first write must not alter the canonical generation."
)

promotion_calls <- 0L
rename_spy <- function(from, to) {
  promotion_calls <<- promotion_calls + 1L
  file.rename(from, to)
}
validating_publisher <- function(
    data, output_path, expected_schema, expected_radii, stage_path, expected_rows) {
  publish_prior_exposure_dataset(
    data, output_path, expected_schema, expected_radii,
    rename_path = rename_spy, stage_path = stage_path,
    expected_rows = expected_rows
  )
}
corrupt_stage_days <- function(stage_path) {
  fragment <- list.files(
    stage_path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE
  )[[1L]]
  rows <- arrow::read_parquet(fragment)
  rows$n_days_in_window[[1L]] <- 29L
  arrow::write_parquet(rows, fragment)
}
validation_error <- tryCatch(
  prior_exposure_stream(
    failure_data, failure_canonical,
    before_publish = corrupt_stage_days,
    publish_dataset = validating_publisher
  ),
  error = identity
)
assert_true(
  inherits(validation_error, "error") && promotion_calls == 0L,
  "Stage validation failure must occur before any canonical promotion rename."
)
assert_identical(
  read_failure_publication(failure_canonical), failure_baseline,
  "Stage validation failure must not alter the canonical generation."
)

# Chunk-local drift must fail before promotion.
assert_stream_mutation_fails <- function(label, mutate_result, expected_message) {
  promotion_reached <- FALSE
  mutate_chunk <- function(transaction_ids, data, joined) {
    result <- prior_exposure_process_joined_chunk(transaction_ids, data, joined)
    mutate_result(result)
  }
  error <- tryCatch(
    prior_exposure_stream(
      failure_data,
      tempfile(paste0("prior-exposure-stream-invalid-", label, "-")),
      process_joined = mutate_chunk,
      publish_dataset = function(...) {
        promotion_reached <<- TRUE
        invisible(NULL)
      }
    ),
    error = identity
  )
  assert_true(
    inherits(error, "error") &&
      grepl(expected_message, conditionMessage(error), fixed = TRUE) &&
      !promotion_reached,
    paste("Streaming must reject", label, "before promotion")
  )
}
assert_stream_mutation_fails(
  "duplicate chunk key",
  function(result) data.table::rbindlist(list(result, result[1L])),
  "duplicate public keys"
)
assert_stream_mutation_fails(
  "missing chunk key", function(result) result[-1L], "expected keys"
)
assert_stream_mutation_fails(
  "wrong radius",
  function(result) {
    result$radius[[1L]] <- 999L
    result
  },
  "expected keys"
)
for (invalid_rate in c(NaN, Inf, -Inf)) {
  assert_stream_mutation_fails(
    paste("invalid rate", invalid_rate),
    function(result) {
      result$spill_count_daily_avg[[1L]] <- invalid_rate
      result
    },
    "finite or NA"
  )
}
assert_stream_mutation_fails(
  "sub-30-day row",
  function(result) {
    result$n_days_in_window[[1L]] <- 29L
    result
  },
  "at least 30 complete days"
)

# All established outputs preserve finite rates and weekly = daily * seven.
for (label in names(prefix_results)) {
  result <- prefix_results[[label]]
  rate_columns <- c(
    "spill_count_daily_avg", "spill_hrs_daily_avg",
    "spill_count_weekly_avg", "spill_hrs_weekly_avg"
  )
  record_expected_contract_failure(
    paste(label, "finite-or-NA rates"),
    assert_true(
      all(vapply(
        result[, ..rate_columns],
        function(column) all(is.na(column) | is.finite(column)),
        logical(1)
      )),
      paste(label, "rates must be finite or NA")
    )
  )
  assert_true(
    all(
      is.na(result$spill_count_daily_avg) |
        result$spill_count_weekly_avg == result$spill_count_daily_avg * 7
    ) &&
      all(
        is.na(result$spill_hrs_daily_avg) |
          result$spill_hrs_weekly_avg == result$spill_hrs_daily_avg * 7
      ),
    paste(label, "weekly rates must equal daily rates times seven")
  )
}

if (length(expected_contract_failures) > 0L) {
  stop(
    "Expected not-yet-implemented prior-exposure contracts:\n- ",
    paste(expected_contract_failures, collapse = "\n- "),
    call. = FALSE
  )
}

message("Prior-exposure producer contract tests passed")
