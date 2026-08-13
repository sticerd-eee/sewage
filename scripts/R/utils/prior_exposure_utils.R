############################################################
# Prior-Exposure Publication Utilities
# Project: Sewage
############################################################

prior_exposure_schema_signature <- function(schema) {
  stats::setNames(
    vapply(schema$fields, function(field) field$type$ToString(), character(1)),
    schema$names
  )
}

#' Resolve one authoritative reopened prior-exposure schema.
#'
#' The public matrix is intentionally closed to the four established outputs.
#' Keep these schemas literal so additions or field drift require an explicit
#' contract change rather than an unchecked column mapping.
#'
#' @param market One of `sale` or `rental`.
#' @param grain One of `site` or `radius`.
#' @return An Arrow schema including the restored Hive `radius` field.
prior_exposure_public_schema <- function(market, grain) {
  if (!is.character(market) || length(market) != 1L || is.na(market) ||
      !market %in% c("sale", "rental")) {
    stop("market must be exactly one of: sale, rental.", call. = FALSE)
  }
  if (!is.character(grain) || length(grain) != 1L || is.na(grain) ||
      !grain %in% c("site", "radius")) {
    stop("grain must be exactly one of: site, radius.", call. = FALSE)
  }

  variant <- paste(market, grain, sep = "_")
  switch(
    variant,
    sale_site = arrow::schema(
      house_id = arrow::int32(),
      price = arrow::int32(),
      n_days_in_window = arrow::int32(),
      site_id = arrow::int32(),
      distance_m = arrow::float64(),
      spill_hrs = arrow::float64(),
      spill_count = arrow::float64(),
      site_missing = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    ),
    rental_site = arrow::schema(
      rental_id = arrow::int32(),
      listing_price = arrow::float64(),
      n_days_in_window = arrow::int32(),
      site_id = arrow::int32(),
      distance_m = arrow::float64(),
      spill_hrs = arrow::float64(),
      spill_count = arrow::float64(),
      site_missing = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    ),
    sale_radius = arrow::schema(
      house_id = arrow::int32(),
      price = arrow::int32(),
      n_days_in_window = arrow::int32(),
      spill_hrs = arrow::float64(),
      n_spill_sites = arrow::int32(),
      spill_count = arrow::float64(),
      mean_distance = arrow::float64(),
      min_distance = arrow::float64(),
      has_missing_site = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    ),
    rental_radius = arrow::schema(
      rental_id = arrow::int32(),
      listing_price = arrow::float64(),
      n_days_in_window = arrow::int32(),
      spill_hrs = arrow::float64(),
      n_spill_sites = arrow::int32(),
      spill_count = arrow::float64(),
      mean_distance = arrow::float64(),
      min_distance = arrow::float64(),
      has_missing_site = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    )
  )
}

prior_exposure_variant <- function(market, grain) {
  # Resolve the schema first so unsupported variants fail before any I/O.
  schema <- prior_exposure_public_schema(market, grain)
  variant <- paste(market, grain, sep = "_")
  details <- switch(
    variant,
    sale_site = list(
      id = "house_id", value = "price", endpoint = "date_of_transfer",
      transaction_name = "house_dt", input = "house_price.parquet",
      lookup = "spill_house_lookup.parquet", include_event_evidence = TRUE
    ),
    rental_site = list(
      id = "rental_id", value = "listing_price", endpoint = "rented_est",
      transaction_name = "rental_dt",
      input = file.path("zoopla", "zoopla_rentals.parquet"),
      lookup = file.path("zoopla", "spill_rental_lookup.parquet"),
      include_event_evidence = TRUE
    ),
    sale_radius = list(
      id = "house_id", value = "price", endpoint = "date_of_transfer",
      transaction_name = "house_dt", input = "house_price.parquet",
      lookup = "spill_house_lookup.parquet", include_event_evidence = FALSE
    ),
    rental_radius = list(
      id = "rental_id", value = "listing_price", endpoint = "rented_est",
      transaction_name = "rental_dt",
      input = file.path("zoopla", "zoopla_rentals.parquet"),
      lookup = file.path("zoopla", "spill_rental_lookup.parquet"),
      include_event_evidence = FALSE
    )
  )
  c(details, list(market = market, grain = grain, schema = schema))
}

prior_exposure_complete_days <- function(endpoint, window_start) {
  seconds <- as.numeric(difftime(endpoint, window_start, units = "secs"))
  as.integer(floor(seconds / (24 * 60 * 60)))
}

prior_exposure_normalize_transactions <- function(
    transactions, contract, window_start, input_path) {
  transactions <- data.table::as.data.table(transactions)
  required <- c(contract$id, contract$value, contract$endpoint)
  missing_columns <- setdiff(required, names(transactions))
  if (length(missing_columns) > 0L) {
    stop(
      "Transaction input is missing required field(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }

  pre_filter_rows <- nrow(transactions)
  identifier <- transactions[[contract$id]]
  if (!is.integer(identifier)) {
    stop(contract$id, " must be an integer transaction identifier.", call. = FALSE)
  }
  if (anyNA(identifier)) {
    stop(contract$id, " contains missing transaction identifiers.", call. = FALSE)
  }
  if (anyDuplicated(identifier)) {
    stop(contract$id, " contains duplicate transaction identifiers.", call. = FALSE)
  }

  endpoint <- as.POSIXct(transactions[[contract$endpoint]], tz = "UTC")
  attr(endpoint, "tzone") <- "UTC"
  if (anyNA(endpoint)) {
    stop(contract$endpoint, " contains missing or invalid UTC endpoints.", call. = FALSE)
  }
  window_start <- as.POSIXct(window_start, tz = "UTC")
  attr(window_start, "tzone") <- "UTC"
  n_days <- prior_exposure_complete_days(endpoint, window_start)

  normalized <- data.table::data.table(
    transaction_id = identifier,
    transaction_value = as.double(transactions[[contract$value]]),
    transaction_endpoint = endpoint,
    n_days_in_window = n_days
  )
  normalized <- normalized[n_days_in_window >= 30L]
  if (nrow(normalized) == 0L) {
    stop(
      "No eligible transactions remain in ", basename(input_path),
      " for UTC exposure window starting ", format(window_start, "%Y-%m-%d", tz = "UTC"),
      ": at least 30 complete 24-hour days are required (pre-filter rows: ",
      pre_filter_rows, ").",
      call. = FALSE
    )
  }
  normalized[, cutoff_year := as.integer(format(
    transaction_endpoint - 1, "%Y", tz = "UTC"
  ))]
  data.table::setkey(normalized, transaction_id)
  normalized
}

prior_exposure_public_transactions <- function(transactions, contract) {
  result <- data.table::copy(transactions)
  data.table::setnames(
    result,
    c("transaction_id", "transaction_value", "transaction_endpoint"),
    c(contract$id, contract$value, contract$endpoint)
  )
  result <- result[, c(
    contract$id, contract$value, contract$endpoint,
    "n_days_in_window", "cutoff_year"
  ), with = FALSE]
  data.table::setkeyv(result, contract$id)
  result
}

prior_exposure_load_data <- function(config, market, grain) {
  contract <- prior_exposure_variant(market, grain)
  input_path <- file.path(config$processed_dir, contract$input)
  logger::log_info("Loading datasets from parquet files")

  raw_transactions <- arrow::open_dataset(input_path) |>
    dplyr::select(dplyr::all_of(c(
      contract$id, contract$value, contract$endpoint
    ))) |>
    dplyr::collect()
  transaction_dt <- prior_exposure_normalize_transactions(
    raw_transactions, contract, config$window_start, input_path
  )
  public_transactions <- prior_exposure_public_transactions(transaction_dt, contract)
  logger::log_info("{if (market == 'sale') 'House price' else 'Rental'} data loaded: {nrow(transaction_dt)} eligible rows")

  cutoff_years <- sort(unique(transaction_dt$cutoff_year))
  logger::log_info(
    "{if (market == 'sale') 'Sale' else 'Rental'} exposure completeness prefixes: {config$base_year}-{max(cutoff_years)}; keyed by site_id and cutoff_year"
  )
  crosswalk <- arrow::read_parquet(
    config$site_group_crosswalk_path,
    col_select = c(
      "site_id", "year", "water_company", "annual_status",
      "matched_event_count"
    )
  ) |>
    data.table::as.data.table()
  site_missing_dt <- derive_site_group_prefix_missing_flags(
    crosswalk,
    config$base_year,
    cutoff_years,
    include_event_evidence = contract$include_event_evidence
  ) |>
    data.table::as.data.table()
  data.table::setkey(site_missing_dt, site_id, cutoff_year)

  max_radius <- max(config$radius_thresholds)
  raw_lookup <- arrow::open_dataset(file.path(config$processed_dir, contract$lookup)) |>
    dplyr::select(dplyr::all_of(c(contract$id, "site_id", "distance_m"))) |>
    dplyr::filter(.data$distance_m <= max_radius) |>
    dplyr::collect() |>
    data.table::as.data.table()
  data.table::setnames(raw_lookup, contract$id, "transaction_id")
  raw_lookup <- raw_lookup[transaction_id %in% transaction_dt$transaction_id]
  data.table::setkey(raw_lookup, transaction_id)
  public_lookup <- data.table::copy(raw_lookup)
  data.table::setnames(public_lookup, "transaction_id", contract$id)
  data.table::setkeyv(public_lookup, contract$id)

  raw_events_dt <- arrow::open_dataset(file.path(
    config$processed_dir, "matched_events_annual_data",
    "matched_events_annual_data.parquet"
  )) |>
    dplyr::select("site_id", "start_time", "end_time", "year") |>
    dplyr::filter(.data$year >= config$base_year) |>
    dplyr::collect() |>
    data.table::as.data.table()
  raw_events_dt[, year := NULL]
  data.table::setkey(raw_events_dt, site_id)

  result <- list(
    transaction_dt = transaction_dt,
    internal_lookup_dt = raw_lookup,
    spill_lookup_dt = public_lookup,
    raw_events_dt = raw_events_dt,
    site_missing_dt = site_missing_dt,
    contract = contract,
    config = config
  )
  result[[contract$transaction_name]] <- public_transactions
  result
}

prior_exposure_join_events <- function(transaction_ids, data) {
  contract <- data$contract
  transaction_chunk <- data$transaction_dt[
    data.table::data.table(transaction_id = transaction_ids),
    on = "transaction_id", nomatch = 0L
  ]
  lookup_pairs <- data$internal_lookup_dt[
    data.table::data.table(transaction_id = transaction_ids),
    on = "transaction_id", nomatch = 0L
  ][, .(transaction_id, site_id, distance_m)]
  transaction_sites <- transaction_chunk[lookup_pairs, on = "transaction_id", nomatch = 0L]
  assert_left_row_count(
    lookup_pairs, transaction_sites,
    paste(if (contract$market == "sale") "House" else "Rental", "transaction attachment to lookup pairs")
  )

  missing_fields <- c("site_id", "cutoff_year", "site_missing")
  transaction_sites_with_missing <- data$site_missing_dt[, ..missing_fields][
    transaction_sites, on = .(site_id, cutoff_year)
  ]
  assert_left_row_count(
    transaction_sites, transaction_sites_with_missing,
    paste(if (contract$market == "sale") "House" else "Rental", "Site Group prefix missingness join")
  )
  transaction_sites_with_missing[, site_missing := data.table::fifelse(
    is.na(site_missing), TRUE, site_missing
  )]

  if (contract$include_event_evidence) {
    evidence <- data$site_missing_dt[, .(
      site_id, cutoff_year, has_unknown_event_evidence
    )]
    attached <- evidence[
      transaction_sites_with_missing, on = .(site_id, cutoff_year)
    ]
    assert_left_row_count(
      transaction_sites_with_missing, attached,
      paste(if (contract$market == "sale") "House" else "Rental", "Site Group event-evidence join")
    )
    attached[, has_unknown_event_evidence := data.table::fifelse(
      is.na(has_unknown_event_evidence), TRUE, has_unknown_event_evidence
    )]
  } else {
    attached <- data.table::copy(transaction_sites_with_missing)
    attached[, has_unknown_event_evidence := FALSE]
  }

  lookup_chunk <- attached[, .(
    transaction_id, site_id, distance_m, site_missing,
    has_unknown_event_evidence
  )]
  public_lookup <- data.table::copy(lookup_chunk)
  data.table::setnames(public_lookup, "transaction_id", contract$id)

  if (nrow(lookup_chunk) == 0L) {
    return(list(events_dt = NULL, lookup_chunk = public_lookup, internal_lookup = lookup_chunk))
  }

  # Keep the historical event-join shape separate from the evidence attachment.
  event_sources <- transaction_sites_with_missing[, cutoff_year := NULL]
  joined <- data$raw_events_dt[
    event_sources, on = "site_id", nomatch = NULL, allow.cartesian = TRUE
  ]
  joined <- joined[
    start_time < transaction_endpoint & end_time >= data$config$window_start
  ]
  if (nrow(joined) == 0L) {
    return(list(events_dt = NULL, lookup_chunk = public_lookup, internal_lookup = lookup_chunk))
  }
  joined[, `:=`(
    clamped_start = pmax(start_time, data$config$window_start),
    clamped_end = pmin(end_time, transaction_endpoint)
  )]
  joined[, event_hours := as.numeric(difftime(
    clamped_end, clamped_start, units = "hours"
  ))]
  joined <- joined[event_hours > 0]
  data.table::setnames(joined, "transaction_id", contract$id)
  list(events_dt = joined, lookup_chunk = public_lookup, internal_lookup = lookup_chunk)
}

prior_exposure_transaction_site_metrics <- function(joined, contract) {
  lookup <- data.table::copy(joined$internal_lookup)
  if (nrow(lookup) == 0L) return(NULL)
  site_lookup <- lookup[, .(
    distance_m = min(distance_m),
    site_missing = any(site_missing),
    has_unknown_event_evidence = any(has_unknown_event_evidence)
  ), by = .(transaction_id, site_id)]

  events <- joined$events_dt
  if (!is.null(events) && nrow(events) > 0L) {
    events <- data.table::copy(events)
    data.table::setnames(events, contract$id, "transaction_id")
    event_agg <- events[, .(
      spill_hrs = sum(event_hours, na.rm = TRUE),
      spill_count = count_spills(clamped_start, clamped_end)
    ), by = .(transaction_id, site_id)]
    site_lookup <- merge(
      site_lookup, event_agg,
      by = c("transaction_id", "site_id"), all.x = TRUE
    )
  } else {
    site_lookup[, `:=`(spill_hrs = 0, spill_count = 0)]
  }
  site_lookup[is.na(spill_hrs), spill_hrs := 0]
  site_lookup[is.na(spill_count), spill_count := 0]
  site_lookup
}

prior_exposure_reduce_site <- function(site_metrics, radii) {
  if (is.null(site_metrics) || nrow(site_metrics) == 0L) return(NULL)
  result <- data.table::rbindlist(lapply(sort(radii), function(radius_value) {
    rows <- data.table::copy(site_metrics[distance_m <= radius_value])
    rows[, radius := radius_value]
    rows
  }), use.names = TRUE)
  result[site_missing == TRUE, `:=`(
    spill_hrs = NA_real_, spill_count = NA_real_
  )]
  result
}

prior_exposure_reduce_radius <- function(site_metrics, transaction_ids, radii) {
  grid <- data.table::CJ(
    transaction_id = as.integer(transaction_ids),
    radius = sort(radii), unique = TRUE
  )
  if (is.null(site_metrics) || nrow(site_metrics) == 0L) {
    grid[, `:=`(
      spill_hrs = 0, n_spill_sites = 0L, spill_count = 0,
      mean_distance = NA_real_, min_distance = NA_real_,
      has_missing_site = FALSE
    )]
    return(grid)
  }
  expanded <- site_metrics[
    rep(seq_len(nrow(site_metrics)), each = length(radii))
  ]
  expanded[, radius := rep(sort(radii), times = nrow(site_metrics))]
  expanded <- expanded[distance_m <= radius]
  metrics <- expanded[, .(
    spill_hrs = sum(spill_hrs),
    n_spill_sites = as.integer(.N),
    spill_count = sum(spill_count),
    mean_distance = mean(distance_m),
    min_distance = min(distance_m),
    has_missing_site = any(site_missing)
  ), by = .(transaction_id, radius)]
  metrics[has_missing_site == TRUE, `:=`(
    spill_hrs = NA_real_, spill_count = NA_real_
  )]
  result <- metrics[grid, on = .(transaction_id, radius)]
  result[is.na(has_missing_site), has_missing_site := FALSE]
  result[!has_missing_site & is.na(spill_hrs), spill_hrs := 0]
  result[!has_missing_site & is.na(spill_count), spill_count := 0]
  result[is.na(n_spill_sites), n_spill_sites := 0L]
  result
}

prior_exposure_metadata <- function(transaction_dt, transaction_ids = NULL) {
  rows <- transaction_dt
  if (!is.null(transaction_ids)) {
    rows <- rows[data.table::data.table(transaction_id = transaction_ids),
      on = "transaction_id", nomatch = 0L]
  }
  rows[, .(
    transaction_id, transaction_value, n_days_in_window
  )]
}

prior_exposure_project_public <- function(result, contract) {
  result <- data.table::copy(result)
  data.table::setnames(
    result, c("transaction_id", "transaction_value"),
    c(contract$id, contract$value)
  )
  if (contract$grain == "site") {
    columns <- c(
      contract$id, contract$value, "n_days_in_window", "site_id", "radius",
      "distance_m", "spill_hrs", "spill_count", "site_missing",
      "spill_count_daily_avg", "spill_hrs_daily_avg",
      "spill_count_weekly_avg", "spill_hrs_weekly_avg"
    )
  } else {
    columns <- c(
      contract$id, contract$value, "n_days_in_window", "radius", "spill_hrs",
      "n_spill_sites", "spill_count", "mean_distance", "min_distance",
      "has_missing_site", "spill_count_daily_avg", "spill_hrs_daily_avg",
      "spill_count_weekly_avg", "spill_hrs_weekly_avg"
    )
  }
  result[, ..columns]
}

prior_exposure_site_prototype <- function(contract) {
  result <- data.table::data.table(
    transaction_id = integer(), transaction_value = double(),
    n_days_in_window = integer(), site_id = integer(), radius = double(),
    distance_m = double(), spill_hrs = double(), spill_count = double(),
    site_missing = logical(), spill_count_daily_avg = double(),
    spill_hrs_daily_avg = double(), spill_count_weekly_avg = double(),
    spill_hrs_weekly_avg = double()
  )
  prior_exposure_project_public(result, contract)
}

prior_exposure_process_chunk <- function(transaction_ids, data) {
  contract <- data$contract
  joined <- prior_exposure_join_events(transaction_ids, data)
  site_metrics <- prior_exposure_transaction_site_metrics(joined, contract)
  metadata <- prior_exposure_metadata(data$transaction_dt, transaction_ids)

  if (contract$grain == "site") {
    metrics <- prior_exposure_reduce_site(site_metrics, data$config$radius_thresholds)
    if (is.null(metrics) || nrow(metrics) == 0L) {
      return(prior_exposure_site_prototype(contract))
    }
    result <- metadata[metrics, on = "transaction_id"]
    result[has_unknown_event_evidence == TRUE, `:=`(
      spill_count = NA_real_, spill_hrs = NA_real_
    )]
    result[, has_unknown_event_evidence := NULL]
  } else {
    metrics <- prior_exposure_reduce_radius(
      site_metrics, metadata$transaction_id, data$config$radius_thresholds
    )
    result <- metadata[metrics, on = "transaction_id"]
  }
  result[, `:=`(
    spill_count_daily_avg = spill_count / n_days_in_window,
    spill_hrs_daily_avg = spill_hrs / n_days_in_window,
    spill_count_weekly_avg = spill_count / n_days_in_window * 7,
    spill_hrs_weekly_avg = spill_hrs / n_days_in_window * 7
  )]
  prior_exposure_project_public(result, contract)
}

prior_exposure_calculate_metrics <- function(
    lookup_dt, events_dt, market, grain, radii) {
  contract <- prior_exposure_variant(market, grain)
  lookup <- data.table::as.data.table(data.table::copy(lookup_dt))
  if (contract$id %in% names(lookup)) {
    data.table::setnames(lookup, contract$id, "transaction_id")
  }
  if (!"has_unknown_event_evidence" %in% names(lookup)) {
    lookup[, has_unknown_event_evidence := FALSE]
  }
  joined <- list(internal_lookup = lookup, events_dt = events_dt)
  site_metrics <- prior_exposure_transaction_site_metrics(joined, contract)
  result <- if (grain == "site") {
    prior_exposure_reduce_site(site_metrics, radii)
  } else {
    prior_exposure_reduce_radius(
      site_metrics, unique(lookup$transaction_id), radii
    )
  }
  if (!is.null(result) && "transaction_id" %in% names(result)) {
    data.table::setnames(result, "transaction_id", contract$id)
  }
  result
}

prior_exposure_build <- function(data) {
  contract <- data$contract
  transaction_ids <- data$transaction_dt$transaction_id
  starts <- seq.int(1L, length(transaction_ids), by = data$config$chunk_size)
  result <- data.table::rbindlist(lapply(starts, function(start) {
    end <- min(start + data$config$chunk_size - 1L, length(transaction_ids))
    prior_exposure_process_chunk(transaction_ids[start:end], data)
  }), use.names = TRUE)
  key_columns <- if (contract$grain == "site") {
    c(contract$id, "site_id", "radius")
  } else {
    c(contract$id, "radius")
  }
  if (anyDuplicated(result[, ..key_columns])) {
    stop("Prior-exposure result contains duplicate public keys.", call. = FALSE)
  }
  rate_columns <- grep("_(daily|weekly)_avg$", names(result), value = TRUE)
  if (any(vapply(result[, ..rate_columns], function(x) {
    any(!is.na(x) & !is.finite(x))
  }, logical(1)))) {
    stop("Prior-exposure rates must be finite or NA.", call. = FALSE)
  }
  data.table::setorderv(result, key_columns)
  result
}

prior_exposure_validate_and_cast_public <- function(data, expected_schema) {
  was_tibble <- inherits(data, "tbl_df")
  data <- data.table::as.data.table(data)
  expected_names <- expected_schema$names
  if (!identical(names(data), expected_names)) {
    stop(
      "Prior-exposure fields must exactly match the authoritative schema order.",
      call. = FALSE
    )
  }
  types <- prior_exposure_schema_signature(expected_schema)
  result <- data.table::copy(data)
  for (column in expected_names) {
    value <- result[[column]]
    target <- unname(types[[column]])
    if (target == "int32") {
      if (!is.numeric(value) || any(!is.na(value) & (
        !is.finite(value) | value != floor(value) |
          value < -.Machine$integer.max - 1 |
          value > .Machine$integer.max
      ))) {
        stop(column, " cannot be losslessly cast to int32.", call. = FALSE)
      }
      result[[column]] <- as.integer(value)
    } else if (target == "double") {
      if (!is.numeric(value)) {
        stop(column, " must be numeric for Arrow double.", call. = FALSE)
      }
      result[[column]] <- as.double(value)
    } else if (target == "bool") {
      if (!is.logical(value)) {
        stop(column, " must be logical for Arrow bool.", call. = FALSE)
      }
    }
  }
  id <- if ("house_id" %in% names(result)) "house_id" else if (
    "rental_id" %in% names(result)
  ) "rental_id" else NULL
  if (!is.null(id) && anyNA(result[[id]])) {
    stop(id, " contains missing public transaction identifiers.", call. = FALSE)
  }
  if (!is.null(id)) {
    key <- if ("site_id" %in% names(result)) {
      c(id, "site_id", "radius")
    } else {
      c(id, "radius")
    }
    if (anyDuplicated(result[, ..key])) {
      stop("Prior-exposure candidate contains duplicate public keys.", call. = FALSE)
    }
  }
  if ("n_days_in_window" %in% names(result) && (
      anyNA(result$n_days_in_window) ||
        any(result$n_days_in_window < 30L)
  )) {
    stop("Published prior-exposure rows require at least 30 complete days.", call. = FALSE)
  }
  rate_columns <- grep("_(daily|weekly)_avg$", names(result), value = TRUE)
  if (any(vapply(result[, ..rate_columns], function(x) {
    any(!is.na(x) & !is.finite(x))
  }, logical(1)))) {
    stop("Published prior-exposure rates must be finite or NA.", call. = FALSE)
  }
  if (was_tibble) tibble::as_tibble(result) else result
}

prior_exposure_prepare_public <- function(data, market, grain) {
  schema <- prior_exposure_public_schema(market, grain)
  data <- data.table::as.data.table(data)
  missing <- setdiff(schema$names, names(data))
  extra <- setdiff(names(data), schema$names)
  if (length(missing) > 0L || length(extra) > 0L) {
    stop("Prior-exposure result does not match its public fields.", call. = FALSE)
  }
  prior_exposure_validate_and_cast_public(data[, schema$names, with = FALSE], schema)
}

#' Publish a complete radius-partitioned prior-exposure generation.
#'
#' The candidate is written and validated beside the canonical directory before
#' the canonical generation is moved. Publication assumes one writer per path.
#'
#' @param data Complete in-memory candidate.
#' @param output_path Canonical Arrow dataset directory.
#' @param expected_schema Literal on-disk Arrow schema, including Hive radius.
#' @param expected_radii Exact configured integer radius set.
#' @param rename_path Injectable directory-rename seam used by focused tests.
#' @return `output_path`, invisibly.
publish_prior_exposure_dataset <- function(
    data, output_path, expected_schema, expected_radii,
    rename_path = file.rename) {
  data <- prior_exposure_validate_and_cast_public(data, expected_schema)
  expected_rows <- nrow(data)
  if (is.null(expected_rows) || expected_rows == 0L) {
    stop("Cannot publish an empty prior-exposure candidate.", call. = FALSE)
  }

  expected_radii <- as.numeric(expected_radii)
  if (length(expected_radii) == 0L || anyNA(expected_radii) ||
      any(!is.finite(expected_radii)) || any(expected_radii < 0) ||
      any(expected_radii != floor(expected_radii)) ||
      any(expected_radii > .Machine$integer.max) ||
      anyDuplicated(expected_radii)) {
    stop("expected_radii must be unique, nonnegative integers.", call. = FALSE)
  }
  expected_radii <- sort(as.integer(expected_radii))

  parent_dir <- dirname(output_path)
  dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
  stage_path <- tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = parent_dir
  )
  on.exit({
    if (dir.exists(stage_path)) {
      cleanup_status <- unlink(stage_path, recursive = TRUE)
      if (cleanup_status != 0L && dir.exists(stage_path)) {
        warning("Could not remove prior-exposure stage: ", stage_path, call. = FALSE)
      }
    }
  }, add = TRUE)

  arrow::write_dataset(
    data,
    path = stage_path,
    format = "parquet",
    partitioning = "radius"
  )

  staged <- tryCatch(
    arrow::open_dataset(stage_path),
    error = function(error) {
      stop(
        "Staged prior-exposure dataset could not be reopened: ",
        conditionMessage(error),
        call. = FALSE
      )
    }
  )
  actual_signature <- prior_exposure_schema_signature(staged$schema)
  expected_signature <- prior_exposure_schema_signature(expected_schema)
  if (!identical(actual_signature, expected_signature)) {
    stop(
      "Staged prior-exposure schema mismatch. Expected ",
      paste(names(expected_signature), expected_signature, collapse = ", "),
      "; found ",
      paste(names(actual_signature), actual_signature, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  staged_summary <- staged |>
    dplyr::group_by(.data$radius) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::collect()
  staged_rows <- sum(staged_summary$n)
  if (!identical(as.numeric(staged_rows), as.numeric(expected_rows))) {
    stop(
      "Staged prior-exposure row count mismatch: expected ", expected_rows,
      ", found ", staged_rows, ".",
      call. = FALSE
    )
  }

  staged_radii <- staged_summary |>
    dplyr::pull(.data$radius) |>
    as.integer() |>
    sort()
  if (!identical(staged_radii, expected_radii)) {
    stop(
      "Staged prior-exposure radius mismatch: expected ",
      paste(expected_radii, collapse = ", "), "; found ",
      paste(staged_radii, collapse = ", "), ".",
      call. = FALSE
    )
  }

  previous_path <- paste0(output_path, ".prev")
  canonical_exists <- dir.exists(output_path)
  previous_exists <- dir.exists(previous_path)
  if (!canonical_exists && previous_exists) {
    stop(
      "Interrupted prior-exposure publication: canonical is absent; recoverable prior generation: ",
      previous_path,
      call. = FALSE
    )
  }

  if (canonical_exists) {
    if (previous_exists) {
      remove_status <- unlink(previous_path, recursive = TRUE)
      if (remove_status != 0L || dir.exists(previous_path)) {
        stop(
          "Failed to remove older prior-exposure backup: ", previous_path,
          call. = FALSE
        )
      }
    }

    preserved <- isTRUE(rename_path(output_path, previous_path))
    if (!preserved || dir.exists(output_path) || !dir.exists(previous_path)) {
      recoverable <- if (dir.exists(previous_path)) {
        paste0(" Recoverable prior generation: ", previous_path, ".")
      } else {
        ""
      }
      stop(
        "Failed to preserve the canonical prior-exposure generation.",
        recoverable,
        call. = FALSE
      )
    }
  }

  promoted <- isTRUE(rename_path(stage_path, output_path))
  if (promoted && dir.exists(output_path) && !dir.exists(stage_path)) {
    return(invisible(output_path))
  }

  if (canonical_exists) {
    restored <- isTRUE(rename_path(previous_path, output_path))
    if (restored && dir.exists(output_path) && !dir.exists(previous_path)) {
      stop(
        "Failed to promote the staged prior-exposure dataset; the prior generation was restored.",
        call. = FALSE
      )
    }
    stop(
      "Failed to promote the staged prior-exposure dataset and failed to restore the prior generation. Recoverable prior generation: ",
      previous_path,
      call. = FALSE
    )
  }

  stop("Failed to promote the first prior-exposure generation.", call. = FALSE)
}
