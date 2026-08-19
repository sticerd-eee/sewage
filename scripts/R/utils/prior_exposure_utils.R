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

# The stable-sum discipline now lives in the shared measurement core; these
# names stay as thin aliases so this engine's existing call sites keep reading
# the same, and are retired when the engine moves onto the core wholesale.
prior_exposure_stable_sum <- function(value, na.rm = FALSE) {
  spill_stable_sum(value, na.rm = na.rm)
}

prior_exposure_stable_cumsum <- function(value) {
  spill_stable_cumsum(value)
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
      house_id = arrow::utf8(),
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
      rental_id = arrow::utf8(),
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
      house_id = arrow::utf8(),
      price = arrow::int32(),
      n_days_in_window = arrow::int32(),
      spill_hrs = arrow::float64(),
      n_spill_sites = arrow::int32(),
      spill_count = arrow::float64(),
      mean_distance = arrow::float64(),
      min_distance = arrow::float64(),
      has_missing_site = arrow::bool(),
      annual_returns_na_then_absent = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    ),
    rental_radius = arrow::schema(
      rental_id = arrow::utf8(),
      listing_price = arrow::float64(),
      n_days_in_window = arrow::int32(),
      spill_hrs = arrow::float64(),
      n_spill_sites = arrow::int32(),
      spill_count = arrow::float64(),
      mean_distance = arrow::float64(),
      min_distance = arrow::float64(),
      has_missing_site = arrow::bool(),
      annual_returns_na_then_absent = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    )
  )
}

#' Resolve one market's transaction inputs, independent of grain.
#'
#' The identifiers, ledger fields, and input paths are a property of the
#' market alone, so the public variants and the measurement contract share them.
#'
#' @param market One of `sale` or `rental`.
#' @return The market's input details.
prior_exposure_market_details <- function(market) {
  switch(
    market,
    sale = list(
      id = "house_id", value = "price", endpoint = "date_of_transfer",
      transaction_name = "house_dt", input = "house_price.parquet",
      lookup = "spill_house_lookup.parquet"
    ),
    rental = list(
      id = "rental_id", value = "listing_price", endpoint = "rented_est",
      transaction_name = "rental_dt",
      input = file.path("zoopla", "zoopla_rentals.parquet"),
      lookup = file.path("zoopla", "spill_rental_lookup.parquet")
    )
  )
}

prior_exposure_variant <- function(market, grain) {
  # Resolve the schema first so unsupported variants fail before any I/O; that
  # leaves only the four supported market-grain pairs below.
  schema <- prior_exposure_public_schema(market, grain)
  # The public grains are derivations over measurement rows, so they load the
  # same evidence surface as the measurement contract: all four atomic flags.
  # Each derivation then ORs its own subset into its verdict (R15).
  c(prior_exposure_market_details(market), list(
    market = market, grain = grain, schema = schema,
    include_event_evidence = TRUE,
    include_annual_return_sequence = TRUE,
    include_atomic_evidence_flags = TRUE
  ))
}

#' Resolve the internal measurement table's authoritative schema.
#'
#' The unmasked pair grain: one row per eligible transaction by nearby Site
#' Group. No `radius` column, because per-radius replication is a step inside
#' the derivations rather than a property of this table (R1). No transaction
#' metadata and no rates, because the derivations rejoin those from the ledger
#' (R2). No verdict column: evidence travels as four atomic flags and each
#' derivation ORs its own subset (R3).
#'
#' @param market One of `sale` or `rental`.
#' @return An Arrow schema for the market's measurement table.
prior_exposure_measurement_schema <- function(market) {
  if (!is.character(market) || length(market) != 1L || is.na(market) ||
      !market %in% c("sale", "rental")) {
    stop("market must be exactly one of: sale, rental.", call. = FALSE)
  }
  # The two markets differ only in the name of their transaction identifier.
  fields <- list(
    arrow::utf8(),
    site_id = arrow::int32(),
    distance_m = arrow::float64(),
    spill_hrs = arrow::float64(),
    spill_count = arrow::float64(),
    annual_returns_absent = arrow::bool(),
    annual_returns_na = arrow::bool(),
    reported_positive_without_matched_events = arrow::bool(),
    annual_returns_na_then_absent = arrow::bool()
  )
  names(fields)[[1L]] <- prior_exposure_market_details(market)$id
  do.call(arrow::schema, fields)
}

#' Resolve the measurement-table build contract for one market.
#'
#' Shaped like `prior_exposure_variant()` so the shared loader and event join
#' serve both, but carrying the measurement schema and asking for all four
#' atomic evidence flags rather than their pre-combined verdict.
#'
#' @param market One of `sale` or `rental`.
#' @return A contract list with `grain` `"measurement"`.
prior_exposure_measurement_contract <- function(market) {
  schema <- prior_exposure_measurement_schema(market)
  c(prior_exposure_market_details(market), list(
    market = market, grain = "measurement", schema = schema,
    include_event_evidence = TRUE,
    include_annual_return_sequence = TRUE,
    include_atomic_evidence_flags = TRUE
  ))
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
  if (!is.character(identifier) || any(!is.na(identifier) & !nzchar(identifier))) {
    stop(
      contract$id,
      " must contain non-empty character transaction identifiers.",
      call. = FALSE
    )
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

prior_exposure_load_data <- function(config, market, grain,
                                    contract = prior_exposure_variant(market, grain)) {
  input_path <- file.path(config$processed_dir, contract$input)
  logger::log_info("Loading datasets from parquet files")

  # as.data.frame() materializes arrow's chunked columns before data.table sees
  # them; data.table joins on collected-but-unmaterialized character keys drop
  # rows nondeterministically. See
  # docs/solutions/logic-errors/arrow-altrep-data-table-join-nondeterminism.md
  raw_transactions <- arrow::open_dataset(input_path) |>
    dplyr::select(dplyr::all_of(c(
      contract$id, contract$value, contract$endpoint
    ))) |>
    dplyr::collect() |>
    as.data.frame()
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
    include_event_evidence = contract$include_event_evidence,
    include_annual_return_sequence = isTRUE(contract$include_annual_return_sequence),
    include_atomic_evidence_flags = isTRUE(contract$include_atomic_evidence_flags)
  ) |>
    data.table::as.data.table()
  data.table::setkey(site_missing_dt, site_id, cutoff_year)

  max_radius <- max(config$radius_thresholds)
  raw_lookup <- arrow::open_dataset(file.path(config$processed_dir, contract$lookup)) |>
    dplyr::select(dplyr::all_of(c(contract$id, "site_id", "distance_m"))) |>
    dplyr::filter(.data$distance_m <= max_radius) |>
    dplyr::collect() |>
    as.data.frame() |>
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
    as.data.frame() |>
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

  atomic_flags <- c(
    "annual_returns_absent", "annual_returns_na",
    "reported_positive_without_matched_events"
  )
  wants_atomic <- isTRUE(contract$include_atomic_evidence_flags)
  prefix_fields <- c("site_id", "cutoff_year", "site_missing")
  if (contract$include_event_evidence) {
    prefix_fields <- c(prefix_fields, "has_unknown_event_evidence")
  }
  if (isTRUE(contract$include_annual_return_sequence)) {
    prefix_fields <- c(prefix_fields, "annual_returns_na_then_absent")
  }
  if (wants_atomic) {
    prefix_fields <- c(prefix_fields, atomic_flags)
  }
  attached <- data$site_missing_dt[, ..prefix_fields][
    transaction_sites, on = .(site_id, cutoff_year)
  ]
  assert_left_row_count(
    transaction_sites, attached,
    paste(if (contract$market == "sale") "House" else "Rental", "Site Group prefix missingness join")
  )
  attached[, site_missing := data.table::fifelse(
    is.na(site_missing), TRUE, site_missing
  )]

  if (contract$include_event_evidence) {
    attached[, has_unknown_event_evidence := data.table::fifelse(
      is.na(has_unknown_event_evidence), TRUE, has_unknown_event_evidence
    )]
  } else {
    attached[, has_unknown_event_evidence := FALSE]
  }

  if (isTRUE(contract$include_annual_return_sequence)) {
    attached[, annual_returns_na_then_absent := data.table::fifelse(
      is.na(annual_returns_na_then_absent),
      FALSE,
      annual_returns_na_then_absent
    )]
  }
  if (wants_atomic) {
    # A Site Group the crosswalk never mentions reads as absent, which is the
    # same reading the gap fill applies inside the truth table's universe.
    attached[, annual_returns_absent := data.table::fifelse(
      is.na(annual_returns_absent), TRUE, annual_returns_absent
    )]
    for (flag in c("annual_returns_na",
                   "reported_positive_without_matched_events")) {
      data.table::set(attached, j = flag, value = data.table::fifelse(
        is.na(attached[[flag]]), FALSE, attached[[flag]]
      ))
    }
  }

  lookup_columns <- c(
    "transaction_id", "site_id", "distance_m", "site_missing",
    "has_unknown_event_evidence"
  )
  if (isTRUE(contract$include_annual_return_sequence)) {
    lookup_columns <- c(lookup_columns, "annual_returns_na_then_absent")
  }
  if (wants_atomic) {
    lookup_columns <- c(lookup_columns, atomic_flags)
  }
  lookup_chunk <- attached[, ..lookup_columns]
  public_lookup <- data.table::copy(lookup_chunk)
  data.table::setnames(public_lookup, "transaction_id", contract$id)

  if (nrow(lookup_chunk) == 0L) {
    return(list(events_dt = NULL, lookup_chunk = public_lookup, internal_lookup = lookup_chunk))
  }

  # Keep the historical event-join shape separate from the evidence attachment.
  # data.table's grouped floating reducer is sensitive to the joined table's
  # otherwise-unused columns, so project the exact established public shape.
  event_sources <- data.table::copy(attached)
  event_metadata_fields <- intersect(
    c(
      "cutoff_year", "n_days_in_window", "has_unknown_event_evidence",
      "annual_returns_na_then_absent", atomic_flags
    ),
    names(event_sources)
  )
  event_sources[, (event_metadata_fields) := NULL]
  data.table::setnames(
    event_sources,
    c("transaction_id", "transaction_value", "transaction_endpoint"),
    c(contract$id, contract$value, contract$endpoint)
  )
  data.table::setcolorder(event_sources, c(
    "site_id", "site_missing", contract$id, contract$value,
    contract$endpoint, "distance_m"
  ))
  joined <- data$raw_events_dt[
    event_sources, on = "site_id", nomatch = NULL, allow.cartesian = TRUE
  ]
  # Each transaction clips to its own endpoint, so the shared clip receives a
  # per-row window_end rather than a constant.
  joined <- clip_events_to_window(
    joined, data$config$window_start, joined[[contract$endpoint]]
  )
  if (nrow(joined) == 0L) {
    return(list(events_dt = NULL, lookup_chunk = public_lookup, internal_lookup = lookup_chunk))
  }
  list(events_dt = joined, lookup_chunk = public_lookup, internal_lookup = lookup_chunk)
}

# The prior engine breaks ties on the unclipped endpoints as well, so events
# clipped to the same instant still sort deterministically before summing.
prior_exposure_event_order_key <- function() {
  c("clipped_start", "clipped_end", "start_time", "end_time")
}

prior_exposure_transaction_site_metrics <- function(joined, contract) {
  lookup <- data.table::copy(joined$internal_lookup)
  if (nrow(lookup) == 0L) return(NULL)
  # No min(distance_m) dedupe: pair uniqueness is a validated property of the
  # lookup (asserted by the measurement gate, R13), so each pair's own row is
  # taken as it stands rather than collapsed.
  columns <- c("transaction_id", "site_id", "distance_m", "site_missing")
  if (contract$include_event_evidence) {
    columns <- c(columns, "has_unknown_event_evidence")
  }
  if ("annual_returns_na_then_absent" %in% names(lookup)) {
    columns <- c(columns, "annual_returns_na_then_absent")
  }
  site_lookup <- lookup[, ..columns]

  events <- joined$events_dt
  if (!is.null(events) && nrow(events) > 0L) {
    events <- data.table::copy(events)
    data.table::setnames(events, contract$id, "transaction_id")
    event_agg <- collapse_events_by_group(
      events,
      by = c("transaction_id", "site_id"),
      order_by = prior_exposure_event_order_key(),
      na.rm = TRUE
    )
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

# Pure per-radius replication: each pair row repeats once per configured
# radius that contains it. Masking is the derivation layer's job, not this
# reducer's, so the replicated measures pass through unmodified.
prior_exposure_reduce_site <- function(site_metrics, radii) {
  if (is.null(site_metrics) || nrow(site_metrics) == 0L) return(NULL)
  radii <- sort(radii)
  result <- site_metrics[rep(seq_len(.N), each = length(radii))]
  result[, radius := rep(radii, times = nrow(site_metrics))]
  result[distance_m <= radius]
}

prior_exposure_reduce_radius <- function(site_metrics, transaction_ids, radii) {
  grid <- data.table::CJ(
    transaction_id = transaction_ids,
    radius = sort(radii), unique = TRUE
  )
  if (is.null(site_metrics) || nrow(site_metrics) == 0L) {
    grid[, `:=`(
      spill_hrs = 0, n_spill_sites = 0L, spill_count = 0,
      mean_distance = NA_real_, min_distance = NA_real_,
      has_missing_site = FALSE, has_unknown_evidence = FALSE,
      annual_returns_na_then_absent = FALSE
    )]
    return(grid)
  }
  if (!"annual_returns_na_then_absent" %in% names(site_metrics)) {
    site_metrics[, annual_returns_na_then_absent := FALSE]
  }
  # The derivation's verdict travels as its own pair-grain flag, so the
  # accumulation below can widen the mask without touching what
  # `site_missing` — and therefore `has_missing_site` — means (R15, R16).
  if (!"site_unknown_evidence" %in% names(site_metrics)) {
    site_metrics[, site_unknown_evidence := FALSE]
  }
  # Preserve the historical accumulation order exactly: collapse distance ties,
  # sort by distance, then take cumulative sums for rolling radius thresholds.
  # Summing an expanded transaction/radius table changes low-order floating-point
  # bits in production even though the mathematical result is equivalent.
  data.table::setorder(site_metrics, transaction_id, distance_m, site_id)
  site_agg <- site_metrics[, .(
    spill_hrs = prior_exposure_stable_sum(spill_hrs),
    spill_count = prior_exposure_stable_sum(spill_count),
    n_spill_sites = .N,
    distance_sum = prior_exposure_stable_sum(distance_m),
    missing_sites = prior_exposure_stable_sum(site_missing),
    site_unknown_evidence = any(site_unknown_evidence),
    annual_returns_na_then_absent = any(annual_returns_na_then_absent)
  ), by = .(transaction_id, distance_m)]
  data.table::setorder(site_agg, transaction_id, distance_m)
  site_agg[, `:=`(
    cum_spill_hrs = prior_exposure_stable_cumsum(spill_hrs),
    cum_spill_count = prior_exposure_stable_cumsum(spill_count),
    cum_distance_sum = prior_exposure_stable_cumsum(distance_sum),
    n_spill_sites = prior_exposure_stable_cumsum(n_spill_sites),
    cum_missing_sites = prior_exposure_stable_cumsum(missing_sites),
    cum_unknown_evidence = dplyr::cumany(site_unknown_evidence),
    cum_annual_returns_na_then_absent = dplyr::cumany(
      annual_returns_na_then_absent
    ),
    min_distance = distance_m[1L]
  ), by = transaction_id]
  data.table::setkey(site_agg, transaction_id, distance_m)

  radius_grid <- data.table::CJ(
    transaction_id = unique(site_agg$transaction_id),
    radius = sort(radii), unique = TRUE
  )
  radius_grid[, radius_join := radius]
  data.table::setkey(radius_grid, transaction_id, radius_join)
  metrics <- site_agg[
    radius_grid,
    roll = Inf,
    on = .(transaction_id, distance_m = radius_join)
  ]
  metrics[, `:=`(
    spill_hrs = data.table::fifelse(is.na(cum_spill_hrs), 0, cum_spill_hrs),
    spill_count = data.table::fifelse(
      is.na(cum_spill_count), 0, cum_spill_count
    ),
    n_spill_sites = data.table::fifelse(
      is.na(n_spill_sites), 0L, n_spill_sites
    ),
    mean_distance = data.table::fifelse(
      n_spill_sites > 0L, cum_distance_sum / n_spill_sites, NA_real_
    ),
    min_distance = data.table::fifelse(
      n_spill_sites > 0L, min_distance, NA_real_
    ),
    has_missing_site = data.table::fifelse(
      is.na(cum_missing_sites), FALSE, cum_missing_sites > 0
    ),
    has_unknown_evidence = data.table::fifelse(
      is.na(cum_unknown_evidence), FALSE, cum_unknown_evidence
    ),
    annual_returns_na_then_absent = data.table::fifelse(
      is.na(cum_annual_returns_na_then_absent),
      FALSE,
      cum_annual_returns_na_then_absent
    )
  )]
  # No masking here: the cumulative measures stay as measured, and the
  # derivation layer decides what has_missing_site hides (R16).
  metrics <- metrics[, .(
    transaction_id, radius, spill_hrs, n_spill_sites, spill_count,
    mean_distance, min_distance, has_missing_site, has_unknown_evidence,
    annual_returns_na_then_absent
  )]
  result <- metrics[grid, on = .(transaction_id, radius)]
  result[is.na(has_missing_site), has_missing_site := FALSE]
  result[is.na(has_unknown_evidence), has_unknown_evidence := FALSE]
  result[is.na(annual_returns_na_then_absent), annual_returns_na_then_absent := FALSE]
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
      "has_missing_site", "annual_returns_na_then_absent",
      "spill_count_daily_avg", "spill_hrs_daily_avg",
      "spill_count_weekly_avg", "spill_hrs_weekly_avg"
    )
  }
  result[, ..columns]
}

prior_exposure_site_prototype <- function(contract, transaction_ids = character()) {
  result <- data.table::data.table(
    transaction_id = transaction_ids[0], transaction_value = double(),
    n_days_in_window = integer(), site_id = integer(), radius = double(),
    distance_m = double(), spill_hrs = double(), spill_count = double(),
    site_missing = logical(), spill_count_daily_avg = double(),
    spill_hrs_daily_avg = double(), spill_count_weekly_avg = double(),
    spill_hrs_weekly_avg = double()
  )
  prior_exposure_project_public(result, contract)
}

# Derivation layer
############################################################

# Attach the four published rate columns through the shared rate helper, so
# both derivations and the study family share one formula.
prior_exposure_attach_rates <- function(result) {
  count_rates <- spill_window_rates(result$spill_count, result$n_days_in_window)
  hour_rates <- spill_window_rates(result$spill_hrs, result$n_days_in_window)
  result[, `:=`(
    spill_count_daily_avg = count_rates$daily_avg,
    spill_hrs_daily_avg = hour_rates$daily_avg,
    spill_count_weekly_avg = count_rates$weekly_avg,
    spill_hrs_weekly_avg = hour_rates$weekly_avg
  )]
}

#' Derive one site-grain public chunk from measurement rows.
#'
#' A thin derivation over the unmasked measurement grain: replicate each pair
#' per configured radius that contains it, rejoin transaction metadata from
#' the in-memory ledger, and apply the Stage-1 verdict — today's finding-11
#' rule, the OR of `annual_returns_absent`, `annual_returns_na`, and
#' `reported_positive_without_matched_events` (R15, R16). A transaction with
#' no nearby Site Group keeps no rows here; only the radius grain
#' re-enumerates the universe (R4). The rename of `annual_returns_absent` to
#' `site_missing` happens in this final projection and nowhere else (R19).
#'
#' @param measurement Measurement rows in the market's measurement schema.
#' @param transaction_ids Transaction identifiers in this chunk.
#' @param data Loaded inputs carrying the contract, config, and ledger.
#' @return The chunk's public site-grain rows.
prior_exposure_derive_site_grain <- function(measurement, transaction_ids, data) {
  contract <- data$contract
  metadata <- prior_exposure_metadata(data$transaction_dt, transaction_ids)
  rows <- data.table::as.data.table(data.table::copy(measurement))
  data.table::setnames(rows, contract$id, "transaction_id")
  metrics <- prior_exposure_reduce_site(rows, data$config$radius_thresholds)
  if (is.null(metrics) || nrow(metrics) == 0L) {
    return(prior_exposure_site_prototype(contract, metadata$transaction_id))
  }
  # The event-evidence verdict, computed here and never stored (R15).
  metrics[
    annual_returns_absent | annual_returns_na |
      reported_positive_without_matched_events,
    `:=`(spill_hrs = NA_real_, spill_count = NA_real_)
  ]
  metrics[, site_missing := annual_returns_absent]
  result <- metadata[metrics, on = "transaction_id"]
  prior_exposure_attach_rates(result)
  prior_exposure_project_public(result, contract)
}

#' Derive one radius-grain public chunk from measurement rows.
#'
#' Re-enumerates the transaction universe from the in-memory
#' eligible-transaction ledger, so a transaction with zero nearby Site Groups
#' still gets its complete zero radius grid (R4). Runs the established
#' distance-ordered cumulative reduction with stable sums, then applies the
#' event-evidence verdict — the OR of `annual_returns_absent`,
#' `annual_returns_na`, and `reported_positive_without_matched_events` (R15,
#' R17). `has_missing_site` keeps publishing `annual_returns_absent` alone, so
#' the mask widens without the column changing meaning, and
#' `annual_returns_na_then_absent` passes through unchanged (R16, R19).
#'
#' @param measurement Measurement rows in the market's measurement schema.
#' @param transaction_ids Transaction identifiers in this chunk.
#' @param data Loaded inputs carrying the contract, config, and ledger.
#' @return The chunk's public radius-grain rows.
prior_exposure_derive_radius_grain <- function(measurement, transaction_ids, data) {
  contract <- data$contract
  metadata <- prior_exposure_metadata(data$transaction_dt, transaction_ids)
  rows <- data.table::as.data.table(data.table::copy(measurement))
  data.table::setnames(rows, contract$id, "transaction_id")
  site_metrics <- rows[, .(
    transaction_id, site_id, distance_m,
    site_missing = annual_returns_absent,
    site_unknown_evidence = annual_returns_absent | annual_returns_na |
      reported_positive_without_matched_events,
    annual_returns_na_then_absent,
    spill_hrs, spill_count
  )]
  metrics <- prior_exposure_reduce_radius(
    site_metrics, metadata$transaction_id, data$config$radius_thresholds
  )
  # The event-evidence verdict, computed here and never stored (R15), and
  # applied at the derivation boundary rather than inside the reducer (R16).
  metrics[has_unknown_evidence == TRUE, `:=`(
    spill_hrs = NA_real_, spill_count = NA_real_
  )]
  result <- metadata[metrics, on = "transaction_id"]
  prior_exposure_attach_rates(result)
  prior_exposure_project_public(result, contract)
}

# The public chunk path: build the chunk's unmasked measurement rows, then
# hand them to the grain's derivation. This is the same measurement
# computation the published measurement tables stream through, so the public
# datasets are derivations of the measurement layer, never of another masked
# dataset.
prior_exposure_process_joined_chunk <- function(transaction_ids, data, joined) {
  measurement <- prior_exposure_measurement_chunk(transaction_ids, data, joined)
  if (data$contract$grain == "site") {
    prior_exposure_derive_site_grain(measurement, transaction_ids, data)
  } else {
    prior_exposure_derive_radius_grain(measurement, transaction_ids, data)
  }
}

prior_exposure_process_chunk <- function(transaction_ids, data) {
  joined <- prior_exposure_join_events(transaction_ids, data)
  prior_exposure_process_joined_chunk(transaction_ids, data, joined)
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

# Measurement layer
############################################################

#' Build one chunk of the unmasked measurement table.
#'
#' The pair rows the engine already computes, kept rather than immediately
#' masked (KTD1). Real pairs only, per R4: a transaction with no nearby Site
#' Group contributes nothing, and no sentinel row stands in for it. No
#' `min(distance_m)` dedupe, per R13 — pair uniqueness is asserted in the gate
#' instead, so a duplicated lookup pair fails loudly rather than collapsing.
#'
#' @param transaction_ids Transaction identifiers in this chunk.
#' @param data Eagerly loaded prior-exposure inputs.
#' @param joined Output of `prior_exposure_join_events()`.
#' @return A data.table of measurement rows, keyed by transaction and site.
prior_exposure_measurement_chunk <- function(transaction_ids, data, joined) {
  contract <- data$contract
  lookup <- data.table::copy(joined$internal_lookup)
  measures <- c(
    "transaction_id", "site_id", "distance_m", "spill_hrs", "spill_count",
    "annual_returns_absent", "annual_returns_na",
    "reported_positive_without_matched_events",
    "annual_returns_na_then_absent"
  )
  if (nrow(lookup) == 0L) {
    # A chunk whose transactions have no nearby Site Group contributes nothing,
    # but it still has to carry the schema's exact shape so the stage stays
    # bindable with every other chunk.
    empty <- data.table::data.table(
      transaction_id = character(), site_id = integer(),
      distance_m = numeric(), spill_hrs = numeric(), spill_count = numeric(),
      annual_returns_absent = logical(), annual_returns_na = logical(),
      reported_positive_without_matched_events = logical(),
      annual_returns_na_then_absent = logical()
    )
    data.table::setnames(empty, "transaction_id", contract$id)
    return(empty)
  }

  events <- joined$events_dt
  if (!is.null(events) && nrow(events) > 0L) {
    events <- data.table::copy(events)
    data.table::setnames(events, contract$id, "transaction_id")
    totals <- collapse_events_by_group(
      events,
      by = c("transaction_id", "site_id"),
      order_by = prior_exposure_event_order_key(),
      na.rm = TRUE
    )
    lookup <- merge(
      lookup, totals, by = c("transaction_id", "site_id"), all.x = TRUE
    )
  } else {
    lookup[, `:=`(spill_hrs = 0, spill_count = 0)]
  }
  # A pair with no events in the window is a measured zero, not a gap: the
  # event feed carries positives only.
  lookup[is.na(spill_hrs), spill_hrs := 0]
  lookup[is.na(spill_count), spill_count := 0]

  data.table::setnames(lookup, "transaction_id", contract$id)
  measures[measures == "transaction_id"] <- contract$id
  lookup[, ..measures]
}

#' Cast a projected candidate onto its authoritative Arrow schema.
#'
#' The lossless-cast cascade shared by the public and measurement candidates.
#' It checks representability rather than coercing silently, so a value that
#' cannot round-trip into its Arrow type stops the build by column name. Domain
#' rules beyond representability stay with each caller.
#'
#' @param data Candidate rows already projected onto `expected_schema$names`.
#' @param expected_schema The authoritative Arrow schema.
#' @param allow_missing_bool Whether `NA` is admissible in a boolean column.
#' @param identifier_label What a string column holds, for its error message.
#' @return A copy of `data`, cast.
prior_exposure_cast_to_schema <- function(data, expected_schema,
                                          allow_missing_bool = TRUE,
                                          identifier_label = "identifiers") {
  types <- prior_exposure_schema_signature(expected_schema)
  result <- data.table::copy(data.table::as.data.table(data))
  for (column in expected_schema$names) {
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
      if (!is.logical(value) || (!allow_missing_bool && anyNA(value))) {
        stop(
          column,
          if (allow_missing_bool) {
            " must be logical for Arrow bool."
          } else {
            " must be nonmissing logical for Arrow bool."
          },
          call. = FALSE
        )
      }
    } else if (target == "string") {
      if (!is.character(value) || anyNA(value) || any(!nzchar(value))) {
        stop(column, " must contain nonmissing ", identifier_label, ".", call. = FALSE)
      }
    }
  }
  result
}

#' Project and cast a measurement chunk onto its authoritative schema.
#'
#' @param data Measurement rows.
#' @param expected_schema The market's measurement schema.
#' @return The chunk, cast, with the schema's exact fields in order.
prior_exposure_prepare_measurement <- function(data, expected_schema) {
  data <- data.table::as.data.table(data)
  missing <- setdiff(expected_schema$names, names(data))
  extra <- setdiff(names(data), expected_schema$names)
  if (length(missing) > 0L || length(extra) > 0L) {
    stop(
      "Measurement result does not match its schema fields.",
      call. = FALSE
    )
  }
  # The measurement layer stores evidence, never absence of evidence, so its
  # boolean flags admit no NA.
  result <- prior_exposure_cast_to_schema(
    data[, expected_schema$names, with = FALSE], expected_schema,
    allow_missing_bool = FALSE
  )
  if (anyNA(result$site_id) || anyNA(result$distance_m)) {
    stop("Measurement keys and distances must be nonmissing.", call. = FALSE)
  }
  result
}

prior_exposure_measurement_key_columns <- function(contract) {
  c(contract$id, "site_id")
}

#' Derive the exact pair keys a measurement chunk must contain.
#'
#' The lookup pairs themselves, unmodified. R13 replaces the engine's
#' defensive `min(distance_m)` dedupe with this expectation plus the gate's
#' uniqueness assertion, so a duplicated pair surfaces rather than collapsing.
#'
#' @param transaction_ids Transaction identifiers in this chunk.
#' @param data Eagerly loaded prior-exposure inputs.
#' @param lookup The chunk's internal lookup pairs.
#' @return A data.table of expected keys, sorted.
prior_exposure_expected_measurement_keys <- function(transaction_ids, data,
                                                     lookup = NULL) {
  contract <- data$contract
  if (is.null(lookup)) {
    lookup <- data$internal_lookup_dt[
      data.table::data.table(transaction_id = transaction_ids),
      on = "transaction_id", nomatch = 0L
    ]
  }
  keys <- data.table::data.table(
    transaction_id = as.character(lookup$transaction_id),
    site_id = as.integer(lookup$site_id)
  )
  data.table::setnames(keys, "transaction_id", contract$id)
  data.table::setorderv(keys, prior_exposure_measurement_key_columns(contract))
  keys
}

#' Stop on the first duplicated transaction-site pair, naming it.
#'
#' R13 replaced the engine's defensive `min(distance_m)` dedupe with an
#' assertion, so both the chunk-local and stage-wide gates fail the same way.
#'
#' @param keys A table of the two key columns.
#' @param contract The measurement contract.
#' @return `TRUE`, invisibly, when no pair repeats.
prior_exposure_assert_unique_pairs <- function(keys, contract) {
  duplicated_rows <- duplicated(keys)
  if (any(duplicated_rows)) {
    offender <- keys[which(duplicated_rows)[1L]]
    stop(
      "Measurement table contains a duplicate transaction-site pair: ",
      contract$id, "=", offender[[contract$id]],
      ", site_id=", offender$site_id, ".",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

prior_exposure_validate_measurement_keys <- function(chunk, expected_keys,
                                                     contract) {
  key_columns <- prior_exposure_measurement_key_columns(contract)
  actual_keys <- data.table::copy(chunk[, ..key_columns])
  prior_exposure_assert_unique_pairs(actual_keys, contract)
  data.table::setorderv(actual_keys, key_columns)
  expected_keys <- data.table::copy(expected_keys)
  data.table::setorderv(expected_keys, key_columns)
  data.table::setkeyv(actual_keys, NULL)
  data.table::setkeyv(expected_keys, NULL)
  if (!identical(actual_keys, expected_keys)) {
    stop(
      "Measurement chunk keys do not exactly match their expected keys.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Validate a staged measurement dataset before promotion.
#'
#' Reopens the stage, pins the schema, conserves rows through a bounded
#' row-group scan, and asserts pair uniqueness across the whole stage rather
#' than only within a chunk.
#'
#' The scan reads one row group at a time, so the stage is never materialized
#' whole. Uniqueness across the stage does require remembering every key seen,
#' which is the one allocation that necessarily grows with the table; only the
#' two key columns are retained, and each row group is released before the next
#' is read.
#'
#' @param stage_path Staged dataset directory.
#' @param expected_schema The market's measurement schema.
#' @param expected_rows Scalar expected row total.
#' @param contract The measurement contract.
#' @return `TRUE`, invisibly.
prior_exposure_validate_measurement_stage <- function(stage_path,
                                                      expected_schema,
                                                      expected_rows,
                                                      contract) {
  if (!dir.exists(stage_path) || expected_rows == 0) {
    stop("Cannot publish an empty measurement stage.", call. = FALSE)
  }
  staged <- tryCatch(
    arrow::open_dataset(stage_path),
    error = function(error) {
      stop(
        "Staged measurement dataset could not be reopened: ",
        conditionMessage(error),
        call. = FALSE
      )
    }
  )
  actual_signature <- prior_exposure_schema_signature(staged$schema)
  expected_signature <- prior_exposure_schema_signature(expected_schema)
  if (!identical(actual_signature, expected_signature)) {
    stop(
      "Staged measurement schema mismatch. Expected ",
      paste(names(expected_signature), expected_signature, collapse = ", "),
      "; found ",
      paste(names(actual_signature), actual_signature, collapse = ", "), ".",
      call. = FALSE
    )
  }

  key_columns <- prior_exposure_measurement_key_columns(contract)
  fragment_paths <- list.files(
    stage_path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE
  )
  key_batches <- vector("list", 0L)
  scanned_rows <- 0
  for (fragment_path in fragment_paths) {
    reader <- arrow::ParquetFileReader$create(fragment_path)
    for (row_group_index in seq_len(reader$num_row_groups)) {
      batch <- reader$ReadRowGroup(row_group_index - 1L)$to_data_frame()
      scanned_rows <- scanned_rows + nrow(batch)
      key_batches[[length(key_batches) + 1L]] <-
        data.table::as.data.table(batch[, key_columns, drop = FALSE])
      rm(batch)
      gc(verbose = FALSE)
    }
  }
  if (!identical(as.numeric(scanned_rows), as.numeric(expected_rows))) {
    stop(
      "Staged measurement row count mismatch: expected ", expected_rows,
      ", found ", scanned_rows, ".",
      call. = FALSE
    )
  }
  seen_keys <- data.table::rbindlist(key_batches)
  rm(key_batches)
  gc(verbose = FALSE)
  prior_exposure_assert_unique_pairs(seen_keys, contract)
  if (anyNA(seen_keys)) {
    stop("Measurement keys must never be missing.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Publish a staged measurement generation.
#'
#' @param stage_path Incrementally assembled sibling stage.
#' @param output_path Canonical Arrow dataset directory.
#' @param expected_schema The market's measurement schema.
#' @param expected_rows Scalar expected row total.
#' @param contract The measurement contract.
#' @param rename_path Injectable directory-rename seam used by focused tests.
#' @param remove_path Injectable removal seam used by focused tests.
#' @return `output_path`, invisibly.
publish_prior_exposure_measurement <- function(
    stage_path, output_path, expected_schema, expected_rows, contract,
    rename_path = file.rename,
    remove_path = function(path) unlink(path, recursive = TRUE)) {
  if (!exists("publish_validated_dataset", mode = "function", inherits = TRUE)) {
    stop(
      "dataset_publication_utils.R must be sourced before prior_exposure_utils.R.",
      call. = FALSE
    )
  }
  publish_validated_dataset(
    stage_path = stage_path,
    output_path = output_path,
    validate = function(path) {
      prior_exposure_validate_measurement_stage(
        path, expected_schema, expected_rows, contract
      )
    },
    rename_path = rename_path,
    remove_path = remove_path
  )
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
  result <- prior_exposure_cast_to_schema(
    data, expected_schema,
    identifier_label = "transaction identifiers"
  )
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
    any(is.nan(x) | (!is.na(x) & !is.finite(x)))
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

prior_exposure_validate_radii <- function(expected_radii) {
  expected_radii <- as.numeric(expected_radii)
  if (length(expected_radii) == 0L || anyNA(expected_radii) ||
      any(!is.finite(expected_radii)) || any(expected_radii < 0) ||
      any(expected_radii != floor(expected_radii)) ||
      any(expected_radii > .Machine$integer.max) ||
      anyDuplicated(expected_radii)) {
    stop("expected_radii must be unique, nonnegative integers.", call. = FALSE)
  }
  sort(as.integer(expected_radii))
}

prior_exposure_create_stage <- function(output_path) {
  parent_dir <- dirname(output_path)
  dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
  tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = parent_dir
  )
}

prior_exposure_public_key_columns <- function(contract) {
  if (contract$grain == "site") {
    c(contract$id, "site_id", "radius")
  } else {
    c(contract$id, "radius")
  }
}

prior_exposure_expected_chunk_keys <- function(transaction_ids, data, lookup = NULL) {
  contract <- data$contract
  radii <- sort(as.integer(data$config$radius_thresholds))
  if (contract$grain == "radius") {
    keys <- data.table::CJ(
      transaction_id = transaction_ids,
      radius = radii,
      unique = TRUE
    )
  } else {
    if (is.null(lookup)) {
      lookup <- data$internal_lookup_dt[
        data.table::data.table(transaction_id = transaction_ids),
        on = "transaction_id", nomatch = 0L
      ]
    }
    if (nrow(lookup) == 0L) {
      keys <- data.table::data.table(
        transaction_id = transaction_ids[0],
        site_id = integer(),
        radius = integer()
      )
    } else {
      # The lookup pairs themselves, not a min(distance_m) collapse: pair
      # uniqueness is asserted rather than deduped away (R13), so a duplicated
      # pair surfaces as a duplicate-key failure downstream.
      lookup <- lookup[, .(transaction_id, site_id, distance_m)]
      keys <- lookup[rep(seq_len(.N), each = length(radii))]
      keys[, radius := rep(radii, times = nrow(lookup))]
      keys <- keys[distance_m <= radius, .(transaction_id, site_id, radius)]
    }
  }
  data.table::setnames(keys, "transaction_id", contract$id)
  data.table::set(keys, j = contract$id, value = as.character(keys[[contract$id]]))
  data.table::setorderv(keys, prior_exposure_public_key_columns(contract))
  keys
}

prior_exposure_validate_chunk_keys <- function(chunk, expected_keys, contract) {
  key_columns <- prior_exposure_public_key_columns(contract)
  actual_keys <- data.table::copy(chunk[, ..key_columns])
  if (anyDuplicated(actual_keys)) {
    stop("Prior-exposure chunk contains duplicate public keys.", call. = FALSE)
  }
  data.table::setorderv(actual_keys, key_columns)
  expected_keys <- data.table::copy(expected_keys)
  data.table::setorderv(expected_keys, key_columns)
  data.table::setkeyv(actual_keys, NULL)
  data.table::setkeyv(expected_keys, NULL)
  if (!identical(actual_keys, expected_keys)) {
    stop(
      "Prior-exposure chunk keys do not exactly match their expected keys.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Write one chunk into the sibling stage.
#'
#' @param chunk The chunk to write.
#' @param stage_path The run's sibling stage directory.
#' @param chunk_index Monotone chunk index.
#' @param partitioning Hive partitioning columns, or `NULL` for none. The
#'   measurement grain has no `radius` to partition on (R5).
#' @return `stage_path`, invisibly.
prior_exposure_write_chunk <- function(chunk, stage_path, chunk_index,
                                       partitioning = "radius") {
  # The stage itself is unique per run; the monotone chunk index therefore
  # gives every write a collision-proof namespace inside that stage.
  fragment_token <- sprintf("chunk-%010d", as.integer(chunk_index))
  arrow::write_dataset(
    chunk,
    path = stage_path,
    format = "parquet",
    partitioning = partitioning,
    basename_template = paste0(fragment_token, "-{i}.parquet"),
    existing_data_behavior = "overwrite"
  )
  invisible(stage_path)
}

prior_exposure_validate_stage <- function(
    stage_path, expected_schema, expected_radii, expected_rows) {
  if (!dir.exists(stage_path) || expected_rows == 0) {
    stop("Cannot publish an empty prior-exposure stage.", call. = FALSE)
  }
  expected_radii <- prior_exposure_validate_radii(expected_radii)
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

  rate_columns <- grep("_(daily|weekly)_avg$", expected_schema$names, value = TRUE)
  checked_rows <- 0
  fragment_paths <- list.files(
    stage_path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE
  )
  for (fragment_path in fragment_paths) {
    reader <- arrow::ParquetFileReader$create(fragment_path)
    for (row_group_index in seq_len(reader$num_row_groups)) {
      batch <- reader$ReadRowGroup(row_group_index - 1L)$to_data_frame()
      if ("n_days_in_window" %in% names(batch) &&
          (anyNA(batch$n_days_in_window) || any(batch$n_days_in_window < 30L))) {
        stop(
          "Published prior-exposure rows require at least 30 complete days.",
          call. = FALSE
        )
      }
      if (any(vapply(batch[, rate_columns, drop = FALSE], function(value) {
        any(is.nan(value) | (!is.na(value) & !is.finite(value)))
      }, logical(1)))) {
        stop("Published prior-exposure rates must be finite or NA.", call. = FALSE)
      }
      checked_rows <- checked_rows + nrow(batch)
      rm(batch)
      gc(verbose = FALSE)
    }
  }
  if (!identical(as.numeric(checked_rows), as.numeric(expected_rows))) {
    stop("Staged prior-exposure bounded scan did not conserve rows.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Stream prior-exposure chunks into one validated sibling stage.
#'
#' @param data Eagerly loaded prior-exposure inputs.
#' @param output_path Canonical Arrow dataset directory.
#' @param join_chunk Injectable event-join seam used by focused tests.
#' @param before_publish Injectable validation-failure seam used by focused tests.
#' @param profile The stage-shaping steps this stream varies. A focused test
#'   overrides one step by building its profile with that argument set; there
#'   is no second injection mechanism.
#' @return `output_path`, invisibly.
prior_exposure_stream <- function(
    data, output_path,
    join_chunk = prior_exposure_join_events,
    before_publish = function(stage_path) invisible(stage_path),
    profile = prior_exposure_public_stream_profile()) {
  contract <- data$contract
  expected_schema <- contract$schema
  chunk_size <- data$config$chunk_size
  if (!is.numeric(chunk_size) || length(chunk_size) != 1L || is.na(chunk_size) ||
      chunk_size < 1 || chunk_size != floor(chunk_size)) {
    stop("chunk_size must be one positive integer.", call. = FALSE)
  }

  transaction_ids <- data$transaction_dt$transaction_id
  if (!is.character(transaction_ids) || anyNA(transaction_ids) ||
      any(!nzchar(transaction_ids)) || anyDuplicated(transaction_ids)) {
    stop(
      "Streaming requires unique, nonmissing transaction identifiers.",
      call. = FALSE
    )
  }
  stage_path <- prior_exposure_create_stage(output_path)
  on.exit({
    if (dir.exists(stage_path)) {
      cleanup_status <- unlink(stage_path, recursive = TRUE)
      if (cleanup_status != 0L && dir.exists(stage_path)) {
        warning("Could not remove prior-exposure stage: ", stage_path, call. = FALSE)
      }
    }
  }, add = TRUE)

  starts <- seq.int(1L, length(transaction_ids), by = as.integer(chunk_size))
  expected_rows <- 0
  written_rows <- 0
  assigned_transactions <- 0
  for (chunk_index in seq_along(starts)) {
    started_at <- proc.time()[["elapsed"]]
    start <- starts[[chunk_index]]
    end <- min(start + as.integer(chunk_size) - 1L, length(transaction_ids))
    chunk_ids <- transaction_ids[start:end]
    if (anyNA(chunk_ids) || anyDuplicated(chunk_ids)) {
      stop("A streaming chunk has invalid transaction ownership.", call. = FALSE)
    }
    assigned_transactions <- assigned_transactions + length(chunk_ids)

    joined <- join_chunk(chunk_ids, data)
    lookup_rows <- nrow(joined$internal_lookup)
    joined_event_rows <- if (is.null(joined$events_dt)) 0L else nrow(joined$events_dt)
    chunk <- profile$process(chunk_ids, data, joined)
    chunk <- profile$prepare(chunk, data)
    expected_keys <- profile$expected_keys(
      chunk_ids, data, lookup = joined$internal_lookup
    )
    profile$validate_keys(chunk, expected_keys, contract)

    chunk_expected_rows <- nrow(expected_keys)
    chunk_written_rows <- nrow(chunk)
    expected_rows <- expected_rows + chunk_expected_rows
    if (chunk_written_rows > 0L) {
      profile$write(chunk, stage_path, chunk_index)
      written_rows <- written_rows + chunk_written_rows
    }
    elapsed <- proc.time()[["elapsed"]] - started_at
    logger::log_info(
      paste0(
        "Prior-exposure chunk {chunk_index}/{length(starts)}: ",
        "transactions={length(chunk_ids)}, lookup_pairs={lookup_rows}, ",
        "joined_events={joined_event_rows}, output_rows={chunk_written_rows}, ",
        "elapsed_seconds={round(elapsed, 3)}, stage={basename(stage_path)}"
      )
    )
    rm(chunk, expected_keys, joined, chunk_ids)
    gc(verbose = FALSE)
  }
  if (!identical(as.numeric(assigned_transactions), as.numeric(length(transaction_ids)))) {
    stop("Streaming did not assign every transaction exactly once.", call. = FALSE)
  }
  if (!identical(as.numeric(written_rows), as.numeric(expected_rows))) {
    stop("Streaming expected-versus-written row totals differ.", call. = FALSE)
  }

  before_publish(stage_path)
  profile$publish_stage(stage_path, output_path, data, written_rows)
  invisible(output_path)
}

#' The four public prior-exposure datasets' streaming profile.
#'
#' Radius-partitioned, masked, with transaction metadata rejoined.
#'
#' @param process Injectable reducer seam used by focused tests.
#' @param write Injectable stage-writer seam used by focused tests.
#' @param publish Injectable publication seam used by focused tests.
#' @return A list of the stage-shaping steps `prior_exposure_stream()` varies.
prior_exposure_public_stream_profile <- function(
    process = prior_exposure_process_joined_chunk,
    write = prior_exposure_write_chunk,
    publish = publish_prior_exposure_dataset) {
  list(
    process = process,
    prepare = function(chunk, data) {
      prior_exposure_prepare_public(
        chunk, data$contract$market, data$contract$grain
      )
    },
    expected_keys = prior_exposure_expected_chunk_keys,
    validate_keys = prior_exposure_validate_chunk_keys,
    write = write,
    publish_stage = function(stage_path, output_path, data, written_rows) {
      publish(
        data = NULL,
        output_path = output_path,
        expected_schema = data$contract$schema,
        expected_radii = prior_exposure_validate_radii(
          data$config$radius_thresholds
        ),
        stage_path = stage_path,
        expected_rows = written_rows
      )
    }
  )
}

#' The internal measurement tables' streaming profile.
#'
#' Unpartitioned pair grain, unmasked, with pair uniqueness asserted in the
#' gate rather than deduped away.
#'
#' @param process Injectable reducer seam used by focused tests.
#' @param write Injectable stage-writer seam used by focused tests.
#' @param publish Injectable publication seam used by focused tests.
#' @return A list of the stage-shaping steps `prior_exposure_stream()` varies.
prior_exposure_measurement_stream_profile <- function(
    process = prior_exposure_measurement_chunk,
    write = function(chunk, stage_path, chunk_index) {
      prior_exposure_write_chunk(chunk, stage_path, chunk_index, partitioning = NULL)
    },
    publish = publish_prior_exposure_measurement) {
  list(
    process = process,
    prepare = function(chunk, data) {
      prior_exposure_prepare_measurement(chunk, data$contract$schema)
    },
    expected_keys = prior_exposure_expected_measurement_keys,
    validate_keys = prior_exposure_validate_measurement_keys,
    write = write,
    publish_stage = function(stage_path, output_path, data, written_rows) {
      publish(
        stage_path = stage_path,
        output_path = output_path,
        expected_schema = data$contract$schema,
        expected_rows = written_rows,
        contract = data$contract
      )
    }
  )
}

#' Build and publish one market's measurement table end to end.
#'
#' @param config Builder configuration.
#' @param market One of `sale` or `rental`.
#' @return `config$output_path`, invisibly.
prior_exposure_build_measurement <- function(config, market) {
  contract <- prior_exposure_measurement_contract(market)
  data <- prior_exposure_load_data(config, market, contract$grain, contract)
  prior_exposure_stream(
    data, config$output_path,
    profile = prior_exposure_measurement_stream_profile()
  )
}

#' Publish a complete radius-partitioned prior-exposure generation.
#'
#' A complete in-memory candidate is staged for backwards compatibility. A
#' caller may instead supply an incrementally assembled sibling `stage_path`.
#' Both paths use the same validation, backup, promotion, and restoration seam.
#' Publication assumes one writer per canonical path.
#'
#' @param data Complete in-memory candidate, or `NULL` for an existing stage.
#' @param output_path Canonical Arrow dataset directory.
#' @param expected_schema Literal on-disk Arrow schema, including Hive radius.
#' @param expected_radii Exact configured integer radius set.
#' @param rename_path Injectable directory-rename seam used by focused tests.
#' @param stage_path Existing incrementally assembled sibling stage.
#' @param expected_rows Scalar expected row total for `stage_path`.
#' @return `output_path`, invisibly.
publish_prior_exposure_dataset <- function(
    data, output_path, expected_schema, expected_radii,
    rename_path = file.rename, stage_path = NULL, expected_rows = NULL,
    remove_path = function(path) unlink(path, recursive = TRUE)) {
  supplied_stage <- !is.null(stage_path)
  if (supplied_stage && !is.null(data)) {
    stop("Supply either a complete candidate or an existing stage, not both.", call. = FALSE)
  }
  if (!supplied_stage) {
    data <- prior_exposure_validate_and_cast_public(data, expected_schema)
    expected_rows <- nrow(data)
    if (is.null(expected_rows) || expected_rows == 0L) {
      stop("Cannot publish an empty prior-exposure candidate.", call. = FALSE)
    }
    stage_path <- prior_exposure_create_stage(output_path)
  } else if (!is.numeric(expected_rows) || length(expected_rows) != 1L ||
      is.na(expected_rows) || !is.finite(expected_rows) ||
      expected_rows < 0 || expected_rows != floor(expected_rows)) {
    stop("expected_rows must be one nonnegative integer.", call. = FALSE)
  }
  expected_radii <- prior_exposure_validate_radii(expected_radii)
  on.exit({
    if (dir.exists(stage_path)) {
      cleanup_status <- unlink(stage_path, recursive = TRUE)
      if (cleanup_status != 0L && dir.exists(stage_path)) {
        warning("Could not remove prior-exposure stage: ", stage_path, call. = FALSE)
      }
    }
  }, add = TRUE)

  if (!supplied_stage) {
    arrow::write_dataset(
      data,
      path = stage_path,
      format = "parquet",
      partitioning = "radius"
    )
  }
  if (!exists("publish_validated_dataset", mode = "function", inherits = TRUE)) {
    stop(
      "dataset_publication_utils.R must be sourced before prior_exposure_utils.R.",
      call. = FALSE
    )
  }
  validator <- function(path) {
    prior_exposure_validate_stage(
      path, expected_schema, expected_radii, expected_rows
    )
  }
  publish_validated_dataset(
    stage_path = stage_path,
    output_path = output_path,
    validate = validator,
    rename_path = rename_path,
    remove_path = remove_path
  )
}
