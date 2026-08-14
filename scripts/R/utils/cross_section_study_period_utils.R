############################################################
# Study-Period Cross-Section Utilities
# Project: Sewage
############################################################

study_period_schema_signature <- function(schema) {
  stats::setNames(
    vapply(schema$fields, function(field) field$type$ToString(), character(1)),
    schema$names
  )
}

study_period_window <- function(start_date, end_date) {
  if (!inherits(start_date, "Date") || length(start_date) != 1L ||
      is.na(start_date) || !inherits(end_date, "Date") ||
      length(end_date) != 1L || is.na(end_date)) {
    stop("Study-period bounds must each be one nonmissing Date.", call. = FALSE)
  }
  if (start_date > end_date) {
    stop("Study-period bounds must be ordered from start to end.", call. = FALSE)
  }
  if (format(start_date, "%m-%d") != "01-01") {
    stop("The study period must start on 1 January.", call. = FALSE)
  }
  if (format(end_date, "%m-%d") != "12-31") {
    stop("The study period must end on 31 December.", call. = FALSE)
  }

  start_year <- as.integer(format(start_date, "%Y"))
  end_year <- as.integer(format(end_date, "%Y"))
  list(
    start_date = start_date,
    end_date = end_date,
    years = seq.int(start_year, end_year),
    n_days_in_window = as.integer(end_date - start_date) + 1L
  )
}

study_period_public_schema <- function(market) {
  if (!is.character(market) || length(market) != 1L || is.na(market) ||
      !market %in% c("sale", "rental")) {
    stop("market must be exactly one of: sale, rental.", call. = FALSE)
  }

  if (market == "sale") {
    return(arrow::schema(
      house_id = arrow::int32(),
      price = arrow::int32(),
      ppd_category = arrow::utf8(),
      n_days_in_window = arrow::int32(),
      spill_hrs = arrow::float64(),
      n_spill_sites = arrow::int32(),
      spill_count = arrow::float64(),
      mean_distance = arrow::float64(),
      min_distance = arrow::float64(),
      spatially_eligible = arrow::bool(),
      has_missing_site = arrow::bool(),
      spill_count_daily_avg = arrow::float64(),
      spill_hrs_daily_avg = arrow::float64(),
      spill_count_weekly_avg = arrow::float64(),
      spill_hrs_weekly_avg = arrow::float64(),
      radius = arrow::int32()
    ))
  }

  arrow::schema(
    rental_id = arrow::int32(),
    listing_price = arrow::float64(),
    n_days_in_window = arrow::int32(),
    spill_hrs = arrow::float64(),
    n_spill_sites = arrow::int32(),
    spill_count = arrow::float64(),
    mean_distance = arrow::float64(),
    min_distance = arrow::float64(),
    spatially_eligible = arrow::bool(),
    has_missing_site = arrow::bool(),
    spill_count_daily_avg = arrow::float64(),
    spill_hrs_daily_avg = arrow::float64(),
    spill_count_weekly_avg = arrow::float64(),
    spill_hrs_weekly_avg = arrow::float64(),
    radius = arrow::int32()
  )
}

study_period_validate_annual_states <- function(annual) {
  known_statuses <- c(
    "reported_zero", "reported_positive", "reported_na", "absent"
  )
  if (anyNA(annual$annual_status) ||
      any(!annual$annual_status %in% known_statuses)) {
    stop("annual_status contains an unknown or missing state.", call. = FALSE)
  }

  count <- annual$spill_count_ea
  hours <- annual$spill_hrs_ea
  known <- annual$annual_status %in% c("reported_zero", "reported_positive")
  if (any(known & (is.na(count) | is.na(hours)))) {
    bad_status <- annual$annual_status[which(known & (is.na(count) | is.na(hours)))[1L]]
    stop(bad_status, " requires both EA measures.", call. = FALSE)
  }
  if (any(known & (!is.finite(count) | !is.finite(hours)))) {
    stop("Known EA measures must be finite.", call. = FALSE)
  }
  if (any(known & (count < 0 | hours < 0))) {
    stop("Known EA measures must be nonnegative.", call. = FALSE)
  }

  reported_zero <- annual$annual_status == "reported_zero"
  if (any(reported_zero & (count != 0 | hours != 0))) {
    stop("reported_zero requires both EA measures to equal zero.", call. = FALSE)
  }
  reported_positive <- annual$annual_status == "reported_positive"
  if (any(reported_positive & count == 0 & hours == 0)) {
    stop(
      "reported_positive requires at least one strictly positive EA measure.",
      call. = FALSE
    )
  }

  unknown <- annual$annual_status %in% c("reported_na", "absent")
  if (any(unknown & (!is.na(count) | !is.na(hours)))) {
    bad_status <- annual$annual_status[
      which(unknown & (!is.na(count) | !is.na(hours)))[1L]
    ]
    stop(bad_status, " requires both EA measures to be missing.", call. = FALSE)
  }
  invisible(TRUE)
}

collapse_study_period_annual_returns <- function(annual_returns, window) {
  if (!is.list(window) || !is.integer(window$years) ||
      length(window$years) == 0L || !is.integer(window$n_days_in_window)) {
    stop("window must come from study_period_window().", call. = FALSE)
  }

  annual <- data.table::as.data.table(data.table::copy(annual_returns))
  required <- c(
    "site_id", "year", "annual_status", "spill_count_ea", "spill_hrs_ea"
  )
  missing_columns <- setdiff(required, names(annual))
  if (length(missing_columns) > 0L) {
    stop(
      "Annual-return crosswalk is missing required column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  annual <- annual[, ..required]
  if (!is.numeric(annual$site_id) || anyNA(annual$site_id) ||
      any(!is.finite(annual$site_id)) || any(annual$site_id != floor(annual$site_id)) ||
      any(annual$site_id < -.Machine$integer.max - 1) ||
      any(annual$site_id > .Machine$integer.max)) {
    stop("site_id must be a nonmissing lossless int32 value.", call. = FALSE)
  }
  if (!is.numeric(annual$year) || anyNA(annual$year) ||
      any(!is.finite(annual$year)) || any(annual$year != floor(annual$year))) {
    stop("year must contain nonmissing integer values.", call. = FALSE)
  }
  annual[, `:=`(site_id = as.integer(site_id), year = as.integer(year))]
  annual <- annual[year %in% window$years]

  missing_global_years <- setdiff(window$years, unique(annual$year))
  if (length(missing_global_years) > 0L) {
    stop(
      "Annual-return crosswalk is missing derived study year(s): ",
      paste(missing_global_years, collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (anyDuplicated(annual[, .(site_id, year)])) {
    stop("Annual-return crosswalk contains a duplicate Site Group-year.", call. = FALSE)
  }
  study_period_validate_annual_states(annual)

  complete_grid <- data.table::CJ(
    site_id = sort(unique(annual$site_id)),
    year = window$years,
    unique = TRUE
  )
  annual <- annual[complete_grid, on = .(site_id, year)]
  annual[, missing_evidence := is.na(annual_status) |
    annual_status %in% c("reported_na", "absent")]

  collapsed <- annual[
    order(site_id, year),
    {
      unknown <- any(missing_evidence)
      list(
        spill_count = if (unknown) NA_real_ else base::sum(spill_count_ea),
        spill_hrs = if (unknown) NA_real_ else base::sum(spill_hrs_ea),
        has_missing_evidence = unknown
      )
    },
    by = site_id
  ]
  data.table::setkey(collapsed, site_id)
  collapsed
}

study_period_market_contract <- function(market) {
  schema <- study_period_public_schema(market)
  if (market == "sale") {
    return(list(
      market = market,
      id = "house_id",
      value = "price",
      provenance = "ppd_category",
      source_columns = c(
        "house_id", "price", "ppd_category", "easting", "northing"
      ),
      public_source_columns = c("house_id", "price", "ppd_category"),
      schema = schema
    ))
  }
  list(
    market = market,
    id = "rental_id",
    value = "listing_price",
    provenance = NULL,
    source_columns = c(
      "rental_id", "listing_price", "easting", "northing"
    ),
    public_source_columns = c("rental_id", "listing_price"),
    schema = schema
  )
}

study_period_validate_radii <- function(radii) {
  if (!is.numeric(radii) || length(radii) == 0L || anyNA(radii) ||
      any(!is.finite(radii)) || any(radii != floor(radii)) ||
      any(radii < 0) || any(radii > .Machine$integer.max) ||
      anyDuplicated(radii)) {
    stop("radii must be unique, nonnegative integer values.", call. = FALSE)
  }
  radii <- sort(as.integer(radii))
  if (!identical(radii, c(250L, 500L, 1000L))) {
    stop("Supported study-period radii are exactly 250, 500, and 1000 m.", call. = FALSE)
  }
  radii
}

study_period_source_ledger <- function(source, contract) {
  source <- data.table::as.data.table(data.table::copy(source))
  missing_columns <- setdiff(contract$source_columns, names(source))
  if (length(missing_columns) > 0L) {
    stop(
      "Source metadata is missing required column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  source_columns <- contract$source_columns
  source <- source[, ..source_columns]
  id <- source[[contract$id]]
  if (!is.numeric(id) || anyNA(id) || any(!is.finite(id)) ||
      any(id != floor(id)) || any(id < -.Machine$integer.max - 1) ||
      any(id > .Machine$integer.max)) {
    stop(contract$id, " must contain nonmissing lossless int32 values.", call. = FALSE)
  }
  data.table::set(source, j = contract$id, value = as.integer(id))
  if (anyDuplicated(source[[contract$id]])) {
    stop("Source metadata contains duplicate transaction identifiers.", call. = FALSE)
  }
  if (!is.numeric(source$easting) || !is.numeric(source$northing)) {
    stop("Source easting and northing must be numeric.", call. = FALSE)
  }

  value <- source[[contract$value]]
  if (!is.numeric(value) || anyNA(value) || any(!is.finite(value))) {
    stop(contract$value, " must contain finite nonmissing values.", call. = FALSE)
  }
  if (contract$market == "sale") {
    if (any(value != floor(value)) || any(value < -.Machine$integer.max - 1) ||
        any(value > .Machine$integer.max)) {
      stop("price cannot be losslessly cast to int32.", call. = FALSE)
    }
    source[, price := as.integer(price)]
    if (!is.character(source$ppd_category) || anyNA(source$ppd_category) ||
        any(!source$ppd_category %in% c("A", "B"))) {
      stop("ppd_category must contain only nonmissing A or B values.", call. = FALSE)
    }
  } else {
    source[, listing_price := as.double(listing_price)]
  }

  source[, spatially_eligible :=
    !is.na(easting) & !is.na(northing) & is.finite(easting) & is.finite(northing)]
  data.table::setorderv(source, contract$id)
  source[, source_position := seq_len(.N)]
  data.table::setkeyv(source, contract$id)
  source
}

study_period_validate_lookup_row_group <- function(row_group, contract, ledger) {
  lookup <- data.table::as.data.table(data.table::copy(row_group))
  required <- c(
    contract$id, "site_id", "distance_m", "distance_km", "n_site_groups"
  )
  missing_columns <- setdiff(required, names(lookup))
  if (length(missing_columns) > 0L) {
    stop(
      "Lookup row group is missing required column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  lookup <- lookup[, ..required]
  if (nrow(lookup) == 0L) {
    stop("Lookup contains an empty physical row group.", call. = FALSE)
  }

  transaction_id <- lookup[[contract$id]]
  if (!is.numeric(transaction_id) || anyNA(transaction_id) ||
      any(!is.finite(transaction_id)) ||
      any(transaction_id != floor(transaction_id))) {
    stop("Lookup transaction identifiers must be nonmissing integers.", call. = FALSE)
  }
  data.table::set(lookup, j = contract$id, value = as.integer(transaction_id))
  source_position <- match(lookup[[contract$id]], ledger[[contract$id]])
  if (anyNA(source_position)) {
    stop("Lookup contains a transaction ID absent from the source ledger.", call. = FALSE)
  }
  if (any(!ledger$spatially_eligible[source_position])) {
    stop("Lookup contains a coordinate-ineligible source transaction.", call. = FALSE)
  }

  declared <- lookup$n_site_groups
  if (!is.numeric(declared) || anyNA(declared) || any(!is.finite(declared)) ||
      any(declared != floor(declared)) || any(declared < 0) ||
      any(declared > .Machine$integer.max)) {
    stop("n_site_groups must contain nonmissing nonnegative integers.", call. = FALSE)
  }
  lookup[, n_site_groups := as.integer(n_site_groups)]
  if (!is.numeric(lookup$site_id) ||
      any(!is.na(lookup$site_id) & (
        !is.finite(lookup$site_id) | lookup$site_id != floor(lookup$site_id) |
          lookup$site_id < -.Machine$integer.max - 1 |
          lookup$site_id > .Machine$integer.max
      ))) {
    stop("site_id must be missing or losslessly castable to int32.", call. = FALSE)
  }
  lookup[, site_id := as.integer(site_id)]
  if (!is.numeric(lookup$distance_m) || !is.numeric(lookup$distance_km)) {
    stop("Lookup distances must be numeric.", call. = FALSE)
  }

  positive <- !is.na(lookup$site_id)
  if (any(positive & (
      is.na(lookup$distance_m) | !is.finite(lookup$distance_m) |
        lookup$distance_m < 0 | is.na(lookup$distance_km) |
        !is.finite(lookup$distance_km) | lookup$distance_km < 0
    ))) {
    stop("Matched Site Groups require finite nonnegative distances.", call. = FALSE)
  }
  if (any(positive & abs(lookup$distance_km - lookup$distance_m / 1000) > 1e-10)) {
    stop("distance_km must equal distance_m divided by 1000.", call. = FALSE)
  }
  if (any(!positive & (!is.na(lookup$distance_m) | !is.na(lookup$distance_km)))) {
    stop("A null-site sentinel must have missing distances.", call. = FALSE)
  }
  if (anyDuplicated(lookup[positive, c(contract$id, "site_id"), with = FALSE])) {
    stop("Lookup contains a duplicate transaction-Site Group pair.", call. = FALSE)
  }

  group_contract <- lookup[, .(
    distinct_declared = data.table::uniqueN(n_site_groups),
    declared = n_site_groups[[1L]],
    rows = .N,
    matched_rows = sum(!is.na(site_id)),
    distinct_sites = data.table::uniqueN(site_id[!is.na(site_id)]),
    sentinel_rows = sum(is.na(site_id))
  ), by = c(contract$id)]
  if (any(group_contract$distinct_declared != 1L)) {
    stop("n_site_groups must be constant within each transaction.", call. = FALSE)
  }
  zero_invalid <- group_contract$declared == 0L & (
    group_contract$rows != 1L | group_contract$sentinel_rows != 1L
  )
  positive_invalid <- group_contract$declared > 0L & (
    group_contract$sentinel_rows != 0L |
      group_contract$matched_rows != group_contract$declared |
      group_contract$distinct_sites != group_contract$declared
  )
  if (any(zero_invalid | positive_invalid)) {
    stop(
      "Lookup rows do not reconcile to declared n_site_groups and sentinel shape.",
      call. = FALSE
    )
  }
  lookup
}

study_period_validate_and_cast_public <- function(
    data, contract, require_all_radii = TRUE) {
  data <- data.table::as.data.table(data.table::copy(data))
  expected_names <- contract$schema$names
  if (!identical(names(data), expected_names)) {
    stop(
      "Study-period fields must exactly match the authoritative schema order.",
      call. = FALSE
    )
  }
  signature <- study_period_schema_signature(contract$schema)
  for (column in expected_names) {
    value <- data[[column]]
    target <- unname(signature[[column]])
    if (target == "int32") {
      if (!is.numeric(value) || any(!is.na(value) & (
        !is.finite(value) | value != floor(value) |
          value < -.Machine$integer.max - 1 | value > .Machine$integer.max
      ))) {
        stop(column, " cannot be losslessly cast to int32.", call. = FALSE)
      }
      data[[column]] <- as.integer(value)
    } else if (target == "double") {
      if (!is.numeric(value) || any(is.nan(value) | (!is.na(value) & !is.finite(value)))) {
        stop(column, " must contain finite numbers or NA.", call. = FALSE)
      }
      data[[column]] <- as.double(value)
    } else if (target == "bool") {
      if (!is.logical(value) || anyNA(value)) {
        stop(column, " must contain nonmissing logical values.", call. = FALSE)
      }
    } else if (target == "string") {
      if (!is.character(value) || anyNA(value)) {
        stop(column, " must contain nonmissing strings.", call. = FALSE)
      }
    }
  }

  id <- contract$id
  if (anyNA(data[[id]]) || anyDuplicated(data[, c(id, "radius"), with = FALSE])) {
    stop("Study-period candidate contains missing or duplicate public keys.", call. = FALSE)
  }
  supported_radii <- c(250L, 500L, 1000L)
  if (isTRUE(require_all_radii)) {
    study_period_validate_radii(unique(data$radius))
  }
  if (!all(data$radius %in% supported_radii)) {
    stop("Study-period candidate contains an unexpected radius.", call. = FALSE)
  }
  if (anyNA(data$n_days_in_window) || any(data$n_days_in_window < 1L)) {
    stop("n_days_in_window must be a positive nonmissing integer.", call. = FALSE)
  }
  if (contract$market == "sale" && any(!data$ppd_category %in% c("A", "B"))) {
    stop("Sales ppd_category must contain only A or B.", call. = FALSE)
  }

  ineligible <- !data$spatially_eligible
  exposure_columns <- c(
    "spill_hrs", "spill_count", "spill_count_daily_avg",
    "spill_hrs_daily_avg", "spill_count_weekly_avg", "spill_hrs_weekly_avg"
  )
  if (any(data$has_missing_site[ineligible]) ||
      any(!is.na(data$n_spill_sites[ineligible])) ||
      any(!is.na(data$mean_distance[ineligible])) ||
      any(!is.na(data$min_distance[ineligible])) ||
      any(vapply(data[ineligible, ..exposure_columns], function(x) any(!is.na(x)), logical(1)))) {
    stop("Spatially ineligible rows must have unknown geography and exposure.", call. = FALSE)
  }

  eligible_zero <- data$spatially_eligible & data$n_spill_sites == 0L
  if (any(data$has_missing_site[eligible_zero]) ||
      any(!is.na(data$mean_distance[eligible_zero])) ||
      any(!is.na(data$min_distance[eligible_zero])) ||
      any(data$spill_count[eligible_zero] != 0) ||
      any(data$spill_hrs[eligible_zero] != 0)) {
    stop("Eligible no-site rows must retain true-zero exposure.", call. = FALSE)
  }
  eligible_sites <- data$spatially_eligible & data$n_spill_sites > 0L
  if (any(is.na(data$n_spill_sites[data$spatially_eligible])) ||
      any(is.na(data$mean_distance[eligible_sites])) ||
      any(is.na(data$min_distance[eligible_sites])) ||
      any(data$min_distance[eligible_sites] > data$mean_distance[eligible_sites]) ||
      any(data$mean_distance[eligible_sites] > data$radius[eligible_sites])) {
    stop("Eligible site rows contain invalid count or distance semantics.", call. = FALSE)
  }
  unknown <- eligible_sites & data$has_missing_site
  if (any(vapply(data[unknown, ..exposure_columns], function(x) any(!is.na(x)), logical(1)))) {
    stop("Rows with unknown Site Group evidence must have unknown exposure.", call. = FALSE)
  }
  known <- data$spatially_eligible & !data$has_missing_site
  if (anyNA(data[known, ..exposure_columns]) ||
      any(data$spill_count[known] < 0) || any(data$spill_hrs[known] < 0)) {
    stop("Known study-period exposure must be complete and nonnegative.", call. = FALSE)
  }
  tolerance <- 1e-12
  if (any(abs(
    data$spill_count_daily_avg[known] -
      data$spill_count[known] / data$n_days_in_window[known]
  ) > tolerance) || any(abs(
    data$spill_hrs_daily_avg[known] -
      data$spill_hrs[known] / data$n_days_in_window[known]
  ) > tolerance) || any(abs(
    data$spill_count_weekly_avg[known] -
      data$spill_count_daily_avg[known] * 7
  ) > tolerance) || any(abs(
    data$spill_hrs_weekly_avg[known] -
      data$spill_hrs_daily_avg[known] * 7
  ) > tolerance)) {
    stop("Study-period daily or weekly averages are inconsistent.", call. = FALSE)
  }
  data
}

study_period_reduce_validated_lookup_row_group <- function(
    lookup, ledger, site_totals, contract, radii, n_days_in_window) {
  radii <- study_period_validate_radii(radii)
  if (!is.numeric(n_days_in_window) || length(n_days_in_window) != 1L ||
      is.na(n_days_in_window) || !is.finite(n_days_in_window) ||
      n_days_in_window < 1 || n_days_in_window != floor(n_days_in_window)) {
    stop("n_days_in_window must be one positive integer.", call. = FALSE)
  }
  n_days_in_window <- as.integer(n_days_in_window)
  id <- contract$id
  transaction_ids <- unique(lookup[[id]])
  metadata <- ledger[
    data.table::data.table(transaction_id = transaction_ids),
    on = stats::setNames("transaction_id", id),
    nomatch = 0L
  ]
  data.table::setnames(metadata, "transaction_id", id, skip_absent = TRUE)
  public_source_columns <- contract$public_source_columns
  metadata <- metadata[, ..public_source_columns]
  base <- metadata[rep(seq_len(.N), each = length(radii))]
  base[, radius := rep(radii, times = nrow(metadata))]

  matched <- lookup[!is.na(site_id), c(id, "site_id", "distance_m"), with = FALSE]
  if (nrow(matched) > 0L) {
    matched_rows <- nrow(matched)
    expanded <- matched[rep(seq_len(matched_rows), each = length(radii))]
    expanded[, radius := rep(radii, times = matched_rows)]
    expanded <- expanded[distance_m <= radius]
    totals <- data.table::as.data.table(data.table::copy(site_totals))
    required_totals <- c(
      "site_id", "spill_count", "spill_hrs", "has_missing_evidence"
    )
    if (!all(required_totals %in% names(totals)) || anyDuplicated(totals$site_id)) {
      stop("Collapsed Site Group totals violate their unique schema.", call. = FALSE)
    }
    expanded <- totals[expanded, on = "site_id"]
    expanded[, evidence_unknown :=
      is.na(has_missing_evidence) | has_missing_evidence |
        is.na(spill_count) | is.na(spill_hrs)]
    aggregate <- expanded[, {
      if (.N < 1L || anyNA(distance_m)) {
        stop(
          "Matched Site Group aggregation requires nonmissing distances.",
          call. = FALSE
        )
      }
      unknown <- any(evidence_unknown)
      list(
        n_spill_sites = as.integer(.N),
        spill_count = if (unknown) NA_real_ else base::sum(spill_count),
        spill_hrs = if (unknown) NA_real_ else base::sum(spill_hrs),
        mean_distance = base::mean(distance_m),
        min_distance = base::min(distance_m),
        has_missing_site = unknown
      )
    }, by = c(id, "radius")]
    base <- merge(base, aggregate, by = c(id, "radius"), all.x = TRUE, sort = FALSE)
  } else {
    base[, `:=`(
      n_spill_sites = NA_integer_, spill_count = NA_real_, spill_hrs = NA_real_,
      mean_distance = NA_real_, min_distance = NA_real_,
      has_missing_site = NA
    )]
  }

  no_site <- is.na(base$n_spill_sites)
  base[no_site, `:=`(
    n_spill_sites = 0L,
    spill_count = 0,
    spill_hrs = 0,
    mean_distance = NA_real_,
    min_distance = NA_real_,
    has_missing_site = FALSE
  )]
  base[, `:=`(
    n_days_in_window = n_days_in_window,
    spatially_eligible = TRUE,
    spill_count_daily_avg = spill_count / n_days_in_window,
    spill_hrs_daily_avg = spill_hrs / n_days_in_window
  )]
  base[, `:=`(
    spill_count_weekly_avg = spill_count_daily_avg * 7,
    spill_hrs_weekly_avg = spill_hrs_daily_avg * 7
  )]
  data.table::setcolorder(base, contract$schema$names)
  study_period_validate_and_cast_public(base, contract)
}

study_period_reduce_lookup_row_group <- function(
    row_group, ledger, site_totals, contract, radii, n_days_in_window) {
  lookup <- study_period_validate_lookup_row_group(row_group, contract, ledger)
  study_period_reduce_validated_lookup_row_group(
    lookup, ledger, site_totals, contract, radii, n_days_in_window
  )
}

study_period_ineligible_rows <- function(
    ineligible_ledger, contract, radii, n_days_in_window) {
  radii <- study_period_validate_radii(radii)
  public_source_columns <- contract$public_source_columns
  metadata <- data.table::as.data.table(data.table::copy(ineligible_ledger))[
    , ..public_source_columns
  ]
  result <- metadata[rep(seq_len(.N), each = length(radii))]
  result[, radius := rep(radii, times = nrow(metadata))]
  result[, `:=`(
    n_days_in_window = as.integer(n_days_in_window),
    spill_hrs = NA_real_,
    n_spill_sites = NA_integer_,
    spill_count = NA_real_,
    mean_distance = NA_real_,
    min_distance = NA_real_,
    spatially_eligible = FALSE,
    has_missing_site = FALSE,
    spill_count_daily_avg = NA_real_,
    spill_hrs_daily_avg = NA_real_,
    spill_count_weekly_avg = NA_real_,
    spill_hrs_weekly_avg = NA_real_
  )]
  data.table::setcolorder(result, contract$schema$names)
  study_period_validate_and_cast_public(result, contract)
}

study_period_stream_lookup <- function(
    lookup_path, ledger, site_totals, contract, radii, n_days_in_window,
    write_fragment, ineligible_chunk_size = 100000L,
    log_progress = function(...) invisible(NULL)) {
  if (!file.exists(lookup_path)) {
    stop("Spatial lookup Parquet does not exist: ", lookup_path, call. = FALSE)
  }
  if (!is.function(write_fragment)) {
    stop("write_fragment must be a function.", call. = FALSE)
  }
  if (!is.numeric(ineligible_chunk_size) || length(ineligible_chunk_size) != 1L ||
      is.na(ineligible_chunk_size) || !is.finite(ineligible_chunk_size) ||
      ineligible_chunk_size < 1 || ineligible_chunk_size != floor(ineligible_chunk_size)) {
    stop("ineligible_chunk_size must be one positive integer.", call. = FALSE)
  }
  reader <- arrow::ParquetFileReader$create(lookup_path)
  if (reader$num_row_groups < 1L) {
    stop("Spatial lookup Parquet has no physical row groups.", call. = FALSE)
  }

  seen <- integer(nrow(ledger))
  fragment_index <- 0L
  output_rows <- 0
  for (row_group_index in seq_len(reader$num_row_groups)) {
    started <- proc.time()[["elapsed"]]
    row_group <- reader$ReadRowGroup(row_group_index - 1L)$to_data_frame()
    lookup <- study_period_validate_lookup_row_group(row_group, contract, ledger)
    transaction_ids <- unique(lookup[[contract$id]])
    positions <- match(transaction_ids, ledger[[contract$id]])
    if (any(seen[positions] != 0L)) {
      stop(
        "A transaction appears in more than one physical row group.",
        call. = FALSE
      )
    }
    seen[positions] <- 1L
    chunk <- study_period_reduce_validated_lookup_row_group(
      lookup, ledger, site_totals, contract, radii, n_days_in_window
    )
    fragment_index <- fragment_index + 1L
    write_fragment(chunk, fragment_index)
    output_rows <- output_rows + nrow(chunk)
    log_progress(
      row_group_index = row_group_index,
      row_groups = reader$num_row_groups,
      transactions = length(transaction_ids),
      lookup_pairs = nrow(lookup),
      output_rows = nrow(chunk),
      elapsed_seconds = proc.time()[["elapsed"]] - started
    )
    rm(row_group, lookup, chunk)
    gc(verbose = FALSE)
  }

  eligible <- ledger$spatially_eligible
  if (any(seen[eligible] != 1L)) {
    stop("Spatial lookup is missing coordinate-eligible source transactions.", call. = FALSE)
  }
  if (any(seen[!eligible] != 0L)) {
    stop("Coordinate-ineligible source transactions must not appear in the lookup.", call. = FALSE)
  }

  ineligible_positions <- which(!eligible)
  if (length(ineligible_positions) > 0L) {
    starts <- seq.int(
      1L, length(ineligible_positions), by = as.integer(ineligible_chunk_size)
    )
    for (start in starts) {
      end <- min(
        start + as.integer(ineligible_chunk_size) - 1L,
        length(ineligible_positions)
      )
      chunk <- study_period_ineligible_rows(
        ledger[ineligible_positions[start:end]],
        contract,
        radii,
        n_days_in_window
      )
      fragment_index <- fragment_index + 1L
      write_fragment(chunk, fragment_index)
      output_rows <- output_rows + nrow(chunk)
      rm(chunk)
      gc(verbose = FALSE)
    }
  }

  list(
    row_groups = as.integer(reader$num_row_groups),
    eligible_transactions = as.integer(sum(eligible)),
    ineligible_transactions = as.integer(sum(!eligible)),
    output_rows = output_rows,
    fragments = fragment_index
  )
}

study_period_write_fragment <- function(chunk, stage_path, fragment_index) {
  if (!is.numeric(fragment_index) || length(fragment_index) != 1L ||
      is.na(fragment_index) || !is.finite(fragment_index) ||
      fragment_index < 1 || fragment_index != floor(fragment_index)) {
    stop("fragment_index must be one positive integer.", call. = FALSE)
  }
  dir.create(dirname(stage_path), recursive = TRUE, showWarnings = FALSE)
  fragment_token <- sprintf("part-%010d", as.integer(fragment_index))
  arrow::write_dataset(
    chunk,
    path = stage_path,
    format = "parquet",
    partitioning = "radius",
    basename_template = paste0(fragment_token, "-{i}.parquet"),
    existing_data_behavior = "overwrite"
  )
  invisible(stage_path)
}

study_period_buffered_writer <- function(stage_path, batch_size = 20L) {
  if (!is.numeric(batch_size) || length(batch_size) != 1L || is.na(batch_size) ||
      !is.finite(batch_size) || batch_size < 1 || batch_size != floor(batch_size)) {
    stop("batch_size must be one positive integer.", call. = FALSE)
  }
  state <- new.env(parent = emptyenv())
  state$chunks <- list()
  state$fragments <- 0L

  flush <- function() {
    if (length(state$chunks) == 0L) return(invisible(stage_path))
    state$fragments <- state$fragments + 1L
    chunk <- data.table::rbindlist(state$chunks, use.names = TRUE)
    study_period_write_fragment(chunk, stage_path, state$fragments)
    state$chunks <- list()
    invisible(stage_path)
  }

  write <- function(chunk, fragment_index) {
    state$chunks[[length(state$chunks) + 1L]] <- data.table::copy(chunk)
    if (length(state$chunks) >= as.integer(batch_size)) flush()
    invisible(fragment_index)
  }

  list(
    write = write,
    flush = flush,
    fragments = function() state$fragments
  )
}

study_period_partition_radius <- function(fragment_path) {
  match <- regexec(
    paste0("(?:^|", .Platform$file.sep, ")radius=([0-9]+)(?:", .Platform$file.sep, "|$)"),
    fragment_path
  )
  parts <- regmatches(fragment_path, match)[[1L]]
  if (length(parts) != 2L) {
    stop(
      "Study-period fragment is not inside one Hive radius partition: ",
      fragment_path,
      call. = FALSE
    )
  }
  as.integer(parts[[2L]])
}

study_period_validate_dataset <- function(
    dataset_path, ledger, contract, radii, n_days_in_window,
    log_validation = function(...) invisible(NULL)) {
  radii <- study_period_validate_radii(radii)
  if (!dir.exists(dataset_path)) {
    stop("Study-period dataset does not exist: ", dataset_path, call. = FALSE)
  }
  started <- proc.time()[["elapsed"]]
  dataset <- tryCatch(
    arrow::open_dataset(dataset_path),
    error = function(error) {
      stop(
        "Study-period dataset could not be reopened: ",
        conditionMessage(error),
        call. = FALSE
      )
    }
  )
  actual_signature <- study_period_schema_signature(dataset$schema)
  expected_signature <- study_period_schema_signature(contract$schema)
  if (!identical(actual_signature, expected_signature)) {
    stop(
      "Study-period schema mismatch. Expected ",
      paste(names(expected_signature), expected_signature, collapse = ", "),
      "; found ",
      paste(names(actual_signature), actual_signature, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  fragment_paths <- sort(list.files(
    dataset_path,
    pattern = "[.]parquet$",
    recursive = TRUE,
    full.names = TRUE
  ))
  if (length(fragment_paths) == 0L) {
    stop("Cannot validate an empty study-period dataset.", call. = FALSE)
  }
  fragment_radii <- vapply(
    fragment_paths,
    study_period_partition_radius,
    integer(1)
  )
  if (!identical(sort(unique(fragment_radii)), radii)) {
    stop("Study-period radius partitions do not match the configured radii.", call. = FALSE)
  }

  n_sources <- nrow(ledger)
  occurrences <- matrix(
    0L,
    nrow = n_sources,
    ncol = length(radii),
    dimnames = list(NULL, as.character(radii))
  )
  checked_rows <- 0
  checked_bytes <- 0
  checked_row_groups <- 0L
  for (fragment_index in seq_along(fragment_paths)) {
    fragment_path <- fragment_paths[[fragment_index]]
    radius <- fragment_radii[[fragment_index]]
    checked_bytes <- checked_bytes + file.info(fragment_path)$size
    reader <- arrow::ParquetFileReader$create(fragment_path)
    for (row_group_index in seq_len(reader$num_row_groups)) {
      batch <- data.table::as.data.table(
        reader$ReadRowGroup(row_group_index - 1L)$to_data_frame()
      )
      batch[, radius := radius]
      data.table::setcolorder(batch, contract$schema$names)

      source_positions <- match(batch[[contract$id]], ledger[[contract$id]])
      if (anyNA(source_positions)) {
        stop("Study-period dataset contains an unknown source transaction ID.", call. = FALSE)
      }
      for (source_field in contract$public_source_columns) {
        actual <- batch[[source_field]]
        expected <- ledger[[source_field]][source_positions]
        differs <- xor(is.na(actual), is.na(expected)) |
          (!is.na(actual) & !is.na(expected) & actual != expected)
        if (any(differs)) {
          stop(
            "Study-period source field ", source_field,
            " does not exactly match the source ledger.",
            call. = FALSE
          )
        }
      }
      if (any(batch$spatially_eligible !=
          ledger$spatially_eligible[source_positions])) {
        stop(
          "Study-period source-derived eligibility does not match the source ledger.",
          call. = FALSE
        )
      }
      if (any(batch$n_days_in_window != as.integer(n_days_in_window))) {
        stop("Study-period rows use an inconsistent day count.", call. = FALSE)
      }

      batch <- study_period_validate_and_cast_public(
        batch, contract, require_all_radii = FALSE
      )
      radius_positions <- match(batch$radius, radii)
      cells <- source_positions + (radius_positions - 1L) * n_sources
      occurrences[cells] <- occurrences[cells] + 1L
      checked_rows <- checked_rows + nrow(batch)
      rm(batch)
      checked_row_groups <- checked_row_groups + 1L
      if (checked_row_groups %% 100L == 0L) {
        gc(verbose = FALSE)
      }
    }
    log_validation(
      fragment_index = fragment_index,
      fragments = length(fragment_paths),
      rows = checked_rows,
      bytes = checked_bytes,
      elapsed_seconds = proc.time()[["elapsed"]] - started
    )
  }
  gc(verbose = FALSE)

  if (any(occurrences > 1L)) {
    stop("Study-period dataset contains duplicate public keys.", call. = FALSE)
  }
  if (any(occurrences == 0L)) {
    stop("Study-period dataset is missing source ID-radius public keys.", call. = FALSE)
  }
  expected_rows <- as.numeric(n_sources) * length(radii)
  if (!identical(as.numeric(checked_rows), expected_rows)) {
    stop("Study-period bounded scan did not conserve expected rows.", call. = FALSE)
  }
  list(
    path = dataset_path,
    rows = as.numeric(checked_rows),
    transactions = as.integer(n_sources),
    fragments = as.integer(length(fragment_paths)),
    bytes = as.numeric(checked_bytes),
    elapsed_seconds = proc.time()[["elapsed"]] - started
  )
}

study_period_validate_config <- function(config) {
  required <- c(
    "market", "source_path", "lookup_path", "crosswalk_path", "output_path",
    "start_date", "end_date", "radii"
  )
  missing_fields <- setdiff(required, names(config))
  if (length(missing_fields) > 0L) {
    stop(
      "Study-period configuration is missing field(s): ",
      paste(missing_fields, collapse = ", "), ".",
      call. = FALSE
    )
  }
  contract <- study_period_market_contract(config$market)
  window <- study_period_window(config$start_date, config$end_date)
  radii <- study_period_validate_radii(config$radii)
  for (path_field in c("source_path", "lookup_path", "crosswalk_path")) {
    path <- config[[path_field]]
    if (!is.character(path) || length(path) != 1L || is.na(path) ||
        !file.exists(path)) {
      stop(path_field, " does not name an existing input: ", path, call. = FALSE)
    }
  }
  if (!is.character(config$output_path) || length(config$output_path) != 1L ||
      is.na(config$output_path) || !nzchar(config$output_path)) {
    stop("output_path must be one nonempty path.", call. = FALSE)
  }
  ineligible_chunk_size <- config$ineligible_chunk_size
  if (is.null(ineligible_chunk_size)) ineligible_chunk_size <- 100000L
  output_batch_size <- config$output_batch_size
  if (is.null(output_batch_size)) output_batch_size <- 20L
  if (!is.numeric(output_batch_size) || length(output_batch_size) != 1L ||
      is.na(output_batch_size) || !is.finite(output_batch_size) ||
      output_batch_size < 1 || output_batch_size != floor(output_batch_size)) {
    stop("output_batch_size must be one positive integer.", call. = FALSE)
  }
  list(
    config = config,
    contract = contract,
    window = window,
    radii = radii,
    ineligible_chunk_size = ineligible_chunk_size,
    output_batch_size = as.integer(output_batch_size)
  )
}

study_period_read_parquet_columns <- function(path, columns, context) {
  dataset <- tryCatch(
    arrow::open_dataset(path),
    error = function(error) {
      stop(context, " could not be opened: ", conditionMessage(error), call. = FALSE)
    }
  )
  missing_columns <- setdiff(columns, dataset$schema$names)
  if (length(missing_columns) > 0L) {
    stop(
      context, " is missing required column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  dataset |>
    dplyr::select(dplyr::all_of(columns)) |>
    dplyr::collect() |>
    data.table::as.data.table()
}

study_period_create_stage <- function(output_path) {
  parent <- dirname(output_path)
  dir.create(parent, recursive = TRUE, showWarnings = FALSE)
  tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = parent
  )
}

build_study_period_cross_section <- function(config) {
  resolved <- study_period_validate_config(config)
  config <- resolved$config
  contract <- resolved$contract
  window <- resolved$window
  radii <- resolved$radii
  if (!exists("dataset_publication_check_state", mode = "function", inherits = TRUE) ||
      !exists("publish_validated_dataset", mode = "function", inherits = TRUE)) {
    stop(
      "dataset_publication_utils.R must be sourced before the study-period utility.",
      call. = FALSE
    )
  }
  dataset_publication_check_state(config$output_path)

  ledger <- study_period_source_ledger(
    study_period_read_parquet_columns(
      config$source_path,
      contract$source_columns,
      paste(tools::toTitleCase(contract$market), "source")
    ),
    contract
  )
  annual_returns <- study_period_read_parquet_columns(
    config$crosswalk_path,
    c("site_id", "year", "annual_status", "spill_count_ea", "spill_hrs_ea"),
    "Annual-return crosswalk"
  )
  site_totals <- collapse_study_period_annual_returns(annual_returns, window)

  stage_path <- study_period_create_stage(config$output_path)
  on.exit({
    if (dir.exists(stage_path)) {
      cleanup_status <- unlink(stage_path, recursive = TRUE)
      if (cleanup_status != 0L && dir.exists(stage_path)) {
        warning("Could not remove study-period stage: ", stage_path, call. = FALSE)
      }
    }
  }, add = TRUE)

  log_progress <- function(row_group_index, row_groups, transactions,
                           lookup_pairs, output_rows, elapsed_seconds) {
    logger::log_info(paste0(
      "Study-period row group {row_group_index}/{row_groups}: ",
      "transactions={transactions}, lookup_pairs={lookup_pairs}, ",
      "output_rows={output_rows}, elapsed_seconds={round(elapsed_seconds, 3)}, ",
      "stage={basename(stage_path)}"
    ))
  }
  buffered_writer <- study_period_buffered_writer(
    stage_path, resolved$output_batch_size
  )
  stream_result <- study_period_stream_lookup(
    lookup_path = config$lookup_path,
    ledger = ledger,
    site_totals = site_totals,
    contract = contract,
    radii = radii,
    n_days_in_window = window$n_days_in_window,
    ineligible_chunk_size = resolved$ineligible_chunk_size,
    write_fragment = buffered_writer$write,
    log_progress = log_progress
  )
  buffered_writer$flush()
  stream_result$logical_fragments <- stream_result$fragments
  stream_result$fragments <- buffered_writer$fragments()

  validator <- function(path) {
    result <- study_period_validate_dataset(
      path,
      ledger,
      contract,
      radii,
      window$n_days_in_window,
      log_validation = function(fragment_index, fragments, rows, bytes,
                                elapsed_seconds) {
        logger::log_info(paste0(
          "Study-period validation {fragment_index}/{fragments}: ",
          "rows={rows}, bytes={bytes}, ",
          "elapsed_seconds={round(elapsed_seconds, 3)}, path={path}"
        ))
      }
    )
    logger::log_info(paste0(
      "Validated study-period dataset: rows={result$rows}, ",
      "transactions={result$transactions}, fragments={result$fragments}, ",
      "bytes={result$bytes}, elapsed_seconds={round(result$elapsed_seconds, 3)}, ",
      "path={path}"
    ))
    invisible(result)
  }
  publish_validated_dataset(stage_path, config$output_path, validator)
  invisible(list(
    output_path = config$output_path,
    market = contract$market,
    years = window$years,
    n_days_in_window = window$n_days_in_window,
    source_transactions = nrow(ledger),
    stream = stream_result
  ))
}
