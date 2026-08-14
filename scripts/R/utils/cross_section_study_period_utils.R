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
