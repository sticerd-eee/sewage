# ==============================================================================
# Long-Run Repeat-Property Panel Utilities
# ==============================================================================
#
# Purpose: Build the long-run repeat-property panel for one market (sales or
#          rentals): window rows loaded through Arrow, Repeat Groups attached,
#          geography checked, England and price and location filters applied,
#          near-far distance bands assigned from the long-run nearest-site
#          lookup, and properties kept only when observed on at least two
#          distinct dates without observed characteristic changes.
#
# Author: Jacopo Olivieri
# Date: 2026-09-25
#
# ==============================================================================

if (!exists("as_transaction_date", mode = "function")) {
  source(here::here("scripts", "R", "utils", "repeat_transactions_utils.R"))
}
if (!exists("build_extensive_margin_sample", mode = "function")) {
  source(here::here("scripts", "R", "09_analysis", "05_news", "extensive_margin_news_utils.R"))
}

`%chin%` <- data.table::`%chin%`

#' Load one market's window rows, filtered inside Arrow
#'
#' @param spec Market specification with `id`, `date`, `date_type`, `price`,
#'   `start`, `end`, `change_cols` and `history`.
#' @return data.table with a `date` column; timestamp bounds are UTC days.
load_window_history <- function(spec) {
  cols <- unique(c(spec$id, spec$date, spec$price, "country", "easting", "northing",
                   "lsoa", "msoa", spec$change_cols))
  lower <- spec$start
  upper <- spec$end + 1L
  if (spec$date_type == "timestamp_utc") {
    lower <- as.POSIXct(format(lower), tz = "UTC")
    upper <- as.POSIXct(format(upper), tz = "UTC")
  }
  history <- arrow::open_dataset(spec$history) |>
    dplyr::select(dplyr::all_of(cols)) |>
    dplyr::filter(.data[[spec$date]] >= lower, .data[[spec$date]] < upper) |>
    dplyr::collect() |>
    as.data.frame()
  data.table::setDT(history)
  history[, date := as_transaction_date(get(spec$date))]
  if (spec$date != "date") history[, (spec$date) := NULL]
  if (anyNA(history$date) || min(history$date) < spec$start || max(history$date) > spec$end) {
    stop("Window rows fall outside ", spec$start, " to ", spec$end, ".", call. = FALSE)
  }
  if (anyDuplicated(history[[spec$id]]) > 0L) {
    stop("History IDs are not unique within the window.", call. = FALSE)
  }
  history
}

#' Attach Repeat Group IDs, reading only the mapping rows for the window IDs
attach_repeat_ids <- function(history, spec) {
  ids <- history[[spec$id]]
  mapping <- arrow::open_dataset(spec$mapping) |>
    dplyr::select(dplyr::all_of(c(spec$id, "repeat_id"))) |>
    dplyr::filter(.data[[spec$id]] %in% ids) |>
    dplyr::collect() |>
    as.data.frame()
  if (anyDuplicated(mapping[[spec$id]]) > 0L || anyNA(mapping$repeat_id)) {
    stop("Repeat mapping is not unique on ", spec$id, " or lacks repeat_id.", call. = FALSE)
  }
  history[, repeat_id := mapping$repeat_id[data.table::chmatch(ids, mapping[[spec$id]])]]
  history[, (spec$id) := NULL]
  history
}

#' Count Repeat Groups with more than one distinct nonmissing geography value
#'
#' Missing-to-observed differences are not contradictions.
geography_contradictions <- function(dt) {
  checks <- list(country = "country", coordinates = c("easting", "northing"),
                 lsoa = "lsoa", msoa = "msoa")
  vapply(checks, function(cols) {
    x <- dt[stats::complete.cases(dt[, cols, with = FALSE]), c("repeat_id", cols), with = FALSE]
    as.numeric(nrow(unique(x)[, .N, by = repeat_id][N > 1L]))
  }, numeric(1))
}

#' Repeat Groups observed on at least two distinct dates (AE1)
repeat_eligible_ids <- function(dt) {
  unique(dt[, .(repeat_id, date)])[, .N, by = repeat_id][N >= 2L, repeat_id]
}

#' Repeat Groups where a characteristic takes more than one nonmissing value (AE2)
changed_property_ids <- function(dt, change_cols) {
  unique(unlist(lapply(change_cols, function(col) {
    unique(dt[!is.na(get(col)), c("repeat_id", col), with = FALSE])[
      , .N, by = repeat_id][N > 1L, repeat_id]
  })))
}

#' Build the long-run repeat-property panel for one market
#'
#' @param spec Market specification (see `load_window_history()`), plus
#'   `mapping`.
#' @param lookup data.table keyed by integer `easting` and `northing` with
#'   `min_distance` to the nearest usable Site Group.
#' @param comparison Validated comparison from `validate_comparison_config()`.
#' @return data.table with `repeat_id`, `date`, `log_price`, `near_bin`,
#'   `min_distance`, `easting`, `northing`, `lsoa`, `msoa` and the change columns.
build_long_run_repeat_panel <- function(spec, lookup, comparison) {
  dt <- attach_repeat_ids(load_window_history(spec), spec)
  dt <- dt[!is.na(repeat_id)]

  contradictions <- geography_contradictions(dt)
  if (any(contradictions > 0)) {
    stop("Repeat Groups with contradictory geography: ",
         paste(names(contradictions), contradictions, sep = "=", collapse = ", "), call. = FALSE)
  }

  dt <- dt[!is.na(country) & country == "England"]
  dt[, log_price := log(as.numeric(get(spec$price)))]
  dt <- dt[is.finite(log_price) & get(spec$price) > 0]
  dt <- dt[!is.na(easting) & !is.na(northing) & !is.na(lsoa) & nzchar(lsoa) &
             !is.na(msoa) & nzchar(msoa)]
  dt[, c(spec$price, "country") := NULL]

  if (nrow(dt[!lookup, on = c("easting", "northing")]) > 0L) {
    stop("Coordinates absent from the long-run nearest-site lookup; ",
         "rerun site_nearest_long_run_match.R.", call. = FALSE)
  }
  data.table::setDF(dt)
  dt <- build_extensive_margin_sample(dt, lookup, c("easting", "northing"), comparison)
  data.table::setDT(dt)
  dt[, c("comparison_id", "comparison_label") := NULL]

  dt <- dt[repeat_id %chin% repeat_eligible_ids(dt)]
  dt <- dt[!repeat_id %chin% changed_property_ids(dt, spec$change_cols)]
  dt[repeat_id %chin% repeat_eligible_ids(dt)]
}
