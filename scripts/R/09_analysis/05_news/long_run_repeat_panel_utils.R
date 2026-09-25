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

#' Load one market's window rows, filtered inside Arrow
#'
#' Reads only the columns the panel needs and filters the transfer or rental
#' date to the market window inside Arrow before collecting. Timestamp columns
#' are compared against UTC day bounds, and the native date column is replaced
#' by a `date` column of class Date.
#'
#' @param spec Market specification with `id`, `date`, `date_type`, `price`,
#'   `start`, `end`, `change_cols` and `history`.
#' @return data.table with the market ID, `date`, the price column, `country`,
#'   `easting`, `northing`, `lsoa`, `msoa` and the change columns, one row per
#'   unique ID within the window.
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

#' Attach Repeat Group IDs to window rows
#'
#' Reads only the mapping rows for the IDs present in the window, checks that
#' the mapping is unique on the market ID, attaches `repeat_id` and drops the
#' market ID. Rows without a Repeat Group keep a missing `repeat_id`.
#'
#' @param history data.table returned by `load_window_history()`.
#' @param spec Market specification with `id` and `mapping`.
#' @return The input data.table with `repeat_id` in place of the market ID.
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

#' Count Repeat Groups with contradictory geography
#'
#' A Repeat Group contradicts itself when a geography field takes more than one
#' distinct nonmissing value across its rows. Coordinates are checked as a
#' pair. Missing-to-observed differences are not contradictions.
#'
#' @param dt data.table with `repeat_id`, `country`, `easting`, `northing`,
#'   `lsoa` and `msoa`.
#' @return Named numeric vector of contradicting Repeat Group counts for
#'   `country`, `coordinates`, `lsoa` and `msoa`.
geography_contradictions <- function(dt) {
  checks <- list(country = "country", coordinates = c("easting", "northing"),
                 lsoa = "lsoa", msoa = "msoa")
  vapply(checks, function(cols) {
    x <- dt[stats::complete.cases(dt[, cols, with = FALSE]), c("repeat_id", cols), with = FALSE]
    as.numeric(nrow(unique(x)[, .N, by = repeat_id][N > 1L]))
  }, numeric(1))
}

#' Find Repeat Groups observed on at least two distinct dates
#'
#' Implements repeat eligibility (AE1): several rows on the same date count as
#' one observation date.
#'
#' @param dt data.table with `repeat_id` and `date`.
#' @return Vector of eligible `repeat_id` values.
repeat_eligible_ids <- function(dt) {
  unique(dt[, .(repeat_id, date)])[, .N, by = repeat_id][N >= 2L, repeat_id]
}

#' Find Repeat Groups with observed characteristic changes
#'
#' Implements the change exclusion (AE2): a Repeat Group changes when any
#' change column takes more than one distinct nonmissing value across its rows.
#' Missing-to-observed differences are not changes.
#'
#' @param dt data.table with `repeat_id` and the change columns.
#' @param change_cols Character vector of characteristic columns to check.
#' @return Vector of `repeat_id` values with at least one changed
#'   characteristic.
changed_property_ids <- function(dt, change_cols) {
  unique(unlist(lapply(change_cols, function(col) {
    unique(dt[!is.na(get(col)), c("repeat_id", col), with = FALSE])[
      , .N, by = repeat_id][N > 1L, repeat_id]
  })))
}

#' Build the long-run repeat-property panel for one market
#'
#' Loads the window rows, attaches Repeat Groups, stops on contradictory
#' geography, keeps England rows with a positive finite price and complete
#' location, stops on coordinates absent from the lookup, and assigns the
#' near-far distance bands with `build_extensive_margin_sample()`.
#'
#' Repeat eligibility (AE1) is applied twice. The first pass runs after the
#' band assignment, which drops rows outside both bands, so changes are judged
#' only on Repeat Groups that are still repeats in the banded sample. The
#' change exclusion (AE2) then drops flagged Repeat Groups and the panel is
#' regrouped, and the second pass reapplies the two-distinct-dates rule to the
#' regrouped panel, as the report's `exclude_changes` variant does. Because the
#' exclusion drops whole Repeat Groups, the second pass removes no rows today;
#' it keeps the rule as an explicit property of the returned panel.
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

  dt <- dt[data.table::`%chin%`(repeat_id, repeat_eligible_ids(dt))]
  dt <- dt[!data.table::`%chin%`(repeat_id, changed_property_ids(dt, spec$change_cols))]
  dt[data.table::`%chin%`(repeat_id, repeat_eligible_ids(dt))]
}
