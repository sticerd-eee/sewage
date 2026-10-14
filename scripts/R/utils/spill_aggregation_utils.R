############################################################
# Shared Utilities for Spill Aggregation
# Project: Sewage
# Date: 28/08/2025
# Author: Jacopo Olivieri
############################################################

#' Shared functions for aggregating sewage spill data across multiple scripts.
#' Contains common functionality for temporal boundary handling, spill counting,
#' and data preparation used by both general and dry spill aggregation scripts.

# Dependencies
############################################################

# Required packages (these should be loaded by calling scripts)
required_packages <- c(
  "data.table", "lubridate", "dplyr", "tidyr"
)

# Fail before attaching anything when the project environment is incomplete
missing_packages <- required_packages[!vapply(
  required_packages,
  requireNamespace,
  quietly = TRUE,
  FUN.VALUE = logical(1)
)]
if (length(missing_packages) > 0L) {
  stop(
    "Missing required packages: ",
    paste(missing_packages, collapse = ", "),
    ". Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

# Preserve the utility's established package-attachment behavior
for (pkg in required_packages) {
  if (!paste0("package:", pkg) %in% search()) {
    library(pkg, character.only = TRUE)
  }
}

# Functions
############################################################

#' Clamp spill records to their labelled calendar year
#'
#' Uses explicit UTC year boundaries so results do not depend on the machine
#' timezone.
#'
#' @param df Data frame containing year, start_time, and end_time
#' @return data.table with start_time and end_time clamped to the labelled year
#' @export
clamp_spill_records_to_year <- function(df) {
  dt <- as.data.table(df)

  dt[, c("year_start", "year_end") := {
    list(
      ISOdatetime(year, 1, 1, 0, 0, 0, tz = "UTC"),
      ISOdatetime(year + 1, 1, 1, 0, 0, 0, tz = "UTC")
    )
  }, by = year]

  dt[, `:=`(
    start_time = pmax(start_time, year_start),
    end_time = pmin(end_time, year_end)
  )]
  dt[, c("year_start", "year_end") := NULL]

  dt[]
}

#' Split records that cross month boundaries into separate monthly records
#'
#' This function handles spills that span multiple months by creating separate
#' records for each month portion, with start/end times clamped to month boundaries.
#'
#' @param df Data frame containing spill records with start_time and end_time
#' @return Data frame with split records for cross-month spills
#' @export
split_monthly_records <- function(df) {
  dt <- as.data.table(df)[end_time > start_time]
  
  # 1. Build a "calendar" of all month‐windows covering your data
  all_months <- seq(
    floor_date(min(dt$start_time), "month"),
    floor_date(max(dt$end_time),   "month"),
    by = "month"
  )
  cal <- data.table(
    month_start = as.POSIXct(all_months, tz = "UTC"),
    month_end = as.POSIXct(all_months + months(1), tz = "UTC")
  )
  
  # 2. Key both tables for an interval‐overlap join
  setkey(dt,    start_time, end_time)
  setkey(cal, month_start, month_end)
  
  # 3. Join, then "clamp" each record to its month‐slice
  out <- foverlaps(dt, cal, nomatch = 0L)[
    , .(
      start_time = pmax(start_time, month_start),
      end_time   = pmin(end_time,   month_end),
      # carry through every other column from dt:
      .SD[, setdiff(names(dt), c("start_time","end_time")), with = FALSE]
    )
  ]

  # Closed interval joins also match records that only touch a boundary.
  out <- out[end_time > start_time]

  return(out[])
}

#' Split records that cross day boundaries into separate daily records
#'
#' This function handles spills that span multiple days by creating separate
#' records for each day portion, with start/end times clamped to day boundaries.
#' Modelled on split_monthly_records().
#'
#' @param df Data frame containing spill records with start_time and end_time
#' @return data.table with split records for cross-day spills, plus a date column
#' @export
split_daily_records <- function(df) {
  dt <- as.data.table(df)[end_time > start_time]

  if (nrow(dt) == 0) {
    return(data.table(start_time = as.POSIXct(character()),
                      end_time = as.POSIXct(character()),
                      date = as.Date(character())))
  }

  # 1. Build a "calendar" of all day-windows covering the data
  all_days <- seq(
    as.Date(min(dt$start_time)),
    as.Date(max(dt$end_time)),
    by = "day"
  )
  cal <- data.table(
    day_start = as.POSIXct(all_days, tz = "UTC"),
    day_end   = as.POSIXct(all_days + 1L, tz = "UTC") - 1
  )

  # 2. Key both tables for an interval-overlap join
  setkey(dt,  start_time, end_time)
  setkey(cal, day_start,  day_end)

  # 3. Join then clamp each record to its day-slice
  out <- foverlaps(dt, cal, nomatch = 0L)
  out[, `:=`(
    start_time = pmax(start_time, day_start),
    end_time   = pmin(end_time,   day_end),
    date       = as.Date(day_start)
  )]
  out[, c("day_start", "day_end") := NULL]

  # 5. Drop zero-duration slivers
  out <- out[end_time > start_time]

  return(out[])
}

#' Prepare spill data for aggregation by handling year/month boundaries
#'
#' Prepares spill data for temporal aggregation by truncating spills that cross
#' year boundaries and creating separate datasets for yearly and monthly analysis.
#'
#' @param data Input dataframe containing spill data with start_time, end_time, year
#' @param base_year Integer base year used to construct sequential month and quarter IDs
#' @return List of data tables prepared for yearly and monthly aggregation
#'   \itemize{
#'     \item yearly: Data table with year boundary handling
#'     \item monthly: Data table with month splits and added month/quarter columns
#'   }
#' @export
prepare_spill_data <- function(data, base_year) {
  # Initial data prep and year boundary handling
  dt <- as.data.table(data) 
  ## Remove key NAs
  dt <- dt[!is.na(site_id) & !is.na(start_time)]
  dt <- clamp_spill_records_to_year(dt)
  data.table::setkey(dt, site_id, start_time)
  
  # Prepare yearly dataset
  dt_yearly <- copy(dt)
  
  # Prepare monthly dataset from yearly data
  dt_monthly <- split_monthly_records(dt)
  dt_monthly[, month := data.table::month(start_time)]
  dt_monthly[, quarter := ceiling(month/3)] 
  dt_monthly[, month_id := (year - base_year) * 12 + month]
  dt_monthly[, qtr_id := (year - base_year) * 4 + quarter]
  
  return(list(
    yearly = dt_yearly,
    monthly = dt_monthly
  ))
}

#' Count spills using the 12/24 counting method
#'
#' This function implements the Environment Agency's 12/24 hour counting methodology
#' for aggregating overlapping or consecutive spill events:
#' - First spill or a spill at/after the active block end: Creates a 12-hour block
#' - Subsequent spills within blocks: Creates 24-hour blocks
#' - Long spills: Generate additional 24-hour blocks if duration exceeds block size
#'
#' @param start_times Vector of spill start times (POSIXct)
#' @param end_times Vector of spill end times (POSIXct)
#' @return Integer count of spill events using 12/24 hour methodology
#' @export
count_spills <- function(start_times, end_times) {
  if (!length(start_times)) {
    return(0L)
  }
  
  # POSIXct to numeric (seconds since epoch) to optimise speed
  start_times <- as.numeric(start_times)
  end_times <- as.numeric(end_times)
  
  # Pre-sort inputs by start_time
  ord <- order(start_times)
  start_times <- start_times[ord]
  end_times <- end_times[ord]
  
  # Pre-compute durations in seconds
  dur12 <- 12 * 3600
  dur24 <- 24 * 3600
  
  # Initialise variables
  spill_count <- 0L
  block_start <- NA_real_
  block_end <- NA_real_
  
  for (i in seq_along(start_times)) {
    current_start <- start_times[i]
    current_end <- end_times[i]
    
    # Block endpoints are excluded: a spill beginning at or after the current
    # endpoint starts a new 12-hour counting block.
    if (is.na(block_end) || current_start >= block_end) {
      block_start <- current_start
      block_end <- current_start + dur12
      
      # How many 24-hour blocks occur after the first 12 hours (if any)
      diff_current <- (current_end - block_start) / 3600
      spill_over_12h <- ceiling(pmax(0, diff_current - 12) / 24)
      spill_count <- spill_count + 1L + spill_over_12h
      
      # Update block times
      block_start <- block_end + (dur24 * spill_over_12h)
      block_end <- block_start + dur24
    } else {
      # A spill inside the active sequence advances through 24-hour blocks.
      # Update spill count
      diff_current <- (current_end - block_start) / 3600
      spill_over_24h <- ceiling(pmax(0, diff_current) / 24)
      spill_count <- spill_count + spill_over_24h
      
      # Update block times
      block_start <- block_end + dur24 * (spill_over_24h - 1)
      block_end <- block_start + dur24
    }
  }
  
  return(spill_count)
}

#' Calculate total spill hours from start and end times
#'
#' Helper function to calculate total hours of spilling, handling NA values appropriately.
#'
#' @param start_times Vector of spill start times (POSIXct)
#' @param end_times Vector of spill end times (POSIXct)
#' @return Numeric total hours of spilling
#' @export
calculate_spill_hours <- function(start_times, end_times) {
  sum(
    as.numeric(difftime(end_times, start_times, units = "hours")), 
    na.rm = TRUE
  )
}

# Shared Measurement Core: spill arithmetic
############################################################

# data.table may replace grouped sum/cumsum calls with optimizer-specific
# implementations. These wrappers, combined with a fixed row order at every
# aggregation boundary, keep published floating values reproducible. Both
# exposure engines route every sum and cumulative sum through them (R12).

#' Sum a numeric vector without data.table's grouped optimiser
#'
#' @param value Numeric vector to sum.
#' @param na.rm Whether to drop missing values.
#' @return The sum, computed by `base::sum()`.
#' @export
spill_stable_sum <- function(value, na.rm = FALSE) {
  base::sum(value, na.rm = na.rm)
}

#' Accumulate a numeric vector without data.table's grouped optimiser
#'
#' @param value Numeric vector to accumulate.
#' @return The cumulative sum, computed by `base::cumsum()`.
#' @export
spill_stable_cumsum <- function(value) {
  base::cumsum(value)
}

#' Clip spill events to an exposure window
#'
#' The single implementation of the overlap filter, the clamping, and the
#' positive-duration filter that both exposure engines apply. `window_start`
#' and `window_end` accept either one scalar bound shared by every row or a
#' per-row vector: the study engine passes its two window constants, and the
#' prior engine passes each transaction's own endpoint.
#'
#' An event enters the window when it starts strictly before `window_end` and
#' has not already ended before `window_start`. Surviving events are clamped to
#' the window and those left with no positive duration are dropped, so a caller
#' never has to remember the `event_hours > 0` filter separately.
#'
#' Carries no validation, per R14: correctness lives in the unit fixtures and
#' the publication gate.
#'
#' @param events Event table with POSIXct `start_time` and `end_time`.
#' @param window_start Window opening, scalar or one value per event row.
#' @param window_end Window closing, scalar or one value per event row.
#' @return A new data.table of the surviving events, carrying every input
#'   column plus `clipped_start`, `clipped_end`, and `event_hours`. The
#'   caller's table is never modified.
#' @export
clip_events_to_window <- function(events, window_start, window_end) {
  dt <- as.data.table(events)
  window_start <- rep(window_start, length.out = nrow(dt))
  window_end <- rep(window_end, length.out = nrow(dt))

  # Subset the bounds alongside the rows so a per-row window stays aligned
  # with its own event once the overlap filter has dropped rows.
  overlaps <- dt$start_time < window_end & dt$end_time >= window_start
  dt <- dt[overlaps]
  window_start <- window_start[overlaps]
  window_end <- window_end[overlaps]

  dt[, `:=`(
    clipped_start = pmax(start_time, window_start),
    clipped_end = pmin(end_time, window_end)
  )]
  dt[, event_hours := as.numeric(difftime(
    clipped_end, clipped_start, units = "hours"
  ))]
  dt[event_hours > 0]
}

#' Collapse clipped events to spill totals per group
#'
#' The single per-group reduction behind both engines: the prior engine groups
#' by transaction and site, the study engine by site alone. Rows are ordered
#' explicitly before the reduction so the floating sum is reproducible (R12),
#' and `count_spills()` stays the only 12/24 implementation.
#'
#' Carries no validation, per R14.
#'
#' @param clipped_events Output of `clip_events_to_window()`.
#' @param by Character vector of grouping key columns.
#' @param order_by Character vector of tie-breaking sort columns applied after
#'   the grouping keys. Defaults to the clipped start alone; the prior engine
#'   adds the clipped end and both unclipped endpoints so that events sharing a
#'   start still sort deterministically. Changing this changes which order the
#'   floating sum accumulates in, so each engine passes its own established key.
#' @param na.rm Whether to drop missing event hours from the sum. Defaults to
#'   `FALSE`, which lets an unexpected NA reach the caller's own guard rather
#'   than being silently absorbed; the prior engine passes `TRUE`.
#' @return A data.table with one row per group carrying `spill_hrs` and
#'   `spill_count`.
#' @export
collapse_events_by_group <- function(clipped_events, by,
                                     order_by = "clipped_start",
                                     na.rm = FALSE) {
  events <- as.data.table(clipped_events)
  setorderv(events, c(by, order_by))
  events[, .(
    spill_hrs = spill_stable_sum(event_hours, na.rm = na.rm),
    spill_count = as.numeric(count_spills(clipped_start, clipped_end))
  ), by = by]
}

#' Convert a window total into daily and weekly averages
#'
#' The single owner of both rate formulas. The weekly average is the daily
#' average times seven rather than an independent expression, so the two can
#' never drift apart.
#'
#' Carries no validation, per R14.
#'
#' @param total Numeric window total, such as spill hours or spill counts.
#' @param n_days_in_window Length of the window in days, scalar or per-row.
#' @return A list with numeric `daily_avg` and `weekly_avg`.
#' @export
spill_window_rates <- function(total, n_days_in_window) {
  daily_avg <- total / n_days_in_window
  list(daily_avg = daily_avg, weekly_avg = daily_avg * 7)
}

#' Return standard rainfall offset column names
#'
#' These offsets are shared across daily-panel and spill-level rainfall matching.
#'
#' @return Character vector of standard rainfall offset column names
#' @export
get_standard_rainfall_offset_cols <- function() {
  c("date_0", "date_minus1", "date_minus2", "date_minus3")
}

#' Add standard rainfall offset columns relative to a base date column
#'
#' @param data Data frame or data.table containing a base date column
#' @param date_col Character scalar naming the base date column
#' @return data.table with date_0, date_minus1, date_minus2, and date_minus3 added
#' @export
add_standard_rainfall_offsets <- function(data, date_col) {
  dt <- as.data.table(data)

  if (!date_col %in% names(dt)) {
    stop(sprintf("Column '%s' not found in data.", date_col))
  }

  base_dates <- as.Date(dt[[date_col]])
  dt[, `:=`(
    date_0 = base_dates,
    date_minus1 = base_dates - 1L,
    date_minus2 = base_dates - 2L,
    date_minus3 = base_dates - 3L
  )]

  dt[]
}

#' Return the shared rainfall indicator column names
#'
#' @return Character vector of rainfall indicator column names
#' @export
get_standard_rainfall_indicator_cols <- function() {
  c(
    "rainfall_1cell_d01_na_rm",
    "rainfall_1cell_d01_strict",
    "rainfall_max_9cell_d01_na_rm",
    "rainfall_max_9cell_d01_strict",
    "rainfall_max_9cell_d0123_na_rm",
    "rainfall_max_9cell_d0123_strict"
  )
}

#' Return rainfall indicator columns used in the daily site-day panel
#'
#' Keeps the same-day 9-cell measure separate from the dry-spill indicator set,
#' so spill-level dry-spill outputs are not widened unintentionally.
#'
#' @return Character vector of daily-panel rainfall indicator column names
#' @export
get_daily_panel_rainfall_indicator_cols <- function() {
  c(
    "rainfall_max_9cell_d0_na_rm",
    get_standard_rainfall_indicator_cols()
  )
}

#' Calculate the shared rainfall indicators from matched long-form rainfall data
#'
#' The input must already be matched to rainfall observations and include:
#' - `time_offset`: one of date_0, date_minus1, date_minus2, date_minus3
#' - `rainfall`: rainfall value
#' - `is_center`: TRUE for the centre cell, FALSE for neighbouring cells
#'
#' @param matched_data data.table with matched rainfall observations
#' @param by_cols Character vector of grouping columns
#' @param include_same_day_max_9cell_na_rm Logical; when TRUE, also returns
#'   `rainfall_max_9cell_d0_na_rm` for same-day daily-panel analysis.
#' @return data.table with the shared rainfall indicators
#' @export
calculate_standard_rainfall_indicators <- function(
    matched_data,
    by_cols,
    include_same_day_max_9cell_na_rm = FALSE) {
  dt <- as.data.table(matched_data)
  required_cols <- c(by_cols, "time_offset", "rainfall", "is_center")
  missing_cols <- setdiff(required_cols, names(dt))

  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "Missing required rainfall columns: %s",
        paste(missing_cols, collapse = ", ")
      )
    )
  }

  if (nrow(dt) == 0L) {
    empty <- dt[0, ..by_cols]
    indicator_cols <- if (isTRUE(include_same_day_max_9cell_na_rm)) {
      get_daily_panel_rainfall_indicator_cols()
    } else {
      get_standard_rainfall_indicator_cols()
    }
    for (col in indicator_cols) {
      empty[, (col) := numeric()]
    }
    return(empty[])
  }

  calc_max_both <- function(v) {
    if (length(v) == 0L) {
      return(list(na_rm = NA_real_, strict = NA_real_))
    }

    v_na_rm <- suppressWarnings(max(v, na.rm = TRUE))
    if (is.infinite(v_na_rm)) v_na_rm <- NA_real_

    v_strict <- if (any(is.na(v))) NA_real_ else suppressWarnings(max(v, na.rm = FALSE))
    if (is.infinite(v_strict)) v_strict <- NA_real_

    list(na_rm = v_na_rm, strict = v_strict)
  }

  dt[, {
    max_d0_both <- calc_max_both(
      rainfall[time_offset == "date_0"]
    )
    closest_d01_both <- calc_max_both(
      rainfall[is_center == TRUE & time_offset %in% c("date_0", "date_minus1")]
    )
    max_d01_both <- calc_max_both(
      rainfall[time_offset %in% c("date_0", "date_minus1")]
    )
    max_d0123_both <- calc_max_both(
      rainfall[time_offset %in% c("date_0", "date_minus1", "date_minus2", "date_minus3")]
    )

    out <- list(
      rainfall_1cell_d01_na_rm = closest_d01_both$na_rm,
      rainfall_1cell_d01_strict = closest_d01_both$strict,
      rainfall_max_9cell_d01_na_rm = max_d01_both$na_rm,
      rainfall_max_9cell_d01_strict = max_d01_both$strict,
      rainfall_max_9cell_d0123_na_rm = max_d0123_both$na_rm,
      rainfall_max_9cell_d0123_strict = max_d0123_both$strict
    )
    if (isTRUE(include_same_day_max_9cell_na_rm)) {
      out <- c(
        list(rainfall_max_9cell_d0_na_rm = max_d0_both$na_rm),
        out
      )
    }
    out
  }, by = by_cols]
}

#' Validate spill data for aggregation
#'
#' Performs basic validation checks on spill data before aggregation.
#'
#' @param data Data frame with spill data
#' @param required_cols Vector of required column names
#' @return Logical indicating if data passes validation
#' @export
validate_spill_data <- function(data, required_cols = c("site_id", "start_time", "end_time", "water_company", "year")) {
  if (nrow(data) == 0) {
    warning("Empty dataset provided")
    return(FALSE)
  }
  
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    warning("Missing required columns: ", paste(missing_cols, collapse = ", "))
    return(FALSE)
  }
  
  # Check for basic data quality issues
  if (any(is.na(data$site_id))) {
    warning("NA values found in site_id column")
  }
  
  if (any(is.na(data$start_time))) {
    warning("NA values found in start_time column")
  }
  
  return(TRUE)
}
