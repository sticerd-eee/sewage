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

# Load required packages if not already loaded
for(pkg in required_packages) {
  if(!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  if(!paste0("package:", pkg) %in% search()) {
    library(pkg, character.only = TRUE)
  }
}

# Functions
############################################################

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
    month_start = as.POSIXct(all_months,               tz = "UTC"),
    month_end   = as.POSIXct(all_months + months(1), tz = "UTC") - 1
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
  
  # 4. Return as a data.frame (or data.table)
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
#' @return List of data tables prepared for yearly and monthly aggregation
#'   \itemize{
#'     \item yearly: Data table with year boundary handling
#'     \item monthly: Data table with month splits and added month/quarter columns
#'   }
#' @export
prepare_spill_data <- function(data) {
  # Initial data prep and year boundary handling
  dt <- as.data.table(data) 
  ## Remove key NAs
  dt <- dt[!is.na(site_id) & !is.na(start_time)]
  ## Truncate start/end times that cross year boundaries
  dt[, c("lower", "upper") := {
    lr <- as.POSIXct(ISOdatetime(year, 1, 1, 0, 0, 0), tz = "UTC")
    ur <- as.POSIXct(ISOdatetime(year + 1, 1, 1, 0, 0, 0), tz = "UTC")
    list(lr, ur)
  }, by = year]
  
  dt[, `:=`(
    start_time = pmax(start_time, lower),
    end_time   = pmin(end_time,   upper)
  )]
  dt[, c("lower", "upper") := NULL]
  data.table::setkey(dt, site_id, start_time)
  
  # Prepare yearly dataset
  dt_yearly <- copy(dt)
  
  # Prepare monthly dataset from yearly data
  dt_monthly <- split_monthly_records(dt)
  dt_monthly[, month := data.table::month(start_time)]
  dt_monthly[, quarter := ceiling(month/3)] 
  base_year <- 2021
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
#' - First spill or spills with >0 hour gap: Creates 12-hour block (counts as 1)
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
    
    # Calculate difference between spill start and current block end
    if (!is.na(block_end)) {
      gap <- (current_start - block_end) / 3600
    }
    
    # 12-hour block (first spill or >24h gap)
    if (is.na(block_end) || gap > 0) {
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
      # 24 hour block
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
