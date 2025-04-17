############################################################
# Aggregate Spill Statistics
# Project: Sewage
# Date: 28/12/2024
# Author: Jacopo Olivieri
############################################################

#' This script individual sewage overflow spills into total spill counts and
#' durations based on data affecting the same catchment area.
#' It's part of the data preparation pipeline for analysing sewage discharge
#' events.

# Set Up
############################################################

# Initialise packages with version control with renv
#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "data.table"
  )

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here(
    "output", "log", "11_aggregate_spill_stats.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}
############################################################


# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed"),
  years = 2021:2023
)
############################################################

# Functions
############################################################

#' Load individual sewage spill data
#' @param file_name Name of the data file to load
#' @return Individual spill data with location
load_data <- function(file_name = "merged_edm_data_with_summary.RData") {
  file_path <- fs::path(CONFIG$processed_dir, file_name)
  logger::log_info("Loading data: {file_path}")

  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }

  tryCatch(
    {
      df <- rio::import(file_path, trust = TRUE)
      if (is.list(df)) df <- df[[1]]
      logger::log_info("Loaded data")
      return(df)
    },
    error = function(e) {
      error_msg <- glue::glue("Failed to load data: {e$message}")
      logger::log_error(error_msg)
      stop(error_msg)
    }
  )
}


#' Split records that cross month boundaries into separate monthly records
#' @param df Data frame containing spill records
#' @return Data frame with split records for cross-month spills
split_monthly_records <- function(df) {
  # Convert to data.table once at the start
  dt <- data.table::copy(as.data.table(df))
  dt <- dt[end_time > start_time]

  # Pre-calculate floor dates for all records at once
  dt[, `:=`(
    floor_start = floor_date(start_time, "month"),
    floor_end = floor_date(end_time, "month")
  )][, needs_split := floor_start != floor_end]

  # Split the data.table based on the needs_split column
  result_list <- list(
    # Single-month records
    dt[needs_split == FALSE],

    # Multi-month records that need splitting
    dt[needs_split == TRUE,
      {
        months_seq <- seq(
          as.Date(floor_start),
          as.Date(floor_end),
          by = "month"
        )

        data.table(
          start_time = c(
            start_time,
            as.POSIXct(months_seq[-1], tz = "UTC")
          ),
          end_time = c(
            as.POSIXct(months_seq[-1], tz = "UTC") - 1,
            end_time
          )
        )
      },
      by = setdiff(names(dt), c(
        "start_time", "end_time", "key_join",
        "floor_start", "floor_end", "needs_split"
      ))
    ]
  )

  # Combine results and clean up temporary columns
  result <- rbindlist(result_list, fill = TRUE)
  result[, c("floor_start", "floor_end", "needs_split") := NULL]

  return(result)
}


#' Prepare spill data for aggregation by handling year/month boundaries
#' @param data Input dataframe containing spill data
#' @return List of data tables prepared for yearly and monthly aggregation
prepare_spill_data <- function(data) {
  # Initial data prep and year boundary handling
  dt <- as.data.table(data)
  ## Remove key NAs
  dt <- dt[!is.na(outlet_discharge_ngr) & !is.na(start_time)]
  ## Truncate start/end times that cross year boundaries
  dt[, `:=`(
    start_time = pmax(
      start_time,
      as.POSIXct(paste0(year, "-01-01 00:00:00"), tz = "UTC")
    ),
    end_time = pmin(
      end_time,
      as.POSIXct(paste0(year + 1, "-01-01 00:00:00"), tz = "UTC")
    )
  )]
  ## Sort data
  data.table::setkey(dt, water_company, start_time)

  # Prepare yearly dataset
  dt_yearly <- copy(dt)

  # Prepare monthly dataset from yearly data
  dt_monthly <- split_monthly_records(dt)
  dt_monthly[, month := month(start_time)]

  return(list(
    yearly = dt_yearly,
    monthly = dt_monthly
  ))
}


#' Count spills using the 12/24 counting method
#' @param start_times Vector of spill start times (POSIXct)
#' @param end_times Vector of spill end times (POSIXct)
#' @return Integer count of spill events
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


#' Aggregate spill data by water company and year/month
#' @param data Input dataframe containing individual spill data
#' @return dataframe with aggregated spill statistics
aggregate_spills <- function(data) {
  # Load data
  prepared_data <- prepare_spill_data(data)
  dt_yearly <- prepared_data$yearly
  dt_monthly <- prepared_data$monthly

  # Create yearly combinations
  yearly_combinations <- expand_grid(
    water_company = unique(dt_yearly$water_company),
    year = 2021:2023
  )

  # Create monthly combinations
  monthly_combinations <- expand_grid(
    water_company = unique(dt_monthly$water_company),
    year = 2021:2023,
    month = 1:12
  )

  # Yearly aggregation
  yearly_result <- map2(
    yearly_combinations$water_company,
    yearly_combinations$year,
    ~ {
      # Get data for current company-year
      current_data <- dt_yearly[water_company == .x & year == .y]

      if (nrow(current_data) == 0) {
        return(data.table(
          water_company = .x,
          year = .y,
          spill_count_yr = 0L,
          spill_hrs_yr = 0L,
          spill_count_yr_ea = 0L,
          spill_hrs_yr_ea = 0L
        ))
      }

      # Get unique keys for current subset
      keys_year <- unique(c(
        unlist(current_data$key_join),
        "outlet_discharge_ngr", "site_id"
      ))

      # Group by keys and calculate statistics
      current_data[, .(
        spill_count_yr = count_spills(start_time, end_time),
        spill_hrs_yr = sum(as.numeric(difftime(end_time, start_time, units = "hours")), na.rm = TRUE),
        spill_count_yr_ea = mean(sum_spill_count_yr, na.rm = TRUE),
        spill_hrs_yr_ea = mean(sum_spill_duration_hrs_yr, na.rm = TRUE)
      ), by = keys_year]
    }
  )

  # Monthly aggregation
  monthly_result <- pmap(
    list(
      monthly_combinations$water_company,
      monthly_combinations$year,
      monthly_combinations$month
    ),
    ~ {
      # Get data for current company-year-month
      current_data <- dt_monthly[
        water_company == ..1 &
          year == ..2 &
          month == ..3
      ]

      if (nrow(current_data) == 0) {
        return(data.table(
          water_company = ..1,
          year = ..2,
          month = ..3,
          spill_count_mo = 0L,
          spill_hrs_mo = 0L
        ))
      }

      # Get unique keys for current subset
      keys_month <- unique(c(
        unlist(current_data$key_join),
        "outlet_discharge_ngr", "site_id",
        "month"
      ))

      # Group by keys and calculate statistics
      current_data[, .(
        spill_count_mo = count_spills(start_time, end_time),
        spill_hrs_mo = sum(as.numeric(difftime(end_time, start_time, units = "hours")), na.rm = TRUE)
      ), by = keys_month]
    }
  )

  return(list(
    yearly = bind_rows(yearly_result),
    monthly = bind_rows(monthly_result)
  ))
}

#' Complete data observations for time series consistency
#' @param data List containing yearly and monthly spill data
#' @return List with completed yearly and monthly data
complete_data_observations <- function(data) {
  # Define site metadata columns to be filled
  site_metadata_cols <- c(
    "site_name_ea", "site_name_wa_sc", "permit_reference_ea",
    "activity_reference", "permit_reference_wa_sc", "unique_id"
  )

  # Get non-missing site_id
  site_ids_by_year <- data$yearly %>%
    split(.$year) %>%
    lapply(function(df) unique(pull(df, site_id)))

  # Complete yearly data
  logger::log_info("Completing yearly data observations")
  completed_yearly <- data$yearly %>%
    group_by(water_company, outlet_discharge_ngr, site_id) %>%
    complete(year = CONFIG$years) %>%
    # Fill site metadata from other rows in the same group
    group_by(water_company, outlet_discharge_ngr, site_id) %>%
    fill(all_of(intersect(site_metadata_cols, names(data$yearly))), .direction = "downup") %>%
    # Fill missing numeric columns with zeros
    mutate(across(
      c(spill_count_yr, spill_hrs_yr, spill_count_yr_ea, spill_hrs_yr_ea),
      ~ replace_na(., 0)
    )) %>%
    ungroup() %>%
    # Set NA for years with no observations in EA data
    mutate(
      across(
        c(spill_count_yr, spill_hrs_yr, spill_count_yr_ea, spill_hrs_yr_ea),
        ~ case_when(
          year == 2021 & !(site_id %in% site_ids_by_year[["2021"]]) ~ NA_real_,
          year == 2022 & !(site_id %in% site_ids_by_year[["2022"]]) ~ NA_real_,
          year == 2023 & !(site_id %in% site_ids_by_year[["2023"]]) ~ NA_real_,
          TRUE ~ .
        )
      )
    )

  # Complete monthly data
  logger::log_info("Completing monthly data observations")
  completed_monthly <- data$monthly %>%
    group_by(water_company, outlet_discharge_ngr, site_id) %>%
    complete(year = CONFIG$years, month = 1:12) %>%
    # Fill site metadata from other rows in the same group
    group_by(water_company, outlet_discharge_ngr, site_id) %>%
    fill(all_of(intersect(site_metadata_cols, names(data$monthly))), .direction = "downup") %>%
    # Fill missing numeric columns with zeros
    mutate(across(
      c(spill_count_mo, spill_hrs_mo),
      ~ replace_na(., 0)
    )) %>%
    ungroup() %>%
    # Set NA for years with no observations in EA data
    mutate(
      across(
        c(spill_count_mo, spill_hrs_mo),
        ~ case_when(
          year == 2021 & !(site_id %in% site_ids_by_year[["2021"]]) ~ NA_real_,
          year == 2022 & !(site_id %in% site_ids_by_year[["2022"]]) ~ NA_real_,
          year == 2023 & !(site_id %in% site_ids_by_year[["2023"]]) ~ NA_real_,
          TRUE ~ .
        )
      )
    )

  return(list(
    yearly = completed_yearly,
    monthly = completed_monthly
  ))
}

#' Export processed data
#' @param data Combined data frame to export
#' @return NULL
export_data <- function(data) {
  tryCatch(
    {
      # File path
      rdata_file <- file.path(CONFIG$processed_dir, "individual_edm_location.RData")
      csv_file_year <- file.path(
        CONFIG$processed_dir,
        "individual_edm_location_year.csv"
      )
      csv_file_month <- file.path(
        CONFIG$processed_dir,
        "individual_edm_location_month.csv"
      )
      dir.create(dirname(rdata_file), recursive = TRUE, showWarnings = FALSE)

      # Save data
      save(data, file = rdata_file)
      logger::log_info("Data exported to {rdata_file}")
      write_csv(data$yearly, csv_file_year)
      logger::log_info("CSV backup saved to {csv_file_year}")
      write_csv(data$monthly, csv_file_month)
      logger::log_info("CSV backup saved to {csv_file_month}")
    },
    error = function(e) {
      logger::log_error("Error exporting data: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

############################################################


# Main execution
############################################################

main <- function() {
  # Setup
  initialise_environment()
  setup_logging()

  tryCatch(
    {
      # Load and process data
      logger::log_info("Starting data processing pipeline")
      data <- load_data()

      # Aggregate spills
      logger::log_info("Aggregating spill statistics")
      aggregated_data <- aggregate_spills(data)

      # Complete time series observations
      logger::log_info("Completing time series observations")
      completed_data <- complete_data_observations(aggregated_data)

      # Export results
      logger::log_info("Exporting results")
      export_data(completed_data)

      logger::log_info("Processing completed successfully")
    },
    error = function(e) {
      logger::log_error("Pipeline failed: {e$message}")
      stop(glue::glue("Pipeline failed: {e$message}"))
    }
  )
}

# Execute main function
if (sys.nframe() == 0) {
  main()
}
