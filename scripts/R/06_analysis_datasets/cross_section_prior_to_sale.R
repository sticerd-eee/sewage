############################################################
# Create Prior-to-Sale Cross-sectional Database
# Project: Sewage
# Date: 18/12/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates a cross-sectional database that aggregates
#' sewage spill data from January 1, 2021 to the day before each house sale.
#' It calculates daily average spill count and spill hours.

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  required_packages <- c(
    "here", "logger", "glue", "fs",
    "lubridate", "arrow", "data.table", "dplyr"
  )

  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))

  # Source shared utilities for count_spills function
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "cross_section_prior_to_sale.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed"),
  radius_thresholds = c(250, 500, 1000),
  base_year = 2021,
  window_start = as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
  chunk_size = 100000  # Number of houses to process per batch
)

# Data Loading Functions
############################################################

#' Load datasets from parquet files
#' @return List containing house_dt, spill_lookup_dt, raw_events_dt
load_data <- function() {
  logger::log_info("Loading datasets from parquet files")

  # Load house price data
  house_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "house_price.parquet")
  ) |>
    dplyr::select(house_id, price, date_of_transfer) |>
    dplyr::filter(date_of_transfer >= CONFIG$window_start) |>
    dplyr::collect() |>
    as.data.table()
  setkey(house_dt, house_id)
  logger::log_info("House price data loaded: {nrow(house_dt)} rows")

 # Load spill lookup - filter to max radius threshold to reduce join size
  max_radius <- max(CONFIG$radius_thresholds)
  spill_lookup_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "spill_house_lookup.parquet")
  ) |>
    dplyr::select(house_id, site_id, distance_m) |>
    dplyr::filter(distance_m <= max_radius) |>
    dplyr::collect() |>
    as.data.table()
  setkey(spill_lookup_dt, house_id)
  logger::log_info("Spill lookup data loaded: {nrow(spill_lookup_dt)} rows (filtered to {max_radius}m)")

  # Load raw spill events
  raw_events_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "matched_events_annual_data",
              "matched_events_annual_data.parquet")
  ) |>
    dplyr::select(site_id, start_time, end_time, year) |>
    dplyr::filter(year >= CONFIG$base_year) |>
    dplyr::collect() |>
    as.data.table()
  raw_events_dt[, year := NULL]
  setkey(raw_events_dt, site_id)
  logger::log_info("Raw spill events loaded: {nrow(raw_events_dt)} rows")

  list(
    house_dt = house_dt,
    spill_lookup_dt = spill_lookup_dt,
    raw_events_dt = raw_events_dt
  )
}

# Processing Functions
############################################################

#' Create joined events table for a subset of houses
#' @param house_ids Vector of house_ids to process
#' @param data List containing loaded data tables
#' @return list with `events_dt` (joined data) and `lookup_chunk`
create_joined_events <- function(house_ids, data) {
  # Subset to this chunk of houses
  house_chunk <- data$house_dt[.(house_ids), nomatch = 0L]
  lookup_chunk <- data$spill_lookup_dt[
    data.table(house_id = house_ids),
    on = "house_id",
    nomatch = 0L
  ]
  lookup_chunk <- lookup_chunk[, .(house_id, site_id, distance_m)]

  if (nrow(lookup_chunk) == 0) {
    return(list(events_dt = NULL, lookup_chunk = lookup_chunk))
  }

  # Join: house -> spill_lookup
  house_sites <- lookup_chunk[house_chunk, nomatch = NULL]

  # Join: house_sites -> raw_events
  joined <- data$raw_events_dt[house_sites, on = "site_id", nomatch = NULL, allow.cartesian = TRUE]

  # Filter to overlapping events and clamp times
  joined <- joined[start_time < date_of_transfer & end_time >= CONFIG$window_start]

  if (nrow(joined) == 0) {
    return(list(events_dt = NULL, lookup_chunk = lookup_chunk))
  }

  joined[, `:=`(
    clamped_start = pmax(start_time, CONFIG$window_start),
    clamped_end = pmin(end_time, date_of_transfer)
  )]
  joined[, event_hours := as.numeric(difftime(clamped_end, clamped_start, units = "hours"))]

  joined <- joined[event_hours > 0]
  return(list(events_dt = joined, lookup_chunk = lookup_chunk))
}

#' Calculate spill metrics per radius with a single pass over site-level data
#' @param lookup_dt Spill lookup data.table (house_id, site_id, distance_m)
#' @param events_dt Joined events data.table (or NULL if no events)
#' @return data.table with spill counts, hours, site counts, and distance metrics
calculate_metrics_by_radius <- function(lookup_dt, events_dt) {
  radii <- sort(CONFIG$radius_thresholds)

  if (is.null(lookup_dt) || nrow(lookup_dt) == 0) {
    return(NULL)
  }

  site_lookup <- lookup_dt[, .(distance_m = min(distance_m)), by = .(house_id, site_id)]

  if (!is.null(events_dt) && nrow(events_dt) > 0) {
    event_agg <- events_dt[, .(
      spill_hrs = sum(event_hours, na.rm = TRUE),
      spill_count = count_spills(clamped_start, clamped_end)
    ), by = .(house_id, site_id)]

    site_agg <- merge(
      site_lookup,
      event_agg,
      by = c("house_id", "site_id"),
      all.x = TRUE
    )
    site_agg[, `:=`(
      spill_hrs = fifelse(is.na(spill_hrs), 0, spill_hrs),
      spill_count = fifelse(is.na(spill_count), 0, spill_count)
    )]
  } else {
    site_agg <- copy(site_lookup)
    site_agg[, `:=`(
      spill_hrs = 0,
      spill_count = 0
    )]
  }

  # Collapse same-distance sites per house to avoid roll-join ties
  site_agg <- site_agg[, .(
    spill_hrs = sum(spill_hrs),
    spill_count = sum(spill_count),
    n_spill_sites = .N,
    distance_sum = sum(distance_m)
  ), by = .(house_id, distance_m)]

  # Order once and build cumulative metrics so each radius can use a rolling join
  setorder(site_agg, house_id, distance_m)
  site_agg[, `:=`(
    cum_spill_hrs = cumsum(spill_hrs),
    cum_spill_count = cumsum(spill_count),
    cum_distance_sum = cumsum(distance_sum),
    n_spill_sites = cumsum(n_spill_sites),
    min_distance = distance_m[1]
  ), by = house_id]
  setkey(site_agg, house_id, distance_m)

  radius_grid <- CJ(house_id = unique(site_agg$house_id), radius = radii)
  radius_grid[, radius_join := radius]
  setkey(radius_grid, house_id, radius_join)

  metrics <- site_agg[
    radius_grid,
    roll = Inf,
    on = .(house_id, distance_m = radius_join)
  ]

  metrics[, `:=`(
    spill_hrs = fifelse(is.na(cum_spill_hrs), 0, cum_spill_hrs),
    spill_count = fifelse(is.na(cum_spill_count), 0, cum_spill_count),
    n_spill_sites = fifelse(is.na(n_spill_sites), 0L, n_spill_sites),
    mean_distance = fifelse(n_spill_sites > 0, cum_distance_sum / n_spill_sites, NA_real_),
    min_distance = fifelse(n_spill_sites > 0, min_distance, NA_real_)
  )]

  metrics[, .(house_id, radius, spill_hrs, n_spill_sites, spill_count, mean_distance, min_distance)]
}

#' Get house metadata (price, n_days_in_window)
#' @param house_dt House price data.table
#' @return data.table with house metadata
get_house_metadata <- function(house_dt) {
  metadata <- house_dt[, .(
    house_id,
    price,
    n_days_in_window = as.integer(difftime(date_of_transfer, CONFIG$window_start, units = "days"))
  )]
  return(metadata)
}

#' Process a single chunk of houses
#' @param house_ids Vector of house_ids to process
#' @param data List containing loaded data tables
#' @return data.table with results for this chunk
process_chunk <- function(house_ids, data) {
  joined <- create_joined_events(house_ids, data)
  lookup_chunk <- joined$lookup_chunk

  # Get house metadata for this chunk
  house_meta <- get_house_metadata(data$house_dt[.(house_ids), nomatch = 0L])

  # Create grid for houses in this chunk
  chunk_houses <- CJ(
    house_id = house_meta$house_id,
    radius = CONFIG$radius_thresholds
  )

  # Merge all results
  setkey(chunk_houses, house_id, radius)
  setkey(house_meta, house_id)

  metrics_dt <- calculate_metrics_by_radius(lookup_chunk, joined$events_dt)
  if (!is.null(metrics_dt) && nrow(metrics_dt) > 0) setkey(metrics_dt, house_id, radius)

  result <- if (!is.null(metrics_dt) && nrow(metrics_dt) > 0) {
    metrics_dt[chunk_houses, on = .(house_id, radius)]
  } else {
    chunk_houses
  }
  result <- house_meta[result, on = "house_id"]

  return(result)
}

#' Create the prior-to-sale cross-sectional database
#' @param data List containing loaded data tables
#' @return data.table with aggregated prior-to-sale data
create_prior_to_sale_db <- function(data) {
  logger::log_info("Creating prior-to-sale cross-sectional database")

  # Split house_ids into chunks (index ranges to avoid pre-materializing all chunks)
  all_house_ids <- unique(data$house_dt$house_id)
  n_ids <- length(all_house_ids)
  n_chunks <- ceiling(n_ids / CONFIG$chunk_size)
  starts <- seq(1L, n_ids, by = CONFIG$chunk_size)

  logger::log_info("Processing {length(all_house_ids)} houses in {n_chunks} chunks")

  # Process all chunks
  result <- rbindlist(
    lapply(seq_along(starts), function(i) {
      logger::log_info("Processing chunk {i}/{n_chunks}")
      start <- starts[[i]]
      end <- min(start + CONFIG$chunk_size - 1L, n_ids)
      res <- process_chunk(all_house_ids[start:end], data)
      res
    }),
    use.names = TRUE,
    fill = TRUE
  )

  # Single pass NA/zero handling
  metric_cols <- c("spill_count", "spill_hrs", "n_spill_sites")
  for (col in metric_cols) {
    if (!col %in% names(result)) {
      result[, (col) := 0]
    } else {
      set(result, which(is.na(result[[col]])), col, 0)
    }
  }

  # Compute daily averages once after metrics are filled
  result[, `:=`(
    spill_count_daily_avg = spill_count / n_days_in_window,
    spill_hrs_daily_avg = spill_hrs / n_days_in_window
  )]

  setorder(result, house_id, radius)
  logger::log_info("Prior-to-sale database created: {nrow(result)} rows")
  return(result)
}

#' Export data to Parquet format
#' @param data data.table to export
#' @return NULL
export_data <- function(data) {
  tryCatch(
    {
      output_path <- here::here(
        "data", "processed", "cross_section", "sales", "prior_to_sale"
      )

      logger::log_info("Exporting prior-to-sale data to parquet")
      arrow::write_dataset(
        data,
        path = output_path,
        format = "parquet",
        partitioning = "radius"
      )

      logger::log_info("Data export complete")
      logger::log_info("Data saved to: {output_path}")
    },
    error = function(e) {
      logger::log_error("Data export failed: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

# Main Execution
############################################################

#' Main execution function
#' @return NULL
main <- function() {
  initialise_environment()
  setup_logging()

  # Load data from parquet files
  data <- load_data()

  # Create prior-to-sale cross-sectional database
  prior_to_sale_data <- create_prior_to_sale_db(data)

  # Export data
  export_data(prior_to_sale_data)

  logger::log_info("Script completed successfully")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
