############################################################
# Create Prior-to-Rental Cross-sectional Database
# Project: Sewage
# Date: 19/12/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates a cross-sectional database that aggregates
#' sewage spill data from January 1, 2021 to the day before each rental.
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
  log_path <- here::here("output", "log", "cross_section_prior_to_rental.log")
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
  chunk_size = 100000  # Number of rentals to process per batch
)

# Data Loading Functions
############################################################

#' Load datasets from parquet files
#' @return List containing rental_dt, spill_lookup_dt, raw_events_dt
load_data <- function() {
  logger::log_info("Loading datasets from parquet files")

  # Load rental data
  rental_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "zoopla", "zoopla_rentals.parquet")
  ) |>
    dplyr::select(rental_id, listing_price, rented_est) |>
    dplyr::filter(rented_est > CONFIG$window_start) |>
    dplyr::collect() |>
    as.data.table()
  setkey(rental_dt, rental_id)
  logger::log_info("Rental data loaded: {nrow(rental_dt)} rows")

 # Load spill lookup - filter to max radius threshold to reduce join size
  max_radius <- max(CONFIG$radius_thresholds)
  spill_lookup_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "zoopla", "spill_rental_lookup.parquet")
  ) |>
    dplyr::select(rental_id, site_id, distance_m) |>
    dplyr::filter(distance_m <= max_radius) |>
    dplyr::collect() |>
    as.data.table()
  setkey(spill_lookup_dt, rental_id)
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
    rental_dt = rental_dt,
    spill_lookup_dt = spill_lookup_dt,
    raw_events_dt = raw_events_dt
  )
}

# Processing Functions
############################################################

#' Create joined events table for a subset of rentals
#' @param rental_ids Vector of rental_ids to process
#' @param data List containing loaded data tables
#' @return list with `events_dt` (joined data) and `lookup_chunk`
create_joined_events <- function(rental_ids, data) {
  # Subset to this chunk of rentals
  rental_chunk <- data$rental_dt[.(rental_ids), nomatch = 0L]
  lookup_chunk <- data$spill_lookup_dt[
    data.table(rental_id = rental_ids),
    on = "rental_id",
    nomatch = 0L
  ]
  lookup_chunk <- lookup_chunk[, .(rental_id, site_id, distance_m)]

  if (nrow(lookup_chunk) == 0) {
    return(list(events_dt = NULL, lookup_chunk = lookup_chunk))
  }

  # Join: rental -> spill_lookup
  rental_sites <- lookup_chunk[rental_chunk, nomatch = NULL]

  # Join: rental_sites -> raw_events
  joined <- data$raw_events_dt[rental_sites, on = "site_id", nomatch = NULL, allow.cartesian = TRUE]

  # Filter to overlapping events and clamp times
  joined <- joined[start_time < rented_est & end_time >= CONFIG$window_start]

  if (nrow(joined) == 0) {
    return(list(events_dt = NULL, lookup_chunk = lookup_chunk))
  }

  joined[, `:=`(
    clamped_start = pmax(start_time, CONFIG$window_start),
    clamped_end = pmin(end_time, rented_est)
  )]
  joined[, event_hours := as.numeric(difftime(clamped_end, clamped_start, units = "hours"))]

  joined <- joined[event_hours > 0]
  return(list(events_dt = joined, lookup_chunk = lookup_chunk))
}

#' Calculate spill metrics per radius with a single pass over site-level data
#' @param lookup_dt Spill lookup data.table (rental_id, site_id, distance_m)
#' @param events_dt Joined events data.table (or NULL if no events)
#' @return data.table with spill counts, hours, site counts, and distance metrics
calculate_metrics_by_radius <- function(lookup_dt, events_dt) {
  radii <- sort(CONFIG$radius_thresholds)

  if (is.null(lookup_dt) || nrow(lookup_dt) == 0) {
    return(NULL)
  }

  site_lookup <- lookup_dt[, .(distance_m = min(distance_m)), by = .(rental_id, site_id)]

  if (!is.null(events_dt) && nrow(events_dt) > 0) {
    event_agg <- events_dt[, .(
      spill_hrs = sum(event_hours, na.rm = TRUE),
      spill_count = count_spills(clamped_start, clamped_end)
    ), by = .(rental_id, site_id)]

    site_agg <- merge(
      site_lookup,
      event_agg,
      by = c("rental_id", "site_id"),
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

  # Order once and build cumulative metrics so each radius can use a rolling join
  setorder(site_agg, rental_id, distance_m)
  site_agg[, `:=`(
    cum_spill_hrs = cumsum(spill_hrs),
    cum_spill_count = cumsum(spill_count),
    cum_distance_sum = cumsum(distance_m),
    n_spill_sites = seq_len(.N),
    min_distance = distance_m[1]
  ), by = rental_id]
  setkey(site_agg, rental_id, distance_m)

  radius_grid <- CJ(rental_id = unique(site_agg$rental_id), radius = radii)
  radius_grid[, radius_join := radius]
  setkey(radius_grid, rental_id, radius_join)

  metrics <- site_agg[
    radius_grid,
    roll = Inf,
    on = .(rental_id, distance_m = radius_join)
  ]

  metrics[, `:=`(
    spill_hrs = fifelse(is.na(cum_spill_hrs), 0, cum_spill_hrs),
    spill_count = fifelse(is.na(cum_spill_count), 0, cum_spill_count),
    n_spill_sites = fifelse(is.na(n_spill_sites), 0L, n_spill_sites),
    mean_distance = fifelse(n_spill_sites > 0, cum_distance_sum / n_spill_sites, NA_real_),
    min_distance = fifelse(n_spill_sites > 0, min_distance, NA_real_)
  )]

  metrics[, .(rental_id, radius, spill_hrs, n_spill_sites, spill_count, mean_distance, min_distance)]
}

#' Get rental metadata (listing_price, n_days_in_window)
#' @param rental_dt Rental data.table
#' @return data.table with rental metadata
get_rental_metadata <- function(rental_dt) {
  metadata <- rental_dt[, .(
    rental_id,
    listing_price,
    n_days_in_window = as.integer(difftime(rented_est, CONFIG$window_start, units = "days"))
  )]
  return(metadata)
}

#' Process a single chunk of rentals
#' @param rental_ids Vector of rental_ids to process
#' @param data List containing loaded data tables
#' @return data.table with results for this chunk
process_chunk <- function(rental_ids, data) {
  joined <- create_joined_events(rental_ids, data)
  lookup_chunk <- joined$lookup_chunk

  # Get rental metadata for this chunk
  rental_meta <- get_rental_metadata(data$rental_dt[.(rental_ids), nomatch = 0L])

  # Create grid for rentals in this chunk
  chunk_rentals <- CJ(
    rental_id = rental_meta$rental_id,
    radius = CONFIG$radius_thresholds
  )

  # Merge all results
  setkey(chunk_rentals, rental_id, radius)
  setkey(rental_meta, rental_id)

  metrics_dt <- calculate_metrics_by_radius(lookup_chunk, joined$events_dt)
  if (!is.null(metrics_dt) && nrow(metrics_dt) > 0) setkey(metrics_dt, rental_id, radius)

  result <- if (!is.null(metrics_dt) && nrow(metrics_dt) > 0) {
    metrics_dt[chunk_rentals, on = .(rental_id, radius)]
  } else {
    chunk_rentals
  }
  result <- rental_meta[result, on = "rental_id"]

  return(result)
}

#' Create the prior-to-rental cross-sectional database
#' @param data List containing loaded data tables
#' @return data.table with aggregated prior-to-rental data
create_prior_to_rental_db <- function(data) {
  logger::log_info("Creating prior-to-rental cross-sectional database")

  # Split rental_ids into chunks (index ranges to avoid pre-materializing all chunks)
  all_rental_ids <- unique(data$rental_dt$rental_id)
  n_ids <- length(all_rental_ids)
  n_chunks <- ceiling(n_ids / CONFIG$chunk_size)
  starts <- seq(1L, n_ids, by = CONFIG$chunk_size)

  logger::log_info("Processing {length(all_rental_ids)} rentals in {n_chunks} chunks")

  # Process all chunks
  result <- rbindlist(
    lapply(seq_along(starts), function(i) {
      logger::log_info("Processing chunk {i}/{n_chunks}")
      start <- starts[[i]]
      end <- min(start + CONFIG$chunk_size - 1L, n_ids)
      res <- process_chunk(all_rental_ids[start:end], data)
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

  setorder(result, rental_id, radius)
  logger::log_info("Prior-to-rental database created: {nrow(result)} rows")
  return(result)
}

#' Export data to Parquet format
#' @param data data.table to export
#' @return NULL
export_data <- function(data) {
  tryCatch(
    {
      output_path <- here::here(
        "data", "processed", "cross_section", "rentals", "prior_to_rental"
      )

      logger::log_info("Exporting prior-to-rental data to parquet")
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

  # Create prior-to-rental cross-sectional database
  prior_to_rental_data <- create_prior_to_rental_db(data)

  # Export data
  export_data(prior_to_rental_data)

  logger::log_info("Script completed successfully")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
