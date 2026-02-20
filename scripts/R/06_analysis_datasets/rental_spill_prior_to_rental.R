############################################################
# Create Prior-to-Rental Cross-sectional Database: Rental-Site Level
# Project: Sewage
# Date: 05/02/2026
# Author: Alina Zeltikova
############################################################

#' This script creates a cross-sectional database that aggregates
#' sewage spill data from January 1, 2021 to the day before each rental.
#' It calculates daily average spill count and spill hours for each rental-spill site pair.

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
  log_path <- here::here("output", "log", "rental_spill_prior_to_rental.log")
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
  chunk_size = 10000,  # Number of rentals to process per batch
  unique_spill_sites_path = here::here("data", "processed", "unique_spill_sites.parquet")
)

# Data Loading Functions
############################################################

#' Derive site-level missing flags for the rental-year window
#' @param unique_sites_dt Unique spill sites data.table
#' @param rental_years Integer vector of rental years
#' @return data.table with site_id and site_missing
derive_site_missing_flags <- function(unique_sites_dt, rental_years) {
  if (is.null(unique_sites_dt) || nrow(unique_sites_dt) == 0) {
    return(data.table(site_id = character(), site_missing = logical()))
  }
  
  avail_cols <- paste0("available_year_", rental_years)
  missing_cols <- setdiff(avail_cols, names(unique_sites_dt))
  if (length(missing_cols) > 0) {
    logger::log_warn(
      "Missing availability columns in unique spill sites: {paste(missing_cols, collapse = ', ')}; treating as missing"
    )
    for (col in missing_cols) {
      unique_sites_dt[, (col) := FALSE]
    }
  }
  
  unique_sites_dt[, (avail_cols) := lapply(.SD, function(x) {
    x <- as.logical(x)
    fifelse(is.na(x), FALSE, x)
  }), .SDcols = avail_cols]
  
  unique_sites_dt[, site_missing := !Reduce(`&`, .SD), .SDcols = avail_cols]
  site_missing_dt <- unique_sites_dt[, .(site_missing = any(site_missing)), by = site_id]
  setkey(site_missing_dt, site_id)
  return(site_missing_dt)
}

#' Load datasets from parquet files
#' @return List containing rental_dt, spill_lookup_dt, raw_events_dt, site_missing_dt
load_data <- function() {
  logger::log_info("Loading datasets from parquet files")
  
  # Load rental data
  rental_dt <- arrow::open_dataset(
    file.path(CONFIG$processed_dir, "zoopla", "zoopla_rentals.parquet")
  ) |>
    dplyr::select(rental_id, listing_price, rented_est) |>
    dplyr::filter(rented_est >= CONFIG$window_start) |>
    dplyr::collect() |>
    as.data.table()
  setkey(rental_dt, rental_id)
  logger::log_info("Rental data loaded: {nrow(rental_dt)} rows")
  
  rental_years <- sort(unique(lubridate::year(rental_dt$rented_est)))
  min_rental_year <- min(rental_years)
  max_rental_year <- max(rental_years)
  sample_years <- seq(min_rental_year, max_rental_year)
  logger::log_info("Rental year window for missingness: {min_rental_year}-{max_rental_year}")
  
  unique_sites_dt <- arrow::read_parquet(CONFIG$unique_spill_sites_path) |>
    as.data.table()
  site_missing_dt <- derive_site_missing_flags(unique_sites_dt, sample_years)
  logger::log_info(
    "Unique spill sites loaded: {nrow(unique_sites_dt)} rows; missing flags for {nrow(site_missing_dt)} sites"
  )
  logger::log_info(
    "Sites flagged as missing in rental-year window: {site_missing_dt[site_missing == TRUE, .N]}"
  )
  
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
    raw_events_dt = raw_events_dt,
    site_missing_dt = site_missing_dt
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
  lookup_chunk <- data$site_missing_dt[lookup_chunk, on = "site_id"]
  lookup_chunk[, site_missing := fifelse(is.na(site_missing), TRUE, site_missing)]
  
  if (nrow(lookup_chunk) == 0) {
    return(list(events_dt = NULL, lookup_chunk = lookup_chunk))
  }
  
  # Join: rental -> spill_lookup
  rental_sites <- lookup_chunk[rental_chunk, on = "rental_id", nomatch = NULL]
  
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

#' Calculate spill metrics per radius at site level
#' @param lookup_dt Spill lookup data.table (rental_id, site_id, distance_m, site_missing)
#' @param events_dt Joined events data.table (or NULL if no events)
#' @return data.table with spill counts, hours, site counts, distance metrics, and missing flag

calculate_metrics_by_radius <- function(lookup_dt, events_dt) {
  radii <- sort(CONFIG$radius_thresholds)
  
  if (is.null(lookup_dt) || nrow(lookup_dt) == 0) {
    return(NULL)
  }
  
  site_lookup <- lookup_dt[, .(
    distance_m = min(distance_m),
    site_missing = any(site_missing)
  ), by = .(rental_id, site_id)]
  
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
  
  # TEST: REMOVE LATER
  metrics_list <- vector("list", length(radii))
  
  for (i in seq_along(radii)) {
    r <- radii[i]
    # Shallow copy (copies structure, not data) and filter
    dt <- site_agg[distance_m <= r]
    dt[, radius := r]
    metrics_list[[i]] <- dt
  }
  
  metrics <- rbindlist(metrics_list, use.names = TRUE)
  
  # Replicate site data for each radius threshold and filter by distance
  # metrics <- rbindlist(lapply(radii, function(r) {
  #   dt <- copy(site_agg)
  #   dt[, radius := r]
  #   dt <- dt[distance_m <= r]
  #   dt
  # }))
  
  # Set missing metrics to NA
  metrics[site_missing == TRUE, `:=`(
    spill_hrs = NA_real_,
    spill_count = NA_real_
  )]
  
  return(metrics[, .(rental_id, site_id, radius, distance_m,
                     spill_hrs, spill_count, site_missing)])
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
  
  # Deduplicate to ensure unique rental-site pairs
  lookup_chunk <- unique(lookup_chunk, by = c("rental_id", "site_id"))
  
  # Get rental metadata for this chunk
  rental_meta <- get_rental_metadata(data$rental_dt[.(rental_ids), nomatch = 0L])
  
  # Calculate site-level metrics with radius filtering
  metrics_dt <- calculate_metrics_by_radius(lookup_chunk, joined$events_dt)
  
  if (is.null(metrics_dt) || nrow(metrics_dt) == 0) {
    # Return empty result with proper structure
    return(data.table(
      rental_id = character(),
      site_id = character(),
      radius = numeric(),
      distance_m = numeric(),
      spill_hrs = numeric(),
      spill_count = numeric(),
      site_missing = logical(),
      price = numeric(),
      n_days_in_window = integer()
    ))
  }
  
  # Merge with rental metadata
  setkey(metrics_dt, rental_id)
  setkey(rental_meta, rental_id)
  result <- rental_meta[metrics_dt, on = "rental_id"]
  
  return(result)
}

#' Create the prior-to-rental cross-sectional database
#' @param data List containing loaded data tables
#' @return data.table with aggregated prior-to-rental data

create_prior_to_rental_db <- function(data) {
  logger::log_info("Creating prior-to-rental cross-sectional database")
  
  all_rental_ids <- unique(data$rental_dt$rental_id)
#  all_rental_ids <- head(all_rental_ids, 50000) # TEST: REMOVE LATER
  n_ids <- length(all_rental_ids)
  n_chunks <- ceiling(n_ids / CONFIG$chunk_size)
  starts <- seq(1L, n_ids, by = CONFIG$chunk_size)
  
  logger::log_info("Processing {length(all_rental_ids)} rentals in {n_chunks} chunks")
  
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
  
  # NA/zero handling
  if (!"site_missing" %in% names(result)) {
    result[, site_missing := FALSE]
  } else {
    result[is.na(site_missing), site_missing := FALSE]
  }
  
  metric_cols <- c("spill_count", "spill_hrs")
  for (col in metric_cols) {
    if (!col %in% names(result)) {
      result[, (col) := 0]
    } else {
      result[is.na(get(col)) & !site_missing, (col) := 0]
    }
  }
  
  # Compute daily averages per site-rental
  result[, `:=`(
    spill_count_daily_avg = spill_count / n_days_in_window,
    spill_hrs_daily_avg = spill_hrs / n_days_in_window
  )]
  
  logger::log_info("Rows with missing sites (spill metrics set to NA): {result[site_missing == TRUE, .N]}")
  
  # Order by rental_id, site_id, radius
  setorder(result, rental_id, site_id, radius)
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
        "data", "processed", "cross_section", "rentals", "prior_to_rental", "rental_site"
      )
      
      logger::log_info("Exporting prior-to-rental data to parquet")
      arrow::write_dataset(
        data,
        path = output_path,
        format = "parquet",
        partitioning = c("radius") 
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
