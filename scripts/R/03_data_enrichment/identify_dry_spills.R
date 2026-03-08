############################################################
# Identify Dry Spills Using Rainfall Data
# Project: Sewage
# Date: 28/12/2024
# Author: Refactored for academic sewage spills research project
############################################################

#' This script enriches spill data with rainfall information to identify
#' "dry spills" - sewage spills that occurred during periods of low rainfall.
#' Multiple methodologies are applied:
#' - Exact site grid cell analysis
#' - 9-cell surrounding area analysis  
#' - EA methodology (day + prior day)
#' - BBC methodology (4-day lookback)

# Configuration
############################################################

CONFIG <- list(
  # File paths
  spills_file = here::here(
    "data", "processed", "matched_events_annual_data", 
    "matched_events_annual_data.parquet"),
  rainfall_file = here::here("data", "processed", "rainfall", "rainfall_data_cleaned.parquet"),
  unique_sites_file = here::here("data", "processed", "unique_spill_sites.parquet"),
  lookup_table_file = here::here("data", "processed", "rainfall", "spill_site_grid_lookup.parquet"),
  dry_spills_export_path = here::here("data", "processed", "rainfall", "dry_spills.parquet"),
  
  # Rainfall parameters
  dry_threshold_mm = 0.25,
  
  # Temporal parameters (days before spill)
  time_offsets = c(0, -1, -2, -3),  # Day 0 = spill day, -1 = day before, etc.
  temporal_lookback_days = 4,  # Number of days to look back for rainfall data filtering
  
  # Performance settings
  sites_per_chunk = 500,  # Number of sites to process per chunk (memory management)
  
  # Parallel processing settings optimized for M1 MacBook Pro (8 cores, 16GB RAM)
  n_cores = min(6, parallel::detectCores() - 2)  # Use 6 cores, leave 2 free for system
)

# Set Up Functions
############################################################

#' Initialise the R environment with required packages
#' @return NULL
initialise_environment <- function() {
  required_packages <- c(
    "dplyr", "lubridate", "here", "ncdf4", "rnrfa",
    "abind", "purrr", "data.table", "stringr", "logger",
    "glue", "fs", "arrow", "future", "furrr"
  )
  
  invisible(lapply(required_packages, library, character.only = TRUE))
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here(
    "output", "log", "identify_dry_spills.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Parallel Processing Functions
############################################################

#' Setup parallel processing configuration
#' @param n_cores Integer. Number of cores for parallel processing
#' @return NULL (invisible). Configures global future plan
setup_parallel_processing <- function(n_cores) {
  logger::log_info("Configuring parallel processing with {n_cores} cores")
  future::plan(future::multisession, workers = n_cores)
}

# Data Loading Functions
############################################################

#' Load spill data
#' @param file_path Path to spills RData file
#' @return data.table of spill data
load_spill_data <- function(file_path = CONFIG$spills_file) {
  
  logger::log_info("Loading spill data from: {basename(file_path)}")
  spills_dt <- rio::import(file_path, trust = TRUE) |>
    dplyr::select(site_id, year, water_company, start_time, end_time, ngr) |>
    data.table::as.data.table()
  
  return(spills_dt)
}

#' Load rainfall data
#' @param file_path Path to rainfall parquet file
#' @return data.table of rainfall data with columns: x_idx, y_idx, date, rainfall
load_rainfall_data <- function(file_path = CONFIG$rainfall_file) {
  
  logger::log_info("Loading rainfall data from: {basename(file_path)}")
  rainfall_dt <- arrow::read_parquet(file_path) |>
    data.table::as.data.table()  

  return(rainfall_dt)
}

#' Load spill site to grid cell lookup table
#' @param file_path Path to lookup table parquet file
#' @return data.table with columns: ngr, easting, northing, x_idx, y_idx, is_center
load_site_grid_lookup <- function(file_path = CONFIG$lookup_table_file) {
  
  logger::log_info("Loading site-grid lookup table from: {basename(file_path)}")
  lookup_dt <- arrow::read_parquet(file_path) |>
    data.table::as.data.table()
  
  return(lookup_dt)
}

# Temporal Matching Functions
############################################################

#' Process spill start times and add temporal offset dates
#' @param spills_dt data.table with start_time column
#' @return data.table with added date columns
process_spill_dates <- function(spills_dt) {
  
  # Extract date from start_time and add temporal offsets
  spills_dt[, spill_date := as.Date(start_time)]
  spills_dt <- add_standard_rainfall_offsets(spills_dt, "spill_date")
  
  return(spills_dt)
}

#' Get unique dates needed for rainfall filtering
#' @param spills_dt data.table with date offset columns
#' @return vector of unique dates
get_required_rainfall_dates <- function(spills_dt) {
  offset_cols <- get_standard_rainfall_offset_cols()
  
  # Collect all dates from all offset columns
  all_dates <- unlist(spills_dt[, ..offset_cols], use.names = FALSE)
  
  unique_dates <- sort(unique(all_dates))
  
  return(unique_dates)
}

#' Match spills with rainfall data through spatial lookup
#' @param spills_dt data.table with spill data and date columns
#' @param site_grid_lookup data.table mapping NGRs to grid cells
#' @param rainfall_dt data.table with rainfall data
#' @param required_dates vector of dates to filter rainfall data
#' @return data.table with spills joined to rainfall data
match_spills_to_rainfall <- function(spills_dt, site_grid_lookup, rainfall_dt, required_dates) {
  
  # Filter rainfall data to required dates only
  rainfall_filtered <- rainfall_dt[date %in% required_dates]
  
  # Pre-filter site grid lookup to only NGRs present in spills (memory optimization)
  spill_ngrs <- unique(spills_dt$ngr)
  site_grid_filtered <- site_grid_lookup[ngr %in% spill_ngrs]
  
  # Join spills to site grid lookup (allow Cartesian since each NGR maps to 9 cells)
  spills_with_grids <- spills_dt[site_grid_filtered, on = "ngr", nomatch = 0, allow.cartesian = TRUE]
  
  # Create long format for temporal matching
  spills_long <- data.table::melt(
    spills_with_grids,
    id.vars = c(
      "site_id", "year", "water_company", 
      "start_time", "end_time",
      "ngr", "northing", "easting", "x_idx", "y_idx", "is_center"),
    measure.vars = c("date_0", "date_minus1", "date_minus2", "date_minus3"),
    variable.name = "time_offset",
    value.name = "date"
  )
  
  # Join with rainfall data
  result <- spills_long[rainfall_filtered, on = c("x_idx", "y_idx", "date"), nomatch = 0]
  
  return(result)
}

# Rainfall Processing Functions
############################################################

#' Calculate rainfall statistics for each spill
#' @param matched_data data.table with spills matched to rainfall
#' @return data.table with rainfall statistics per spill
calculate_rainfall_metrics <- function(matched_data) {
  
  # Create spill identifier (use available columns)
  matched_data[, spill_id := paste(site_id, start_time, sep = "_")]
  
  rainfall_metrics <- calculate_standard_rainfall_indicators(
    matched_data,
    by_cols = "spill_id"
  )
  
  # Add back spill metadata (get first occurrence of each spill_id)
  spill_metadata <- matched_data[, .SD[1], by = spill_id,
                                 .SDcols = c("site_id", "year", "water_company", "ngr", "start_time", "end_time")]
  
  # Join rainfall metrics with metadata
  rainfall_metrics <- rainfall_metrics[spill_metadata, on = "spill_id"]

  # Remove superfluous spill_id, retain site_id for final dataset
  rainfall_metrics[, spill_id := NULL]
  
  return(rainfall_metrics)
}

# Data Filtering Functions for Parallel Processing
############################################################

#' Calculate required date range for a chunk of spills (with lookback buffer)
#' @param chunk_spills data.table with spills from a specific chunk of sites
#' @return list with min_date and max_date (min_date includes 4-day buffer for t-4 analysis)
get_chunk_date_range <- function(chunk_spills) {
  spill_dates <- as.Date(chunk_spills$start_time)
  min_date <- min(spill_dates, na.rm = TRUE) - CONFIG$temporal_lookback_days  # Lookback for temporal analysis
  max_date <- max(spill_dates, na.rm = TRUE)      # Up to spill date for t-0 analysis
  
  return(list(min_date = min_date, max_date = max_date))
}

#' Filter rainfall data to only required dates and grid cells for a chunk
#' @param rainfall_dt data.table with all rainfall data
#' @param chunk_lookup data.table with site-grid mappings for chunk sites only
#' @param date_range list with min_date and max_date
#' @return data.table with filtered rainfall data for chunk
filter_rainfall_for_chunk <- function(rainfall_dt, chunk_lookup, date_range) {
  # Get unique grid cells for this chunk
  required_grid_cells <- unique(chunk_lookup[, .(x_idx, y_idx)])
  n_grid_cells <- nrow(required_grid_cells)
  
  # Filter rainfall by date range and grid cells
  filtered_rainfall <- rainfall_dt[
    date >= date_range$min_date & 
    date <= date_range$max_date
  ]
  
  # Further filter by grid cells using efficient join
  filtered_rainfall <- filtered_rainfall[required_grid_cells, on = c("x_idx", "y_idx"), nomatch = 0]
  
  return(filtered_rainfall)
}

#' Prepare filtered datasets for a chunk before parallel processing
#' @param chunk_sites character vector of NGRs for this chunk
#' @param year_spills data.table with all spills for the target year
#' @param site_grid_lookup data.table with all site-grid mappings
#' @param rainfall_dt data.table with all rainfall data
#' @return list with chunk_spills, chunk_lookup, and chunk_rainfall
prepare_chunk_data <- function(chunk_sites, year_spills, site_grid_lookup, rainfall_dt) {
  # Filter spills to only sites in this chunk
  chunk_spills <- year_spills[ngr %in% chunk_sites]
  
  # Filter lookup table to only sites in this chunk
  chunk_lookup <- site_grid_lookup[ngr %in% chunk_sites]
  
  # Calculate required date range (with 4-day lookback)
  date_range <- get_chunk_date_range(chunk_spills)
  
  # Filter rainfall data to only required dates and grid cells
  chunk_rainfall <- filter_rainfall_for_chunk(rainfall_dt, chunk_lookup, date_range)
  
  return(list(
    chunk_spills = chunk_spills,
    chunk_lookup = chunk_lookup,
    chunk_rainfall = chunk_rainfall
  ))
}

#' Process a chunk with pre-filtered data (memory efficient)
#' @param chunk_data list with chunk_spills, chunk_lookup, and chunk_rainfall
#' @param chunk_id integer identifier for this chunk (for logging)
#' @param total_chunks integer total number of chunks (for logging)
#' @return data.table with rainfall metrics for spills in this chunk
process_chunk_with_filtered_data <- function(chunk_data, chunk_id, total_chunks) {
  
  chunk_spills <- chunk_data$chunk_spills
  chunk_lookup <- chunk_data$chunk_lookup
  chunk_rainfall <- chunk_data$chunk_rainfall
  
  # Process dates for chunk spills
  chunk_spills <- process_spill_dates(chunk_spills)
  
  # Get required dates for rainfall filtering
  required_dates <- get_required_rainfall_dates(chunk_spills)
  
  # Match spills to rainfall using the filtered data
  matched_data <- match_spills_to_rainfall(chunk_spills, chunk_lookup, chunk_rainfall, required_dates)
  
  # Calculate rainfall metrics
  rainfall_metrics <- calculate_rainfall_metrics(matched_data)
  
  return(rainfall_metrics)
}

#' Process spills for a single year with rainfall matching
#' @param spills_dt data.table with all spills
#' @param target_year integer year to process
#' @param site_grid_lookup data.table mapping NGRs to grid cells
#' @param rainfall_dt data.table with all rainfall data
#' @return data.table with rainfall metrics for spills in target year
process_year <- function(spills_dt, target_year, site_grid_lookup, rainfall_dt) {
  
  logger::log_info("Processing spills for year {target_year} using site-based chunking")
  
  # Filter spills to target year
  year_spills <- spills_dt[year == target_year]
  
  # Get unique sites with spills in this year
  unique_sites <- unique(year_spills$ngr)
  
  # Split sites into chunks for memory management
  chunk_size <- CONFIG$sites_per_chunk
  site_chunks <- split(unique_sites, ceiling(seq_along(unique_sites) / chunk_size))
  total_chunks <- length(site_chunks)
  
  # Prepare filtered data for each chunk before parallel processing
  chunk_datasets <- lapply(
  site_chunks, prepare_chunk_data,
  year_spills = year_spills,
  site_grid_lookup = site_grid_lookup,
  rainfall_dt = rainfall_dt
)

  # Process each chunk in parallel
  all_chunk_results <- furrr::future_imap(
    chunk_datasets,
    ~process_chunk_with_filtered_data(
      chunk_data = .x,
      chunk_id = .y,
      total_chunks = total_chunks
    ),
    .options = furrr::furrr_options(seed = TRUE)
  )
  
  # Filter out failed/empty results
  all_chunk_results <- all_chunk_results[!sapply(all_chunk_results, is.null)]
  
  # Combine all chunk results
  rainfall_metrics <- data.table::rbindlist(all_chunk_results, use.names = TRUE)
  
  return(rainfall_metrics)
}

#' Process all years sequentially
#' @param spills_dt data.table with all spill data
#' @param available_years numeric vector of years to process
#' @param site_grid_lookup data.table mapping sites to grid cells
#' @param rainfall_dt data.table with all rainfall data
#' @return list of data.tables with results for each successful year
process_years_sequential <- function(spills_dt, available_years, site_grid_lookup, rainfall_dt) {
  logger::log_info("Processing {length(available_years)} years sequentially...")
  
  all_results <- purrr::map(available_years, function(year) {
    logger::log_info("=== Processing Year {year} ===")
    
    {
      year_results <- process_year(spills_dt, year, site_grid_lookup, rainfall_dt)
      
      if (nrow(year_results) > 0) {
        logger::log_info("Completed {year}: {nrow(year_results)} spills")
        return(year_results)
      } else {
        logger::log_warn("No results for year {year}")
        return(NULL)
      }
    }
  })
  
  # Filter out failed/empty results
  successful_results <- all_results[!sapply(all_results, is.null)]
  logger::log_info("Successfully processed {length(successful_results)}/{length(available_years)} years")
  
  return(successful_results)
}

# Finalise data
############################################################

#' Filter results to keep spills where ANY indicator < threshold
#' @param final_results data.table with rainfall metrics
#' @param dry_threshold_mm numeric threshold for dry spills (default from CONFIG)
#' @return data.table filtered to spills considered dry by at least one indicator
filter_dry_spills <- function(final_results, dry_threshold_mm = CONFIG$dry_threshold_mm) {
  
  # Consider the six explicit rainfall columns
  rainfall_cols <- get_standard_rainfall_indicator_cols()
  
  # Row-wise minimum across indicators (any < threshold ⇒ min < threshold)
  final_results[, min_rainfall := do.call(pmin, c(.SD, na.rm = TRUE)), .SDcols = rainfall_cols]
  
  # Keep rows with a finite minimum below threshold (exclude rows with all-NA ⇒ Inf)
  dry_spills <- final_results[is.finite(min_rainfall) & (min_rainfall < dry_threshold_mm)]
  
  # Remove the temporary helper column
  dry_spills[, min_rainfall := NULL]
  
  return(dry_spills)
}

#' Export dry spills data to processed folder
#' @param dry_spills_data data.table with filtered dry spills
#' @param output_path character path for export (optional)
#' @return NULL (invisible). Saves file as side effect
export_dry_spills <- function(dry_spills_data, output_path = CONFIG$dry_spills_export_path) {
  
  # Create output directory if needed
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  
  # Reorder columns for readability
  preferred_order <- c(
    "site_id", "year", "water_company", "ngr", "start_time", "end_time",
    get_standard_rainfall_indicator_cols()
  )
  existing <- intersect(preferred_order, names(dry_spills_data))
  remaining <- setdiff(names(dry_spills_data), existing)
  dry_spills_data <- dry_spills_data[, c(existing, remaining), with = FALSE]

  # Sort records in a sensible order
  data.table::setorder(dry_spills_data, site_id, year, start_time)

  logger::log_info("Exporting {nrow(dry_spills_data)} dry spills to: {output_path}")
  arrow::write_parquet(dry_spills_data, output_path)
  logger::log_info("Dry spills export completed")
  
  invisible()
}

# Main Execution
############################################################

main <- function() {
  # Setup
  initialise_environment()
  setup_logging()
  
  logger::log_info("Starting dry spill identification pipeline")
  
  # Load data
  spills_dt <- load_spill_data()
  rainfall_dt <- load_rainfall_data()
  site_grid_lookup <- load_site_grid_lookup()
  
  # Get available years
  available_years <- sort(unique(spills_dt$year))
  logger::log_info("Processing spills for years: {paste(available_years, collapse = ', ')}")
  
  # Setup parallel processing for site chunks (used within process_year)
  setup_parallel_processing(CONFIG$n_cores)
  on.exit(future::plan(future::sequential), add = TRUE)
  
  # Process years sequentially and chunks in parallel
  all_results <- process_years_sequential(
    spills_dt, available_years, site_grid_lookup, rainfall_dt)
  final_results <- data.table::rbindlist(all_results, use.names = TRUE)
  
  # Filter to dry spills only and export for downstream aggregation
  dry_spills_data <- filter_dry_spills(final_results)
  export_dry_spills(dry_spills_data)
  
  logger::log_info("Dry spill identification completed successfully")
}

# Execute main function
if (sys.nframe() == 0) {
  main()
}
