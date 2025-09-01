############################################################
# Aggregate Rainfall Statistics by Site and Time Period
# Project: Sewage
# Date: 01/09/2025
# Author: Jacopo Olivieri
############################################################

#' This script aggregates rainfall data at the site-day level into temporal 
#' periods (yearly, monthly, quarterly) for all spill sites. Creates continuous
#' rainfall time series independent of spill events for correlation analysis.

# Set Up Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL (invisible). Loads packages and sources utilities
initialise_environment <- function() {
  required_packages <- c(
    "rmarkdown",  # Document generation and reporting
    "rio",        # Universal data import/export
    "tidyverse",  # Core data manipulation and visualisation packages
    "purrr",      # Functional programming tools for iteration
    "here",       # Platform-independent file paths
    "logger",     # Structured logging and debugging
    "glue",       # String interpolation for messages
    "fs",         # File system operations
    "data.table", # High-performance data manipulation
    "arrow",      # Parquet file I/O for large datasets
    "lubridate"   # Date and time manipulation
  )
  
  # Install missing packages and load all required libraries
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL (invisible). Creates log file and configures logger
setup_logging <- function() {
  log_path <- here::here("output", "log", "aggregate_rainfall_stats.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  # Input file paths - preprocessed data from earlier pipeline stages
  spill_sites_path = here::here("data", "processed", "unique_spill_sites.parquet"),      # Site metadata with coordinates
  rainfall_path = here::here("data", "processed", "rainfall", "rainfall_data_cleaned.parquet"),  # Grid-based daily rainfall data
  lookup_path = here::here("data", "processed", "rainfall", "spill_site_grid_lookup.parquet"),   # Site-to-grid mapping (9-cell neighbourhoods)
  
  # Output configuration - rainfall time series aggregated by temporal periods
  output_dir = here::here("data", "processed", "rainfall"),
  output_files = list(
    yearly = "rainfall_agg_yr.parquet",     # Annual rainfall sums by site
    monthly = "rainfall_agg_mo.parquet",    # Monthly rainfall sums by site
    quarterly = "rainfall_agg_qtr.parquet"  # Quarterly rainfall sums by site
  ),
  
  # Temporal range for aggregation - covers main study period for spill analysis
  start_date = as.Date("2021-01-01"),  # Start of comprehensive data availability
  end_date = as.Date("2023-12-31"),    # End of current study period
  
  # Memory management settings - optimised for large site-day grid processing
  sites_per_chunk = 500,  # Processes ~500 sites × 1,095 days = 547K records per chunk (prevents 16GB memory limit)
  n_cores = min(6, parallel::detectCores() - 2)  # Reserve cores for system stability during intensive processing
)

# Data Loading Functions
############################################################

#' Load unique spill sites data
#' @return data.table with site_id, water_company, ngr columns
load_spill_sites <- function() {
  logger::log_info("Loading spill sites from: {basename(CONFIG$spill_sites_path)}")
  sites_dt <- arrow::read_parquet(CONFIG$spill_sites_path) |>
    select(site_id, water_company, ngr) |>
    as.data.table()
  
  logger::log_info("Loaded {nrow(sites_dt)} unique spill sites")
  return(sites_dt)
}

#' Load cleaned rainfall data 
#' @return data.table with x_idx, y_idx, date, rainfall columns
load_rainfall_data <- function() {
  logger::log_info("Loading rainfall data from: {basename(CONFIG$rainfall_path)}")
  rainfall_dt <- arrow::read_parquet(CONFIG$rainfall_path) |>
    as.data.table()
  
  # Filter to required date range for efficiency
  rainfall_dt <- rainfall_dt[date >= CONFIG$start_date & date <= CONFIG$end_date]
  
  logger::log_info("Loaded rainfall data: {nrow(rainfall_dt)} records covering {CONFIG$start_date} to {CONFIG$end_date}")
  return(rainfall_dt)
}

#' Load spill site to grid cell lookup table
#' @return data.table with ngr, x_idx, y_idx, is_center columns  
load_site_grid_lookup <- function() {
  logger::log_info("Loading site-grid lookup from: {basename(CONFIG$lookup_path)}")
  lookup_dt <- arrow::read_parquet(CONFIG$lookup_path) |>
    select(ngr, x_idx, y_idx, is_center) |>
    as.data.table()
  
  logger::log_info("Loaded lookup table: {nrow(lookup_dt)} site-grid mappings")
  return(lookup_dt)
}

#' Load all base datasets required for processing
#' @return List with sites_dt, rainfall_dt, and lookup_dt
load_base_data <- function() {
  logger::log_info("Loading all base datasets")
  
  # Load all required datasets in parallel for efficiency
  list(
    sites = load_spill_sites(),      # Site metadata for creating complete time series
    rainfall = load_rainfall_data(), # Daily rainfall observations (already date-filtered)
    lookup = load_site_grid_lookup() # Spatial mapping between sites and rainfall grid cells
  )
}

# Site Chunking Functions
############################################################

#' Split sites into manageable chunks for memory-efficient processing
#' @param sites_dt data.table with site information
#' @return List of site chunks (each chunk is a data.table)
split_sites_into_chunks <- function(sites_dt) {
  logger::log_info("Splitting {nrow(sites_dt)} sites into chunks of {CONFIG$sites_per_chunk}")
  
  # Create chunk indices
  n_sites <- nrow(sites_dt)
  chunk_indices <- split(1:n_sites, ceiling(seq_len(n_sites) / CONFIG$sites_per_chunk))
  
  # Create site chunks
  site_chunks <- map(chunk_indices, function(indices) {
    sites_dt[indices, ]
  })
  
  logger::log_info("Created {length(site_chunks)} site chunks")
  return(site_chunks)
}

#' Create site-day grid for a single chunk of sites
#' @param chunk_sites data.table with sites for this chunk
#' @return data.table with site_id, water_company, ngr, date combinations for chunk
create_chunk_site_day_grid <- function(chunk_sites) {
  logger::log_debug("Creating site-day grid for chunk: {nrow(chunk_sites)} sites")
  
  # Generate complete date sequence
  date_seq <- seq(from = CONFIG$start_date, to = CONFIG$end_date, by = "day")
  
  # Create Cartesian product: chunk sites × all dates
  chunk_grid <- data.table::CJ(
    site_id = chunk_sites$site_id,
    date = date_seq,
    sorted = FALSE
  )
  
  # Join with site metadata
  chunk_grid <- chunk_sites[chunk_grid, on = "site_id"]
  
  logger::log_debug("Generated chunk grid: {nrow(chunk_grid)} site-day combinations")
  return(chunk_grid)
}

# Rainfall Calculation Functions
############################################################

#' Filter rainfall and lookup data to only required sites for memory efficiency
#' @param chunk_sites data.table with sites in current chunk
#' @param rainfall_dt data.table with all rainfall data
#' @param lookup_dt data.table with all site-grid mappings
#' @return List with filtered rainfall and lookup data for chunk
filter_data_for_chunk <- function(chunk_sites, rainfall_dt, lookup_dt) {
  logger::log_debug("Filtering data for chunk with {nrow(chunk_sites)} sites")
  
  # Extract NGRs for this chunk to filter spatial data
  chunk_ngrs <- chunk_sites$ngr
  
  # Filter lookup table to only sites in this chunk (reduces Cartesian product size)
  chunk_lookup <- lookup_dt[ngr %in% chunk_ngrs]
  
  # Get unique grid cells required for this chunk (each site maps to 9 cells)
  chunk_grid_cells <- unique(chunk_lookup[, .(x_idx, y_idx)])
  
  # Filter rainfall data to only required grid cells using efficient inner join
  # This dramatically reduces memory usage by eliminating irrelevant grid cells
  chunk_rainfall <- rainfall_dt[chunk_grid_cells, on = c("x_idx", "y_idx"), nomatch = 0]
  
  logger::log_debug("Filtered data: {nrow(chunk_lookup)} lookup entries, {nrow(chunk_rainfall)} rainfall records")
  
  return(list(
    rainfall = chunk_rainfall,
    lookup = chunk_lookup
  ))
}

#' Calculate daily rainfall indicators for a chunk of sites
#' @param chunk_sites data.table with sites in current chunk
#' @param rainfall_dt data.table with rainfall data
#' @param lookup_dt data.table with site-grid mappings
#' @return data.table with rainfall indicators for chunk
calculate_chunk_rainfall_indicators <- function(chunk_sites, rainfall_dt, lookup_dt) {
  logger::log_debug("Calculating rainfall indicators for chunk: {nrow(chunk_sites)} sites")
  
  # Filter data to only what's needed for this chunk (memory optimisation)
  chunk_data <- filter_data_for_chunk(chunk_sites, rainfall_dt, lookup_dt)
  
  # Create complete site-day grid for this chunk only
  chunk_grid <- create_chunk_site_day_grid(chunk_sites)
  
  # Join site-day grid with spatial lookup using Cartesian product
  # Each site-day gets matched to all 9 grid cells in its neighbourhood
  expanded_grid <- chunk_grid[chunk_data$lookup, on = "ngr", allow.cartesian = TRUE]
  
  # Join with rainfall observations using spatial and temporal keys
  rainfall_joined <- expanded_grid[chunk_data$rainfall, on = c("x_idx", "y_idx", "date"), nomatch = 0]
  
  # Calculate rainfall indicators by site-date aggregation
  rainfall_indicators <- rainfall_joined[, .(
    # Single cell rainfall: site center cell only (r1 = radius 0)
    rainfall_r1 = rainfall[is_center == TRUE][1], # Extract center cell value
    
    # Multi-cell rainfall: maximum across 9-cell neighbourhood (r9 = radius 1)
    rainfall_r9 = max(rainfall, na.rm = TRUE)     # Handle missing values in aggregation
  ), by = .(site_id, water_company, ngr, date)]
  
  # Clean up edge cases from aggregation
  rainfall_indicators[is.na(rainfall_r1), rainfall_r1 := NA_real_]           # No center cell data
  rainfall_indicators[is.infinite(rainfall_r9), rainfall_r9 := NA_real_]     # All cells missing
  
  logger::log_debug("Calculated rainfall indicators for {nrow(rainfall_indicators)} site-days in chunk")
  
  return(rainfall_indicators)
}

#' Process complete chunk: calculate indicators and aggregate by periods
#' @param chunk_sites data.table with sites in current chunk
#' @param rainfall_dt data.table with rainfall data  
#' @param lookup_dt data.table with site-grid mappings
#' @param chunk_id integer identifier for logging
#' @param total_chunks integer total number of chunks
#' @return List with yearly, monthly, quarterly aggregations for chunk
process_complete_chunk <- function(chunk_sites, rainfall_dt, lookup_dt, chunk_id, total_chunks) {
  logger::log_info("Processing chunk {chunk_id}/{total_chunks}: {nrow(chunk_sites)} sites")
  
  tryCatch({
    # Calculate daily rainfall indicators for this chunk
    chunk_indicators <- calculate_chunk_rainfall_indicators(chunk_sites, rainfall_dt, lookup_dt)
    
    # Aggregate by temporal periods
    chunk_aggregated <- aggregate_by_periods(chunk_indicators)
    
    logger::log_info("Completed chunk {chunk_id}/{total_chunks}")
    return(chunk_aggregated)
    
  }, error = function(e) {
    logger::log_error("Failed to process chunk {chunk_id}: {e$message}")
    return(NULL)
  })
}

#' Combine aggregated results from all chunks
#' @param chunk_results List of chunk results (each containing yearly/monthly/quarterly)
#' @return List with combined yearly, monthly, quarterly aggregations
combine_chunk_results <- function(chunk_results) {
  logger::log_info("Combining results from {length(chunk_results)} chunks")
  
  # Filter out failed chunks (NULL results)
  successful_chunks <- chunk_results[!sapply(chunk_results, is.null)]
  
  if (length(successful_chunks) == 0) {
    stop("All chunks failed to process")
  }
  
  logger::log_info("Successfully processed {length(successful_chunks)}/{length(chunk_results)} chunks")
  
  # Combine results for each temporal period
  combined_results <- list(
    yearly = rbindlist(map(successful_chunks, ~ .$yearly), use.names = TRUE, fill = TRUE),
    monthly = rbindlist(map(successful_chunks, ~ .$monthly), use.names = TRUE, fill = TRUE), 
    quarterly = rbindlist(map(successful_chunks, ~ .$quarterly), use.names = TRUE, fill = TRUE)
  )
  
  # Log final counts
  logger::log_info("Combined results: {nrow(combined_results$yearly)} yearly, {nrow(combined_results$monthly)} monthly, {nrow(combined_results$quarterly)} quarterly records")
  
  return(combined_results)
}

# Temporal Aggregation Functions  
############################################################

#' Add temporal period columns to site-day rainfall data
#' @param rainfall_indicators data.table with site-day rainfall data
#' @return data.table with year, month, quarter columns added
add_temporal_periods <- function(rainfall_indicators) {
  logger::log_info("Adding temporal period columns")
  
  # Extract temporal grouping variables for aggregation
  # Using data.table's efficient column assignment by reference
  rainfall_indicators[, `:=`(
    year = lubridate::year(date),      # Annual aggregation grouping
    month = lubridate::month(date),    # Monthly aggregation grouping 
    quarter = lubridate::quarter(date) # Quarterly aggregation grouping
  )]
  
  return(rainfall_indicators)
}

#' Aggregate rainfall by yearly periods
#' @param rainfall_indicators data.table with site-day rainfall and temporal columns
#' @return data.table aggregated by site-year
aggregate_yearly <- function(rainfall_indicators) {
  logger::log_info("Aggregating rainfall by yearly periods")
  
  # Sum daily rainfall by site and year using data.table's efficient aggregation
  yearly_agg <- rainfall_indicators[, .(
    rainfall_r1_yr = sum(rainfall_r1, na.rm = TRUE),  # Annual sum of single-cell rainfall
    rainfall_r9_yr = sum(rainfall_r9, na.rm = TRUE)   # Annual sum of multi-cell maximum rainfall
  ), by = .(site_id, water_company, year)]
  
  logger::log_info("Created yearly aggregation: {nrow(yearly_agg)} site-year combinations")
  
  return(yearly_agg)
}

#' Aggregate rainfall by monthly periods  
#' @param rainfall_indicators data.table with site-day rainfall and temporal columns
#' @return data.table aggregated by site-month
aggregate_monthly <- function(rainfall_indicators) {
  logger::log_info("Aggregating rainfall by monthly periods")
  
  monthly_agg <- rainfall_indicators[, .(
    rainfall_r1_mo = sum(rainfall_r1, na.rm = TRUE),
    rainfall_r9_mo = sum(rainfall_r9, na.rm = TRUE)
  ), by = .(site_id, water_company, year, month)]
  
  logger::log_info("Created monthly aggregation: {nrow(monthly_agg)} site-month combinations")
  
  return(monthly_agg)
}

#' Aggregate rainfall by quarterly periods
#' @param rainfall_indicators data.table with site-day rainfall and temporal columns  
#' @return data.table aggregated by site-quarter
aggregate_quarterly <- function(rainfall_indicators) {
  logger::log_info("Aggregating rainfall by quarterly periods")
  
  quarterly_agg <- rainfall_indicators[, .(
    rainfall_r1_qt = sum(rainfall_r1, na.rm = TRUE),
    rainfall_r9_qt = sum(rainfall_r9, na.rm = TRUE)
  ), by = .(site_id, water_company, year, quarter)]
  
  logger::log_info("Created quarterly aggregation: {nrow(quarterly_agg)} site-quarter combinations")
  
  return(quarterly_agg)
}

#' Aggregate rainfall data by all temporal periods
#' @param rainfall_indicators data.table with site-day rainfall data
#' @return List with yearly, monthly, and quarterly aggregations
aggregate_by_periods <- function(rainfall_indicators) {
  logger::log_info("Aggregating rainfall by all temporal periods")
  
  # Add temporal grouping columns to daily data
  rainfall_with_periods <- add_temporal_periods(rainfall_indicators)
  
  # Perform aggregation for each temporal scale in parallel
  # Returns structured list for consistent downstream processing
  list(
    yearly = aggregate_yearly(rainfall_with_periods),      # Annual rainfall sums for trend analysis
    monthly = aggregate_monthly(rainfall_with_periods),    # Monthly rainfall sums for seasonal patterns
    quarterly = aggregate_quarterly(rainfall_with_periods) # Quarterly rainfall sums for economic periods
  )
}

# Export Functions
############################################################

#' Export rainfall aggregations to parquet files
#' @param aggregated_results List with yearly, monthly, quarterly data.tables
#' @return NULL (invisible). Saves files as side effect
export_rainfall_aggregations <- function(aggregated_results) {
  logger::log_info("Exporting rainfall aggregations to parquet files")
  
  # Ensure output directory exists
  fs::dir_create(CONFIG$output_dir, recurse = TRUE)
  
  # Export each temporal aggregation
  periods <- c("yearly", "monthly", "quarterly")
  
  for (period in periods) {
    file_path <- file.path(CONFIG$output_dir, CONFIG$output_files[[period]])
    
    # Sort data for consistent output
    data_to_export <- aggregated_results[[period]]
    if (period == "yearly") {
      data.table::setorder(data_to_export, site_id, year)
    } else if (period == "monthly") {
      data.table::setorder(data_to_export, site_id, year, month)
    } else if (period == "quarterly") {
      data.table::setorder(data_to_export, site_id, year, quarter)
    }
    
    arrow::write_parquet(data_to_export, file_path)
    
    logger::log_info("Exported {period} aggregation: {nrow(data_to_export)} records to {basename(file_path)}")
  }
  
  logger::log_info("All rainfall aggregation exports completed successfully")
}

# Main Execution Pipeline
############################################################

#' Main execution function orchestrating the chunked rainfall aggregation pipeline
#' @return NULL. Executes full rainfall aggregation pipeline using memory-efficient chunking
main <- function() {
  # Initialize environment
  initialise_environment()
  setup_logging()
  
  logger::log_info("Starting chunked rainfall aggregation pipeline")
  logger::log_info("Memory management: {CONFIG$sites_per_chunk} sites per chunk")
  
  # Load all base data
  base_data <- load_base_data()
  
  # Split sites into chunks for memory-efficient processing
  site_chunks <- split_sites_into_chunks(base_data$sites)
  total_chunks <- length(site_chunks)
  
  logger::log_info("Processing {total_chunks} chunks sequentially for memory efficiency")
  
  # Prepare chunk datasets for processing (following established pattern)
  chunk_datasets <- lapply(site_chunks, function(chunk_sites) {
    list(chunk_sites = chunk_sites)  # Structured format for consistent processing
  })
  
  # Process each chunk sequentially using functional programming approach
  # Sequential processing prevents memory overflow from concurrent chunk operations
  chunk_results <- imap(chunk_datasets, function(chunk_data, i) {
    logger::log_info("=== Processing Chunk {i}/{total_chunks} ===")
    
    # Process complete workflow for this chunk: indicators + temporal aggregation
    result <- process_complete_chunk(
      chunk_sites = chunk_data$chunk_sites,
      rainfall_dt = base_data$rainfall,  # Full rainfall dataset (filtered within chunk)
      lookup_dt = base_data$lookup,      # Full lookup table (filtered within chunk)
      chunk_id = i,
      total_chunks = total_chunks
    )
    
    return(result)
  })
  
  # Combine results from all chunks
  aggregated_results <- combine_chunk_results(chunk_results)
  
  # Export final results
  export_rainfall_aggregations(aggregated_results)
  
  logger::log_info("Chunked rainfall aggregation pipeline completed successfully")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}