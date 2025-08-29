############################################################
# Pre-process and Clean UK Rainfall Data
# Project: Sewage
# Date: 26/08/2025
# Author: Jacopo Olivieri
############################################################

# This script extracts daily rainfall data from UK Met Office HadUK-Grid NetCDF files
# for grid cells near sewage spill locations. The spatial filtering reduces data volume
# while preserving all rainfall information needed for spill-precipitation analysis.

# Set Up Functions
############################################################

#' Initialize Environment with Required Packages
#'
#' @return NULL (invisible). Packages are loaded into the global environment.
initialise_environment <- function() {
  # Package dependencies: NetCDF I/O, spatial data manipulation, logging, parallel processing
  required_packages <- c(
    "ncdf4", "dplyr", "purrr", "here", "arrow", "data.table",
    "logger", "stringr", "rnrfa", "fs", "lubridate", "future", "furrr"
  )
  
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}

#' Configure Logging for Data Processing Pipeline
#'
#' @return NULL (invisible). Configures global logger settings.
setup_logging <- function() {
  log_path <- here::here(
    "output", "log", "clean_rainfall_data.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  log_appender(appender_file(log_path))
  log_layout(layout_glue_colors)
  log_threshold(DEBUG)
  log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  # Input file paths
  spill_sites_path = here::here(
    "data", "processed", "unique_spill_sites.parquet"),
  rainfall_dir = here::here("data", "raw", "haduk_rainfall_data"),
  
  # Output file paths
  output_file = here::here(
    "data", "processed", "rainfall", "rainfall_data_cleaned.parquet"
  ),
  lookup_table_file = here::here(
    "data", "processed", "rainfall", "spill_site_grid_lookup.parquet"
  ),
  
  # Date range for rainfall data processing
  start_date = as.Date("2020-12-01"),
  end_date = as.Date("2023-12-31"),
  
  # Parallel processing settings optimized for M1 MacBook Pro (8 cores, 16GB RAM)
  n_cores = min(6, parallel::detectCores() - 2)  # Use 6 cores, leave 2 free for system
)

# Functions
############################################################

#' Load Sewage Spill Site Coordinates
#'
#' @param file_path Character. Path to parquet file containing spill site data.
#' @return data.table with columns: ngr, easting, northing.
load_spill_sites <- function(file_path = CONFIG$spill_sites_path) {
  
  logger::log_info("Loading spill data from: {basename(file_path)}")
  spill_sites <- arrow::read_parquet(file_path) |>
    select(ngr, easting, northing) |> 
    as.data.table()

  logger::log_info("Loaded {nrow(spill_sites)} unique NGRs")

  return(spill_sites)
}

#' Identify Required Rainfall Grid Cells
#'
#' @param spill_sites_dt data.table. Spill sites with easting/northing coordinates.
#' @param grid_bounds List. Grid boundary data from NetCDF (xbound, ybound).
#' @param radius Integer. Neighborhood radius (default: 1 for 3×3 grid).
#' @return data.table with unique x_idx, y_idx combinations.
get_required_grid_indices <- function(spill_sites_dt, grid_bounds, radius = 1) {
  log_info("Mapping {nrow(spill_sites_dt)} spill sites to grid cells (+/− {radius} neighbors)")

  # Create break vectors for spatial indexing (grid cell boundaries)
  x_breaks <- c(grid_bounds$xbound[1, ], grid_bounds$xbound[2, ncol(grid_bounds$xbound)])
  y_breaks <- c(grid_bounds$ybound[1, ], grid_bounds$ybound[2, ncol(grid_bounds$ybound)])

  # Map spill site coordinates to grid cell indices
  x_idx <- findInterval(spill_sites_dt$easting,  x_breaks, rightmost.closed = TRUE)
  y_idx <- findInterval(spill_sites_dt$northing, y_breaks, rightmost.closed = TRUE)

  # Generate neighborhood offsets (3×3 grid around each site by default)
  offsets <- data.table::CJ(dx = -radius:radius, dy = -radius:radius)

  # Expand each site to include neighboring grid cells
  all_indices <- data.table::rbindlist(
    Map(function(x, y) offsets[, .(x_idx = x + dx, y_idx = y + dy)], x_idx, y_idx)
  )

  # Remove duplicates - multiple spill sites may share grid cells
  out <- unique(all_indices)

  data.table::setorder(out, x_idx, y_idx)
  log_info("Selected {nrow(out)} unique grid cells (including neighbors)")
  return(out)
}

#' Extract Rainfall Data from NetCDF File
#'
#' @param file_path Character. Path to NetCDF rainfall file.
#' @param required_indices data.table. Grid indices to extract (x_idx, y_idx).
#' @return data.table with columns: x_idx, y_idx, date, rainfall.
process_rainfall_file <- function(file_path, required_indices) {
  # Progress tracking (logger may not work in parallel workers)
  message(paste("Processing file:", basename(file_path)))
  
  nc_data <- ncdf4::nc_open(file_path)
  on.exit(ncdf4::nc_close(nc_data))
  
  # Get metadata with proper time unit parsing
  time_vals <- ncdf4::ncvar_get(nc_data, "time")
  time_units <- ncdf4::ncatt_get(nc_data, "time", "units")$value
  
  # Parse time units (e.g., "hours since 1800-01-01 00:00:00")
  base_date_str <- stringr::str_extract(time_units, "\\d{4}-\\d{2}-\\d{2}")
  base_datetime_str <- stringr::str_extract(time_units, "\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}")
  
  if (is.na(base_datetime_str)) {
    base_datetime_str <- paste(base_date_str, "00:00:00")
  }
  
  base_datetime <- as.POSIXct(base_datetime_str, tz = "UTC")
  
  # Convert time values based on units
  if (grepl("hours since", time_units, ignore.case = TRUE)) {
    dates <- as.Date(base_datetime + lubridate::hours(time_vals))
  } else if (grepl("days since", time_units, ignore.case = TRUE)) {
    dates <- as.Date(base_datetime + lubridate::days(time_vals))
  } else if (grepl("seconds since", time_units, ignore.case = TRUE)) {
    dates <- as.Date(base_datetime + lubridate::seconds(time_vals))
  } else {
    # Fallback: assume days (original logic)
    warning("Unknown time units: ", time_units, ". Assuming days since base date.")
    dates <- as.Date(base_date_str) + time_vals
  }
  
  # Load complete rainfall array from NetCDF file
  rainfall_full <- ncdf4::ncvar_get(nc_data, "rainfall")
  
  # Prepare data structures for extraction
  n_times <- length(dates)
  n_cells <- nrow(required_indices)
  
  # Pre-allocate result vectors
  x_idx_vec <- rep(required_indices$x_idx, each = n_times)
  y_idx_vec <- rep(required_indices$y_idx, each = n_times)
  date_vec <- rep(dates, n_cells)
  rainfall_vec <- numeric(n_times * n_cells)
  
  # Extract rainfall data for each required grid cell
  for (i in seq_len(n_cells)) {
    start_idx <- (i - 1) * n_times + 1
    end_idx <- i * n_times
    rainfall_vec[start_idx:end_idx] <- rainfall_full[
      required_indices$x_idx[i], 
      required_indices$y_idx[i], 
    ]
  }
  
  # Combine into tidy format for analysis
  result <- data.table::data.table(
    x_idx = x_idx_vec,
    y_idx = y_idx_vec,
    date = date_vec,
    rainfall = rainfall_vec
  )
  
  return(result)
}

#' Generate Monthly NetCDF File Paths
#'
#' @param start_date Date. Start date for rainfall data extraction.
#' @param end_date Date. End date for rainfall data extraction.
#' @return Character vector of existing file paths.
generate_rainfall_paths <- function(start_date, end_date) {
  date_seq <- seq(from = start_date, to = end_date, by = "month")
  
  file_paths <- sapply(date_seq, function(date) {
    year <- lubridate::year(date)
    month <- lubridate::month(date)
    file.path(
      CONFIG$rainfall_dir,
      sprintf("rainfall_%d_%02d.nc", year, month)
    )
  })
  
  # Check file availability and warn about missing data
  existing_files <- file_paths[fs::file_exists(file_paths)]
  if (length(existing_files) < length(file_paths)) {
    log_warn("Missing {length(file_paths) - length(existing_files)} expected rainfall files.")
  }
  
  return(existing_files)
}

#' Process NetCDF Files in Parallel
#'
#' @param rainfall_files Character vector. Paths to NetCDF files.
#' @param required_indices data.table. Grid indices to extract.
#' @param n_cores Integer. Number of cores for parallel processing.
#' @return data.table with combined rainfall data from all files.
process_files_parallel <- function(rainfall_files, required_indices, n_cores) {
  log_info("Processing {length(rainfall_files)} files using {n_cores} cores (future framework)...")
  
  # Configure future parallelisation (optimised for M1 Mac)
  future::plan(future::multisession, workers = n_cores)
  on.exit(future::plan(future::sequential), add = TRUE)
  
  # Execute parallel file processing with error handling using furrr
  results <- furrr::future_map(rainfall_files, function(file) {
    tryCatch({
      process_rainfall_file(file, required_indices)
    }, error = function(e) {
      message(paste("Failed to process file", basename(file), ":", e$message))
      return(NULL)
    })
  }, .options = furrr::furrr_options(seed = TRUE))
  
  # Filter out failed files and combine results
  results <- results[!sapply(results, is.null)]
  
  if (length(results) == 0) {
    stop("All files failed to process")
  }
  
  # Combine all data into single table
  combined_data <- data.table::rbindlist(results, use.names = TRUE)
  
  log_info("Successfully processed {length(results)}/{length(rainfall_files)} files")
  
  return(combined_data)
}

#' Create Spill Site to Grid Cell Lookup Table
#'
#' @param spill_sites_dt data.table. Spill sites with ngr, easting, northing.
#' @param grid_bounds List. Grid boundary data from NetCDF.
#' @param radius Integer. Neighborhood radius (default: 1).
#' @return data.table with columns: ngr, easting, northing, x_idx, y_idx, is_center.
create_spill_site_lookup <- function(spill_sites_dt, grid_bounds, radius = 1) {
  log_info("Creating lookup table for {nrow(spill_sites_dt)} spill sites")
  
  # Create break vectors for spatial indexing (grid cell boundaries)
  x_breaks <- c(grid_bounds$xbound[1, ], grid_bounds$xbound[2, ncol(grid_bounds$xbound)])
  y_breaks <- c(grid_bounds$ybound[1, ], grid_bounds$ybound[2, ncol(grid_bounds$ybound)])
  
  # Map spill site coordinates to center grid cell indices
  x_idx_center <- findInterval(spill_sites_dt$easting,  x_breaks, rightmost.closed = TRUE)
  y_idx_center <- findInterval(spill_sites_dt$northing, y_breaks, rightmost.closed = TRUE)
  
  # Generate neighborhood offsets (3×3 grid around each site by default)
  offsets <- data.table::CJ(dx = -radius:radius, dy = -radius:radius)
  
  # Create lookup table with all neighborhood combinations for each spill site
  lookup_list <- lapply(seq_len(nrow(spill_sites_dt)), function(i) {
    site_data <- spill_sites_dt[i, ]
    
    # Generate all 9 grid cell combinations for this spill site
    neighborhood <- offsets[, .(
      ngr = site_data$ngr,
      easting = site_data$easting,
      northing = site_data$northing,
      x_idx = x_idx_center[i] + dx,
      y_idx = y_idx_center[i] + dy,
      is_center = (dx == 0 & dy == 0)  # TRUE only for center cell
    )]
    
    return(neighborhood)
  })
  
  # Combine all site neighborhoods into single lookup table
  lookup_table <- data.table::rbindlist(lookup_list)
  
  # Order by NGR then by grid position for consistent results
  data.table::setorder(lookup_table, ngr, x_idx, y_idx)
  
  log_info("Generated lookup table with {nrow(lookup_table)} entries ({nrow(lookup_table)/9} sites × 9 cells)")
  
  return(lookup_table)
}

# Main Workflow
############################################################

#' Main Rainfall Data Processing Pipeline
#'
#' Orchestrates complete workflow: loads spill sites, extracts grid structure,
#' processes NetCDF files in parallel, and saves cleaned data with lookup table.
#'
#' @return NULL (invisible). Creates output files as side effects.
main <- function() {
  initialise_environment()
  setup_logging()
  
  log_info("Starting rainfall data extraction pipeline")
  log_info("Using {CONFIG$n_cores} cores for parallel processing")
  
  # Load sewage spill site coordinates
  spill_sites <- load_spill_sites()
  
  # Identify available rainfall data files
  rainfall_files <- generate_rainfall_paths(CONFIG$start_date, CONFIG$end_date)
  log_info("Found {length(rainfall_files)} rainfall files to process")
  
  # Extract grid structure from sample file
  sample_nc <- nc_open(rainfall_files[1])
  grid_bounds <- list(
    xbound = ncvar_get(sample_nc, "projection_x_coordinate_bnds"),
    ybound = ncvar_get(sample_nc, "projection_y_coordinate_bnds")
  )
  nc_close(sample_nc)
  
  # Create lookup table for spill sites to grid cells
  spill_site_lookup <- create_spill_site_lookup(spill_sites, grid_bounds)
  
  # Determine which grid cells to extract based on spill locations
  required_indices <- get_required_grid_indices(spill_sites, grid_bounds)
  
  # Extract rainfall data from all files
  all_rainfall_data <- process_files_parallel(
    rainfall_files, 
    required_indices, 
    CONFIG$n_cores
  )
  
  # Save processed data and lookup table for analysis
  dir.create(dirname(CONFIG$output_file), recursive = TRUE, showWarnings = FALSE)
  
  log_info("Saving cleaned data to: {CONFIG$output_file}")
  arrow::write_parquet(all_rainfall_data, CONFIG$output_file)
  
  log_info("Saving spill site lookup table to: {CONFIG$lookup_table_file}")
  arrow::write_parquet(spill_site_lookup, CONFIG$lookup_table_file)
}

# Run pipeline when script is executed directly
if (sys.nframe() == 0) {
  main()
}