############################################################
# Aggregate Daily Spill and Rainfall Panel
# Project: Sewage
# Date: 04/03/2026
# Author: Jacopo Olivieri
############################################################

#' This script creates a complete balanced site-day panel combining daily spill
#' counts/hours with daily rainfall indicators for all spill sites over 2021-2023.
#' Enables day-level analysis of the relationship between rainfall and spill activity.

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

  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))

  # Source shared utilities for spill aggregation and rainfall indicators.
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
}

#' Set up logging configuration
#' @return NULL (invisible). Creates log file and configures logger
setup_logging <- function() {
  log_path <- here::here("output", "log", "aggregate_daily_spill_rainfall.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  # Input file paths
  spill_events_path = here::here("data", "processed", "matched_events_annual_data", "matched_events_annual_data.parquet"),
  spill_sites_path  = here::here("data", "processed", "unique_spill_sites.parquet"),
  rainfall_path     = here::here("data", "processed", "rainfall", "rainfall_data_cleaned.parquet"),
  lookup_path       = here::here("data", "processed", "rainfall", "spill_site_grid_lookup.parquet"),

  # Output
  output_dir  = here::here("data", "processed", "agg_spill_stats"),
  output_file = "agg_spill_daily.parquet",

  # Temporal range
  start_date = as.Date("2021-01-01"),
  end_date   = as.Date("2023-12-31"),
  study_years = 2021:2023,
  rainfall_lookback_days = 3L,

  # Memory management
  sites_per_chunk = 500
)

# Data Loading Functions
############################################################

#' Load unique spill sites with availability flags
#' @return data.table with site_id, water_company, ngr, available_year_* columns
load_spill_sites <- function() {
  logger::log_info("Loading spill sites from: {basename(CONFIG$spill_sites_path)}")
  sites_dt <- arrow::read_parquet(CONFIG$spill_sites_path) |>
    select(site_id, water_company, ngr,
           available_year_2021, available_year_2022, available_year_2023) |>
    as.data.table()

  logger::log_info("Loaded {nrow(sites_dt)} unique spill sites")
  return(sites_dt)
}

#' Load spill events filtered to study period
#' @return data.table with site_id, water_company, start_time, end_time, year
load_spill_events <- function() {
  logger::log_info("Loading spill events from: {basename(CONFIG$spill_events_path)}")
  events_dt <- arrow::read_parquet(CONFIG$spill_events_path) |>
    select(site_id, water_company, start_time, end_time, year) |>
    as.data.table()

  events_dt <- events_dt[year %in% CONFIG$study_years]
  events_dt <- events_dt[!is.na(site_id) & !is.na(start_time)]

  logger::log_info("Loaded {nrow(events_dt)} spill events for {paste(CONFIG$study_years, collapse = ', ')}")
  return(events_dt)
}

#' Load cleaned rainfall data
#' @return data.table with x_idx, y_idx, date, rainfall columns
load_rainfall_data <- function() {
  logger::log_info("Loading rainfall data from: {basename(CONFIG$rainfall_path)}")
  rainfall_dt <- arrow::read_parquet(CONFIG$rainfall_path) |>
    as.data.table()

  rainfall_dt <- rainfall_dt[
    date >= (CONFIG$start_date - CONFIG$rainfall_lookback_days) &
      date <= CONFIG$end_date
  ]

  logger::log_info("Loaded rainfall data: {nrow(rainfall_dt)} records")
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

# Processing Functions
############################################################

#' Clamp spill events to year boundaries and split into daily records
#' @param events_dt data.table of spill events
#' @return data.table with daily spill records aggregated by site_id and date
aggregate_daily_spills <- function(events_dt) {
  if (nrow(events_dt) == 0) {
    return(data.table(site_id = integer(), date = as.Date(character()),
                      spill_count = integer(), spill_hrs = numeric()))
  }

  dt <- copy(events_dt)

  # Clamp events to year boundaries (same as prepare_spill_data() in utils)
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

  # Split into daily records
  daily_dt <- split_daily_records(dt)

  # Aggregate by site_id and date
  daily_agg <- daily_dt[, .(
    spill_count = 1L,
    spill_hrs = calculate_spill_hours(start_time, end_time)
  ), by = .(site_id, date)]

  return(daily_agg)
}

#' Calculate daily rainfall indicators for a chunk of sites
#' Reuses pattern from aggregate_rainfall_stats.R
#' @param chunk_sites data.table with sites in current chunk
#' @param rainfall_dt data.table with rainfall data
#' @param lookup_dt data.table with site-grid mappings
#' @return data.table with site_id, date, and daily-panel rainfall indicator columns
calculate_chunk_rainfall <- function(chunk_sites, rainfall_dt, lookup_dt) {
  chunk_ngrs <- chunk_sites$ngr
  chunk_lookup <- lookup_dt[ngr %in% chunk_ngrs]

  chunk_grid_cells <- unique(chunk_lookup[, .(x_idx, y_idx)])
  chunk_rainfall <- rainfall_dt[chunk_grid_cells, on = c("x_idx", "y_idx"), nomatch = 0]

  # Create complete site-day grid for this chunk
  date_seq <- seq(from = CONFIG$start_date, to = CONFIG$end_date, by = "day")
  chunk_grid <- data.table::CJ(
    site_id = chunk_sites$site_id,
    date = date_seq,
    sorted = FALSE
  )
  chunk_grid <- chunk_sites[, .(site_id, ngr)][chunk_grid, on = "site_id"]
  chunk_grid <- add_standard_rainfall_offsets(chunk_grid, "date")

  # Expand site-day observations across the shared time offsets and lookup cells.
  rainfall_long <- data.table::melt(
    chunk_grid,
    id.vars = c("site_id", "date", "ngr"),
    measure.vars = get_standard_rainfall_offset_cols(),
    variable.name = "time_offset",
    value.name = "rainfall_date"
  )
  expanded_grid <- rainfall_long[chunk_lookup, on = "ngr", allow.cartesian = TRUE]

  # Join with rainfall observations for the required cell-date combinations.
  rainfall_joined <- expanded_grid[chunk_rainfall,
                                   on = c("x_idx", "y_idx", "rainfall_date" = "date"),
                                   nomatch = 0]

  calculate_standard_rainfall_indicators(
    rainfall_joined,
    by_cols = c("site_id", "date"),
    include_same_day_max_9cell_na_rm = TRUE
  )
}

#' Build per-year missingness flags for a chunk of sites
#' @param chunk_sites data.table with sites including available_year_* columns
#' @return data.table with site_id, year, site_missing (long format)
build_missingness_flags <- function(chunk_sites) {
  avail_cols <- paste0("available_year_", CONFIG$study_years)

  # Melt available_year columns to long format
  flags_long <- melt(
    chunk_sites[, c("site_id", avail_cols), with = FALSE],
    id.vars = "site_id",
    variable.name = "avail_col",
    value.name = "available"
  )

  # Extract year from column name and derive missing flag
  flags_long[, year := as.integer(sub("available_year_", "", avail_col))]
  flags_long[, available := as.logical(available)]
  flags_long[is.na(available), available := FALSE]
  flags_long[, site_missing := !available]

  return(flags_long[, .(site_id, year, site_missing)])
}

#' Process a single chunk of sites
#' @param chunk_sites data.table with sites for this chunk
#' @param events_dt data.table with all spill events
#' @param rainfall_dt data.table with all rainfall data
#' @param lookup_dt data.table with site-grid mappings
#' @param chunk_id integer chunk identifier for logging
#' @param total_chunks integer total number of chunks
#' @return data.table with complete site-day panel for this chunk
process_chunk <- function(chunk_sites, events_dt, rainfall_dt, lookup_dt,
                          chunk_id, total_chunks) {
  logger::log_info("Processing chunk {chunk_id}/{total_chunks}: {nrow(chunk_sites)} sites")

  tryCatch({
    # 1. Create complete site-day grid
    date_seq <- seq(from = CONFIG$start_date, to = CONFIG$end_date, by = "day")
    grid <- data.table::CJ(
      site_id = chunk_sites$site_id,
      date = date_seq,
      sorted = FALSE
    )
    grid[, year := data.table::year(date)]

    # 2. Join site metadata (water_company)
    grid <- chunk_sites[, .(site_id, water_company)][grid, on = "site_id"]

    # 3. Compute daily spill aggregates
    chunk_events <- events_dt[site_id %in% chunk_sites$site_id]
    daily_spills <- aggregate_daily_spills(chunk_events)

    # 4. Compute daily rainfall indicators
    daily_rainfall <- calculate_chunk_rainfall(chunk_sites, rainfall_dt, lookup_dt)

    # 5. Build missingness flags
    miss_flags <- build_missingness_flags(chunk_sites)

    # 6. Left-join everything onto the grid
    grid <- daily_spills[grid, on = .(site_id, date)]
    grid <- daily_rainfall[grid, on = .(site_id, date)]
    grid <- miss_flags[grid, on = .(site_id, year)]

    # 7. Apply NA conventions:
    #    - site_missing == FALSE & no spill → spill_count = 0, spill_hrs = 0
    #    - site_missing == TRUE → spill_count = NA, spill_hrs = NA
    grid[site_missing == FALSE & is.na(spill_count), `:=`(spill_count = 0L, spill_hrs = 0)]
    grid[site_missing == TRUE, `:=`(spill_count = NA_integer_, spill_hrs = NA_real_)]

    logger::log_info("Completed chunk {chunk_id}/{total_chunks}: {nrow(grid)} rows")
    return(grid)

  }, error = function(e) {
    logger::log_error("Failed to process chunk {chunk_id}: {e$message}")
    return(NULL)
  })
}

# Main Execution Pipeline
############################################################

#' Main execution function
#' @return NULL. Executes full daily spill-rainfall panel pipeline
main <- function() {
  initialise_environment()
  setup_logging()

  logger::log_info("Starting daily spill-rainfall panel pipeline")
  logger::log_info("Memory management: {CONFIG$sites_per_chunk} sites per chunk")

  # Load all base data
  sites_dt    <- load_spill_sites()
  events_dt   <- load_spill_events()
  rainfall_dt <- load_rainfall_data()
  lookup_dt   <- load_site_grid_lookup()

  # Split sites into chunks
  n_sites <- nrow(sites_dt)
  chunk_indices <- split(1:n_sites, ceiling(seq_len(n_sites) / CONFIG$sites_per_chunk))
  site_chunks <- lapply(chunk_indices, function(idx) sites_dt[idx, ])
  total_chunks <- length(site_chunks)

  logger::log_info("Processing {total_chunks} chunks sequentially")

  # Process each chunk
  chunk_results <- imap(site_chunks, function(chunk_sites, i) {
    logger::log_info("=== Processing Chunk {i}/{total_chunks} ===")
    process_chunk(
      chunk_sites  = chunk_sites,
      events_dt    = events_dt,
      rainfall_dt  = rainfall_dt,
      lookup_dt    = lookup_dt,
      chunk_id     = i,
      total_chunks = total_chunks
    )
  })

  # Combine results
  successful <- chunk_results[!sapply(chunk_results, is.null)]
  logger::log_info("Successfully processed {length(successful)}/{total_chunks} chunks")

  if (length(successful) == 0) {
    stop("All chunks failed to process")
  }

  panel <- rbindlist(successful, use.names = TRUE, fill = TRUE)

  # Select and order final columns
  panel <- panel[
    ,
    c(
      "site_id",
      "water_company",
      "date",
      "year",
      "spill_count",
      "spill_hrs",
      get_daily_panel_rainfall_indicator_cols(),
      "site_missing"
    ),
    with = FALSE
  ]
  data.table::setorder(panel, site_id, date)

  # Export
  fs::dir_create(CONFIG$output_dir, recurse = TRUE)
  output_path <- file.path(CONFIG$output_dir, CONFIG$output_file)
  arrow::write_parquet(panel, output_path)

  logger::log_info("Exported daily panel: {nrow(panel)} rows, {ncol(panel)} columns to {basename(output_path)}")
  logger::log_info("Sites: {uniqueN(panel$site_id)}, Date range: {min(panel$date)} to {max(panel$date)}")
  logger::log_info("Pipeline completed successfully")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
