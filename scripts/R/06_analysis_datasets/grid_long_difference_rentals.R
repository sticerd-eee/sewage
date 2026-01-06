############################################################
# Long Difference Grid Dataset (Rentals)
# Project: Sewage
# Date: 05/01/2026
# Author: Jacopo Olivieri
############################################################

#' This script creates a 250m x 250m grid-level dataset for long-difference
#' analysis of advertised rental prices and sewage spill exposure. Each row represents
#' a grid cell x year observation (2021, 2022, 2023).


# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {

  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  required_packages <- c(
    "arrow",
    "rio",
    "data.table",
    "here",
    "logger",
    "glue",
    "lubridate"
  )

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
  log_path <- here::here("output", "log", "long_difference_grid_rentals.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  log_appender(appender_file(log_path))
  log_layout(layout_glue_colors)
  log_threshold(INFO)
  log_info("Script started at {Sys.time()}")
}


# Configuration
############################################################

CONFIG <- list(
  # Input paths
  rental_data_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  spill_lookup_path = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
  agg_spill_yr_path = here::here("data", "processed", "agg_spill_stats", "agg_spill_yr.parquet"),


  # Output paths
  output_dir = here::here("data", "processed", "long_difference"),
  output_path = here::here(
    "data", "processed", "long_difference", "long_diff_grid_rentals.parquet"
  ),

  # Parameters
  grid_size = 250L,
  spill_radius = 250L,
  years = c(2021L, 2022L, 2023L),

  # England bounding box (CRS 27700)
  bbox = list(
    xmin = 82000,
    xmax = 656000,
    ymin = 5000,
    ymax = 658000
  )
)


# Data Loading Functions
############################################################

#' Load all required datasets
#' @return List containing rental_dt, spill_lookup_dt, agg_spill_dt
load_data <- function() {
  log_info("Loading datasets")

  # Load rental listing data
  if (!file.exists(CONFIG$rental_data_path)) {
    log_error("Rental listing file not found: {CONFIG$rental_data_path}")
    stop(glue("Missing input file: {CONFIG$rental_data_path}"))
  }

  rental_dt <- tryCatch(
    {
      dt <- rio::import(CONFIG$rental_data_path, trust = TRUE)
      as.data.table(dt)
    },
    error = function(e) {
      log_error("Failed to load rental listing data: {e$message}")
      stop(e)
    }
  )
  log_info("Loaded {nrow(rental_dt)} rental listing records")

  # Load spill lookup (filter to radius)
  if (!file.exists(CONFIG$spill_lookup_path)) {
    log_error("Rental spill lookup file not found: {CONFIG$spill_lookup_path}")
    stop(glue("Missing input file: {CONFIG$spill_lookup_path}"))
  }

  spill_lookup_dt <- tryCatch(
    {
      dt <- arrow::read_parquet(CONFIG$spill_lookup_path)
      dt <- as.data.table(dt)
      dt[distance_m <= CONFIG$spill_radius, .(rental_id, site_id, distance_m)]
    },
    error = function(e) {
      log_error("Failed to load rental spill lookup: {e$message}")
      stop(e)
    }
  )
  log_info("Loaded {nrow(spill_lookup_dt)} rental-site pairs within {CONFIG$spill_radius}m")

  # Load annual spill statistics
  if (!file.exists(CONFIG$agg_spill_yr_path)) {
    log_error("Annual spill file not found: {CONFIG$agg_spill_yr_path}")
    stop(glue("Missing input file: {CONFIG$agg_spill_yr_path}"))
  }

  agg_spill_dt <- tryCatch(
    {
      dt <- rio::import(CONFIG$agg_spill_yr_path, trust = TRUE)
      dt <- as.data.table(dt)
      dt[, .(site_id, year, spill_count_yr, spill_hrs_yr)]
    },
    error = function(e) {
      log_error("Failed to load annual spill data: {e$message}")
      stop(e)
    }
  )
  log_info("Loaded {nrow(agg_spill_dt)} site-year spill records")

  list(
    rental_dt = rental_dt,
    spill_lookup_dt = spill_lookup_dt,
    agg_spill_dt = agg_spill_dt
  )
}


# Data Processing Functions
############################################################

#' Prepare rental data: filter years, create has_nearby_site flag
#' @param rental_dt data.table of rentals
#' @param spill_lookup_dt data.table of rental-site pairs
#' @return data.table with prepared rental data
prepare_rental_data <- function(rental_dt, spill_lookup_dt) {
  log_info("Preparing rental data")

  # Filter to relevant years
  rental_dt[, year := year(rented_est)]
  rental_dt <- rental_dt[year %in% CONFIG$years]
  log_info("Filtered to {nrow(rental_dt)} rentals in years {paste(CONFIG$years, collapse = ', ')}")

  # Create flag for rentals near spill sites
  rentals_near_sites <- unique(spill_lookup_dt$rental_id)
  rental_dt[, has_nearby_site := rental_id %in% rentals_near_sites]

  n_near <- rental_dt[has_nearby_site == TRUE, .N]
  pct_near <- round(100 * n_near / nrow(rental_dt), 1)
  log_info("Rentals with nearby sites: {n_near} ({pct_near}%)")

  rental_dt
}

#' Assign grid cells to rentals
#' @param rental_dt data.table with easting/northing
#' @return data.table with grid cell assignments
assign_grid_cells <- function(rental_dt) {
  log_info("Assigning grid cells")

  # Compute grid cell SW corner
  rental_dt[, cell_easting := floor(easting / CONFIG$grid_size) * CONFIG$grid_size]
  rental_dt[, cell_northing := floor(northing / CONFIG$grid_size) * CONFIG$grid_size]

  # Create grid cell ID
  rental_dt[, grid_cell_id := paste0("E", cell_easting, "_N", cell_northing)]

  # Compute log price
  rental_dt[, log_price := log(listing_price)]

  # Filter to England bounding box
  n_before <- nrow(rental_dt)
  rental_dt <- rental_dt[
    easting >= CONFIG$bbox$xmin & easting <= CONFIG$bbox$xmax &
      northing >= CONFIG$bbox$ymin & northing <= CONFIG$bbox$ymax
  ]
  n_after <- nrow(rental_dt)

  if (n_before > n_after) {
    log_info("Filtered {n_before - n_after} rentals outside England bbox")
  }

  log_info("Assigned {nrow(rental_dt)} rentals to {uniqueN(rental_dt$grid_cell_id)} grid cells")

  rental_dt
}

#' Compute rental-level spill exposure by calendar year
#' @param rental_dt data.table with rental data
#' @param spill_lookup_dt data.table with rental-site pairs
#' @param agg_spill_dt data.table with annual spill stats
#' @return data.table with rental-year spill exposure
compute_rental_spill_exposure <- function(rental_dt, spill_lookup_dt, agg_spill_dt) {
  log_info("Computing rental-level spill exposure")

  # Get unique rental-year combinations
  rental_years <- unique(rental_dt[, .(rental_id, year, has_nearby_site)])

  # Rentals WITH nearby sites: compute spill exposure
  rental_near <- rental_years[has_nearby_site == TRUE]

  if (nrow(rental_near) > 0) {
    # Join to spill lookup
    rental_near <- merge(
      rental_near,
      spill_lookup_dt,
      by = "rental_id",
      allow.cartesian = TRUE
    )

    # Join to annual spill stats
    rental_near <- merge(
      rental_near,
      agg_spill_dt,
      by = c("site_id", "year"),
      all.x = TRUE
    )

    # Aggregate: sum spills across all sites within radius for each rental-year
    rental_spill_near <- rental_near[, .(
      spill_count = sum(spill_count_yr),
      spill_hrs = sum(spill_hrs_yr),
      n_sites = uniqueN(site_id),
      min_distance = min(distance_m, na.rm = TRUE)
    ), by = .(rental_id, year)]

    log_info("Computed spill exposure for {nrow(rental_spill_near)} rental-year pairs with nearby sites")
  } else {
    rental_spill_near <- data.table(
      rental_id = integer(),
      year = integer(),
      spill_count = numeric(),
      spill_hrs = numeric(),
      n_sites = integer(),
      min_distance = numeric()
    )
  }

  # Rentals WITHOUT nearby sites: spill exposure is NA
  rental_far <- rental_years[has_nearby_site == FALSE, .(rental_id, year)]
  if (nrow(rental_far) > 0) {
    rental_far[, `:=`(
      spill_count = NA_real_,
      spill_hrs = NA_real_,
      n_sites = 0L,
      min_distance = NA_real_
    )]
    log_info("Set NA spill exposure for {nrow(rental_far)} rental-year pairs without nearby sites")
  }

  # Combine
  rental_spill <- rbindlist(list(rental_spill_near, rental_far), use.names = TRUE)

  rental_spill
}

#' Aggregate rental-level data to grid-cell x year
#' @param rental_dt data.table with rental data and grid assignments
#' @param rental_spill_dt data.table with rental-year spill exposure
#' @return data.table aggregated by grid_cell_id and year
aggregate_to_grid_year <- function(rental_dt, rental_spill_dt) {
  log_info("Aggregating to grid-cell x year")

  # Join spill exposure to rental data
  rental_dt <- merge(
    rental_dt,
    rental_spill_dt,
    by = c("rental_id", "year"),
    all.x = TRUE
  )

  # Aggregate by grid cell and year
  cell_year <- rental_dt[, .(
    # Price measures
    mean_log_price = mean(log_price, na.rm = TRUE),
    median_log_price = median(log_price, na.rm = TRUE),
    sd_log_price = sd(log_price, na.rm = TRUE),

    # Spill exposure measures (unconditional mean across all transactions)
    mean_spill_count = mean(ifelse(n_sites == 0, 0, spill_count)),
    mean_spill_hrs = mean(ifelse(n_sites == 0, 0, spill_hrs)),

    # Transaction counts
    n_transactions = .N,
    n_exposed_transactions = sum(n_sites > 0, na.rm = TRUE),

    # Composition controls
    pct_detached = mean(property_type == "D", na.rm = TRUE),
    pct_semi = mean(property_type == "S", na.rm = TRUE),
    pct_terraced = mean(property_type == "T", na.rm = TRUE),
    pct_flat = mean(property_type == "F", na.rm = TRUE),
    pct_new_build = NA_real_,
    pct_freehold = NA_real_,

    # Grid cell coordinates (for centroid calculation)
    cell_easting = first(cell_easting),
    cell_northing = first(cell_northing)
  ), by = .(grid_cell_id, year)]

  log_info("Created {nrow(cell_year)} grid-cell x year observations")

  cell_year
}

#' Finalize dataset: add centroids, handle NaN values
#' @param cell_year data.table aggregated by grid_cell_id and year
#' @return data.table with finalized dataset
finalize_dataset <- function(cell_year) {
  log_info("Finalizing dataset")

  # Add fixed centroids (SW corner + half grid size)
  cell_year[, centroid_easting := cell_easting + CONFIG$grid_size / 2]
  cell_year[, centroid_northing := cell_northing + CONFIG$grid_size / 2]

  # Handle NaN from mean() when all values are NA
  cell_year[is.nan(mean_spill_count), mean_spill_count := NA_real_]
  cell_year[is.nan(mean_spill_hrs), mean_spill_hrs := NA_real_]

  # Reorder columns
  setcolorder(cell_year, c(
    "grid_cell_id", "year",
    "centroid_easting", "centroid_northing",
    "mean_log_price", "median_log_price", "sd_log_price",
    "mean_spill_count", "mean_spill_hrs",
    "n_transactions", "n_exposed_transactions",
    "pct_detached", "pct_semi", "pct_terraced", "pct_flat",
    "pct_new_build", "pct_freehold",
    "cell_easting", "cell_northing"
  ))

  # Sort by grid cell and year
  setorder(cell_year, grid_cell_id, year)

  log_info("Finalized dataset with {nrow(cell_year)} rows and {ncol(cell_year)} columns")

  cell_year
}


# Validation Functions
############################################################

#' Run validation checks and log results
#' @param dt data.table with final dataset
#' @return NULL
run_validation_checks <- function(dt) {
  log_info("========== VALIDATION CHECKS ==========")

  # 1. Coverage by year
  log_info("1. Coverage by year:")
  coverage <- dt[, .N, by = year][order(year)]
  for (i in seq_len(nrow(coverage))) {
    log_info("   {coverage$year[i]}: {coverage$N[i]} grid cells")
  }

  # 2. Unique grid cells
  n_unique_cells <- uniqueN(dt$grid_cell_id)
  log_info("2. Unique grid cells: {n_unique_cells}")

  # 3. Missing spill exposure coverage
  log_info("3. Missing spill exposure by year:")
  missing_share <- dt[, .(
    missing_share = mean(is.na(mean_spill_count)),
    n_missing = sum(is.na(mean_spill_count)),
    n_valid = sum(!is.na(mean_spill_count))
  ), by = year][order(year)]
  for (i in seq_len(nrow(missing_share))) {
    log_info("   {missing_share$year[i]}: {round(100 * missing_share$missing_share[i], 1)}% missing ({missing_share$n_missing[i]} cells)")
  }

  # 4. Consistency check: zero exposure should align with n_exposed_transactions == 0
  inconsistent <- dt[n_exposed_transactions == 0 & mean_spill_count != 0, .N]
  if (inconsistent > 0) {
    log_warn("4. Zero exposure check failed: {inconsistent} rows")
  } else {
    log_info("4. Zero exposure check: PASSED")
  }

  # 5. Transaction distribution
  log_info("5. Transaction distribution by year:")
  tx_stats <- dt[, .(
    mean = round(mean(n_transactions), 1),
    median = as.double(median(n_transactions)),
    max = max(n_transactions)
  ), by = year][order(year)]
  for (i in seq_len(nrow(tx_stats))) {
    log_info("   {tx_stats$year[i]}: mean={tx_stats$mean[i]}, median={tx_stats$median[i]}, max={tx_stats$max[i]}")
  }

  # 6. Coordinate range
  log_info("6. Coordinate ranges:")
  log_info("   Easting: [{min(dt$centroid_easting)}, {max(dt$centroid_easting)}]")
  log_info("   Northing: [{min(dt$centroid_northing)}, {max(dt$centroid_northing)}]")

  # 7. Price and spill summary (non-NA values)
  log_info("7. Variable summaries (non-NA):")
  log_info("   mean_log_price: mean={round(mean(dt$mean_log_price, na.rm = TRUE), 3)}, sd={round(sd(dt$mean_log_price, na.rm = TRUE), 3)}")

  valid_spill <- dt[!is.na(mean_spill_count)]
  if (nrow(valid_spill) > 0) {
    log_info("   mean_spill_count: mean={round(mean(valid_spill$mean_spill_count), 2)}, sd={round(sd(valid_spill$mean_spill_count), 2)}")
  }

  log_info("========================================")
}


# Export Functions
############################################################

#' Export dataset to parquet
#' @param dt data.table to export
#' @return NULL
export_data <- function(dt) {
  log_info("Exporting dataset")

  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  tryCatch(
    {
      setorder(dt, grid_cell_id, year)
      arrow::write_parquet(dt, CONFIG$output_path)
      log_info("Exported {nrow(dt)} rows to {CONFIG$output_path}")
    },
    error = function(e) {
      log_error("Failed to export data: {e$message}")
      stop(e)
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

  tryCatch(
    {
      # Load data
      data <- load_data()

      # Prepare rental data
      rental_dt <- prepare_rental_data(data$rental_dt, data$spill_lookup_dt)

      # Assign grid cells
      rental_dt <- assign_grid_cells(rental_dt)

      # Compute rental-level spill exposure
      rental_spill_dt <- compute_rental_spill_exposure(
        rental_dt,
        data$spill_lookup_dt,
        data$agg_spill_dt
      )

      # Aggregate to grid-cell x year
      cell_year_dt <- aggregate_to_grid_year(rental_dt, rental_spill_dt)

      # Finalize dataset
      cell_year_dt <- finalize_dataset(cell_year_dt)

      # Run validation checks
      run_validation_checks(cell_year_dt)

      # Export data
      export_data(cell_year_dt)

      log_info("Script completed successfully at {Sys.time()}")
    },
    error = function(e) {
      log_error("Fatal error: {e$message}")
      stop(e)
    }
  )
}


# Execute when run directly
if (sys.nframe() == 0) {
  main()
}
