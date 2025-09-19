############################################################
# Aggregate Dry Spill Statistics by Multiple Rainfall Indicators
# Project: Sewage
# Date: 28/08/2025
# Author: Jacopo Olivieri
############################################################

#' This script aggregates sewage spills into counts and durations based on
#' independent classification by 6 different rainfall indicators.

# Set Up Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL (invisible). Loads packages and sources utilities
initialise_environment <- function() {
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs",
    "data.table", "arrow"
  )
  
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
  
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
}

#' Set up logging configuration
#' @return NULL (invisible). Creates log file and configures logger
setup_logging <- function() {
  log_path <- here::here("output", "log", "aggregate_dry_spill_stats.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  # Input/output paths
  dry_spills_path = here::here("data", "processed", "rainfall", "dry_spills.parquet"),
  output_dir = here::here("data", "processed", "agg_spill_stats"),
  
  # Rainfall threshold (mm) - official definition for dry conditions
  dry_threshold_mm = 0.25,
  
  # Years to process
  years = 2021:2023,
  base_year = 2021,
  
  # Rainfall indicators: spatial (1cell/9cell) x temporal (d01/d0123) x NA handling (strict/na_rm)
  rainfall_indicators = c(
    "rainfall_1cell_d01_na_rm",      # Site cell only, day 0-1, ignore NAs
    "rainfall_1cell_d01_strict",     # Site cell only, day 0-1, strict NA handling
    "rainfall_max_9cell_d01_na_rm",  # 9-cell max, day 0-1, ignore NAs
    "rainfall_max_9cell_d01_strict", # 9-cell max, day 0-1, strict NA handling
    "rainfall_max_9cell_d0123_na_rm",    # 9-cell max, days 0-3, ignore NAs
    "rainfall_max_9cell_d0123_strict"    # 9-cell max, days 0-3, strict NA handling
  )
)

# Functions
############################################################

#' Load spill data with rainfall metrics from parquet file
#' @return data.table containing spill events with rainfall classifications
load_spill_rainfall_data <- function() {
  logger::log_info("Loading spill data from: {basename(CONFIG$dry_spills_path)}")
  as.data.table(arrow::read_parquet(CONFIG$dry_spills_path))
}

#' Parse indicator components for standardized naming
#' @param indicator Character string of rainfall indicator name
#' @return List with spatial, temporal, and NA handling components
parse_indicator_components <- function(indicator) {
  list(
    spatial = ifelse(grepl("1cell", indicator), "r0", "r1"),     # r0=site cell, r1=9 cells
    temporal = ifelse(grepl("d0123", indicator), "d0123", "d01"), # Days included
    handling = ifelse(grepl("strict", indicator), "strict", "weak") # NA handling
  )
}

#' Create standardized column names for a given indicator and period
#' @param indicator Character string of rainfall indicator name
#' @param period Character string of temporal period (yr/mo/qt)
#' @return List with old and new column names for count and hours
create_standardized_names <- function(indicator, period) {
  components <- parse_indicator_components(indicator)
  
  list(
    count_old = paste0(indicator, "_dry_count_", period),
    hrs_old = paste0(indicator, "_dry_hrs_", period),
    count_new = paste("dry_spill_count", period, components$spatial, 
                     components$temporal, components$handling, sep = "_"),
    hrs_new = paste("dry_spill_hrs", period, components$spatial, 
                   components$temporal, components$handling, sep = "_")
  )
}

#' Classify spills as dry/wet for each rainfall indicator
#' @param spills_dt data.table with spill and rainfall data
#' @return data.table with additional boolean columns for dry classification
classify_spills_by_indicators <- function(spills_dt) {
  logger::log_info("Classifying spills by {length(CONFIG$rainfall_indicators)} rainfall indicators")
  
  # Classify each indicator
  for (indicator in CONFIG$rainfall_indicators) {
    dry_col <- paste0(indicator, "_is_dry")
    # Spill is dry if rainfall exists and is below threshold
    spills_dt[, (dry_col) := !is.na(get(indicator)) & get(indicator) < CONFIG$dry_threshold_mm]
  }
  
  # Log summary statistics after classification
  dry_summary <- sapply(paste0(CONFIG$rainfall_indicators, "_is_dry"), 
                        function(col) sum(spills_dt[[col]], na.rm = TRUE))
  logger::log_debug("Dry spill counts: {paste(names(dry_summary), dry_summary, sep=':', collapse=', ')}")
  
  return(spills_dt)
}

#' Generic temporal aggregation function
#' @param dt data.table to aggregate
#' @param grouping_vars Character vector of grouping columns
#' @param count_col Name for output count column
#' @param hrs_col Name for output hours column
#' @return data.table with aggregated counts and hours
aggregate_temporal <- function(dt, grouping_vars, count_col, hrs_col) {
  result <- dt[,
    .(
      count_val = count_spills(start_time, end_time),
      hrs_val = calculate_spill_hours(start_time, end_time)
    ),
    by = grouping_vars
  ]
  
  setnames(result, c("count_val", "hrs_val"), c(count_col, hrs_col))
  result
}

#' Aggregate dry spills for a single indicator across all temporal periods
#' @param indicator Character string of rainfall indicator name
#' @param spills_dt data.table with classified spills
#' @return List with yearly, monthly, and quarterly aggregations
aggregate_indicator_spills <- function(indicator, spills_dt) {
  logger::log_info("Aggregating dry spills for indicator: {indicator}")
  
  # Filter to dry spills for this indicator
  dry_col <- paste0(indicator, "_is_dry")
  dry_spills <- spills_dt[get(dry_col) == TRUE, 
                          .(site_id, year, water_company, start_time, end_time)]
  
  # Handle case of no dry spills
  if (nrow(dry_spills) == 0) {
    logger::log_warn("No dry spills found for indicator {indicator}")
    return(list(yearly = data.table(), monthly = data.table(), quarterly = data.table()))
  }
  
  # Prepare data for temporal aggregation (adds month/quarter columns)
  prepared_data <- prepare_spill_data(dry_spills)
  
  # Define aggregation parameters for each temporal period
  aggregations <- list(
    yearly = list(
      data = prepared_data$yearly,
      grouping = c("water_company", "site_id", "year"),
      period = "yr"
    ),
    monthly = list(
      data = prepared_data$monthly,
      grouping = c("water_company", "site_id", "month_id"),
      period = "mo"
    ),
    quarterly = list(
      data = prepared_data$monthly,  # Uses monthly data with quarter column
      grouping = c("water_company", "site_id", "qtr_id"),
      period = "qt"
    )
  )
  
  # Perform aggregations with standardized naming
  results <- map(aggregations, function(agg) {
    names <- create_standardized_names(indicator, agg$period)
    result <- aggregate_temporal(agg$data, agg$grouping, names$count_old, names$hrs_old)
    
    # Apply standardized column names
    if (nrow(result) > 0) {
      setnames(result, names$count_old, names$count_new)
      setnames(result, names$hrs_old, names$hrs_new)
    }
    result
  })
  
  return(results)
}

#' Aggregate all indicators and combine into consolidated datasets
#' @param spills_dt data.table with classified spills
#' @return List with yearly, monthly, and quarterly aggregated data
aggregate_all_indicators <- function(spills_dt) {
  logger::log_info("Aggregating dry spills across all indicators")
  
  # Aggregate each indicator independently
  all_results <- map(CONFIG$rainfall_indicators, 
                    ~aggregate_indicator_spills(., spills_dt))
  names(all_results) <- CONFIG$rainfall_indicators
  
  # Combine results for each temporal period
  combine_results <- function(period) {
    results <- map(all_results, ~ .[[period]])
    
    # Use data.table merge instead of dplyr full_join
    reduce(results, function(x, y) {
      if (nrow(x) == 0) return(y)
      if (nrow(y) == 0) return(x)
      
      # Define join keys based on temporal period
      by_cols <- switch(period,
        yearly = c("water_company", "site_id", "year"),
        monthly = c("water_company", "site_id", "month_id"),
        quarterly = c("water_company", "site_id", "qtr_id")
      )
      
      # Merge using data.table for consistency and performance
      merge(x, y, by = by_cols, all = TRUE)
    })
  }
  
  list(
    yearly = combine_results("yearly"),
    monthly = combine_results("monthly"),
    quarterly = combine_results("quarterly")
  )
}

#' Load and integrate dry spill statistics with main aggregation data
#' @param dry_spill_results List with aggregated dry spill data by period
#' @return List with integrated datasets for each temporal period
integrate_with_main_aggregations <- function(dry_spill_results) {
  logger::log_info("Loading main aggregation data for integration")
  
  # Define file paths for existing aggregations
  files <- list(
    yearly = file.path(CONFIG$output_dir, "agg_spill_yr.parquet"),
    monthly = file.path(CONFIG$output_dir, "agg_spill_mo.parquet"),
    quarterly = file.path(CONFIG$output_dir, "agg_spill_qtr.parquet")
  )
  
  # Check file availability
  if (!all(file.exists(unlist(files)))) {
    logger::log_warn("Main aggregation files not found. Creating dry spill only outputs.")
    return(dry_spill_results)
  }
  
  # Process each temporal period
  integrated <- map2(files, dry_spill_results, function(file_path, dry_data) {
    # Load existing aggregation
    main_data <- as.data.table(arrow::read_parquet(file_path))
    
    # Add flag to track dry data matches (for proper zero imputation)
    dry_data <- copy(dry_data)
    dry_data[, .has_dry_match := TRUE]
    
    # Determine join keys (exclude dry_spill columns and flag)
    potential_keys <- intersect(names(main_data), names(dry_data))
    join_keys <- setdiff(potential_keys, c(grep("^dry_spill_", names(dry_data), value = TRUE), ".has_dry_match"))
    
    # Left join: keep all main records, add dry spill data where available
    combined <- merge(main_data, dry_data, 
                     by = join_keys, 
                     all.x = TRUE,  
                     all.y = FALSE)
    
    # Calculate match statistics BEFORE removing flag
    n_matched <- sum(!is.na(combined$.has_dry_match), na.rm = TRUE)
    
    # Set dry spill values to 0 for unmatched records (no dry spills = 0, not NA)
    dry_cols <- grep("^dry_spill_", names(combined), value = TRUE)
    combined[is.na(.has_dry_match), (dry_cols) := 0]
    
    # Remove the flag column
    combined[, .has_dry_match := NULL]
    
    # Log merge statistics
    logger::log_debug("Merged {nrow(main_data)} main records with {nrow(dry_data)} dry records -> {nrow(combined)} total records ({n_matched} matched)")
    
    return(combined)
  })
  
  logger::log_info("Integration completed successfully")
  return(integrated)
}

#' Export integrated results to parquet files
#' @param integrated_results List with integrated data for each period
#' @return NULL (invisible). Saves files as side effect
export_integrated_results <- function(integrated_results) {
  logger::log_info("Exporting integrated results")
  
  # Ensure output directory exists
  fs::dir_create(CONFIG$output_dir, recurse = TRUE)
  
  # Define output file names
  output_files <- list(
    yearly = "agg_spill_dry_yr.parquet",
    monthly = "agg_spill_dry_mo.parquet",
    quarterly = "agg_spill_dry_qtr.parquet"
  )
  
  # Export each temporal period
  iwalk(output_files, function(filename, period) {
    file_path <- file.path(CONFIG$output_dir, filename)
    arrow::write_parquet(integrated_results[[period]], file_path)
    logger::log_info("{period} data: {nrow(integrated_results[[period]])} records, {ncol(integrated_results[[period]])} columns")
  })
  
  logger::log_info("Export completed successfully")
}

# Main Execution
############################################################

#' Main execution function orchestrating the pipeline
#' @return NULL. Executes full dry spill aggregation pipeline
main <- function() {
  # Initialize environment
  initialise_environment()
  setup_logging()
  
  logger::log_info("Starting integrated dry spill aggregation pipeline")
  
  # Load and classify spill data
  spills_data <- load_spill_rainfall_data()
  classified_spills <- classify_spills_by_indicators(spills_data)
  
  # Aggregate dry spills by indicator and temporal period
  dry_spill_results <- aggregate_all_indicators(classified_spills)
  
  # Integrate with existing aggregations (adds zeros for non-dry periods)
  integrated_results <- integrate_with_main_aggregations(dry_spill_results)
  
  # Export final results
  export_integrated_results(integrated_results)
  
  logger::log_info("Pipeline completed successfully")
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
