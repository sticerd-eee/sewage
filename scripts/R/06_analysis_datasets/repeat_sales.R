############################################################
# Identify Repeat Sales Transactions
# Project: Sewage
# Date: 26/12/2025
# Author: Jacopo Olivieri
############################################################

#' This script identifies repeat sales transactions (same property sold
#' multiple times) using address matching. It creates a unique repeat_id
#' for each property and generates summary statistics by distance to
#' sewage spill sites.

# Setup Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }
  required_packages <- c(
    "rio", "here", "logger", "glue", "dplyr", "stringr",
    "data.table", "arrow"
  )
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
    library(pkg, character.only = TRUE)
  }))
}

#' Set up logging configuration
#' @return NULL
setup_logging <- function() {
  log_path <- here::here("output", "log", "repeat_sales.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  log_appender(appender_file(log_path))
  log_layout(layout_glue_colors)
  log_threshold(INFO)
  log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  input_path = here::here("data", "processed", "house_price.parquet"),
  spill_lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  output_dir = here::here("data", "processed", "repeated_transactions"),
  output_path = here::here(
    "data", "processed", "repeated_transactions", "repeated_sales.parquet"
  ),
  output_summary_path = here::here(
    "data", "processed", "repeated_transactions", "repeated_sales_summary.parquet"
  )
)

# Helper Functions
############################################################

#' Basic string cleaning: uppercase, collapse whitespace, convert empty to NA
#' @param x Character vector
#' @return Cleaned character vector
clean_basic <- function(x) {
  x %>%
    str_to_upper() %>%
    str_replace_all("\\s+", " ") %>%
    str_squish() %>%
    na_if("")
}

#' Normalise postcode: uppercase and remove all whitespace
#' @param x Character vector of postcodes
#' @return Normalised postcode character vector
normalise_postcode <- function(x) {
  x %>%
    str_to_upper() %>%
    str_replace_all("\\s+", "") %>%
    na_if("")
}

# Data Processing Functions
############################################################

#' Load house price data from parquet
#' @return data.table of sales data
load_data <- function() {
  log_info("Loading sales data from {CONFIG$input_path}")
  tryCatch(
    {
      dt <- rio::import(CONFIG$input_path, trust = TRUE)
      dt <- as.data.table(dt)
      log_info("Loaded {nrow(dt)} sales records")
      return(dt)
    },
    error = function(e) {
      log_error("Failed to load sales data: {e$message}")
      stop(e)
    }
  )
}

#' Clean addresses and create matching key
#' @param dt data.table of sales data
#' @return data.table with cleaned address columns and simple_key
clean_addresses <- function(dt) {
  log_info("Cleaning addresses and creating matching keys")

  dt[, `:=`(
    date_of_transfer = as.IDate(date_of_transfer),
    price = as.numeric(price),
    postcode_clean = normalise_postcode(postcode),
    paon_clean = clean_basic(paon),
    saon_clean = clean_basic(saon),
    street_clean = clean_basic(street)
  )]

  dt[,
    simple_key := fifelse(
      !is.na(postcode_clean) & !is.na(paon_clean),
      paste(postcode_clean, paon_clean, saon_clean, street_clean, sep = "|"),
      NA_character_
    )
  ]

  # Remove keys that are effectively all NA

dt[grepl("^(NA\\|)*NA$", simple_key), simple_key := NA_character_]

  n_with_key <- dt[!is.na(simple_key), .N]
  log_info("Created matching keys for {n_with_key} records ({round(100 * n_with_key / nrow(dt), 1)}%)")

  return(dt)
}

#' Identify repeat transactions and calculate metrics
#' @param dt data.table with simple_key column
#' @return data.table with repeat transaction metrics
identify_repeat_transactions <- function(dt) {
  log_info("Identifying repeat transactions")

  setorder(dt, simple_key, date_of_transfer, transaction_id)

  dt[
    !is.na(simple_key),
    `:=`(
      sale_sequence = seq_len(.N),
      sale_count = .N,
      lag_date = shift(date_of_transfer),
      lag_price = shift(price)
    ),
    by = simple_key
  ]

  dt[
    !is.na(simple_key),
    `:=`(
      holding_period_days = as.numeric(date_of_transfer - lag_date),
      price_change = price - lag_price,
      pct_change = fifelse(
        !is.na(lag_price) & lag_price > 0,
        price / lag_price - 1,
        NA_real_
      )
    )
  ]

  n_repeat <- dt[!is.na(simple_key) & sale_count > 1, .N]
  n_properties_repeat <- dt[!is.na(simple_key) & sale_count > 1, uniqueN(simple_key)]
  log_info("Found {n_repeat} transactions from {n_properties_repeat} properties with repeat sales")

  return(dt)
}

#' Export repeat transaction IDs to parquet
#' @param dt data.table with repeat transaction data
#' @return NULL
export_repeat_ids <- function(dt) {
  log_info("Exporting repeat transaction IDs")

  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  # Prepare repeated sales (sale_count > 1)
  repeat_dt <- dt[!is.na(simple_key) & sale_count > 1]
  repeat_dt[, repeat_id := .GRP, by = simple_key]

  repeated_output <- repeat_dt[, .(house_id, repeat_id)]

  # Prepare single sales (sale_count == 1)
  max_repeat_id <- max(repeated_output$repeat_id)
  single_dt <- dt[!is.na(simple_key) & sale_count == 1]
  single_dt[, repeat_id := max_repeat_id + seq_len(.N)]

  single_output <- single_dt[, .(house_id, repeat_id)]

  # Combine
  all_output <- rbindlist(list(repeated_output, single_output))

  tryCatch(
    {
      arrow::write_parquet(all_output, CONFIG$output_path)
      log_info("Exported {nrow(all_output)} records to {CONFIG$output_path}")
    },
    error = function(e) {
      log_error("Failed to export repeat IDs: {e$message}")
      stop(e)
    }
  )
}

#' Load spill-house lookup and compute distance flags
#' @return data.table with distance flags per house_id
load_spill_lookup <- function() {
  log_info("Loading spill-house lookup from {CONFIG$spill_lookup_path}")

  tryCatch(
    {
      spill_lookup <- arrow::open_dataset(CONFIG$spill_lookup_path) |>
        dplyr::group_by(house_id) |>
        dplyr::summarise(distance_m = min(distance_m, na.rm = TRUE)) |>
        dplyr::collect()

      spill_lookup <- as.data.table(spill_lookup)

      spill_lookup[, `:=`(
        within_250m = distance_m <= 250,
        within_500m = distance_m <= 500,
        within_1000m = distance_m <= 1000,
        outside_1000m = distance_m > 1000
      )]

      log_info("Loaded distance data for {nrow(spill_lookup)} properties")
      return(spill_lookup)
    },
    error = function(e) {
      log_error("Failed to load spill lookup: {e$message}")
      stop(e)
    }
  )
}

#' Generate summary statistics by sale count and distance
#' @param spill_lookup data.table with distance flags
#' @return data.table with summary statistics
generate_summary <- function(spill_lookup) {
  log_info("Generating summary statistics")

  # Load the repeat IDs we just exported
  sales_dt <- rio::import(CONFIG$output_path, trust = TRUE)
  sales_dt <- as.data.table(sales_dt)

  # Join with spill lookup
  summary_dt <- sales_dt[
    spill_lookup,
    on = "house_id",
    nomatch = NA
  ][,
    .(
      sale_count = .N,
      within_250m = any(within_250m, na.rm = TRUE),
      within_500m = any(within_500m, na.rm = TRUE),
      within_1000m = any(within_1000m, na.rm = TRUE),
      outside_1000m = any(outside_1000m, na.rm = TRUE)
    ),
    by = repeat_id
  ][,
    .(
      n_properties = .N,
      share_within_250m = mean(within_250m, na.rm = TRUE),
      share_within_500m = mean(within_500m, na.rm = TRUE),
      share_within_1000m = mean(within_1000m, na.rm = TRUE),
      share_outside_1000m = mean(outside_1000m, na.rm = TRUE)
    ),
    by = sale_count
  ][,
    share_properties := n_properties / sum(n_properties)
  ][
    order(sale_count)
  ]

  log_info("Generated summary for {nrow(summary_dt)} sale count categories")
  return(summary_dt)
}

#' Export summary statistics to parquet
#' @param summary_dt data.table with summary statistics
#' @return NULL
export_summary <- function(summary_dt) {
  log_info("Exporting summary statistics")

  tryCatch(
    {
      arrow::write_parquet(summary_dt, CONFIG$output_summary_path)
      log_info("Exported summary to {CONFIG$output_summary_path}")
    },
    error = function(e) {
      log_error("Failed to export summary: {e$message}")
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
      # Load and process sales data
      sales_dt <- load_data()
      sales_dt <- clean_addresses(sales_dt)
      sales_dt <- identify_repeat_transactions(sales_dt)

      # Export repeat IDs
      export_repeat_ids(sales_dt)

      # Clean up sales data to free memory
      rm(sales_dt)
      gc(full = TRUE)

      # Generate and export summary
      spill_lookup <- load_spill_lookup()
      summary_dt <- generate_summary(spill_lookup)
      export_summary(summary_dt)

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
