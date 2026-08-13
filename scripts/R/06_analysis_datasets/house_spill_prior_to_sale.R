############################################################
# Create Prior-to-Sale Cross-sectional Database: House-Site Level
# Project: Sewage
# Date: 22/01/2026
# Author: Alina Zeltikova
############################################################

initialise_environment <- function() {
  required_packages <- c(
    "here", "logger", "glue", "fs",
    "lubridate", "arrow", "data.table", "dplyr"
  )
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
    library(pkg, character.only = TRUE)
  }))
  source(here::here("scripts", "R", "utils", "spill_aggregation_utils.R"))
  source(here::here("scripts", "R", "utils", "site_group_utils.R"))
  source(here::here("scripts", "R", "utils", "prior_exposure_utils.R"))
}

setup_logging <- function() {
  log_path <- here::here("output", "log", "house_site_prior_to_sale.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

CONFIG <- list(
  processed_dir = here::here("data", "processed"),
  radius_thresholds = c(250, 500, 1000),
  base_year = 2021,
  window_start = as.POSIXct("2021-01-01 00:00:00", tz = "UTC"),
  chunk_size = 10000,
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  )
)

load_data <- function() {
  prior_exposure_load_data(CONFIG, "sale", "site")
}

create_joined_events <- function(house_ids, data) {
  prior_exposure_join_events(house_ids, data)
}

calculate_metrics_by_radius <- function(lookup_dt, events_dt) {
  prior_exposure_calculate_metrics(
    lookup_dt, events_dt, "sale", "site", CONFIG$radius_thresholds
  )
}

get_house_metadata <- function(house_dt) {
  house_dt[, .(house_id, price, n_days_in_window)]
}

process_chunk <- function(house_ids, data) {
  prior_exposure_process_chunk(house_ids, data)
}

create_prior_to_sale_db <- function(data) {
  logger::log_info("Creating prior-to-sale cross-sectional database")
  output_path <- here::here(
    "data", "processed", "cross_section", "sales",
    "prior_to_sale_house_site"
  )
  prior_exposure_stream(data, output_path)
  logger::log_info("Prior-to-sale database created and published")
  invisible(output_path)
}

export_data <- function(data) {
  tryCatch({
    output_path <- create_prior_to_sale_db(data)
    logger::log_info("Data saved to: {output_path}")
  }, error = function(e) {
    logger::log_error("Data export failed: {e$message}")
    stop(glue::glue("Failed to export data: {e$message}"))
  })
}

main <- function() {
  initialise_environment()
  setup_logging()
  export_data(load_data())
  logger::log_info("Script completed successfully")
}

if (sys.nframe() == 0) main()
