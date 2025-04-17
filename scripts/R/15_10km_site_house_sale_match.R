############################################################
# Merge House Price Data with Nearby Spill Sites
# Project: Spatial Data Integration
# Date: 04/02/2025
# Author: Jacopo Olivieri
############################################################

# Set Up
############################################################

#' Initialise the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", "fs", "rnrfa", "sf"
  )

  # Install and load packages
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
  log_path <- here::here("output", "log", "14_10km_site_house_sale_match.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}

############################################################
# Configuration
############################################################

CONFIG <- list(
  processed_dir = here::here("data", "processed")
)

############################################################
# Functions
############################################################

#' Load spatial datasets for house prices and spill sites.
#' @return List containing house price data and spill sites data
load_data <- function() {
  logger::log_info("Loading spatial datasets")

  house_data <- rio::import(here::here("data", "processed", "house_price.rds"),
    trust = TRUE
  )
  spill_data <- rio::import(here::here("data", "processed", "unique_spill_sites.rds"),
    trust = TRUE
  )

  return(list(house = house_data, spill = spill_data))
}

#' Prepare spill sites spatial data and create a lookup table.
#' @param spill_data DataFrame of spill sites
#' @return List containing the spatial spill sites and a lookup table
prepare_spill_sites <- function(spill_data) {
  logger::log_info("Preparing spill sites spatial data")

  spill_sites_sf <- spill_data %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700) %>%
    rename(spill_geom = geometry) %>%
    select(site_id, spill_geom)

  spill_lookup <- spill_sites_sf %>%
    st_set_geometry(NULL) %>%
    mutate(spill_geom = spill_sites_sf$spill_geom) %>%
    select(site_id, spill_geom)

  return(list(spill_sf = spill_sites_sf, lookup = spill_lookup))
}

#' Prepare house price spatial data.
#' @param house_data DataFrame of house price records
#' @return sf object containing the house price data
prepare_house_data <- function(house_data) {
  logger::log_info("Preparing house price spatial data")

  houses_sf <- house_data %>%
    filter(!is.na(easting) & !is.na(northing)) %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700) %>%
    select(house_id, geometry)

  return(houses_sf)
}

#' Perform spatial join to match houses with nearby spill sites.
#' @param houses_sf sf object containing house price data
#' @param spill_sites_sf sf object containing spill sites
#' @param spill_lookup Lookup table for spill sites
#' @param radius_km Numeric radius in kilometres for matching (default: 10)
#' @return sf object with merged house and spill data, including distance calculations
perform_spatial_join <- function(houses_sf, spill_sites_sf, spill_lookup, radius_km = 10) {
  logger::log_info("Performing spatial join between houses and spill sites")

  radius_m <- radius_km * 1000
  chunk_size <- 2000

  house_chunks <- split(
    houses_sf,
    (seq_len(nrow(houses_sf)) - 1) %/% chunk_size
  )

  merged_chunks <- purrr::map(house_chunks, function(chunk) {
    logger::log_info(glue::glue("Processing chunk with {nrow(chunk)} houses"))

    st_join(chunk, spill_sites_sf,
      join = st_is_within_distance,
      dist = radius_m,
      left = TRUE
    ) %>%
      left_join(spill_lookup, by = "site_id") %>%
      mutate(
        # Calculate the straight-line distance to the spill site
        distance_m = if_else(is.na(spill_geom), NA_real_,
          as.numeric(st_distance(geometry, spill_geom, by_element = TRUE))
        ),
        distance_km = distance_m / 1000
      ) %>%
      group_by(house_id) %>%
      mutate(n_discharge_outlet = sum(!is.na(site_id))) %>%
      ungroup() %>%
      st_drop_geometry() %>%
      select(house_id, site_id, distance_m, distance_km, n_discharge_outlet)
  })

  dplyr::bind_rows(merged_chunks)
}


#' Process the spatial data by loading, preparing, and merging house price and spill site data.
#' @param radius_km Numeric radius in kilometres for the spatial join (default: 10)
#' @return sf object containing the merged data
process_spatial_data <- function(radius_km = 10) {
  datasets <- load_data()

  houses_sf <- prepare_house_data(datasets$house)
  spill_data <- prepare_spill_sites(datasets$spill)

  merged_spatial_data <- perform_spatial_join(houses_sf, spill_data$spill_sf, spill_data$lookup, radius_km)

  return(merged_spatial_data)
}

# New export function added here
export_data <- function(df) {
  # Define output paths
  rds_path <- file.path(CONFIG$processed_dir, "spill_house_lookup.rds")
  csv_path <- file.path(CONFIG$processed_dir, "spill_house_lookup.csv")

  # Export data with error handling
  tryCatch(
    {
      saveRDS(df, rds_path)
      write_csv(df, csv_path)

      logger::log_info(glue::glue("Data exported successfully to:"))
      logger::log_info(glue::glue("  - RDS: {rds_path}"))
      logger::log_info(glue::glue("  - CSV: {csv_path}"))
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export data: {e$message}")
      logger::log_error(err_msg)
      stop(err_msg)
    }
  )
}

############################################################
# Main Execution
############################################################

#' Main function to execute the spatial data integration pipeline.
#' @return Invisible NULL
main <- function() {
  tryCatch(
    {
      initialise_environment()
      setup_logging()

      logger::log_info("Starting spatial data processing pipeline")
      merged_data <- process_spatial_data()
      logger::log_info("Spatial join completed successfully")

      # Export the merged data using the export_data function
      logger::log_info("Exporting data")
      export_data(merged_data)
    },
    error = function(e) {
      logger::log_error(glue::glue("Pipeline failed: {e$message}"))
      stop(e)
    }
  )

  invisible(NULL)
}

if (sys.nframe() == 0) {
  main()
}
