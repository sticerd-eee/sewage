############################################################
# Merge Zoopla Rental Data with Nearby Spill Sites (10km)
# Project: Sewage
# Date: 15/09/2025
# Author: Jacopo Olivieri (adapted for rentals)
############################################################

# Set Up Functions
############################################################

#' Initialise the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  # Package management with renv (optional)
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
  }

  # Define required packages
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue",
    "fs", "sf", "arrow"
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
  log_path <- here::here("output", "log", "10km_site_rental_match.log")
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
  zoopla_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  spill_sites_path = here::here("data", "processed", "unique_spill_sites.parquet"),
  output_path = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
  radius_km = 5,
  chunk_size = 2000
)


# Functions
############################################################

#' Load spatial datasets for Zoopla rentals and spill sites.
#' @return List with `rentals` and `spill` data frames
load_data <- function() {
  logger::log_info("Loading Zoopla rentals and spill site datasets")

  rentals <- rio::import(CONFIG$zoopla_path, trust = TRUE)
  spill <- rio::import(CONFIG$spill_sites_path, trust = TRUE)

  return(list(rentals = rentals, spill = spill))
}

#' Convert spill sites to sf and build lookup table
#' @param spill_data Data frame of spill sites with easting/northing
#' @return List with `spill_sf` (sf) and `lookup` (tibble)
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

#' Convert rental records to sf points
#' @param rentals_df Data frame of rentals with easting/northing
#' @return sf object with rental_id and geometry
prepare_rental_data <- function(rentals_df) {
  logger::log_info("Preparing Zoopla rental spatial data")

  rentals_sf <- rentals_df %>%
    filter(!is.na(easting) & !is.na(northing)) %>%
    st_as_sf(coords = c("easting", "northing"), crs = 27700) %>%
    select(rental_id, geometry)

  return(rentals_sf)
}

#' Match rentals to spill sites within specified radius
#' @param rentals_sf sf of rentals
#' @param spill_sites_sf sf of spills
#' @param spill_lookup tibble linking site_id to geometry
#' @param radius_km search radius in km (default 10)
#' @param chunk_size number of rentals per chunk (default from CONFIG)
#' @return Tibble of matches with distances and counts
perform_spatial_join <- function(rentals_sf, spill_sites_sf, spill_lookup,
                                 radius_km = CONFIG$radius_km,
                                 chunk_size = CONFIG$chunk_size) {
  logger::log_info("Performing spatial join between rentals and spill sites")

  radius_m <- radius_km * 1000

  rental_chunks <- split(
    rentals_sf,
    (seq_len(nrow(rentals_sf)) - 1) %/% chunk_size
  )

  merged_chunks <- purrr::map(seq_along(rental_chunks), function(i) {
    chunk <- rental_chunks[[i]]
    logger::log_info(glue::glue("Processing chunk {i}/{length(rental_chunks)} with {nrow(chunk)} rentals"))

    st_join(chunk, spill_sites_sf,
      join = st_is_within_distance,
      dist = radius_m,
      left = TRUE
    ) %>%
      left_join(spill_lookup, by = "site_id") %>%
      mutate(
        # Straight-line distance to the spill site
        distance_m = if_else(is.na(spill_geom), NA_real_,
          as.numeric(st_distance(geometry, spill_geom, by_element = TRUE))
        ),
        distance_km = distance_m / 1000
      ) %>%
      group_by(rental_id) %>%
      mutate(n_discharge_outlet = sum(!is.na(site_id))) %>%
      ungroup() %>%
      st_drop_geometry() %>%
      select(rental_id, site_id, distance_m, distance_km, n_discharge_outlet)
  })

  dplyr::bind_rows(merged_chunks)
}


#' Process the spatial data by loading, preparing, and merging Zoopla rentals and spill site data.
#' @param data List with rentals and spill data
#' @param radius_km Numeric radius in kilometres for the spatial join (default: 10)
#' @return tibble containing the merged data
process_spatial_data <- function(data, radius_km = CONFIG$radius_km) {
  rentals_sf <- prepare_rental_data(data$rentals)
  spill_data <- prepare_spill_sites(data$spill)

  merged_spatial_data <- perform_spatial_join(
    rentals_sf, spill_data$spill_sf, spill_data$lookup, radius_km
  )

  return(merged_spatial_data)
}


#' Export processed data to a Parquet file
#' @param df Tibble to export
#' @return NULL
export_data <- function(df) {
  parquet_path <- CONFIG$output_path

  # Ensure output directory exists
  dir.create(dirname(parquet_path), recursive = TRUE, showWarnings = FALSE)

  tryCatch(
    {
      arrow::write_parquet(df, parquet_path)
      logger::log_info("Data exported successfully to Parquet file at {parquet_path}")
    },
    error = function(e) {
      err_msg <- glue::glue("Failed to export data to Parquet: {e$message}")
      logger::log_error(err_msg)
      stop(err_msg)
    }
  )
}


# Main Execution
############################################################

#' Main execution function
main <- function() {
  tryCatch(
    {
      initialise_environment()
      setup_logging()

      # Load data
      data_list <- load_data()

      # 10km radius match
      logger::log_info("Starting spatial data processing pipeline (rentals)")
      merged_data <- process_spatial_data(data = data_list, radius_km = CONFIG$radius_km)
      logger::log_info("Spatial join for rentals completed successfully")

      # Export the merged data using the export_data function
      logger::log_info("Exporting rental lookup data")
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