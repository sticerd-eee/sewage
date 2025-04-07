############################################################
# Create House-Level Monthly Panel Dataset
# Project: Sewage
# Date: 05/04/2025
# Author: Jacopo Olivieri
############################################################

#' Defines configuration settings for downloading data from various APIs,
#' primarily targeting ArcGIS Feature/Map Server endpoints for EDM data.
#'
#' This script sets up:
#' - Base paths for saving raw data and logs.
#' - Default parameters like records per request for pagination.
#' - A list (`api_config_list`) specifying parameters for each API source.
#'
#' This configuration is intended to be sourced by download scripts (e.g., `run_edm_downloads.R`).

# Setup Functions
############################################################

initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c("here")

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
}
initialise_environment()


# General Settings
############################################################

#' Base directory for raw downloaded API data.
BASE_SAVE_DIRECTORY <- here::here("data", "raw", "edm_data", "raw_api_responses")

#' Path to CSV file used for logging download attempts (success/failure).
METADATA_LOG_FILE <- here::here("output", "log", "edm_api_download_log.csv")

#' Default maximum records per request for paginated APIs (e.g., ArcGIS Server).
#' This value is used if not explicitly overridden in the API-specific config.
DEFAULT_MAX_RECORDS_PER_REQUEST <- 1000


# API Specific Configurations
############################################################

#' A list where each named element defines the configuration for one API source.
#' The *name* of the list element (e.g., 'stream_outfalls') serves as the unique `ApiID`
#' `ApiID` used in logging and file naming.
api_config_list <- list(

  # --- API 1: Anglian Water ---
  anglian_water = list(
    name = "anglian water",
    base_url = "https://services3.arcgis.com/VCOY1atHWVcDlvlJ/arcgis/rest/services/stream_service_outfall_locations_view/FeatureServer/0",

    #' Base query parameters added to every request for this API.
    query_params = list(
      where = "1=1", # Query filter (1=1 gets all features)
      outFields = "*", # Fields to return ('*' for all)
      returnGeometry = "true", # Include geometry objects in the raw JSON response?
      outSR = "4326", # Output spatial reference system (4326 = WGS84)
      f = "json" # Required response format
    )

    #' Optional: Override the DEFAULT_MAX_RECORDS_PER_REQUEST for this specific API if known.
    # max_records_per_request = 2000
  ),

  # --- API 2: Northumbrian Water ---
  northumbrian_water = list(
    name = "northumbrian water",
    base_url = "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 3: Severn Trent Water ---
  severn_trent_water = list(
    name = "severn trent water",
    base_url = "https://services1.arcgis.com/NO7lTIlnxRMMG9Gw/arcgis/rest/services/Severn_Trent_Water_Storm_Overflow_Activity/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 4: Southern Water ---
  southern_water = list(
    name = "southern water",
    base_url = "https://services-eu1.arcgis.com/XxS6FebPX29TRGDJ/arcgis/rest/services/Southern_Water_Storm_Overflow_Activity/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 5: South West Water ---
  south_west_water = list(
    name = "south west water",
    base_url = "https://services-eu1.arcgis.com/OMdMOtfhATJPcHe3/arcgis/rest/services/NEH_outlets_PROD/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 6: Thames Water ---
  thames_water = list(
    name = "thames water",
    base_url = "https://services2.arcgis.com/g6o32ZDQ33GpCIu3/arcgis/rest/services/Thames_Water_Storm_Overflow_Activity_(Production)_view/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 7: United Utilities ---
  united_utilities = list(
    name = "united utilities",
    base_url = "https://services5.arcgis.com/5eoLvR0f8HKb7HWP/arcgis/rest/services/United_Utilities_Storm_Overflow_Activity/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 8: Wessex Water ---
  wessex_water = list(
    name = "wessex water",
    base_url = "https://services.arcgis.com/3SZ6e0uCvPROr4mS/arcgis/rest/services/Wessex_Water_Storm_Overflow_Activity/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  ),

  # --- API 9: Yorkshire Water ---
  yorkshire_water = list(
    name = "yorkshire water",
    base_url = "https://services-eu1.arcgis.com/1WqkK5cDKUbF0CkH/arcgis/rest/services/Yorkshire_Water_Storm_Overflow_Activity/FeatureServer/0/query?outFields=*&where=1%3D1",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  )

  # --- API 10: Welsh Water ---
)
