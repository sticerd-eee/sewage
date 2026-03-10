# ==============================================================================
# EDM API Configuration
# ==============================================================================
#
# Purpose: Define the shared configuration for the live 2024+ EDM API pipeline,
#          including company scope, storage paths, and per-company ArcGIS
#          endpoint settings used by ingestion and downstream processing steps.
#
# Author: Jacopo Olivieri
# Date: 2025-04-05
# Date Modified: 2026-03-10
#
# Inputs:
#   - NA
#
# Outputs:
#   - NA
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to source scripts/config/api_config.R. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}


# General Settings
############################################################

#' Base directory for raw downloaded API data.
BASE_SAVE_DIRECTORY <- here::here("data", "raw", "edm_data", "raw_api_responses")

#' Path to CSV file used for logging download attempts (success/failure).
METADATA_LOG_FILE <- here::here("output", "log", "edm_api_download_log.csv")

#' Default maximum records per request for paginated APIs (e.g., ArcGIS Server).
DEFAULT_MAX_RECORDS_PER_REQUEST <- 1000

#' Directory containing processed per-company API parquet files.
EDM_API_PROCESSED_DIRECTORY <- here::here("data", "processed", "edm_api_data")

#' Consolidated 2024+ API parquet written after the combine step.
EDM_API_COMBINED_FILE <- file.path(
  EDM_API_PROCESSED_DIRECTORY,
  "combined_api_data.parquet"
)

#' Explicit scope for the live 2024+ API pipeline.
EDM_API_PIPELINE_SCOPE <- "england_only"

#' Human-readable description of the live 2024+ API scope.
EDM_API_PIPELINE_SCOPE_LABEL <- paste(
  "England-only National Storm Overflows Hub feed.",
  "Welsh Water is intentionally out of scope for the live 2024+ API pipeline."
)


# API Specific Configurations
############################################################

#' A list where each named element defines the configuration for one API source.
#' The *name* of the list element serves as the unique `ApiID`.
api_config_list <- list(

  # --- API 1: Anglian Water ---
  anglian_water = list(
    name = "anglian water",
    # Store the ArcGIS layer URL only. The downloader appends `/query`.
    base_url = "https://services3.arcgis.com/VCOY1atHWVcDlvlJ/arcgis/rest/services/stream_service_outfall_locations_view/FeatureServer/0",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "true",
      outSR = "4326",
      f = "json"
    )
  ),

  # --- API 2: Northumbrian Water ---
  northumbrian_water = list(
    name = "northumbrian water",
    base_url = "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0",
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
    base_url = "https://services1.arcgis.com/NO7lTIlnxRMMG9Gw/arcgis/rest/services/Severn_Trent_Water_Storm_Overflow_Activity/FeatureServer/0",
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
    base_url = "https://services-eu1.arcgis.com/XxS6FebPX29TRGDJ/arcgis/rest/services/Southern_Water_Storm_Overflow_Activity/FeatureServer/0",
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
    base_url = "https://services-eu1.arcgis.com/OMdMOtfhATJPcHe3/arcgis/rest/services/NEH_outlets_PROD/FeatureServer/0",
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
    base_url = "https://services2.arcgis.com/g6o32ZDQ33GpCIu3/arcgis/rest/services/Thames_Water_Storm_Overflow_Activity_(Production)_view/FeatureServer/0",
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
    base_url = "https://services5.arcgis.com/5eoLvR0f8HKb7HWP/arcgis/rest/services/United_Utilities_Storm_Overflow_Activity/FeatureServer/0",
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
    base_url = "https://services.arcgis.com/3SZ6e0uCvPROr4mS/arcgis/rest/services/Wessex_Water_Storm_Overflow_Activity/FeatureServer/0",
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
    base_url = "https://services-eu1.arcgis.com/1WqkK5cDKUbF0CkH/arcgis/rest/services/Yorkshire_Water_Storm_Overflow_Activity/FeatureServer/0",
    query_params = list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "false",
      f = "json"
    )
  )
)

# The live 2024+ API pipeline currently tracks the nine England companies in the
# National Storm Overflows Hub. Welsh Water publishes a separate public map and
# should only be added here through a deliberate source-contract change.

get_edm_api_contract <- function() {
  company_ids <- names(api_config_list)
  company_names <- vapply(
    api_config_list,
    function(config) tools::toTitleCase(config$name),
    character(1)
  )

  list(
    scope = EDM_API_PIPELINE_SCOPE,
    scope_label = EDM_API_PIPELINE_SCOPE_LABEL,
    raw_directory = BASE_SAVE_DIRECTORY,
    processed_directory = EDM_API_PROCESSED_DIRECTORY,
    combined_file = EDM_API_COMBINED_FILE,
    company_ids = unname(company_ids),
    company_names = unname(company_names),
    company_id_to_name = stats::setNames(unname(company_names), company_ids),
    company_name_to_id = stats::setNames(company_ids, unname(company_names)),
    parquet_files = paste0(company_ids, ".parquet")
  )
}

compare_edm_api_company_ids <- function(
    actual_company_ids,
    contract = get_edm_api_contract()) {
  actual_company_ids <- unique(as.character(actual_company_ids))
  actual_company_ids <- actual_company_ids[
    !is.na(actual_company_ids) & nzchar(actual_company_ids)
  ]

  list(
    actual_company_ids = actual_company_ids,
    expected_company_ids = contract$company_ids,
    valid_company_ids = intersect(actual_company_ids, contract$company_ids),
    missing_company_ids = setdiff(contract$company_ids, actual_company_ids),
    unexpected_company_ids = setdiff(actual_company_ids, contract$company_ids)
  )
}
