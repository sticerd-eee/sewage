# ==============================================================================
# EA Consented Discharges Database Cleaning
# ==============================================================================
#
# Purpose: Load the tracked Environment Agency consented discharges workbook,
#          standardise key column names and water company labels, and export a
#          cleaned database for downstream matching and analysis.
#
# Author: Jacopo Olivieri
# Date: 2024-12-10
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/raw/ea_consents/consents_all.xlsx - EA consented discharges workbook
#
# Outputs:
#   - data/processed/consent_discharges_db.RData
#   - data/processed/consent_discharges_db.csv
#   - output/log/11_clean_consented_discharges_database.log
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c("dplyr", "fs", "janitor", "logger", "rio")
LOG_FILE <- here::here(
  "output", "log",
  "11_clean_consented_discharges_database.log"
)

check_required_packages(REQUIRED_PACKAGES)

CONFIG <- list(
  raw_dir = here::here("data", "raw", "ea_consents"),
  processed_dir = here::here("data", "processed"),
  input_file = "consents_all.xlsx",
  water_company_mapping = c(
    "ANGLIAN WATER SERVICES LIMITED" = "Anglian Water",
    "DWR CYMRU CYFYNGEDIG" = "Welsh Water",
    "NORTHUMBRIAN WATER LIMITED" = "Northumbrian Water",
    "NORTHUMBRIAN WATER LIMITED (ESSEX & SUFFOLK WATER)" = "Northumbrian Water",
    "SEVERN TRENT WATER LIMITED" = "Severn Trent",
    "SOUTH WEST WATER LIMITED" = "South West Water",
    "SOUTHERN WATER SERVICES LIMITED" = "Southern Water",
    "THAMES WATER UTILITIES LTD" = "Thames Water",
    "UNITED UTILITIES WATER LIMITED" = "United Utilities",
    "WESSEX WATER SERVICES LIMITED" = "Wessex Water",
    "YORKSHIRE WATER SERVICES LIMITED" = "Yorkshire Water"
  ),
  column_name_mapping = c(
    "company_name" = "water_company",
    "discharge_site_name" = "site_name_ea",
    "discharge_site_type_code" = "activity_reference",
    "permit_number" = "permit_reference_ea",
    "outlet_grid_ref" = "outlet_discharge_ngr"
  )
)

rename_mapped_columns <- function(column_names, mapping = CONFIG$column_name_mapping) {
  renamed_columns <- unname(mapping[column_names])
  renamed_columns[is.na(renamed_columns)] <- column_names[is.na(renamed_columns)]
  renamed_columns
}

load_data <- function(file_name = CONFIG$input_file) {
  file_path <- fs::path(CONFIG$raw_dir, file_name)
  logger::log_info("Loading data: {file_path}")

  if (!fs::file_exists(file_path)) {
    stop("File not found: ", file_path, call. = FALSE)
  }

  rio::import(file_path)
}

clean_data <- function(df) {
  cleaned <- df |>
    janitor::clean_names() |>
    dplyr::rename_with(rename_mapped_columns) |>
    dplyr::mutate(
      water_company = dplyr::recode(water_company, !!!CONFIG$water_company_mapping)
    ) |>
    dplyr::filter(water_company %in% unname(CONFIG$water_company_mapping))

  logger::log_info("Cleaned {nrow(cleaned)} records")
  cleaned
}

export_data <- function(df) {
  fs::dir_create(CONFIG$processed_dir)

  rdata_file <- fs::path(CONFIG$processed_dir, "consent_discharges_db.RData")
  csv_file <- fs::path(CONFIG$processed_dir, "consent_discharges_db.csv")

  save(df, file = rdata_file)
  rio::export(df, csv_file)

  logger::log_info("Data exported successfully to {rdata_file} and {csv_file}")
}

main <- function(file_name = CONFIG$input_file) {
  setup_logging(LOG_FILE)
  logger::log_info("Script started at {Sys.time()}")
  on.exit(logger::log_info("Script finished at {Sys.time()}"), add = TRUE)

  start_time <- Sys.time()
  logger::log_info("Starting data processing")

  cleaned_data <- load_data(file_name) |>
    clean_data()

  export_data(cleaned_data)

  execution_time <- difftime(Sys.time(), start_time, units = "mins")
  logger::log_info("Processing completed in {round(execution_time, 2)} minutes")
}

if (sys.nframe() == 0) {
  main()
}
