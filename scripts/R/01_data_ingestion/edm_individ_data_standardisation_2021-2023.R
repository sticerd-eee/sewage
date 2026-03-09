# ==============================================================================
# EDM Spill Data File Standardisation
# ==============================================================================
#
# Purpose: Unzip historical EDM archives for 2021-2023, identify year and water
#          company from raw filenames, and rename supported files into a stable
#          standard format for downstream ingestion steps.
#
# Author: Jacopo Olivieri
# Date: 2024-12-01
#
# Inputs:
#   - data/raw/edm_data/EDM*.zip - Raw FOI EDM archives from water companies
#
# Outputs:
#   - data/raw/edm_data/{year}_{company}_edm.{extension} - Standardised files
#   - output/log/01_edm_individ_data_standardisation_2021-2023.log
#
# Key Invariants:
#   - Output filename contract is "{year}_{company}_edm.{extension}"
#   - Supported extensions are csv, xlsx, and xlsb only
#   - Archives extract into data/raw/edm_data/temp_edm_extract during processing
#   - Company regex order matters: first match wins
#
# Notes:
#   - This script standardises filenames only; it does not harmonise contents.
#   - Temporary extraction artifacts are cleaned up on exit.
#
# ==============================================================================

VALID_EXTENSIONS <- c("csv", "xlsx", "xlsb")

COMPANY_PATTERNS <- c(
  "anglian" = "anglian_water",
  "northumbrian|nwl" = "northumbrian_water",
  "severn" = "severn_trent_water",
  "south\\s*west" = "south_west_water",
  "southern" = "southern_water",
  "thames" = "thames_water",
  "uu|united" = "united_utilities",
  "welsh" = "welsh_water",
  "wessex" = "wessex_water",
  "yorkshire" = "yorkshire_water",
  "all water and sewerage companies" = "annual_return"
)

setup_logging <- function() {
  log_path <- here::here(
    "output", "log",
    "01_edm_individ_data_standardisation_2021-2023.log"
  )
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)

  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
}

match_company <- function(filename, company_patterns = COMPANY_PATTERNS) {
  filename_lower <- tolower(filename)
  matched_patterns <- names(company_patterns)[vapply(
    names(company_patterns),
    function(pattern) stringr::str_detect(filename_lower, pattern),
    logical(1)
  )]

  if (length(matched_patterns) == 0) {
    return(NA_character_)
  }

  unname(company_patterns[[matched_patterns[[1]]]])
}

extract_edm_archives <- function(data_path, temp_dir = fs::path(data_path, "temp_edm_extract")) {
  logger::log_info("Starting file extraction from: {data_path}")

  if (!fs::dir_exists(data_path)) {
    logger::log_error("Directory does not exist: {data_path}")
    stop("Directory does not exist: ", data_path)
  }

  zip_files <- fs::dir_ls(data_path, regexp = "EDM.*\\.zip$")
  if (length(zip_files) == 0) {
    logger::log_error("No EDM zip files found in: {data_path}")
    stop("No EDM zip files found in: ", data_path)
  }

  if (fs::dir_exists(temp_dir)) {
    fs::dir_delete(temp_dir)
  }
  fs::dir_create(temp_dir)

  successful_extractions <- 0L

  for (zip_file in zip_files) {
    tryCatch(
      {
        logger::log_info("Processing {fs::path_file(zip_file)}")
        utils::unzip(zip_file, exdir = temp_dir)
        successful_extractions <- successful_extractions + 1L
      },
      error = function(e) {
        logger::log_warn("Error extracting {fs::path_file(zip_file)}: {e$message}")
        warning("Error extracting ", fs::path_file(zip_file), ": ", e$message)
      }
    )
  }

  if (successful_extractions == 0) {
    logger::log_error("No directories were successfully extracted")
    stop("No directories were successfully extracted")
  }

  logger::log_info("Successfully extracted {successful_extractions} archives")
  temp_dir
}

build_file_mapping <- function(
    extract_path,
    valid_extensions = VALID_EXTENSIONS,
    company_patterns = COMPANY_PATTERNS) {
  logger::log_info("Creating file mappings from {extract_path}")

  file_pattern <- paste0("\\.(", paste(valid_extensions, collapse = "|"), ")$")
  files <- fs::dir_ls(extract_path, recurse = TRUE, type = "file", regexp = file_pattern)

  if (length(files) == 0) {
    logger::log_error("No valid files found in {extract_path}")
    stop("No valid files found to process")
  }

  file_names <- fs::path_file(files)

  mappings <- tibble::tibble(
    original_path = files,
    year = stringr::str_extract(file_names, "(?<!\\d)\\d{4}(?!\\d)"),
    company = vapply(
      file_names,
      match_company,
      character(1),
      company_patterns = company_patterns
    ),
    extension = fs::path_ext(files)
  ) |>
    dplyr::filter(!is.na(year) & !is.na(company)) |>
    dplyr::mutate(new_name = paste0(year, "_", company, "_edm.", extension))

  validate_mappings(mappings)
  mappings
}

validate_mappings <- function(mappings) {
  logger::log_info("Validating file mappings")
  logger::log_info("Total files found: {nrow(mappings)}")
  logger::log_info("Files by year: {deparse(table(mappings$year))}")
  logger::log_info("Files by company: {deparse(table(mappings$company))}")

  duplicate_rows <- mappings[
    duplicated(mappings[c("year", "company")]) |
      duplicated(mappings[c("year", "company")], fromLast = TRUE),
    ,
    drop = FALSE
  ]

  if (nrow(duplicate_rows) > 0) {
    logger::log_warn("Found {nrow(duplicate_rows)} possible duplicate files")
    print(duplicate_rows)
  }
}

move_standardised_files <- function(mappings, data_path) {
  purrr::walk2(
    mappings$original_path,
    mappings$new_name,
    ~ fs::file_move(.x, fs::path(data_path, .y))
  )

  logger::log_info("Operations completed successfully!")
  logger::log_info("Files standardised: {nrow(mappings)}")
}

main <- function(data_path = here::here()) {
  start_time <- Sys.time()
  temp_dir <- fs::path(data_path, "temp_edm_extract")

  on.exit({
    if (fs::dir_exists(temp_dir)) {
      fs::dir_delete(temp_dir)
    }
  }, add = TRUE)

  tryCatch(
    {
      setup_logging()
      logger::log_info("Starting EDM file standardisation process")

      extract_edm_archives(data_path, temp_dir)
      mappings <- build_file_mapping(temp_dir)
      move_standardised_files(mappings, data_path)
    },
    error = function(e) {
      logger::log_error("Fatal error: {e$message}")
      stop(e$message, call. = FALSE)
    },
    finally = {
      formatted_duration <- format(Sys.time() - start_time)
      logger::log_info(
        "===== EDM File Standardisation Finished in {formatted_duration} ====="
      )
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

if (sys.nframe() == 0) {
  data_path <- here::here("data", "raw", "edm_data")
  main(data_path)
}
############################################################
