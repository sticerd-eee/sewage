# ==============================================================================
# CDRC Zoopla Rental Transactions Data Cleaner
# ==============================================================================
#
# Purpose: Clean safeguarded Zoopla rental listings for 2014-2023, remove exact
#          duplicates, assign content-stable transaction IDs, and write paired
#          long-run/study candidates from one build.
#
# Author: Jacopo Olivieri
# Date: 2025-09-02
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/raw/zoopla/rentals_safeguarded_2014-2022.csv
#   - data/raw/zoopla/rentals_safeguarded_2023.csv
#   - data/raw/uk_postcodes/2602_uk_postcodes.csv
#   - scripts/R/utils/postcode_processing_utils.R
#
# Candidate outputs (canonical files are never written by this script):
#   - data/processed/zoopla/zoopla_rentals_long_run_candidate.parquet
#   - data/processed/zoopla/zoopla_rentals_candidate.parquet
#   - output/log/clean_zoopla_data.log
#
# Source:
#   - WhenFresh / Zoopla Property Transactions
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "hash_utils.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "data.table",
  "digest",
  "dplyr",
  "fs",
  "glue",
  "logger",
  "lubridate",
  "rio",
  "stringr",
  "tibble"
)

LOG_FILE <- here::here("output", "log", "clean_zoopla_data.log")

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "utils", "postcode_processing_utils.R"),
  local = TRUE
)

CLEANING_MANIFEST_VERSION <- "1"
CLEANING_METADATA_REQUIRED_KEYS <- c(
  "cleaning_manifest_version",
  "cleaning_run_stamp",
  "cleaning_market",
  "cleaning_artifact_role",
  "cleaning_year_min",
  "cleaning_year_max",
  "cleaning_source_row_count",
  "cleaning_parent_role"
)
RENTAL_IDENTITY_FIELDS <- c(
  "postcode", "address_line_01", "address_line_02", "address_line_03",
  "listing_price", "latest_to_rent", "rented"
)

# Configuration
############################################################

CONFIG <- list(
  # Time-frame
  years = 2014:2023,
  study_years = 2021:2023,
  base_year = 2021L,
  # Data retention options
  keep_address_line_1 = TRUE,  # Toggle to FALSE to exclude address data
  # Input file paths
  input_dir = here::here("data", "raw", "zoopla"),
  local_postcode_lookup_path = here::here(
    "data", "raw", "uk_postcodes", "2602_uk_postcodes.csv"
  ),
  
  # Output file paths
  long_run_candidate_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals_long_run_candidate.parquet"
  ),
  study_candidate_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals_candidate.parquet"
  ),
  # Column renaming map: old_name = new_name
  column_name_mapping = c(
    "zp.Address1" = "address_line_01",      # First line of address
    "zp.Address2" = "address_line_02",      # Second line of address
    "zp.Address3" = "address_line_03",      # Third line of address
    "zp.Postcode" = "postcode",             # Full postcode (spaces removed later)
    "zp.PropertyType" = "property_type",     # Zoopla property type
    "zp.Bedrooms" = "bedrooms",             # Listed bedrooms
    "zp.Bathrooms" = "bathrooms",           # Listed bathrooms
    "zp.Receptions" = "receptions",         # Listed receptions
    "zp.Floors" = "floors",                 # Listed floors
    "zp.ListingCreated" = "listing_created", # Listing creation date
    "zp.ListingPageViews" = "listing_page_views", # Listing page views
    "zp.ListingPrice" = "listing_price",     # Advertised rent
    "zp.LatestToRent" = "latest_to_rent",    # Last day shown "to rent"
    "zp.Rented" = "rented",                  # Date set to rented/let agreed
    "epc.EnergyEfficiency" = "epc_energy_efficiency", # EPC score
    "epc.EnergyRating" = "epc_energy_rating"        # EPC letter
  )
)

# Functions
############################################################

#' Attach the packages used unqualified in this script
#' @return NULL
initialise_environment <- function() {
  invisible(lapply("dplyr", function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

#' Load Zoopla Rental Data
#'
#' Reads the safeguarded Zoopla rental CSV files for 2014-2022 and 2023
#' from the raw input directory and combines them into a single tibble.
#'
#' @param file_path Character. Path to raw Zoopla input directory.
#' @return A tibble with all records combined.
load_data <- function(file_path = CONFIG$input_dir) {
  logger::log_info("Loading Zoopla data from: {basename(file_path)}")

  # Expected input files
  files <- c(
    fs::path(file_path, "rentals_safeguarded_2014-2022.csv"),
    fs::path(file_path, "rentals_safeguarded_2023.csv")
  )

  # Import separately for file-specific tidying
  logger::log_info("Reading files: {paste(basename(files), collapse = ' + ')}")
  df_2014_2022 <- rio::import(files[[1]]) %>% 
    select(-cdrc.File)
  df_2023 <- rio::import(files[[2]]) %>% 
    select(-V1, -cdrc.File)

  # Combine and return
  df <- dplyr::bind_rows(df_2014_2022, df_2023)
  logger::log_info("Loaded {nrow(df)} rows x {ncol(df)} cols")
  tibble::as_tibble(df)
}

#' Clean Zoopla Rental Data
#'
#' - Forms `rented_est` as coalesce(rented, latest_to_rent), then filters only
#'   on the year of that same field so selection and time indexing agree.
#' - Renames variables to snake_case, aligning with LR naming where possible.
#'
#' @param df Tibble returned by `load_data()`
#' @param years Inclusive years retained in the long-run output.
#' @param track_raw_origin Keep a temporary source-row index for dedupe evidence.
#' @return Cleaned tibble
clean_zoopla_data <- function(
    df,
    years = CONFIG$years,
    track_raw_origin = FALSE) {
  data <- tibble::as_tibble(df)
  if (isTRUE(track_raw_origin)) {
    data <- dplyr::mutate(data, .raw_origin_row = dplyr::row_number())
  }

  data %>%
    # Drop address lines (postcode retained, optionally keep Address1)
    {if (CONFIG$keep_address_line_1) {
      select(., -matches("zp\\.Address[4-9]"))  # Keep Address1, remove others
    } else {
      select(., -contains("zp.Address"))         # Remove all address fields
    }} %>%
    # Standardise names using CONFIG map
    rename_with(
      ~ if_else(
        .x %in% names(CONFIG$column_name_mapping),
        CONFIG$column_name_mapping[.x],
        .x
      )
    ) %>%
    # Drop observations without price data
    filter(!is.na(listing_price)) %>% 
    # Postcode normalisation; prefer rented date, else latest_to_rent
    mutate(
      postcode = stringr::str_remove_all(postcode, stringr::fixed(" ")),
      rented_est = dplyr::coalesce(rented, latest_to_rent)
    ) %>%
    filter(lubridate::year(rented_est) %in% years) %>%
    # LR‑aligned time IDs from rented_est
    mutate(
      qtr_id = (lubridate::year(rented_est) - CONFIG$base_year) * 4 + lubridate::quarter(rented_est),
      month_id = (lubridate::year(rented_est) - CONFIG$base_year) * 12 + lubridate::month(rented_est)
    ) %>%
    # Map property types to codes (keep bungalows as "B")
    mutate(
      property_type = stringr::str_to_upper(stringr::str_trim(property_type)),
      property_type = dplyr::case_when(
        is.na(property_type) | property_type == "" ~ NA_character_,
        property_type == "DETACHED" ~ "D",
        property_type == "SEMI-DETACHED" ~ "S",
        property_type == "TERRACED" ~ "T",
        property_type == "FLAT" ~ "F",
        property_type == "BUNGALOW" ~ "B"
      )
    )
}

#' Remove rows identical across every post-cleaning field
#'
#' A temporary raw-row index is excluded from equality and removed from output.
#' One duplicate group's source rows are retained as a logged origin spot check.
deduplicate_zoopla_transactions <- function(data, raw_data = NULL) {
  data <- tibble::as_tibble(data)
  origin_column <- intersect(".raw_origin_row", names(data))
  comparison_columns <- setdiff(names(data), c("rental_id", origin_column))
  if (length(comparison_columns) == 0L) {
    stop("Zoopla dedupe requires post-cleaning columns.", call. = FALSE)
  }

  comparison <- data.table::as.data.table(data[comparison_columns])
  removed <- duplicated(comparison, by = comparison_columns)
  duplicate_members <- removed |
    duplicated(comparison, by = comparison_columns, fromLast = TRUE)
  duplicate_group_count <- if (any(duplicate_members)) {
    data.table::uniqueN(
      comparison[duplicate_members],
      by = comparison_columns
    )
  } else {
    0L
  }

  origin_spot_check <- tibble::tibble()
  if (any(duplicate_members) && length(origin_column) == 1L) {
    first_member <- which(duplicate_members)[[1]]
    sample_group <- comparison[first_member]
    same_group <- comparison[
      sample_group,
      on = comparison_columns,
      which = TRUE,
      nomatch = 0L
    ]
    origin_rows <- as.integer(data$.raw_origin_row[same_group])
    raw_rows_identical <- NA
    if (!is.null(raw_data) && all(origin_rows %in% seq_len(nrow(raw_data)))) {
      raw_sample <- as.data.frame(raw_data[origin_rows, , drop = FALSE])
      raw_columns <- setdiff(names(raw_sample), c("V1", "cdrc.File"))
      raw_rows_identical <- nrow(unique(raw_sample[raw_columns])) == 1L
    }
    origin_spot_check <- tibble::tibble(
      .raw_origin_row = origin_rows,
      raw_rows_identical = raw_rows_identical
    )
  }

  deduplicated <- data[!removed, , drop = FALSE]
  deduplicated <- dplyr::select(
    deduplicated,
    -dplyr::any_of(".raw_origin_row")
  )

  list(
    data = deduplicated,
    removed_count = as.integer(sum(removed)),
    duplicate_group_count = as.integer(duplicate_group_count),
    origin_spot_check = origin_spot_check
  )
}

assert_unique_postcode_lookup <- function(postcode_data) {
  if (!("postcode" %in% names(postcode_data))) {
    stop("Postcode lookup is missing `postcode`.", call. = FALSE)
  }
  if (anyDuplicated(postcode_data$postcode)) {
    stop("Postcode lookup contains duplicate postcode keys.", call. = FALSE)
  }
  invisible(postcode_data)
}

enrich_zoopla_postcodes <- function(data, postcode_data) {
  assert_unique_postcode_lookup(postcode_data)
  input_rows <- nrow(data)
  enriched <- dplyr::left_join(data, postcode_data, by = "postcode")
  if (nrow(enriched) != input_rows) {
    stop(
      "Zoopla postcode enrichment was not row-count preserving: ",
      input_rows, " input rows became ", nrow(enriched), " rows.",
      call. = FALSE
    )
  }
  enriched
}

assert_rental_identity_unique <- function(data) {
  missing_fields <- setdiff(RENTAL_IDENTITY_FIELDS, names(data))
  if (length(missing_fields) > 0L) {
    stop(
      "Zoopla identity is missing composite field(s): ",
      paste(missing_fields, collapse = ", "),
      call. = FALSE
    )
  }
  serialized <- serialize_hash_fields(data, RENTAL_IDENTITY_FIELDS)
  duplicate_composite <- duplicated(serialized) | duplicated(serialized, fromLast = TRUE)
  if (any(duplicate_composite)) {
    profile <- data[duplicate_composite, RENTAL_IDENTITY_FIELDS, drop = FALSE] |>
      dplyr::count(dplyr::across(dplyr::everything()), name = "rows") |>
      dplyr::arrange(dplyr::desc(.data$rows)) |>
      dplyr::slice_head(n = 5L)
    stop(
      "Zoopla seven-field rental identity composite is not unique (",
      sum(duplicated(serialized)), " excess row(s); sample profile: ",
      paste(capture.output(print(profile)), collapse = " "), ").",
      call. = FALSE
    )
  }
  invisible(serialized)
}

#' Assign rental IDs in the superset and derive the study subset unchanged
build_zoopla_output_pair <- function(
    enriched_data,
    long_run_years = CONFIG$years,
    study_years = CONFIG$study_years) {
  data <- tibble::as_tibble(enriched_data)
  rented_year <- lubridate::year(data$rented_est)
  if (anyNA(rented_year) || any(!rented_year %in% long_run_years)) {
    stop(
      "Zoopla rented_est must be non-missing and within ",
      min(long_run_years), "-", max(long_run_years), ".",
      call. = FALSE
    )
  }

  assert_rental_identity_unique(data)
  long_run <- data |>
    dplyr::mutate(rental_id = hash_rental_identity(data)) |>
    dplyr::relocate("rental_id", .before = 1)
  if (anyNA(long_run$rental_id) || anyDuplicated(long_run$rental_id)) {
    stop("Hashed Zoopla rental_id values must be non-missing and unique; possible hash collision.", call. = FALSE)
  }
  if (any(!grepl("^[0-9a-f]{16}$", long_run$rental_id))) {
    stop("Hashed Zoopla rental_id values must be lowercase 16-character hex.", call. = FALSE)
  }

  study <- dplyr::filter(
    long_run,
    lubridate::year(.data$rented_est) %in% study_years
  )
  list(long_run = long_run, study = study)
}

new_cleaning_run_stamp <- function(time = Sys.time()) {
  format(as.POSIXct(time, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

cleaning_metadata <- function(
    run_stamp,
    market,
    artifact_role,
    years,
    source_row_count,
    parent_role,
    removed_duplicate_count) {
  metadata <- list(
    cleaning_manifest_version = CLEANING_MANIFEST_VERSION,
    cleaning_run_stamp = as.character(run_stamp),
    cleaning_market = as.character(market),
    cleaning_artifact_role = as.character(artifact_role),
    cleaning_year_min = as.character(min(years)),
    cleaning_year_max = as.character(max(years)),
    cleaning_source_row_count = as.character(source_row_count),
    cleaning_parent_role = as.character(parent_role),
    cleaning_removed_duplicate_rows = as.character(removed_duplicate_count)
  )
  if (!all(CLEANING_METADATA_REQUIRED_KEYS %in% names(metadata))) {
    stop("Cleaning metadata is incomplete.", call. = FALSE)
  }
  metadata
}

assert_candidate_output_path <- function(path) {
  if (!grepl("_candidate\\.parquet$", basename(path))) {
    stop(
      "Cleaning outputs must use candidate sibling paths ending in `_candidate.parquet`; refusing: ",
      path,
      call. = FALSE
    )
  }
  invisible(path)
}

write_cleaning_candidate <- function(data, path, metadata) {
  assert_candidate_output_path(path)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  table <- arrow::Table$create(as.data.frame(data))
  table$metadata <- metadata
  stage_path <- tempfile(
    pattern = paste0(".", basename(path), "-"),
    tmpdir = dirname(path),
    fileext = ".parquet"
  )
  on.exit(unlink(stage_path), add = TRUE)
  arrow::write_parquet(table, stage_path)
  if (!file.rename(stage_path, path)) {
    stop("Failed to promote staged cleaning candidate to ", path, ".", call. = FALSE)
  }
  invisible(path)
}

write_zoopla_candidates <- function(
    outputs,
    long_run_path = CONFIG$long_run_candidate_path,
    study_path = CONFIG$study_candidate_path,
    run_stamp = new_cleaning_run_stamp(),
    source_row_count = nrow(outputs$long_run),
    removed_duplicate_count = 0L) {
  if (!is.list(outputs) || !all(c("long_run", "study") %in% names(outputs))) {
    stop("`outputs` must contain `long_run` and `study` data frames.", call. = FALSE)
  }
  if (identical(normalizePath(long_run_path, mustWork = FALSE),
                normalizePath(study_path, mustWork = FALSE))) {
    stop("Long-run and study candidates must use distinct paths.", call. = FALSE)
  }
  expected_study <- dplyr::filter(
    outputs$long_run,
    lubridate::year(.data$rented_est) %in% CONFIG$study_years
  )
  if (!isTRUE(all.equal(
    as.data.frame(expected_study),
    as.data.frame(outputs$study),
    check.attributes = FALSE
  ))) {
    stop("Zoopla study candidate must be an unchanged filter of the long-run candidate.", call. = FALSE)
  }

  long_metadata <- cleaning_metadata(
    run_stamp, "rentals", "long_run", CONFIG$years,
    source_row_count, "raw_safeguarded_files", removed_duplicate_count
  )
  study_metadata <- cleaning_metadata(
    run_stamp, "rentals", "study", CONFIG$study_years,
    source_row_count, "long_run", removed_duplicate_count
  )
  write_cleaning_candidate(outputs$long_run, long_run_path, long_metadata)
  write_cleaning_candidate(outputs$study, study_path, study_metadata)
  logger::log_info(
    "Wrote paired Zoopla candidates with run stamp {run_stamp}: long_run={nrow(outputs$long_run)}, study={nrow(outputs$study)}"
  )
  invisible(list(
    long_run_path = long_run_path,
    study_path = study_path,
    run_stamp = run_stamp
  ))
}

# Main Workflow
############################################################

#' Main Zoopla Data Processing Pipeline
#'
#' Orchestrates the workflow: initialises environment, sets up logging,
#' loads and cleans the Zoopla rental data, processes postcodes, and exports.
#'
#' @param refresh_postcodes Boolean retained for compatibility but ignored
#' @return List containing the output path and postcode diagnostics
main <- function(refresh_postcodes = FALSE) {
  tryCatch({
    # Setup
    initialise_environment()
    initialise_logging()

    logger::log_info(
      "`refresh_postcodes` is ignored for Zoopla builds; local postcode files are used instead."
    )

    # Load raw data
    df_raw <- load_data()

    # Clean data
    df <- clean_zoopla_data(df_raw, CONFIG$years, track_raw_origin = TRUE)
    dedupe <- deduplicate_zoopla_transactions(df, df_raw)
    df <- dedupe$data
    logger::log_info(
      "Removed {dedupe$removed_count} exact duplicate Zoopla row(s) across {dedupe$duplicate_group_count} group(s) before rental_id assignment."
    )
    if (nrow(dedupe$origin_spot_check) > 0L) {
      logger::log_info(
        "Raw-origin duplicate spot check: source rows {paste(dedupe$origin_spot_check$.raw_origin_row, collapse = ', ')}; raw rows identical excluding file bookkeeping={dedupe$origin_spot_check$raw_rows_identical[[1]]}."
      )
    } else {
      logger::log_info("Raw-origin duplicate spot check: no duplicate group available.")
    }

    # Build postcode enrichment using the local lookup keyed on postcode only
    postcode_result <- get_local_postcode_data_for_sales(
      df,
      lookup_path = CONFIG$local_postcode_lookup_path
    )

    final_data <- enrich_zoopla_postcodes(df, postcode_result$postcode_data)
    outputs <- build_zoopla_output_pair(
      final_data,
      CONFIG$years,
      CONFIG$study_years
    )
    publication <- write_zoopla_candidates(
      outputs,
      run_stamp = new_cleaning_run_stamp(),
      source_row_count = nrow(df_raw),
      removed_duplicate_count = dedupe$removed_count
    )
    logger::log_info(
      "Validated unique seven-field rental identity and rental_id hashes across {nrow(outputs$long_run)} long-run rows."
    )

    invisible(
      list(
        output_paths = publication[c("long_run_path", "study_path")],
        run_stamp = publication$run_stamp,
        row_counts = c(
          long_run = nrow(outputs$long_run),
          study = nrow(outputs$study)
        ),
        dedupe = dedupe[c(
          "removed_count", "duplicate_group_count", "origin_spot_check"
        )],
        diagnostics = postcode_result$diagnostics
      )
    )
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Run pipeline when script is executed directly
if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
