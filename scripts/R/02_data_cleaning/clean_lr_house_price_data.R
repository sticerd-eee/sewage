# ==============================================================================
# HM Land Registry Price Paid Data Cleaner
# ==============================================================================
#
# Purpose: Clean one same-vintage 2014-2024 Land Registry download, enrich it
#          with postcode attributes, assign content-stable transaction-grain
#          `house_id` values, and write paired candidate datasets. The
#          2021-2024 study file is always derived from the long-run table.
#
# Inputs:
#   - data/raw/lr_house_price/pp-2014.csv ... pp-2024.csv
#   - data/raw/uk_postcodes/2602_uk_postcodes.csv
#
# Candidate outputs (canonical files are never written by this script):
#   - data/processed/house_price_long_run_candidate.parquet
#   - data/processed/house_price_candidate.parquet
#   - output/log/clean_lr_house_price_data.log
#
# Source:
#   - https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
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
  "digest",
  "dplyr",
  "glue",
  "logger",
  "lubridate",
  "purrr",
  "rio",
  "stringr",
  "tibble"
)

LOG_FILE <- here::here("output", "log", "clean_lr_house_price_data.log")

check_required_packages(REQUIRED_PACKAGES)

CLEANING_MANIFEST_VERSION <- "1"
LR_EXPECTED_YEARS <- 2014:2024
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

CONFIG <- list(
  years = LR_EXPECTED_YEARS,
  study_years = 2021:2024,
  base_year = 2021L,
  input_dir = here::here("data", "raw", "lr_house_price"),
  long_run_candidate_path = here::here(
    "data", "processed", "house_price_long_run_candidate.parquet"
  ),
  study_candidate_path = here::here(
    "data", "processed", "house_price_candidate.parquet"
  ),
  local_postcode_lookup_path = here::here(
    "data", "raw", "uk_postcodes", "2602_uk_postcodes.csv"
  ),
  column_name_mapping = c(
    "transaction_id", # V1  - Transaction unique identifier
    "price", # V2  - Sale price
    "date_of_transfer", # V3  - Date when sale was completed
    "postcode", # V4  - Postcode at time of transaction
    "property_type", # V5  - D/S/T/F/O
    "old_new", # V6  - Y/N for new build or established
    "duration", # V7  - F/L for Freehold/Leasehold
    "paon", # V8  - Primary address (house number/name)
    "saon", # V9  - Secondary address (flat number etc)
    "street", # V10 - Street name
    "locality", # V11 - Locality
    "town_city", # V12 - Town/City
    "district", # V13 - District
    "county", # V14 - County
    "ppd_category", # V15 - A/B for transaction type
    "record_status" # V16 - A/C/D for record status
  )
)

# Setup Functions
############################################################

initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

setup_parallel <- function() {
  logger::log_info("Running sequentially (no future::multisession)")
}

initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# Data Loading and Validation
############################################################

#' Validate the complete annual LR input set before reading any file
#' @return Named character vector of input paths in configured year order
validate_expected_lr_files <- function(input_dir, years) {
  if (!identical(as.integer(years), as.integer(LR_EXPECTED_YEARS))) {
    stop(
      "Land Registry cleaning requires the exact 2014-2024 annual window.",
      call. = FALSE
    )
  }
  expected_names <- sprintf("pp-%d.csv", years)
  expected_paths <- file.path(input_dir, expected_names)
  missing_names <- expected_names[!file.exists(expected_paths)]

  if (length(missing_names) > 0L) {
    stop(
      "Land Registry same-vintage input is incomplete. Missing exact annual file(s): ",
      paste(missing_names, collapse = ", "),
      ". Expected pp-2014.csv through pp-2024.csv from one download session.",
      call. = FALSE
    )
  }

  stats::setNames(expected_paths, as.character(years))
}

parse_lr_transfer_date <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz = "UTC"))
  if (inherits(x, "Date")) return(as.POSIXct(x, tz = "UTC"))
  lubridate::ymd_hm(x, quiet = TRUE, tz = "UTC")
}

#' Clean and standardize one annual source table
clean_data <- function(df, year, base_year = CONFIG$base_year) {
  if (ncol(df) != length(CONFIG$column_name_mapping)) {
    stop(
      "Land Registry file for ", year, " has ", ncol(df),
      " columns; expected ", length(CONFIG$column_name_mapping), ".",
      call. = FALSE
    )
  }

  names(df) <- CONFIG$column_name_mapping

  dplyr::mutate(
    tibble::as_tibble(df),
    postcode = stringr::str_remove_all(.data$postcode, stringr::fixed(" ")),
    date_of_transfer = parse_lr_transfer_date(.data$date_of_transfer),
    qtr_id = (lubridate::year(.data$date_of_transfer) - base_year) * 4L +
      lubridate::quarter(.data$date_of_transfer),
    month_id = (lubridate::year(.data$date_of_transfer) - base_year) * 12L +
      lubridate::month(.data$date_of_transfer)
  )
}

load_year_data <- function(year, file_path) {
  logger::log_info("Loading Land Registry data for {year} from {file_path}")
  rio::import(file_path, setclass = "tbl")
}

#' Load only a path set already validated by `validate_expected_lr_files`
load_all_years <- function(input_paths, base_year = CONFIG$base_year) {
  expected_names <- sprintf("pp-%s.csv", names(input_paths))
  if (!identical(basename(unname(input_paths)), expected_names)) {
    stop("Land Registry input paths must be the validated exact annual files.", call. = FALSE)
  }

  purrr::map2_dfr(names(input_paths), unname(input_paths), function(year, path) {
    clean_data(load_year_data(as.integer(year), path), as.integer(year), base_year)
  })
}

#' Fail hard on source identity and long-run date-window violations
validate_lr_transactions <- function(data, years) {
  required <- c("transaction_id", "date_of_transfer")
  missing_columns <- setdiff(required, names(data))
  if (length(missing_columns) > 0L) {
    stop(
      "Land Registry data is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  transaction_id <- as.character(data$transaction_id)
  if (anyNA(transaction_id)) {
    stop("Land Registry transaction_id contains missing values.", call. = FALSE)
  }
  if (any(!nzchar(trimws(transaction_id)))) {
    stop("Land Registry transaction_id contains empty values.", call. = FALSE)
  }

  duplicate_count <- sum(duplicated(transaction_id))
  if (duplicate_count > 0L) {
    stop(
      "Land Registry transaction_id contains ", duplicate_count,
      " duplicate row(s); the raw files are not a valid same-vintage set.",
      call. = FALSE
    )
  }

  transfer_year <- lubridate::year(data$date_of_transfer)
  if (anyNA(transfer_year) || any(!transfer_year %in% years)) {
    stop(
      "Land Registry date_of_transfer must be non-missing and within ",
      min(years), "-", max(years), ".",
      call. = FALSE
    )
  }

  invisible(data)
}

# Postcode Enrichment and Output Construction
############################################################

assert_unique_postcode_lookup <- function(postcode_data) {
  if (!("postcode" %in% names(postcode_data))) {
    stop("Postcode lookup is missing `postcode`.", call. = FALSE)
  }
  if (anyDuplicated(postcode_data$postcode)) {
    stop("Postcode lookup contains duplicate postcode keys.", call. = FALSE)
  }
  invisible(postcode_data)
}

enrich_lr_postcodes <- function(data, postcode_data) {
  assert_unique_postcode_lookup(postcode_data)
  input_rows <- nrow(data)

  enriched <- dplyr::left_join(data, postcode_data, by = "postcode")
  if (nrow(enriched) != input_rows) {
    stop(
      "Land Registry postcode enrichment was not row-count preserving: ",
      input_rows, " input rows became ", nrow(enriched), " rows.",
      call. = FALSE
    )
  }

  enriched
}

#' Assign stable transaction-grain IDs once, then derive the study subset
build_lr_output_pair <- function(
    enriched_data,
    long_run_years = CONFIG$years,
    study_years = CONFIG$study_years) {
  validate_lr_transactions(enriched_data, long_run_years)

  long_run <- enriched_data |>
    dplyr::mutate(house_id = hash_transaction_id(.data$transaction_id)) |>
    dplyr::relocate("house_id", .before = "transaction_id")

  if (anyNA(long_run$house_id) || anyDuplicated(long_run$house_id)) {
    stop("Hashed Land Registry house_id values must be non-missing and unique.", call. = FALSE)
  }
  if (any(!grepl("^[0-9a-f]{16}$", long_run$house_id))) {
    stop("Hashed Land Registry house_id values must be lowercase 16-character hex.", call. = FALSE)
  }

  long_run <- dplyr::select(long_run, -dplyr::all_of("transaction_id"))
  study <- dplyr::filter(
    long_run,
    lubridate::year(.data$date_of_transfer) %in% study_years
  )

  list(long_run = long_run, study = study)
}

# Candidate Publication
############################################################

new_cleaning_run_stamp <- function(time = Sys.time()) {
  format(as.POSIXct(time, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

cleaning_metadata <- function(
    run_stamp,
    market,
    artifact_role,
    years,
    source_row_count,
    parent_role) {
  if (length(run_stamp) != 1L || is.na(run_stamp) || !nzchar(run_stamp)) {
    stop("Cleaning run stamp must be one non-empty value.", call. = FALSE)
  }
  metadata <- list(
    cleaning_manifest_version = CLEANING_MANIFEST_VERSION,
    cleaning_run_stamp = as.character(run_stamp),
    cleaning_market = as.character(market),
    cleaning_artifact_role = as.character(artifact_role),
    cleaning_year_min = as.character(min(years)),
    cleaning_year_max = as.character(max(years)),
    cleaning_source_row_count = as.character(source_row_count),
    cleaning_parent_role = as.character(parent_role)
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

write_lr_candidates <- function(
    outputs,
    long_run_path = CONFIG$long_run_candidate_path,
    study_path = CONFIG$study_candidate_path,
    run_stamp = new_cleaning_run_stamp()) {
  if (!is.list(outputs) || !all(c("long_run", "study") %in% names(outputs))) {
    stop("`outputs` must contain `long_run` and `study` data frames.", call. = FALSE)
  }
  if (identical(normalizePath(long_run_path, mustWork = FALSE),
                normalizePath(study_path, mustWork = FALSE))) {
    stop("Long-run and study candidates must use distinct paths.", call. = FALSE)
  }
  expected_study <- dplyr::filter(
    outputs$long_run,
    lubridate::year(.data$date_of_transfer) %in% CONFIG$study_years
  )
  if (!isTRUE(all.equal(
    as.data.frame(expected_study),
    as.data.frame(outputs$study),
    check.attributes = FALSE
  ))) {
    stop(
      "LR study candidate must be an unchanged filter of the long-run candidate.",
      call. = FALSE
    )
  }

  long_metadata <- cleaning_metadata(
    run_stamp = run_stamp,
    market = "sales",
    artifact_role = "long_run",
    years = CONFIG$years,
    source_row_count = nrow(outputs$long_run),
    parent_role = "raw_annual_files"
  )
  study_metadata <- cleaning_metadata(
    run_stamp = run_stamp,
    market = "sales",
    artifact_role = "study",
    years = CONFIG$study_years,
    source_row_count = nrow(outputs$long_run),
    parent_role = "long_run"
  )

  write_cleaning_candidate(outputs$long_run, long_run_path, long_metadata)
  write_cleaning_candidate(outputs$study, study_path, study_metadata)

  logger::log_info(
    "Wrote paired LR candidates with run stamp {run_stamp}: long_run={nrow(outputs$long_run)}, study={nrow(outputs$study)}"
  )

  invisible(list(
    long_run_path = long_run_path,
    study_path = study_path,
    run_stamp = run_stamp
  ))
}

# Main Execution
############################################################

main <- function(refresh_postcodes = FALSE) {
  initialise_environment()
  initialise_logging()
  setup_parallel()

  logger::log_info(
    "`refresh_postcodes` is ignored for LR builds; local postcode files are used instead."
  )

  # This is intentionally the first data operation: an incomplete annual set
  # stops before rio reads even one large source file.
  input_paths <- validate_expected_lr_files(CONFIG$input_dir, CONFIG$years)
  raw_data <- load_all_years(input_paths, CONFIG$base_year)
  validate_lr_transactions(raw_data, CONFIG$years)
  logger::log_info(
    "Validated {nrow(raw_data)} LR rows: transaction_id is complete and globally unique; dates lie within {min(CONFIG$years)}-{max(CONFIG$years)}."
  )

  source(
    here::here("scripts", "R", "utils", "postcode_processing_utils.R"),
    local = TRUE
  )
  postcode_result <- get_local_postcode_data_for_sales(
    raw_data,
    lookup_path = CONFIG$local_postcode_lookup_path
  )
  final_data <- enrich_lr_postcodes(raw_data, postcode_result$postcode_data)
  outputs <- build_lr_output_pair(final_data, CONFIG$years, CONFIG$study_years)
  publication <- write_lr_candidates(outputs)

  logger::log_info("Script finished at {Sys.time()}")

  invisible(list(
    output_paths = publication[c("long_run_path", "study_path")],
    run_stamp = publication$run_stamp,
    row_counts = c(long_run = nrow(outputs$long_run), study = nrow(outputs$study)),
    diagnostics = postcode_result$diagnostics
  ))
}

if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
