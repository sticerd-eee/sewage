# ==============================================================================
# HM Land Registry Price Paid Data Cleaner
# ==============================================================================
#
# Purpose: Clean the 1995-2024 Land Registry Price Paid history from an explicit
#          year-to-source manifest, enrich it with postcode attributes, assign
#          content-stable transaction-grain `house_id` values, and write paired
#          candidate datasets. The 2021-2024 study file is always derived from
#          the long-run table.
#
# Source manifest (one validated source set per year; never both):
#   - 1995-2013: the two split files pp-YYYY-part1.csv and pp-YYYY-part2.csv
#   - 2014-2024: the exact complete annual files pp-YYYY.csv consumed since the
#     2026-08-14 vintage. Split files that coexist for 2014-2017 are never read.
#
# Inputs:
#   - data/raw/lr_house_price/pp-1995-part1.csv ... pp-2013-part2.csv
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

CLEANING_MANIFEST_VERSION <- "2"
# Years served by the two-part split download and years served by the exact
# complete annual file. Together they are the locked long-run window.
LR_SPLIT_YEARS <- 1995:2013
LR_COMPLETE_YEARS <- 2014:2024
LR_EXPECTED_YEARS <- c(LR_SPLIT_YEARS, LR_COMPLETE_YEARS)
LR_SPLIT_PARTS <- c("part1", "part2")
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

#' The explicit year-to-source manifest for the locked long-run window
#'
#' One row per source file. Split years list both parts; complete years list
#' exactly the complete annual file. A year never appears under both kinds, so
#' the overlapping 2014-2017 split files can never be consumed alongside their
#' complete files. Existence is not checked here: see
#' `validate_expected_lr_files()`.
#' @return Tibble with year, source_id, kind, file and path
lr_source_manifest <- function(input_dir, years = LR_EXPECTED_YEARS) {
  years <- as.integer(years)
  if (!identical(years, as.integer(LR_EXPECTED_YEARS))) {
    stop(
      "Land Registry cleaning requires the exact ",
      min(LR_EXPECTED_YEARS), "-", max(LR_EXPECTED_YEARS),
      " long-run window.",
      call. = FALSE
    )
  }
  rows <- lapply(years, function(year) {
    if (year %in% LR_SPLIT_YEARS) {
      files <- sprintf("pp-%d-%s.csv", year, LR_SPLIT_PARTS)
      kind <- "split_part"
    } else {
      files <- sprintf("pp-%d.csv", year)
      kind <- "complete_annual"
    }
    tibble::tibble(
      year = year,
      source_id = sub("\\.csv$", "", files),
      kind = kind,
      file = files,
      path = file.path(input_dir, files)
    )
  })
  manifest <- dplyr::bind_rows(rows)
  if (anyDuplicated(manifest$file) || anyDuplicated(manifest$source_id)) {
    stop("Land Registry source manifest lists a file twice.", call. = FALSE)
  }
  kinds_per_year <- tapply(manifest$kind, manifest$year, function(k) length(unique(k)))
  if (any(kinds_per_year != 1L)) {
    stop("A Land Registry year cannot mix split and complete sources.", call. = FALSE)
  }
  manifest
}

#' Validate that every manifest source exists before reading any file
#' @return The manifest, with rows in configured year (and part) order
validate_expected_lr_files <- function(input_dir, years) {
  manifest <- lr_source_manifest(input_dir, years)
  missing <- manifest$file[!file.exists(manifest$path)]
  if (length(missing) > 0L) {
    stop(
      "Land Registry source manifest is incomplete. Missing file(s): ",
      paste(missing, collapse = ", "),
      ". Expected pp-1995-part1.csv/pp-1995-part2.csv through pp-2013-part2.csv ",
      "and the complete annual files pp-2014.csv through pp-2024.csv.",
      call. = FALSE
    )
  }
  manifest
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

#' Load and clean every source of one manifest year
#'
#' Each source is cleaned and validated against its own manifest row before
#' the parts of a split year are stacked, so a wrong-year or duplicated source
#' is named individually. Returns the cleaned rows of that year only.
load_manifest_year <- function(manifest, year, base_year = CONFIG$base_year) {
  rows <- manifest[manifest$year == as.integer(year), , drop = FALSE]
  if (nrow(rows) == 0L) {
    stop("Year ", year, " is not in the Land Registry source manifest.", call. = FALSE)
  }
  parts <- lapply(seq_len(nrow(rows)), function(i) {
    cleaned <- clean_data(load_year_data(as.integer(year), rows$path[[i]]), as.integer(year), base_year)
    validate_lr_year_source(cleaned, year, rows$source_id[[i]])
    cleaned
  })
  combined <- dplyr::bind_rows(parts)
  # Split parts of one year must not overlap.
  duplicate_count <- sum(duplicated(combined$transaction_id))
  if (duplicate_count > 0L) {
    stop(
      "Land Registry sources for ", year, " (", paste(rows$source_id, collapse = ", "),
      ") share ", duplicate_count, " transaction_id value(s).",
      call. = FALSE
    )
  }
  combined
}

#' Distinct cleaned postcodes and row counts across every manifest source
#'
#' A cheap first pass over the postcode column alone: the postcode lookup is
#' resolved once for the union of postcodes (per-postcode enrichment does not
#' depend on which rows request it), and the per-source row counts give the
#' long-run source row count before the first full source is read.
scan_manifest_postcodes <- function(manifest) {
  postcode_column <- match("postcode", CONFIG$column_name_mapping)
  scans <- lapply(seq_len(nrow(manifest)), function(i) {
    column <- data.table::fread(
      manifest$path[[i]],
      select = postcode_column,
      header = FALSE,
      colClasses = "character",
      showProgress = FALSE
    )[[1L]]
    list(
      n_rows = length(column),
      postcodes = unique(stringr::str_remove_all(column, stringr::fixed(" ")))
    )
  })
  list(
    source_row_counts = stats::setNames(
      vapply(scans, function(x) x$n_rows, numeric(1)),
      manifest$source_id
    ),
    postcodes = unique(unlist(lapply(scans, function(x) x$postcodes), use.names = FALSE))
  )
}

#' Fail hard when one cleaned source disagrees with its manifest year
#'
#' Every source file must carry non-missing, non-empty, within-file unique
#' transaction ids and parseable transfer dates that all fall in the year the
#' manifest declares for it. Global uniqueness across sources is checked
#' separately once every source has been read.
validate_lr_year_source <- function(data, year, source_id) {
  transaction_id <- as.character(data$transaction_id)
  if (anyNA(transaction_id) || any(!nzchar(trimws(transaction_id)))) {
    stop(
      "Land Registry source ", source_id,
      " contains missing or empty transaction_id values.",
      call. = FALSE
    )
  }
  duplicate_count <- sum(duplicated(transaction_id))
  if (duplicate_count > 0L) {
    stop(
      "Land Registry source ", source_id, " contains ", duplicate_count,
      " duplicated transaction_id value(s).",
      call. = FALSE
    )
  }
  transfer_year <- lubridate::year(data$date_of_transfer)
  if (anyNA(transfer_year)) {
    stop(
      "Land Registry source ", source_id, " has ", sum(is.na(transfer_year)),
      " unparseable date_of_transfer value(s).",
      call. = FALSE
    )
  }
  outside <- transfer_year != as.integer(year)
  if (any(outside)) {
    stop(
      "Land Registry source ", source_id, " has ", sum(outside),
      " row(s) dated outside its manifest year ", year,
      " (observed ", min(transfer_year), "-", max(transfer_year), ").",
      call. = FALSE
    )
  }
  invisible(data)
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

# The persisted long-run schema. Every yearly table is cast to it before it is
# appended, so per-file type inference (a year whose secondary address is
# always empty, say) cannot change the column types of the candidate.
lr_output_schema <- function() {
  utf8_cols <- function(names) lapply(stats::setNames(names, names), function(x) arrow::utf8())
  arrow::schema(c(
    list(house_id = arrow::utf8(), price = arrow::int32(),
         date_of_transfer = arrow::timestamp("us", timezone = "UTC")),
    utf8_cols(c(
      "postcode", "property_type", "old_new", "duration", "paon", "saon",
      "street", "locality", "town_city", "district", "county", "ppd_category",
      "record_status"
    )),
    list(qtr_id = arrow::float64(), month_id = arrow::float64(),
         quality = arrow::int32(), easting = arrow::int32(),
         northing = arrow::int32(), longitude = arrow::float64(),
         latitude = arrow::float64()),
    utf8_cols(c(
      "country", "nhs_ha", "region", "lsoa", "msoa", "admin_county", "admin_ward"
    ))
  ))
}

#' Cast one cleaned yearly frame to the persisted long-run schema
cast_lr_output <- function(data, schema = lr_output_schema()) {
  missing <- setdiff(names(schema), names(data))
  extra <- setdiff(names(data), names(schema))
  if (length(missing) > 0L || length(extra) > 0L) {
    stop(
      "Cleaned Land Registry columns do not match the long-run schema",
      if (length(missing) > 0L) paste0("; missing: ", paste(missing, collapse = ", ")),
      if (length(extra) > 0L) paste0("; unexpected: ", paste(extra, collapse = ", ")),
      ".",
      call. = FALSE
    )
  }
  arrow::Table$create(as.data.frame(data)[names(schema)])$cast(schema)
}

#' Open the paired streaming candidate writers
#'
#' Yearly tables are appended in manifest order to staged files beside the
#' candidate paths; `close()` promotes the staged files onto the candidate
#' paths only after every year was written, and `abandon()` removes them.
open_lr_candidate_writers <- function(
    long_run_path,
    study_path,
    source_row_count,
    run_stamp = new_cleaning_run_stamp(),
    long_run_years = CONFIG$years,
    study_years = CONFIG$study_years) {
  assert_candidate_output_path(long_run_path)
  assert_candidate_output_path(study_path)
  if (identical(normalizePath(long_run_path, mustWork = FALSE),
                normalizePath(study_path, mustWork = FALSE))) {
    stop("Long-run and study candidates must use distinct paths.", call. = FALSE)
  }
  metadata <- list(
    long_run = cleaning_metadata(run_stamp, "sales", "long_run", long_run_years,
                                 source_row_count, "raw_annual_files"),
    study = cleaning_metadata(run_stamp, "sales", "study", study_years,
                              source_row_count, "long_run")
  )
  targets <- c(long_run = long_run_path, study = study_path)
  schema <- lr_output_schema()
  staged <- vapply(names(targets), function(role) {
    dir.create(dirname(targets[[role]]), recursive = TRUE, showWarnings = FALSE)
    tempfile(
      pattern = paste0(".", basename(targets[[role]]), "-"),
      tmpdir = dirname(targets[[role]]),
      fileext = ".parquet"
    )
  }, character(1))
  sinks <- lapply(staged, arrow::FileOutputStream$create)
  writers <- lapply(names(targets), function(role) {
    arrow::ParquetFileWriter$create(
      schema$WithMetadata(metadata[[role]]),
      sinks[[role]],
      properties = arrow::ParquetWriterProperties$create(names(schema))
    )
  })
  names(writers) <- names(targets)
  state <- new.env()
  state$rows <- c(long_run = 0, study = 0)
  state$open <- TRUE
  write_year <- function(outputs, year) {
    if (!isTRUE(state$open)) stop("Candidate writers are closed.", call. = FALSE)
    long_table <- cast_lr_output(outputs$long_run, schema)
    writers$long_run$WriteTable(long_table, chunk_size = 1000000L)
    state$rows[["long_run"]] <- state$rows[["long_run"]] + long_table$num_rows
    if (as.integer(year) %in% study_years) {
      study_table <- cast_lr_output(outputs$study, schema)
      if (study_table$num_rows != long_table$num_rows) {
        stop("Study rows for ", year, " must equal the long-run rows of that year.", call. = FALSE)
      }
      writers$study$WriteTable(study_table, chunk_size = 1000000L)
      state$rows[["study"]] <- state$rows[["study"]] + study_table$num_rows
    } else if (nrow(outputs$study) != 0L) {
      stop("Study rows outside the study years for ", year, ".", call. = FALSE)
    }
    invisible(state$rows)
  }
  finish <- function() {
    for (role in names(writers)) {
      writers[[role]]$Close()
      sinks[[role]]$close()
    }
    state$open <- FALSE
  }
  abandon <- function() {
    if (isTRUE(state$open)) try(finish(), silent = TRUE)
    unlink(staged)
    invisible(NULL)
  }
  close <- function() {
    finish()
    if (state$rows[["long_run"]] != source_row_count) {
      unlink(staged)
      stop(
        "Long-run candidate rows (", state$rows[["long_run"]],
        ") differ from the manifest source rows (", source_row_count, ").",
        call. = FALSE
      )
    }
    for (role in names(targets)) {
      if (!file.rename(staged[[role]], targets[[role]])) {
        unlink(staged)
        stop("Failed to promote staged cleaning candidate to ", targets[[role]], ".", call. = FALSE)
      }
    }
    logger::log_info(
      "Wrote paired LR candidates with run stamp {run_stamp}: long_run={state$rows[['long_run']]}, study={state$rows[['study']]}"
    )
    invisible(list(
      long_run_path = long_run_path,
      study_path = study_path,
      run_stamp = run_stamp,
      row_counts = state$rows
    ))
  }
  list(write_year = write_year, close = close, abandon = abandon, run_stamp = run_stamp)
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

  # This is intentionally the first data operation: an incomplete manifest
  # stops before rio reads even one large source file.
  manifest <- validate_expected_lr_files(CONFIG$input_dir, CONFIG$years)
  logger::log_info(
    "Source manifest: {nrow(manifest)} files for {length(unique(manifest$year))} years ({sum(manifest$kind == 'split_part')} split parts, {sum(manifest$kind == 'complete_annual')} complete annual files)."
  )
  scan <- scan_manifest_postcodes(manifest)
  source_row_count <- sum(scan$source_row_counts)
  logger::log_info(
    "Postcode scan: {source_row_count} source rows, {length(scan$postcodes)} distinct postcodes."
  )

  source(
    here::here("scripts", "R", "utils", "postcode_processing_utils.R"),
    local = TRUE
  )
  postcode_result <- get_local_postcode_data_for_sales(
    tibble::tibble(postcode = scan$postcodes),
    lookup_path = CONFIG$local_postcode_lookup_path
  )
  postcode_data <- postcode_result$postcode_data
  rm(scan)

  # One year in memory at a time; ids are kept for the global uniqueness check.
  writers <- open_lr_candidate_writers(
    CONFIG$long_run_candidate_path,
    CONFIG$study_candidate_path,
    source_row_count = source_row_count
  )
  on.exit(writers$abandon(), add = TRUE)
  house_ids <- vector("list", length(CONFIG$years))
  row_counts <- stats::setNames(numeric(length(CONFIG$years)), CONFIG$years)
  for (i in seq_along(CONFIG$years)) {
    year <- CONFIG$years[[i]]
    year_data <- load_manifest_year(manifest, year, CONFIG$base_year)
    enriched <- enrich_lr_postcodes(year_data, postcode_data)
    rm(year_data)
    outputs <- build_lr_output_pair(enriched, year, CONFIG$study_years)
    rm(enriched)
    writers$write_year(outputs, year)
    house_ids[[i]] <- outputs$long_run$house_id
    row_counts[[i]] <- nrow(outputs$long_run)
    logger::log_info("Cleaned {year}: {nrow(outputs$long_run)} rows ({nrow(outputs$study)} study rows).")
    rm(outputs)
    invisible(gc())
  }
  all_ids <- unlist(house_ids, use.names = FALSE)
  rm(house_ids)
  duplicate_count <- sum(duplicated(all_ids))
  if (duplicate_count > 0L) {
    stop(
      "Hashed Land Registry house_id values repeat across years (", duplicate_count,
      " duplicate row(s)); the source manifest is not a valid transaction set.",
      call. = FALSE
    )
  }
  logger::log_info(
    "Validated {length(all_ids)} LR rows: house_id is globally unique; every source lies within its manifest year ({min(CONFIG$years)}-{max(CONFIG$years)})."
  )
  rm(all_ids)
  publication <- writers$close()
  on.exit(NULL)

  logger::log_info("Script finished at {Sys.time()}")

  invisible(list(
    output_paths = publication[c("long_run_path", "study_path")],
    run_stamp = publication$run_stamp,
    row_counts = publication$row_counts,
    rows_by_year = row_counts,
    diagnostics = postcode_result$diagnostics
  ))
}

if (sys.nframe() == 0) {
  main(refresh_postcodes = FALSE)
}
