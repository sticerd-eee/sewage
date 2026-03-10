# ==============================================================================
# Individual EDM Parquet Combiner (2021-2023)
# ==============================================================================
#
# Purpose: Combine the per-company EDM Parquet files for 2021-2023, standardise
#          their schema and datetime fields, and export the canonical combined
#          dataset used by downstream spill analysis steps.
#
# Author: Jacopo Olivieri
# Date: 2024-12-09
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/processed/edm_data_2021_2023/{year}_{water_company}.parquet
#
# Outputs:
#   - data/processed/combined_edm_data.parquet
#   - output/log/03_combine_individ_edm_data_2021-2023.log
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

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "fs",
  "glue",
  "janitor",
  "logger",
  "lubridate",
  "purrr",
  "stringr"
)

LOG_FILE <- here::here(
  "output", "log",
  "03_combine_individ_edm_data_2021-2023.log"
)

check_required_packages(REQUIRED_PACKAGES)

# Setup Functions
############################################################

#' Attach the packages used unqualified in this script
#' @return NULL
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
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

#' Build the expected company-year Parquet inventory from CONFIG
#' @return Data frame of expected Parquet files and paths
build_expected_input_manifest <- function() {
  expand.grid(
    year = CONFIG$years,
    water_company = CONFIG$water_company_names,
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      year = as.integer(year),
      company_clean_name = vapply(water_company, janitor::make_clean_names, character(1)),
      filename = paste0(year, "_", company_clean_name, ".parquet"),
      path = file.path(CONFIG$input_dir, filename)
    ) %>%
    arrange(year, water_company)
}

#' Collapse example values for logs and errors
#' @param values Vector of values to sample
#' @param max_examples Maximum number of unique samples to include
#' @return Character scalar with sample values
format_value_samples <- function(values, max_examples = 5L) {
  samples <- unique(as.character(values))
  samples <- samples[!is.na(samples)]
  samples <- head(samples, max_examples)

  if (length(samples) == 0) {
    return("none")
  }

  paste(shQuote(samples), collapse = ", ")
}

#' Detect known non-event datetime markers
#' @param values Vector of raw datetime values
#' @return Logical vector
is_known_non_event_datetime <- function(values) {
  normalised_values <- values %>%
    as.character() %>%
    stringr::str_squish() %>%
    stringr::str_to_lower()

  !is.na(normalised_values) &
    normalised_values %in% CONFIG$known_non_event_datetime_tokens
}

#' Initialise parsed datetime columns with stable POSIXct types
#' @param df Data frame to augment
#' @param parsed_cols Parsed datetime column names
#' @param tz Time zone for POSIXct columns
#' @return Data frame with parsed datetime columns
initialise_parsed_datetime_columns <- function(df, parsed_cols, tz = "UTC") {
  parsed_template <- as.POSIXct(rep(NA_real_, nrow(df)), origin = "1970-01-01", tz = tz)

  for (parsed_col in parsed_cols) {
    df[[parsed_col]] <- parsed_template
  }

  df
}

# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  input_dir = here::here("data", "processed", "edm_data_2021_2023"),
  processed_dir = here::here("data", "processed"),
  water_company_names = c(
    "Anglian Water",
    "Welsh Water",
    "Northumbrian Water",
    "Severn Trent Water",
    "South West Water",
    "Southern Water",
    "Thames Water",
    "United Utilities",
    "Wessex Water",
    "Yorkshire Water"
  ),
  datetime_formats = c(
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%OSZ",
    "%d-%m-%YT%H:%M:%SZ",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%OS%z"
  ),
  known_non_event_datetime_tokens = c(
    "",
    "na",
    "n/a",
    "null",
    "no discharges"
  ),
  column_name_mapping = c(
    "activity_reference_on_permit" = "activity_reference",
    "wa_sc_supplementary_permit_ref_optional" = "permit_reference_wa_sc",
    "ea_permit_reference_ea_consents_database" = "permit_reference_ea",
    "ea_id" = "unique_id",
    "yw_discharge_urn" = "unique_id",
    "srn" = "unique_id",
    "storm_discharge_asset_type_treatment_works" = "asset_type"
  )
)

# Data Processing Functions
############################################################

#' Load Parquet files from the output of script 02
#' @return List of dataframes for each water company for each year
load_data <- function() {
  logger::log_info("Loading data from Parquet files")

  # Check if input directory exists
  if (!dir.exists(CONFIG$input_dir)) {
    stop(glue::glue("Input directory not found: {CONFIG$input_dir}"))
  }

  # Find all Parquet files
  parquet_files <- fs::dir_ls(CONFIG$input_dir, regexp = "\\d{4}_.*\\.parquet$")

  # Skip the combined file if it exists
  parquet_files <- parquet_files[!grepl("combined_edm_data", parquet_files)]

  if (length(parquet_files) == 0) {
    stop("No Parquet files found in input directory")
  }

  logger::log_info("Discovered {length(parquet_files)} candidate Parquet files")

  expected_manifest <- build_expected_input_manifest()
  discovered_filenames <- basename(parquet_files)

  unexpected_files <- setdiff(discovered_filenames, expected_manifest$filename)
  if (length(unexpected_files) > 0) {
    logger::log_warn(
      "Ignoring {length(unexpected_files)} unexpected Parquet files: {paste(unexpected_files, collapse = ', ')}"
    )
  }

  missing_files <- setdiff(expected_manifest$filename, discovered_filenames)
  if (length(missing_files) > 0) {
    stop(
      glue::glue(
        "Missing expected company-year Parquet files ({length(missing_files)} of {nrow(expected_manifest)}): {paste(missing_files, collapse = ', ')}"
      )
    )
  }

  logger::log_info("Validated expected Parquet inventory: {nrow(expected_manifest)} company-year files")

  result <- list()

  for (i in seq_len(nrow(expected_manifest))) {
    manifest_row <- expected_manifest[i, ]
    file_path <- manifest_row$path[[1]]
    year_key <- as.character(manifest_row$year[[1]])
    company_name <- manifest_row$water_company[[1]]

    if (!year_key %in% names(result)) {
      result[[year_key]] <- list()
    }

    tryCatch(
      {
        df <- arrow::read_parquet(file_path) %>% collect()
        logger::log_info("Successfully loaded {nrow(df)} rows from {basename(file_path)}")
        result[[year_key]][[company_name]] <- df
      },
      error = function(e) {
        stop(
          glue::glue("Failed to read expected Parquet file {basename(file_path)}: {e$message}")
        )
      }
    )
  }

  logger::log_info(
    "Successfully loaded {sum(lengths(result))} company-year datasets across {length(result)} years"
  )
  return(result)
}

#' Clean variable names, remove NA rows, add year and company name columns
#' @param df Data frame to clean
#' @param df_name Name of the dataframe
#' @return Cleaned data frame
clean_data <- function(df, df_name) {
  tryCatch(
    {
      # Get year and water company name
      df_name <- str_remove(df_name, "^\\d{4}_") # remove initial YYYY_
      year <- as.integer(stringr::str_extract(df_name, "\\d{4}$"))
      water_company <- stringr::str_trim(
        stringr::str_replace(df_name, "\\d{4}$", "")
      )

      cleaned <- df %>%
        # remove duplicate rows
        unique() %>%
        # clean variable names
        janitor::clean_names() %>%
        rename_with(
          ~ {
            lower_name <- tolower(.)

            case_when(
              grepl("site", lower_name) & grepl("ea", lower_name) ~ "site_name_ea",
              grepl("site", lower_name) & grepl("wa_sc", lower_name) ~ "site_name_wa_sc",
              grepl("site", lower_name) & grepl("name", lower_name) ~ "site_name_ea",
              grepl("start|begin", lower_name) ~ "start_time",
              grepl("end|stop", lower_name) ~ "end_time",
              . %in% names(CONFIG$column_name_mapping) ~ CONFIG$column_name_mapping[.],
              TRUE ~ .
            )
          }
        ) %>%
        # remove rows with NA in event duration monitoring data
        filter(if_any(
          .cols = c(start_time, end_time),
          ~ !is.na(.)
        )) %>%
        # add year and water company columns
        mutate(
          year = year,
          water_company = water_company,
          .before = everything()
        )

      # Company specific variable renaming
      ## Southern Water, South West Water 2022, Thames Water 2023
      if (water_company == "Southern Water" |
        (water_company == "South West Water" & year == 2022L) |
        (water_company == "Thames Water" & year == 2023L)) {
        cleaned <- cleaned %>%
          rename(site_name_wa_sc = site_name_ea)
      }

      logger::log_info("Successfully cleaned data for {df_name}")
      return(cleaned)
    },
    error = function(e) {
      msg <- glue::glue("Error cleaning data for {df_name}: {e$message}")
      logger::log_error(msg)
      stop(msg)
    }
  )
}

#' Parse datetime columns using multiple methods to standardize formats
#' @param df Data frame containing datetime columns
#' @param df_name Dataset name for logs and errors
#' @return Data frame with parsed datetime columns
clean_datetime_columns <- function(df, df_name) {
  # Setup parsing configuration
  time_cols <- c("start_time", "end_time")
  parsed_cols <- paste0("parsed_", time_cols)
  formats <- CONFIG$datetime_formats
  tz <- "UTC"

  start_missingish <- is.na(df$start_time) | is_known_non_event_datetime(df$start_time)
  end_missingish <- is.na(df$end_time) | is_known_non_event_datetime(df$end_time)
  known_non_event_rows <- start_missingish & end_missingish
  start_only_missing_rows <- start_missingish & !end_missingish
  end_only_missing_rows <- !start_missingish & end_missingish
  incomplete_datetime_rows <- start_only_missing_rows | end_only_missing_rows

  if (any(known_non_event_rows)) {
    logger::log_info(
      paste0(
        "Dropping ", sum(known_non_event_rows),
        " rows where both timestamps are known non-event markers in ", df_name,
        " (start_time ", sum(start_missingish),
        ", end_time ", sum(end_missingish), ")"
      )
    )
  }

  if (any(incomplete_datetime_rows)) {
    logger::log_warn(
      paste0(
        "Dropping ", sum(incomplete_datetime_rows),
        " rows with only one missing/non-event datetime value in ", df_name,
        " (start_time only ", sum(start_only_missing_rows),
        ", end_time only ", sum(end_only_missing_rows), ")"
      )
    )
  }

  rows_to_drop <- known_non_event_rows | incomplete_datetime_rows
  if (any(rows_to_drop)) {
    df <- df[!rows_to_drop, , drop = FALSE]
  }

  # Initialize parsed columns
  df <- initialise_parsed_datetime_columns(df, parsed_cols, tz)

  logger::log_debug("Starting datetime parsing for {df_name}")

  # Parse dates using multiple methods
  for (i in seq_along(time_cols)) {
    col <- time_cols[i]
    parsed_col <- parsed_cols[i]
    original_values <- df[[col]]

    logger::log_debug("Processing column {col} for {df_name}")

    # 1. Handle existing POSIXct values
    if (inherits(original_values, "POSIXct")) {
      df[[parsed_col]] <- original_values
      logger::log_debug("Column {col} already in POSIXct format for {df_name}")
      next
    }

    # 2. Try parsing ISO format dates
    still_na <- is.na(df[[parsed_col]])
    if (inherits(original_values, "character")) {
      if (any(still_na)) {
        for (fmt in formats) {
          if (!any(still_na)) break

          temp_times <- suppressWarnings(
            as.POSIXct(original_values[still_na],
              format = fmt,
              tz = tz
            )
          )

          good_parse <- !is.na(temp_times)
          if (any(good_parse)) {
            df[[parsed_col]][still_na][good_parse] <- temp_times[good_parse]
            still_na[still_na][good_parse] <- FALSE
            logger::log_debug(
              "Successfully parsed {sum(good_parse)} values in {df_name} with format: {fmt}"
            )
          }
        }
      }
    }

    # 3. Try Excel date conversion for remaining NAs
    if (any(still_na)) {
      excel_vals <- suppressWarnings(as.numeric(original_values[still_na]))
      valid_excel <- !is.na(excel_vals) & excel_vals > 1 & excel_vals < 73050

      if (any(valid_excel)) {
        excel_dates <- as.POSIXct(
          (excel_vals[valid_excel] - 25569) * 86400,
          origin = "1970-01-01",
          tz = tz
        )
        df[[parsed_col]][still_na][valid_excel] <- excel_dates
        logger::log_debug("Successfully parsed {sum(valid_excel)} Excel date values in {df_name}")
      }
    }

    # 4. Try lubridate ymd_hms for remaining unparsed values
    still_na <- is.na(df[[parsed_col]]) # Refresh NA check
    if (any(still_na)) {
      temp_times <- suppressWarnings(
        lubridate::ymd_hms(original_values[still_na], tz = tz)
      )
      good_parse <- !is.na(temp_times)
      if (any(good_parse)) {
        df[[parsed_col]][still_na][good_parse] <- temp_times[good_parse]
        logger::log_debug("Successfully parsed {sum(good_parse)} values in {df_name} with ymd_hms")
      }
    }
  }

  # Final state and validation
  final_nas <- sapply(parsed_cols, function(col) sum(is.na(df[[col]])))
  counts_str <- paste(names(final_nas), final_nas, collapse = ", ")
  logger::log_info("Final parsing results for {df_name}: {counts_str}")
  total_nas <- sum(final_nas)
  if (total_nas > 0) {
    start_examples <- format_value_samples(df$start_time[is.na(df$parsed_start_time)])
    end_examples <- format_value_samples(df$end_time[is.na(df$parsed_end_time)])
    stop(
      glue::glue(
        paste0(
          "Unparseable datetime values in ", df_name, ": ",
          "parsed_start_time ", final_nas[["parsed_start_time"]], ", ",
          "parsed_end_time ", final_nas[["parsed_end_time"]], ". ",
          "start_time examples: ", start_examples, ". ",
          "end_time examples: ", end_examples, "."
        )
      )
    )
  }
  logger::log_info("Datetime parsing completed for {df_name}")

  # Clean variable names and prepare for merge
  df %>%
    mutate(across(c(start_time, end_time), as.character)) %>%
    rename(
      start_time_og = start_time,
      end_time_og = end_time,
      start_time = parsed_start_time,
      end_time = parsed_end_time
    ) %>%
    return()
}

#' Clean and filter time ranges to ensure proper event sequencing
#' @param df Data frame with datetime columns
#' @param df_name Dataset name for logs and errors
#' @return Data frame with cleaned time ranges
clean_time_range <- function(df, df_name) {
  tryCatch(
    {
      if (any(is.na(df$start_time)) || any(is.na(df$end_time))) {
        stop(glue::glue("Unexpected NA datetimes reached clean_time_range() for {df_name}"))
      }

      start_year <- lubridate::year(df$start_time)
      end_year <- lubridate::year(df$end_time)
      year_start <- as.POSIXct(paste0(df$year, "-01-01 00:00:00"), tz = "UTC")
      start_before_year_with_end_outside_year <- start_year < df$year & end_year != df$year
      end_after_year_with_start_outside_year <- end_year > df$year & start_year != df$year
      ends_after_year_start <- df$end_time > year_start

      clean_df <- df %>%
        filter(
          # If start_time is before the year, end_time must be in that year
          !start_before_year_with_end_outside_year &
            # If end_time is after the year, start_time must be in that year
            !end_after_year_with_start_outside_year &
            # end_time is strictly after year start
            ends_after_year_start
        ) %>%
        # Rectify spills that should actually start one day later
        mutate(
          end_time = if_else(
            start_time > end_time &
              (end_time + days(1)) > start_time &
              format(end_time, "%H:%M:%S") == "00:00:00",
            end_time + days(1),
            end_time
          )
        ) %>%
        # Eliminate spills that have negative duration
        filter(end_time > start_time)

      logger::log_info("Successfully cleaned time ranges for {df_name}")
      return(clean_df)
    },
    error = function(e) {
      msg <- glue::glue("Error filtering time ranges: {e$message}")
      logger::log_error(msg)
      stop(msg)
    }
  )
}

#' Combine and export data to Parquet format
#' @param df List of dataframes to combine
#' @return NULL
export_data <- function(df) {
  tryCatch(
    {
      # Combine all dataframes
      vars <- c(
        "water_company", "year", "site_name_ea", "site_name_wa_sc",
        "start_time", "end_time", "start_time_og", "end_time_og"
      )
      combined_df <- bind_rows(df) %>%
        select(
          any_of(vars),
          everything()
        )

      parquet_file <- file.path(CONFIG$processed_dir, "combined_edm_data.parquet")
      temp_file <- file.path(
        CONFIG$processed_dir,
        paste0(
          "combined_edm_data.tmp-",
          Sys.getpid(), "-",
          format(Sys.time(), "%Y%m%d%H%M%S"),
          ".parquet"
        )
      )

      on.exit(
        {
          if (file.exists(temp_file)) {
            file.remove(temp_file)
          }
        },
        add = TRUE
      )

      arrow::write_parquet(combined_df, temp_file)

      if (!file.exists(temp_file)) {
        stop("Temporary Parquet output was not created")
      }

      validation_df <- arrow::read_parquet(temp_file) %>% collect()
      if (nrow(validation_df) != nrow(combined_df)) {
        stop(
          glue::glue(
            "Temporary Parquet validation failed: expected {nrow(combined_df)} rows, found {nrow(validation_df)}"
          )
        )
      }
      if (!identical(names(validation_df), names(combined_df))) {
        stop("Temporary Parquet validation failed: column names changed during staged write")
      }

      if (!file.rename(temp_file, parquet_file)) {
        stop(glue::glue("Failed to promote staged Parquet output to {parquet_file}"))
      }

      logger::log_info("Data exported successfully to {parquet_file}")
    },
    error = function(e) {
      msg <- glue::glue("Error exporting data: {e$message}")
      logger::log_error(msg)
      stop(msg)
    }
  )
}

# Main Execution
############################################################

#' Main pipeline orchestration function
main <- function() {
  start_time <- Sys.time()

  tryCatch(
    {
      # Setup
      initialise_environment()
      initialise_logging()

      # Load data
      logger::log_info("===== Starting Individual EDM Data Processing (2021-2023) =====")
      raw_data <- load_data()

      # Process data
      dataset_count <- sum(lengths(raw_data))
      logger::log_info("Processing data for {dataset_count} datasets")
      processed_data <- raw_data %>%
        imap(~ setNames(.x, paste(names(.x), .y))) %>%
        list_flatten() %>%
        purrr::imap(~ clean_data(.x, .y)) %>%
        purrr::imap(~ clean_datetime_columns(.x, .y)) %>%
        purrr::imap(~ clean_time_range(.x, .y))

      # Export data
      export_data(processed_data)

      logger::log_info("Processing completed successfully")
    },
    error = function(e) {
      msg <- glue::glue("Fatal error in main execution: {e$message}")
      logger::log_error(msg)
      stop(msg)
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info(
        "===== Individual EDM Data Processing Finished in {formatted_duration} ====="
      )
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}
