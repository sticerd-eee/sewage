# ==============================================================================
# Individual EDM Raw File Converter (2021-2023)
# ==============================================================================
#
# Purpose: Load per-company raw EDM files for 2021-2023, normalise workbook and
#          CSV format differences, and export per-company plus year-level
#          parquet outputs for downstream individual-EDM combination steps.
#
# Author: Jacopo Olivieri
# Date: 2024-12-05
# Date Modified: 2026-03-10
#
# Inputs:
#   - data/raw/edm_data/{year}_{water_company}_edm.xlsx
#   - data/raw/edm_data/{year}_{water_company}_edm.xlsb
#   - data/raw/edm_data/{year}_{water_company}_edm.csv
#
# Outputs:
#   - data/processed/edm_data_2021_2023/{year}_{water_company}.parquet
#   - data/processed/edm_data_2021_2023/combined_edm_data_{year}.parquet
#   - output/log/02_convert_individ_raw_data_to_rdata_2021-2023.log
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
  "furrr",
  "future",
  "glue",
  "here",
  "janitor",
  "logger",
  "memuse",
  "parallelly",
  "purrr",
  "readxlsb",
  "rio",
  "tibble"
)

LOG_FILE <- here::here(
  "output", "log",
  "02_convert_individ_raw_data_to_rdata_2021-2023.log"
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

#' Configure parallel processing for improved performance
#' @return NULL
setup_parallel <- function() {
  # Get number of CPU cores
  n_cores <- parallelly::availableCores()
  percentage_cores <- 0.8 # Increase to 1 if needed
  n_workers <- max(1, n_cores * percentage_cores)

  # Memory options
  machine_ram <- memuse::Sys.meminfo()$totalram
  percentage_ram <- 0.7
  ram_limit <- as.numeric(percentage_ram * machine_ram)
  options(future.globals.maxSize = ram_limit)

  # Set up parallel processing
  future::plan(future::multisession, workers = n_workers)
  logger::log_info("Parallel processing initialized with {n_workers} workers")
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "INFO")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  input_dir = here::here("data", "raw", "edm_data"),
  output_dir = here::here("data", "processed"),
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
  )
)

# Data Processing Functions
############################################################

#' Load data for a specific year for all water companies
#' @param year Integer representing the year to process
#' @return List of tibbles containing the raw data for each water company
#' @throws error if file not found
load_data <- function(year) {
  # Validate input
  if (!year %in% CONFIG$years) {
    err_msg <- glue::glue("Invalid year: {year}")
    logger::log_error(err_msg)
    stop(err_msg)
  }

  logger::log_info("Processing data for year: {year}")

  # Convert water company name to filename format
  company_filenames <- make_clean_names(CONFIG$water_company_names)

  # Find matching file using regex
  patterns <- glue::glue("{year}_{company_filenames}_edm\\.\\w+$")
  matched_files <- purrr::map(
    patterns,
    ~ fs::dir_ls(CONFIG$input_dir,
      regexp = .x
    )
  )

  # Extract file path and handle errors
  file_paths <- map2(matched_files, CONFIG$water_company_names, ~ {
    files_found <- .x
    company <- .y

    if (length(files_found) == 0) {
      err_msg <- glue("No data file found for {company} in {year}.")
      logger::log_error(err_msg)
      stop(err_msg)
    } else if (length(files_found) > 1) {
      err_msg <- glue("Multiple files found for {company} in {year}: {paste(files_found, collapse = ', ')}.")
      logger::log_error(err_msg)
      stop(err_msg)
    }

    # Return the single file path
    files_found[[1]]
  }) %>%
    set_names(CONFIG$water_company_names)

  # Import data based on file format
  import_data <- function(file_path) {
    logger::log_info("Loading file: {file_path}")
    # For united utilities in 2021-2022 (only files in xlsb format)
    if (grepl("\\.xlsb$", file_path)) {
      sheet_names <- c("Jan to April", "May to August", "September to December")
      furrr::future_map(sheet_names, function(sheet_name) {
        readxlsb::read_xlsb(file_path, ,
          sheet = sheet_name,
          col_names = TRUE
        )
      },
      .options = furrr::furrr_options(seed = TRUE)
      ) %>%
        bind_rows() %>%
        as_tibble() %>%
        filter(!is.na(.[[1]]))
      # For all other files (xlsx and csv format)
    } else {
      # For south west water in 2021 (data in second sheet)
      if (grepl("2021_south_west_water", file_path)) {
        rio::import(file_path, sheet = "2021 StartStop data", setclass = "tbl")
      } else {
        # For all other files
        rio::import(file_path, setclass = "tbl")
      }
    }
  }

  # Return a list of data frames for all water companies for a given year
  logger::log_info("Processing {length(file_paths)} company files for {year}")

  furrr::future_map(file_paths, import_data,
    .options = furrr::furrr_options(seed = TRUE)
  )
}

#' #' Export processed data to RData format
#' #' @param data List of data frames by company to export
#' #' @return NULL
#' #' @throws error if export fails
#' export_to_rdata <- function(data) {
#'   tryCatch(
#'     {
#'       # File path
#'       rdata_file <- file.path(CONFIG$output_dir, "individual_edm_by_company.Rdata")
#'       dir.create(dirname(rdata_file), recursive = TRUE, showWarnings = FALSE)
#' 
#'       # Save data
#'       edm_data <- data
#'       save(edm_data, file = rdata_file)
#' 
#'       file_size_kb <- round(file.info(rdata_file)$size / 1024, 2)
#'       logger::log_info("Data exported to {rdata_file} ({file_size_kb} KB)")
#'     },
#'     error = function(e) {
#'       logger::log_error("Error exporting data: {e$message}")
#'       stop(glue::glue("Failed to export data: {e$message}"))
#'     }
#'   )
#' }


#' Export processed data to Parquet format
#' @param data List of data frames by company and year to export
#' @return NULL
#' @throws error if export fails
export_to_parquet <- function(data) {
  tryCatch(
    {
      output_dir <- file.path(CONFIG$output_dir, "edm_data_2021_2023")
      fs::dir_create(output_dir)

      logger::log_info("Exporting data to Parquet format")

      # Process each year
      for (year in names(data)) {
        year_data <- data[[year]]

        # Process each company
        for (company in names(year_data)) {
          company_data <- year_data[[company]]
          company_clean_name <- janitor::make_clean_names(company)

          # Create file path
          parquet_file <- file.path(
            output_dir,
            glue::glue("{year}_{company_clean_name}.parquet")
          )

          # Export to parquet
          arrow::write_parquet(company_data, parquet_file)

          file_size_kb <- round(fs::file_info(parquet_file)$size / 1024, 2)
          logger::log_info("Exported {company} data for {year} to {parquet_file} ({file_size_kb} KB)")
        }

        logger::log_info("Completed export for year {year}")
      }

      # Also create a combined file for all years
      combined_data <- list()

      # Function to standardise date columns to character
      standardise_date_columns <- function(df) {
        # Identify potential date columns by name
        date_col_patterns <- c(
          "date", "time", "start", "end", "duration", "discharge"
        )

        # Get column names that might be dates
        potential_date_cols <- grep(
          paste(date_col_patterns, collapse = "|"),
          names(df),
          ignore.case = TRUE,
          value = TRUE
        )

        # Convert each potential date column to character
        if (length(potential_date_cols) > 0) {
          for (col in potential_date_cols) {
            if (col %in% names(df)) {
              # Check if column exists and convert to character
              if (inherits(df[[col]], c("POSIXct", "POSIXlt", "Date"))) {
                df[[col]] <- as.character(df[[col]])
                logger::log_debug("Converted {col} from date/time to character")
              }
            }
          }
        }

        return(df)
      }

      # Flatten the nested list structure with standardised columns
      for (year in names(data)) {
        for (company in names(data[[year]])) {
          company_data <- data[[year]][[company]]

          # Standardise date columns to character format
          company_data <- standardise_date_columns(company_data)

          # Add metadata
          company_data$year <- as.integer(year)
          company_data$water_company <- company

          combined_data[[length(combined_data) + 1]] <- company_data
        }
      }

      # Log column types for debug purposes
      logger::log_info("Preparing to combine {length(combined_data)} datasets")

      # Create separate parquet files for combined data by year
      for (year in names(data)) {
        year_combined <- combined_data[grep(paste0("year = ", year), sapply(combined_data, function(df) as.character(df$year[1])))]

        if (length(year_combined) > 0) {
          year_all_data <- dplyr::bind_rows(year_combined)
          year_combined_file <- file.path(output_dir, glue::glue("combined_edm_data_{year}.parquet"))

          arrow::write_parquet(year_all_data, year_combined_file)

          file_size_mb <- round(fs::file_info(year_combined_file)$size / (1024 * 1024), 2)
          logger::log_info("Created combined dataset for {year} with {nrow(year_all_data)} records: {year_combined_file} ({file_size_mb} MB)")
        }
      }

      logger::log_info("Successfully created year-specific combined datasets")

      # Instead of attempting to create one big combined file, note that we've created year-specific files
      logger::log_info("Note: To avoid column type inconsistencies, data has been exported as separate year-specific combined files")
    },
    error = function(e) {
      logger::log_error("Error exporting data: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

# Main Execution
############################################################

#' Main pipeline orchestration function
#' @return NULL
main <- function() {
  start_time <- Sys.time()

  tryCatch(
    {
      # Initialize environment and logging
      initialise_environment()
      initialise_logging()

      logger::log_info("===== Starting Raw Data Conversion to Parquet (Sequential Mode) =====")

      # Process all years
      logger::log_info("Processing data for years: {paste(CONFIG$years, collapse=', ')}")

      edm_data <- purrr::map(
        CONFIG$years,
        load_data
      ) %>%
        set_names(CONFIG$years)

      # Export the data
      export_to_parquet(edm_data)

      logger::log_info("Processing completed successfully")
    },
    error = function(e) {
      logger::log_error("Fatal error in main execution: {e$message}")
      stop(glue::glue("Script execution failed: {e$message}"))
    },
    finally = {
      end_time <- Sys.time()
      duration <- end_time - start_time
      formatted_duration <- format(duration)
      logger::log_info(
        "===== Data Conversion Finished in {formatted_duration} ====="
      )
      logger::log_info("Script finished at {Sys.time()}")
    }
  )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}
