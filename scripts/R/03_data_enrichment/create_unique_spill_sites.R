############################################################
# Create Unique Spill Sites
# Project: Sewage
# Date: 03/02/2025
# Author: Jacopo Olivieri
############################################################

#' This script creates unique spill sites from the merged EDM data
#' by cleaning location data and extracting distinct sites.

# Set Up Functions
############################################################

#' Initialize the R environment with required packages and settings
#' @return NULL
initialise_environment <- function() {
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }
  
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue", 
    "fs", "rnrfa", "arrow"
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
  log_path <- here::here("output", "log", "13_create_unique_spill_sites.log")
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  logger::log_appender(logger::appender_file(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::DEBUG)
  logger::log_info("Script started at {Sys.time()}")
}


# Configuration
############################################################

CONFIG <- list(
  spill_data_path = here::here(
    "data", "processed", "matched_events_annual_data", 
    "matched_events_annual_data.parquet"),
  unique_spills_parquet = here::here(
    "data", "processed", "unique_spill_sites.parquet"),
  unique_spills_xlsx = here::here(
    "data", "processed", "unique_spill_sites.xlsx")
)


# Functions
############################################################

#' Load individual sewage spill data
#' @return A data frame containing individual sewage spill records
load_data <- function() {
  file_path <- fs::path(CONFIG$spill_data_path)
  logger::log_info("Loading data: {file_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  
  tryCatch(
    {
      df <- rio::import(file_path, trust = TRUE)
      logger::log_info("Loaded data")
      return(df)
    },
    error = function(e) {
      error_msg <- glue::glue("Failed to load data: {e$message}")
      logger::log_error(error_msg)
      stop(error_msg)
    }
  )
}


#' Standardize grid references (NGR)
#' @param x Character vector of raw NGR strings
#' @return Cleaned NGR strings or NA
clean_ngr <- function(x) {
  x <- dplyr::case_when(
    toupper(x) %in% c("UNABLE TO MATCH TO CONSENTS DATABASE",
                      "NOT IN CONSENTS DATABASE") ~ NA_character_,
    TRUE ~ x
  )
  
  # Extract content after 'and', drop trailing notes
  x <- ifelse(stringr::str_detect(x, "(?i)and"),
              stringr::str_extract(x, "(?i)(?<=and).*"), x)
  x <- stringr::str_replace(x, "(?i)Not.*", "")
  
  # Keep only leading alphanumeric block
  x <- stringr::str_extract(x, "^[^,&/\\(]+")
  x <- stringr::str_remove_all(x, "\\s+")
  
  trimws(x)
}


#' Return the first non-missing value from a vector
#' @param x Vector
#' @return First non-NA value or typed NA
first_non_na <- function(x) {
  x <- x[!is.na(x)]
  x[1]
}


#' Parse EDM commission date text to Date object
#' @param text Character string describing commission date
#' @return Date object or NA_Date_
parse_commission_date <- function(text) {
  if (is.na(text) || text == "") {
    return(as.Date(NA))
  }

  text <- trimws(text)

  # Month-Year patterns: "Mar 2021", "June 2021", "December 2023"
  month_year_pattern <- "(?i)\\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\\s+(\\d{4})\\b"
  month_year_match <- stringr::str_match(text, month_year_pattern)

  if (!is.na(month_year_match[1, 1])) {
    month_str <- month_year_match[1, 2]
    year_str <- month_year_match[1, 3]
    date_str <- paste0("01 ", month_str, " ", year_str)
    parsed <- as.Date(date_str, format = "%d %b %Y")
    if (is.na(parsed)) {
      parsed <- as.Date(date_str, format = "%d %B %Y")
    }
    if (!is.na(parsed)) return(parsed)
  }

  # "to be installed by [Month] [Year]" pattern
  future_pattern <- "(?i)(?:to be installed|installed)\\s+(?:by\\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\\s+(\\d{4})"
  future_match <- stringr::str_match(text, future_pattern)

  if (!is.na(future_match[1, 1])) {
    month_str <- future_match[1, 2]
    year_str <- future_match[1, 3]
    date_str <- paste0("01 ", month_str, " ", year_str)
    parsed <- as.Date(date_str, format = "%d %b %Y")
    if (is.na(parsed)) {
      parsed <- as.Date(date_str, format = "%d %B %Y")
    }
    if (!is.na(parsed)) return(parsed)
  }

  # Pre-2016 pattern: use 2016-01-01 as conservative sentinel
  if (stringr::str_detect(text, "(?i)pre[- ]?2016")) {
    return(as.Date("2016-01-01"))
  }

  # "Commissioned in YYYY" pattern (with or without additional context)
  commissioned_pattern <- "(?i)commissioned\\s+(?:in\\s+)?(\\d{4})"
  commissioned_match <- stringr::str_match(text, commissioned_pattern)

  if (!is.na(commissioned_match[1, 1])) {
    year_str <- commissioned_match[1, 2]
    return(as.Date(paste0(year_str, "-01-01")))
  }

  # Standalone year pattern: just a 4-digit year
  year_only_pattern <- "^(\\d{4})$"
  year_only_match <- stringr::str_match(text, year_only_pattern)

  if (!is.na(year_only_match[1, 1])) {
    return(as.Date(paste0(year_only_match[1, 2], "-01-01")))
  }

  # Fallback: extract any 4-digit year
  any_year <- stringr::str_extract(text, "\\d{4}")
  if (!is.na(any_year)) {
    return(as.Date(paste0(any_year, "-01-01")))
  }

  as.Date(NA)
}


#' Get precision score for EDM commission date text
#' @param text Character string describing commission date
#' @return Integer precision score: 3=month-level, 2=year-level, 1=vague, 0=NA/unknown
get_commission_date_precision <- function(text) {
  if (is.na(text) || text == "") {
    return(0L)
  }

  text <- trimws(text)

  # Month-Year patterns get highest precision (3)
  month_year_pattern <- "(?i)\\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\\s+(\\d{4})\\b"
  if (stringr::str_detect(text, month_year_pattern)) {
    return(3L)
  }

  # Year-level patterns (2): "Commissioned in YYYY" or standalone year
  commissioned_pattern <- "(?i)commissioned\\s+(?:in\\s+)?\\d{4}"
  year_only_pattern <- "^\\d{4}$"
  if (stringr::str_detect(text, commissioned_pattern) ||
      stringr::str_detect(text, year_only_pattern)) {
    return(2L)
  }

  # Vague patterns (1): pre-2016
  if (stringr::str_detect(text, "(?i)pre[- ]?2016")) {
    return(1L)
  }

  # Any other year mention gets year-level
  if (stringr::str_detect(text, "\\d{4}")) {
    return(2L)
  }

  0L
}


#' Resolve conflicting EDM commission date values across years
#' @param texts Character vector of commission date text values
#' @param years Integer vector of reporting years
#' @return Resolved Date directly
resolve_commission_date <- function(texts, years) {
  # Handle empty or all-NA input
  keep <- !is.na(texts) & texts != ""
  if (sum(keep) == 0) {
    return(as.Date(NA))
  }

  texts <- texts[keep]
  years <- years[keep]

  # Parse dates and compute precision for each value
  parsed_dates <- vapply(texts, parse_commission_date, FUN.VALUE = as.Date(NA))
  parsed_dates <- as.Date(parsed_dates, origin = "1970-01-01")
  precisions <- vapply(texts, get_commission_date_precision, FUN.VALUE = 0L)

  # Identify future installation dates (text contains "to be installed")
  is_future <- stringr::str_detect(texts, "(?i)to be installed")

  # Create a data frame for resolution
  candidates <- data.frame(
    text = texts,
    year = years,
    parsed_date = parsed_dates,
    precision = precisions,
    is_future = is_future,
    stringsAsFactors = FALSE
  )

  # Resolution rules:
  # 0. Drop future installation-only values (not operational yet)
  if (all(candidates$is_future)) {
    return(as.Date(NA))
  }
  # 1. Prefer non-future dates when later data exists
  #    If we have a future date from year Y and actual date from year Y+1, use actual
  non_future <- candidates[!candidates$is_future, ]
  if (nrow(non_future) > 0 && any(candidates$is_future)) {
    max_future_year <- max(candidates$year[candidates$is_future])
    if (any(non_future$year > max_future_year)) {
      candidates <- non_future
    }
  }

  # 2. Higher precision wins
  max_precision <- max(candidates$precision, na.rm = TRUE)
  candidates <- candidates[candidates$precision == max_precision, ]

  # 3. Among same precision, most recent reporting year wins
  if (nrow(candidates) > 1) {
    max_year <- max(candidates$year, na.rm = TRUE)
    candidates <- candidates[candidates$year == max_year, ]
  }

  # Return the first remaining candidate's date
  as.Date(candidates$parsed_date[1], origin = "1970-01-01")
}


#' Derive unique spill sites with OSGB coordinates
#' @param data Raw spill dataframe
#' @return Tidy table: one row per site, availability per year, BNG coords
clean_location <- function(data) {
  data %>%
    # clean ngr
    mutate(ngr = clean_ngr(ngr)) %>% 
    # deduplicate on cleaned ref
    distinct(water_company, year, site_id, ngr) %>%
    # pivot availability by year
    mutate(available = TRUE) %>%
    pivot_wider(
      id_cols     = c(water_company, site_id, ngr),
      names_from  = year,
      values_from = available,
      names_prefix= "available_year_",
      values_fill = FALSE
    ) %>%
    # parse British National Grid coords
    mutate(
      coords = map(ngr, ~ tryCatch(
        rnrfa::osg_parse(.x, coord_system = "BNG"),
        error = function(e) list(easting=NA_real_, northing=NA_real_)
      ))
    ) %>%
    mutate(
      easting  = map_dbl(coords, "easting"),
      northing = map_dbl(coords, "northing")
    ) %>%
    select(-coords)
}

#' Summarise site metadata for EDM commissioning and operation status
#' @param data Raw spill dataframe
#' @return One row per site_id with edm_commission_date and EDM operational metadata
summarise_site_metadata <- function(data) {
  site_year_meta <- data %>%
    select(
      site_id, year,
      edm_commission_date, edm_operation_percent, edm_operation_reason
    ) %>%
    distinct()

  conflicts <- site_year_meta %>%
    group_by(site_id, year) %>%
    summarise(
      n_commission = n_distinct(edm_commission_date, na.rm = TRUE),
      n_operation  = n_distinct(edm_operation_percent, na.rm = TRUE),
      n_reason     = n_distinct(edm_operation_reason, na.rm = TRUE),
      .groups = "drop"
    )

  if (any(conflicts$n_operation > 1)) {
    logger::log_warn(
      "Multiple edm_operation_percent values for {sum(conflicts$n_operation > 1)} site-years"
    )
  }

  if (any(conflicts$n_commission > 1)) {
    logger::log_warn(
      "Multiple edm_commission_date values for {sum(conflicts$n_commission > 1)} site-years"
    )
  }

  if (any(conflicts$n_reason > 1)) {
    logger::log_warn(
      "Multiple edm_operation_reason values for {sum(conflicts$n_reason > 1)} site-years"
    )
  }

  site_year_meta <- site_year_meta %>%
    group_by(site_id, year) %>%
    summarise(
      edm_commission_date    = first_non_na(edm_commission_date),
      edm_operation_percent  = first_non_na(edm_operation_percent),
      edm_operation_reason   = first_non_na(edm_operation_reason),
      .groups = "drop"
    )

  commission_summary <- site_year_meta %>%
    group_by(site_id) %>%
    summarise(
      edm_commission_date = resolve_commission_date(edm_commission_date, year),
      .groups = "drop"
    ) %>%
    mutate(edm_commission_date = as.Date(edm_commission_date, origin = "1970-01-01"))

  operation_wide <- site_year_meta %>%
    select(site_id, year, edm_operation_percent) %>%
    pivot_wider(
      names_from = year,
      values_from = edm_operation_percent,
      names_prefix = "edm_operation_percent_"
    )

  operation_reason_wide <- site_year_meta %>%
    filter(year %in% c(2021, 2022, 2023)) %>%
    select(site_id, year, edm_operation_reason) %>%
    pivot_wider(
      names_from = year,
      values_from = edm_operation_reason,
      names_prefix = "edm_operation_reason_"
    )

  commission_summary %>%
    left_join(operation_wide, by = "site_id") %>%
    left_join(operation_reason_wide, by = "site_id") %>%
    {
      percent_cols <- grep("^edm_operation_percent_", names(.), value = TRUE)
      reason_cols <- grep("^edm_operation_reason_", names(.), value = TRUE)
      if (length(percent_cols) > 0 && length(reason_cols) > 0) {
        relocate(., all_of(reason_cols), .after = all_of(percent_cols))
      } else {
        .
      }
    }
}


#' Export unique spills data to parquet and Excel
#' @param unique_spills_df The unique spills dataframe
#' @param excel_output Path to Excel output file
#' @param parquet_output Path to parquet output file
#' @return NULL
export_data <- function(
    unique_spills_df,
    excel_output = CONFIG$unique_spills_xlsx,
    parquet_output = CONFIG$unique_spills_parquet) {
  tryCatch(
    {
      
      # Export to parquet files
      arrow::write_parquet(unique_spills_df, parquet_output)
      logger::log_info("Exported to parquet: {parquet_output}")
      
      # Export to Excel 
      rio::export(unique_spills_df, excel_output)
      logger::log_info("Exported data to Excel file: {excel_output}")
    },
    error = function(e) {
      logger::log_error("Data export failed: {e$message}")
      stop(glue::glue("Failed to export data: {e$message}"))
    }
  )
}

# Main execution
############################################################

main <- function() {
  tryCatch(
    {
      # Setup
      initialise_environment()
      setup_logging()
      
      # Load and process data
      logger::log_info("Loading processed merged data")
      spill_data <- load_data()
      
      logger::log_info("Creating unique spill sites")
      site_metadata <- summarise_site_metadata(spill_data)
      unique_sites <- spill_data %>%
        clean_location() %>%
        left_join(site_metadata, by = "site_id") %>%
        relocate(site_id, everything())
      
      # Export results
      logger::log_info("Exporting unique spill sites")
      export_data(unique_spills_df = unique_sites)
      logger::log_info("Processing completed successfully")
    },
    error = function(e) {
      logger::log_error("Fatal error: {e$message}")
      stop(e)
    }
  )
}

# Execute main function if script is run directly
if (sys.nframe() == 0) {
  main()
}