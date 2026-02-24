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
  annual_data_path = here::here(
    "data", "processed", "annual_return_edm.parquet"),
  lookup_data_path = here::here(
    "data", "processed", "annual_return_lookup.parquet"),
  availability_years = 2021:2023,
  metadata_years = 2021:2023,
  unique_spills_parquet = here::here(
    "data", "processed", "unique_spill_sites.parquet"),
  unique_spills_xlsx = here::here(
    "data", "processed", "unique_spill_sites.xlsx")
)


# Functions
############################################################

#' Load data from disk
#' @param file_path Input data path
#' @param label Dataset label for logging
#' @return A data frame containing input records
load_data <- function(file_path, label = "dataset") {
  file_path <- fs::path(file_path)
  logger::log_info("Loading {label}: {file_path}")
  
  if (!file.exists(file_path)) {
    stop(glue::glue("File not found: {file_path}"))
  }
  
  tryCatch(
    {
      df <- rio::import(file_path, trust = TRUE)
      logger::log_info("Loaded {label} ({nrow(df)} rows)")
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
  x <- as.character(x)
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

#' Convert blank/whitespace strings to NA_character_
#' @param x Character-like vector
#' @return Character vector with blank entries set to NA
normalise_missing_character <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x
}


#' Return the first non-missing value from a vector
#' @param x Vector
#' @return First non-NA value or typed NA
first_non_na <- function(x) {
  idx <- which(!is.na(x))
  if (length(idx) == 0) {
    return(x[NA_integer_][1])
  }
  x[idx[1]]
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


#' Parse British National Grid coordinates from NGR strings
#' @param ngr Character vector of NGR values
#' @return Tibble with easting and northing columns
parse_bng_coordinates <- function(ngr) {
  coords <- purrr::map(
    ngr,
    ~ tryCatch(
      suppressWarnings(rnrfa::osg_parse(.x, coord_system = "BNG")),
      error = function(e) list(easting = NA_real_, northing = NA_real_)
    )
  )

  tibble(
    easting = purrr::map_dbl(coords, "easting"),
    northing = purrr::map_dbl(coords, "northing")
  )
}

#' Build matched-event site availability while preserving current semantics
#' @param data Matched event-annual dataframe
#' @param availability_years Integer vector of availability years
#' @return One row per matched site_id with availability flags and fallback fields
build_matched_site_data <- function(data, availability_years = 2021:2023) {
  matched_data <- data %>%
    filter(!is.na(site_id), year %in% availability_years) %>%
    mutate(
      water_company = normalise_missing_character(water_company),
      ngr = clean_ngr(ngr)
    )

  availability <- matched_data %>%
    distinct(site_id, year) %>%
    mutate(available = TRUE) %>%
    pivot_wider(
      id_cols = site_id,
      names_from = year,
      values_from = available,
      names_prefix = "available_year_",
      values_fill = FALSE
    )

  availability_cols <- paste0("available_year_", availability_years)
  for (col_name in availability_cols) {
    if (!col_name %in% names(availability)) {
      availability[[col_name]] <- FALSE
    }
  }

  availability <- availability %>%
    select(site_id, all_of(availability_cols))

  matched_site_fields <- matched_data %>%
    arrange(site_id, desc(year)) %>%
    group_by(site_id) %>%
    summarise(
      water_company_matched = first_non_na(water_company),
      ngr_matched = first_non_na(ngr),
      .groups = "drop"
    )

  matched_coords <- parse_bng_coordinates(matched_site_fields$ngr_matched)
  matched_site_fields <- bind_cols(
    matched_site_fields,
    matched_coords %>%
      rename(
        easting_matched = easting,
        northing_matched = northing
      )
  )

  availability %>%
    full_join(matched_site_fields, by = "site_id")
}

#' Map annual-return rows to canonical site IDs from lookup
#' @param annual_data Annual return EDM dataframe
#' @param lookup_data Annual return lookup dataframe
#' @param metadata_years Integer vector of metadata years
#' @return Annual-return rows with canonical site_id
map_annual_to_canonical_sites <- function(
    annual_data,
    lookup_data,
    metadata_years = 2021:2023
) {
  id_cols <- paste0("site_id_", metadata_years)

  lookup_long <- lookup_data %>%
    select(site_id, any_of(id_cols)) %>%
    pivot_longer(
      cols = any_of(id_cols),
      names_to = "id_year_col",
      values_to = "year_site_id"
    ) %>%
    mutate(year = as.integer(readr::parse_number(id_year_col))) %>%
    filter(year %in% metadata_years, !is.na(year_site_id))

  lookup_duplicates <- lookup_long %>%
    count(year, year_site_id, name = "n") %>%
    filter(n > 1)

  if (nrow(lookup_duplicates) > 0) {
    logger::log_warn(
      "Lookup has duplicated year-site IDs for {nrow(lookup_duplicates)} entries; using first match"
    )
  }

  lookup_long <- lookup_long %>%
    group_by(year, year_site_id) %>%
    summarise(site_id = first_non_na(site_id), .groups = "drop")

  annual_long <- annual_data %>%
    filter(year %in% metadata_years) %>%
    select(
      year,
      water_company,
      outlet_discharge_ngr,
      edm_commission_date,
      edm_operation_percent,
      edm_operation_reason,
      any_of(id_cols)
    ) %>%
    pivot_longer(
      cols = any_of(id_cols),
      names_to = "id_year_col",
      values_to = "year_site_id"
    ) %>%
    mutate(id_year = as.integer(readr::parse_number(id_year_col))) %>%
    filter(year == id_year, !is.na(year_site_id)) %>%
    select(-id_year_col, -id_year)

  annual_long %>%
    left_join(lookup_long, by = c("year", "year_site_id")) %>%
    mutate(site_id = coalesce(as.integer(site_id), as.integer(year_site_id))) %>%
    select(
      site_id,
      year,
      water_company,
      outlet_discharge_ngr,
      edm_commission_date,
      edm_operation_percent,
      edm_operation_reason
    )
}

#' Summarise site metadata for EDM commissioning and operation status
#' @param data Annual-return site-year dataframe with canonical site_id
#' @param metadata_years Integer vector of metadata years
#' @return One row per site_id with metadata columns used in unique_spill_sites
summarise_site_metadata <- function(data, metadata_years = 2021:2023) {
  site_year_meta <- data %>%
    mutate(
      water_company = normalise_missing_character(water_company),
      ngr = clean_ngr(outlet_discharge_ngr),
      edm_commission_date = normalise_missing_character(edm_commission_date),
      edm_operation_reason = normalise_missing_character(edm_operation_reason)
    ) %>%
    select(
      site_id,
      year,
      water_company,
      ngr,
      edm_commission_date,
      edm_operation_percent,
      edm_operation_reason
    ) %>%
    distinct()

  conflicts <- site_year_meta %>%
    group_by(site_id, year) %>%
    summarise(
      n_water_company = n_distinct(water_company, na.rm = TRUE),
      n_ngr = n_distinct(ngr, na.rm = TRUE),
      n_commission = n_distinct(edm_commission_date, na.rm = TRUE),
      n_operation = n_distinct(edm_operation_percent, na.rm = TRUE),
      n_reason = n_distinct(edm_operation_reason, na.rm = TRUE),
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

  if (any(conflicts$n_water_company > 1)) {
    logger::log_warn(
      "Multiple water_company values for {sum(conflicts$n_water_company > 1)} site-years"
    )
  }

  if (any(conflicts$n_ngr > 1)) {
    logger::log_warn(
      "Multiple NGR values for {sum(conflicts$n_ngr > 1)} site-years"
    )
  }

  site_year_meta <- site_year_meta %>%
    group_by(site_id, year) %>%
    summarise(
      water_company = first_non_na(water_company),
      ngr = first_non_na(ngr),
      edm_commission_date = first_non_na(edm_commission_date),
      edm_operation_percent = first_non_na(edm_operation_percent),
      edm_operation_reason = first_non_na(edm_operation_reason),
      .groups = "drop"
    )

  site_fields <- site_year_meta %>%
    filter(year %in% metadata_years) %>%
    arrange(site_id, desc(year)) %>%
    group_by(site_id) %>%
    summarise(
      water_company = first_non_na(water_company),
      ngr = first_non_na(ngr),
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
    filter(year %in% metadata_years) %>%
    select(site_id, year, edm_operation_percent) %>%
    tidyr::complete(site_id, year = metadata_years) %>%
    pivot_wider(
      names_from = year,
      values_from = edm_operation_percent,
      names_prefix = "edm_operation_percent_"
    )

  operation_reason_wide <- site_year_meta %>%
    filter(year %in% metadata_years) %>%
    select(site_id, year, edm_operation_reason) %>%
    tidyr::complete(site_id, year = metadata_years) %>%
    pivot_wider(
      names_from = year,
      values_from = edm_operation_reason,
      names_prefix = "edm_operation_reason_"
    )

  site_fields %>%
    left_join(commission_summary, by = "site_id") %>%
    left_join(operation_wide, by = "site_id") %>%
    left_join(operation_reason_wide, by = "site_id")
}

#' Apply "No longer operational" carryforward to availability flags
#' @param unique_sites Candidate unique spill sites table
#' @param matched_data Matched event-annual dataframe
#' @param availability_years Integer vector of availability years
#' @param metadata_years Integer vector of metadata years
#' @return unique_sites with updated availability flags and nlo_carryforward_year
apply_nlo_carryforward <- function(
    unique_sites,
    matched_data,
    availability_years = 2021:2023,
    metadata_years = 2021:2023
) {
  availability_cols <- paste0("available_year_", availability_years)
  reason_cols <- paste0("edm_operation_reason_", metadata_years)
  max_availability_year <- max(availability_years)

  reasons_long <- unique_sites %>%
    select(site_id, any_of(reason_cols)) %>%
    pivot_longer(
      cols = any_of(reason_cols),
      names_to = "reason_col",
      values_to = "reason_text"
    ) %>%
    mutate(
      year = as.integer(readr::parse_number(reason_col)),
      reason_text = normalise_missing_character(reason_text),
      is_nlo = !is.na(reason_text) &
        stringr::str_detect(stringr::str_to_lower(reason_text), "no longer operational")
    ) %>%
    filter(is_nlo) %>%
    group_by(site_id) %>%
    summarise(first_nlo_year = min(year, na.rm = TRUE), .groups = "drop")

  positive_years <- tibble(site_id = integer(), first_post_nlo_positive_year = integer())
  has_event_metrics <- all(c("spill_count_ea", "spill_hrs_ea") %in% names(matched_data))

  if (!has_event_metrics) {
    logger::log_warn(
      "Matched data does not include spill_count_ea/spill_hrs_ea; applying uncapped NLO carryforward"
    )
  } else {
    positive_site_year <- matched_data %>%
      filter(!is.na(site_id), year %in% availability_years) %>%
      mutate(
        has_positive_event = (
          (!is.na(spill_count_ea) & spill_count_ea > 0) |
            (!is.na(spill_hrs_ea) & spill_hrs_ea > 0)
        )
      ) %>%
      group_by(site_id, year) %>%
      summarise(has_positive_event = any(has_positive_event), .groups = "drop") %>%
      filter(has_positive_event)

    positive_years <- reasons_long %>%
      select(site_id, first_nlo_year) %>%
      inner_join(positive_site_year, by = "site_id") %>%
      filter(year > first_nlo_year) %>%
      group_by(site_id) %>%
      summarise(first_post_nlo_positive_year = min(year, na.rm = TRUE), .groups = "drop")
  }

  nlo_windows <- reasons_long %>%
    left_join(positive_years, by = "site_id") %>%
    mutate(
      carryforward_end_year = dplyr::coalesce(
        first_post_nlo_positive_year - 1L,
        max_availability_year
      )
    ) %>%
    select(site_id, first_nlo_year, first_post_nlo_positive_year, carryforward_end_year)

  availability_long <- unique_sites %>%
    select(site_id, any_of(availability_cols)) %>%
    pivot_longer(
      cols = any_of(availability_cols),
      names_to = "availability_col",
      values_to = "available"
    ) %>%
    mutate(
      year = as.integer(readr::parse_number(availability_col)),
      available = replace_na(as.logical(available), FALSE)
    ) %>%
    left_join(nlo_windows, by = "site_id") %>%
    mutate(
      carryforward_applies = !is.na(first_nlo_year) &
        year >= first_nlo_year &
        year <= carryforward_end_year,
      carryforward_added = carryforward_applies & !available,
      available = available | carryforward_applies
    )

  carryforward_year <- availability_long %>%
    filter(carryforward_added) %>%
    group_by(site_id) %>%
    summarise(nlo_carryforward_year = min(year, na.rm = TRUE), .groups = "drop")

  availability_wide <- availability_long %>%
    select(site_id, availability_col, available) %>%
    pivot_wider(
      names_from = availability_col,
      values_from = available
    )

  sites_with_nlo <- nrow(reasons_long)
  sites_with_post_nlo_positive <- nrow(positive_years)
  sites_carryforwarded <- nrow(carryforward_year)
  logger::log_info("Sites with NLO reason: {sites_with_nlo}")
  logger::log_info("Sites with post-NLO positive evidence: {sites_with_post_nlo_positive}")
  logger::log_info("Sites with availability carryforward applied: {sites_carryforwarded}")

  unique_sites %>%
    select(-any_of(availability_cols), -any_of("nlo_carryforward_year")) %>%
    left_join(availability_wide, by = "site_id") %>%
    left_join(carryforward_year, by = "site_id")
}

#' Assemble final unique_spill_sites output
#' @param site_universe Distinct canonical site IDs
#' @param annual_metadata Annual-return metadata summarised at site level
#' @param matched_sites Matched-event availability and fallback site fields
#' @param matched_data Matched event-annual dataframe
#' @param availability_years Integer vector of availability years
#' @param metadata_years Integer vector of metadata years
#' @return Final unique site table with stable schema
assemble_unique_sites <- function(
    site_universe,
    annual_metadata,
    matched_sites,
    matched_data,
    availability_years = 2021:2023,
    metadata_years = 2021:2023
) {
  availability_cols <- paste0("available_year_", availability_years)
  percent_cols <- paste0("edm_operation_percent_", metadata_years)
  reason_cols <- paste0("edm_operation_reason_", metadata_years)

  unique_sites <- site_universe %>%
    left_join(annual_metadata, by = "site_id") %>%
    left_join(matched_sites, by = "site_id") %>%
    mutate(
      water_company = coalesce(water_company, water_company_matched),
      ngr = coalesce(ngr, ngr_matched),
      ngr = clean_ngr(ngr)
    )

  parsed_coords <- parse_bng_coordinates(unique_sites$ngr)
  unique_sites <- bind_cols(unique_sites, parsed_coords) %>%
    mutate(
      easting = coalesce(easting, easting_matched),
      northing = coalesce(northing, northing_matched)
    ) %>%
    select(-water_company_matched, -ngr_matched, -easting_matched, -northing_matched)

  unique_sites <- apply_nlo_carryforward(
    unique_sites = unique_sites,
    matched_data = matched_data,
    availability_years = availability_years,
    metadata_years = metadata_years
  )

  if (!"edm_commission_date" %in% names(unique_sites)) {
    unique_sites$edm_commission_date <- as.Date(NA)
  }

  for (col_name in availability_cols) {
    if (!col_name %in% names(unique_sites)) {
      unique_sites[[col_name]] <- FALSE
    }
  }

  for (col_name in percent_cols) {
    if (!col_name %in% names(unique_sites)) {
      unique_sites[[col_name]] <- NA_real_
    }
  }

  for (col_name in reason_cols) {
    if (!col_name %in% names(unique_sites)) {
      unique_sites[[col_name]] <- NA_character_
    }
  }

  if (!"nlo_carryforward_year" %in% names(unique_sites)) {
    unique_sites$nlo_carryforward_year <- NA_integer_
  }

  if (!"easting" %in% names(unique_sites)) {
    unique_sites$easting <- NA_real_
  }
  if (!"northing" %in% names(unique_sites)) {
    unique_sites$northing <- NA_real_
  }

  unique_sites %>%
    mutate(
      site_id = as.integer(site_id),
      water_company = normalise_missing_character(water_company),
      nlo_carryforward_year = as.integer(nlo_carryforward_year),
      across(all_of(availability_cols), ~ replace_na(as.logical(.x), FALSE))
    ) %>%
    select(
      site_id,
      water_company,
      ngr,
      all_of(availability_cols),
      nlo_carryforward_year,
      easting,
      northing,
      edm_commission_date,
      all_of(percent_cols),
      all_of(reason_cols)
    ) %>%
    arrange(site_id)
}

#' Log diagnostics for output coverage and completeness
#' @param unique_sites Final unique sites dataframe
#' @param availability_years Integer vector of availability years
#' @param metadata_years Integer vector of metadata years
#' @return NULL
log_output_diagnostics <- function(
    unique_sites,
    availability_years = 2021:2023,
    metadata_years = 2021:2023
) {
  availability_cols <- paste0("available_year_", availability_years)
  reason_cols <- paste0("edm_operation_reason_", metadata_years)

  availability_matrix <- unique_sites %>%
    select(all_of(availability_cols)) %>%
    mutate(across(everything(), ~ replace_na(.x, FALSE))) %>%
    as.matrix()

  annual_only_sites <- sum(rowSums(availability_matrix) == 0)
  logger::log_info("Final unique sites: {nrow(unique_sites)}")
  logger::log_info("Sites without matched-event availability (annual-only): {annual_only_sites}")

  for (reason_col in reason_cols) {
    non_missing <- sum(!is.na(unique_sites[[reason_col]]))
    logger::log_info("{reason_col} non-missing values: {non_missing}")
  }

  if ("nlo_carryforward_year" %in% names(unique_sites)) {
    n_carryforward <- sum(!is.na(unique_sites$nlo_carryforward_year))
    logger::log_info("nlo_carryforward_year non-missing values: {n_carryforward}")
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
      
      # Load source data
      matched_data <- load_data(CONFIG$spill_data_path, "matched event-annual data")
      annual_data <- load_data(CONFIG$annual_data_path, "annual return EDM data")
      lookup_data <- load_data(CONFIG$lookup_data_path, "annual return lookup")

      # Build site universe from lookup
      site_universe <- lookup_data %>%
        select(site_id) %>%
        distinct() %>%
        filter(!is.na(site_id)) %>%
        mutate(site_id = as.integer(site_id))

      logger::log_info("Canonical site universe size: {nrow(site_universe)}")

      # Build matched-event availability and fallback fields
      logger::log_info("Building matched-event availability flags")
      matched_sites <- build_matched_site_data(
        matched_data,
        availability_years = CONFIG$availability_years
      )

      # Build metadata from annual returns mapped to canonical site IDs
      logger::log_info("Mapping annual-return rows to canonical site IDs")
      annual_mapped <- map_annual_to_canonical_sites(
        annual_data,
        lookup_data,
        metadata_years = CONFIG$metadata_years
      )

      logger::log_info("Summarising annual metadata at site level")
      annual_metadata <- summarise_site_metadata(
        annual_mapped,
        metadata_years = CONFIG$metadata_years
      )

      # Assemble final output table
      logger::log_info("Assembling unique spill sites")
      unique_sites <- assemble_unique_sites(
        site_universe = site_universe,
        annual_metadata = annual_metadata,
        matched_sites = matched_sites,
        matched_data = matched_data,
        availability_years = CONFIG$availability_years,
        metadata_years = CONFIG$metadata_years
      )

      log_output_diagnostics(
        unique_sites,
        availability_years = CONFIG$availability_years,
        metadata_years = CONFIG$metadata_years
      )
      
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
