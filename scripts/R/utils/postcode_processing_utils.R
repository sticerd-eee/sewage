############################################################
# Postcode Processing Utilities
# Project: Sewage
# Date: 02/09/2025
# Author: Jacopo Olivieri
############################################################

#' Shared functions for processing UK postcodes via local ONS lookups.

# Dependencies
############################################################

# Lightweight, defensive attachment of required packages for utilities only.
required_packages <- c(
  "tidyverse",   # tibble/dplyr utilities
  "logger",      # logging
  "glue",        # string interpolation
  "data.table"   # efficient rbindlist
)

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  if (!paste0("package:", pkg) %in% search()) library(pkg, character.only = TRUE)
}))

# Configuration
############################################################

LOCAL_POSTCODE_NUMERIC_VARS <- c(
  "quality",
  "easting",
  "northing",
  "longitude",
  "latitude"
)

LOCAL_POSTCODE_LABEL_VARS <- c(
  "country",
  "nhs_ha",
  "region",
  "lsoa",
  "msoa",
  "admin_county",
  "admin_ward"
)

LOCAL_POSTCODE_OUTPUT_VARS <- c(
  "quality",
  "easting",
  "northing",
  "longitude",
  "latitude",
  "country",
  "nhs_ha",
  "region",
  "lsoa",
  "msoa",
  "admin_county",
  "admin_ward"
)

LOCAL_POSTCODE_CODE_VARS <- c(
  "ctry25cd",
  "hlth19cd",
  "rgn25cd",
  "lsoa11cd",
  "msoa11cd",
  "cty25cd",
  "wd25cd"
)

LOCAL_POSTCODE_LABEL_SPECS <- list(
  country = list(
    source_col = "ctry25cd",
    file_name = "CTRY Country names and codes UK as at 05_25.csv",
    code_col = "CTRY25CD",
    name_col = "CTRY25NM"
  ),
  nhs_ha = list(
    source_col = "hlth19cd",
    file_name = "HLTH health authority names and codes UK as at 04_19 (OSHLTHAU).csv",
    code_col = "HLTHAUCD",
    name_col = "HLTHAUNM"
  ),
  region = list(
    source_col = "rgn25cd",
    file_name = "RGN Region names and codes EN as at 05_25.csv",
    code_col = "RGN25CD",
    name_col = "RGN25NM"
  ),
  lsoa = list(
    source_col = "lsoa11cd",
    file_name = "LSOA (2011) names and codes UK as at 12_12.csv",
    code_col = "LSOA11CD",
    name_col = "LSOA11NM"
  ),
  msoa = list(
    source_col = "msoa11cd",
    file_name = "MSOA (2011) names and codes UK as at 12_12.csv",
    code_col = "MSOA11CD",
    name_col = "MSOA11NM"
  ),
  admin_county = list(
    source_col = "cty25cd",
    file_name = "CTY County names and codes UK as at 05_25.csv",
    code_col = "CTY25CD",
    name_col = "CTY25NM"
  ),
  admin_ward = list(
    source_col = "wd25cd",
    file_name = "WD Ward names and codes UK as at 05_25.csv",
    code_col = "WD25CD",
    name_col = "WD25NM"
  )
)

# Functions
############################################################

#' Normalise UK postcode strings to a consistent join key
#' @param x Character vector of postcodes
#' @return Character vector with whitespace removed and uppercase applied
#' @export
normalise_postcode <- function(x) {
  x <- as.character(x)
  x <- stringr::str_trim(x)
  x <- stringr::str_replace_all(x, "\\s+", "")
  x <- stringr::str_to_upper(x)
  x[x %in% c("", "NA")] <- NA_character_
  x
}

#' Load the local ONS postcode lookup keyed by normalised postcode
#' @param lookup_path Path to the ONS postcode CSV
#' @return Tibble keyed by normalised postcode
#' @export
load_local_postcode_lookup <- function(lookup_path) {
  if (!file.exists(lookup_path)) {
    stop(glue::glue("Local postcode lookup not found: {lookup_path}"), call. = FALSE)
  }

  lookup_cols <- c(
    "pcds",
    "pcd8",
    "gridind",
    "east1m",
    "north1m",
    "lat",
    "long",
    "ctry25cd",
    "hlth19cd",
    "rgn25cd",
    "lsoa11cd",
    "msoa11cd",
    "cty25cd",
    "wd25cd"
  )

  logger::log_info("Loading local postcode lookup from {lookup_path}")

  lookup_data <- data.table::fread(
    lookup_path,
    select = lookup_cols,
    na.strings = c("", "NA"),
    showProgress = interactive()
  ) %>%
    tibble::as_tibble() %>%
    dplyr::transmute(
      postcode = normalise_postcode(dplyr::coalesce(.data[["pcds"]], .data[["pcd8"]])),
      quality = suppressWarnings(as.integer(.data[["gridind"]])),
      easting = suppressWarnings(as.integer(.data[["east1m"]])),
      northing = suppressWarnings(as.integer(.data[["north1m"]])),
      longitude = suppressWarnings(as.numeric(.data[["long"]])),
      latitude = suppressWarnings(as.numeric(.data[["lat"]])),
      ctry25cd = dplyr::na_if(.data[["ctry25cd"]], ""),
      hlth19cd = dplyr::na_if(.data[["hlth19cd"]], ""),
      rgn25cd = dplyr::na_if(.data[["rgn25cd"]], ""),
      lsoa11cd = dplyr::na_if(.data[["lsoa11cd"]], ""),
      msoa11cd = dplyr::na_if(.data[["msoa11cd"]], ""),
      cty25cd = dplyr::na_if(.data[["cty25cd"]], ""),
      wd25cd = dplyr::na_if(.data[["wd25cd"]], "")
    ) %>%
    dplyr::filter(
      !is.na(postcode),
      !is.na(easting),
      !is.na(northing)
    )

  duplicate_postcodes <- lookup_data %>%
    dplyr::count(postcode, name = "n") %>%
    dplyr::filter(n > 1)

  if (nrow(duplicate_postcodes) > 0) {
    sample_dupes <- duplicate_postcodes %>%
      dplyr::slice_head(n = 10) %>%
      dplyr::pull(postcode) %>%
      paste(collapse = ", ")

    stop(
      glue::glue(
        "Local postcode lookup contains duplicate normalised postcodes. ",
        "Sample duplicates: {sample_dupes}"
      ),
      call. = FALSE
    )
  }

  logger::log_info("Local postcode lookup loaded with {nrow(lookup_data)} unique postcode rows")

  lookup_data
}

#' Load one ONS code-to-name lookup table from the local postcode bundle
#' @param documents_dir Directory containing ONS code/name tables
#' @param file_name Lookup file name inside `documents_dir`
#' @param code_col Code column name in the lookup file
#' @param name_col Label column name in the lookup file
#' @param output_col Output label column name
#' @return Tibble with a `code` join key and a single label column
#' @export
load_ons_name_lookup <- function(documents_dir, file_name, code_col, name_col, output_col) {
  lookup_path <- file.path(documents_dir, file_name)

  if (!file.exists(lookup_path)) {
    stop(glue::glue("ONS lookup not found: {lookup_path}"), call. = FALSE)
  }

  logger::log_info("Loading ONS label lookup from {lookup_path}")

  lookup_data <- utils::read.csv(
    lookup_path,
    check.names = FALSE,
    fill = TRUE,
    na.strings = c("", "NA"),
    stringsAsFactors = FALSE
  )

  expected_cols <- c(code_col, name_col)
  missing_cols <- setdiff(expected_cols, names(lookup_data))

  if (length(missing_cols) > 0) {
    available_cols <- names(lookup_data)
    available_cols <- available_cols[available_cols != ""]

    stop(
      glue::glue(
        "ONS lookup {lookup_path} is missing expected columns: ",
        "{paste(missing_cols, collapse = ', ')}. ",
        "Available named columns: {paste(available_cols, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }

  lookup_data[expected_cols] %>%
    tibble::as_tibble() %>%
    dplyr::transmute(
      code = dplyr::na_if(as.character(.data[[code_col]]), ""),
      !!output_col := dplyr::na_if(as.character(.data[[name_col]]), "")
    ) %>%
    dplyr::filter(!is.na(code)) %>%
    dplyr::distinct(code, .keep_all = TRUE)
}

#' Build label-style geography fields from local ONS code lookups
#' @param ons_lookup Tibble returned by `load_local_postcode_lookup`
#' @param lookup_path Path to the ONS postcode CSV
#' @return Tibble keyed by normalised postcode with label-style geography columns
#' @export
build_local_postcode_label_lookup <- function(ons_lookup, lookup_path) {
  documents_dir <- file.path(dirname(lookup_path), "Documents")

  if (!dir.exists(documents_dir)) {
    stop(glue::glue("ONS lookup documents directory not found: {documents_dir}"), call. = FALSE)
  }

  postcode_labels <- ons_lookup %>%
    dplyr::select(postcode, dplyr::all_of(LOCAL_POSTCODE_CODE_VARS))

  for (output_col in names(LOCAL_POSTCODE_LABEL_SPECS)) {
    spec <- LOCAL_POSTCODE_LABEL_SPECS[[output_col]]

    name_lookup <- load_ons_name_lookup(
      documents_dir = documents_dir,
      file_name = spec$file_name,
      code_col = spec$code_col,
      name_col = spec$name_col,
      output_col = output_col
    )

    postcode_labels <- postcode_labels %>%
      dplyr::left_join(name_lookup, by = setNames("code", spec$source_col))
  }

  postcode_labels <- postcode_labels %>%
    dplyr::mutate(
      admin_county = dplyr::if_else(
        stringr::str_starts(admin_county, fixed("(pseudo)")),
        NA_character_,
        admin_county
      )
    )

  postcode_labels %>%
    dplyr::select(postcode, dplyr::all_of(LOCAL_POSTCODE_LABEL_VARS))
}

#' Build postcode enrichment for sales using the local ONS lookup
#'
#' @param df Sales data containing `postcode`
#' @param lookup_path Path to the ONS postcode CSV
#' @return List with `postcode_data` and `diagnostics`
#' @export
get_local_postcode_data_for_sales <- function(df, lookup_path) {
  required_cols <- c("postcode")
  missing_cols <- setdiff(required_cols, names(df))

  if (length(missing_cols) > 0) {
    stop(
      glue::glue(
        "Local postcode enrichment requires columns: {paste(required_cols, collapse = ', ')}. ",
        "Missing: {paste(missing_cols, collapse = ', ')}"
      ),
      call. = FALSE
    )
  }

  sales_data <- df %>%
    dplyr::transmute(postcode = normalise_postcode(postcode))

  unique_postcodes <- sales_data %>%
    dplyr::filter(!is.na(postcode)) %>%
    dplyr::distinct(postcode)

  ons_lookup <- load_local_postcode_lookup(lookup_path = lookup_path) %>%
    dplyr::semi_join(unique_postcodes, by = "postcode")

  label_lookup <- build_local_postcode_label_lookup(
    ons_lookup = ons_lookup,
    lookup_path = lookup_path
  )

  postcode_data <- ons_lookup %>%
    dplyr::left_join(label_lookup, by = "postcode") %>%
    dplyr::select(postcode, dplyr::all_of(LOCAL_POSTCODE_OUTPUT_VARS)) %>%
    dplyr::distinct(postcode, .keep_all = TRUE)

  postcode_misses <- unique_postcodes %>%
    dplyr::anti_join(postcode_data %>% dplyr::distinct(postcode), by = "postcode")

  missing_rows <- sales_data %>%
    dplyr::semi_join(postcode_misses, by = "postcode")

  label_missing_counts <- postcode_data %>%
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(LOCAL_POSTCODE_LABEL_VARS),
        ~ sum(is.na(.x)),
        .names = "{.col}"
      )
    ) %>%
    as.list()

  diagnostics <- list(
    total_rows = nrow(df),
    distinct_postcodes = nrow(unique_postcodes),
    ons_matched_postcodes = nrow(unique_postcodes) - nrow(postcode_misses),
    ons_missing_postcodes = nrow(postcode_misses),
    ons_missing_rows = nrow(missing_rows),
    ons_missing_samples = head(postcode_misses$postcode, 10),
    label_missing_counts = label_missing_counts
  )

  logger::log_info(
    "Local postcode coverage: {diagnostics$ons_matched_postcodes}/{diagnostics$distinct_postcodes} distinct postcodes found in ONS"
  )
  logger::log_info(
    "Local postcode misses: {diagnostics$ons_missing_postcodes} distinct postcodes covering {diagnostics$ons_missing_rows} rows"
  )

  if (length(diagnostics$ons_missing_samples) > 0) {
    logger::log_warn(
      "Sample ONS-missing postcodes: {paste(diagnostics$ons_missing_samples, collapse = ', ')}"
    )
  }

  logger::log_info(
    "Label-field NA counts among matched postcodes: {paste(names(diagnostics$label_missing_counts), unlist(diagnostics$label_missing_counts), sep = '=', collapse = ', ')}"
  )

  list(
    postcode_data = postcode_data,
    diagnostics = diagnostics
  )
}
