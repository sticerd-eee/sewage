# ==============================================================================
# Extensive-Margin News Analysis Utilities
# ==============================================================================
#
# Purpose: Provide shared helpers for configurable extensive-margin sewage-news
#          analyses built from nearest-overflow distance bands.
#
# Author: Jacopo Olivieri
# Date: 2026-04-07
#
# ==============================================================================

#' Validate and enrich the distance-band comparison configuration
#'
#' @param comparison Named list with `near_min`, `near_max`, `far_min`,
#'   `far_max`, and optional `comparison_id` and `comparison_label`.
#' @return Validated comparison list with added display labels.
validate_comparison_config <- function(comparison) {
  required_fields <- c("near_min", "near_max", "far_min", "far_max")
  missing_fields <- setdiff(required_fields, names(comparison))

  if (length(missing_fields) > 0L) {
    stop(
      "Comparison config is missing required fields: ",
      paste(missing_fields, collapse = ", "),
      call. = FALSE
    )
  }

  out <- comparison

  for (field in required_fields) {
    out[[field]] <- as.integer(out[[field]])
  }

  if (out$near_min < 0L) {
    stop("`near_min` must be non-negative.", call. = FALSE)
  }
  if (out$near_min > out$near_max) {
    stop("`near_min` must be less than or equal to `near_max`.", call. = FALSE)
  }
  if (out$far_min <= out$near_max) {
    stop("`far_min` must be strictly greater than `near_max`.", call. = FALSE)
  }
  if (out$far_min > out$far_max) {
    stop("`far_min` must be less than or equal to `far_max`.", call. = FALSE)
  }

  if (is.null(out$comparison_id) || !nzchar(out$comparison_id)) {
    out$comparison_id <- paste0(
      out$near_max,
      "_vs_",
      out$far_min,
      "_",
      out$far_max
    )
  }

  if (is.null(out$comparison_label) || !nzchar(out$comparison_label)) {
    out$comparison_label <- paste0(
      out$near_min,
      "-",
      out$near_max,
      "m vs ",
      out$far_min,
      "-",
      out$far_max,
      "m"
    )
  }

  out$near_band_label <- paste0(out$near_min, "-", out$near_max, "m")
  out$far_band_label <- paste0(out$far_min, "-", out$far_max, "m")

  out
}

#' Ensure the output directory for a file path exists
#'
#' @param output_path Character path to an output file.
#' @return The input path, invisibly.
ensure_output_dir <- function(output_path) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  invisible(output_path)
}

#' Load cumulative LexisNexis article counts
#'
#' @param path Character path to the monthly article parquet.
#' @param start_month_id Integer first month to retain.
#' @param end_month_id Integer last month to retain.
#' @return Tibble with monthly and cumulative article counts.
load_articles_data <- function(path, start_month_id = 1L, end_month_id = 36L) {
  arrow::read_parquet(path) |>
    dplyr::filter(
      .data$month_id >= start_month_id,
      .data$month_id <= end_month_id
    ) |>
    dplyr::arrange(.data$month_id) |>
    dplyr::mutate(
      cumulative_articles = cumsum(.data$article_count),
      log_cumulative_articles = log(.data$cumulative_articles)
    )
}

#' Load the Google Trends peak period
#'
#' @param path Character path to the Google Trends workbook.
#' @param sheet Sheet name to read.
#' @param base_year Integer baseline year for month and quarter IDs.
#' @return Named list with peak date, month ID, and quarter ID.
load_google_trends_peak <- function(
  path,
  sheet = "united_kingdom",
  base_year = 2021L
) {
  google_trends <- readxl::read_excel(path, sheet = sheet) |>
    dplyr::filter(
      .data$Year >= base_year,
      .data$Year <= base_year + 2L
    )

  peak_row <- google_trends |>
    dplyr::slice_max(
      `'Sewage Spill' Google Searches`,
      n = 1,
      with_ties = FALSE
    )

  peak_year <- peak_row$Year[[1]]
  peak_month <- as.integer(substr(peak_row$Date[[1]], 6, 7))

  list(
    peak_date = peak_row$Date[[1]],
    peak_month_id = (peak_year - base_year) * 12L + peak_month,
    peak_qtr_id = (peak_year - base_year) * 4L + ceiling(peak_month / 3)
  )
}

#' Load the nearest-overflow distance for each property
#'
#' @param path Character path to the lookup parquet.
#' @param id_col Character identifier column (`house_id` or `rental_id`).
#' @param max_distance Maximum distance to retain in metres.
#' @return Tibble with one row per property and its nearest distance.
load_nearest_distance_lookup <- function(path, id_col, max_distance) {
  arrow::open_dataset(path) |>
    dplyr::select(dplyr::all_of(c(id_col, "distance_m"))) |>
    dplyr::filter(
      !is.na(.data$distance_m),
      .data$distance_m <= max_distance
    ) |>
    dplyr::collect() |>
    dplyr::group_by(dplyr::across(dplyr::all_of(id_col))) |>
    dplyr::summarise(
      min_distance = min(.data$distance_m, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tibble::as_tibble()
}

#' Load house sales transactions for the news analyses
#'
#' @param path Character path to `house_price.parquet`.
#' @return Tibble with property-type fields coerced to factors.
load_sales_transactions <- function(path) {
  rio::import(path, trust = TRUE) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      property_type = forcats::as_factor(.data$property_type),
      old_new = forcats::as_factor(.data$old_new),
      duration = forcats::as_factor(.data$duration)
    )
}

#' Load rental transactions for the news analyses
#'
#' @param path Character path to `zoopla_rentals.parquet`.
#' @return Tibble with property-type fields coerced to factors.
load_rental_transactions <- function(path) {
  rio::import(path, trust = TRUE) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      property_type = forcats::as_factor(.data$property_type)
    )
}

#' Restrict properties to the configured near and far bands
#'
#' @param transactions Tibble of property transactions.
#' @param lookup Tibble returned by `load_nearest_distance_lookup()`.
#' @param id_col Character identifier column.
#' @param comparison Validated comparison config.
#' @return Tibble restricted to the configured extensive-margin comparison.
build_extensive_margin_sample <- function(transactions, lookup, id_col, comparison) {
  transactions |>
    dplyr::inner_join(lookup, by = id_col) |>
    dplyr::filter(
      (.data$min_distance >= comparison$near_min &
         .data$min_distance <= comparison$near_max) |
        (.data$min_distance > comparison$far_min &
           .data$min_distance <= comparison$far_max)
    ) |>
    dplyr::mutate(
      near_bin = as.integer(.data$min_distance <= comparison$near_max),
      comparison_id = comparison$comparison_id,
      comparison_label = comparison$comparison_label
    )
}

#' Coerce common sales fixed-effect and control variables to factors
#'
#' @param data Tibble ready for estimation.
#' @return Tibble with factor columns standardised and dropped to used levels.
standardise_sales_estimation_data <- function(data) {
  out <- data |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      property_type = forcats::fct_drop(forcats::as_factor(.data$property_type)),
      old_new = forcats::fct_drop(forcats::as_factor(.data$old_new)),
      duration = forcats::fct_drop(forcats::as_factor(.data$duration))
    )

  if ("msoa" %in% names(out)) {
    out <- out |>
      dplyr::mutate(
        msoa = forcats::fct_drop(forcats::as_factor(.data$msoa))
      )
  }

  out
}

#' Coerce common rental fixed-effect and control variables to factors
#'
#' @param data Tibble ready for estimation.
#' @return Tibble with factor columns standardised and dropped to used levels.
standardise_rental_estimation_data <- function(data) {
  out <- data |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      property_type = forcats::fct_drop(forcats::as_factor(.data$property_type))
    )

  if ("msoa" %in% names(out)) {
    out <- out |>
      dplyr::mutate(
        msoa = forcats::fct_drop(forcats::as_factor(.data$msoa))
      )
  }

  out
}

#' Print a compact near/far sample summary
#'
#' @param data Tibble used in estimation.
#' @param market_label Character label for stdout.
#' @param comparison Validated comparison config.
#' @return `NULL`, invisibly.
print_extensive_margin_summary <- function(data, market_label, comparison) {
  cat(sprintf(
    "  %s sample: %s observations\n",
    market_label,
    format(nrow(data), big.mark = ",")
  ))
  cat(sprintf(
    "    Near band (%s): %s\n",
    comparison$near_band_label,
    format(sum(data$near_bin == 1L, na.rm = TRUE), big.mark = ",")
  ))
  cat(sprintf(
    "    Far band (%s): %s\n",
    comparison$far_band_label,
    format(sum(data$near_bin == 0L, na.rm = TRUE), big.mark = ",")
  ))
  cat(sprintf(
    "    Nearest-distance range: %.1f to %.1f m\n",
    min(data$min_distance, na.rm = TRUE),
    max(data$min_distance, na.rm = TRUE)
  ))

  invisible(NULL)
}

#' Describe the comparison for table notes
#'
#' @param comparison Validated comparison config.
#' @return Character sentence fragment for table notes.
comparison_note_text <- function(comparison) {
  paste0(
    "The sample includes properties whose nearest mapped overflow lies either within ",
    comparison$near_band_label,
    " (near bin) or ",
    comparison$far_band_label,
    " (far bin). "
  )
}

#' Apply the repository's preferred tabularray tweaks to a modelsummary table
#'
#' @param table_latex Character scalar returned by `modelsummary()`.
#' @param label Table label without surrounding braces.
#' @param notes Preformatted tabularray note string.
#' @param width Optional width string, e.g. `0.9\\\\linewidth`.
#' @return Patched LaTeX table.
patch_modelsummary_latex <- function(table_latex, label, notes, width = NULL) {
  width_prefix <- if (is.null(width)) {
    ""
  } else {
    paste0("width=", width, ",\n")
  }

  table_latex <- sub(
    "\\\\begin\\{table\\}",
    "\\\\begin{table}[H]",
    table_latex
  )

  table_latex <- sub(
    "caption=\\{([^}]*)\\},",
    paste0("caption={\\1},\nlabel={", label, "},"),
    table_latex
  )

  table_latex <- sub(
    "(\\{\\s*%% tabularray inner open\\n)",
    paste0(
      "\\1",
      width_prefix,
      "colsep=3pt,\n",
      "cells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n"
    ),
    table_latex
  )

  table_latex <- sub(
    "note\\{\\}=\\{\\s*\\},",
    notes,
    table_latex
  )

  table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)
  table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)

  table_latex
}

#' Build a coefficient map for event-study `i()` terms
#'
#' @param models List of `fixest` model objects.
#' @param event_var Character event-time variable stem.
#' @return Named character vector for `coef_map`, or `NULL` if not found.
build_event_coef_map <- function(models, event_var = "event_qtr") {
  all_coefs <- unique(unlist(lapply(models, function(model) names(stats::coef(model)))))
  event_coefs <- all_coefs[grepl(event_var, all_coefs)]

  if (length(event_coefs) == 0L) {
    warning("No event-time coefficients found. Using default labels.")
    return(NULL)
  }

  event_times <- as.integer(stringr::str_extract(event_coefs, "-?\\d+"))
  valid_idx <- !is.na(event_times)

  event_coefs <- event_coefs[valid_idx]
  event_times <- event_times[valid_idx]

  ord <- order(event_times)
  event_coefs <- event_coefs[ord]
  event_times <- event_times[ord]

  stats::setNames(
    paste0("Event time ", event_times),
    event_coefs
  )
}
