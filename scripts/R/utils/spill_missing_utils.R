############################################################
# Missing Group-Year Utilities for Descriptive Outputs
# Project: Sewage
############################################################

#' Helpers for the missing-data rule in
#' `docs/reports/2026-10-07-001-spill-data-decisions-report.qmd`, applied to
#' `data/processed/agg_spill_stats/agg_spill_yr.parquet`.
#'
#' A group-year is missing when `annual_status` is `absent` or `reported_na`,
#' or when it is `reported_positive` with no matched events (`spill_count_yr`
#' is NA). Each overflow is averaged over its non-missing years only, and
#' MSOAs are classified as `no_overflow`, `no_data` (overflows but no
#' non-missing year), `zero` or `positive`.

MISSING_ANNUAL_STATUSES <- c("absent", "reported_na")
MSOA_SPILL_CLASS_LEVELS <- c("no_overflow", "no_data", "zero", "positive")

#' Flag missing group-years.
#'
#' @param annual_data Yearly aggregates with `site_id`, `year`,
#'   `annual_status` and `spill_count_yr`.
#' @return A tibble with `site_id`, `year` and `is_missing_group_year`.
flag_missing_group_years <- function(annual_data) {
  required <- c("site_id", "year", "annual_status", "spill_count_yr")
  missing_cols <- setdiff(required, names(annual_data))
  if (length(missing_cols) > 0) {
    stop(
      "flag_missing_group_years() needs columns: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  flags <- tibble::as_tibble(annual_data) |>
    dplyr::transmute(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year),
      is_missing_group_year =
        is.na(.data$annual_status) |
        .data$annual_status %in% MISSING_ANNUAL_STATUSES |
        (.data$annual_status == "reported_positive" &
          is.na(.data$spill_count_yr))
    )

  if (anyDuplicated(flags[, c("site_id", "year")])) {
    stop(
      "flag_missing_group_years() expects one row per site_id-year.",
      call. = FALSE
    )
  }

  flags
}

#' Attach the missing flag to another group-year table.
#'
#' @param data Group-year table with `site_id` and `year`.
#' @param missing_flags Output of `flag_missing_group_years()`.
#' @return `data` with `is_missing_group_year`; unflagged group-years are
#'   treated as missing.
attach_missing_group_year_flag <- function(data, missing_flags) {
  before <- nrow(data)
  joined <- data |>
    dplyr::mutate(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year)
    ) |>
    dplyr::select(-dplyr::any_of("is_missing_group_year")) |>
    dplyr::left_join(missing_flags, by = c("site_id", "year")) |>
    dplyr::mutate(
      is_missing_group_year =
        tidyr::replace_na(.data$is_missing_group_year, TRUE)
    )
  if (nrow(joined) != before) {
    stop(
      "attach_missing_group_year_flag() changed the row count.",
      call. = FALSE
    )
  }
  joined
}

#' Average annual values per overflow over its non-missing years.
#'
#' @param data Group-year table with `site_id`, `is_missing_group_year` and
#'   `value_cols`, already restricted to the target years.
#' @param value_cols Yearly value columns.
#' @return One row per `site_id` with `n_years_with_data` and, per column,
#'   `avg_annual_<col>` and `n_years_<col>` (years with a non-NA value).
summarise_overflow_annual_averages <- function(data, value_cols) {
  data |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(value_cols),
        ~ dplyr::if_else(.data$is_missing_group_year, NA_real_, as.numeric(.x))
      )
    ) |>
    dplyr::group_by(.data$site_id) |>
    dplyr::summarise(
      n_years_with_data = sum(!.data$is_missing_group_year),
      dplyr::across(
        dplyr::all_of(value_cols),
        ~ if (any(!is.na(.x))) mean(.x, na.rm = TRUE) else NA_real_,
        .names = "avg_annual_{.col}"
      ),
      dplyr::across(
        dplyr::all_of(value_cols),
        ~ sum(!is.na(.x)),
        .names = "n_years_{.col}"
      ),
      .groups = "drop"
    )
}

#' Classify MSOAs from per-overflow counts.
#'
#' @param site_count Number of mapped overflows in the MSOA.
#' @param sites_with_data Number of those with at least one non-missing year.
#' @param summed_average Sum of per-overflow averages.
#' @return A factor with levels `MSOA_SPILL_CLASS_LEVELS`.
classify_msoa_spill_status <- function(site_count, sites_with_data,
                                       summed_average) {
  site_count <- tidyr::replace_na(site_count, 0L)
  sites_with_data <- tidyr::replace_na(sites_with_data, 0L)
  summed_average <- tidyr::replace_na(summed_average, 0)

  factor(
    dplyr::case_when(
      site_count == 0 ~ "no_overflow",
      sites_with_data == 0 ~ "no_data",
      summed_average > 0 ~ "positive",
      TRUE ~ "zero"
    ),
    levels = MSOA_SPILL_CLASS_LEVELS
  )
}

#' Sum per-overflow averages to MSOA level and classify each MSOA.
#'
#' @param site_averages Output of `summarise_overflow_annual_averages()` with
#'   `msoa_code` (and optionally `msoa_name`).
#' @param value_col Yearly value column whose average is summed.
#' @return One row per MSOA with overflows: `site_count`, `sites_with_data`,
#'   `summed_average`, `spill_class`. Reclassify after filling in MSOAs with
#'   no overflow.
aggregate_overflow_averages_to_msoa <- function(site_averages, value_col) {
  group_cols <- intersect(c("msoa_code", "msoa_name"), names(site_averages))
  avg_col <- paste0("avg_annual_", value_col)
  n_col <- paste0("n_years_", value_col)

  site_averages |>
    dplyr::filter(!is.na(.data$msoa_code)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(
      site_count = dplyr::n(),
      sites_with_data = sum(.data[[n_col]] > 0),
      summed_average = sum(.data[[avg_col]], na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      spill_class = classify_msoa_spill_status(
        .data$site_count, .data$sites_with_data, .data$summed_average
      )
    )
}
