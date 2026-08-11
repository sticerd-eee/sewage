# ==============================================================================
# Canonical EDM Commission Figure Utilities
# ==============================================================================

if (!exists("validate_commission_resolution", mode = "function")) {
  if (!requireNamespace("here", quietly = TRUE)) {
    stop("Package `here` is required to source commission contracts.", call. = FALSE)
  }
  source(
    here::here("scripts", "R", "utils", "edm_commission_utils.R"),
    local = TRUE
  )
}

EDM_COMMISSION_STATUS_LABELS <- c(
  resolved = "Resolved",
  missing = "Missing",
  future_only = "Future-only",
  not_commissioned = "Not commissioned as of report",
  not_feasible = "Not feasible",
  actual_state_conflict = "Actual/state conflict",
  conflicting_actual_dates = "Conflicting actual dates",
  before_2016 = "Pre-2016 (imprecise)",
  invalid_placeholder = "Invalid placeholder",
  unparseable = "Unparseable"
)

assert_edm_commission_figure_input <- function(unique_sites) {
  required <- c(
    "site_id_canonical",
    "edm_commission_date",
    "edm_commission_date_precision",
    "edm_commission_resolution_status"
  )
  missing_columns <- setdiff(required, names(unique_sites))
  if (length(missing_columns) > 0L) {
    stop(
      "Canonical commission figure input missing columns: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  canonical_id <- unique_sites$site_id_canonical
  missing_id <- is.na(canonical_id)
  if (is.character(canonical_id)) missing_id <- missing_id | trimws(canonical_id) == ""
  if (any(missing_id) || anyDuplicated(canonical_id)) {
    stop(
      "Canonical commission figure input site_id_canonical must be non-missing and unique.",
      call. = FALSE
    )
  }
  if (!inherits(unique_sites$edm_commission_date, "Date")) {
    stop(
      "Canonical commission figure input edm_commission_date must use Date type.",
      call. = FALSE
    )
  }

  validate_commission_resolution(unique_sites)
  invisible(unique_sites)
}

prepare_edm_commission_figure_data <- function(unique_sites) {
  assert_edm_commission_figure_input(unique_sites)

  n_canonical_sites <- nrow(unique_sites)
  resolved <- unique_sites |>
    dplyr::filter(.data$edm_commission_resolution_status == "resolved") |>
    dplyr::mutate(
      commission_year = as.integer(format(.data$edm_commission_date, "%Y"))
    )
  n_resolved <- nrow(resolved)
  if (n_resolved == 0L) {
    stop(
      "Canonical commission figure input has no resolved commission histories.",
      call. = FALSE
    )
  }

  annual_timing <- resolved |>
    dplyr::count(.data$commission_year, name = "n_canonical_sites") |>
    dplyr::arrange(.data$commission_year) |>
    dplyr::mutate(
      conditional_percentage = .data$n_canonical_sites / .env$n_resolved * 100,
      share_of_canonical_universe =
        .data$n_canonical_sites / .env$n_canonical_sites * 100
    )

  annual_cumulative <- annual_timing |>
    dplyr::mutate(
      cumulative_count = cumsum(.data$n_canonical_sites),
      cumulative_percentage = .data$cumulative_count / .env$n_resolved * 100
    )

  status_counts <- unique_sites |>
    dplyr::count(
      .data$edm_commission_resolution_status,
      name = "n_canonical_sites"
    )
  completeness <- tibble::tibble(
    edm_commission_resolution_status = EDM_COMMISSION_RESOLUTION_STATUSES
  ) |>
    dplyr::left_join(
      status_counts,
      by = "edm_commission_resolution_status"
    ) |>
    dplyr::mutate(
      n_canonical_sites = dplyr::coalesce(.data$n_canonical_sites, 0L),
      share_of_canonical_universe =
        .data$n_canonical_sites / .env$n_canonical_sites * 100,
      status_label = unname(
        EDM_COMMISSION_STATUS_LABELS[.data$edm_commission_resolution_status]
      )
    )

  pre_2016 <- completeness |>
    dplyr::filter(.data$edm_commission_resolution_status == "before_2016") |>
    dplyr::transmute(
      timing_category = "Pre-2016 (imprecise)",
      timing_basis = "imprecise_pre_2016",
      commission_year = NA_integer_,
      n_canonical_sites = .data$n_canonical_sites,
      conditional_percentage = NA_real_,
      share_of_canonical_universe = .data$share_of_canonical_universe
    )
  timing_categories <- dplyr::bind_rows(
    pre_2016,
    annual_timing |>
      dplyr::transmute(
        timing_category = as.character(.data$commission_year),
        timing_basis = "resolved_year",
        commission_year = .data$commission_year,
        n_canonical_sites = .data$n_canonical_sites,
        conditional_percentage = .data$conditional_percentage,
        share_of_canonical_universe = .data$share_of_canonical_universe
      )
  )

  n_pre_2016 <- completeness$n_canonical_sites[
    completeness$edm_commission_resolution_status == "before_2016"
  ]
  diagnostics <- tibble::tibble(
    n_canonical_sites = as.integer(n_canonical_sites),
    n_resolved = as.integer(n_resolved),
    n_pre_2016 = as.integer(n_pre_2016),
    n_other_unresolved = as.integer(n_canonical_sites - n_resolved - n_pre_2016),
    resolved_share_of_canonical_universe = n_resolved / n_canonical_sites * 100
  )

  list(
    annual_timing = annual_timing,
    annual_cumulative = annual_cumulative,
    timing_categories = timing_categories,
    completeness = completeness,
    diagnostics = diagnostics
  )
}

format_edm_commission_figure_note <- function(figure_data) {
  diagnostics <- figure_data$diagnostics
  completeness <- figure_data$completeness |>
    dplyr::filter(.data$edm_commission_resolution_status != "resolved")
  status_summary <- paste0(
    completeness$status_label,
    ": ",
    completeness$n_canonical_sites,
    " (",
    sprintf("%.1f", completeness$share_of_canonical_universe),
    "%)",
    collapse = "; "
  )

  paste0(
    "Unit: Canonical Spill Site (stable monitored discharge point). Timing ",
    "percentages are conditional on ", diagnostics$n_resolved,
    " resolved commission histories (",
    sprintf("%.1f", diagnostics$resolved_share_of_canonical_universe),
    "% of ", diagnostics$n_canonical_sites,
    " canonical sites). Full-universe completeness: ", status_summary, "."
  )
}

print_edm_commission_diagnostics <- function(figure_data) {
  diagnostics <- figure_data$diagnostics
  cat("Canonical EDM commission figure diagnostics:\n")
  cat("  Canonical Spill Sites:", diagnostics$n_canonical_sites, "\n")
  cat(
    "  Resolved timing denominator:", diagnostics$n_resolved,
    sprintf("(%.2f%%)\n", diagnostics$resolved_share_of_canonical_universe)
  )
  cat("  Status reconciliation:\n")
  for (index in seq_len(nrow(figure_data$completeness))) {
    row <- figure_data$completeness[index, ]
    cat(
      "   - ", row$edm_commission_resolution_status, ": ",
      row$n_canonical_sites,
      sprintf(" (%.2f%%)\n", row$share_of_canonical_universe),
      sep = ""
    )
  }
  invisible(figure_data)
}
