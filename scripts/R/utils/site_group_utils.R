############################################################
# Site Group Consumer Utilities
# Project: Sewage
############################################################

#' Assert that a table has one row per Site Group.
#'
#' @param table A data frame containing `site_id`.
#' @param context Description used in failure messages.
#' @return `table`, invisibly.
assert_unique_site_groups <- function(table, context = "Site Group table") {
  if (!"site_id" %in% names(table)) {
    stop(context, " must contain site_id.", call. = FALSE)
  }
  if (any(is.na(table$site_id))) {
    stop(context, " must not contain missing site_id values.", call. = FALSE)
  }
  if (anyDuplicated(table$site_id)) {
    stop(context, " must be unique on site_id.", call. = FALSE)
  }
  invisible(table)
}

#' Assert that metadata attachment did not change the left-side row count.
#'
#' @param before Left-side table before the join.
#' @param after Joined table.
#' @param context Description used in failure messages.
#' @return `after`, invisibly.
assert_left_row_count <- function(before, after, context = "metadata join") {
  if (nrow(after) != nrow(before)) {
    stop(
      context, " changed the left-side row count from ", nrow(before),
      " to ", nrow(after), ".",
      call. = FALSE
    )
  }
  invisible(after)
}

#' Derive analysis-window missingness at Site Group grain.
#'
#' A Site Group is missing for an analysis window when at least one requested
#' group-year has `annual_status == "absent"` (or is not present in the
#' crosswalk). This deliberately uses the group-year status and never combines
#' Canonical Spill Site availability across members.
#'
#' @param crosswalk Site Group crosswalk at `site_id`-`year`-company grain.
#' @param years Analysis years that must all be reported.
#' @return A tibble unique on `site_id` with logical `site_missing`.
derive_site_group_missing_flags <- function(crosswalk, years) {
  required_columns <- c("site_id", "year", "water_company", "annual_status")
  missing_columns <- setdiff(required_columns, names(crosswalk))
  if (length(missing_columns) > 0L) {
    stop(
      "Site Group crosswalk is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  years <- as.integer(years)
  if (length(years) == 0L || anyNA(years) || anyDuplicated(years)) {
    stop("years must be a non-empty vector of unique integers.", call. = FALSE)
  }

  crosswalk <- tibble::as_tibble(crosswalk) |>
    dplyr::transmute(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year),
      water_company = as.character(.data$water_company),
      annual_status = as.character(.data$annual_status)
    )

  if (nrow(crosswalk) == 0L || anyNA(crosswalk[c("site_id", "year", "water_company")])) {
    stop("Site Group crosswalk keys must be non-empty and non-missing.", call. = FALSE)
  }
  if (anyDuplicated(crosswalk[c("site_id", "year", "water_company")])) {
    stop(
      "Site Group crosswalk must be unique on site_id, year, water_company.",
      call. = FALSE
    )
  }

  company_counts <- crosswalk |>
    dplyr::summarise(
      n_water_company = dplyr::n_distinct(.data$water_company),
      .by = "site_id"
    )
  if (any(company_counts$n_water_company != 1L)) {
    stop("Each Site Group must have exactly one water_company.", call. = FALSE)
  }

  valid_statuses <- c("absent", "reported_zero", "reported_positive", "reported_na")
  observed <- crosswalk |>
    dplyr::filter(.data$year %in% years)
  if (anyNA(observed$annual_status) || any(!observed$annual_status %in% valid_statuses)) {
    stop("Requested Site Group years must have valid annual_status values.", call. = FALSE)
  }

  tidyr::expand_grid(
    site_id = sort(unique(crosswalk$site_id)),
    year = sort(years)
  ) |>
    dplyr::left_join(
      dplyr::select(observed, "site_id", "year", "annual_status"),
      by = c("site_id", "year")
    ) |>
    dplyr::mutate(
      annual_status = tidyr::replace_na(.data$annual_status, "absent")
    ) |>
    dplyr::summarise(
      site_missing = !all(.data$annual_status != "absent"),
      .by = "site_id"
    ) |>
    dplyr::arrange(.data$site_id)
}

#' Read Site Group analysis-window missingness from the crosswalk.
#'
#' @param file_path Path to `site_group_crosswalk.parquet`.
#' @inheritParams derive_site_group_missing_flags
#' @return A tibble unique on `site_id` with logical `site_missing`.
read_site_group_missing_flags <- function(file_path, years) {
  derive_site_group_missing_flags(arrow::read_parquet(file_path), years)
}

#' Derive cumulative Site Group missingness and event evidence for transaction cutoffs.
#'
#' Each cutoff covers the prefix from `base_year` through `cutoff_year`. The
#' cutoff immediately before `base_year` represents an explicit empty prefix
#' for known Site Groups. Required exposure years must exist in the crosswalk's
#' global year coverage; a missing Site Group row within a supported year is
#' still interpreted as `annual_status == "absent"`.
#'
#' @inheritParams derive_site_group_missing_flags
#' @param base_year First year in the exposure window.
#' @param cutoff_years Exclusive transaction cutoff years to return. Values may
#'   include `base_year - 1L` for the explicit empty prefix.
#' @param include_event_evidence Whether to include cumulative unknown-event
#'   evidence alongside the historical prefix-missingness contract.
#' @return A tibble unique on `site_id` and `cutoff_year`, with logical
#'   `site_missing` and, when requested, `has_unknown_event_evidence`.
derive_site_group_prefix_missing_flags <- function(crosswalk, base_year,
                                                   cutoff_years,
                                                   include_event_evidence = FALSE) {
  required_columns <- c(
    "site_id", "year", "water_company", "annual_status",
    "matched_event_count"
  )
  missing_columns <- setdiff(required_columns, names(crosswalk))
  if (length(missing_columns) > 0L) {
    stop(
      "Site Group crosswalk is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  base_year <- as.integer(base_year)
  cutoff_years <- as.integer(cutoff_years)
  if (length(base_year) != 1L || is.na(base_year)) {
    stop("base_year must be one integer.", call. = FALSE)
  }
  if (length(cutoff_years) == 0L || anyNA(cutoff_years) ||
      anyDuplicated(cutoff_years)) {
    stop(
      "cutoff_years must be a non-empty vector of unique integers.",
      call. = FALSE
    )
  }
  if (any(cutoff_years < base_year - 1L)) {
    stop(
      "cutoff_years must not precede the explicit empty-prefix year.",
      call. = FALSE
    )
  }
  cutoff_years <- sort(cutoff_years)

  crosswalk <- tibble::as_tibble(crosswalk)
  matched_event_count <- crosswalk$matched_event_count
  if (!is.numeric(matched_event_count) || anyNA(matched_event_count) ||
      any(!is.finite(matched_event_count)) ||
      any(matched_event_count < 0) ||
      any(matched_event_count != floor(matched_event_count)) ||
      any(matched_event_count > .Machine$integer.max)) {
    stop(
      "matched_event_count must contain non-missing, nonnegative, integer-like values.",
      call. = FALSE
    )
  }

  crosswalk <- crosswalk |>
    dplyr::transmute(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year),
      water_company = as.character(.data$water_company),
      annual_status = as.character(.data$annual_status),
      matched_event_count = as.integer(.data$matched_event_count)
    )

  if (nrow(crosswalk) == 0L ||
      anyNA(crosswalk[c("site_id", "year", "water_company")])) {
    stop("Site Group crosswalk keys must be non-empty and non-missing.", call. = FALSE)
  }
  if (anyDuplicated(crosswalk[c("site_id", "year", "water_company")])) {
    stop(
      "Site Group crosswalk must be unique on site_id, year, water_company.",
      call. = FALSE
    )
  }

  company_counts <- crosswalk |>
    dplyr::summarise(
      n_water_company = dplyr::n_distinct(.data$water_company),
      .by = "site_id"
    )
  if (any(company_counts$n_water_company != 1L)) {
    stop("Each Site Group must have exactly one water_company.", call. = FALSE)
  }

  valid_statuses <- c("absent", "reported_zero", "reported_positive", "reported_na")
  if (anyNA(crosswalk$annual_status) ||
      any(!crosswalk$annual_status %in% valid_statuses)) {
    stop("Requested Site Group years must have valid annual_status values.", call. = FALSE)
  }

  contradiction_statuses <- c("reported_zero", "reported_na", "absent")
  contradiction_counts <- crosswalk |>
    dplyr::filter(
      .data$annual_status %in% contradiction_statuses,
      .data$matched_event_count > 0L
    ) |>
    dplyr::count(.data$annual_status, name = "n")
  for (status in contradiction_statuses) {
    count <- contradiction_counts |>
      dplyr::filter(.data$annual_status == status) |>
      dplyr::pull(.data$n)
    count <- dplyr::first(count, default = 0L)
    message(
      "Event-bearing Annual Status contradiction: ", status, " = ", count,
      " Site Group-year(s)."
    )
  }

  non_empty_cutoffs <- cutoff_years[cutoff_years >= base_year]
  required_years <- if (length(non_empty_cutoffs) == 0L) {
    integer()
  } else {
    seq.int(base_year, max(non_empty_cutoffs))
  }
  unsupported_years <- setdiff(required_years, sort(unique(crosswalk$year)))
  if (length(unsupported_years) > 0L) {
    stop(
      "Unsupported Site Group crosswalk year(s): ",
      paste(unsupported_years, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  observed <- crosswalk |>
    dplyr::filter(.data$year %in% required_years)

  all_site_ids <- sort(unique(crosswalk$site_id))
  non_empty_prefixes <- if (length(required_years) == 0L) {
    tibble::tibble(
      site_id = integer(),
      cutoff_year = integer(),
      site_missing = logical(),
      has_unknown_event_evidence = logical()
    )
  } else {
    tidyr::expand_grid(
      site_id = all_site_ids,
      cutoff_year = required_years
    ) |>
      dplyr::left_join(
        dplyr::select(
          observed,
          "site_id",
          cutoff_year = "year",
          "annual_status",
          "matched_event_count"
        ),
        by = c("site_id", "cutoff_year")
      ) |>
      dplyr::arrange(.data$site_id, .data$cutoff_year) |>
      dplyr::mutate(
        annual_status = tidyr::replace_na(.data$annual_status, "absent"),
        matched_event_count = tidyr::replace_na(.data$matched_event_count, 0L),
        event_evidence_unknown =
          .data$annual_status %in% c("reported_na", "absent") |
          (.data$annual_status == "reported_positive" &
            .data$matched_event_count == 0L),
        site_missing = dplyr::cumany(.data$annual_status == "absent"),
        has_unknown_event_evidence = dplyr::cumany(.data$event_evidence_unknown),
        .by = "site_id"
      ) |>
      dplyr::filter(.data$cutoff_year %in% cutoff_years) |>
      dplyr::select(
        "site_id", "cutoff_year", "site_missing",
        "has_unknown_event_evidence"
      )
  }

  empty_prefixes <- if ((base_year - 1L) %in% cutoff_years) {
    tibble::tibble(
      site_id = all_site_ids,
      cutoff_year = base_year - 1L,
      site_missing = FALSE,
      has_unknown_event_evidence = FALSE
    )
  } else {
    tibble::tibble(
      site_id = integer(),
      cutoff_year = integer(),
      site_missing = logical(),
      has_unknown_event_evidence = logical()
    )
  }

  prefixes <- dplyr::bind_rows(empty_prefixes, non_empty_prefixes) |>
    dplyr::arrange(.data$site_id, .data$cutoff_year)
  if (!isTRUE(include_event_evidence)) {
    prefixes <- dplyr::select(
      prefixes, "site_id", "cutoff_year", "site_missing"
    )
  }
  prefixes
}

#' Derive one explicit metadata row per Site Group.
#'
#' Location follows the crosswalk's representative-location contract: among
#' configured years, use the most recent row with a complete parsed NGR and
#' coordinates. A newer unparseable NGR cannot shadow an older valid point.
#'
#' @param crosswalk Site Group crosswalk at `site_id`-`year`-company grain.
#' @param years Explicit configured reporting years.
#' @param include_availability Whether to add `available_year_YYYY` columns.
#' @return A tibble unique on `site_id`.
derive_site_group_projection <- function(crosswalk, years,
                                         include_availability = FALSE) {
  required_columns <- c(
    "site_id", "year", "water_company", "annual_status",
    "ngr", "easting", "northing"
  )
  missing_columns <- setdiff(required_columns, names(crosswalk))
  if (length(missing_columns) > 0L) {
    stop(
      "Site Group crosswalk is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  years <- as.integer(years)
  if (length(years) == 0L || anyNA(years) || anyDuplicated(years)) {
    stop("years must be a non-empty vector of unique integers.", call. = FALSE)
  }
  years <- sort(years)

  crosswalk <- tibble::as_tibble(crosswalk) |>
    dplyr::mutate(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year),
      water_company = as.character(.data$water_company),
      annual_status = as.character(.data$annual_status),
      ngr = as.character(.data$ngr),
      easting = as.numeric(.data$easting),
      northing = as.numeric(.data$northing)
    )

  if (nrow(crosswalk) == 0L) {
    stop("Site Group crosswalk must not be empty.", call. = FALSE)
  }
  if (anyNA(crosswalk$site_id) || anyNA(crosswalk$year) ||
      anyNA(crosswalk$water_company)) {
    stop(
      "Site Group crosswalk keys must not contain missing site_id, year, or water_company.",
      call. = FALSE
    )
  }
  if (anyDuplicated(crosswalk[c("site_id", "year", "water_company")])) {
    stop(
      "Site Group crosswalk must be unique on site_id, year, water_company.",
      call. = FALSE
    )
  }

  company_counts <- crosswalk |>
    dplyr::group_by(.data$site_id) |>
    dplyr::summarise(
      n_water_company = dplyr::n_distinct(.data$water_company),
      .groups = "drop"
    )
  if (any(company_counts$n_water_company != 1L)) {
    stop("Each Site Group must have exactly one water_company.", call. = FALSE)
  }

  configured <- crosswalk |>
    dplyr::filter(.data$year %in% years)
  expected_rows <- length(years)
  year_counts <- configured |>
    dplyr::group_by(.data$site_id) |>
    dplyr::summarise(
      n_year = dplyr::n_distinct(.data$year),
      n_row = dplyr::n(),
      .groups = "drop"
    )
  all_site_ids <- sort(unique(crosswalk$site_id))
  if (!identical(sort(year_counts$site_id), all_site_ids) ||
      any(year_counts$n_year != expected_rows) ||
      any(year_counts$n_row != expected_rows)) {
    stop(
      "Each Site Group must have one row for every configured year.",
      call. = FALSE
    )
  }

  valid_statuses <- c("absent", "reported_zero", "reported_positive", "reported_na")
  if (anyNA(configured$annual_status) ||
      any(!configured$annual_status %in% valid_statuses)) {
    stop(
      "Configured Site Group years must have a valid annual_status.",
      call. = FALSE
    )
  }

  companies <- configured |>
    dplyr::group_by(.data$site_id) |>
    dplyr::summarise(
      water_company = dplyr::first(.data$water_company),
      .groups = "drop"
    )

  valid_locations <- configured |>
    dplyr::filter(
      !is.na(.data$ngr),
      !is.na(.data$easting),
      !is.na(.data$northing)
    ) |>
    dplyr::arrange(.data$site_id, dplyr::desc(.data$year)) |>
    dplyr::group_by(.data$site_id) |>
    dplyr::slice_head(n = 1L) |>
    dplyr::ungroup() |>
    dplyr::select("site_id", "ngr", "easting", "northing")

  projection <- tibble::tibble(site_id = all_site_ids) |>
    dplyr::left_join(companies, by = "site_id") |>
    dplyr::left_join(valid_locations, by = "site_id")

  if (isTRUE(include_availability)) {
    availability <- configured |>
      dplyr::transmute(
        site_id = .data$site_id,
        availability_column = paste0("available_year_", .data$year),
        available = .data$annual_status != "absent"
      ) |>
      tidyr::pivot_wider(
        names_from = "availability_column",
        values_from = "available"
      ) |>
      dplyr::select("site_id", dplyr::all_of(paste0("available_year_", years)))
    projection <- dplyr::left_join(projection, availability, by = "site_id")
  }

  assert_unique_site_groups(projection, "Site Group projection")
  projection
}

#' Read and project the Site Group crosswalk.
#'
#' @param file_path Path to `site_group_crosswalk.parquet`.
#' @inheritParams derive_site_group_projection
#' @return A tibble unique on `site_id`.
read_site_group_projection <- function(file_path, years,
                                       include_availability = FALSE) {
  derive_site_group_projection(
    arrow::read_parquet(file_path),
    years = years,
    include_availability = include_availability
  )
}

#' Attach a unique Site Group projection with a row-conservation gate.
#'
#' @param left Left-side data frame.
#' @param projection One-row-per-`site_id` Site Group metadata.
#' @param context Description used in failure messages.
#' @return The left join result in original left-side order.
left_join_site_group_projection <- function(left, projection,
                                            context = "Site Group metadata join") {
  assert_unique_site_groups(projection, "Site Group projection")
  joined <- dplyr::left_join(left, projection, by = "site_id")
  assert_left_row_count(left, joined, context)
  joined
}
