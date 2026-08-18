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

# Shared Measurement Core: evidence classification
############################################################

#' Expand crosswalk rows to a complete Site Group-by-year universe.
#'
#' The one place that decides what a missing crosswalk row means. A site-year
#' the crosswalk never mentions is indistinguishable from one it reports as
#' `absent`, so the gap is filled as `absent` with no matched events before any
#' classification happens.
#'
#' The universe belongs to the caller because the two families need different
#' ones: the prefix reducer expands over the crosswalk's full horizon, the
#' window reducer over the years of one fixed window. Keeping the universe out
#' here leaves the truth table a pure classifier of the rows it is handed.
#'
#' Carries no validation, per R14.
#'
#' @param site_years Crosswalk rows at `site_id`-`year` grain.
#' @param site_ids Site Groups the universe must cover.
#' @param years Years the universe must cover.
#' @return A tibble with one row per `site_id`-`year` in the universe, carrying
#'   every column of `site_years` and ordered by `site_id` then `year`. Rows
#'   with no crosswalk match have `annual_status` `"absent"` and, when the
#'   column is present, `matched_event_count` `0L`; every other column stays
#'   missing. A filled gap is therefore indistinguishable from a row the
#'   crosswalk reports as `absent`, which is the intended reading.
expand_site_year_universe <- function(site_years, site_ids, years) {
  site_years <- tibble::as_tibble(site_years)

  expanded <- tidyr::expand_grid(
    site_id = sort(unique(as.integer(site_ids))),
    year = sort(unique(as.integer(years)))
  ) |>
    dplyr::left_join(site_years, by = c("site_id", "year")) |>
    dplyr::mutate(
      annual_status = tidyr::replace_na(.data$annual_status, "absent")
    )

  if ("matched_event_count" %in% names(expanded)) {
    expanded <- dplyr::mutate(
      expanded,
      matched_event_count = tidyr::replace_na(.data$matched_event_count, 0L)
    )
  }

  expanded
}

#' Classify Site Group-year evidence into the three atomic condition flags.
#'
#' The single evidence truth table behind both exposure families. It answers
#' only what one site-year's record says, and leaves every question of which
#' site-years to ask about, and how to combine their answers, to the reducers
#' above it.
#'
#' The three conditions are deliberately atomic rather than pre-combined: each
#' derivation ORs its own subset of them into a verdict, and holding them apart
#' is what lets those verdicts differ without the classification differing.
#'
#' Carries no validation, per R14.
#'
#' @param site_years Site Group-year rows carrying `site_id`, `year`, and
#'   `annual_status`. A missing `annual_status` counts as `absent`, so callers
#'   that have filled universe gaps through `expand_site_year_universe()` and
#'   callers that have not classify a missing record the same way.
#'   `matched_event_count` is optional: without it,
#'   `reported_positive_without_matched_events` comes back missing rather than
#'   falsely `FALSE`, so a verdict that ORs the flag is loud instead of
#'   silently wrong for a caller that does not yet read the column.
#' @return A tibble with one row per input site-year, in input order, carrying
#'   `site_id`, `year`, and the three logical flags `annual_returns_absent`,
#'   `annual_returns_na`, and `reported_positive_without_matched_events`.
classify_annual_returns_evidence <- function(site_years) {
  site_years <- tibble::as_tibble(site_years)
  annual_status <- site_years$annual_status
  matched_event_count <- if ("matched_event_count" %in% names(site_years)) {
    site_years$matched_event_count
  } else {
    NA_integer_
  }
  reported <- !is.na(annual_status)

  tibble::tibble(
    site_id = site_years$site_id,
    year = site_years$year,
    annual_returns_absent = !reported | annual_status == "absent",
    annual_returns_na = reported & annual_status == "reported_na",
    reported_positive_without_matched_events =
      reported & annual_status == "reported_positive" &
        matched_event_count == 0L
  )
}

#' Reduce Site Group-year evidence flags cumulatively to each cutoff year.
#'
#' The prior family's reducer. Each transaction looks back over the prefix of
#' years from the window's start through its own cutoff, so a flag raised in
#' any earlier year stays raised for every later cutoff.
#'
#' Carries no validation, per R14.
#'
#' @param evidence Output of `classify_annual_returns_evidence()`.
#' @return A tibble with one row per `site_id`-`year`, ordered by `site_id`
#'   then `year`, carrying `cutoff_year` and each flag accumulated with a
#'   running `any` within its Site Group.
reduce_evidence_flags_to_prefix <- function(evidence) {
  flag_columns <- setdiff(names(evidence), c("site_id", "year"))
  evidence |>
    dplyr::rename(cutoff_year = "year") |>
    dplyr::arrange(.data$site_id, .data$cutoff_year) |>
    dplyr::mutate(
      dplyr::across(dplyr::all_of(flag_columns), dplyr::cumany),
      .by = "site_id"
    )
}

#' Reduce Site Group-year evidence flags across one fixed window.
#'
#' The study family's reducer. Every transaction shares one window, so each
#' flag collapses to a single `any` per Site Group over the window's years.
#'
#' Carries no validation, per R14.
#'
#' @param evidence Output of `classify_annual_returns_evidence()`.
#' @return A tibble with one row per Site Group, ordered by `site_id`, carrying
#'   each flag reduced with `any` across the window.
reduce_evidence_flags_over_window <- function(evidence) {
  flag_columns <- setdiff(names(evidence), c("site_id", "year"))
  evidence |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(flag_columns), any),
      .by = "site_id"
    ) |>
    dplyr::arrange(.data$site_id)
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
#' @param include_annual_return_sequence Whether to include the site-level
#'   `annual_returns_na_then_absent` sequence flag. The full available
#'   crosswalk horizon is used for the later `absent` test.
#' @return A tibble unique on `site_id` and `cutoff_year`, with logical
#'   `site_missing` and, when requested, `has_unknown_event_evidence` and
#'   `annual_returns_na_then_absent`.
derive_site_group_prefix_missing_flags <- function(crosswalk, base_year,
                                                   cutoff_years,
                                                   include_event_evidence = FALSE,
                                                   include_annual_return_sequence = FALSE) {
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
  full_window_end_year <- if (isTRUE(include_annual_return_sequence)) {
    max(crosswalk$year)
  } else {
    -Inf
  }
  required_end_year <- max(
    c(
      full_window_end_year,
      if (length(non_empty_cutoffs) == 0L) -Inf else max(non_empty_cutoffs)
    )
  )
  required_years <- if (!is.finite(required_end_year)) {
    integer()
  } else {
    seq.int(base_year, required_end_year)
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
      has_unknown_event_evidence = logical(),
      has_reported_na_prefix = logical()
    )
  } else {
    # Each prefix flag is the shared truth table's atomic condition accumulated
    # to the cutoff. Accumulating the three conditions separately and ORing them
    # afterwards is the same result as ORing first and accumulating once, so the
    # historical `has_unknown_event_evidence` is unchanged.
    universe <- expand_site_year_universe(
      dplyr::select(
        observed, "site_id", "year", "annual_status", "matched_event_count"
      ),
      site_ids = all_site_ids,
      years = required_years
    )
    prefix_flags <- reduce_evidence_flags_to_prefix(
      classify_annual_returns_evidence(universe)
    )
    prefix_flags |>
      dplyr::transmute(
        site_id = .data$site_id,
        cutoff_year = .data$cutoff_year,
        site_missing = .data$annual_returns_absent,
        has_unknown_event_evidence = .data$annual_returns_absent |
          .data$annual_returns_na |
          .data$reported_positive_without_matched_events,
        has_reported_na_prefix = .data$annual_returns_na
      ) |>
      dplyr::filter(.data$cutoff_year %in% cutoff_years)
  }

  if (isTRUE(include_annual_return_sequence) && nrow(non_empty_prefixes) > 0L) {
    absent_rows <- observed |>
      dplyr::filter(.data$annual_status == "absent")
    last_absent_year <- if (nrow(absent_rows) == 0L) {
      tibble::tibble(site_id = integer(), last_absent_year = integer())
    } else {
      absent_rows |>
        dplyr::summarise(
          last_absent_year = max(.data$year),
          .by = "site_id"
        )
    }
    non_empty_prefixes <- non_empty_prefixes |>
      dplyr::left_join(last_absent_year, by = "site_id") |>
      dplyr::mutate(
        annual_returns_na_then_absent =
          .data$has_reported_na_prefix &
          !.data$site_missing &
          !is.na(.data$last_absent_year) &
          .data$last_absent_year > .data$cutoff_year
      ) |>
      dplyr::select(-"last_absent_year")
  } else {
    non_empty_prefixes$annual_returns_na_then_absent <- FALSE
  }

  empty_prefixes <- if ((base_year - 1L) %in% cutoff_years) {
    tibble::tibble(
      site_id = all_site_ids,
      cutoff_year = base_year - 1L,
      site_missing = FALSE,
      has_unknown_event_evidence = FALSE,
      has_reported_na_prefix = FALSE,
      annual_returns_na_then_absent = FALSE
    )
  } else {
    tibble::tibble(
      site_id = integer(),
      cutoff_year = integer(),
      site_missing = logical(),
      has_unknown_event_evidence = logical(),
      has_reported_na_prefix = logical(),
      annual_returns_na_then_absent = logical()
    )
  }

  prefixes <- dplyr::bind_rows(empty_prefixes, non_empty_prefixes) |>
    dplyr::arrange(.data$site_id, .data$cutoff_year)
  output_columns <- c("site_id", "cutoff_year", "site_missing")
  if (isTRUE(include_event_evidence)) {
    output_columns <- c(output_columns, "has_unknown_event_evidence")
  }
  if (isTRUE(include_annual_return_sequence)) {
    output_columns <- c(output_columns, "annual_returns_na_then_absent")
  }
  prefixes <- dplyr::select(prefixes, dplyr::all_of(output_columns))
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
