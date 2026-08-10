# ==============================================================================
# EDM Commission Evidence Utilities
# ==============================================================================

EDM_COMMISSION_OBSERVATION_CLASSES <- c(
  "missing",
  "actual",
  "future_deadline",
  "before_2016",
  "not_commissioned_as_of_report",
  "not_feasible",
  "invalid_placeholder",
  "unparseable"
)

EDM_COMMISSION_RESOLUTION_STATUSES <- c(
  "resolved",
  "missing",
  "future_only",
  "not_commissioned",
  "not_feasible",
  "actual_state_conflict",
  "conflicting_actual_dates",
  "before_2016",
  "invalid_placeholder",
  "unparseable"
)

EDM_COMMISSION_PRECISIONS <- c(
  "day", "month", "year", "vague", "unknown", "conflict"
)

EDM_ENGLISH_MONTHS <- c(
  jan = 1L, january = 1L,
  feb = 2L, february = 2L,
  mar = 3L, march = 3L,
  apr = 4L, april = 4L,
  may = 5L,
  jun = 6L, june = 6L,
  jul = 7L, july = 7L,
  aug = 8L, august = 8L,
  sep = 9L, sept = 9L, september = 9L,
  oct = 10L, october = 10L,
  nov = 11L, november = 11L,
  dec = 12L, december = 12L
)

#' Normalize commission source text without losing the original value
#' @param x Character-like values
#' @return Lower-case, whitespace-normalized character values
normalise_commission_text <- function(x) {
  normalized <- trimws(as.character(x))
  normalized <- gsub("[[:space:]]+", " ", normalized)
  normalized[normalized == ""] <- NA_character_
  tolower(normalized)
}

commission_month_end <- function(date) {
  as.Date(seq(date, by = "month", length.out = 2L)[2L]) - 1
}

commission_year_interval <- function(year) {
  list(
    start = as.Date(sprintf("%04d-01-01", year)),
    end = as.Date(sprintf("%04d-12-31", year)),
    precision = "year"
  )
}

parse_commission_interval <- function(normalized_text) {
  if (is.na(normalized_text)) return(NULL)

  excel_match <- grepl("^[0-9]{5}$", normalized_text)
  if (excel_match) {
    decoded <- as.Date(as.integer(normalized_text), origin = "1899-12-30")
    decoded_year <- as.integer(format(decoded, "%Y"))
    if (!is.na(decoded_year) && decoded_year >= 1900L && decoded_year <= 2100L) {
      return(list(start = decoded, end = decoded, precision = "day"))
    }
    return(NULL)
  }

  iso_match <- regmatches(
    normalized_text,
    regexec("^((?:19|20)[0-9]{2})-([0-9]{2})-([0-9]{2})$", normalized_text, perl = TRUE)
  )[[1L]]
  if (length(iso_match) > 0L) {
    parsed <- suppressWarnings(as.Date(iso_match[1L]))
    if (!is.na(parsed)) return(list(start = parsed, end = parsed, precision = "day"))
  }

  month_names <- paste(names(EDM_ENGLISH_MONTHS), collapse = "|")
  month_pattern <- sprintf(
    "\\b(%s)\\s+((?:19|20)[0-9]{2})\\b",
    month_names
  )
  month_match <- regmatches(
    normalized_text,
    regexec(month_pattern, normalized_text, perl = TRUE)
  )[[1L]]
  if (length(month_match) > 0L) {
    month <- unname(EDM_ENGLISH_MONTHS[[month_match[2L]]])
    year <- as.integer(month_match[3L])
    start <- as.Date(sprintf("%04d-%02d-01", year, month))
    return(list(
      start = start,
      end = commission_month_end(start),
      precision = "month"
    ))
  }

  commissioned_match <- regmatches(
    normalized_text,
    regexec(
      "\\bcommissioned(?:\\s+in)?\\s+((?:19|20)[0-9]{2})\\b",
      normalized_text,
      perl = TRUE
    )
  )[[1L]]
  if (length(commissioned_match) > 0L) {
    return(commission_year_interval(as.integer(commissioned_match[2L])))
  }

  if (grepl("^(?:19|20)[0-9]{2}$", normalized_text, perl = TRUE)) {
    return(commission_year_interval(as.integer(normalized_text)))
  }

  future_year_match <- regmatches(
    normalized_text,
    regexec(
      "(?:to be installed|installed)\\s+(?:by\\s+)?((?:19|20)[0-9]{2})\\b",
      normalized_text,
      perl = TRUE
    )
  )[[1L]]
  if (length(future_year_match) > 0L) {
    return(commission_year_interval(as.integer(future_year_match[2L])))
  }

  NULL
}

classify_commission_observation <- function(text, report_year) {
  original_text <- as.character(text)
  normalized_text <- normalise_commission_text(text)
  report_year <- as.integer(report_year)

  result <- data.frame(
    original_text = original_text,
    normalized_text = normalized_text,
    report_year = report_year,
    observation_class = "missing",
    candidate_start = as.Date(NA),
    candidate_end = as.Date(NA),
    candidate_precision = "unknown",
    stringsAsFactors = FALSE
  )

  if (is.na(normalized_text)) return(result)

  if (identical(normalized_text, "0")) {
    result$observation_class <- "invalid_placeholder"
    return(result)
  }

  if (grepl("not technically feasible|not feasible", normalized_text, perl = TRUE)) {
    result$observation_class <- "not_feasible"
    return(result)
  }

  if (grepl("not yet commissioned|not commissioned", normalized_text, perl = TRUE)) {
    result$observation_class <- "not_commissioned_as_of_report"
    return(result)
  }

  if (grepl("pre[- ]?2016|before 2016", normalized_text, perl = TRUE)) {
    result$observation_class <- "before_2016"
    result$candidate_end <- as.Date("2015-12-31")
    result$candidate_precision <- "vague"
    return(result)
  }

  interval <- parse_commission_interval(normalized_text)
  has_future_wording <- grepl(
    "to be installed|installed\\s+(?:by\\s+)",
    normalized_text,
    perl = TRUE
  )

  if (is.null(interval)) {
    if (has_future_wording) result$observation_class <- "future_deadline"
    else result$observation_class <- "unparseable"
    return(result)
  }

  result$candidate_start <- interval$start
  result$candidate_end <- interval$end
  result$candidate_precision <- interval$precision
  candidate_year <- as.integer(format(interval$start, "%Y"))
  if (has_future_wording || (!is.na(report_year) && candidate_year > report_year)) {
    result$observation_class <- "future_deadline"
  } else {
    result$observation_class <- "actual"
  }
  result
}

#' Classify annual EDM commission observations
#' @param texts Source values, preserved in `original_text`
#' @param report_years Reporting years corresponding to `texts`
#' @return A data frame of classified observations and candidate intervals
classify_commission_observations <- function(texts, report_years) {
  if (length(texts) != length(report_years)) {
    stop("Commission texts and reporting years must have equal length.", call. = FALSE)
  }
  if (length(texts) == 0L) {
    return(data.frame(
      original_text = character(),
      normalized_text = character(),
      report_year = integer(),
      observation_class = character(),
      candidate_start = as.Date(character()),
      candidate_end = as.Date(character()),
      candidate_precision = character(),
      stringsAsFactors = FALSE
    ))
  }
  if (anyNA(report_years)) {
    stop("Commission reporting years must be non-missing.", call. = FALSE)
  }

  observations <- do.call(
    rbind,
    Map(classify_commission_observation, texts, report_years)
  )
  rownames(observations) <- NULL
  validate_commission_observations(observations)
  observations
}

#' Validate classified observation vocabularies and interval combinations
#' @param observations Classified commission observations
#' @return `observations`, invisibly
validate_commission_observations <- function(observations) {
  unknown_class <- setdiff(
    unique(observations$observation_class),
    EDM_COMMISSION_OBSERVATION_CLASSES
  )
  if (length(unknown_class) > 0L) {
    stop(
      "Unknown commission observation class: ",
      paste(unknown_class, collapse = ", "),
      call. = FALSE
    )
  }

  unknown_precision <- setdiff(
    unique(observations$candidate_precision),
    c("day", "month", "year", "vague", "unknown")
  )
  if (length(unknown_precision) > 0L) {
    stop(
      "Unknown commission observation precision: ",
      paste(unknown_precision, collapse = ", "),
      call. = FALSE
    )
  }

  is_actual <- observations$observation_class == "actual"
  is_future <- observations$observation_class == "future_deadline"
  is_before <- observations$observation_class == "before_2016"
  has_complete_interval <- !is.na(observations$candidate_start) &
    !is.na(observations$candidate_end)
  has_dated_precision <- observations$candidate_precision %in% c(
    "day", "month", "year"
  )
  invalid <-
    (is_actual & (!has_complete_interval | !has_dated_precision)) |
    (is_future &
      ((has_complete_interval & !has_dated_precision) |
        (!has_complete_interval & observations$candidate_precision != "unknown"))) |
    (!is_actual & !is_future & !is_before &
      (has_complete_interval | observations$candidate_precision != "unknown")) |
    (is_before &
      (!is.na(observations$candidate_start) |
        is.na(observations$candidate_end) |
        observations$candidate_precision != "vague")) |
    (has_complete_interval &
      observations$candidate_start > observations$candidate_end)
  if (any(invalid)) {
    stop("Invalid commission observation combination.", call. = FALSE)
  }

  invisible(observations)
}

new_commission_resolution <- function(status, precision, date = as.Date(NA)) {
  result <- data.frame(
    edm_commission_date = as.Date(date, origin = "1970-01-01"),
    edm_commission_date_precision = precision,
    edm_commission_resolution_status = status,
    stringsAsFactors = FALSE
  )
  validate_commission_resolution(result)
  result
}

#' Validate canonical commission resolution vocabularies and combinations
#' @param resolution One or more canonical resolution rows
#' @return `resolution`, invisibly
validate_commission_resolution <- function(resolution) {
  required <- c(
    "edm_commission_date",
    "edm_commission_date_precision",
    "edm_commission_resolution_status"
  )
  missing_columns <- setdiff(required, names(resolution))
  if (length(missing_columns) > 0L) {
    stop(
      "Commission resolution missing columns: ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  unknown_status <- setdiff(
    unique(resolution$edm_commission_resolution_status),
    EDM_COMMISSION_RESOLUTION_STATUSES
  )
  if (length(unknown_status) > 0L) {
    stop(
      "Unknown commission resolution status: ",
      paste(unknown_status, collapse = ", "),
      call. = FALSE
    )
  }

  unknown_precision <- setdiff(
    unique(resolution$edm_commission_date_precision),
    EDM_COMMISSION_PRECISIONS
  )
  if (length(unknown_precision) > 0L) {
    stop(
      "Unknown commission resolution precision: ",
      paste(unknown_precision, collapse = ", "),
      call. = FALSE
    )
  }

  status <- resolution$edm_commission_resolution_status
  precision <- resolution$edm_commission_date_precision
  has_date <- !is.na(resolution$edm_commission_date)
  valid <-
    (status == "resolved" & has_date & precision %in% c("day", "month", "year")) |
    (status == "before_2016" & !has_date & precision == "vague") |
    (status %in% c("actual_state_conflict", "conflicting_actual_dates") &
      !has_date & precision == "conflict") |
    (status %in% c(
      "missing", "future_only", "not_commissioned", "not_feasible",
      "invalid_placeholder", "unparseable"
    ) & !has_date & precision == "unknown")

  if (any(!valid)) {
    stop("Invalid commission resolution combination.", call. = FALSE)
  }
  invisible(resolution)
}

resolve_actual_intervals <- function(actual) {
  intersection_start <- max(actual$candidate_start)
  intersection_end <- min(actual$candidate_end)
  if (intersection_start > intersection_end) return(NULL)

  precision_rank <- c(year = 1L, month = 2L, day = 3L)
  precision <- names(which.max(precision_rank[actual$candidate_precision]))
  precise_rows <- actual$candidate_precision == precision
  date <- max(actual$candidate_start[precise_rows])
  list(date = as.Date(date, origin = "1970-01-01"), precision = precision)
}

#' Resolve a canonical commission history from classified annual evidence
#' @param texts Source commission values
#' @param report_years Corresponding reporting years
#' @return One typed row with date, precision, and canonical status
resolve_commission_history <- function(texts, report_years) {
  observations <- classify_commission_observations(texts, report_years)
  actual <- observations[observations$observation_class == "actual", , drop = FALSE]
  before <- observations[
    observations$observation_class == "before_2016", , drop = FALSE
  ]
  states <- observations[
    observations$observation_class %in% c(
      "future_deadline", "not_commissioned_as_of_report", "not_feasible"
    ),
    , drop = FALSE
  ]

  actual_evidence <- rbind(actual, before)
  if (nrow(actual_evidence) > 0L && nrow(states) > 0L) {
    latest_actual_report <- max(actual_evidence$report_year, na.rm = TRUE)
    if (any(states$report_year >= latest_actual_report, na.rm = TRUE)) {
      return(new_commission_resolution(
        "actual_state_conflict", "conflict"
      ))
    }
  }

  if (nrow(actual) > 0L) {
    if (nrow(before) > 0L && any(actual$candidate_end >= as.Date("2016-01-01"))) {
      return(new_commission_resolution(
        "conflicting_actual_dates", "conflict"
      ))
    }
    resolved <- resolve_actual_intervals(actual)
    if (is.null(resolved)) {
      return(new_commission_resolution(
        "conflicting_actual_dates", "conflict"
      ))
    }
    return(new_commission_resolution(
      "resolved", resolved$precision, resolved$date
    ))
  }

  if (nrow(before) > 0L) {
    return(new_commission_resolution("before_2016", "vague"))
  }

  if (nrow(states) > 0L) {
    latest_year <- max(states$report_year, na.rm = TRUE)
    latest <- states[states$report_year == latest_year, , drop = FALSE]
    # Conservative, order-independent tie rule for states in the same return.
    state_rank <- c(
      future_deadline = 1L,
      not_commissioned_as_of_report = 2L,
      not_feasible = 3L
    )
    selected_class <- names(which.max(state_rank[latest$observation_class]))
    status <- switch(
      selected_class,
      future_deadline = "future_only",
      not_commissioned_as_of_report = "not_commissioned",
      not_feasible = "not_feasible"
    )
    return(new_commission_resolution(status, "unknown"))
  }

  residual <- observations[
    observations$observation_class %in% c("invalid_placeholder", "unparseable"),
    , drop = FALSE
  ]
  if (nrow(residual) > 0L) {
    latest_year <- max(residual$report_year, na.rm = TRUE)
    latest <- residual[residual$report_year == latest_year, , drop = FALSE]
    status <- if (any(latest$observation_class == "unparseable")) {
      "unparseable"
    } else {
      "invalid_placeholder"
    }
    return(new_commission_resolution(status, "unknown"))
  }

  new_commission_resolution("missing", "unknown")
}

#' Reconstruct current commission source-form evidence through the lookup
#' @param annual_data Annual Return EDM rows
#' @param lookup_data Annual Return Lookup rows
#' @return One runtime validation row per normalized non-empty form
build_commission_runtime_enumeration <- function(annual_data, lookup_data) {
  required_annual <- c("year", "edm_commission_date")
  required_lookup <- "site_id"
  if (length(setdiff(required_annual, names(annual_data))) > 0L ||
      !required_lookup %in% names(lookup_data)) {
    stop("Annual Return or lookup data lacks commission coverage columns.", call. = FALSE)
  }

  normalized <- normalise_commission_text(annual_data$edm_commission_date)
  keep <- !is.na(normalized)
  annual <- annual_data[keep, , drop = FALSE]
  normalized <- normalized[keep]
  canonical_site_id <- rep(NA_integer_, nrow(annual))

  for (report_year in sort(unique(as.integer(annual$year)))) {
    id_column <- paste0("site_id_", report_year)
    if (!id_column %in% names(annual) || !id_column %in% names(lookup_data)) {
      stop("Missing Annual Return Lookup column: ", id_column, call. = FALSE)
    }
    rows <- which(as.integer(annual$year) == report_year)
    canonical_site_id[rows] <- as.integer(lookup_data$site_id[
      match(annual[[id_column]][rows], lookup_data[[id_column]])
    ])
  }

  if (anyNA(canonical_site_id)) {
    stop(
      "Commission runtime enumeration found non-empty observations without lookup mapping.",
      call. = FALSE
    )
  }

  records <- data.frame(
    original_text = as.character(annual$edm_commission_date),
    normalized_text = normalized,
    report_year = as.integer(annual$year),
    site_id_canonical = canonical_site_id,
    stringsAsFactors = FALSE
  )
  groups <- split(seq_len(nrow(records)), records$normalized_text)
  rows <- lapply(groups, function(index) {
    values <- records[index, , drop = FALSE]
    semantic_years <- sort(unique(values$report_year))
    classified <- classify_commission_observations(
      rep(values$normalized_text[1L], length(semantic_years)),
      semantic_years
    )
    semantics <- unique(data.frame(
      observation_class = classified$observation_class,
      candidate_start = as.character(classified$candidate_start),
      candidate_end = as.character(classified$candidate_end),
      candidate_precision = classified$candidate_precision,
      stringsAsFactors = FALSE
    ))
    if (nrow(semantics) != 1L) {
      stop(
        "Normalized commission form has report-year-dependent semantics: ",
        values$normalized_text[1L],
        call. = FALSE
      )
    }
    site_examples <- head(sort(unique(values$site_id_canonical)), 5L)
    text_examples <- head(sort(unique(values$original_text)), 3L)
    data.frame(
      normalized_text = values$normalized_text[1L],
      observation_class = semantics$observation_class,
      candidate_start = semantics$candidate_start,
      candidate_end = semantics$candidate_end,
      candidate_precision = semantics$candidate_precision,
      n_observations = as.integer(nrow(values)),
      n_canonical_sites = as.integer(length(unique(values$site_id_canonical))),
      report_years = paste(semantic_years, collapse = ";"),
      original_text_examples = paste(text_examples, collapse = " | "),
      canonical_site_examples = paste(site_examples, collapse = ";"),
      stringsAsFactors = FALSE
    )
  })
  result <- do.call(rbind, rows)
  rownames(result) <- NULL
  result[order(result$normalized_text), , drop = FALSE]
}

#' Fail when runtime commission forms differ from reviewed fixture semantics
#' @param runtime Runtime enumeration from annual returns and lookup
#' @param fixture Reviewed golden source-form fixture
#' @return `TRUE`, invisibly
assert_commission_fixture_coverage <- function(runtime, fixture) {
  fixture_normalized <- normalise_commission_text(fixture$source_text)
  if (anyNA(fixture_normalized) || anyDuplicated(fixture_normalized)) {
    stop("Commission fixture must contain each normalized non-empty form once.", call. = FALSE)
  }

  missing_fixture <- setdiff(runtime$normalized_text, fixture_normalized)
  stale_fixture <- setdiff(fixture_normalized, runtime$normalized_text)
  if (length(missing_fixture) > 0L || length(stale_fixture) > 0L) {
    stop(
      paste0(
        "Commission fixture coverage differs from runtime forms.",
        if (length(missing_fixture) > 0L) {
          paste0(" Unreviewed: ", paste(missing_fixture, collapse = ", "), ".")
        } else "",
        if (length(stale_fixture) > 0L) {
          paste0(" Not observed: ", paste(stale_fixture, collapse = ", "), ".")
        } else ""
      ),
      call. = FALSE
    )
  }

  fixture_view <- data.frame(
    normalized_text = fixture_normalized,
    observation_class = fixture$observation_class,
    candidate_start = ifelse(is.na(fixture$candidate_start), NA_character_, fixture$candidate_start),
    candidate_end = ifelse(is.na(fixture$candidate_end), NA_character_, fixture$candidate_end),
    candidate_precision = fixture$candidate_precision,
    stringsAsFactors = FALSE
  )
  fixture_view <- fixture_view[order(fixture_view$normalized_text), , drop = FALSE]
  runtime_view <- runtime[c(
    "normalized_text", "observation_class", "candidate_start",
    "candidate_end", "candidate_precision"
  )]
  runtime_view <- runtime_view[order(runtime_view$normalized_text), , drop = FALSE]
  rownames(fixture_view) <- NULL
  rownames(runtime_view) <- NULL
  if (!identical(runtime_view, fixture_view)) {
    stop("Runtime commission semantics differ from the reviewed fixture.", call. = FALSE)
  }
  invisible(TRUE)
}
