############################################################
# Merge Matching Utilities
# Project: Sewage
############################################################

MERGE_MATCHING_TUPLE_COLS <- c(
  "site_name_ea",
  "site_name_wa_sc",
  "permit_reference_ea",
  "permit_reference_wa_sc",
  "activity_reference",
  "unique_id"
)

MERGE_MATCHING_DECISION_PROTOTYPE <- tibble::tibble(
  water_company = character(),
  year = integer(),
  site_name_ea = character(),
  site_name_wa_sc = character(),
  permit_reference_ea = character(),
  permit_reference_wa_sc = character(),
  activity_reference = character(),
  unique_id = character(),
  site_id = integer(),
  match_method = character(),
  match_quality = double(),
  reason = character(),
  annual_status_hint = character()
)

normalise_match_value <- function(x) {
  x <- as.character(x)
  x <- stringr::str_squish(stringr::str_to_upper(stringr::str_trim(x)))
  x[x %in% c("", "TBC", "N/A", "NA")] <- NA_character_
  x
}

add_normalised_tuple_cols <- function(data) {
  data %>%
    dplyr::mutate(
      site_name_ea_norm = normalise_match_value(.data$site_name_ea),
      site_name_wa_sc_norm = normalise_match_value(.data$site_name_wa_sc),
      permit_reference_ea_norm = normalise_match_value(.data$permit_reference_ea),
      permit_reference_wa_sc_norm =
        normalise_match_value(.data$permit_reference_wa_sc),
      activity_reference_norm = normalise_match_value(.data$activity_reference),
      unique_id_norm = normalise_match_value(.data$unique_id)
    )
}

prepare_event_tuples <- function(events) {
  missing_cols <- setdiff(c("water_company", "year", MERGE_MATCHING_TUPLE_COLS), names(events))
  if (length(missing_cols) > 0) {
    stop(
      "Event data missing required tuple column(s): ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  for (col in MERGE_MATCHING_TUPLE_COLS) {
    events[[col]] <- as.character(events[[col]])
  }

  events <- events %>%
    dplyr::mutate(year = as.integer(.data$year)) %>%
    add_normalised_tuple_cols()

  group_cols <- c(
    "water_company", "year", MERGE_MATCHING_TUPLE_COLS,
    paste0(MERGE_MATCHING_TUPLE_COLS, "_norm")
  )
  group_cols <- intersect(group_cols, names(events))

  grouped <- events %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols)))

  if (all(c("start_time", "end_time") %in% names(events))) {
    out <- grouped %>%
      dplyr::summarise(
        event_count = dplyr::n(),
        event_hours = calculate_spill_hours(.data$start_time, .data$end_time),
        .groups = "drop"
      )
  } else if ("event_hours" %in% names(events)) {
    out <- grouped %>%
      dplyr::summarise(
        event_count = dplyr::n(),
        event_hours = dplyr::first(.data$event_hours),
        .groups = "drop"
      )
  } else {
    out <- grouped %>%
      dplyr::summarise(
        event_count = dplyr::n(),
        event_hours = 0,
        .groups = "drop"
      )
  }

  out %>%
    dplyr::arrange(.data$year, .data$water_company)
}

prepare_resolver_for_matching <- function(resolver) {
  required <- c("site_id", "water_company", "year")
  missing_cols <- setdiff(required, names(resolver))
  if (length(missing_cols) > 0) {
    stop(
      "Resolver missing required column(s): ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  for (col in MERGE_MATCHING_TUPLE_COLS) {
    if (!col %in% names(resolver)) {
      resolver[[col]] <- NA_character_
    }
  }
  for (col in c("spill_hrs_ea", "spill_count_ea")) {
    if (!col %in% names(resolver)) {
      resolver[[col]] <- NA_real_
    }
  }

  resolver %>%
    dplyr::mutate(
      site_id = as.integer(.data$site_id),
      year = as.integer(.data$year)
    ) %>%
    add_normalised_tuple_cols()
}

unique_work_resolution <- function(candidates) {
  works <- sort(unique(stats::na.omit(candidates$site_id)))
  if (length(works) == 0) {
    return(list(status = "none", site_id = NA_integer_, n_works = 0L))
  }
  if (length(works) == 1) {
    return(list(status = "unique", site_id = as.integer(works[[1L]]), n_works = 1L))
  }
  list(status = "spans", site_id = NA_integer_, n_works = length(works))
}

candidate_rows_for_rung <- function(tuple, resolver, rung) {
  same_company <- resolver %>%
    dplyr::filter(.data$water_company == tuple$water_company)

  if (identical(rung, "unique_id")) {
    if (is.na(tuple$unique_id_norm)) return(same_company[0, ])
    return(same_company %>%
      dplyr::filter(
        .data$year == tuple$year,
        .data$unique_id_norm == tuple$unique_id_norm
      ))
  }

  if (identical(rung, "permit_activity")) {
    if (is.na(tuple$permit_reference_ea_norm) ||
        is.na(tuple$activity_reference_norm)) {
      return(same_company[0, ])
    }
    return(same_company %>%
      dplyr::filter(
        .data$permit_reference_ea_norm == tuple$permit_reference_ea_norm,
        .data$activity_reference_norm == tuple$activity_reference_norm
      ))
  }

  if (identical(rung, "permit_only")) {
    if (is.na(tuple$permit_reference_ea_norm)) return(same_company[0, ])
    return(same_company %>%
      dplyr::filter(.data$permit_reference_ea_norm == tuple$permit_reference_ea_norm))
  }

  if (identical(rung, "site_name_ea")) {
    if (is.na(tuple$site_name_ea_norm)) return(same_company[0, ])
    return(same_company %>%
      dplyr::filter(.data$site_name_ea_norm == tuple$site_name_ea_norm))
  }

  if (identical(rung, "site_name_wa_sc")) {
    if (is.na(tuple$site_name_wa_sc_norm)) return(same_company[0, ])
    return(same_company %>%
      dplyr::filter(.data$site_name_wa_sc_norm == tuple$site_name_wa_sc_norm))
  }

  stop("Unknown matching rung: ", rung, call. = FALSE)
}

resolve_ladder_for_tuple <- function(tuple, resolver) {
  rungs <- c(
    "unique_id", "permit_activity", "permit_only",
    "site_name_ea", "site_name_wa_sc"
  )
  rows_by_rung <- list()
  resolutions <- list()

  rows_by_rung$unique_id <- candidate_rows_for_rung(tuple, resolver, "unique_id")
  resolutions$unique_id <- unique_work_resolution(rows_by_rung$unique_id)

  rows_by_rung$permit_activity <-
    candidate_rows_for_rung(tuple, resolver, "permit_activity")
  resolutions$permit_activity <-
    unique_work_resolution(rows_by_rung$permit_activity)

  if (identical(resolutions$permit_activity$status, "none")) {
    rows_by_rung$permit_only <-
      candidate_rows_for_rung(tuple, resolver, "permit_only")
    resolutions$permit_only <- unique_work_resolution(rows_by_rung$permit_only)
  } else {
    rows_by_rung$permit_only <- resolver[0, ]
    resolutions$permit_only <- list(
      status = "skipped",
      site_id = NA_integer_,
      n_works = 0L
    )
  }

  rows_by_rung$site_name_ea <-
    candidate_rows_for_rung(tuple, resolver, "site_name_ea")
  resolutions$site_name_ea <- unique_work_resolution(rows_by_rung$site_name_ea)

  rows_by_rung$site_name_wa_sc <-
    candidate_rows_for_rung(tuple, resolver, "site_name_wa_sc")
  resolutions$site_name_wa_sc <-
    unique_work_resolution(rows_by_rung$site_name_wa_sc)

  unique_rungs <- rungs[vapply(
    resolutions[rungs],
    function(x) identical(x$status, "unique"),
    logical(1)
  )]

  unique_site_ids <- unique(vapply(
    unique_rungs,
    function(rung) resolutions[[rung]]$site_id,
    integer(1)
  ))

  if (length(unique_site_ids) > 1L) {
    return(list(
      status = "unmatched",
      site_id = NA_integer_,
      match_method = NA_character_,
      match_quality = NA_real_,
      reason = "key_conflict",
      rows_by_rung = rows_by_rung,
      resolutions = resolutions
    ))
  }

  if (length(unique_rungs) > 0L) {
    selected_rung <- unique_rungs[[1L]]
    return(list(
      status = "matched",
      site_id = resolutions[[selected_rung]]$site_id,
      match_method = selected_rung,
      match_quality = 1,
      reason = NA_character_,
      rows_by_rung = rows_by_rung,
      resolutions = resolutions
    ))
  }

  for (name_rung in c("site_name_ea", "site_name_wa_sc")) {
    if (identical(resolutions[[name_rung]]$status, "spans")) {
      return(list(
        status = "needs_agreement",
        site_id = NA_integer_,
        match_method = NA_character_,
        match_quality = NA_real_,
        reason = "name_spans_works",
        agreement_rung = name_rung,
        agreement_candidates = rows_by_rung[[name_rung]],
        rows_by_rung = rows_by_rung,
        resolutions = resolutions
      ))
    }
  }

  list(
    status = "unmatched",
    site_id = NA_integer_,
    match_method = NA_character_,
    match_quality = NA_real_,
    reason = "no_usable_key",
    rows_by_rung = rows_by_rung,
    resolutions = resolutions
  )
}

calculate_relative_error <- function(event_hours, candidate_hours, abs_floor) {
  denominator <- pmax(abs(event_hours), abs(candidate_hours), abs_floor)
  abs(candidate_hours - event_hours) / denominator
}

resolve_agreement_for_tuple <- function(tuple, candidates, config) {
  same_year <- candidates %>%
    dplyr::filter(.data$year == tuple$year) %>%
    dplyr::group_by(.data$site_id) %>%
    dplyr::summarise(
      candidate_hours = if (all(is.na(.data$spill_hrs_ea))) {
        NA_real_
      } else {
        sum(.data$spill_hrs_ea, na.rm = TRUE)
      },
      .groups = "drop"
    ) %>%
    dplyr::filter(!is.na(.data$candidate_hours))

  if (nrow(same_year) == 0) {
    return(list(status = "unmatched", reason = "name_spans_works"))
  }

  if (abs(tuple$event_hours) <= config$agreement_abs_floor_hrs &&
      all(abs(same_year$candidate_hours) <= config$agreement_abs_floor_hrs)) {
    return(list(status = "unmatched", reason = "agreement_uninformative"))
  }

  scored <- same_year %>%
    dplyr::mutate(
      relative_error = calculate_relative_error(
        tuple$event_hours,
        .data$candidate_hours,
        config$agreement_abs_floor_hrs
      )
    ) %>%
    dplyr::arrange(.data$relative_error, .data$site_id)

  if (nrow(scored) == 0 || is.na(scored$relative_error[[1L]])) {
    return(list(status = "unmatched", reason = "agreement_failed"))
  }

  best <- scored[1L, ]
  runner_up_error <- if (nrow(scored) >= 2L) {
    scored$relative_error[[2L]]
  } else {
    Inf
  }

  if (best$relative_error <= config$agreement_rel_error &&
      runner_up_error >= config$agreement_runner_up_ratio * best$relative_error) {
    return(list(
      status = "matched",
      site_id = as.integer(best$site_id),
      match_method = "agreement",
      match_quality = as.numeric(best$relative_error),
      reason = NA_character_
    ))
  }

  list(status = "unmatched", reason = "agreement_failed")
}

same_year_return_present <- function(site_id, year, resolver) {
  any(resolver$site_id == site_id & resolver$year == year)
}

decision_from_resolution <- function(tuple, resolution, resolver) {
  annual_status_hint <- NA_character_
  if (identical(resolution$status, "matched") &&
      !same_year_return_present(resolution$site_id, tuple$year, resolver)) {
    annual_status_hint <- "absent"
  }

  tibble::tibble(
    water_company = tuple$water_company,
    year = as.integer(tuple$year),
    site_name_ea = tuple$site_name_ea,
    site_name_wa_sc = tuple$site_name_wa_sc,
    permit_reference_ea = tuple$permit_reference_ea,
    permit_reference_wa_sc = tuple$permit_reference_wa_sc,
    activity_reference = tuple$activity_reference,
    unique_id = tuple$unique_id,
    site_id = as.integer(resolution$site_id),
    match_method = resolution$match_method,
    match_quality = as.numeric(resolution$match_quality),
    reason = resolution$reason,
    annual_status_hint = annual_status_hint
  )
}

validate_manual_overrides <- function(manual_overrides, resolver) {
  if (is.null(manual_overrides) || nrow(manual_overrides) == 0) {
    return(tibble::tibble())
  }

  unknown <- setdiff(as.integer(manual_overrides$site_id), unique(resolver$site_id))
  if (length(unknown) > 0) {
    stop(
      "Manual overrides reference unknown works site_id(s): ",
      paste(sort(unknown), collapse = ", "),
      call. = FALSE
    )
  }

  manual_overrides %>%
    dplyr::mutate(year = as.integer(.data$year)) %>%
    add_normalised_tuple_cols()
}

apply_manual_overrides <- function(decisions, tuples, manual_overrides, resolver) {
  overrides <- validate_manual_overrides(manual_overrides, resolver)
  if (nrow(overrides) == 0) {
    return(decisions)
  }

  key_cols <- c("water_company", "year", MERGE_MATCHING_TUPLE_COLS)
  override_matches <- decisions %>%
    dplyr::filter(is.na(.data$site_id)) %>%
    dplyr::select(dplyr::all_of(key_cols)) %>%
    dplyr::left_join(overrides, by = key_cols, suffix = c("", "_override")) %>%
    dplyr::filter(!is.na(.data$site_id))

  if (nrow(override_matches) == 0) {
    return(decisions)
  }

  decisions %>%
    dplyr::rows_update(
      override_matches %>%
        dplyr::transmute(
          water_company,
          year,
          site_name_ea,
          site_name_wa_sc,
          permit_reference_ea,
          permit_reference_wa_sc,
          activity_reference,
          unique_id,
          site_id = as.integer(.data$site_id),
          match_method = "manual_override",
          match_quality = 1,
          reason = NA_character_,
          annual_status_hint = dplyr::if_else(
            purrr::map2_lgl(.data$site_id, .data$year, same_year_return_present, resolver = resolver),
            NA_character_,
            "absent"
          )
        ),
      by = key_cols
    )
}

resolve_merge_matches <- function(
    events,
    resolver,
    manual_overrides = tibble::tibble(),
    config = list(
      agreement_rel_error = 0.25,
      agreement_runner_up_ratio = 2,
      agreement_abs_floor_hrs = 1
    )) {
  tuples <- prepare_event_tuples(events)
  resolver <- prepare_resolver_for_matching(resolver)

  decisions <- purrr::map_dfr(seq_len(nrow(tuples)), function(i) {
    tuple <- tuples[i, ]
    ladder <- resolve_ladder_for_tuple(tuple, resolver)
    resolution <- ladder

    if (identical(ladder$status, "needs_agreement")) {
      agreement <- resolve_agreement_for_tuple(
        tuple,
        ladder$agreement_candidates,
        config
      )
      resolution <- utils::modifyList(ladder, agreement)
    }

    decision_from_resolution(tuple, resolution, resolver)
  })

  decisions <- apply_manual_overrides(decisions, tuples, manual_overrides, resolver)

  if (nrow(decisions) != nrow(tuples)) {
    stop("Row-accounting failure: decisions do not match tuple count.", call. = FALSE)
  }

  decisions %>%
    dplyr::arrange(.data$year, .data$water_company, .data$site_name_ea, .data$unique_id) %>%
    dplyr::select(dplyr::all_of(names(MERGE_MATCHING_DECISION_PROTOTYPE)))
}
