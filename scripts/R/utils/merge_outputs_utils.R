############################################################
# Merge Output Assembly Utilities
# Project: Sewage
############################################################

MERGE_OUTPUT_FILES <- c(
  site_works_crosswalk = "site_works_crosswalk.parquet",
  matched_events = "matched_events_annual_data.parquet",
  events_unmatched = "events_unmatched.parquet",
  annual_unmatched = "annual_unmatched.parquet",
  near_miss_report = "near_miss_report.parquet"
)

MERGE_TUPLE_COLS <- c(
  "site_name_ea",
  "site_name_wa_sc",
  "permit_reference_ea",
  "permit_reference_wa_sc",
  "activity_reference",
  "unique_id"
)

SITE_WORKS_CROSSWALK_PROTOTYPE <- tibble::tibble(
  site_id = integer(),
  year = integer(),
  water_company = character(),
  site_id_members = character(),
  n_outlets = integer(),
  n_outlets_reporting = integer(),
  annual_status = character(),
  spill_hrs_ea = double(),
  spill_count_ea = double(),
  ngr = character(),
  easting = double(),
  northing = double(),
  edm_operation_percent = double(),
  no_full_years_edm_data = double(),
  edm_commission_date = as.Date(character()),
  matched_event_count = integer(),
  match_methods = character()
)

MATCHED_EVENTS_PROTOTYPE <- tibble::tibble(
  water_company = character(),
  year = integer(),
  site_name_ea = character(),
  site_name_wa_sc = character(),
  permit_reference_ea = character(),
  permit_reference_wa_sc = character(),
  activity_reference = character(),
  unique_id = character(),
  start_time = as.POSIXct(character(), tz = "UTC"),
  end_time = as.POSIXct(character(), tz = "UTC"),
  site_id = integer(),
  match_method = character(),
  match_quality = double(),
  annual_status = character(),
  spill_hrs_ea = double(),
  spill_count_ea = double(),
  ngr = character()
)

EVENTS_UNMATCHED_PROTOTYPE <- tibble::tibble(
  water_company = character(),
  year = integer(),
  site_name_ea = character(),
  site_name_wa_sc = character(),
  permit_reference_ea = character(),
  permit_reference_wa_sc = character(),
  activity_reference = character(),
  unique_id = character(),
  start_time = as.POSIXct(character(), tz = "UTC"),
  end_time = as.POSIXct(character(), tz = "UTC"),
  reason = character()
)

ANNUAL_UNMATCHED_PROTOTYPE <- SITE_WORKS_CROSSWALK_PROTOTYPE

NEAR_MISS_REPORT_PROTOTYPE <- tibble::tibble(
  report_type = character(),
  water_company = character(),
  year = integer(),
  site_id = integer(),
  candidate_site_id = integer(),
  site_name_ea = character(),
  reason = character(),
  distance_m = double(),
  match_quality = double()
)

conform_merge_output <- function(tbl, prototype) {
  if (is.null(tbl) || nrow(tbl) == 0) {
    return(prototype)
  }

  missing_cols <- setdiff(names(prototype), names(tbl))
  for (col in missing_cols) {
    tbl[[col]] <- prototype[[col]][NA_integer_][1]
  }

  tbl %>%
    dplyr::select(dplyr::all_of(names(prototype))) %>%
    dplyr::mutate(
      dplyr::across(
        names(prototype),
        ~ vctrs::vec_cast(.x, prototype[[dplyr::cur_column()]])
      )
    )
}

empty_merge_outputs <- function() {
  list(
    site_works_crosswalk = SITE_WORKS_CROSSWALK_PROTOTYPE,
    matched_events = MATCHED_EVENTS_PROTOTYPE,
    events_unmatched = EVENTS_UNMATCHED_PROTOTYPE,
    annual_unmatched = ANNUAL_UNMATCHED_PROTOTYPE,
    near_miss_report = NEAR_MISS_REPORT_PROTOTYPE
  )
}

annual_status_from_metrics <- function(has_return, spill_hrs, spill_count) {
  dplyr::case_when(
    !has_return ~ "absent",
    is.na(spill_hrs) & is.na(spill_count) ~ "reported_na",
    dplyr::coalesce(spill_hrs, 0) == 0 & dplyr::coalesce(spill_count, 0) == 0 ~
      "reported_zero",
    TRUE ~ "reported_positive"
  )
}

most_recent_representative_location <- function(resolver, site_id, year) {
  candidates <- resolver %>%
    dplyr::filter(
      .data$site_id == !!site_id,
      .data$member_site_id == !!site_id,
      !is.na(.data$ngr)
    ) %>%
    dplyr::mutate(before_or_same_year = .data$year <= !!year) %>%
    dplyr::arrange(
      dplyr::desc(.data$before_or_same_year),
      dplyr::desc(.data$year)
    )

  if (nrow(candidates) == 0) {
    return(tibble::tibble(ngr = NA_character_, easting = NA_real_, northing = NA_real_))
  }

  candidates[1, c("ngr", "easting", "northing")]
}

build_site_works_crosswalk <- function(membership, resolver, decisions, years) {
  if (nrow(membership) == 0) {
    return(SITE_WORKS_CROSSWALK_PROTOTYPE)
  }

  if (!"ngr" %in% names(resolver) && "ngr_clean" %in% names(resolver)) {
    resolver$ngr <- resolver$ngr_clean
  }
  for (col in c("edm_operation_percent", "no_full_years_edm_data")) {
    if (!col %in% names(resolver)) resolver[[col]] <- NA_real_
  }
  if (!"edm_commission_date" %in% names(resolver)) {
    resolver$edm_commission_date <- as.Date(NA)
  }

  works <- membership %>%
    dplyr::group_by(.data$site_id, .data$water_company) %>%
    dplyr::summarise(
      site_id_members = paste(sort(.data$member_site_id), collapse = ";"),
      n_outlets = dplyr::n_distinct(.data$member_site_id),
      .groups = "drop"
    )

  annual_summary <- resolver %>%
    dplyr::group_by(.data$site_id, .data$year) %>%
    dplyr::summarise(
      has_return = TRUE,
      n_outlets_reporting = dplyr::n_distinct(.data$member_site_id),
      spill_hrs_ea = if (all(is.na(.data$spill_hrs_ea))) {
        NA_real_
      } else {
        sum(.data$spill_hrs_ea, na.rm = TRUE)
      },
      spill_count_ea = if (all(is.na(.data$spill_count_ea))) {
        NA_real_
      } else {
        sum(.data$spill_count_ea, na.rm = TRUE)
      },
      edm_operation_percent = if (all(is.na(.data$edm_operation_percent))) {
        NA_real_
      } else {
        mean(.data$edm_operation_percent, na.rm = TRUE)
      },
      no_full_years_edm_data = if (all(is.na(.data$no_full_years_edm_data))) {
        NA_real_
      } else {
        max(.data$no_full_years_edm_data, na.rm = TRUE)
      },
      edm_commission_date = suppressWarnings(min(.data$edm_commission_date, na.rm = TRUE)),
      .groups = "drop"
    )

  decision_summary <- decisions %>%
    dplyr::filter(!is.na(.data$site_id)) %>%
    dplyr::group_by(.data$site_id, .data$year) %>%
    dplyr::summarise(
      matched_event_count = dplyr::n(),
      match_methods = paste(sort(unique(.data$match_method)), collapse = ";"),
      .groups = "drop"
    )

  grid <- tidyr::expand_grid(site_id = works$site_id, year = as.integer(years)) %>%
    dplyr::left_join(works, by = "site_id") %>%
    dplyr::left_join(annual_summary, by = c("site_id", "year")) %>%
    dplyr::mutate(
      has_return = dplyr::coalesce(.data$has_return, FALSE),
      n_outlets_reporting = dplyr::coalesce(.data$n_outlets_reporting, 0L),
      annual_status = annual_status_from_metrics(
        .data$has_return,
        .data$spill_hrs_ea,
        .data$spill_count_ea
      ),
      spill_hrs_ea = dplyr::if_else(.data$annual_status == "absent", NA_real_, .data$spill_hrs_ea),
      spill_count_ea = dplyr::if_else(.data$annual_status == "absent", NA_real_, .data$spill_count_ea)
    ) %>%
    dplyr::select(-"has_return")

  locations <- purrr::map2_dfr(
    grid$site_id,
    grid$year,
    ~ most_recent_representative_location(resolver, .x, .y)
  )

  grid %>%
    dplyr::bind_cols(locations) %>%
    dplyr::left_join(decision_summary, by = c("site_id", "year")) %>%
    dplyr::mutate(
      matched_event_count = dplyr::coalesce(.data$matched_event_count, 0L),
      match_methods = dplyr::coalesce(.data$match_methods, NA_character_)
    ) %>%
    dplyr::arrange(.data$site_id, .data$year) %>%
    conform_merge_output(SITE_WORKS_CROSSWALK_PROTOTYPE)
}

join_events_to_decisions <- function(events, decisions) {
  key_cols <- c("water_company", "year", MERGE_TUPLE_COLS)
  events %>%
    dplyr::left_join(decisions, by = key_cols, suffix = c("", "_decision"))
}

assemble_matched_events <- function(events, decisions, crosswalk) {
  join_events_to_decisions(events, decisions) %>%
    dplyr::filter(!is.na(.data$site_id)) %>%
    dplyr::left_join(
      crosswalk %>%
        dplyr::select(
          site_id,
          year,
          annual_status,
          spill_hrs_ea,
          spill_count_ea,
          ngr
        ),
      by = c("site_id", "year")
    ) %>%
    conform_merge_output(MATCHED_EVENTS_PROTOTYPE)
}

assemble_events_unmatched <- function(events, decisions) {
  join_events_to_decisions(events, decisions) %>%
    dplyr::filter(is.na(.data$site_id)) %>%
    conform_merge_output(EVENTS_UNMATCHED_PROTOTYPE)
}

assemble_annual_unmatched <- function(crosswalk, matched_events) {
  matched_site_years <- matched_events %>%
    dplyr::distinct(.data$site_id, .data$year)

  crosswalk %>%
    dplyr::filter(.data$annual_status == "reported_positive") %>%
    dplyr::anti_join(matched_site_years, by = c("site_id", "year")) %>%
    conform_merge_output(ANNUAL_UNMATCHED_PROTOTYPE)
}

assemble_near_miss_report <- function(register_near_misses = tibble::tibble(),
                                      agreement_near_misses = tibble::tibble()) {
  register_report <- register_near_misses
  if (nrow(register_report) > 0) {
    register_report <- register_report %>%
      dplyr::transmute(
        report_type = "register_distance",
        water_company,
        year = NA_integer_,
        site_id = as.integer(.data$site_id_from),
        candidate_site_id = as.integer(.data$site_id_to),
        site_name_ea = .data$site_name_ea_norm,
        reason,
        distance_m,
        match_quality = NA_real_
      )
  }

  agreement_report <- agreement_near_misses
  if (nrow(agreement_report) > 0) {
    for (col in names(NEAR_MISS_REPORT_PROTOTYPE)) {
      if (!col %in% names(agreement_report)) {
        agreement_report[[col]] <- NEAR_MISS_REPORT_PROTOTYPE[[col]][NA_integer_][1]
      }
    }
  }

  dplyr::bind_rows(register_report, agreement_report) %>%
    conform_merge_output(NEAR_MISS_REPORT_PROTOTYPE)
}

assemble_merge_outputs <- function(events, membership, resolver, decisions,
                                   register_near_misses = tibble::tibble(),
                                   agreement_near_misses = tibble::tibble(),
                                   years = sort(unique(events$year))) {
  crosswalk <- build_site_works_crosswalk(membership, resolver, decisions, years)
  matched_events <- assemble_matched_events(events, decisions, crosswalk)
  events_unmatched <- assemble_events_unmatched(events, decisions)

  if (nrow(events) != nrow(matched_events) + nrow(events_unmatched)) {
    stop("Row-accounting failure: events != matched + unmatched.", call. = FALSE)
  }

  expected_crosswalk_rows <- dplyr::n_distinct(membership$site_id) * length(years)
  if (nrow(crosswalk) != expected_crosswalk_rows) {
    stop("Row-accounting failure: crosswalk is not full works x years.", call. = FALSE)
  }

  list(
    site_works_crosswalk = crosswalk,
    matched_events = matched_events,
    events_unmatched = events_unmatched,
    annual_unmatched = assemble_annual_unmatched(crosswalk, matched_events),
    near_miss_report = assemble_near_miss_report(
      register_near_misses,
      agreement_near_misses
    )
  )
}

validate_publishable_merge_outputs <- function(outputs) {
  required <- names(MERGE_OUTPUT_FILES)
  missing_outputs <- setdiff(required, names(outputs))
  if (length(missing_outputs) > 0) {
    stop(
      "Missing merge output object(s): ",
      paste(missing_outputs, collapse = ", "),
      call. = FALSE
    )
  }

  prototypes <- empty_merge_outputs()
  for (name in required) {
    output <- outputs[[name]]
    if (is.null(output)) {
      stop("Cannot publish NULL output: ", name, call. = FALSE)
    }
    missing_cols <- setdiff(names(prototypes[[name]]), names(output))
    if (length(missing_cols) > 0) {
      stop(
        "Cannot publish output ", name, ": missing column(s) ",
        paste(missing_cols, collapse = ", "),
        call. = FALSE
      )
    }
  }

  if (nrow(outputs$site_works_crosswalk) == 0 || nrow(outputs$matched_events) == 0) {
    stop("Cannot publish empty crosswalk or matched-events output.", call. = FALSE)
  }

  invisible(TRUE)
}

write_merge_outputs <- function(outputs, output_dir) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  for (name in names(MERGE_OUTPUT_FILES)) {
    arrow::write_parquet(outputs[[name]], file.path(output_dir, MERGE_OUTPUT_FILES[[name]]))
  }
  invisible(output_dir)
}

complete_merge_output_set <- function(path) {
  dir.exists(path) &&
    all(file.exists(file.path(path, unname(MERGE_OUTPUT_FILES))))
}

promote_staged_merge_outputs <- function(staging_dir, canonical_dir,
                                         crash_at = Sys.getenv("MERGE_OUTPUTS_CRASH_AT", "")) {
  previous_dir <- paste0(canonical_dir, ".prev")
  if (dir.exists(previous_dir)) {
    unlink(previous_dir, recursive = TRUE, force = TRUE)
  }

  if (dir.exists(canonical_dir)) {
    ok <- file.rename(canonical_dir, previous_dir)
    if (!isTRUE(ok)) stop("Failed to move canonical output to .prev.", call. = FALSE)
  }

  if (identical(crash_at, "after_backup")) {
    stop("Intentional crash after canonical -> .prev promotion step.", call. = FALSE)
  }

  ok <- file.rename(staging_dir, canonical_dir)
  if (!isTRUE(ok)) {
    stop("Failed to move staging output to canonical path.", call. = FALSE)
  }

  invisible(canonical_dir)
}

publish_merge_outputs <- function(outputs, canonical_dir,
                                  staging_dir = paste0(canonical_dir, ".staging")) {
  validate_publishable_merge_outputs(outputs)
  if (dir.exists(staging_dir)) {
    unlink(staging_dir, recursive = TRUE, force = TRUE)
  }
  write_merge_outputs(outputs, staging_dir)
  promote_staged_merge_outputs(staging_dir, canonical_dir)
  invisible(canonical_dir)
}
