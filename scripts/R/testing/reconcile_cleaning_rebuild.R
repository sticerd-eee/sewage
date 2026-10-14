# ==============================================================================
# Cleaning Rebuild Reconciliation
# ==============================================================================
#
# Purpose: Produce non-mutating gate evidence for cleaning rebuild candidates.
#          Covers Land Registry and Zoopla old-versus-new allowed deltas,
#          paired-candidate metadata, and source/candidate checksums.
#
# This script never promotes, replaces, archives, or deletes canonical data.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}

source(here::here("scripts", "R", "utils", "hash_utils.R"), local = TRUE)

LR_STABLE_EXCLUDED_COLUMNS <- c(
  "house_id",
  "transaction_id",
  "date_of_transfer",
  "qtr_id",
  "month_id"
)
ZOOPLA_STABLE_EXCLUDED_COLUMNS <- c("rental_id")

empty_delta_table <- function() {
  tibble::tibble(
    house_id = character(),
    transaction_id = character(),
    column = character(),
    old_value = character(),
    new_value = character()
  )
}

values_match <- function(old, new) {
  both_missing <- is.na(old) & is.na(new)
  both_present <- !is.na(old) & !is.na(new)
  both_missing | (both_present & as.character(old) == as.character(new))
}

assert_lr_reconciliation_schema <- function(old, candidate_long, candidate_study) {
  if (!("transaction_id" %in% names(old))) {
    stop("Historical LR data must retain transaction_id for reconciliation.", call. = FALSE)
  }
  if (!("house_id" %in% names(candidate_long)) ||
      !("house_id" %in% names(candidate_study))) {
    stop("Both LR candidates must contain house_id.", call. = FALSE)
  }
  if ("transaction_id" %in% names(candidate_long) ||
      "transaction_id" %in% names(candidate_study)) {
    stop("LR candidate schemas must not expose transaction_id.", call. = FALSE)
  }
  if (anyNA(candidate_long$house_id) || anyDuplicated(candidate_long$house_id)) {
    stop("The LR long-run candidate must have unique, non-missing house_id values.", call. = FALSE)
  }
  if (anyNA(candidate_study$house_id) || anyDuplicated(candidate_study$house_id)) {
    stop("The LR study candidate must have unique, non-missing house_id values.", call. = FALSE)
  }
  invisible(TRUE)
}

candidate_period_issues <- function(candidate_long, base_year = 2021L) {
  expected_qtr <- (lubridate::year(candidate_long$date_of_transfer) - base_year) * 4L +
    lubridate::quarter(candidate_long$date_of_transfer)
  expected_month <- (lubridate::year(candidate_long$date_of_transfer) - base_year) * 12L +
    lubridate::month(candidate_long$date_of_transfer)

  bad <- is.na(candidate_long$date_of_transfer) |
    is.na(candidate_long$qtr_id) |
    is.na(candidate_long$month_id) |
    candidate_long$qtr_id != expected_qtr |
    candidate_long$month_id != expected_month

  tibble::tibble(
    house_id = candidate_long$house_id[bad],
    date_of_transfer = candidate_long$date_of_transfer[bad],
    qtr_id = candidate_long$qtr_id[bad],
    expected_qtr_id = expected_qtr[bad],
    month_id = candidate_long$month_id[bad],
    expected_month_id = expected_month[bad]
  )
}

candidate_pair_issues <- function(candidate_long, candidate_study, study_years) {
  expected <- candidate_long[
    lubridate::year(candidate_long$date_of_transfer) %in% study_years,
    ,
    drop = FALSE
  ]
  expected <- expected[order(expected$house_id), , drop = FALSE]
  observed <- candidate_study[order(candidate_study$house_id), , drop = FALSE]

  same_schema <- identical(names(expected), names(observed))
  same_rows <- same_schema && isTRUE(all.equal(
    as.data.frame(expected),
    as.data.frame(observed),
    check.attributes = FALSE
  ))

  tibble::tibble(
    check = c("study_schema_equals_filtered_long_run", "study_rows_equal_filtered_long_run"),
    passed = c(same_schema, same_rows),
    detail = c(
      paste0("expected columns=", ncol(expected), "; observed columns=", ncol(observed)),
      paste0("expected rows=", nrow(expected), "; observed rows=", nrow(observed))
    )
  ) |>
    dplyr::filter(!.data$passed)
}

collect_stable_value_deltas <- function(old_keyed, candidate_long) {
  stable_columns <- setdiff(
    intersect(names(old_keyed), names(candidate_long)),
    c(LR_STABLE_EXCLUDED_COLUMNS, ".old_row", ".stable_house_id")
  )
  if (length(stable_columns) == 0L) return(empty_delta_table())

  old_projection <- old_keyed |>
    dplyr::select(
      ".old_row",
      ".stable_house_id",
      "transaction_id",
      dplyr::all_of(stable_columns)
    )
  new_projection <- candidate_long |>
    dplyr::rename(.stable_house_id = "house_id") |>
    dplyr::select(".stable_house_id", dplyr::all_of(stable_columns))
  compared <- dplyr::left_join(
    old_projection,
    new_projection,
    by = ".stable_house_id",
    suffix = c("_old", "_new")
  )

  deltas <- lapply(stable_columns, function(column) {
    old_value <- compared[[paste0(column, "_old")]]
    new_value <- compared[[paste0(column, "_new")]]
    changed <- !values_match(old_value, new_value)
    changed[is.na(changed)] <- TRUE

    tibble::tibble(
      house_id = compared$.stable_house_id[changed],
      transaction_id = compared$transaction_id[changed],
      column = column,
      old_value = as.character(old_value[changed]),
      new_value = as.character(new_value[changed])
    )
  })

  dplyr::bind_rows(deltas)
}

#' Compare the historical LR study file with rebuilt long-run/study candidates
#'
#' Allowed changes are the positional-to-hash ID flip, transaction_id removal,
#' refreshed transfer dates and their period indices, and study membership
#' implied by those refreshed dates. Other common-column changes are reported.
reconcile_lr_allowed_deltas <- function(
    old,
    candidate_long,
    candidate_study,
    study_years = 2021:2024,
    base_year = 2021L) {
  old <- tibble::as_tibble(old)
  candidate_long <- tibble::as_tibble(candidate_long)
  candidate_study <- tibble::as_tibble(candidate_study)
  assert_lr_reconciliation_schema(old, candidate_long, candidate_study)

  old_keyed <- old |>
    dplyr::mutate(
      .old_row = dplyr::row_number(),
      .stable_house_id = hash_transaction_id(.data$transaction_id)
    )

  old_duplicate_ids <- old_keyed |>
    dplyr::count(.stable_house_id, name = "old_row_count") |>
    dplyr::filter(.data$old_row_count > 1L) |>
    dplyr::rename(house_id = ".stable_house_id")

  new_projection <- candidate_long |>
    dplyr::transmute(
      .stable_house_id = .data$house_id,
      new_date_of_transfer = .data$date_of_transfer,
      new_qtr_id = .data$qtr_id,
      new_month_id = .data$month_id,
      in_candidate_long = TRUE
    )
  study_ids <- candidate_study$house_id

  transitions <- old_keyed |>
    dplyr::transmute(
      .data$.old_row,
      transaction_id = as.character(.data$transaction_id),
      house_id = .data$.stable_house_id,
      old_date_of_transfer = .data$date_of_transfer,
      old_qtr_id = .data$qtr_id,
      old_month_id = .data$month_id,
      old_in_study_window = lubridate::year(.data$date_of_transfer) %in% study_years
    ) |>
    dplyr::left_join(
      new_projection,
      by = c("house_id" = ".stable_house_id")
    ) |>
    dplyr::mutate(
      in_candidate_long = tidyr::replace_na(.data$in_candidate_long, FALSE),
      new_in_study_window = lubridate::year(.data$new_date_of_transfer) %in% study_years,
      in_candidate_study = .data$house_id %in% study_ids,
      date_changed = .data$in_candidate_long &
        !values_match(.data$old_date_of_transfer, .data$new_date_of_transfer),
      qtr_changed = .data$in_candidate_long &
        !values_match(.data$old_qtr_id, .data$new_qtr_id),
      month_changed = .data$in_candidate_long &
        !values_match(.data$old_month_id, .data$new_month_id),
      membership_changed = .data$old_in_study_window != .data$in_candidate_study
    )

  unexpected_membership_deltas <- dplyr::bind_rows(
    transitions |>
      dplyr::filter(!.data$in_candidate_long) |>
      dplyr::transmute(
        house_id = .data$house_id,
        issue = "historical_id_missing_from_long_run_candidate"
      ),
    transitions |>
      dplyr::filter(
        .data$in_candidate_long,
        .data$new_in_study_window != .data$in_candidate_study
      ) |>
      dplyr::transmute(
        house_id = .data$house_id,
        issue = "study_membership_not_implied_by_candidate_date"
      ),
    tibble::tibble(
      house_id = setdiff(candidate_study$house_id, candidate_long$house_id),
      issue = "study_id_missing_from_long_run_candidate"
    )
  ) |>
    dplyr::distinct()

  duplicate_records <- old_keyed |>
    dplyr::filter(.data$.stable_house_id %in% old_duplicate_ids$house_id) |>
    dplyr::transmute(
      house_id = .data$.stable_house_id,
      transaction_id = as.character(.data$transaction_id),
      old_date_of_transfer = .data$date_of_transfer,
      old_qtr_id = .data$qtr_id,
      old_month_id = .data$month_id
    ) |>
    dplyr::left_join(
      dplyr::select(
        candidate_long,
        "house_id",
        new_date_of_transfer = "date_of_transfer",
        new_qtr_id = "qtr_id",
        new_month_id = "month_id"
      ),
      by = "house_id"
    ) |>
    dplyr::mutate(retained_in_study_candidate = .data$house_id %in% study_ids)

  period_issues <- candidate_period_issues(candidate_long, base_year)
  pair_issues <- candidate_pair_issues(candidate_long, candidate_study, study_years)
  value_deltas <- collect_stable_value_deltas(old_keyed, candidate_long)

  new_subset_ids <- setdiff(candidate_study$house_id, unique(old_keyed$.stable_house_id))
  removed_subset_ids <- setdiff(unique(old_keyed$.stable_house_id), candidate_study$house_id)

  summary <- tibble::tibble(
    metric = c(
      "old_rows", "old_distinct_transaction_ids", "old_duplicate_transaction_ids",
      "candidate_long_rows", "candidate_study_rows", "date_changes",
      "qtr_changes", "month_changes", "study_ids_added", "study_ids_removed",
      "unexpected_value_deltas", "unexpected_membership_deltas",
      "candidate_period_issues", "candidate_pair_issues"
    ),
    value = c(
      nrow(old), dplyr::n_distinct(old_keyed$.stable_house_id), nrow(old_duplicate_ids),
      nrow(candidate_long), nrow(candidate_study), sum(transitions$date_changed, na.rm = TRUE),
      sum(transitions$qtr_changed, na.rm = TRUE),
      sum(transitions$month_changed, na.rm = TRUE),
      length(new_subset_ids), length(removed_subset_ids),
      nrow(value_deltas), nrow(unexpected_membership_deltas),
      nrow(period_issues), nrow(pair_issues)
    )
  )

  list(
    summary = summary,
    transitions = transitions,
    old_duplicate_ids = old_duplicate_ids,
    old_duplicate_records = duplicate_records,
    study_ids_added = tibble::tibble(house_id = new_subset_ids),
    study_ids_removed = tibble::tibble(house_id = removed_subset_ids),
    unexpected_value_deltas = value_deltas,
    unexpected_membership_deltas = unexpected_membership_deltas,
    candidate_period_issues = period_issues,
    candidate_pair_issues = pair_issues
  )
}

zoopla_candidate_period_issues <- function(candidate_long, base_year = 2021L) {
  expected_qtr <- (lubridate::year(candidate_long$rented_est) - base_year) * 4L +
    lubridate::quarter(candidate_long$rented_est)
  expected_month <- (lubridate::year(candidate_long$rented_est) - base_year) * 12L +
    lubridate::month(candidate_long$rented_est)
  bad <- is.na(candidate_long$rented_est) |
    is.na(candidate_long$qtr_id) |
    is.na(candidate_long$month_id) |
    candidate_long$qtr_id != expected_qtr |
    candidate_long$month_id != expected_month

  tibble::tibble(
    rental_id = candidate_long$rental_id[bad],
    rented_est = candidate_long$rented_est[bad],
    qtr_id = candidate_long$qtr_id[bad],
    expected_qtr_id = expected_qtr[bad],
    month_id = candidate_long$month_id[bad],
    expected_month_id = expected_month[bad]
  )
}

zoopla_candidate_pair_issues <- function(candidate_long, candidate_study, study_years) {
  expected <- candidate_long[
    lubridate::year(candidate_long$rented_est) %in% study_years,
    ,
    drop = FALSE
  ]
  expected <- expected[order(expected$rental_id), , drop = FALSE]
  observed <- candidate_study[order(candidate_study$rental_id), , drop = FALSE]
  same_schema <- identical(names(expected), names(observed))
  same_rows <- same_schema && isTRUE(all.equal(
    as.data.frame(expected),
    as.data.frame(observed),
    check.attributes = FALSE
  ))

  tibble::tibble(
    check = c("study_schema_equals_filtered_long_run", "study_rows_equal_filtered_long_run"),
    passed = c(same_schema, same_rows),
    detail = c(
      paste0("expected columns=", ncol(expected), "; observed columns=", ncol(observed)),
      paste0("expected rows=", nrow(expected), "; observed rows=", nrow(observed))
    )
  ) |>
    dplyr::filter(!.data$passed)
}

assert_zoopla_reconciliation_schema <- function(old, candidate_long, candidate_study) {
  required_old <- c(
    "rental_id", "postcode", "address_line_01", "address_line_02",
    "address_line_03", "listing_price", "latest_to_rent", "rented", "rented_est"
  )
  missing_old <- setdiff(required_old, names(old))
  if (length(missing_old) > 0L) {
    stop(
      "Historical Zoopla data is missing required column(s): ",
      paste(missing_old, collapse = ", "),
      call. = FALSE
    )
  }
  for (candidate in list(candidate_long, candidate_study)) {
    if (!("rental_id" %in% names(candidate)) ||
        anyNA(candidate$rental_id) || anyDuplicated(candidate$rental_id)) {
      stop("Each Zoopla candidate must have unique, non-missing rental_id values.", call. = FALSE)
    }
  }
  invisible(TRUE)
}

empty_zoopla_delta_table <- function() {
  tibble::tibble(
    rental_id = character(),
    old_rental_id = character(),
    column = character(),
    old_value = character(),
    new_value = character()
  )
}

collect_zoopla_stable_value_deltas <- function(old_expected, candidate_study) {
  stable_columns <- setdiff(
    intersect(names(old_expected), names(candidate_study)),
    c(ZOOPLA_STABLE_EXCLUDED_COLUMNS, ".stable_rental_id", ".old_rental_id")
  )
  if (length(stable_columns) == 0L) return(empty_zoopla_delta_table())

  old_projection <- old_expected |>
    dplyr::select(
      ".stable_rental_id",
      ".old_rental_id",
      dplyr::all_of(stable_columns)
    )
  new_projection <- candidate_study |>
    dplyr::rename(.stable_rental_id = "rental_id") |>
    dplyr::select(".stable_rental_id", dplyr::all_of(stable_columns))
  compared <- dplyr::inner_join(
    old_projection,
    new_projection,
    by = ".stable_rental_id",
    suffix = c("_old", "_new")
  )

  dplyr::bind_rows(lapply(stable_columns, function(column) {
    old_value <- compared[[paste0(column, "_old")]]
    new_value <- compared[[paste0(column, "_new")]]
    changed <- !values_match(old_value, new_value)
    changed[is.na(changed)] <- TRUE
    tibble::tibble(
      rental_id = compared$.stable_rental_id[changed],
      old_rental_id = compared$.old_rental_id[changed],
      column = column,
      old_value = as.character(old_value[changed]),
      new_value = as.character(new_value[changed])
    )
  }))
}

#' Enforce the rental R6a contract and quantify its two permitted sample shifts
reconcile_zoopla_allowed_deltas <- function(
    old,
    candidate_long,
    candidate_study,
    study_years = 2021:2023,
    long_run_years = 2014:2023,
    base_year = 2021L) {
  old <- tibble::as_tibble(old)
  candidate_long <- tibble::as_tibble(candidate_long)
  candidate_study <- tibble::as_tibble(candidate_study)
  assert_zoopla_reconciliation_schema(old, candidate_long, candidate_study)

  exact_columns <- setdiff(names(old), "rental_id")
  exact_frame <- data.table::as.data.table(old[exact_columns])
  duplicate_rows <- duplicated(exact_frame, by = exact_columns)
  duplicate_members <- duplicate_rows |
    duplicated(exact_frame, by = exact_columns, fromLast = TRUE)
  duplicate_group_count <- if (any(duplicate_members)) {
    data.table::uniqueN(exact_frame[duplicate_members], by = exact_columns)
  } else {
    0L
  }
  old_unique <- old[!duplicate_rows, , drop = FALSE] |>
    dplyr::mutate(
      .old_rental_id = as.character(.data$rental_id),
      .stable_rental_id = hash_rental_identity(dplyr::pick(dplyr::everything()))
    )

  identity_conflicts <- old_unique |>
    dplyr::count(.data$.stable_rental_id, name = "rows") |>
    dplyr::filter(.data$rows > 1L) |>
    dplyr::rename(rental_id = ".stable_rental_id")

  old_unique <- old_unique |>
    dplyr::mutate(
      old_or_in_study = lubridate::year(.data$latest_to_rent) %in% study_years |
        lubridate::year(.data$rented) %in% study_years,
      rented_est_in_study = lubridate::year(.data$rented_est) %in% study_years
    )
  selection_removed <- old_unique |>
    dplyr::filter(.data$old_or_in_study, !.data$rented_est_in_study) |>
    dplyr::transmute(
      rental_id = .data$.stable_rental_id,
      old_rental_id = .data$.old_rental_id,
      latest_to_rent = .data$latest_to_rent,
      rented = .data$rented,
      rented_est = .data$rented_est,
      old_or_in_study = .data$old_or_in_study,
      rented_est_in_study = .data$rented_est_in_study
    )
  expected_old <- old_unique |>
    dplyr::filter(.data$rented_est_in_study)
  expected_ids <- expected_old$.stable_rental_id
  observed_ids <- candidate_study$rental_id

  unexpected_membership_deltas <- dplyr::bind_rows(
    tibble::tibble(
      rental_id = setdiff(expected_ids, observed_ids),
      issue = "rented_est_eligible_historical_row_missing_from_study_candidate"
    ),
    tibble::tibble(
      rental_id = setdiff(observed_ids, expected_ids),
      issue = "unexpected_study_candidate_row"
    )
  ) |>
    dplyr::distinct()

  value_deltas <- collect_zoopla_stable_value_deltas(expected_old, candidate_study)
  period_issues <- zoopla_candidate_period_issues(candidate_long, base_year)
  pair_issues <- zoopla_candidate_pair_issues(candidate_long, candidate_study, study_years)
  long_window_issues <- candidate_long |>
    dplyr::filter(
      is.na(.data$rented_est) |
        !lubridate::year(.data$rented_est) %in% long_run_years
    ) |>
    dplyr::select("rental_id", "rented_est")

  dedupe_summary <- tibble::tibble(
    old_rows = nrow(old),
    deduplicated_rows = nrow(old_unique),
    removed_rows = as.integer(sum(duplicate_rows)),
    duplicate_groups = as.integer(duplicate_group_count)
  )
  summary <- tibble::tibble(
    metric = c(
      "old_rows", "old_rows_after_exact_dedupe", "dedupe_removed_rows",
      "dedupe_groups", "or_to_rented_est_rows_removed", "candidate_long_rows",
      "candidate_study_rows", "identity_conflicts", "unexpected_value_deltas",
      "unexpected_membership_deltas", "candidate_period_issues",
      "candidate_pair_issues", "long_window_issues"
    ),
    value = c(
      nrow(old), nrow(old_unique), sum(duplicate_rows), duplicate_group_count,
      nrow(selection_removed), nrow(candidate_long), nrow(candidate_study),
      nrow(identity_conflicts), nrow(value_deltas),
      nrow(unexpected_membership_deltas), nrow(period_issues),
      nrow(pair_issues), nrow(long_window_issues)
    )
  )

  list(
    summary = summary,
    dedupe_summary = dedupe_summary,
    duplicate_records = old[duplicate_members, , drop = FALSE],
    selection_removed = selection_removed,
    identity_conflicts = identity_conflicts,
    unexpected_value_deltas = value_deltas,
    unexpected_membership_deltas = unexpected_membership_deltas,
    candidate_period_issues = period_issues,
    candidate_pair_issues = pair_issues,
    long_window_issues = long_window_issues
  )
}

#' Report immutable file facts for archived/current vintages and candidates
build_file_vintage_report <- function(paths, vintage) {
  if (length(paths) == 0L || any(!file.exists(paths))) {
    missing <- paths[!file.exists(paths)]
    stop(
      "Cannot checksum missing file(s): ", paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  info <- file.info(paths)
  tibble::tibble(
    vintage = as.character(vintage),
    path = normalizePath(paths, mustWork = TRUE),
    basename = basename(paths),
    size_bytes = as.numeric(info$size),
    mtime = format(info$mtime, "%Y-%m-%d %H:%M:%S %Z", tz = "UTC"),
    sha256 = unname(tools::sha256sum(paths))
  )
}

read_cleaning_candidate <- function(path) {
  table <- arrow::read_parquet(path, as_data_frame = FALSE)
  list(data = tibble::as_tibble(table), metadata = table$metadata)
}

validate_shared_run_stamp <- function(
    long_metadata,
    study_metadata,
    expected_market = "sales") {
  required <- c(
    "cleaning_manifest_version", "cleaning_run_stamp", "cleaning_market",
    "cleaning_artifact_role", "cleaning_year_min", "cleaning_year_max",
    "cleaning_source_row_count", "cleaning_parent_role"
  )
  missing_long <- setdiff(required, names(long_metadata))
  missing_study <- setdiff(required, names(study_metadata))
  issues <- character()
  if (length(missing_long)) {
    issues <- c(issues, paste("long-run missing", paste(missing_long, collapse = ", ")))
  }
  if (length(missing_study)) {
    issues <- c(issues, paste("study missing", paste(missing_study, collapse = ", ")))
  }
  if (length(issues) == 0L &&
      !identical(long_metadata$cleaning_run_stamp, study_metadata$cleaning_run_stamp)) {
    issues <- c(issues, "candidate run stamps differ")
  }
  if (length(issues) == 0L &&
      !identical(long_metadata$cleaning_manifest_version,
                 study_metadata$cleaning_manifest_version)) {
    issues <- c(issues, "candidate manifest versions differ")
  }
  if (length(issues) == 0L &&
      (!identical(long_metadata$cleaning_market, expected_market) ||
       !identical(study_metadata$cleaning_market, expected_market))) {
    issues <- c(issues, paste0("candidate market metadata is not ", expected_market))
  }
  if (length(issues) == 0L && long_metadata$cleaning_artifact_role != "long_run") {
    issues <- c(issues, "long-run candidate role is not long_run")
  }
  if (length(issues) == 0L && study_metadata$cleaning_artifact_role != "study") {
    issues <- c(issues, "study candidate role is not study")
  }
  if (length(issues) == 0L && study_metadata$cleaning_parent_role != "long_run") {
    issues <- c(issues, "study candidate does not declare long_run parent")
  }

  tibble::tibble(
    check = "shared_cleaning_run_stamp_contract",
    passed = length(issues) == 0L,
    detail = if (length(issues)) paste(issues, collapse = "; ") else
      paste0("shared run stamp ", long_metadata$cleaning_run_stamp)
  )
}

write_reconciliation_tables <- function(result, output_dir) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  table_names <- c(
    "summary", "transitions", "old_duplicate_ids", "old_duplicate_records",
    "study_ids_added", "study_ids_removed", "unexpected_value_deltas",
    "unexpected_membership_deltas", "candidate_period_issues",
    "candidate_pair_issues"
  )
  for (name in table_names) {
    utils::write.csv(
      result[[name]],
      file.path(output_dir, paste0("lr_", name, ".csv")),
      row.names = FALSE,
      na = ""
    )
  }
  invisible(output_dir)
}

default_reconciliation_config <- function() {
  list(
    old_study_path = here::here("data", "processed", "house_price.parquet"),
    candidate_long_path = here::here(
      "data", "processed", "house_price_long_run_candidate.parquet"
    ),
    candidate_study_path = here::here(
      "data", "processed", "house_price_candidate.parquet"
    ),
    old_raw_dir = Sys.getenv("LR_REBUILD_OLD_RAW_DIR", unset = ""),
    new_raw_dir = here::here("data", "raw", "lr_house_price"),
    output_dir = Sys.getenv(
      "CLEANING_REBUILD_EVIDENCE_DIR",
      unset = here::here("output", "cleaning_rebuild_reconciliation_2026-08-14")
    ),
    years = 2014:2024,
    study_years = 2021:2024
  )
}

run_lr_reconciliation <- function(config = default_reconciliation_config()) {
  if (!nzchar(config$old_raw_dir)) {
    stop(
      "Set LR_REBUILD_OLD_RAW_DIR to the dated archive of the pre-refresh raw vintage before gate reconciliation.",
      call. = FALSE
    )
  }

  required_data_paths <- c(
    config$old_study_path,
    config$candidate_long_path,
    config$candidate_study_path
  )
  if (any(!file.exists(required_data_paths))) {
    stop(
      "Required LR reconciliation artifact(s) missing: ",
      paste(required_data_paths[!file.exists(required_data_paths)], collapse = ", "),
      call. = FALSE
    )
  }

  expected_new_raw <- file.path(config$new_raw_dir, sprintf("pp-%d.csv", config$years))
  if (any(!file.exists(expected_new_raw))) {
    stop(
      "The same-session LR refresh is incomplete; missing: ",
      paste(basename(expected_new_raw[!file.exists(expected_new_raw)]), collapse = ", "),
      call. = FALSE
    )
  }
  old_raw <- list.files(config$old_raw_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(old_raw) == 0L) {
    stop("The archived LR raw vintage contains no CSV files.", call. = FALSE)
  }

  old <- arrow::read_parquet(config$old_study_path)
  long_candidate <- read_cleaning_candidate(config$candidate_long_path)
  study_candidate <- read_cleaning_candidate(config$candidate_study_path)
  metadata_check <- validate_shared_run_stamp(
    long_candidate$metadata,
    study_candidate$metadata
  )
  result <- reconcile_lr_allowed_deltas(
    old,
    long_candidate$data,
    study_candidate$data,
    config$study_years
  )

  dir.create(config$output_dir, recursive = TRUE, showWarnings = FALSE)
  write_reconciliation_tables(result, config$output_dir)
  checksums <- dplyr::bind_rows(
    build_file_vintage_report(old_raw, "archived_pre_refresh"),
    build_file_vintage_report(expected_new_raw, "same_session_refresh"),
    build_file_vintage_report(required_data_paths, "reconciliation_artifact")
  )
  utils::write.csv(
    checksums,
    file.path(config$output_dir, "lr_file_vintage_checksums.csv"),
    row.names = FALSE,
    na = ""
  )
  utils::write.csv(
    metadata_check,
    file.path(config$output_dir, "lr_candidate_metadata_check.csv"),
    row.names = FALSE,
    na = ""
  )

  blocking_count <- nrow(result$unexpected_value_deltas) +
    nrow(result$unexpected_membership_deltas) +
    nrow(result$candidate_period_issues) +
    nrow(result$candidate_pair_issues) +
    sum(!metadata_check$passed)
  if (blocking_count > 0L) {
    stop(
      "LR cleaning reconciliation found ", blocking_count,
      " contract violation(s). Review evidence in ", config$output_dir, ".",
      call. = FALSE
    )
  }

  invisible(list(
    result = result,
    metadata_check = metadata_check,
    checksums = checksums,
    output_dir = config$output_dir
  ))
}

write_zoopla_reconciliation_tables <- function(result, output_dir) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  table_names <- c(
    "summary", "dedupe_summary", "duplicate_records", "selection_removed",
    "identity_conflicts", "unexpected_value_deltas",
    "unexpected_membership_deltas", "candidate_period_issues",
    "candidate_pair_issues", "long_window_issues"
  )
  for (name in table_names) {
    utils::write.csv(
      result[[name]],
      file.path(output_dir, paste0("zoopla_", name, ".csv")),
      row.names = FALSE,
      na = ""
    )
  }
  invisible(output_dir)
}

default_zoopla_reconciliation_config <- function() {
  list(
    old_study_path = here::here(
      "data", "processed", "zoopla", "zoopla_rentals.parquet"
    ),
    candidate_long_path = here::here(
      "data", "processed", "zoopla", "zoopla_rentals_long_run_candidate.parquet"
    ),
    candidate_study_path = here::here(
      "data", "processed", "zoopla", "zoopla_rentals_candidate.parquet"
    ),
    raw_paths = c(
      here::here("data", "raw", "zoopla", "rentals_safeguarded_2014-2022.csv"),
      here::here("data", "raw", "zoopla", "rentals_safeguarded_2023.csv")
    ),
    output_dir = Sys.getenv(
      "CLEANING_REBUILD_EVIDENCE_DIR",
      unset = here::here("output", "cleaning_rebuild_reconciliation_2026-08-14")
    ),
    years = 2014:2023,
    study_years = 2021:2023
  )
}

run_zoopla_reconciliation <- function(
    config = default_zoopla_reconciliation_config()) {
  required_paths <- c(
    config$old_study_path,
    config$candidate_long_path,
    config$candidate_study_path,
    config$raw_paths
  )
  if (any(!file.exists(required_paths))) {
    stop(
      "Required Zoopla reconciliation artifact(s) missing: ",
      paste(required_paths[!file.exists(required_paths)], collapse = ", "),
      call. = FALSE
    )
  }

  old <- arrow::read_parquet(config$old_study_path)
  long_candidate <- read_cleaning_candidate(config$candidate_long_path)
  study_candidate <- read_cleaning_candidate(config$candidate_study_path)
  metadata_check <- validate_shared_run_stamp(
    long_candidate$metadata,
    study_candidate$metadata,
    expected_market = "rentals"
  )
  result <- reconcile_zoopla_allowed_deltas(
    old,
    long_candidate$data,
    study_candidate$data,
    config$study_years,
    config$years
  )

  dir.create(config$output_dir, recursive = TRUE, showWarnings = FALSE)
  write_zoopla_reconciliation_tables(result, config$output_dir)
  checksums <- build_file_vintage_report(
    required_paths,
    "zoopla_reconciliation_artifact"
  )
  utils::write.csv(
    checksums,
    file.path(config$output_dir, "zoopla_file_checksums.csv"),
    row.names = FALSE,
    na = ""
  )
  utils::write.csv(
    metadata_check,
    file.path(config$output_dir, "zoopla_candidate_metadata_check.csv"),
    row.names = FALSE,
    na = ""
  )

  blocking_count <- nrow(result$identity_conflicts) +
    nrow(result$unexpected_value_deltas) +
    nrow(result$unexpected_membership_deltas) +
    nrow(result$candidate_period_issues) +
    nrow(result$candidate_pair_issues) +
    nrow(result$long_window_issues) +
    sum(!metadata_check$passed)
  if (blocking_count > 0L) {
    stop(
      "Zoopla cleaning reconciliation found ", blocking_count,
      " contract violation(s). Review evidence in ", config$output_dir, ".",
      call. = FALSE
    )
  }

  invisible(list(
    result = result,
    metadata_check = metadata_check,
    checksums = checksums,
    output_dir = config$output_dir
  ))
}

if (sys.nframe() == 0) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) > 0L && identical(args[[1]], "rentals")) {
    run_zoopla_reconciliation()
  } else {
    run_lr_reconciliation()
  }
}
