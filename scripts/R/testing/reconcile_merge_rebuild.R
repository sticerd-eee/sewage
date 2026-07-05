# ==============================================================================
# Reconcile merge_individ_annual_location Rebuild
# ==============================================================================
#
# Purpose: Compare the CH6 works-register rebuild against the CH2 old-script
#          baseline and write the CH7 gate-review evidence.
#
# Author: Jacopo Olivieri
# Date: 2026-07-05
#
# Inputs:
#   - data/processed/combined_edm_data.parquet
#   - data/processed/annual_return_edm.parquet
#   - data/processed/annual_return_lookup.parquet
#   - data/processed/matched_events_annual_data_baseline_2026-07-05_ch2_current_inputs/
#   - data/processed/matched_events_annual_data/
#
# Outputs:
#   - output/merge_rebuild_reconciliation_2026-07-05/
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(purrr)
  library(rnrfa)
  library(stringr)
  library(tibble)
  library(tidyr)
})

options(lifecycle_verbosity = "quiet")

source(here::here("scripts", "R", "utils", "ngr_utils.R"), local = TRUE)

CONFIG <- list(
  baseline_dir = here::here(
    "data", "processed",
    "matched_events_annual_data_baseline_2026-07-05_ch2_current_inputs"
  ),
  new_dir = here::here("data", "processed", "matched_events_annual_data"),
  output_dir = here::here("output", "merge_rebuild_reconciliation_2026-07-05"),
  event_path = here::here("data", "processed", "combined_edm_data.parquet"),
  annual_path = here::here("data", "processed", "annual_return_edm.parquet"),
  lookup_path = here::here("data", "processed", "annual_return_lookup.parquet"),
  match_rate_soft_floor = 0.95
)

# Old-baseline rows are aligned to raw events on this group key. `unique_id`
# is deliberately EXCLUDED: the old script rewrote unique_id in its own output
# (141,740 rows, mostly Anglian 2024 and Yorkshire 2022), so any join through
# it misclassifies matched events as missing. Within groups that share the
# same timestamps (batch-recorded simultaneous CSO events), rows are paired by
# a membership-respecting assignment, not by file order — see the pairing
# section below.
EVENT_GROUP_KEY_COLS <- c(
  "water_company",
  "year",
  "start_time",
  "end_time"
)

NEW_EVENT_KEY_COLS <- c(
  "water_company",
  "year",
  "site_name_ea",
  "site_name_wa_sc",
  "permit_reference_ea",
  "permit_reference_wa_sc",
  "activity_reference",
  "asset_type",
  "unique_id",
  "start_time",
  "end_time"
)

# `unique_id` here is the raw event's value, carried through the new-side
# alignment; the old output's (rewritten) value is reported as old_unique_id.
REPORT_EVENT_KEY_COLS <- c(EVENT_GROUP_KEY_COLS, "unique_id")

EXACT_NEW_METHODS <- c(
  "unique_id",
  "permit_activity",
  "permit",
  "site_name_ea",
  "site_name_wa_sc",
  "manual_override"
)

dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

path_out <- function(...) {
  file.path(CONFIG$output_dir, ...)
}

write_csv <- function(data, path) {
  utils::write.csv(data, path, row.names = FALSE, na = "")
  invisible(path)
}

write_parquet_safe <- function(data, path) {
  arrow::write_parquet(data, path)
  invisible(path)
}

md_table <- function(data, digits = 3) {
  if (nrow(data) == 0) {
    return("_None._")
  }

  printable <- data %>%
    dplyr::mutate(
      dplyr::across(where(is.numeric), ~ round(.x, digits = digits))
    ) %>%
    dplyr::mutate(dplyr::across(everything(), as.character))

  header <- paste0("| ", paste(names(printable), collapse = " | "), " |")
  rule <- paste0("| ", paste(rep("---", ncol(printable)), collapse = " | "), " |")
  rows <- apply(printable, 1, function(row) {
    paste0("| ", paste(row, collapse = " | "), " |")
  })

  paste(c(header, rule, rows), collapse = "\n")
}

current_fingerprints <- function(recorded) {
  purrr::map_dfr(seq_len(nrow(recorded)), function(i) {
    path <- here::here(recorded$path[i])
    info <- file.info(path)
    tibble::tibble(
      artifact = recorded$artifact[i],
      path = recorded$path[i],
      size_bytes = as.numeric(info$size),
      mtime = format(info$mtime, "%Y-%m-%d %H:%M:%S %Z", tz = "Europe/London"),
      sha256 = unname(tools::sha256sum(path))
    )
  })
}

assert_input_fingerprints <- function() {
  fingerprint_path <- file.path(CONFIG$baseline_dir, "ch2_input_fingerprints.csv")
  recorded <- utils::read.csv(fingerprint_path, stringsAsFactors = FALSE)
  current <- current_fingerprints(recorded)

  comparison <- recorded %>%
    dplyr::rename(
      recorded_size_bytes = .data$size_bytes,
      recorded_mtime = .data$mtime,
      recorded_sha256 = .data$sha256
    ) %>%
    dplyr::left_join(
      current %>%
        dplyr::rename(
          current_size_bytes = .data$size_bytes,
          current_mtime = .data$mtime,
          current_sha256 = .data$sha256
        ),
      by = c("artifact", "path")
    ) %>%
    dplyr::mutate(
      size_matches = .data$recorded_size_bytes == .data$current_size_bytes,
      mtime_matches = .data$recorded_mtime == .data$current_mtime,
      sha256_matches = .data$recorded_sha256 == .data$current_sha256,
      fingerprint_matches = .data$size_matches & .data$mtime_matches & .data$sha256_matches
    )

  write_csv(comparison, path_out("input_fingerprint_check.csv"))

  if (!all(comparison$fingerprint_matches)) {
    stop(
      "CH2 input fingerprints no longer match on-disk inputs; re-baseline before reconciliation.",
      call. = FALSE
    )
  }

  comparison
}

add_event_instances <- function(tbl, key_cols) {
  tbl %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) %>%
    dplyr::mutate(event_key_instance = dplyr::row_number()) %>%
    dplyr::ungroup()
}

attach_event_ids <- function(tbl, event_index, key_cols) {
  indexed <- add_event_instances(tbl, key_cols)
  indexed %>%
    dplyr::left_join(
      event_index,
      by = c(key_cols, "event_key_instance"),
      na_matches = "na"
    )
}

ensure_count_columns <- function(tbl, cols) {
  for (col in cols) {
    if (!col %in% names(tbl)) {
      tbl[[col]] <- 0L
    }
  }
  tbl
}

classify_old_tier <- function(match_method) {
  dplyr::case_when(
    match_method == "windfall" ~ "old_exact",
    match_method == "max" ~ "old_max",
    match_method == "fuzzy" ~ "old_fuzzy",
    TRUE ~ "old_other"
  )
}

classify_new_tier <- function(match_method) {
  dplyr::case_when(
    match_method %in% EXACT_NEW_METHODS ~ "new_exact_ladder",
    match_method == "agreement" ~ "new_agreement",
    is.na(match_method) ~ "new_unmatched",
    TRUE ~ "new_other"
  )
}

parse_unique_ngrs <- function(ngr_values, prefix) {
  values <- tibble::tibble(ngr = unique(as.character(ngr_values))) %>%
    dplyr::mutate(ngr_clean = clean_ngr(.data$ngr))

  coords <- parse_bng_coordinates(values$ngr_clean)

  values %>%
    dplyr::bind_cols(coords) %>%
    dplyr::rename(
      !!paste0(prefix, "_ngr") := .data$ngr,
      !!paste0(prefix, "_ngr_clean") := .data$ngr_clean,
      !!paste0(prefix, "_easting") := .data$easting,
      !!paste0(prefix, "_northing") := .data$northing
    )
}

run_contract_tests <- function() {
  command <- "Rscript scripts/R/testing/test_merge_individ_annual_contracts.R"
  output <- suppressWarnings(system2(
    "Rscript",
    "scripts/R/testing/test_merge_individ_annual_contracts.R",
    stdout = TRUE,
    stderr = TRUE
  ))
  status <- attr(output, "status")
  if (is.null(status)) status <- 0L

  writeLines(output, path_out("contract_tests.log"))

  tibble::tibble(
    command = command,
    exit_status = as.integer(status),
    passed = identical(as.integer(status), 0L)
  )
}

fingerprints <- assert_input_fingerprints()

message("Reading raw events and merge outputs...")
events <- arrow::read_parquet(CONFIG$event_path) %>%
  dplyr::select(dplyr::any_of(c(NEW_EVENT_KEY_COLS, "new_unqiue_id")))

# Mirror the orchestrator's ingest normalisation: Anglian 2024 events carry
# their unique_id only in the typo-named `new_unqiue_id` column, and the
# pipeline coalesces it at load. Raw-event identity must match the pipeline's
# effective input or the new-side alignment fails for exactly those rows.
if ("new_unqiue_id" %in% names(events)) {
  events <- events %>%
    dplyr::mutate(
      unique_id = dplyr::coalesce(.data$unique_id, .data$new_unqiue_id)
    ) %>%
    dplyr::select(-"new_unqiue_id")
}

events <- events %>%
  dplyr::mutate(event_id = dplyr::row_number())

new_event_index <- events %>%
  dplyr::select(.data$event_id, dplyr::all_of(NEW_EVENT_KEY_COLS)) %>%
  add_event_instances(NEW_EVENT_KEY_COLS)

old_matched <- arrow::read_parquet(file.path(CONFIG$baseline_dir, "matched_events_annual_data.parquet"))
old_unmatched <- arrow::read_parquet(file.path(CONFIG$baseline_dir, "events_unmatched.parquet"))
old_site_metadata <- arrow::read_parquet(file.path(CONFIG$baseline_dir, "site_metadata.parquet"))

new_matched <- arrow::read_parquet(file.path(CONFIG$new_dir, "matched_events_annual_data.parquet"))
new_unmatched <- arrow::read_parquet(file.path(CONFIG$new_dir, "events_unmatched.parquet"))
new_crosswalk <- arrow::read_parquet(file.path(CONFIG$new_dir, "site_works_crosswalk.parquet"))
new_near_miss <- arrow::read_parquet(file.path(CONFIG$new_dir, "near_miss_report.parquet"))

old_pseudo_rows <- old_matched %>%
  dplyr::filter(is.na(.data$start_time) | is.na(.data$end_time))
old_matched_real <- old_matched %>%
  dplyr::filter(!is.na(.data$start_time), !is.na(.data$end_time))

old_rows <- old_matched_real %>%
  dplyr::select(
    dplyr::all_of(EVENT_GROUP_KEY_COLS),
    old_unique_id = .data$unique_id,
    old_site_id = .data$site_id,
    old_match_method = .data$match_method,
    old_match_quality = .data$match_quality,
    old_match_key = .data$match_key,
    old_ngr = .data$ngr
  ) %>%
  dplyr::mutate(
    old_tier = classify_old_tier(.data$old_match_method),
    old_row_id = dplyr::row_number()
  )

new_matched_ids <- new_matched %>%
  dplyr::select(
    dplyr::all_of(NEW_EVENT_KEY_COLS),
    new_site_id = .data$site_id,
    new_match_method = .data$match_method,
    new_match_quality = .data$match_quality,
    new_annual_status = .data$annual_status,
    new_ngr = .data$ngr
  ) %>%
  attach_event_ids(new_event_index, NEW_EVENT_KEY_COLS) %>%
  dplyr::mutate(
    new_outcome = "matched",
    new_reason = NA_character_,
    new_tier = classify_new_tier(.data$new_match_method)
  )

new_unmatched_ids <- new_unmatched %>%
  dplyr::select(
    dplyr::all_of(NEW_EVENT_KEY_COLS),
    new_reason = .data$reason
  ) %>%
  attach_event_ids(new_event_index, NEW_EVENT_KEY_COLS) %>%
  dplyr::mutate(
    new_site_id = NA_integer_,
    new_match_method = NA_character_,
    new_match_quality = NA_real_,
    new_annual_status = NA_character_,
    new_ngr = NA_character_,
    new_outcome = "unmatched",
    new_tier = "new_unmatched"
  )

new_outcomes <- dplyr::bind_rows(
  new_matched_ids,
  new_unmatched_ids
) %>%
  dplyr::select(
    .data$event_id,
    dplyr::all_of(NEW_EVENT_KEY_COLS),
    .data$new_outcome,
    .data$new_reason,
    .data$new_site_id,
    .data$new_match_method,
    .data$new_match_quality,
    .data$new_annual_status,
    .data$new_ngr,
    .data$new_tier
  )

new_missing_event_ids <- events %>%
  dplyr::select(.data$event_id) %>%
  dplyr::anti_join(
    new_outcomes %>% dplyr::filter(!is.na(.data$event_id)) %>% dplyr::distinct(.data$event_id),
    by = "event_id"
  )
new_duplicate_event_ids <- new_outcomes %>%
  dplyr::filter(!is.na(.data$event_id)) %>%
  dplyr::count(.data$event_id, name = "n") %>%
  dplyr::filter(.data$n > 1)
new_unmapped_rows <- new_outcomes %>%
  dplyr::filter(is.na(.data$event_id))

write_csv(new_missing_event_ids, path_out("new_missing_event_ids.csv"))
write_csv(new_duplicate_event_ids, path_out("new_duplicate_event_ids.csv"))
write_csv(new_unmapped_rows, path_out("new_unmapped_output_rows.csv"))

site_members <- new_crosswalk %>%
  dplyr::distinct(
    new_site_id = .data$site_id,
    new_site_id_members = .data$site_id_members
  ) %>%
  tidyr::separate_rows(.data$new_site_id_members, sep = ";") %>%
  dplyr::mutate(member_site_id = as.integer(.data$new_site_id_members)) %>%
  dplyr::transmute(
    .data$new_site_id,
    .data$member_site_id,
    old_site_in_new_works = TRUE
  )

new_site_members <- new_crosswalk %>%
  dplyr::distinct(
    new_site_id = .data$site_id,
    new_site_id_members = .data$site_id_members
  )

# ------------------------------------------------------------------------------
# Membership-respecting old -> new pairing.
#
# Both the baseline and the new outputs partition the same raw events, but
# neither carries a shared row id, and batch-recorded CSO events duplicate the
# (company, year, start, end) group key (up to 40 simultaneous events). Pairing
# rows by file order inside those groups fabricates works changes between
# unrelated simultaneous events. Instead: within each group, an old row is
# first paired with a new row assigned to the SAME works as the old site (via
# crosswalk membership); only the remainder pairs by within-group order.
# ------------------------------------------------------------------------------

member_to_works <- site_members %>%
  dplyr::distinct(.data$member_site_id, works_of_member = .data$new_site_id)

member_multiplicity <- member_to_works %>%
  dplyr::count(.data$member_site_id, name = "n") %>%
  dplyr::filter(.data$n > 1)
if (nrow(member_multiplicity) > 0) {
  stop(
    "Crosswalk member site_ids map to more than one works; ",
    "register components are not disjoint.",
    call. = FALSE
  )
}

new_side <- new_outcomes %>%
  dplyr::select(
    .data$event_id,
    dplyr::all_of(EVENT_GROUP_KEY_COLS),
    .data$unique_id,
    .data$new_outcome,
    .data$new_reason,
    .data$new_site_id,
    .data$new_match_method,
    .data$new_match_quality,
    .data$new_annual_status,
    .data$new_ngr,
    .data$new_tier
  )

old_side <- old_rows %>%
  dplyr::left_join(member_to_works, by = c("old_site_id" = "member_site_id"))

old_tier1 <- old_side %>%
  dplyr::filter(!is.na(.data$works_of_member)) %>%
  dplyr::group_by(
    dplyr::across(dplyr::all_of(EVENT_GROUP_KEY_COLS)),
    .data$works_of_member
  ) %>%
  dplyr::mutate(pair_rank = dplyr::row_number()) %>%
  dplyr::ungroup()

new_tier1 <- new_side %>%
  dplyr::filter(.data$new_outcome == "matched", !is.na(.data$new_site_id)) %>%
  dplyr::group_by(
    dplyr::across(dplyr::all_of(EVENT_GROUP_KEY_COLS)),
    .data$new_site_id
  ) %>%
  dplyr::mutate(pair_rank = dplyr::row_number()) %>%
  dplyr::ungroup()

paired_tier1 <- old_tier1 %>%
  dplyr::inner_join(
    new_tier1,
    by = c(EVENT_GROUP_KEY_COLS, "works_of_member" = "new_site_id", "pair_rank"),
    na_matches = "na"
  ) %>%
  dplyr::mutate(new_site_id = .data$works_of_member)

old_tier2 <- old_side %>%
  dplyr::anti_join(paired_tier1, by = "old_row_id") %>%
  dplyr::group_by(dplyr::across(dplyr::all_of(EVENT_GROUP_KEY_COLS))) %>%
  dplyr::mutate(pair_rank = dplyr::row_number()) %>%
  dplyr::ungroup()

new_tier2 <- new_side %>%
  dplyr::anti_join(
    paired_tier1 %>% dplyr::distinct(.data$event_id),
    by = "event_id"
  ) %>%
  dplyr::group_by(dplyr::across(dplyr::all_of(EVENT_GROUP_KEY_COLS))) %>%
  dplyr::mutate(pair_rank = dplyr::row_number()) %>%
  dplyr::ungroup()

paired_tier2 <- old_tier2 %>%
  dplyr::left_join(
    new_tier2,
    by = c(EVENT_GROUP_KEY_COLS, "pair_rank"),
    na_matches = "na"
  )

old_new_compare <- dplyr::bind_rows(paired_tier1, paired_tier2) %>%
  dplyr::left_join(new_site_members, by = "new_site_id") %>%
  dplyr::mutate(
    old_site_in_new_works = !is.na(.data$works_of_member) &
      !is.na(.data$new_site_id) &
      .data$works_of_member == .data$new_site_id
  )

if (nrow(old_new_compare) != nrow(old_rows)) {
  stop(
    "Old->new pairing changed the baseline row count: ",
    nrow(old_new_compare), " paired vs ", nrow(old_rows), " baseline rows.",
    call. = FALSE
  )
}
if (anyDuplicated(old_new_compare$old_row_id) > 0) {
  stop("Old->new pairing duplicated baseline rows.", call. = FALSE)
}
paired_event_dupes <- old_new_compare %>%
  dplyr::filter(!is.na(.data$event_id)) %>%
  dplyr::count(.data$event_id, name = "n") %>%
  dplyr::filter(.data$n > 1)
if (nrow(paired_event_dupes) > 0) {
  stop(
    "Old->new pairing assigned ", nrow(paired_event_dupes),
    " new rows to more than one baseline row.",
    call. = FALSE
  )
}

total_by_year <- events %>%
  dplyr::count(.data$year, name = "total_events")

# Baseline rows are 1:1 with raw events (matched + unmatched + pseudo = raw),
# so tier counts come straight from the baseline rows. The previous version
# counted only rows that survived an event-id join through unique_id, which
# silently dropped 147k rewritten-unique_id rows and understated old rates.
old_tier_counts <- old_rows %>%
  dplyr::count(.data$year, .data$old_tier, name = "n") %>%
  tidyr::pivot_wider(names_from = .data$old_tier, values_from = .data$n, values_fill = 0)

new_tier_counts <- new_outcomes %>%
  dplyr::filter(.data$new_outcome == "matched", !is.na(.data$event_id)) %>%
  dplyr::distinct(.data$event_id, .data$year, .data$new_tier) %>%
  dplyr::count(.data$year, .data$new_tier, name = "n") %>%
  tidyr::pivot_wider(names_from = .data$new_tier, values_from = .data$n, values_fill = 0)

new_absent_counts <- new_outcomes %>%
  dplyr::filter(.data$new_outcome == "matched", .data$new_annual_status == "absent") %>%
  dplyr::distinct(.data$event_id, .data$year) %>%
  dplyr::count(.data$year, name = "new_absent_matches")

match_rates_by_year <- total_by_year %>%
  dplyr::left_join(old_tier_counts, by = "year") %>%
  dplyr::left_join(new_tier_counts, by = "year") %>%
  dplyr::left_join(new_absent_counts, by = "year") %>%
  ensure_count_columns(c(
    "old_exact",
    "old_max",
    "old_fuzzy",
    "new_exact_ladder",
    "new_agreement",
    "new_absent_matches"
  )) %>%
  dplyr::mutate(dplyr::across(where(is.numeric), ~ tidyr::replace_na(.x, 0))) %>%
  dplyr::mutate(
    old_matched_events = .data$old_exact + .data$old_max + .data$old_fuzzy,
    new_matched_including_absent = .data$new_exact_ladder + .data$new_agreement,
    new_absent_matches = as.integer(.data$new_absent_matches),
    new_matched_excluding_absent = .data$new_matched_including_absent - .data$new_absent_matches,
    old_match_rate = .data$old_matched_events / .data$total_events,
    new_match_rate_including_absent = .data$new_matched_including_absent / .data$total_events,
    new_match_rate_excluding_absent = .data$new_matched_excluding_absent / .data$total_events
  ) %>%
  dplyr::arrange(.data$year)

write_csv(match_rates_by_year, path_out("match_rates_by_year.csv"))

overall_rates <- tibble::tibble(
  total_events = nrow(events),
  old_matched_events = nrow(old_rows),
  old_pseudo_rows = nrow(old_pseudo_rows),
  old_unmatched_rows = nrow(old_unmatched),
  new_matched_including_absent = dplyr::n_distinct(new_matched_ids$event_id, na.rm = TRUE),
  new_absent_matches = dplyr::n_distinct(
    new_matched_ids$event_id[new_matched_ids$new_annual_status == "absent"],
    na.rm = TRUE
  ),
  new_unmatched_rows = nrow(new_unmatched_ids)
) %>%
  dplyr::mutate(
    new_match_rate_including_absent = .data$new_matched_including_absent / .data$total_events,
    new_match_rate_excluding_absent = (
      .data$new_matched_including_absent - .data$new_absent_matches
    ) / .data$total_events
  )

write_csv(overall_rates, path_out("overall_match_rates.csv"))

old_max_fate <- old_new_compare %>%
  dplyr::filter(.data$old_match_method == "max") %>%
  dplyr::mutate(
    fate = dplyr::case_when(
      is.na(.data$new_outcome) ~ "unpaired_alignment_failure",
      .data$new_outcome == "unmatched" ~ paste0("unmatched_", .data$new_reason),
      .data$new_annual_status == "absent" ~ "matched_to_absent",
      .data$new_match_method == "agreement" ~ "agreement_matched",
      .data$old_site_in_new_works ~ "register_absorbed",
      TRUE ~ paste0("matched_", .data$new_match_method)
    )
  )

old_max_fate_summary <- old_max_fate %>%
  dplyr::count(.data$year, .data$fate, name = "n") %>%
  dplyr::arrange(.data$year, dplyr::desc(.data$n), .data$fate)

write_csv(old_max_fate_summary, path_out("baseline_max_fate_by_year.csv"))

exact_changed_works <- old_new_compare %>%
  dplyr::filter(
    .data$old_match_method == "windfall",
    .data$new_outcome == "matched",
    !is.na(.data$old_site_id),
    !is.na(.data$new_site_id),
    .data$old_site_id != .data$new_site_id
  ) %>%
  dplyr::mutate(
    exception_explanation = dplyr::if_else(
      .data$old_site_in_new_works,
      "register_collapse",
      "unexplained_works_change"
    )
  )

exact_lost <- old_new_compare %>%
  dplyr::filter(
    .data$old_match_method == "windfall",
    is.na(.data$new_outcome) | .data$new_outcome != "matched"
  ) %>%
  dplyr::mutate(
    exception_explanation = dplyr::case_when(
      is.na(.data$new_outcome) ~ "unpaired_alignment_failure",
      TRUE ~ paste0("new_unmatched_", .data$new_reason)
    )
  )

# Reason tallies so a reconciliation-side alignment failure can never
# masquerade as a new-pipeline regression in the gate evidence.
exact_lost_tally <- exact_lost %>%
  dplyr::count(.data$exception_explanation, name = "n") %>%
  dplyr::arrange(dplyr::desc(.data$n))
exact_changed_tally <- exact_changed_works %>%
  dplyr::count(.data$exception_explanation, name = "n") %>%
  dplyr::arrange(dplyr::desc(.data$n))
write_csv(exact_lost_tally, path_out("old_exact_lost_reason_tally.csv"))
write_csv(exact_changed_tally, path_out("old_exact_changed_works_tally.csv"))

tally_to_evidence <- function(tally) {
  if (nrow(tally) == 0) {
    return("none")
  }
  paste0(tally$exception_explanation, "=", tally$n, collapse = ", ")
}

write_parquet_safe(
  exact_changed_works %>%
    dplyr::select(
      .data$event_id,
      dplyr::all_of(REPORT_EVENT_KEY_COLS),
      .data$old_unique_id,
      .data$old_site_id,
      .data$new_site_id,
      .data$new_site_id_members,
      .data$old_match_method,
      .data$new_match_method,
      .data$new_annual_status,
      .data$old_ngr,
      .data$new_ngr,
      .data$exception_explanation
    ),
  path_out("old_exact_changed_works.parquet")
)
write_parquet_safe(
  exact_lost %>%
    dplyr::select(
      .data$event_id,
      dplyr::all_of(REPORT_EVENT_KEY_COLS),
      .data$old_unique_id,
      .data$old_site_id,
      .data$old_match_method,
      .data$old_match_key,
      .data$new_outcome,
      .data$new_reason,
      .data$exception_explanation
    ),
  path_out("old_exact_lost_matches.parquet")
)

message("Computing coordinate churn...")
coordinate_compare <- old_new_compare %>%
  dplyr::filter(.data$new_outcome == "matched", !is.na(.data$event_id)) %>%
  dplyr::select(
    .data$event_id,
    dplyr::all_of(REPORT_EVENT_KEY_COLS),
    .data$old_site_id,
    .data$new_site_id,
    .data$new_site_id_members,
    .data$old_site_in_new_works,
    .data$new_annual_status,
    .data$old_ngr,
    .data$new_ngr
  ) %>%
  dplyr::left_join(parse_unique_ngrs(.$old_ngr, "old"), by = "old_ngr") %>%
  dplyr::left_join(parse_unique_ngrs(.$new_ngr, "new"), by = "new_ngr") %>%
  dplyr::mutate(
    distance_m = sqrt(
      (.data$old_easting - .data$new_easting)^2 +
        (.data$old_northing - .data$new_northing)^2
    ),
    churn_bucket = dplyr::case_when(
      is.na(.data$distance_m) ~ "unparseable",
      .data$distance_m == 0 ~ "0m",
      .data$distance_m <= 100 ~ "(0,100]m",
      .data$distance_m <= 250 ~ "(100,250]m",
      .data$distance_m <= 1000 ~ "(250,1000]m",
      TRUE ~ ">1000m"
    ),
    coordinate_attribution = dplyr::case_when(
      .data$old_site_id == .data$new_site_id ~ "same_representative_site_ktd10_location_rule",
      .data$old_site_in_new_works ~ "register_collapse_ktd10_representative_location",
      TRUE ~ "changed_works_not_register_collapse"
    )
  )

coordinate_churn_summary <- coordinate_compare %>%
  dplyr::count(.data$churn_bucket, name = "n") %>%
  dplyr::mutate(share = .data$n / sum(.data$n)) %>%
  dplyr::arrange(factor(
    .data$churn_bucket,
    levels = c("0m", "(0,100]m", "(100,250]m", "(250,1000]m", ">1000m", "unparseable")
  ))

coordinate_gt1km <- coordinate_compare %>%
  dplyr::filter(.data$distance_m > 1000) %>%
  dplyr::arrange(dplyr::desc(.data$distance_m))

write_csv(coordinate_churn_summary, path_out("coordinate_churn_summary.csv"))
write_parquet_safe(coordinate_gt1km, path_out("coordinate_churn_gt1km.parquet"))

site_count_change <- tibble::tibble(
  metric = c(
    "old_distinct_matched_site_id",
    "old_site_metadata_rows",
    "new_distinct_works_site_id",
    "new_crosswalk_rows"
  ),
  value = c(
    dplyr::n_distinct(old_matched$site_id, na.rm = TRUE),
    nrow(old_site_metadata),
    dplyr::n_distinct(new_crosswalk$site_id, na.rm = TRUE),
    nrow(new_crosswalk)
  )
)
write_csv(site_count_change, path_out("site_count_change.csv"))

near_miss_summary <- new_near_miss %>%
  dplyr::count(.data$report_type, .data$reason, name = "n") %>%
  dplyr::arrange(dplyr::desc(.data$n))
write_csv(near_miss_summary, path_out("near_miss_summary.csv"))

contract_tests <- run_contract_tests()
write_csv(contract_tests, path_out("contract_tests_status.csv"))

row_accounting <- tibble::tibble(
  check = c(
    "new_matched_plus_unmatched_equals_raw_events",
    "new_outputs_all_map_to_raw_event_ids",
    "new_outputs_have_no_duplicate_event_ids",
    "new_outputs_have_no_missing_event_ids"
  ),
  passed = c(
    nrow(new_outcomes) == nrow(events),
    nrow(new_unmapped_rows) == 0,
    nrow(new_duplicate_event_ids) == 0,
    nrow(new_missing_event_ids) == 0
  ),
  evidence = c(
    paste0(nrow(new_outcomes), " output rows vs ", nrow(events), " raw events"),
    paste0(nrow(new_unmapped_rows), " unmapped output rows"),
    paste0(nrow(new_duplicate_event_ids), " duplicate event ids"),
    paste0(nrow(new_missing_event_ids), " missing event ids")
  )
)
write_csv(row_accounting, path_out("row_accounting_checks.csv"))

exact_gate <- tibble::tibble(
  check = c(
    "old_exact_no_lost_matches",
    "old_exact_works_changes_all_explained"
  ),
  passed = c(
    nrow(exact_lost) == 0,
    all(exact_changed_works$exception_explanation == "register_collapse")
  ),
  evidence = c(
    paste0(
      nrow(exact_lost),
      " old windfall rows are unmatched or missing in new outputs (",
      tally_to_evidence(exact_lost_tally), ")"
    ),
    paste0(
      nrow(exact_changed_works), " old windfall rows changed works (",
      tally_to_evidence(exact_changed_tally), ")"
    )
  )
)
write_csv(exact_gate, path_out("exact_tier_gate_checks.csv"))

hard_gates <- tibble::tibble(
  gate = c(
    "Input fingerprints unchanged",
    "Exact-tier preservation",
    "Zero unexplained event-row loss",
    "Contract tests green"
  ),
  passed = c(
    all(fingerprints$fingerprint_matches),
    all(exact_gate$passed),
    all(row_accounting$passed),
    all(contract_tests$passed)
  ),
  evidence = c(
    "All CH2 size, mtime, and SHA-256 fingerprints match current inputs.",
    paste(exact_gate$evidence, collapse = " / "),
    paste(row_accounting$evidence, collapse = " / "),
    paste0("Exit status ", contract_tests$exit_status)
  )
)
write_csv(hard_gates, path_out("hard_gates.csv"))

overall_new_rate <- overall_rates$new_match_rate_including_absent[[1]]
soft_gate_status <- if (overall_new_rate >= CONFIG$match_rate_soft_floor) {
  "Within the mid-90s expectation; no CONFIG tolerance change proposed."
} else {
  "Below the mid-90s expectation; review near-miss evidence before deciding CONFIG changes."
}

tolerance_proposal <- if (overall_new_rate >= CONFIG$match_rate_soft_floor) {
  "No tolerance change proposed. The overall including-absent match rate is at or above 95%, and the near-miss report contains only the listed register-distance cases."
} else {
  "Potential review items: inspect the near-miss report, then consider works_near_miss_m, agreement_rel_error, agreement_runner_up_ratio, and manual overrides as explicit logged decisions."
}

report_lines <- c(
  "# CH7 merge rebuild reconciliation gate report",
  "",
  paste0("Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z", tz = "Europe/London")),
  "",
  "## Inputs",
  "",
  "- Baseline: `data/processed/matched_events_annual_data_baseline_2026-07-05_ch2_current_inputs/`",
  "- New outputs: `data/processed/matched_events_annual_data/`",
  "- Evidence directory: `output/merge_rebuild_reconciliation_2026-07-05/`",
  "",
  "## Fingerprint Gate",
  "",
  md_table(fingerprints %>% dplyr::select(.data$artifact, .data$fingerprint_matches)),
  "",
  "## Match Rates By Year",
  "",
  md_table(
    match_rates_by_year %>%
      dplyr::select(
        .data$year,
        .data$total_events,
        .data$old_exact,
        .data$old_max,
        .data$old_fuzzy,
        .data$old_match_rate,
        .data$new_exact_ladder,
        .data$new_agreement,
        .data$new_absent_matches,
        .data$new_match_rate_including_absent,
        .data$new_match_rate_excluding_absent
      ),
    digits = 4
  ),
  "",
  "## Overall Rates",
  "",
  md_table(overall_rates, digits = 4),
  "",
  "## Baseline Max-To-One Fate",
  "",
  md_table(old_max_fate_summary, digits = 4),
  "",
  "## Site Count Change",
  "",
  md_table(site_count_change, digits = 4),
  "",
  "## Coordinate Churn",
  "",
  md_table(coordinate_churn_summary, digits = 4),
  "",
  paste0(
    "- Events with assigned location moving >1 km: ",
    nrow(coordinate_gt1km),
    " (full list: `coordinate_churn_gt1km.parquet`)."
  ),
  "- Attribution uses KTD-10: the new location is the representative works member's most recent non-NA NGR carried across absent years.",
  "",
  "## Old Exact-Tier Exceptions",
  "",
  paste0(
    "- Old windfall rows whose works changed: ",
    nrow(exact_changed_works),
    " (full list: `old_exact_changed_works.parquet`)."
  ),
  "",
  md_table(exact_changed_tally),
  "",
  paste0(
    "- Old windfall rows lost or unmatched in new outputs: ",
    nrow(exact_lost),
    " (full list: `old_exact_lost_matches.parquet`)."
  ),
  "",
  md_table(exact_lost_tally),
  "",
  paste0(
    "- Pairing note: old rows are aligned to new rows on (company, year, ",
    "start, end) with same-works pairing preferred inside duplicated groups; ",
    "`unpaired_alignment_failure` rows (expected 0) are reconciliation ",
    "alignment failures, not pipeline losses."
  ),
  "",
  "## Near-Miss Evidence",
  "",
  md_table(near_miss_summary, digits = 4),
  "",
  "## Hard Gates",
  "",
  md_table(hard_gates, digits = 4),
  "",
  "## Soft Gate",
  "",
  paste0("- Status: ", soft_gate_status),
  paste0("- CONFIG tolerance proposal: ", tolerance_proposal),
  "",
  "## Output Files",
  "",
  "- `input_fingerprint_check.csv`",
  "- `match_rates_by_year.csv`",
  "- `overall_match_rates.csv`",
  "- `baseline_max_fate_by_year.csv`",
  "- `site_count_change.csv`",
  "- `coordinate_churn_summary.csv`",
  "- `coordinate_churn_gt1km.parquet`",
  "- `old_exact_changed_works.parquet`",
  "- `old_exact_changed_works_tally.csv`",
  "- `old_exact_lost_matches.parquet`",
  "- `old_exact_lost_reason_tally.csv`",
  "- `near_miss_summary.csv`",
  "- `row_accounting_checks.csv`",
  "- `exact_tier_gate_checks.csv`",
  "- `hard_gates.csv`",
  "- `contract_tests.log`",
  "- `contract_tests_status.csv`",
  ""
)

writeLines(report_lines, path_out("merge_rebuild_reconciliation_report.md"))

message("Reconciliation report written to: ", path_out("merge_rebuild_reconciliation_report.md"))
message("Hard gates passed: ", all(hard_gates$passed))
message("Soft gate status: ", soft_gate_status)
