# ==============================================================================
# Reconcile the Canonical Spill Site / Site Group migration
# ==============================================================================
#
# This retained validation script is read-only with respect to production data.
# It accepts temporary candidate paths so a migration can be proved before the
# coordinated production publication step.
#
# Environment overrides:
#   SITE_GRAIN_BASELINE_UNIQUE      legacy unique_spill_sites parquet (optional)
#   SITE_GRAIN_NEW_UNIQUE           canonical candidate parquet
#   SITE_GRAIN_EVIDENCE_DIR         reconciliation evidence directory
#   SITE_GRAIN_BASELINE_AGG_DIR     pre-migration aggregate directory (optional)
#   SITE_GRAIN_NEW_AGG_DIR          candidate aggregate directory
#   SITE_GRAIN_BASELINE_EVENTS      pre-migration matched events (optional)
#   SITE_GRAIN_NEW_EVENTS           candidate matched events
#   SITE_GRAIN_ALLOW_PENDING_PUBLICATION  true only for pre-publication temp proof
#
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

path_from_env <- function(name, fallback = "") {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else fallback
}

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) stop(message, call. = FALSE)
}

assert_required_columns <- function(data, required, label) {
  missing <- setdiff(required, names(data))
  if (length(missing) > 0L) {
    stop(label, " is missing columns: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  invisible(data)
}

assert_unique_key <- function(data, key, label) {
  assert_required_columns(data, key, label)
  assert_true(
    all(stats::complete.cases(data[key])),
    paste0(label, " has missing key values.")
  )
  assert_true(
    !anyDuplicated(data[key]),
    paste0(label, " is not unique on ", paste(key, collapse = " + "), ".")
  )
  invisible(data)
}

same_values <- function(left, right) {
  both_na <- is.na(left) & is.na(right)
  both_na | (!is.na(left) & !is.na(right) & as.character(left) == as.character(right))
}

expand_site_group_membership <- function(crosswalk) {
  assert_required_columns(
    crosswalk,
    c("site_id", "year", "water_company", "site_id_canonical_members", "annual_status"),
    "Site Group crosswalk"
  )
  assert_unique_key(
    crosswalk,
    c("site_id", "year", "water_company"),
    "Site Group crosswalk"
  )
  assert_true(
    all(crosswalk$annual_status %in% c(
      "reported_zero", "reported_positive", "reported_na", "absent"
    )),
    "Site Group crosswalk contains an unknown annual_status."
  )

  crosswalk |>
    distinct(.data$site_id, .data$water_company, .data$site_id_canonical_members) |>
    separate_longer_delim("site_id_canonical_members", delim = ";") |>
    transmute(
      site_id = as.integer(.data$site_id),
      site_id_canonical = suppressWarnings(as.integer(trimws(.data$site_id_canonical_members))),
      water_company = .data$water_company
    ) |>
    distinct()
}

reconcile_canonical_contract <- function(candidate, lookup, crosswalk, annual) {
  required <- c(
    "site_id", "site_id_canonical", "water_company", "ngr",
    "edm_commission_date", "edm_commission_date_precision",
    "edm_commission_resolution_status", "no_longer_operational_year"
  )
  assert_required_columns(candidate, required, "Canonical candidate")
  assert_unique_key(candidate, "site_id_canonical", "Canonical candidate")
  assert_unique_key(lookup, "site_id", "Annual Return Lookup")

  lookup_ids <- sort(as.integer(lookup$site_id))
  candidate_ids <- sort(as.integer(candidate$site_id_canonical))
  assert_true(
    identical(candidate_ids, lookup_ids),
    "Canonical candidate IDs do not exactly equal the Annual Return Lookup universe."
  )

  membership <- expand_site_group_membership(crosswalk)
  assert_true(
    !anyNA(membership$site_id_canonical),
    "Site Group membership contains a missing or non-integer canonical ID."
  )
  assert_unique_key(membership, "site_id_canonical", "Canonical Site Group membership")
  assert_true(
    identical(sort(membership$site_id_canonical), lookup_ids),
    "Site Group membership does not cover every lookup ID exactly once."
  )

  representative_check <- membership |>
    group_by(.data$site_id) |>
    summarise(smallest_member = min(.data$site_id_canonical), .groups = "drop")
  assert_true(
    all(representative_check$site_id == representative_check$smallest_member),
    "A Site Group ID is not its smallest canonical member ID."
  )

  attached <- candidate |>
    select("site_id", "site_id_canonical", "water_company") |>
    inner_join(
      membership |>
        transmute(
          site_id_expected = .data$site_id,
          site_id_canonical = .data$site_id_canonical,
          water_company_expected = .data$water_company
        ),
      by = "site_id_canonical"
    )
  assert_true(
    nrow(attached) == nrow(candidate) &&
      all(attached$site_id == attached$site_id_expected) &&
      all(attached$water_company == attached$water_company_expected),
    "Canonical candidate Site Group IDs or companies disagree with crosswalk membership."
  )

  builder <- new.env(parent = globalenv())
  sys.source(
    here::here("scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"),
    envir = builder
  )
  builder$validate_commission_resolution(candidate)

  future_only <- candidate$edm_commission_resolution_status == "future_only"
  assert_true(
    all(is.na(candidate$edm_commission_date[future_only])),
    "A future-only history acquired an observed commission date."
  )

  years <- builder$CONFIG$years
  mapped <- builder$map_annual_to_canonical_sites(annual, lookup, years)
  expected_availability <- mapped |>
    distinct(.data$site_id_canonical, .data$year) |>
    mutate(expected = TRUE) |>
    complete(
      site_id_canonical = lookup_ids,
      year = years,
      fill = list(expected = FALSE)
    )
  observed_availability <- candidate |>
    select("site_id_canonical", all_of(paste0("available_year_", years))) |>
    pivot_longer(
      starts_with("available_year_"),
      names_to = "year",
      values_to = "observed"
    ) |>
    mutate(year = as.integer(sub("available_year_", "", .data$year)))
  availability <- inner_join(
    expected_availability,
    observed_availability,
    by = c("site_id_canonical", "year")
  )
  assert_true(
    nrow(availability) == length(lookup_ids) * length(years) &&
      all(availability$expected == availability$observed),
    "Canonical availability does not equal Annual Return row presence."
  )

  for (year in years) {
    for (stem in c("edm_operation_percent", "edm_operation_reason")) {
      value <- paste0(stem, "_", year)
      conflict <- paste0(stem, "_conflict_", year)
      assert_required_columns(candidate, c(value, conflict), "Canonical candidate")
      assert_true(
        all(!candidate[[conflict]] | is.na(candidate[[value]])),
        paste0("Conflicted ", stem, " values must be NA for ", year, ".")
      )
    }
  }

  list(
    membership = membership,
    availability = availability,
    years = years
  )
}

reconcile_legacy_inventory <- function(baseline, candidate) {
  if (is.null(baseline)) {
    return(tibble(check = "legacy_inventory", status = "pending_publication", detail = "No baseline path supplied"))
  }
  assert_unique_key(baseline, "site_id", "Legacy unique_spill_sites baseline")
  candidate_groups <- candidate |>
    distinct(.data$site_id)
  assert_true(
    identical(sort(as.integer(baseline$site_id)), sort(as.integer(candidate_groups$site_id))),
    "Site Group membership changed relative to the legacy unique inventory."
  )
  tibble(
    check = "legacy_inventory",
    status = "passed",
    detail = paste0(nrow(baseline), " legacy groups; ", nrow(candidate), " canonical sites")
  )
}

reconcile_aggregate_outputs <- function(baseline_dir, candidate_dir) {
  if (!nzchar(baseline_dir)) {
    return(tibble(check = "aggregate_stability", status = "pending_publication", detail = "No baseline aggregate directory supplied"))
  }
  specs <- tribble(
    ~file, ~key, ~values,
    "agg_spill_yr.parquet", list(c("site_id", "water_company", "year")), list(c("spill_count_yr", "spill_hrs_yr", "annual_status")),
    "agg_spill_mo.parquet", list(c("site_id", "water_company", "month_id")), list(c("spill_count_mo", "spill_hrs_mo", "annual_status")),
    "agg_spill_qtr.parquet", list(c("site_id", "water_company", "qtr_id")), list(c("spill_count_qt", "spill_hrs_qt", "annual_status"))
  )
  bind_rows(lapply(seq_len(nrow(specs)), function(index) {
    old_path <- file.path(baseline_dir, specs$file[index])
    new_path <- file.path(candidate_dir, specs$file[index])
    assert_true(file.exists(old_path), paste0("Missing aggregate baseline: ", old_path))
    assert_true(file.exists(new_path), paste0("Missing aggregate candidate: ", new_path))
    old <- read_parquet(old_path)
    new <- read_parquet(new_path)
    key <- specs$key[[index]]
    values <- specs$values[[index]]
    assert_unique_key(old, key, paste0(specs$file[index], " baseline"))
    assert_unique_key(new, key, paste0(specs$file[index], " candidate"))
    assert_true(nrow(old) == nrow(new), paste0(specs$file[index], " row count changed."))
    compared <- full_join(
      select(old, all_of(c(key, values))),
      select(new, all_of(c(key, values))),
      by = key,
      suffix = c("_old", "_new")
    )
    assert_true(nrow(compared) == nrow(old), paste0(specs$file[index], " key membership changed."))
    for (value in values) {
      assert_true(
        all(same_values(compared[[paste0(value, "_old")]], compared[[paste0(value, "_new")]])),
        paste0(specs$file[index], " changed ", value, ".")
      )
    }
    tibble(
      check = paste0("aggregate_stability_", sub("\\.parquet$", "", specs$file[index])),
      status = "passed",
      detail = paste0(nrow(new), " keys and core values unchanged")
    )
  }))
}

reconcile_event_totals <- function(baseline_path, candidate_path) {
  if (!nzchar(baseline_path)) {
    return(tibble(check = "matched_event_stability", status = "pending_publication", detail = "No baseline event path supplied"))
  }
  assert_true(file.exists(baseline_path), paste0("Missing matched-event baseline: ", baseline_path))
  assert_true(file.exists(candidate_path), paste0("Missing matched-event candidate: ", candidate_path))
  summarise_events <- function(path) {
    read_parquet(path, col_select = c(site_id, year, water_company, start_time, end_time)) |>
      group_by(.data$site_id, .data$year, .data$water_company) |>
      summarise(
        matched_event_rows = n(),
        event_hours = sum(as.numeric(difftime(.data$end_time, .data$start_time, units = "hours"))),
        .groups = "drop"
      )
  }
  old <- summarise_events(baseline_path)
  new <- summarise_events(candidate_path)
  key <- c("site_id", "year", "water_company")
  assert_unique_key(old, key, "Matched-event baseline summary")
  assert_unique_key(new, key, "Matched-event candidate summary")
  compared <- full_join(old, new, by = key, suffix = c("_old", "_new"))
  assert_true(
    nrow(compared) == nrow(old) && nrow(old) == nrow(new),
    "Matched-event Site Group keys changed."
  )
  assert_true(
    all(compared$matched_event_rows_old == compared$matched_event_rows_new) &&
      all(abs(compared$event_hours_old - compared$event_hours_new) < 1e-9),
    "Matched-event Site Group row counts or hours changed."
  )
  tibble(
    check = "matched_event_stability",
    status = "passed",
    detail = paste0(nrow(new), " Site Group-year-company totals unchanged")
  )
}

reconcile_consumer_manifest <- function() {
  consumer_env <- new.env(parent = globalenv())
  sys.source(
    here::here("scripts", "R", "testing", "reconcile_site_group_consumers.R"),
    envir = consumer_env
  )
  summary <- consumer_env$audit_site_grain_manifest()
  fixture <- consumer_env$run_fixture_reconciliation()
  assert_true(all(fixture$unexplained_changes == 0L), "Consumer fixture reconciliation found fanout or drift.")
  tibble(
    check = "consumer_manifest",
    status = "passed",
    detail = paste0(sum(summary$files), " classified grain-token surfaces; no fixture fanout")
  )
}

reconcile_figure_denominators <- function(candidate) {
  figure_env <- new.env(parent = globalenv())
  sys.source(
    here::here("scripts", "R", "utils", "edm_commission_figure_utils.R"),
    envir = figure_env
  )
  figure <- figure_env$prepare_edm_commission_figure_data(candidate)
  assert_true(
    sum(figure$completeness$n_canonical_sites) == nrow(candidate),
    "Commission completeness categories do not exhaust the canonical universe."
  )
  assert_true(
    sum(figure$annual_timing$n_canonical_sites) == figure$diagnostics$n_resolved,
    "Commission annual timing does not exhaust the resolved denominator."
  )
  assert_true(
    nrow(figure$timing_categories |> filter(.data$timing_basis == "imprecise_pre_2016")) == 1L,
    "Pre-2016 evidence is not represented separately."
  )
  list(
    check = tibble(
      check = "commission_figure_denominators",
      status = "passed",
      detail = paste0(
        figure$diagnostics$n_canonical_sites, " canonical sites; ",
        figure$diagnostics$n_resolved, " resolved timing histories; ",
        figure$diagnostics$n_pre_2016, " pre-2016 histories"
      )
    ),
    completeness = figure$completeness,
    diagnostics = figure$diagnostics
  )
}

check_property_lookup_schema <- function() {
  paths <- c(
    here::here("data", "processed", "spill_house_lookup.parquet"),
    here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
  )
  present <- paths[file.exists(paths)]
  if (length(present) == 0L) {
    return(tibble(check = "property_lookup_schema", status = "pending_publication", detail = "No property lookup artifacts found"))
  }
  blockers <- character()
  for (path in present) {
    columns <- names(arrow::open_dataset(path)$schema)
    if (!"n_site_groups" %in% columns) {
      blockers <- c(blockers, paste0(path, " is missing n_site_groups"))
    }
    if ("n_discharge_outlet" %in% columns) {
      blockers <- c(blockers, paste0(path, " retains n_discharge_outlet"))
    }
  }
  if (length(blockers) > 0L) {
    return(tibble(
      check = "property_lookup_schema",
      status = "pending_publication",
      detail = paste(blockers, collapse = "; ")
    ))
  }
  tibble(
    check = "property_lookup_schema",
    status = "passed",
    detail = paste0(length(present), " lookup schema(s) use n_site_groups")
  )
}

audit_stale_contracts <- function(root = here::here()) {
  candidates <- list.files(
    root,
    pattern = "\\.(R|Rmd|qmd|md)$",
    recursive = TRUE,
    full.names = TRUE
  )
  relative <- sub(paste0("^", root, "/?"), "", candidates)
  excluded <- grepl(
    paste0(
      "(^|/)(docs/plans|docs/solutions|docs/meetings|docs/wayfinder|docs/ideas|",
      "todos/_archive|output|book/_freeze|residual-review-findings)(/|$)"
    ),
    relative
  )
  candidates <- candidates[!excluded]
  relative <- relative[!excluded]
  patterns <- c(
    legacy_crosswalk_path = "site_works_crosswalk\\.parquet",
    legacy_property_count = "n_discharge_outlet",
    removed_commission_seam = "summarise_site_metadata",
    obsolete_glossary_heading = "^### Works( Register)?$",
    obsolete_grain_prose = "\\bWorks[- ](grain|year|register|crosswalk)\\b"
  )
  allowed <- tribble(
    ~path, ~pattern, ~reason,
    "scripts/R/testing/reconcile_site_grain_migration.R", "legacy_crosswalk_path", "Retained stale-contract detector",
    "scripts/R/testing/reconcile_site_grain_migration.R", "legacy_property_count", "Retained stale-contract detector",
    "scripts/R/testing/reconcile_site_grain_migration.R", "removed_commission_seam", "Retained stale-contract detector",
    "scripts/R/testing/reconcile_site_group_consumers.R", "legacy_crosswalk_path", "Manifest discovery detects unreviewed legacy readers",
    "scripts/R/testing/test_merge_outputs_contracts.R", "legacy_crosswalk_path", "Negative assertion proves the alias is absent",
    "scripts/R/testing/test_site_group_consumer_contracts.R", "legacy_property_count", "Negative assertions prove the old count is absent",
    "todos/2026-07-06-review-create-unique-spill-sites.md", "obsolete_grain_prose", "Historical defect rationale retained below resolved status"
  )

  findings <- bind_rows(lapply(seq_along(candidates), function(index) {
    lines <- readLines(candidates[index], warn = FALSE)
    bind_rows(lapply(names(patterns), function(pattern_name) {
      hits <- which(grepl(patterns[[pattern_name]], lines, ignore.case = TRUE, perl = TRUE))
      if (length(hits) == 0L) return(NULL)
      tibble(
        path = relative[index],
        line = hits,
        pattern = pattern_name,
        text = trimws(lines[hits])
      )
    }))
  }))
  if (nrow(findings) == 0L) {
    findings <- tibble(
      path = character(), line = integer(), pattern = character(), text = character()
    )
  }
  classified <- findings |>
    left_join(allowed, by = c("path", "pattern")) |>
    mutate(exempt = !is.na(.data$reason))
  blockers <- classified |>
    filter(!.data$exempt)
  assert_true(
    nrow(blockers) == 0L,
    paste0(
      "Stale live grain contract(s): ",
      paste0(blockers$path, ":", blockers$line, " [", blockers$pattern, "]", collapse = ", ")
    )
  )
  list(
    check = tibble(
      check = "stale_contract_scan",
      status = "passed",
      detail = paste0(nrow(classified), " justified historical/negative detector occurrence(s); zero live blockers")
    ),
    findings = classified
  )
}

check_freshness <- function(candidate_path, producer_paths) {
  candidate_time <- file.info(candidate_path)$mtime
  existing <- producer_paths[file.exists(producer_paths)]
  stale <- existing[file.info(existing)$mtime > candidate_time]
  assert_true(
    length(stale) == 0L,
    paste0("Canonical candidate is older than producer/input(s): ", paste(stale, collapse = ", "))
  )
  tibble(
    check = "canonical_candidate_freshness",
    status = "passed",
    detail = paste0("Candidate is newer than ", length(existing), " producer/input files")
  )
}

main <- function() {
  allow_pending_publication <- tolower(path_from_env(
    "SITE_GRAIN_ALLOW_PENDING_PUBLICATION",
    "false"
  )) %in% c("1", "true", "yes")
  candidate_path <- path_from_env(
    "SITE_GRAIN_NEW_UNIQUE",
    path_from_env("UNIQUE_SPILL_SITES_NEW", here::here("data", "processed", "unique_spill_sites.parquet"))
  )
  baseline_path <- path_from_env(
    "SITE_GRAIN_BASELINE_UNIQUE",
    path_from_env("UNIQUE_SPILL_SITES_BASELINE")
  )
  evidence_dir <- path_from_env(
    "SITE_GRAIN_EVIDENCE_DIR",
    file.path(tempdir(), "site_grain_migration_evidence")
  )
  lookup_path <- here::here("data", "processed", "annual_return_lookup.parquet")
  annual_path <- here::here("data", "processed", "annual_return_edm.parquet")
  crosswalk_path <- here::here(
    "data", "processed", "matched_events_annual_data", "site_group_crosswalk.parquet"
  )
  required_paths <- c(candidate_path, lookup_path, annual_path, crosswalk_path)
  missing <- required_paths[!file.exists(required_paths)]
  assert_true(length(missing) == 0L, paste0("Missing migration input(s): ", paste(missing, collapse = ", ")))
  assert_true(
    !file.exists(here::here(
      "data", "processed", "matched_events_annual_data", "site_works_crosswalk.parquet"
    )),
    "The legacy Works-named crosswalk still exists beside the Site Group artifact."
  )

  candidate <- read_parquet(candidate_path)
  lookup <- read_parquet(lookup_path)
  annual <- read_parquet(annual_path)
  crosswalk <- read_parquet(crosswalk_path)
  baseline <- if (nzchar(baseline_path)) read_parquet(baseline_path) else NULL

  canonical <- reconcile_canonical_contract(candidate, lookup, crosswalk, annual)
  figure <- reconcile_figure_denominators(candidate)
  stale_contracts <- audit_stale_contracts()
  checks <- bind_rows(
    tibble(
      check = c("lookup_coverage", "membership_exactly_once", "commission_contract", "availability_row_presence"),
      status = "passed",
      detail = c(
        paste0(nrow(candidate), " canonical IDs"),
        paste0(nrow(canonical$membership), " canonical memberships in ", n_distinct(candidate$site_id), " Site Groups"),
        "Closed status/precision/date vocabulary; future-only dates absent",
        paste0(nrow(canonical$availability), " canonical site-year cells")
      )
    ),
    reconcile_legacy_inventory(baseline, candidate),
    reconcile_event_totals(
      path_from_env("SITE_GRAIN_BASELINE_EVENTS"),
      path_from_env(
        "SITE_GRAIN_NEW_EVENTS",
        here::here("data", "processed", "matched_events_annual_data", "matched_events_annual_data.parquet")
      )
    ),
    reconcile_aggregate_outputs(
      path_from_env("SITE_GRAIN_BASELINE_AGG_DIR"),
      path_from_env(
        "SITE_GRAIN_NEW_AGG_DIR",
        here::here("data", "processed", "agg_spill_stats")
      )
    ),
    reconcile_consumer_manifest(),
    stale_contracts$check,
    figure$check,
    check_property_lookup_schema(),
    check_freshness(
      candidate_path,
      c(
        here::here("scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"),
        here::here("scripts", "R", "utils", "edm_commission_utils.R"),
        lookup_path,
        annual_path,
        crosswalk_path
      )
    ),
    tibble(
      check = "production_canonical_publication",
      status = if (normalizePath(candidate_path, mustWork = TRUE) == normalizePath(
        here::here("data", "processed", "unique_spill_sites.parquet"),
        mustWork = FALSE
      )) "passed" else "pending_publication",
      detail = if (normalizePath(candidate_path, mustWork = TRUE) == normalizePath(
        here::here("data", "processed", "unique_spill_sites.parquet"),
        mustWork = FALSE
      )) "Production canonical artifact was validated" else
        "Temporary canonical candidate validated; coordinated production publication remains pending"
    )
  )

  dir.create(evidence_dir, recursive = TRUE, showWarnings = FALSE)
  write_csv(checks, file.path(evidence_dir, "migration_checks.csv"))
  write_csv(
    candidate |>
      count(.data$edm_commission_resolution_status, .data$edm_commission_date_precision),
    file.path(evidence_dir, "commission_status_counts.csv")
  )
  write_csv(figure$completeness, file.path(evidence_dir, "commission_figure_completeness.csv"))
  write_csv(figure$diagnostics, file.path(evidence_dir, "commission_figure_denominators.csv"))
  write_csv(stale_contracts$findings, file.path(evidence_dir, "stale_contract_exemptions.csv"))
  writeLines(
    c(
      "# Canonical Spill Site grain migration reconciliation",
      "",
      paste0("Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
      "",
      paste0("- Canonical candidate: `", candidate_path, "`"),
      paste0("- Canonical rows: ", nrow(candidate)),
      paste0("- Site Groups: ", n_distinct(candidate$site_id)),
      paste0("- Crosswalk rows: ", nrow(crosswalk)),
      "",
      "All checks with supplied evidence passed. Checks marked",
      "`pending_publication` require the coordinated production publication or",
      "a supplied pre-migration baseline before the strict gate can pass."
    ),
    file.path(evidence_dir, "reconciliation.md")
  )
  print(checks, n = Inf)
  pending <- checks |>
    filter(.data$status != "passed")
  if (nrow(pending) > 0L && !allow_pending_publication) {
    stop(
      "Migration reconciliation has pending publication/baseline gate(s): ",
      paste(pending$check, collapse = ", "),
      call. = FALSE
    )
  }
  if (nrow(pending) > 0L) {
    cat("Canonical Spill Site / Site Group pre-publication checks passed; publication gates remain pending.\n")
  } else {
    cat("Canonical Spill Site / Site Group migration reconciliation passed.\n")
  }
}

if (sys.nframe() == 0L) main()
