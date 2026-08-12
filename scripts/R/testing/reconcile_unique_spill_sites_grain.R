# Reconcile unique_spill_sites Canonical-Grain Migration
# Focused U3 evidence only. U7 owns downstream/end-to-end reconciliation.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
  library(readr)
  library(tidyr)
})
path_from_env <- function(name, fallback) {
  value <- Sys.getenv(name, unset = "")
  if (nzchar(value)) value else fallback
}

baseline_path <- path_from_env(
  "UNIQUE_SPILL_SITES_BASELINE",
  here::here(
    "output", "canonical_site_grain_migration", "baseline",
    "unique_spill_sites.parquet"
  )
)
new_path <- path_from_env(
  "UNIQUE_SPILL_SITES_NEW",
  here::here("data", "processed", "unique_spill_sites.parquet")
)
lookup_path <- here::here("data", "processed", "annual_return_lookup.parquet")
annual_path <- here::here("data", "processed", "annual_return_edm.parquet")
crosswalk_path <- here::here(
  "data", "processed", "matched_events_annual_data",
  "site_group_crosswalk.parquet"
)
evidence_dir <- path_from_env(
  "UNIQUE_SPILL_SITES_EVIDENCE_DIR",
  here::here("output", "canonical_site_grain_migration", "u3")
)
required_files <- c(baseline_path, new_path, lookup_path, annual_path, crosswalk_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0L) {
  stop("Missing reconciliation inputs: ", paste(missing_files, collapse = ", "))
}
dir.create(evidence_dir, recursive = TRUE, showWarnings = FALSE)
builder <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_unique_spill_sites.R"),
  local = builder
)
old <- arrow::read_parquet(baseline_path)
new <- arrow::read_parquet(new_path)
lookup <- arrow::read_parquet(lookup_path)
annual <- arrow::read_parquet(annual_path)
crosswalk <- arrow::read_parquet(crosswalk_path)
years <- builder$CONFIG$years
required_new <- c(
  "site_id", "site_id_canonical", "water_company", "ngr",
  paste0("available_year_", years), "no_longer_operational_year",
  "easting", "northing", "edm_commission_date",
  "edm_commission_date_precision", "edm_commission_resolution_status",
  paste0("edm_operation_percent_", years),
  paste0("edm_operation_percent_conflict_", years),
  paste0("edm_operation_reason_", years),
  paste0("edm_operation_reason_conflict_", years)
)
missing_new <- setdiff(required_new, names(new))
if (length(missing_new) > 0L) {
  stop("New canonical output missing columns: ", paste(missing_new, collapse = ", "))
}
membership <- builder$build_canonical_membership(crosswalk, lookup)
lookup_ids <- sort(as.integer(lookup$site_id))
if (!identical(sort(as.integer(new$site_id_canonical)), lookup_ids) ||
    anyDuplicated(new$site_id_canonical)) {
  stop("New canonical IDs do not uniquely and exactly cover the lookup universe.")
}
if (!identical(
  new[c("site_id", "site_id_canonical")],
  new[c("site_id", "site_id_canonical")] |>
    arrange(site_id, site_id_canonical)
)) {
  stop("New output is not ordered by Site Group and canonical ID.")
}
builder$validate_commission_resolution(new)
mapped <- builder$map_annual_to_canonical_sites(annual, lookup, years)
row_presence <- mapped |>
  distinct(site_id_canonical, year) |>
  mutate(expected_available = TRUE) |>
  complete(
    site_id_canonical = lookup_ids,
    year = years,
    fill = list(expected_available = FALSE)
  )
old_availability <- old |>
  select(site_id, any_of(paste0("available_year_", years)),
         any_of("nlo_carryforward_year")) |>
  pivot_longer(
    starts_with("available_year_"),
    names_to = "availability_column",
    values_to = "old_available"
  ) |>
  mutate(year = as.integer(sub("available_year_", "", availability_column))) |>
  select(-availability_column)
if (!"nlo_carryforward_year" %in% names(old_availability)) {
  old_availability$nlo_carryforward_year <- NA_integer_
}

availability <- new |>
  select(site_id, site_id_canonical, starts_with("available_year_")) |>
  pivot_longer(
    starts_with("available_year_"),
    names_to = "availability_column",
    values_to = "new_available"
  ) |>
  mutate(year = as.integer(sub("available_year_", "", availability_column))) |>
  select(-availability_column) |>
  left_join(old_availability, by = c("site_id", "year")) |>
  left_join(row_presence, by = c("site_id_canonical", "year")) |>
  mutate(
    old_available = replace_na(as.logical(old_available), FALSE),
    new_available = replace_na(as.logical(new_available), FALSE),
    availability_explanation = case_when(
      new_available != expected_available ~ "blocker_not_canonical_row_presence",
      old_available == new_available ~ "unchanged",
      old_available & !new_available & !is.na(nlo_carryforward_year) ~
        "removed_nlo_carryforward",
      site_id != site_id_canonical ~ "restored_member_canonical_row_presence",
      TRUE ~ "canonical_row_presence_replaces_legacy_availability"
    )
  )
if (any(availability$availability_explanation == "blocker_not_canonical_row_presence")) {
  stop("New availability differs from canonical Annual Return row presence.")
}
old_group <- old |>
  distinct(site_id, .keep_all = TRUE) |>
  rename_with(~ paste0("old_", .x), -site_id)
detail <- new |>
  left_join(old_group, by = "site_id") |>
  mutate(
    grain_explanation = if_else(
      site_id == site_id_canonical,
      "group_representative_retained",
      "canonical_member_restored"
    ),
    company_changed = !coalesce(water_company == old_water_company, FALSE),
    location_changed = !coalesce(
      ngr == old_ngr & easting == old_easting & northing == old_northing,
      FALSE
    ),
    company_explanation = if_else(
      company_changed, "most_recent_nonmissing_canonical_company", "unchanged"
    ),
    location_explanation = if_else(
      location_changed, "most_recent_parseable_canonical_location", "unchanged"
    ),
    commission_explanation = "classified_canonical_commission_resolution"
  )
operation_conflict_columns <- c(
  paste0("edm_operation_percent_conflict_", years),
  paste0("edm_operation_reason_conflict_", years)
)
summary <- tibble(
  metric = c(
    "old_rows", "new_rows", "lookup_canonical_ids", "site_groups",
    "restored_member_rows", "company_changes", "location_changes",
    "availability_changes", "removed_nlo_carryforward_cells",
    "operation_conflict_cells", "resolved_commission_dates",
    "unresolved_commission_histories"
  ),
  value = c(
    nrow(old), nrow(new), length(lookup_ids), n_distinct(new$site_id),
    sum(new$site_id != new$site_id_canonical), sum(detail$company_changed),
    sum(detail$location_changed), sum(availability$old_available != availability$new_available),
    sum(availability$availability_explanation == "removed_nlo_carryforward"),
    sum(as.matrix(new[operation_conflict_columns])),
    sum(new$edm_commission_resolution_status == "resolved"),
    sum(new$edm_commission_resolution_status != "resolved")
  )
)
readr::write_csv(summary, file.path(evidence_dir, "summary.csv"))
readr::write_csv(availability, file.path(evidence_dir, "availability_details.csv"))
readr::write_csv(
  detail |>
    select(
      site_id, site_id_canonical, grain_explanation,
      company_changed, company_explanation,
      location_changed, location_explanation,
      edm_commission_resolution_status, commission_explanation
    ),
  file.path(evidence_dir, "canonical_metadata_details.csv")
)
readr::write_csv(
  new |> count(edm_commission_resolution_status, edm_commission_date_precision),
  file.path(evidence_dir, "commission_status_counts.csv")
)
cat("Canonical unique_spill_sites reconciliation passed.\n")
print(summary)
