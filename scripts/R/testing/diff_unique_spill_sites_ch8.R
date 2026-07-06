# ==============================================================================
# CH8 Unique Spill Sites Downstream Diff
# ==============================================================================
#
# Compares the old-pipeline unique_spill_sites baseline against the CH8 output at
# works grain. Writes a Markdown report and CSV detail files under the CH8 evidence
# directory. Runnable standalone via plain Rscript.
#
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
  library(glue)
  library(here)
  library(readr)
  library(stringr)
  library(tibble)
  library(tidyr)
})

evidence_dir <- here::here(
  "output",
  "merge_rebuild_downstream_migration_2026-07-06",
  "ch8"
)
baseline_path <- here::here(
  "output",
  "merge_rebuild_downstream_migration_2026-07-06",
  "baseline_old_pipeline",
  "unique_spill_sites.parquet"
)
new_path <- here::here("data", "processed", "unique_spill_sites.parquet")
crosswalk_path <- here::here(
  "data",
  "processed",
  "matched_events_annual_data",
  "site_works_crosswalk.parquet"
)

fs::dir_create(evidence_dir)

required_file <- function(path, label) {
  if (!file.exists(path)) {
    stop(glue("{label} not found: {path}"), call. = FALSE)
  }
}

required_file(baseline_path, "Old-pipeline baseline")
required_file(new_path, "CH8 unique_spill_sites output")
required_file(crosswalk_path, "Site works crosswalk")

availability_years <- 2021:2024
availability_cols <- paste0("available_year_", availability_years)

as_availability <- function(data) {
  data %>%
    mutate(across(all_of(availability_cols), ~ tidyr::replace_na(as.logical(.x), FALSE)))
}

old_sites <- arrow::read_parquet(baseline_path) %>%
  as_availability()
new_sites <- arrow::read_parquet(new_path) %>%
  as_availability()
crosswalk <- arrow::read_parquet(crosswalk_path)

member_map <- crosswalk %>%
  select(works_site_id = site_id, site_id_members) %>%
  distinct() %>%
  mutate(site_id_members = as.character(site_id_members)) %>%
  tidyr::separate_rows(site_id_members, sep = ";") %>%
  transmute(
    member_site_id = suppressWarnings(as.integer(site_id_members)),
    works_site_id = as.integer(works_site_id)
  ) %>%
  filter(!is.na(member_site_id), !is.na(works_site_id)) %>%
  distinct()

crosswalk_works <- crosswalk %>%
  distinct(works_site_id = as.integer(site_id))

old_inventory_availability <- old_sites %>%
  transmute(
    old_site_id = as.integer(site_id),
    old_any_available = if_any(
      all_of(availability_cols),
      ~ tidyr::replace_na(as.logical(.x), FALSE)
    )
  )

old_inventory <- old_sites %>%
  distinct(old_site_id = as.integer(site_id)) %>%
  left_join(member_map, by = c("old_site_id" = "member_site_id")) %>%
  left_join(old_inventory_availability, by = "old_site_id") %>%
  mutate(
    mapped_works_site_id = coalesce(works_site_id, old_site_id),
    inventory_explanation = case_when(
      old_site_id %in% new_sites$site_id ~ "retained_as_works_id",
      !is.na(works_site_id) &
        works_site_id != old_site_id &
        works_site_id %in% new_sites$site_id ~ "collapsed_member_into_works_representative",
      is.na(works_site_id) & !old_any_available ~
        "old_lookup_only_outside_rebuilt_crosswalk_no_availability",
      is.na(works_site_id) & old_any_available ~
        "old_lookup_only_with_availability_unexplained",
      TRUE ~ "old_inventory_unexplained"
    )
  )

old_only_inventory <- old_inventory %>%
  filter(!old_site_id %in% new_sites$site_id)

new_only_inventory <- new_sites %>%
  distinct(new_site_id = as.integer(site_id)) %>%
  anti_join(old_sites %>% distinct(old_site_id = as.integer(site_id)),
            by = c("new_site_id" = "old_site_id")) %>%
  mutate(
    inventory_explanation = case_when(
      new_site_id %in% crosswalk_works$works_site_id ~
        "new_crosswalk_works_id_not_present_in_old_unique_sites",
      TRUE ~ "new_inventory_unexplained"
    )
  )

inventory_details <- bind_rows(
  old_only_inventory %>%
    transmute(
      diff_side = "old_only",
      site_id = old_site_id,
      mapped_works_site_id,
      old_any_available,
      inventory_explanation
    ),
  new_only_inventory %>%
    transmute(
      diff_side = "new_only",
      site_id = new_site_id,
      mapped_works_site_id = new_site_id,
      old_any_available = NA,
      inventory_explanation
    )
) %>%
  arrange(diff_side, site_id)

readr::write_csv(
  inventory_details,
  fs::path(evidence_dir, "unique_spill_sites_inventory_diff_details.csv")
)

old_availability_works <- old_sites %>%
  select(old_site_id = site_id, all_of(availability_cols)) %>%
  pivot_longer(
    cols = all_of(availability_cols),
    names_to = "availability_col",
    values_to = "old_available"
  ) %>%
  mutate(
    old_site_id = as.integer(old_site_id),
    year = as.integer(readr::parse_number(availability_col)),
    old_available = tidyr::replace_na(as.logical(old_available), FALSE)
  ) %>%
  left_join(member_map, by = c("old_site_id" = "member_site_id")) %>%
  mutate(works_site_id = coalesce(works_site_id, old_site_id)) %>%
  group_by(works_site_id, year) %>%
  summarise(
    old_available = any(old_available),
    old_member_rows = n_distinct(old_site_id),
    .groups = "drop"
  )

new_availability <- new_sites %>%
  select(works_site_id = site_id, all_of(availability_cols), nlo_carryforward_year) %>%
  pivot_longer(
    cols = all_of(availability_cols),
    names_to = "availability_col",
    values_to = "new_available"
  ) %>%
  mutate(
    works_site_id = as.integer(works_site_id),
    year = as.integer(readr::parse_number(availability_col)),
    new_available = tidyr::replace_na(as.logical(new_available), FALSE),
    nlo_carryforward_year = as.integer(nlo_carryforward_year)
  ) %>%
  select(works_site_id, year, new_available, nlo_carryforward_year)

crosswalk_status <- crosswalk %>%
  mutate(
    works_site_id = as.integer(site_id),
    year = as.integer(year),
    annual_status = as.character(annual_status),
    has_positive_works_year = annual_status == "reported_positive" |
      (!is.na(spill_count_ea) & spill_count_ea > 0) |
      (!is.na(spill_hrs_ea) & spill_hrs_ea > 0)
  ) %>%
  group_by(works_site_id, year) %>%
  summarise(
    annual_status = paste(sort(unique(annual_status)), collapse = ";"),
    crosswalk_available = any(annual_status != "absent"),
    has_positive_works_year = any(has_positive_works_year),
    .groups = "drop"
  )

nlo_windows <- new_availability %>%
  filter(!is.na(nlo_carryforward_year)) %>%
  distinct(works_site_id, nlo_carryforward_year) %>%
  left_join(
    crosswalk_status %>%
      filter(has_positive_works_year) %>%
      select(works_site_id, positive_year = year),
    by = "works_site_id"
  ) %>%
  group_by(works_site_id, nlo_carryforward_year) %>%
  summarise(
    first_post_nlo_positive_year = {
      post_nlo_positive_years <- positive_year[
        !is.na(positive_year) & positive_year > nlo_carryforward_year
      ]
      if (length(post_nlo_positive_years) == 0) {
        NA_integer_
      } else {
        min(post_nlo_positive_years)
      }
    },
    .groups = "drop"
  )

availability_comparison <- full_join(
  old_availability_works,
  new_availability,
  by = c("works_site_id", "year")
) %>%
  full_join(crosswalk_status, by = c("works_site_id", "year")) %>%
  left_join(nlo_windows, by = c("works_site_id", "nlo_carryforward_year")) %>%
  mutate(
    old_available = tidyr::replace_na(old_available, FALSE),
    new_available = tidyr::replace_na(new_available, FALSE),
    crosswalk_available = tidyr::replace_na(crosswalk_available, FALSE),
    nlo_carryforward_expected = !is.na(nlo_carryforward_year) &
      year >= nlo_carryforward_year &
      (
        is.na(first_post_nlo_positive_year) |
          year < first_post_nlo_positive_year
      ),
    expected_new_available = crosswalk_available | nlo_carryforward_expected,
    availability_changed = old_available != new_available,
    availability_explanation = case_when(
      new_available != expected_new_available ~ "blocker_new_availability_not_crosswalk_or_nlo",
      !availability_changed ~ "unchanged",
      new_available & !old_available & annual_status == "reported_zero" ~
        "crosswalk_reported_zero_recovered",
      new_available & !old_available & annual_status == "reported_na" ~
        "crosswalk_reported_na_recovered",
      new_available & !old_available & annual_status == "reported_positive" ~
        "crosswalk_reported_positive_recovered",
      new_available & !old_available & nlo_carryforward_expected ~
        "nlo_carryforward_added_with_crosswalk_cap",
      old_available & !new_available & annual_status == "absent" ~
        "crosswalk_absent_not_available_after_rebuild",
      TRUE ~ "unexplained_availability_change"
    )
  )

availability_changes <- availability_comparison %>%
  filter(availability_changed | str_starts(availability_explanation, "blocker")) %>%
  arrange(works_site_id, year)

readr::write_csv(
  availability_changes,
  fs::path(evidence_dir, "unique_spill_sites_availability_diff_details.csv")
)

inventory_explanation_counts <- inventory_details %>%
  count(diff_side, inventory_explanation, name = "n") %>%
  arrange(diff_side, inventory_explanation)

availability_explanation_counts <- availability_comparison %>%
  count(availability_explanation, annual_status, name = "n") %>%
  arrange(availability_explanation, annual_status)

readr::write_csv(
  inventory_explanation_counts,
  fs::path(evidence_dir, "unique_spill_sites_inventory_explanation_counts.csv")
)
readr::write_csv(
  availability_explanation_counts,
  fs::path(evidence_dir, "unique_spill_sites_availability_explanation_counts.csv")
)

reported_zero_blockers <- availability_comparison %>%
  filter(annual_status == "reported_zero", !new_available)

inventory_blockers <- inventory_details %>%
  filter(
    inventory_explanation %in% c(
      "old_inventory_unexplained",
      "new_inventory_unexplained",
      "old_lookup_only_with_availability_unexplained"
    )
  )
availability_blockers <- availability_comparison %>%
  filter(
    availability_explanation %in% c(
      "blocker_new_availability_not_crosswalk_or_nlo",
      "unexplained_availability_change"
    )
  )

summary_counts <- tibble(
  metric = c(
    "old_rows",
    "old_distinct_site_ids",
    "new_rows",
    "new_distinct_works_site_ids",
    "crosswalk_distinct_works",
    "crosswalk_works_years",
    "crosswalk_member_site_ids",
    "old_only_site_ids",
    "new_only_site_ids",
    "old_lookup_only_with_old_availability",
    "availability_changed_works_years",
    "inventory_blockers",
    "availability_blockers",
    "reported_zero_unavailable_blockers"
  ),
  value = c(
    nrow(old_sites),
    n_distinct(old_sites$site_id),
    nrow(new_sites),
    n_distinct(new_sites$site_id),
    n_distinct(crosswalk$site_id),
    nrow(crosswalk),
    n_distinct(member_map$member_site_id),
    nrow(old_only_inventory),
    nrow(new_only_inventory),
    sum(
      inventory_details$inventory_explanation ==
        "old_lookup_only_with_availability_unexplained"
    ),
    nrow(availability_changes),
    nrow(inventory_blockers),
    nrow(availability_blockers),
    nrow(reported_zero_blockers)
  )
)

readr::write_csv(summary_counts, fs::path(evidence_dir, "unique_spill_sites_summary_counts.csv"))

format_counts <- function(data) {
  if (nrow(data) == 0) {
    return("- None")
  }
  paste0(
    "- ",
    apply(data, 1, function(row) paste(names(row), row, sep = "=", collapse = ", ")),
    collapse = "\n"
  )
}

crosswalk_status_counts <- crosswalk %>%
  count(annual_status, name = "n") %>%
  arrange(annual_status)

report_lines <- c(
  "# CH8 unique_spill_sites downstream diff",
  "",
  glue("Generated: {Sys.time()}"),
  "",
  "## Scope",
  "",
  "This report compares the old-pipeline `unique_spill_sites.parquet` baseline against the CH8 rebuilt output. The old output is a lookup-era site inventory; the new output is deliberately works-grain, keyed to `site_works_crosswalk.parquet`.",
  "",
  "## Inventory",
  "",
  glue("- Old baseline rows: {nrow(old_sites)}"),
  glue("- Old baseline distinct site_ids: {n_distinct(old_sites$site_id)}"),
  glue("- New CH8 rows: {nrow(new_sites)}"),
  glue("- New CH8 distinct works site_ids: {n_distinct(new_sites$site_id)}"),
  glue("- Crosswalk works: {n_distinct(crosswalk$site_id)}"),
  glue("- Crosswalk works-years: {nrow(crosswalk)}"),
  glue("- Crosswalk member site_ids: {n_distinct(member_map$member_site_id)}"),
  glue(
    "- Old lookup-only rows outside crosswalk with old availability: {sum(inventory_details$inventory_explanation == 'old_lookup_only_with_availability_unexplained')}"
  ),
  "",
  "### Inventory explanations",
  "",
  format_counts(inventory_explanation_counts),
  "",
  "## Crosswalk Status Counts",
  "",
  format_counts(crosswalk_status_counts),
  "",
  "## Availability",
  "",
  glue("- Availability changed works-years: {nrow(availability_changes)}"),
  glue("- Reported-zero unavailable blockers: {nrow(reported_zero_blockers)}"),
  "",
  "### Availability explanations",
  "",
  format_counts(availability_explanation_counts),
  "",
  "## Blockers",
  "",
  glue("- Inventory blockers: {nrow(inventory_blockers)}"),
  glue("- Availability blockers: {nrow(availability_blockers)}"),
  glue("- Reported-zero unavailable blockers: {nrow(reported_zero_blockers)}"),
  "",
  "Detail files:",
  "",
  "- `unique_spill_sites_inventory_diff_details.csv`",
  "- `unique_spill_sites_availability_diff_details.csv`",
  "- `unique_spill_sites_inventory_explanation_counts.csv`",
  "- `unique_spill_sites_availability_explanation_counts.csv`",
  "- `unique_spill_sites_summary_counts.csv`"
)

report_path <- fs::path(evidence_dir, "unique_spill_sites_ch8_diff_report.md")
writeLines(report_lines, report_path)

if (
  nrow(inventory_blockers) > 0 ||
    nrow(availability_blockers) > 0 ||
    nrow(reported_zero_blockers) > 0
) {
  stop(
    glue(
      "CH8 diff has unexplained blockers; see {report_path}"
    ),
    call. = FALSE
  )
}

cat(glue("CH8 unique_spill_sites diff passed. Report: {report_path}\n"))
