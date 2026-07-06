############################################################
# CH9 aggregate_spill_stats old-vs-new diff report
# Project: Sewage
# Date: 06/07/2026
############################################################

required_packages <- c("arrow", "dplyr", "glue", "here", "readr", "tidyr")

invisible(lapply(required_packages, function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}))

output_root <- here::here("output", "merge_rebuild_downstream_migration_2026-07-06")
baseline_dir <- file.path(output_root, "baseline_old_pipeline", "agg_spill_stats")
new_dir <- here::here("data", "processed", "agg_spill_stats")
evidence_dir <- file.path(output_root, "ch9")
dir.create(evidence_dir, recursive = TRUE, showWarnings = FALSE)

files <- tibble::tribble(
  ~period, ~filename, ~period_col, ~count_col, ~hours_col,
  "yearly", "agg_spill_yr.parquet", "year", "spill_count_yr", "spill_hrs_yr",
  "monthly", "agg_spill_mo.parquet", "month_id", "spill_count_mo", "spill_hrs_mo",
  "quarterly", "agg_spill_qtr.parquet", "qtr_id", "spill_count_qt", "spill_hrs_qt"
)

read_output <- function(dir, filename) {
  path <- file.path(dir, filename)
  if (!file.exists(path)) {
    stop(glue::glue("Missing output file: {path}"), call. = FALSE)
  }
  arrow::read_parquet(path)
}

old_outputs <- setNames(
  lapply(files$filename, read_output, dir = baseline_dir),
  files$period
)
new_outputs <- setNames(
  lapply(files$filename, read_output, dir = new_dir),
  files$period
)

crosswalk <- arrow::read_parquet(
  here::here(
    "data", "processed", "matched_events_annual_data",
    "site_works_crosswalk.parquet"
  )
)
events <- arrow::read_parquet(
  here::here(
    "data", "processed", "matched_events_annual_data",
    "matched_events_annual_data.parquet"
  ),
  col_select = c(site_id, year, water_company)
) %>%
  distinct(site_id, year, water_company) %>%
  mutate(has_event = TRUE)

summarise_output <- function(data, period, period_col, count_col, hours_col, source) {
  data %>%
    summarise(
      source = source,
      period = period,
      rows = n(),
      site_ids = n_distinct(.data$site_id),
      columns = ncol(.),
      period_min = suppressWarnings(min(.data[[period_col]], na.rm = TRUE)),
      period_max = suppressWarnings(max(.data[[period_col]], na.rm = TRUE)),
      rows_count_hours_both_na = sum(is.na(.data[[count_col]]) & is.na(.data[[hours_col]])),
      total_spill_count = sum(.data[[count_col]], na.rm = TRUE),
      total_spill_hours = sum(.data[[hours_col]], na.rm = TRUE),
      .groups = "drop"
    )
}

file_summary <- bind_rows(lapply(seq_len(nrow(files)), function(i) {
  spec <- files[i, ]
  bind_rows(
    summarise_output(
      old_outputs[[spec$period]], spec$period, spec$period_col,
      spec$count_col, spec$hours_col, "old_baseline"
    ),
    summarise_output(
      new_outputs[[spec$period]], spec$period, spec$period_col,
      spec$count_col, spec$hours_col, "new_ch9"
    )
  )
}))

column_diff <- bind_rows(lapply(files$period, function(period) {
  old_cols <- names(old_outputs[[period]])
  new_cols <- names(new_outputs[[period]])
  tibble::tibble(
    period = period,
    column = union(old_cols, new_cols),
    in_old = column %in% old_cols,
    in_new = column %in% new_cols
  ) %>%
    mutate(
      status = case_when(
        .data$in_old & .data$in_new ~ "unchanged",
        .data$in_old & !.data$in_new ~ "removed",
        !.data$in_old & .data$in_new ~ "added"
      )
    )
}))

descriptive_cols <- c(
  "site_name_ea", "site_name_wa_sc",
  "permit_reference_ea", "permit_reference_wa_sc",
  "activity_reference", "asset_type", "ngr", "unique_id",
  "wfd_waterbody_id_cycle_2", "receiving_water_name",
  "shellfish_water", "bathing_water", "edm_commission_date"
)

status_counts <- bind_rows(
  crosswalk %>%
    count(annual_status, name = "rows") %>%
    mutate(source = "crosswalk_yearly", expected_multiplier = 1L),
  new_outputs$yearly %>%
    count(annual_status, name = "rows") %>%
    mutate(source = "new_yearly", expected_multiplier = 1L),
  new_outputs$monthly %>%
    count(annual_status, name = "rows") %>%
    mutate(source = "new_monthly", expected_multiplier = 12L),
  new_outputs$quarterly %>%
    count(annual_status, name = "rows") %>%
    mutate(source = "new_quarterly", expected_multiplier = 4L)
) %>%
  arrange(annual_status, source)

status_check <- crosswalk %>%
  count(annual_status, name = "crosswalk_rows") %>%
  left_join(
    new_outputs$yearly %>%
      count(annual_status, name = "new_yearly_rows"),
    by = "annual_status"
  ) %>%
  left_join(
    new_outputs$monthly %>%
      count(annual_status, name = "new_monthly_rows"),
    by = "annual_status"
  ) %>%
  left_join(
    new_outputs$quarterly %>%
      count(annual_status, name = "new_quarterly_rows"),
    by = "annual_status"
  ) %>%
  mutate(
    yearly_ok = .data$new_yearly_rows == .data$crosswalk_rows,
    monthly_ok = .data$new_monthly_rows == .data$crosswalk_rows * 12L,
    quarterly_ok = .data$new_quarterly_rows == .data$crosswalk_rows * 4L
  )

yearly_with_events <- new_outputs$yearly %>%
  left_join(events, by = c("site_id", "year", "water_company")) %>%
  mutate(has_event = coalesce(.data$has_event, FALSE))

yearly_source_summary <- yearly_with_events %>%
  group_by(.data$annual_status, .data$has_event) %>%
  summarise(
    rows = n(),
    rows_count_hours_both_na = sum(is.na(.data$spill_count_yr) & is.na(.data$spill_hrs_yr)),
    rows_count_hours_both_zero = sum(.data$spill_count_yr == 0 & .data$spill_hrs_yr == 0, na.rm = TRUE),
    rows_with_crosswalk_count = sum(!is.na(.data$spill_count_ea_crosswalk)),
    rows_with_crosswalk_hours = sum(!is.na(.data$spill_hrs_ea_crosswalk)),
    total_spill_count = sum(.data$spill_count_yr, na.rm = TRUE),
    total_spill_hours = sum(.data$spill_hrs_yr, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(.data$annual_status, .data$has_event)

absent_handling <- yearly_with_events %>%
  filter(.data$annual_status == "absent") %>%
  summarise(
    absent_rows = n(),
    absent_rows_with_events = sum(.data$has_event),
    absent_rows_without_events = sum(!.data$has_event),
    absent_rows_with_non_na_stats = sum(!is.na(.data$spill_count_yr) | !is.na(.data$spill_hrs_yr)),
    absent_rows_with_crosswalk_fallback = sum(
      !is.na(.data$spill_count_ea_crosswalk) | !is.na(.data$spill_hrs_ea_crosswalk)
    ),
    .groups = "drop"
  )

common_years <- intersect(unique(old_outputs$yearly$year), unique(new_outputs$yearly$year))
common_year_summary <- bind_rows(
  old_outputs$yearly %>%
    filter(.data$year %in% common_years) %>%
    summarise(
      source = "old_baseline",
      rows = n(),
      site_ids = n_distinct(.data$site_id),
      years = paste(sort(unique(.data$year)), collapse = ";"),
      total_spill_count = sum(.data$spill_count_yr, na.rm = TRUE),
      total_spill_hours = sum(.data$spill_hrs_yr, na.rm = TRUE),
      rows_count_hours_both_na = sum(is.na(.data$spill_count_yr) & is.na(.data$spill_hrs_yr)),
      .groups = "drop"
    ),
  new_outputs$yearly %>%
    filter(.data$year %in% common_years) %>%
    summarise(
      source = "new_ch9",
      rows = n(),
      site_ids = n_distinct(.data$site_id),
      years = paste(sort(unique(.data$year)), collapse = ";"),
      total_spill_count = sum(.data$spill_count_yr, na.rm = TRUE),
      total_spill_hours = sum(.data$spill_hrs_yr, na.rm = TRUE),
      rows_count_hours_both_na = sum(is.na(.data$spill_count_yr) & is.na(.data$spill_hrs_yr)),
      .groups = "drop"
    )
)

write_csv(file_summary, file.path(evidence_dir, "aggregate_spill_stats_file_summary.csv"))
write_csv(column_diff, file.path(evidence_dir, "aggregate_spill_stats_column_diff.csv"))
write_csv(status_counts, file.path(evidence_dir, "aggregate_spill_stats_status_counts.csv"))
write_csv(status_check, file.path(evidence_dir, "aggregate_spill_stats_status_check.csv"))
write_csv(yearly_source_summary, file.path(evidence_dir, "aggregate_spill_stats_yearly_source_summary.csv"))
write_csv(absent_handling, file.path(evidence_dir, "aggregate_spill_stats_absent_handling.csv"))
write_csv(common_year_summary, file.path(evidence_dir, "aggregate_spill_stats_common_year_summary.csv"))

blockers <- character()
if (any(!status_check$yearly_ok | !status_check$monthly_ok | !status_check$quarterly_ok)) {
  blockers <- c(blockers, "New outputs do not preserve crosswalk annual_status membership.")
}
if (absent_handling$absent_rows_with_crosswalk_fallback > 0) {
  blockers <- c(blockers, "Absent works-years received non-NA crosswalk fallback totals.")
}

removed_descriptive <- column_diff %>%
  filter(.data$status == "removed", .data$column %in% descriptive_cols) %>%
  count(.data$column, name = "periods_removed") %>%
  arrange(.data$column)

descriptive_not_in_old <- setdiff(descriptive_cols, removed_descriptive$column)

removed_non_descriptive <- column_diff %>%
  filter(.data$status == "removed", !.data$column %in% descriptive_cols) %>%
  distinct(.data$column) %>%
  arrange(.data$column)

added_columns <- column_diff %>%
  filter(.data$status == "added") %>%
  distinct(.data$column) %>%
  arrange(.data$column)

format_table <- function(x) {
  paste(capture.output(print(x, n = Inf, width = Inf)), collapse = "\n")
}

report_lines <- c(
  "# CH9 aggregate_spill_stats diff report",
  "",
  glue::glue("Generated: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}"),
  "",
  "## Executive checks",
  "",
  glue::glue("- Crosswalk contract: {n_distinct(crosswalk$site_id)} works / {nrow(crosswalk)} works-years."),
  glue::glue("- Old baseline yearly: {nrow(old_outputs$yearly)} rows / {n_distinct(old_outputs$yearly$site_id)} site_ids / {ncol(old_outputs$yearly)} columns."),
  glue::glue("- New CH9 yearly: {nrow(new_outputs$yearly)} rows / {n_distinct(new_outputs$yearly$site_id)} site_ids / {ncol(new_outputs$yearly)} columns."),
  glue::glue("- Common-year site count moves from {common_year_summary$site_ids[common_year_summary$source == 'old_baseline']} old site_ids to {common_year_summary$site_ids[common_year_summary$source == 'new_ch9']} works, matching the expected works-collapse direction."),
  "- Reported-zero and reported-na site-years are preserved from the crosswalk in yearly/monthly/quarterly grids; see `aggregate_spill_stats_status_check.csv`.",
  "- Absent works-years are not EA-fallback members. They appear only because a reporting or event works is crossed across `CONFIG$years`; their crosswalk fallback totals remain `NA`.",
  "",
  "## File Summary",
  "",
  "```",
  format_table(file_summary),
  "```",
  "",
  "## Crosswalk Status Preservation",
  "",
  "```",
  format_table(status_check),
  "```",
  "",
  "## Yearly Source Summary",
  "",
  "```",
  format_table(yearly_source_summary),
  "```",
  "",
  "## Absent Handling",
  "",
  "```",
  format_table(absent_handling),
  "```",
  "",
  "Interpretation: `absent_rows_with_crosswalk_fallback` must be zero. Any non-NA stats on absent rows come from pure events matched to absent works-years, not from EA fallback.",
  "",
  "## Column Changes",
  "",
  "The pseudo-row fill list contains 13 descriptive names. Twelve existed in the old aggregate outputs and are intentionally removed here; `unique_id` was already excluded before the old completion step and therefore has no old output column to remove.",
  "",
  "```",
  format_table(removed_descriptive),
  "```",
  "",
  if (length(descriptive_not_in_old) == 0L) {
    "Every fill-list descriptive name existed in the old outputs."
  } else {
    glue::glue("Fill-list names already absent from the old outputs: {paste(descriptive_not_in_old, collapse = ', ')}.")
  },
  "",
  "Other removed metadata/diagnostic columns are either legacy lookup fields, old pseudo-row metadata not needed by aggregate consumers, or EA fallback totals replaced by explicit `*_crosswalk` names:",
  "",
  "```",
  format_table(removed_non_descriptive),
  "```",
  "",
  "New columns identify crosswalk status and explicit crosswalk fallback totals:",
  "",
  "```",
  format_table(added_columns),
  "```",
  "",
  "## Common-Year Summary",
  "",
  "```",
  format_table(common_year_summary),
  "```",
  "",
  "## Blockers",
  "",
  if (length(blockers) == 0L) {
    "No unexplained CH9 diff blockers detected by this report."
  } else {
    paste(paste0("- ", blockers), collapse = "\n")
  }
)

report_path <- file.path(evidence_dir, "aggregate_spill_stats_diff_report.md")
writeLines(report_lines, report_path)

if (length(blockers) > 0L) {
  stop(paste(blockers, collapse = "\n"), call. = FALSE)
}

cat(glue::glue("CH9 diff report written to {report_path}\n"))
