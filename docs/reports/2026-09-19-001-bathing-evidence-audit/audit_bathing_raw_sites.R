.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(arrow)
library(dplyr)
library(tidyr)
library(readr)

data_root <- "/Users/jacopoolivieri/projects/sewage/data"
output_dir <- "/private/tmp/salience-bathing-audit"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
source_paths <- c(
  annual = file.path(data_root, "processed/annual_return_edm.parquet"),
  sites = file.path(data_root, "processed/site_characteristics/site_group_characteristics.parquet")
)
source_metadata <- tibble(
  source = names(source_paths), path = unname(source_paths),
  resolved_path = normalizePath(source_paths),
  bytes = as.numeric(file.info(source_paths)$size),
  modified = as.character(file.info(source_paths)$mtime),
  md5 = unname(tools::md5sum(source_paths))
)
write_csv(source_metadata, file.path(output_dir, "raw_site_source_metadata.csv"))
writeLines(capture.output(sessionInfo()), file.path(output_dir, "raw_site_session_info.txt"))

annual <- read_parquet(source_paths[["annual"]], col_select = c("year", "bathing_water")) |>
  mutate(
    raw_is_na = is.na(bathing_water),
    normalized = tolower(trimws(coalesce(as.character(bathing_water), ""))),
    raw_value = if_else(raw_is_na, "<NA>", as.character(bathing_water)),
    parser_status = case_when(
      normalized %in% c("", "0", "no", "not applicable") ~ "not_designated",
      normalized %in% c("tbc", "to be confirmed", "unknown") ~ "unknown",
      TRUE ~ "designated"
    ),
    suspicious_positive = parser_status == "designated" &
      grepl("(^n/?a$|^none$|^nil$|^false$|^not\\b|^no[[:space:]]|unknown|confirm|n/a|not applicable|tbc|^0[.]0*$|^-$|^yes$|^true$|^1$)", normalized)
  )
raw_counts <- annual |>
  count(year, raw_value, raw_is_na, normalized, parser_status, suspicious_positive, name = "n_annual_rows") |>
  arrange(year, desc(n_annual_rows), raw_value)
write_csv(raw_counts, file.path(output_dir, "annual_bathing_values_by_year.csv"))
write_csv(filter(raw_counts, parser_status == "designated"), file.path(output_dir, "annual_positive_strings_by_year.csv"))
write_csv(filter(raw_counts, suspicious_positive), file.path(output_dir, "annual_positive_strings_for_review.csv"))
raw_summary <- annual |>
  group_by(year) |>
  summarise(
    n_rows = n(), n_positive = sum(parser_status == "designated"),
    n_negative = sum(parser_status == "not_designated"), n_unknown = sum(parser_status == "unknown"),
    n_na = sum(raw_is_na), n_blank_non_na = sum(!raw_is_na & normalized == ""),
    n_distinct_positive_strings = n_distinct(raw_value[parser_status == "designated"]),
    n_positive_flagged_for_review = sum(suspicious_positive), .groups = "drop"
  )
write_csv(raw_summary, file.path(output_dir, "annual_bathing_summary_by_year.csv"))

sites <- read_parquet(source_paths[["sites"]])
stopifnot(!anyDuplicated(sites$site_id))
status_long <- sites |>
  select(site_id, matches("^bath_(status|mixed|unknown)_[0-9]{2}$")) |>
  pivot_longer(-site_id, names_to = c(".value", "year"), names_pattern = "bath_(status|mixed|unknown)_([0-9]{2})") |>
  mutate(year = 2000L + as.integer(year))
status_counts <- status_long |>
  count(year, status, mixed, unknown, name = "n_site_groups") |>
  arrange(year, status, mixed, unknown)
write_csv(status_counts, file.path(output_dir, "site_bathing_status_by_year.csv"))

history <- status_long |>
  group_by(site_id) |>
  arrange(year, .by_group = TRUE) |>
  summarise(
    status_history = paste(status, collapse = " | "),
    first_observed_positive_year = if (any(status == "designated")) min(year[status == "designated"]) else NA_integer_,
    n_positive_years = sum(status == "designated"),
    n_negative_years = sum(status == "not_designated"),
    n_unknown_status_years = sum(status == "unknown"),
    any_unknown_evidence = any(unknown), any_mixed_evidence = any(mixed),
    prior_unknown_before_first_positive = if (any(status == "designated")) any(unknown & year < min(year[status == "designated"])) else NA,
    prior_observed_negative_before_first_positive = if (any(status == "designated")) any(status == "not_designated" & year < min(year[status == "designated"])) else NA,
    first_positive_2024 = any(year == 2024L & status == "designated") & !any(year < 2024L & status == "designated"),
    .groups = "drop"
  ) |>
  left_join(select(sites, site_id, distance_to_coast_m, bath_ever_2124, bath_changed_2124, bath_unknown_2124, bath_24), by = "site_id", relationship = "one-to-one")
stopifnot(all(history$bath_ever_2124 == (history$n_positive_years > 0L)))
stopifnot(all(history$bath_unknown_2124 == history$any_unknown_evidence))
write_csv(history, file.path(output_dir, "site_bathing_histories.csv"))
write_csv(filter(history, first_positive_2024), file.path(output_dir, "site_bathing_first_positive_2024.csv"))
first_positive_counts <- history |>
  count(first_observed_positive_year, prior_unknown_before_first_positive, prior_observed_negative_before_first_positive, any_unknown_evidence, any_mixed_evidence, name = "n_site_groups")
write_csv(first_positive_counts, file.path(output_dir, "site_bathing_first_positive_summary.csv"))
write_csv(count(history, status_history, name = "n_site_groups", sort = TRUE), file.path(output_dir, "site_bathing_history_patterns.csv"))
transitions <- status_long |>
  arrange(site_id, year) |>
  group_by(site_id) |>
  mutate(from_year = lag(year), from_status = lag(status), from_unknown = lag(unknown)) |>
  ungroup() |>
  filter(!is.na(from_year)) |>
  count(from_year, year, from_status, status, from_unknown, unknown, name = "n_site_groups")
write_csv(transitions, file.path(output_dir, "site_bathing_annual_transitions.csv"))
site_summary <- history |>
  summarise(
    n_sites = n(), n_ever_bathing = sum(bath_ever_2124),
    n_changed_positive_negative = sum(bath_changed_2124),
    n_any_unknown = sum(any_unknown_evidence),
    n_positive_with_unknown = sum(bath_ever_2124 & any_unknown_evidence),
    n_positive_with_mixed = sum(bath_ever_2124 & any_mixed_evidence),
    n_first_positive_2024 = sum(first_positive_2024),
    n_first_positive_2024_prior_unknown = sum(first_positive_2024 & prior_unknown_before_first_positive, na.rm = TRUE),
    n_first_positive_2024_prior_negative = sum(first_positive_2024 & prior_observed_negative_before_first_positive, na.rm = TRUE),
    n_first_positive_2024_all_prior_unknown = sum(first_positive_2024 & n_unknown_status_years == 3L),
    n_first_positive_2024_all_prior_negative = sum(first_positive_2024 & n_negative_years == 3L),
    n_first_positive_2024_coastal = sum(first_positive_2024 & distance_to_coast_m <= 2000, na.rm = TRUE),
    n_first_positive_2024_inland = sum(first_positive_2024 & distance_to_coast_m > 2000, na.rm = TRUE)
  )
write_csv(site_summary, file.path(output_dir, "site_bathing_summary.csv"))
writeLines(c(
  "Diagnostic only. No builders or regressions were run and no published artifacts were modified.",
  "Annual string classifications reproduce the current parser; suspicious_positive is a heuristic review flag, not an adjudicated error.",
  "First observed positive year is not the official designation date or bathing-season effective date.",
  "Annual status positive can coexist with unknown or mixed evidence across member records.",
  "Official designation and bathing-season crosschecks remain outstanding.",
  "All outputs describe the current saved data, identified by source metadata and MD5 hashes."
), file.path(output_dir, "raw_site_scope.txt"))
print(raw_summary, width = Inf)
print(site_summary, width = Inf)
print(filter(raw_counts, suspicious_positive), n = Inf, width = Inf)
