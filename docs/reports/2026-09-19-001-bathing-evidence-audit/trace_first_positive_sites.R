.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(arrow)
library(dplyr)
library(tidyr)
library(readr)

data_root <- "/Users/jacopoolivieri/projects/sewage/data/processed"
output_dir <- "/private/tmp/salience-bathing-audit"
source_paths <- file.path(data_root, c("annual_return_edm.parquet", "annual_return_lookup.parquet", "unique_spill_sites.parquet"))
write_csv(tibble(path = source_paths, resolved_path = normalizePath(source_paths), bytes = as.numeric(file.info(source_paths)$size), modified = as.character(file.info(source_paths)$mtime), md5 = unname(tools::md5sum(source_paths))), file.path(output_dir, "trace_source_metadata.csv"))
annual <- read_parquet(source_paths[[1L]], col_select = c("year", "water_company", "site_name_ea", "site_name_wa_sc", "permit_reference_ea", "permit_reference_wa_sc", "receiving_water_name", "bathing_water", "site_id_2021", "site_id_2022", "site_id_2023", "site_id_2024"))
lookup <- read_parquet(source_paths[[2L]]) |>
  rename(site_id_canonical = site_id) |>
  select(-component) |>
  pivot_longer(starts_with("site_id_20"), names_to = "year", names_prefix = "site_id_", values_to = "annual_site_id") |>
  mutate(year = as.integer(year)) |>
  filter(!is.na(annual_site_id))
members <- read_parquet(source_paths[[3L]], col_select = c("site_id_canonical", "site_id"))
first_2024 <- read_csv(file.path(output_dir, "site_bathing_first_positive_2024.csv"), show_col_types = FALSE)
annual <- annual |>
  mutate(annual_site_id = case_when(year == 2021L ~ site_id_2021, year == 2022L ~ site_id_2022, year == 2023L ~ site_id_2023, year == 2024L ~ site_id_2024)) |>
  left_join(lookup, by = c("year", "annual_site_id"), relationship = "many-to-one") |>
  left_join(members, by = "site_id_canonical", relationship = "many-to-one")
stopifnot(!anyNA(annual$site_id))
trace <- annual |>
  filter(site_id %in% first_2024$site_id) |>
  select(site_id, site_id_canonical, annual_site_id, year, water_company, site_name_ea, site_name_wa_sc, permit_reference_ea, permit_reference_wa_sc, receiving_water_name, bathing_water) |>
  mutate(raw_is_na = is.na(bathing_water), normalized = tolower(trimws(coalesce(bathing_water, "")))) |>
  arrange(site_id, year, annual_site_id)
write_csv(trace, file.path(output_dir, "first_positive_2024_raw_history.csv"))
review_names <- trace |>
  filter(year == 2024L, !normalized %in% c("", "0", "no", "not applicable", "tbc", "to be confirmed", "unknown")) |>
  distinct(site_id, water_company, bathing_water, receiving_water_name) |>
  left_join(select(first_2024, site_id, distance_to_coast_m), by = "site_id", relationship = "many-to-one")
write_csv(review_names, file.path(output_dir, "first_positive_2024_named_waters.csv"))
write_csv(count(review_names, water_company, bathing_water, sort = TRUE, name = "n_site_groups"), file.path(output_dir, "first_positive_2024_named_water_counts.csv"))
raw_history_summary <- trace |>
  group_by(year) |>
  summarise(n_rows = n(), n_site_groups = n_distinct(site_id), n_raw_na = sum(raw_is_na), n_explicit_negative = sum(!raw_is_na & normalized %in% c("", "0", "no", "not applicable")), .groups = "drop")
write_csv(raw_history_summary, file.path(output_dir, "first_positive_2024_raw_history_summary.csv"))
print(raw_history_summary)
print(count(review_names, water_company, bathing_water, sort = TRUE, name = "n_site_groups"), n = Inf, width = Inf)
