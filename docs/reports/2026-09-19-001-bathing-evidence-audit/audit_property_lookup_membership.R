.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(arrow)
library(dplyr)
library(readr)

data_root <- "/Users/jacopoolivieri/projects/sewage/data/processed"
output_dir <- "/private/tmp/salience-bathing-audit"
history <- read_csv(file.path(output_dir, "site_bathing_histories.csv"), show_col_types = FALSE)
ever_ids <- as.integer(history$site_id[history$bath_ever_2124])
first_2024_ids <- as.integer(history$site_id[history$first_positive_2024])
specs <- list(
  rentals = list(id = "rental_id", path = file.path(data_root, "zoopla/spill_rental_lookup.parquet")),
  sales = list(id = "house_id", path = file.path(data_root, "spill_house_lookup.parquet"))
)

summaries <- lapply(names(specs), function(market) {
  spec <- specs[[market]]
  cat(format(Sys.time()), "Start", market, "lookup scan\n")
  lookup <- open_dataset(spec$path) |>
    select(all_of(c(spec$id, "site_id", "distance_m"))) |>
    filter(!is.na(distance_m), distance_m <= 2000)
  bathing_pairs <- lookup |>
    filter(site_id %in% ever_ids) |>
    collect()
  candidate_keys <- bathing_pairs |>
    distinct(.data[[spec$id]])
  candidate_table <- arrow::Table$create(candidate_keys)
  relevant_pairs <- lookup |>
    inner_join(candidate_table, by = spec$id)
  min_distances <- relevant_pairs |>
    group_by(.data[[spec$id]]) |>
    summarise(distance_m = min(distance_m)) |>
    collect()
  min_table <- arrow::Table$create(min_distances)
  nearest <- relevant_pairs |>
    inner_join(min_table, by = c(spec$id, "distance_m")) |>
    group_by(.data[[spec$id]], distance_m) |>
    summarise(site_id = min(site_id)) |>
    collect() |>
    mutate(
      ever_bathing = site_id %in% ever_ids,
      first_positive_2024 = site_id %in% first_2024_ids,
      distance_band = case_when(distance_m <= 250 ~ "0_250", distance_m <= 500 ~ "250_500", distance_m <= 1000 ~ "500_1000", TRUE ~ "1000_2000")
    )
  nearest_summary <- nearest |>
    filter(ever_bathing) |>
    group_by(distance_band) |>
    summarise(n_current_bathing_keys = n(), n_keys_first_positive_2024_only = sum(first_positive_2024), .groups = "drop") |>
    mutate(market = market, classification = "nearest", .before = 1L)
  radius <- bathing_pairs |>
    filter(distance_m <= 250) |>
    group_by(.data[[spec$id]]) |>
    summarise(any_first_2024 = any(site_id %in% first_2024_ids), any_positive_before_2024 = any(!site_id %in% first_2024_ids), .groups = "drop") |>
    mutate(first_positive_2024_only = any_first_2024 & !any_positive_before_2024)
  radius_summary <- radius |>
    summarise(n_current_bathing_keys = n(), n_keys_first_positive_2024_only = sum(first_positive_2024_only)) |>
    mutate(market = market, classification = "any_within_250m", distance_band = "0_250", .before = 1L)
  write_csv(filter(nearest, ever_bathing), file.path(output_dir, paste0(market, "_nearest_bathing_lookup_keys.csv")))
  write_csv(radius, file.path(output_dir, paste0(market, "_radius250_bathing_lookup_keys.csv")))
  summary <- bind_rows(nearest_summary, radius_summary)
  write_csv(summary, file.path(output_dir, paste0(market, "_lookup_bathing_summary.csv")))
  print(summary, width = Inf)
  cat(format(Sys.time()), "Finished", market, "lookup scan\n")
  summary
})
write_csv(bind_rows(summaries), file.path(output_dir, "property_lookup_bathing_summary.csv"))
paths <- vapply(specs, function(x) x$path, character(1))
write_csv(tibble(market = names(paths), path = unname(paths), resolved_path = normalizePath(paths), bytes = as.numeric(file.info(paths)$size), modified = as.character(file.info(paths)$mtime)), file.path(output_dir, "property_lookup_source_metadata.csv"))
writeLines(c(
  "Counts are distinct identifiers in the existing property-Site Group lookups, not final regression samples.",
  "No transaction-year, price, covariate, London or estimator-removal restrictions have been applied.",
  "Nearest includes exact minimum distance ties resolved using the lowest numeric Site Group ID, matching the current helper.",
  "Radius membership changes only when all bathing-positive sites within 250 m have their first observed positive status in 2024.",
  "Removing 2024-only evidence is a diagnostic counterfactual, not an implemented or recommended reclassification.",
  "First observed positive year does not establish the official designation date.",
  "No models, builders or published artifacts were changed."
), file.path(output_dir, "property_lookup_scope.txt"))
