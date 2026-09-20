.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(arrow)
library(dplyr)
library(readr)

repo_root <- "/Users/jacopoolivieri/.codex/worktrees/site-heterogeneity/sewage"
source_root <- "/Users/jacopoolivieri/projects/sewage"
data_root <- file.path(source_root, "data/processed")
output_dir <- "/private/tmp/salience-bathing-audit"

# Evaluate only the named pure data-preparation function, never script setup/main.
read_preparation_function <- function(relative_path, function_name) {
  parsed <- parse(file.path(repo_root, relative_path))
  selected <- Filter(function(expression) is.call(expression) && identical(expression[[1L]], as.name("<-")) && identical(expression[[2L]], as.name(function_name)), as.list(parsed))
  stopifnot(length(selected) == 1L)
  environment <- new.env(parent = globalenv())
  eval(selected[[1L]], envir = environment)
  environment[[function_name]]
}
hedonic_path <- "scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior_salience.R"
intensive_path <- "scripts/R/09_analysis/05_news/did_articles_prior_salience.R"
prepare_hedonic <- read_preparation_function(hedonic_path, "prepare_salience_hedonic")
prepare_intensive <- read_preparation_function(intensive_path, "prepare_salience_intensive")

nearest <- read_csv(file.path(output_dir, "sales_nearest_bathing_lookup_keys.csv"), show_col_types = FALSE, col_types = cols(house_id = col_character()))
radius <- read_csv(file.path(output_dir, "sales_radius250_bathing_lookup_keys.csv"), show_col_types = FALSE, col_types = cols(house_id = col_character()))
transaction_path <- file.path(data_root, "house_price.parquet")
cat(format(Sys.time()), "Reading sales transactions\n")
transactions <- read_parquet(transaction_path, col_select = c("house_id", "price", "region", "month_id", "lsoa", "latitude", "longitude", "property_type", "old_new", "duration"))
cat(format(Sys.time()), "Sales transactions loaded\n")
exposure_path <- file.path(data_root, "cross_section/sales/prior_to_sale/radius=250")
exposure <- open_dataset(exposure_path) |> mutate(radius = 250L)
articles_path <- file.path(data_root, "lexis_nexis/search1_monthly.parquet")
articles <- read_parquet(articles_path) |>
  filter(month_id >= 1L, month_id <= 48L) |>
  arrange(month_id) |>
  transmute(month_id, log_cumulative_articles = log(cumsum(article_count)))
stopifnot(nrow(articles) == 48L, !anyDuplicated(articles$month_id), all(articles$month_id == 1:48), all(is.finite(articles$log_cumulative_articles)))

baseline <- prepare_hedonic(transactions, exposure, "sales") |>
  inner_join(select(nearest, house_id, first_positive_2024), by = "house_id", relationship = "one-to-one") |>
  filter(!region %in% "London") |>
  left_join(select(transactions, house_id, month_id), by = "house_id", relationship = "one-to-one") |>
  mutate(sample = "hedonic_count_continuous_prior_salience_groups")
intensive <- prepare_intensive(transactions, exposure, articles, "sales", "log_cumulative_articles") |>
  inner_join(select(radius, house_id, first_positive_2024_only), by = "house_id", relationship = "one-to-one") |>
  rename(first_positive_2024 = first_positive_2024_only) |>
  filter(!region %in% "London")
extensive <- transactions |>
  filter(month_id >= 1L, month_id <= 48L) |>
  inner_join(nearest, by = "house_id", relationship = "one-to-one") |>
  filter((distance_m >= 0 & distance_m <= 500) | (distance_m > 1000 & distance_m <= 2000)) |>
  inner_join(articles, by = "month_id", relationship = "many-to-one") |>
  mutate(log_price = log(price), near_bin = as.integer(distance_m <= 500)) |>
  filter(is.finite(log_price), is.finite(log_cumulative_articles), if_all(all_of(c("lsoa", "month_id", "latitude", "longitude", "property_type", "old_new", "duration")), ~ !is.na(.x)), !region %in% "London")
all_samples <- bind_rows(
  baseline,
  mutate(intensive, sample = "did_articles_prior_salience_groups"),
  mutate(intensive, sample = "did_trends_prior_salience_groups"),
  mutate(extensive, sample = "did_articles_prior_extensive_salience_groups"),
  mutate(extensive, sample = "did_trends_prior_extensive_salience_groups")
)
sample_counts <- all_samples |>
  group_by(sample) |>
  summarise(n_bathing_preestimation = n(), n_first_positive_2024_only = sum(first_positive_2024), affected_share = mean(first_positive_2024), .groups = "drop")
saved_audits <- bind_rows(lapply(sample_counts$sample, function(sample_name) {
  read_csv(file.path(source_root, "output/logs", paste0(sample_name, "_cell_counts.csv")), show_col_types = FALSE) |>
    filter(market == "sales", group == "bathing") |>
    transmute(sample = sample_name, saved_n_estimation = n_estimation, saved_nobs = nobs, saved_n_removed_by_estimator = n_removed_by_estimator)
}))
sample_counts <- sample_counts |>
  left_join(saved_audits, by = "sample", relationship = "one-to-one") |>
  mutate(matches_saved_preestimation_count = n_bathing_preestimation == saved_n_estimation)
stopifnot(all(sample_counts$matches_saved_preestimation_count))
write_csv(sample_counts, file.path(output_dir, "sales_preestimation_bathing_summary.csv"))
final_sample_rows <- list()
final_sample_counts <- bind_rows(lapply(sample_counts$sample, function(sample_name) {
  model_path <- file.path(source_root, "output/regs", paste0(sample_name, ".rds"))
  model <- readRDS(model_path)$models$sales$bathing
  sample_data <- filter(all_samples, sample == sample_name)
  stopifnot(nrow(sample_data) == model$nobs_origin)
  keep <- rep(TRUE, nrow(sample_data))
  for (variable in names(model$fixef_removed)) {
    keep <- keep & !as.character(sample_data[[variable]]) %in% as.character(model$fixef_removed[[variable]])
  }
  stopifnot(sum(keep) == model$nobs)
  stopifnot(identical(names(model$obs_selection), "obsRemoved"), length(model$obs_selection$obsRemoved) == sum(!keep))
  final_sample_rows[[sample_name]] <<- sample_data[keep, ]
  tibble(sample = sample_name, n_bathing_final_fitted = sum(keep), n_first_positive_2024_only_final_fitted = sum(keep & sample_data$first_positive_2024), affected_share_final = mean(sample_data$first_positive_2024[keep]), n_removed_by_saved_fixed_effect_levels = sum(!keep), saved_model_nobs = model$nobs)
}))
write_csv(final_sample_counts, file.path(output_dir, "sales_final_fitted_bathing_summary.csv"))
write_csv(all_samples |> group_by(sample, month_id) |> summarise(n_bathing_preestimation = n(), n_first_positive_2024_only = sum(first_positive_2024), .groups = "drop"), file.path(output_dir, "sales_preestimation_bathing_by_month.csv"))
write_csv(extensive |> group_by(near_bin) |> summarise(n_bathing_preestimation = n(), n_first_positive_2024_only = sum(first_positive_2024), .groups = "drop"), file.path(output_dir, "sales_extensive_bathing_by_distance_bin.csv"))
year_counts <- bind_rows(final_sample_rows) |>
  mutate(year = 2021L + as.integer((month_id - 1L) %/% 12L)) |>
  group_by(sample, year) |>
  summarise(n_bathing_final_fitted = n(), n_first_positive_2024_only_final_fitted = sum(first_positive_2024), .groups = "drop")
write_csv(year_counts, file.path(output_dir, "sales_final_fitted_bathing_by_year.csv"))
period_counts <- bind_rows(final_sample_rows) |>
  mutate(transaction_period = if_else(month_id <= 36L, "2021_2023", "2024")) |>
  group_by(sample, transaction_period) |>
  summarise(n_bathing_final_fitted = n(), n_first_positive_2024_only_final_fitted = sum(first_positive_2024), affected_share = mean(first_positive_2024), .groups = "drop")
write_csv(period_counts, file.path(output_dir, "sales_final_fitted_bathing_by_period.csv"))
paths <- c(transaction_path, articles_path, list.files(exposure_path, full.names = TRUE), file.path(repo_root, c(hedonic_path, intensive_path)), file.path(source_root, "output/regs", paste0(sample_counts$sample, ".rds")))
write_csv(tibble(path = paths, resolved_path = normalizePath(paths), bytes = as.numeric(file.info(paths)$size), modified = as.character(file.info(paths)$mtime)), file.path(output_dir, "sales_preestimation_source_metadata.csv"))
writeLines(c(
  "Sales counts reproduce the current bathing sample before estimator exclusions; total counts match each saved audit exactly.",
  "Final fitted sales membership is reconstructed by removing the exact fixed-effect levels listed in each saved model's fixef_removed field. The retained counts match saved nobs exactly, and the number removed equals the full saved obsRemoved count. No positional row-order assumption is used.",
  "Baseline uses the exact parsed current prepare_salience_hedonic function; intensive uses prepare_salience_intensive. Script setup and main are never evaluated.",
  "Extensive sample restrictions were transcribed from the current scripts. Articles are finite in all 48 months; trends and articles consequently share sample membership.",
  "No regressions or builders were run. Removing first-positive-2024 evidence is only a membership diagnostic.",
  "Transaction years are derived from month_id 1=January2021; pre2024 and2024 samples are reported separately. First observed positive is not official designation date."
), file.path(output_dir, "sales_preestimation_scope.txt"))
print(sample_counts, width = Inf)
print(final_sample_counts, width = Inf)
print(period_counts, width = Inf)
