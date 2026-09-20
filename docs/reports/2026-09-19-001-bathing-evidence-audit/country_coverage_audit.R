.libPaths(c("/Users/jacopoolivieri/projects/sewage/rv/library/4.6/arm64", .libPaths()))
library(arrow)
library(dplyr)
library(readr)

repo_root <- here::here()
data_root <- "/Users/jacopoolivieri/projects/sewage/data/processed"
output_dir <- here::here("docs", "reports", "2026-09-19-001-bathing-evidence-audit")

read_preparation_function <- function(relative_path, function_name) {
  parsed <- parse(file.path(repo_root, relative_path))
  selected <- Filter(function(expression) is.call(expression) && identical(expression[[1L]], as.name("<-")) && identical(expression[[2L]], as.name(function_name)), as.list(parsed))
  stopifnot(length(selected) == 1L)
  environment <- new.env(parent = globalenv())
  eval(selected[[1L]], envir = environment)
  environment[[function_name]]
}
prepare_hedonic <- read_preparation_function("scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior_salience.R", "prepare_salience_hedonic")
prepare_intensive <- read_preparation_function("scripts/R/09_analysis/05_news/did_trends_prior_salience.R", "prepare_salience_intensive")
attention <- tibble(month_id = 1:48, post = as.integer(month_id >= 20L))
specs <- list(
  sales = list(id = "house_id", price = "price", controls = c("property_type", "old_new", "duration"), transactions = file.path(data_root, "house_price.parquet"), exposure = file.path(data_root, "cross_section/sales/prior_to_sale/radius=250")),
  rentals = list(id = "rental_id", price = "listing_price", controls = c("property_type", "bedrooms", "bathrooms"), transactions = file.path(data_root, "zoopla/zoopla_rentals.parquet"), exposure = file.path(data_root, "cross_section/rentals/prior_to_rental/radius=250"))
)

results <- lapply(names(specs), function(market) {
  spec <- specs[[market]]
  transactions <- read_parquet(spec$transactions, col_select = all_of(c(spec$id, spec$price, "country", "region", "month_id", "lsoa", "latitude", "longitude", spec$controls)))
  exposure <- open_dataset(spec$exposure) |> mutate(radius = 250L)
  baseline <- prepare_hedonic(transactions, exposure, market) |> mutate(sample = "baseline_hedonic")
  intensive <- prepare_intensive(transactions, exposure, attention, market, "post") |> mutate(sample = "intensive_attention")
  prepared <- bind_rows(baseline, intensive) |>
    left_join(select(transactions, all_of(c(spec$id, "country"))), by = spec$id, relationship = "many-to-one") |>
    mutate(market = market, london = region %in% "London", lsoa_is_ons_code = grepl("^[EWNS][0-9]{8}$", as.character(lsoa)), lsoa_country_prefix = if_else(lsoa_is_ons_code, substr(as.character(lsoa), 1L, 1L), "not_an_ONS_code"))
  counts <- prepared |>
    group_by(market, sample, country, region, lsoa_country_prefix) |>
    summarise(n_before_london_exclusion = n(), n_after_london_exclusion = sum(!london), .groups = "drop")
  lsoa_format <- prepared |>
    group_by(market, sample, lsoa_is_ons_code) |>
    summarise(n = n(), n_lsoa_missing = sum(is.na(lsoa)), .groups = "drop")
  list(counts = counts, lsoa_format = lsoa_format)
})
counts <- bind_rows(lapply(results, `[[`, "counts"))
write_csv(counts, file.path(output_dir, "country_coverage_prepared_samples.csv"))
write_csv(bind_rows(lapply(results, `[[`, "lsoa_format")), file.path(output_dir, "country_coverage_lsoa_format.csv"))
country_counts <- counts |>
  group_by(market, sample, country) |>
  summarise(n_before_london_exclusion = sum(n_before_london_exclusion), n_after_london_exclusion = sum(n_after_london_exclusion), .groups = "drop")
write_csv(country_counts, file.path(output_dir, "country_coverage_summary.csv"))
paths <- unlist(lapply(specs, function(spec) c(spec$transactions, list.files(spec$exposure, full.names = TRUE))), use.names = FALSE)
write_csv(tibble(path = paths, resolved_path = normalizePath(paths), bytes = as.numeric(file.info(paths)$size), modified = as.character(file.info(paths)$mtime)), file.path(output_dir, "country_coverage_source_metadata.csv"))
writeLines(c(
  "Coverage audit of current baseline-hedonic and intensive-attention prepared samples, both markets. No models or builders run.",
  "Preparation functions are parsed individually from current scripts without running setup or main. London exclusion is reported explicitly.",
  "Country is the transaction geography field, not water-company identity. LSOA values are inspected for ONS code format; names must not be interpreted using their first letter.",
  "Counts precede salience subgroup selection and estimator-specific removals. This check establishes the geographic scope of eligible prepared observations, not exact final fitted observations."
), file.path(output_dir, "country_coverage_scope.txt"))
print(country_counts, width = Inf)
