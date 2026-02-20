# ==============================================================================
# Area Within-Variation Diagnostics: Upstream/Downstream + Hedonic Prior
# ==============================================================================
#
# Purpose: Diagnose whether there is enough within-area variation in spill-count
#          exposure for area fixed-effects specifications.
#
# Diagnostics (per exposure variable):
#   1) Share of observations in non-identifying areas
#   2) Within-variation share of total variance
#   3) Effective identifying sample size (# obs / # areas with variation)
#
# Samples:
#   - Upstream/downstream prior sample:
#       * sales: prior_to_sale/house_site + river-direction joins/filters
#       * rentals: prior_to_rental/rental_site + river-direction joins/filters
#       * exposure vars: upstream_count, downstream_count
#   - Hedonic continuous prior sample:
#       * sales: prior_to_sale
#       * rentals: prior_to_rental
#       * exposure var: spill_count_daily_avg
#
# Output: Console only (no files written)
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
DIST_RIVER_MAX <- 1000L
ALPHA <- 1

args <- commandArgs(trailingOnly = TRUE)
GEOGRAPHY <- if (length(args) >= 1) tolower(args[[1]]) else "msoa"

if (!GEOGRAPHY %in% c("lsoa", "msoa")) {
  stop(
    sprintf(
      "Invalid geography '%s'. Use one of: lsoa, msoa.",
      GEOGRAPHY
    ),
    call. = FALSE
  )
}
GEO_LABEL <- toupper(GEOGRAPHY)

# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "here",
  "forcats"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)

# ==============================================================================
# 3. Helpers
# ==============================================================================
validate_required_cols <- function(data, required_cols, dataset_name) {
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "Missing required columns in %s: %s",
        dataset_name,
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }
}

assert_no_duplicate_units <- function(data, unit_var, dataset_name) {
  dup_count <- data |>
    count(.data[[unit_var]], name = "n_unit") |>
    filter(n_unit > 1) |>
    nrow()

  if (dup_count > 0) {
    stop(
      sprintf(
        "Found %s duplicated %s values in %s.",
        dup_count,
        unit_var,
        dataset_name
      ),
      call. = FALSE
    )
  }
}

calc_area_variation_metrics <- function(
    data,
    exposure_var,
    sample_name,
    panel,
    unit_var,
    area_var = "lsoa"
) {
  validate_required_cols(
    data,
    c(unit_var, area_var, exposure_var),
    dataset_name = paste0(sample_name, " / ", panel)
  )

  sample_clean <- data |>
    select(all_of(c(unit_var, area_var, exposure_var))) |>
    filter(!is.na(.data[[area_var]]), !is.na(.data[[exposure_var]]))

  if (nrow(sample_clean) == 0) {
    stop(
      sprintf(
        "No non-missing rows for %s in %s / %s.",
        exposure_var,
        sample_name,
        panel
      ),
      call. = FALSE
    )
  }

  area_stats <- sample_clean |>
    group_by(.data[[area_var]]) |>
    summarise(
      n_obs = n(),
      n_distinct_exposure = n_distinct(.data[[exposure_var]]),
      .groups = "drop"
    ) |>
    mutate(is_identifying = n_distinct_exposure > 1)

  n_obs_total <- nrow(sample_clean)
  n_area_total <- nrow(area_stats)
  n_area_identifying <- sum(area_stats$is_identifying)
  n_obs_identifying <- sum(area_stats$n_obs[area_stats$is_identifying])

  sample_within <- sample_clean |>
    group_by(.data[[area_var]]) |>
    mutate(x_within = .data[[exposure_var]] - mean(.data[[exposure_var]], na.rm = TRUE)) |>
    ungroup()

  var_total <- var(sample_clean[[exposure_var]], na.rm = TRUE)
  var_within <- var(sample_within$x_within, na.rm = TRUE)

  within_variance_share <- if (is.na(var_total) || var_total == 0) {
    warning(
      sprintf(
        "Total variance is zero for %s in %s / %s; within_variance_share set to NA.",
        exposure_var,
        sample_name,
        panel
      ),
      call. = FALSE
    )
    NA_real_
  } else {
    var_within / var_total
  }

  tibble(
    sample = sample_name,
    panel = panel,
    exposure_var = exposure_var,
    n_obs_total = n_obs_total,
    n_area_total = n_area_total,
    n_area_identifying = n_area_identifying,
    share_area_identifying = n_area_identifying / n_area_total,
    n_obs_identifying = n_obs_identifying,
    share_obs_identifying = n_obs_identifying / n_obs_total,
    share_obs_non_identifying = 1 - (n_obs_identifying / n_obs_total),
    var_total = var_total,
    var_within = var_within,
    within_variance_share = within_variance_share
  )
}

print_results_block <- function(results, sample_name, panel_name, ordered_exposures, geo_label) {
  output_table <- results |>
    filter(sample == sample_name, panel == panel_name) |>
    mutate(exposure_var = factor(exposure_var, levels = ordered_exposures)) |>
    arrange(exposure_var) |>
    mutate(
      across(
        c(
          share_area_identifying,
          share_obs_identifying,
          share_obs_non_identifying,
          within_variance_share
        ),
        ~ round(.x, 3)
      ),
      across(c(var_total, var_within), ~ signif(.x, 4))
    ) |>
    select(
      exposure_var,
      n_obs_total,
      n_area_total,
      n_area_identifying,
      share_area_identifying,
      n_obs_identifying,
      share_obs_identifying,
      share_obs_non_identifying,
      within_variance_share,
      var_total,
      var_within
    )

  cat("\n", strrep("=", 95), "\n", sep = "")
  cat(
    geo_label,
    " Variation Diagnostics - ",
    toupper(sample_name),
    " - ",
    toupper(panel_name),
    "\n",
    sep = ""
  )
  cat(strrep("=", 95), "\n", sep = "")
  print(output_table, n = Inf)
}

# ==============================================================================
# 4. Sample Builders
# ==============================================================================
build_updown_prior_sales_sample <- function(area_var = "lsoa") {
  cat("Building upstream/downstream prior sales sample...\n")

  dat_cs_sales <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "sales", "prior_to_sale", "house_site")
  ) |>
    filter(radius == RAD) |>
    collect()

  sales <- rio::import(
    here::here("data", "processed", "house_price.parquet"),
    trust = TRUE
  ) |>
    select(
      -transaction_id,
      -date_of_transfer,
      -quality,
      -paon,
      -saon,
      -street,
      -locality,
      -town_city,
      -district,
      -county,
      -ppd_category,
      -record_status
    ) |>
    mutate(
      property_type = forcats::as_factor(property_type),
      old_new = forcats::as_factor(old_new),
      duration = forcats::as_factor(duration)
    )

  upstream_downstream_sales <- rio::import(
    here::here(
      "upstream_downstream",
      "output",
      "03-02",
      "river_filter",
      "spill_house_signed_with_lateral.csv"
    )
  ) |>
    mutate(
      direction = ifelse(direction == -1, 1, 0),
      direction = factor(direction, levels = c(1, 0), labels = c("Upstream", "Downstream")),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(spill_lateral_m < RAD, house_lateral_m < RAD, dist_river_m < DIST_RIVER_MAX)

  dat_sales_clean <- dat_cs_sales |>
    select(-any_of("price")) |>
    inner_join(sales, by = "house_id") |>
    inner_join(upstream_downstream_sales, by = c("house_id", "site_id")) |>
    mutate(log_price = log(price)) |>
    filter(
      !is.na(spill_count_daily_avg),
      !is.na(.data[[area_var]]),
      !is.na(property_type),
      !is.na(old_new),
      !is.na(duration),
      !is.na(direction)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new = forcats::fct_drop(old_new),
      duration = forcats::fct_drop(duration)
    )

  dat_exposure_sales <- dat_sales_clean |>
    group_by(house_id) |>
    mutate(inv_dist_weight_raw = (dist_river_m + 0.01)^(-ALPHA)) |>
    summarise(
      upstream_count = sum(spill_count_daily_avg * (direction == "Upstream"), na.rm = TRUE),
      downstream_count = sum(spill_count_daily_avg * (direction == "Downstream"), na.rm = TRUE),
      .groups = "drop"
    ) |>
    inner_join(sales, by = "house_id")

  validate_required_cols(
    dat_exposure_sales,
    c("house_id", area_var, "upstream_count", "downstream_count"),
    dataset_name = "upstream/downstream sales sample"
  )
  assert_no_duplicate_units(dat_exposure_sales, "house_id", "upstream/downstream sales sample")

  dat_exposure_sales
}

build_updown_prior_rentals_sample <- function(area_var = "lsoa") {
  cat("Building upstream/downstream prior rentals sample...\n")

  dat_cs_rentals <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "rentals", "prior_to_rental", "rental_site")
  ) |>
    filter(radius == RAD) |>
    collect()

  rentals <- rio::import(
    here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    trust = TRUE
  ) |>
    select(
      -postcode,
      -listing_created,
      -latest_to_rent,
      -rented,
      -rented_est,
      -address_line_01,
      -address_line_02,
      -address_line_03
    ) |>
    mutate(property_type = forcats::as_factor(property_type))

  upstream_downstream_rentals <- rio::import(
    here::here(
      "upstream_downstream",
      "output",
      "03-02",
      "river_filter",
      "spill_rental_signed_with_lateral.csv"
    )
  ) |>
    mutate(
      direction = ifelse(direction == -1, 1, 0),
      direction = factor(direction, levels = c(1, 0), labels = c("Upstream", "Downstream")),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(spill_lateral_m < RAD, rental_lateral_m < RAD, dist_river_m < DIST_RIVER_MAX)

  dat_rental_clean <- dat_cs_rentals |>
    select(-any_of("listing_price")) |>
    inner_join(rentals, by = "rental_id") |>
    inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id")) |>
    mutate(log_price = log(listing_price)) |>
    filter(
      !is.na(spill_count_daily_avg),
      !is.na(.data[[area_var]]),
      !is.na(property_type),
      !is.na(bedrooms),
      !is.na(bathrooms),
      !is.na(direction)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )

  dat_exposure_rentals <- dat_rental_clean |>
    group_by(rental_id) |>
    mutate(inv_dist_weight_raw = (dist_river_m + 0.01)^(-ALPHA)) |>
    summarise(
      upstream_count = sum(spill_count_daily_avg * (direction == "Upstream"), na.rm = TRUE),
      downstream_count = sum(spill_count_daily_avg * (direction == "Downstream"), na.rm = TRUE),
      .groups = "drop"
    ) |>
    inner_join(rentals, by = "rental_id")

  validate_required_cols(
    dat_exposure_rentals,
    c("rental_id", area_var, "upstream_count", "downstream_count"),
    dataset_name = "upstream/downstream rentals sample"
  )
  assert_no_duplicate_units(dat_exposure_rentals, "rental_id", "upstream/downstream rentals sample")

  dat_exposure_rentals
}

build_hedonic_prior_sales_sample <- function(area_var = "lsoa") {
  cat("Building hedonic prior sales sample...\n")

  dat_cs_sales <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
  ) |>
    filter(radius == RAD) |>
    filter(n_spill_sites > 0) |>
    collect()

  sales <- rio::import(
    here::here("data", "processed", "house_price.parquet"),
    trust = TRUE
  ) |>
    select(
      -transaction_id,
      -date_of_transfer,
      -quality,
      -paon,
      -saon,
      -street,
      -locality,
      -town_city,
      -district,
      -county,
      -ppd_category,
      -record_status
    ) |>
    mutate(
      property_type = forcats::as_factor(property_type),
      old_new = forcats::as_factor(old_new),
      duration = forcats::as_factor(duration)
    )

  dat_sales_clean <- dat_cs_sales |>
    select(-any_of("price")) |>
    inner_join(sales, by = "house_id") |>
    mutate(log_price = log(price)) |>
    filter(
      !is.na(spill_count_daily_avg),
      !is.na(.data[[area_var]]),
      !is.na(property_type),
      !is.na(old_new),
      !is.na(duration)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new = forcats::fct_drop(old_new),
      duration = forcats::fct_drop(duration)
    )

  validate_required_cols(
    dat_sales_clean,
    c("house_id", area_var, "spill_count_daily_avg"),
    dataset_name = "hedonic prior sales sample"
  )
  assert_no_duplicate_units(dat_sales_clean, "house_id", "hedonic prior sales sample")

  dat_sales_clean
}

build_hedonic_prior_rentals_sample <- function(area_var = "lsoa") {
  cat("Building hedonic prior rentals sample...\n")

  dat_cs_rentals <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
  ) |>
    filter(radius == RAD) |>
    filter(n_spill_sites > 0) |>
    collect()

  rentals <- rio::import(
    here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    trust = TRUE
  ) |>
    select(
      -postcode,
      -listing_created,
      -latest_to_rent,
      -rented,
      -rented_est,
      -address_line_01,
      -address_line_02,
      -address_line_03
    ) |>
    mutate(property_type = forcats::as_factor(property_type))

  dat_rental_clean <- dat_cs_rentals |>
    select(-any_of("listing_price")) |>
    inner_join(rentals, by = "rental_id") |>
    mutate(log_price = log(listing_price)) |>
    filter(
      !is.na(spill_count_daily_avg),
      !is.na(.data[[area_var]]),
      !is.na(property_type),
      !is.na(bedrooms),
      !is.na(bathrooms)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )

  validate_required_cols(
    dat_rental_clean,
    c("rental_id", area_var, "spill_count_daily_avg"),
    dataset_name = "hedonic prior rentals sample"
  )
  assert_no_duplicate_units(dat_rental_clean, "rental_id", "hedonic prior rentals sample")

  dat_rental_clean
}

# ==============================================================================
# 5. Run Diagnostics
# ==============================================================================
cat(
  "Running ",
  GEO_LABEL,
  " variation diagnostics for upstream/downstream and hedonic prior samples...\n",
  sep = ""
)

updown_sales <- build_updown_prior_sales_sample(area_var = GEOGRAPHY)
updown_rentals <- build_updown_prior_rentals_sample(area_var = GEOGRAPHY)
hedonic_sales <- build_hedonic_prior_sales_sample(area_var = GEOGRAPHY)
hedonic_rentals <- build_hedonic_prior_rentals_sample(area_var = GEOGRAPHY)

results <- bind_rows(
  purrr::map_dfr(
    c("upstream_count", "downstream_count"),
    ~ calc_area_variation_metrics(
      data = updown_sales,
      exposure_var = .x,
      sample_name = "upstream_downstream_prior",
      panel = "sales",
      unit_var = "house_id",
      area_var = GEOGRAPHY
    )
  ),
  purrr::map_dfr(
    c("upstream_count", "downstream_count"),
    ~ calc_area_variation_metrics(
      data = updown_rentals,
      exposure_var = .x,
      sample_name = "upstream_downstream_prior",
      panel = "rentals",
      unit_var = "rental_id",
      area_var = GEOGRAPHY
    )
  ),
  purrr::map_dfr(
    c("spill_count_daily_avg"),
    ~ calc_area_variation_metrics(
      data = hedonic_sales,
      exposure_var = .x,
      sample_name = "hedonic_prior",
      panel = "sales",
      unit_var = "house_id",
      area_var = GEOGRAPHY
    )
  ),
  purrr::map_dfr(
    c("spill_count_daily_avg"),
    ~ calc_area_variation_metrics(
      data = hedonic_rentals,
      exposure_var = .x,
      sample_name = "hedonic_prior",
      panel = "rentals",
      unit_var = "rental_id",
      area_var = GEOGRAPHY
    )
  )
)

print_results_block(
  results,
  sample_name = "upstream_downstream_prior",
  panel_name = "sales",
  ordered_exposures = c("upstream_count", "downstream_count"),
  geo_label = GEO_LABEL
)
print_results_block(
  results,
  sample_name = "upstream_downstream_prior",
  panel_name = "rentals",
  ordered_exposures = c("upstream_count", "downstream_count"),
  geo_label = GEO_LABEL
)
print_results_block(
  results,
  sample_name = "hedonic_prior",
  panel_name = "sales",
  ordered_exposures = c("spill_count_daily_avg"),
  geo_label = GEO_LABEL
)
print_results_block(
  results,
  sample_name = "hedonic_prior",
  panel_name = "rentals",
  ordered_exposures = c("spill_count_daily_avg"),
  geo_label = GEO_LABEL
)

cat("\nDone.\n")
