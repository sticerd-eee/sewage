# ==============================================================================
# Baseline Hedonic by Salience Group
# ==============================================================================
#
# Purpose: Estimate the selected paper specification for All Bathing, All Coastal
#          and All Inland, separately for sales and rentals. These groups overlap.
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/cross_section/{sales,rentals}/prior_to_{sale,rental}/
#   - data/processed/site_characteristics/site_group_characteristics.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#
# Outputs:
#   - output/tables/hedonic_count_continuous_prior_salience_groups.tex
#   - output/regs/hedonic_count_continuous_prior_salience_groups.rds
#   - output/logs/hedonic_count_continuous_prior_salience_groups_{results,cell_counts}.csv
#
# Run from the repository root with Rscript in the rv environment (R 4.6.0).
# Full exploration: docs/reports/2026-09-03-003-heterogeneity-by-salience-report.qmd
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install project dependencies with `rv sync`.",
       call. = FALSE)
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
REQUIRED_PACKAGES <- c("arrow", "dplyr", "fixest", "forcats", "here", "modelsummary", "tibble")
check_required_packages(REQUIRED_PACKAGES)
source(here::here("scripts", "R", "utils", "salience_group_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "salience_publication_utils.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)


# ==============================================================================
# 1. Configuration
# ==============================================================================

CONFIG <- list(
  profile = "open_coast",
  exclude_london = FALSE,
  bathing_policy = "ever_2124",
  site_path = here::here("data", "processed", "site_characteristics", "site_group_characteristics.parquet"),
  coast_rule_m = 2000L,
  output_prefix = "hedonic_count_continuous_prior_salience_groups",
  title = "Baseline Hedonic by Salience Group",
  groups = c("bathing", "coastal", "inland")
)

MARKETS <- list(
  sales = list(
    id = "house_id",
    transactions = here::here("data", "processed", "house_price.parquet"),
    lookup = here::here("data", "processed", "spill_house_lookup.parquet"),
    exposure = here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
  ),
  rentals = list(
    id = "rental_id",
    transactions = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    lookup = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
    exposure = here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
  )
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

prepare_salience_hedonic <- function(transactions, exposure, market = c("sales", "rentals")) {
  market <- match.arg(market)
  id_col <- if (market == "sales") "house_id" else "rental_id"
  price_col <- if (market == "sales") "price" else "listing_price"
  controls <- if (market == "sales") c("property_type", "old_new", "duration") else
    c("property_type", "bedrooms", "bathrooms")
  factors <- if (market == "sales") controls else "property_type"
  transactions <- transactions |>
    dplyr::select(dplyr::all_of(c(id_col, price_col, "region", "lsoa", controls))) |>
    dplyr::mutate(dplyr::across(dplyr::all_of(factors), forcats::as_factor))
  exposure |>
    dplyr::filter(.data$radius == 250L, .data$n_spill_sites > 0L) |>
    dplyr::select(dplyr::all_of(c(id_col, "spill_count_weekly_avg", "spill_hrs_weekly_avg"))) |>
    dplyr::collect() |>
    dplyr::inner_join(transactions, by = id_col, relationship = "one-to-one") |>
    dplyr::mutate(log_price = log(.data[[price_col]])) |>
    # Preserve the parent's joint count/hours availability restriction. Its
    # published inputs already cover sales 2021--2024 and rentals 2021--2023;
    # the baseline does not impose the attention models' coordinate/month filters.
    dplyr::filter(dplyr::if_all(
      dplyr::all_of(c("spill_count_weekly_avg", "spill_hrs_weekly_avg", "lsoa", controls)),
      ~ !is.na(.x)
    )) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      dplyr::across(dplyr::all_of(factors), forcats::fct_drop)
    )
}

prepare_analysis_data <- function(market, characteristics) {
  spec <- MARKETS[[market]]
  data <- prepare_salience_hedonic(
    arrow::read_parquet(spec$transactions), arrow::open_dataset(spec$exposure), market
  )
  nearest <- nearest_salience_sites(
    arrow::open_dataset(spec$lookup), characteristics, spec$id, profile = CONFIG$profile
  )
  if (CONFIG$profile == "open_coast" && any(!data[[spec$id]] %in% nearest[[spec$id]]))
    stop("Missing required nearest Site Group for exposure-positive property.")
  data |>
    dplyr::left_join(dplyr::select(nearest, -"min_distance"), by = spec$id,
                     relationship = "many-to-one") |>
    classify_salience_groups(CONFIG$coast_rule_m, profile = CONFIG$profile, bathing_policy = CONFIG$bathing_policy)
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

fit_salience_hedonic <- function(data, market = c("sales", "rentals")) {
  market <- match.arg(market)
  formula <- if (market == "sales") {
    log_price ~ spill_count_weekly_avg + property_type + old_new + duration | lsoa
  } else log_price ~ spill_count_weekly_avg + property_type + bedrooms + bathrooms | lsoa
  model <- fixest::feols(formula, data = data, vcov = "hetero", lean = TRUE)
  term <- "spill_count_weekly_avg"
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified spill effect: ", market, call. = FALSE)
  }
  model
}

estimate_groups <- function(data, market) {
  counts <- audit_salience_groups(data, market, exclude_london = CONFIG$exclude_london, allow_unavailable = TRUE)
  masks <- salience_group_masks(data)
  models <- lapply(CONFIG$groups, function(group) {
    sample <- data[masks[[group]] & (!CONFIG$exclude_london | !data$london), ]
    cat(sprintf("  %s / %s: %s observations before estimator removals\n",
                market, group, format(nrow(sample), big.mark = ",")))
    fit_salience_group(sample, function(sample) fit_salience_hedonic(sample, market),
      terms = "spill_count_weekly_avg", id = MARKETS[[market]]$id)
  })
  names(models) <- CONFIG$groups
  counts <- salience_fit_counts(counts, models)
  results <- dplyr::bind_rows(lapply(names(models), function(group) {
    salience_group_results(models[[group]]) |>
      dplyr::filter(.data$term %in% "spill_count_weekly_avg") |>
      dplyr::mutate(market = .env$market, group = .env$group, .before = 1L)
  }))
  list(models = models, counts = counts, results = results)
}


# ==============================================================================
# 4. Table and Model Export
# ==============================================================================

export_table <- function(result, path = here::here("output", "tables", paste0(CONFIG$output_prefix, ".tex"))) {
  coefficient_map <- c(spill_count_weekly_avg = "Spills per week (avg.)")
  notes <- paste0(
    "This table presents hedonic estimates of the relationship between sewage spill exposure ",
    "and property values, estimated separately within each salience group. The sample ",
    "includes all properties within 250m of a storm overflow in the study area, including Greater ",
    "London, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are ",
    "available). Properties are excluded where any overflow within the radius has an ",
    "incomplete spill record over the exposure window, since measured spill exposure would ",
    "otherwise be understated. ",
    salience_group_notes("nearest", CONFIG$coast_rule_m, CONFIG$profile),
    "The dependent variable is the log transaction price for sales or the log weekly ",
    "asking rent for rentals. ",
    "Spill exposure is measured as the average number of spill events per week (12/24 count) ",
    "recorded across all overflows within 250m from January 2021 to the transaction date. ",
    "Property controls include type (flat, semi-detached, terraced, other), new build status, ",
    "and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, ",
    "and bathrooms for rentals. ",
    "Heteroskedasticity-robust standard errors are reported in parentheses. ",
    "Each column reports a separate regression; differences between groups are not tested. ",
    "*** $p<0.01$, ** $p<0.05$, * $p<0.1$."
  )
  export_salience_group_table(
    result$models, coefficient_map, CONFIG$title, notes,
    path
  )
}

export_results <- function(result, output_root = here::here("output")) {
  publish_salience_result(result, CONFIG$output_prefix, output_root, export_table)
}

# ==============================================================================
# 5. Execution
# ==============================================================================

main <- function() {
  provenance <- salience_input_provenance()
  characteristics <- arrow::read_parquet(here::here(
    "data", "processed", "site_characteristics", "site_group_characteristics.parquet"
  ))
  output <- lapply(names(MARKETS), function(market) {
    data <- prepare_analysis_data(market, characteristics)
    estimate_groups(data, market)
  })
  names(output) <- names(MARKETS)
  result <- list(
    models = lapply(output, function(x) x$models),
    counts = dplyr::bind_rows(lapply(output, function(x) x$counts)),
    results = dplyr::bind_rows(lapply(output, function(x) x$results)),
    settings = list(config = CONFIG, london = if (CONFIG$exclude_london) "excluded" else "included",
                    profile = CONFIG$profile, bathing_policy = CONFIG$bathing_policy, provenance = provenance,
                    groups_overlap = TRUE,
                    created_at = Sys.time(), r_version = R.version.string)
  )
  export_results(result)
}

if (sys.nframe() == 0L) {
  if (length(commandArgs(trailingOnly = TRUE))) {
    stop("This paper script accepts no arguments. Run the QMD for exploratory/reproduction analyses.",
         call. = FALSE)
  }
  main()
}
