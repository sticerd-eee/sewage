# ==============================================================================
# News/Information DiD Analysis (Lagged Articles, Extensive Margin)
# ==============================================================================
#
# Purpose: Estimate whether lagged cumulative media coverage changes the price
#          gap between properties nearer to versus farther from the nearest
#          sewage overflow. Treatment is configurable proximity to the nearest
#          mapped overflow rather than realised spill intensity.
#
# Author: Jacopo Olivieri
# Date: 2026-04-07
# Date Modified: 2026-04-07
#
# Inputs:
#   - data/processed/lexis_nexis/search1_monthly.parquet
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - scripts/R/09_analysis/05_news/extensive_margin_news_utils.R
#
# Outputs:
#   - output/tables/did_articles_lag4_prior_extensive.tex
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "fixest",
  "forcats",
  "glue",
  "here",
  "modelsummary",
  "rio",
  "tibble"
)

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here("scripts", "R", "09_analysis", "05_news", "extensive_margin_news_utils.R"),
  local = TRUE
)


# ==============================================================================
# 1. Configuration
# ==============================================================================
CONFIG <- list(
  analysis_start_month_id = 1L,
  analysis_end_month_id = 36L,
  lag_months = 4L,
  comparison = list(
    comparison_id = "500_vs_1000_2000",
    comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L,
    near_max = 500L,
    far_min = 1000L,
    far_max = 2000L
  ),
  article_path = here::here(
    "data", "processed", "lexis_nexis", "search1_monthly.parquet"
  ),
  sales_path = here::here("data", "processed", "house_price.parquet"),
  sales_lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  rental_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  rental_lookup_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  output_path = here::here(
    "output", "tables", "did_articles_lag4_prior_extensive.tex"
  ),
  table_label = "tbl:did-articles-lag4-prior-extensive"
)


# ==============================================================================
# 2. Package Management
# ==============================================================================
initialise_environment <- function() {
  invisible(lapply("dplyr", function(pkg) {
    suppressPackageStartupMessages(library(pkg, character.only = TRUE))
  }))
}


# ==============================================================================
# 3. Data Preparation
# ==============================================================================

#' Prepare the sales estimation sample
#'
#' @param comparison Validated comparison config.
#' @param articles Article-count panel from `load_articles_data()`.
#' @return Tibble ready for estimation.
prepare_sales_analysis_data <- function(comparison, articles) {
  cat("Loading sales transactions...\n")

  sales <- load_sales_transactions(CONFIG$sales_path) |>
    filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    )

  sales_lookup <- load_nearest_distance_lookup(
    path = CONFIG$sales_lookup_path,
    id_col = "house_id",
    max_distance = comparison$far_max
  )

  cat("Creating sales analysis dataset...\n")

  dat <- build_extensive_margin_sample(
    transactions = sales,
    lookup = sales_lookup,
    id_col = "house_id",
    comparison = comparison
  ) |>
    mutate(lagged_month_id = .data$month_id - CONFIG$lag_months) |>
    filter(.data$lagged_month_id >= CONFIG$analysis_start_month_id) |>
    inner_join(
      articles |>
        select("month_id", "cumulative_articles", "log_cumulative_articles"),
      by = c("lagged_month_id" = "month_id")
    ) |>
    mutate(log_price = log(.data$price)) |>
    filter(
      is.finite(.data$log_cumulative_articles),
      !is.na(.data$lsoa),
      !is.na(.data$qtr_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$old_new),
      !is.na(.data$duration),
      is.finite(.data$log_price)
    ) |>
    standardise_sales_estimation_data()

  cat(sprintf(
    "  Using %d-month lag for cumulative articles\n",
    CONFIG$lag_months
  ))
  print_extensive_margin_summary(dat, "Sales", comparison)
  cat(sprintf(
    "    Excluded first %d months of 2021 because of the lag\n",
    CONFIG$lag_months
  ))

  dat
}

#' Prepare the rental estimation sample
#'
#' @param comparison Validated comparison config.
#' @param articles Article-count panel from `load_articles_data()`.
#' @return Tibble ready for estimation.
prepare_rental_analysis_data <- function(comparison, articles) {
  cat("Loading rental transactions...\n")

  rentals <- load_rental_transactions(CONFIG$rental_path) |>
    filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    )

  rental_lookup <- load_nearest_distance_lookup(
    path = CONFIG$rental_lookup_path,
    id_col = "rental_id",
    max_distance = comparison$far_max
  )

  cat("Creating rental analysis dataset...\n")

  dat_rental <- build_extensive_margin_sample(
    transactions = rentals,
    lookup = rental_lookup,
    id_col = "rental_id",
    comparison = comparison
  ) |>
    mutate(lagged_month_id = .data$month_id - CONFIG$lag_months) |>
    filter(.data$lagged_month_id >= CONFIG$analysis_start_month_id) |>
    inner_join(
      articles |>
        select("month_id", "cumulative_articles", "log_cumulative_articles"),
      by = c("lagged_month_id" = "month_id")
    ) |>
    mutate(log_price = log(.data$listing_price)) |>
    filter(
      is.finite(.data$log_cumulative_articles),
      !is.na(.data$lsoa),
      !is.na(.data$qtr_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$bedrooms),
      !is.na(.data$bathrooms),
      is.finite(.data$log_price)
    ) |>
    standardise_rental_estimation_data()

  print_extensive_margin_summary(dat_rental, "Rentals", comparison)
  cat(sprintf(
    "    Excluded first %d months of 2021 because of the lag\n",
    CONFIG$lag_months
  ))

  dat_rental
}


# ==============================================================================
# 4. Estimation
# ==============================================================================

#' Estimate the lagged-articles extensive-margin models
#'
#' @param dat Sales estimation sample.
#' @param dat_rental Rental estimation sample.
#' @return Named list of fitted `fixest` models.
estimate_models <- function(dat, dat_rental) {
  cat("\nEstimating regression models...\n")

  models <- list(
    model_sale_1 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_2 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles | lsoa + qtr_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_3 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles + property_type + old_new + duration |
        lsoa + qtr_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_rent_1 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_2 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles | lsoa + qtr_id,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_3 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles + property_type + bedrooms + bathrooms |
        lsoa + qtr_id,
      data = dat_rental,
      vcov = ~lsoa
    )
  )

  cat("  Using LSOA-clustered SEs\n")

  models
}


# ==============================================================================
# 5. Export Table
# ==============================================================================

#' Export the regression table
#'
#' @param models Named list of fitted models.
#' @param comparison Validated comparison config.
#' @return Output path, invisibly.
export_table <- function(models, comparison) {
  cat("\nExporting regression table...\n")

  coef_labels <- c(
    "near_bin" = "Near bin",
    "log_cumulative_articles" = "Log cumulative articles (4-mo lag)",
    "near_bin:log_cumulative_articles" = "{Near bin \\\\ $\\times$ Log cumulative articles}"
  )

  gof_map <- tibble::tribble(
    ~raw, ~clean, ~fmt,
    "nobs", "Observations", 0,
    "adj.r.squared", "Adj. R-squared", 3
  )

  add_rows <- tibble::tribble(
    ~term, ~`(1)`, ~`(2)`, ~`(3)`, ~`(4)`, ~`(5)`, ~`(6)`,
    "Property controls", "No", "No", "Yes", "No", "No", "Yes",
    "LSOA FE", "No", "Yes", "Yes", "No", "Yes", "Yes",
    "Quarter FE", "No", "Yes", "Yes", "No", "Yes", "Yes"
  )
  attr(add_rows, "position") <- "coef_end"

  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} ",
    "Dependent variables are log house price (columns 1--3) and log rental price ",
    "(columns 4--6). ",
    comparison_note_text(comparison),
    "Near bin is an indicator equal to one for properties in the ",
    comparison$near_band_label,
    " band and zero for properties in the ",
    comparison$far_band_label,
    " band. Log cumulative articles (4-month lag) is the natural log of total UK ",
    "news coverage of sewage from January 2021 to four months before the transaction ",
    "(LexisNexis). Standard errors clustered at the LSOA level are reported in ",
    "parentheses. Property controls include type (flat, semi-detached, terraced, ",
    "other), new build status, and tenure for sales; and type (bungalow, detached, ",
    "semi-detached, terraced), bedrooms, and bathrooms for rentals. *** p<0.01, ",
    "** p<0.05, * p<0.1.}},"
  )

  panels <- list(
    "House Sales" = list(
      "(1)" = models$model_sale_1,
      "(2)" = models$model_sale_2,
      "(3)" = models$model_sale_3
    ),
    "House Rentals" = list(
      "(4)" = models$model_rent_1,
      "(5)" = models$model_rent_2,
      "(6)" = models$model_rent_3
    )
  )

  table_latex <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    escape = FALSE,
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = 3,
    coef_map = coef_labels,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = paste0(
      "Effect of Overflow Proximity on Property Values: 4-Month Lagged Media Coverage (",
      comparison$comparison_label,
      ")"
    )
  )

  table_latex <- patch_modelsummary_latex(
    table_latex = table_latex,
    label = CONFIG$table_label,
    notes = custom_notes,
    width = "0.9\\\\linewidth"
  )

  ensure_output_dir(CONFIG$output_path)
  writeLines(table_latex, CONFIG$output_path)

  cat(sprintf("LaTeX table exported to: %s\n", CONFIG$output_path))

  invisible(CONFIG$output_path)
}


# ==============================================================================
# 6. Main Workflow
# ==============================================================================
main <- function() {
  initialise_environment()

  comparison <- validate_comparison_config(CONFIG$comparison)
  articles <- load_articles_data(
    path = CONFIG$article_path,
    start_month_id = CONFIG$analysis_start_month_id,
    end_month_id = CONFIG$analysis_end_month_id
  )

  dat <- prepare_sales_analysis_data(comparison, articles)
  dat_rental <- prepare_rental_analysis_data(comparison, articles)
  models <- estimate_models(dat, dat_rental)
  export_table(models, comparison)

  cat("\nScript completed successfully.\n")

  invisible(
    list(
      output_path = CONFIG$output_path,
      comparison = comparison$comparison_label
    )
  )
}

if (sys.nframe() == 0) {
  main()
}
