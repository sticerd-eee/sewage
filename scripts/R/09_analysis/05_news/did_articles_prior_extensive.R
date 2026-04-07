# ==============================================================================
# News/Information DiD Analysis (Cumulative Article Count, Extensive Margin)
# ==============================================================================
#
# Purpose: Estimate whether cumulative media coverage changes the price gap
#          between properties nearer to versus farther from the nearest sewage
#          overflow. Treatment is configurable proximity to the nearest mapped
#          overflow rather than realised spill intensity.
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
#   - output/tables/did_articles_prior_extensive.tex
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
  output_path = here::here("output", "tables", "did_articles_prior_extensive.tex"),
  table_label = "tbl:did-articles-prior-extensive"
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
    inner_join(
      articles |>
        select("month_id", "cumulative_articles", "log_cumulative_articles"),
      by = "month_id"
    ) |>
    mutate(log_price = log(.data$price)) |>
    filter(
      is.finite(.data$log_cumulative_articles),
      !is.na(.data$lsoa),
      !is.na(.data$month_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$old_new),
      !is.na(.data$duration),
      is.finite(.data$log_price)
    ) |>
    standardise_sales_estimation_data()

  cat(sprintf("  Article counts: %d months\n", nrow(articles)))
  print_extensive_margin_summary(dat, "Sales", comparison)
  cat(sprintf(
    "    Log cumulative articles: mean = %.2f, sd = %.2f\n",
    mean(dat$log_cumulative_articles, na.rm = TRUE),
    stats::sd(dat$log_cumulative_articles, na.rm = TRUE)
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
    inner_join(
      articles |>
        select("month_id", "cumulative_articles", "log_cumulative_articles"),
      by = "month_id"
    ) |>
    mutate(log_price = log(.data$listing_price)) |>
    filter(
      is.finite(.data$log_cumulative_articles),
      !is.na(.data$lsoa),
      !is.na(.data$month_id),
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
    "    Log cumulative articles: mean = %.2f, sd = %.2f\n",
    mean(dat_rental$log_cumulative_articles, na.rm = TRUE),
    stats::sd(dat_rental$log_cumulative_articles, na.rm = TRUE)
  ))

  dat_rental
}


# ==============================================================================
# 4. Estimation
# ==============================================================================

#' Estimate the extensive-margin articles models
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
    model_sale_1b = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles + property_type + old_new + duration,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_2 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles | msoa + month_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_3 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles +
        property_type + old_new + duration | msoa + month_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_4 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles | lsoa + month_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_5 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles +
        property_type + old_new + duration | lsoa + month_id,
      data = dat,
      vcov = ~lsoa
    ),
    model_rent_1 = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_1b = fixest::feols(
      log_price ~ near_bin + log_cumulative_articles +
        near_bin:log_cumulative_articles + property_type + bedrooms + bathrooms,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_2 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles | msoa + month_id,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_3 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles +
        property_type + bedrooms + bathrooms | msoa + month_id,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_4 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles | lsoa + month_id,
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_5 = fixest::feols(
      log_price ~ near_bin + near_bin:log_cumulative_articles +
        property_type + bedrooms + bathrooms | lsoa + month_id,
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
    "log_cumulative_articles" = "$\\log (\\text{Articles})$",
    "near_bin:log_cumulative_articles" = "{Near bin \\\\ $\\times$ $\\log (\\text{Articles})$}"
  )

  gof_map <- tibble::tribble(
    ~raw, ~clean, ~fmt,
    "nobs", "Observations", 0,
    "adj.r.squared", "Adj. R-squared", 3
  )

  add_rows <- tibble::tribble(
    ~term, ~`(1)`, ~`(2)`, ~`(3)`, ~`(4)`, ~`(5)`, ~`(6)`,
    ~`(7)`, ~`(8)`, ~`(9)`, ~`(10)`, ~`(11)`, ~`(12)`,
    "Property controls", "No", "Yes", "No", "Yes", "No", "Yes",
    "No", "Yes", "No", "Yes", "No", "Yes",
    "Location FE", "No", "No", "MSOA", "MSOA", "LSOA", "LSOA",
    "No", "No", "MSOA", "MSOA", "LSOA", "LSOA",
    "Time FE", "No", "No", "Month", "Month", "Month", "Month",
    "No", "No", "Month", "Month", "Month", "Month"
  )
  attr(add_rows, "position") <- "coef_end"

  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} ",
    "This table presents hedonic estimates of the relationship between proximity ",
    "to sewage overflows, public attention, and property values. ",
    comparison_note_text(comparison),
    "The dependent variable is the log transaction price for sales ",
    "(columns 1--6) or the log weekly asking rent for rentals ",
    "(columns 7--12). Near bin is an indicator equal to one for properties in the ",
    comparison$near_band_label,
    " band and zero for properties in the ",
    comparison$far_band_label,
    " band. $\\\\log (\\\\text{Articles})$ is the natural logarithm of cumulative ",
    "UK news coverage of sewage spills from LexisNexis from January 2021 to the ",
    "transaction month. Property controls include type (flat, semi-detached, ",
    "terraced, other), new build status, and tenure for sales; and type ",
    "(bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for ",
    "rentals. Standard errors clustered at the LSOA level are reported in ",
    "parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  panels <- list(
    "House Sales" = list(
      "(1)" = models$model_sale_1,
      "(2)" = models$model_sale_1b,
      "(3)" = models$model_sale_2,
      "(4)" = models$model_sale_3,
      "(5)" = models$model_sale_4,
      "(6)" = models$model_sale_5
    ),
    "House Rentals" = list(
      "(7)" = models$model_rent_1,
      "(8)" = models$model_rent_1b,
      "(9)" = models$model_rent_2,
      "(10)" = models$model_rent_3,
      "(11)" = models$model_rent_4,
      "(12)" = models$model_rent_5
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
      "Effect of Overflow Proximity on Property Values: Log Cumulative Media Coverage ",
      "(Prior to Transaction, ",
      comparison$comparison_label,
      ")"
    )
  )

  table_latex <- patch_modelsummary_latex(
    table_latex = table_latex,
    label = CONFIG$table_label,
    notes = custom_notes
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
