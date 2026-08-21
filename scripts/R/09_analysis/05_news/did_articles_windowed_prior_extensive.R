# ==============================================================================
# News/Information DiD Analysis (Windowed Article Counts, Extensive Margin)
# ==============================================================================
#
# Purpose: Estimate whether recent media coverage changes the price gap between
#          properties nearer to versus farther from the nearest sewage overflow.
#          Treatment is configurable proximity to the nearest mapped overflow
#          rather than realised spill intensity.
#
# Author: Jacopo Olivieri
# Date: 2026-06-22
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
#   - output/tables/did_articles_windowed_prior_extensive_<COMPARISON>_<MEASURE>.tex
#   - output/tables/did_articles_windowed_prior_extensive_<COMPARISON>_effect_sizes.csv
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "fixest",
  "forcats",
  "here",
  "modelsummary",
  "purrr",
  "rio",
  "tibble"
)

check_required_packages(REQUIRED_PACKAGES)

# Shared table formatting helpers
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

source(
  here::here("scripts", "R", "09_analysis", "05_news", "extensive_margin_news_utils.R"),
  local = TRUE
)
source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "windowed_article_effect_size_utils.R"
  ),
  local = TRUE
)
source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "windowed_article_analysis_config.R"
  ),
  local = TRUE
)


# ==============================================================================
# 1. Configuration
# ==============================================================================
CONFIG <- list(
  analysis_start_month_id = 1L,
  sales_end_month_id = 48L,
  rental_end_month_id = 36L,
  windows = windowed_article_windows,
  comparisons = unname(windowed_article_extensive_comparisons),
  article_path = here::here(
    "data", "processed", "lexis_nexis", "search1_monthly.parquet"
  ),
  sales_path = here::here("data", "processed", "house_price.parquet"),
  sales_lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  rental_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  rental_lookup_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  output_dir = here::here("output", "tables")
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
# 3. Helpers
# ==============================================================================
article_log_columns <- function(articles) {
  grep("^log_(cumulative_articles|articles_[0-9]+m)$", names(articles), value = TRUE)
}

interaction_term <- function(salience_col) {
  paste0("near_bin:", salience_col)
}

make_formula <- function(rhs, fixed_effects = NULL) {
  stats::as.formula(paste0(
    "log_price ~ ",
    rhs,
    if (!is.null(fixed_effects)) paste0(" | ", fixed_effects) else ""
  ))
}

measure_slug <- function(measure) {
  windowed_article_measure_slugs[[measure]]
}

extensive_table_path <- function(comparison, measure) {
  file.path(
    CONFIG$output_dir,
    paste0(
      "did_articles_windowed_prior_extensive_", comparison$comparison_id, "_",
      measure_slug(measure), ".tex"
    )
  )
}

extensive_effect_size_path <- function(comparison) {
  file.path(
    CONFIG$output_dir,
    paste0(
      "did_articles_windowed_prior_extensive_", comparison$comparison_id,
      "_effect_sizes.csv"
    )
  )
}

legacy_extensive_table_path <- function(comparison, measure) {
  if (comparison$comparison_id != "500_vs_1000_2000") return(NULL)
  if (measure == "Cumulative") {
    return(file.path(CONFIG$output_dir, "did_articles_prior_extensive.tex"))
  }
  file.path(
    CONFIG$output_dir,
    paste0("did_articles_windowed_prior_extensive_", measure_slug(measure), ".tex")
  )
}


# ==============================================================================
# 4. Data Preparation
# ==============================================================================

#' Prepare the sales estimation sample
#'
#' @param comparison Validated comparison config.
#' @param articles Article-count panel from `load_windowed_articles_data()`.
#' @param sales Filtered sales transactions.
#' @param sales_lookup Nearest-overflow lookup covering every comparison.
#' @return Tibble ready for estimation.
prepare_sales_analysis_data <- function(comparison, articles, sales, sales_lookup) {
  cat("Creating sales analysis dataset...\n")

  log_cols <- article_log_columns(articles)

  dat <- build_extensive_margin_sample(
    transactions = sales,
    lookup = sales_lookup,
    id_col = "house_id",
    comparison = comparison
  ) |>
    dplyr::inner_join(articles, by = "month_id") |>
    dplyr::mutate(log_price = log(.data$price)) |>
    dplyr::filter(
      dplyr::if_all(dplyr::all_of(log_cols), is.finite),
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

  dat
}

#' Prepare the rental estimation sample
#'
#' @param comparison Validated comparison config.
#' @param articles Article-count panel from `load_windowed_articles_data()`.
#' @param rentals Filtered rental transactions.
#' @param rental_lookup Nearest-overflow lookup covering every comparison.
#' @return Tibble ready for estimation.
prepare_rental_analysis_data <- function(comparison, articles, rentals, rental_lookup) {
  cat("Creating rental analysis dataset...\n")

  log_cols <- article_log_columns(articles)

  dat_rental <- build_extensive_margin_sample(
    transactions = rentals,
    lookup = rental_lookup,
    id_col = "rental_id",
    comparison = comparison
  ) |>
    dplyr::inner_join(articles, by = "month_id") |>
    dplyr::mutate(log_price = log(.data$listing_price)) |>
    dplyr::filter(
      dplyr::if_all(dplyr::all_of(log_cols), is.finite),
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

  dat_rental
}


# ==============================================================================
# 5. Estimation
# ==============================================================================

#' Estimate the extensive-margin windowed articles models
#'
#' @param dat Sales estimation sample.
#' @param dat_rental Rental estimation sample.
#' @param salience_col Log article-count measure column.
#' @return Named list of fitted `fixest` models.
estimate_models <- function(dat, dat_rental, salience_col) {
  cat("\nEstimating regression models...\n")

  interaction <- interaction_term(salience_col)

  models <- list(
    model_sale_1 = fixest::feols(
      make_formula(paste0("near_bin + ", salience_col, " + ", interaction)),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_1b = fixest::feols(
      make_formula(
        paste0(
          "near_bin + ",
          salience_col,
          " + ",
          interaction,
          " + property_type + old_new + duration"
        )
      ),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_2 = fixest::feols(
      make_formula(paste0("near_bin + ", interaction), "msoa + month_id"),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_3 = fixest::feols(
      make_formula(
        paste0("near_bin + ", interaction, " + property_type + old_new + duration"),
        "msoa + month_id"
      ),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_4 = fixest::feols(
      make_formula(paste0("near_bin + ", interaction), "lsoa + month_id"),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_5 = fixest::feols(
      make_formula(
        paste0("near_bin + ", interaction, " + property_type + old_new + duration"),
        "lsoa + month_id"
      ),
      data = dat,
      vcov = ~lsoa
    ),
    model_rent_1 = fixest::feols(
      make_formula(paste0("near_bin + ", salience_col, " + ", interaction)),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_1b = fixest::feols(
      make_formula(
        paste0(
          "near_bin + ",
          salience_col,
          " + ",
          interaction,
          " + property_type + bedrooms + bathrooms"
        )
      ),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_2 = fixest::feols(
      make_formula(paste0("near_bin + ", interaction), "msoa + month_id"),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_3 = fixest::feols(
      make_formula(
        paste0("near_bin + ", interaction, " + property_type + bedrooms + bathrooms"),
        "msoa + month_id"
      ),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_4 = fixest::feols(
      make_formula(paste0("near_bin + ", interaction), "lsoa + month_id"),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_5 = fixest::feols(
      make_formula(
        paste0("near_bin + ", interaction, " + property_type + bedrooms + bathrooms"),
        "lsoa + month_id"
      ),
      data = dat_rental,
      vcov = ~lsoa
    )
  )

  cat("  Model battery estimated; using LSOA-clustered SEs\n")

  models
}

preferred_models <- function(models) {
  list(
    sale_msoa = models$model_sale_3,
    sale_lsoa = models$model_sale_5,
    rent_msoa = models$model_rent_3,
    rent_lsoa = models$model_rent_5
  )
}


# ==============================================================================
# 6. Export Table
# ==============================================================================

#' Export one measure-stamped regression table
#'
#' @param models Named list of fitted models.
#' @param comparison Validated comparison config.
#' @param measure Salience-measure display label.
#' @param salience_col Log article-count measure column.
#' @return Output path, invisibly.
export_table <- function(models, comparison, measure, salience_col) {
  cat("\nExporting regression table...\n")

  interaction <- interaction_term(salience_col)
  cumulative <- measure == "Cumulative"
  salience_label <- if (cumulative) {
    "$\\log (\\text{Articles})$"
  } else {
    paste0("$\\log (\\text{Articles}_{", measure, "})$")
  }
  interaction_label <- paste0(
    "{Near bin \\\\ $\\times$ ", salience_label, "}"
  )

  coef_labels <- c(
    "near_bin" = "Near bin",
    stats::setNames(salience_label, salience_col),
    stats::setNames(interaction_label, interaction)
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
    "The sales sample covers 2021--2024 and the rental sample covers 2021--2023 ",
    "(no 2024 rental data are available). The dependent variable is the log transaction price for sales ",
    "(columns 1--6) or the log weekly asking rent for rentals ",
    "(columns 7--12). Near bin is an indicator equal to one for properties in the ",
    comparison$near_band_label,
    " band and zero for properties in the ",
    comparison$far_band_label,
    " band. ",
    salience_label,
    if (cumulative) {
      paste0(
        " is the natural logarithm of cumulative UK news coverage of sewage spills ",
        "from LexisNexis from January 2021 through the transaction month. "
      )
    } else {
      paste0(
        " is the natural logarithm of UK news coverage of sewage spills from ",
        "LexisNexis over the trailing ", sub("m$", "", measure),
        " months, inclusive of the transaction month. "
      )
    },
    "Property controls include ",
    "type (flat, semi-detached, terraced, other), new build status, and tenure ",
    "for sales; and type (bungalow, detached, semi-detached, terraced), ",
    "bedrooms, and bathrooms for rentals. Standard errors clustered at the ",
    "LSOA level are reported in parentheses. *** p<0.01, ** p<0.05, ",
    "* p<0.1.}},"
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
    fmt = fmt_table,
    coef_map = coef_labels,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = paste0(
      "Effect of Overflow Proximity on Property Values: Log ",
      if (cumulative) "Cumulative" else sub("m$", "-month", measure),
      " Media Coverage (Prior to Transaction, ",
      comparison$comparison_label,
      ")"
    )
  )

  table_latex <- patch_modelsummary_latex(
    table_latex = table_latex,
    label = paste0(
      "tbl:did-articles-windowed-prior-extensive-", comparison$comparison_id,
      "-", measure_slug(measure)
    ),
    notes = custom_notes
  )

  output_path <- extensive_table_path(comparison, measure)
  ensure_output_dir(output_path)
  writeLines(table_latex, output_path)

  legacy_path <- legacy_extensive_table_path(comparison, measure)
  if (!is.null(legacy_path)) {
    writeLines(table_latex, legacy_path)
  }

  cat(sprintf("LaTeX table exported to: %s\n", output_path))

  invisible(output_path)
}


# ==============================================================================
# 7. Per-Window Workflow
# ==============================================================================
run_for_comparison <- function(
    comparison, articles, sales, rentals, sales_lookup, rental_lookup) {
  comparison <- validate_comparison_config(comparison, allow_adjacent = TRUE)
  cat("\n========================== Comparison:", comparison$comparison_label,
      "==========================\n")

  sales_articles <- dplyr::filter(
    articles, .data$month_id <= CONFIG$sales_end_month_id
  )
  rental_articles <- dplyr::filter(
    articles, .data$month_id <= CONFIG$rental_end_month_id
  )
  dat <- prepare_sales_analysis_data(comparison, sales_articles, sales, sales_lookup)
  dat_rental <- prepare_rental_analysis_data(
    comparison, rental_articles, rentals, rental_lookup
  )

  salience_cols <- windowed_article_salience_cols
  models_by_measure <- purrr::imap(salience_cols, function(salience_col, measure) {
    models <- estimate_models(dat, dat_rental, salience_col)
    export_table(models, comparison, measure, salience_col)
    preferred_models(models)
  })

  effect_sizes <- write_windowed_article_effect_sizes(
    models_by_measure = models_by_measure,
    salience_cols = salience_cols,
    sales_data = dat,
    rental_data = dat_rental,
    interaction_term_fn = interaction_term,
    margin = "extensive",
    output_path = extensive_effect_size_path(comparison),
    metadata = list(
      comparison_id = comparison$comparison_id,
      comparison_label = comparison$comparison_label,
      near_min = comparison$near_min,
      near_max = comparison$near_max,
      far_min = comparison$far_min,
      far_max = comparison$far_max
    )
  )
  if (comparison$comparison_id == "500_vs_1000_2000") {
    utils::write.csv(
      effect_sizes,
      file.path(
        CONFIG$output_dir,
        "did_articles_windowed_prior_extensive_effect_sizes.csv"
      ),
      row.names = FALSE,
      na = ""
    )
  }

  invisible(list(
    comparison = comparison,
    effect_size_output_path = extensive_effect_size_path(comparison)
  ))
}


# ==============================================================================
# 8. Main Workflow
# ==============================================================================
main <- function() {
  initialise_environment()

  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  articles <- load_windowed_articles_data(
    path = CONFIG$article_path,
    windows = CONFIG$windows,
    start_month_id = CONFIG$analysis_start_month_id,
    end_month_id = max(CONFIG$sales_end_month_id, CONFIG$rental_end_month_id)
  )
  sales <- load_sales_transactions(CONFIG$sales_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$sales_end_month_id
    )
  rentals <- load_rental_transactions(CONFIG$rental_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$rental_end_month_id
    )
  max_far_distance <- max(vapply(CONFIG$comparisons, `[[`, integer(1), "far_max"))
  sales_lookup <- load_nearest_distance_lookup(
    CONFIG$sales_lookup_path, "house_id", max_far_distance
  )
  rental_lookup <- load_nearest_distance_lookup(
    CONFIG$rental_lookup_path, "rental_id", max_far_distance
  )
  results <- purrr::map(
    CONFIG$comparisons,
    run_for_comparison,
    articles = articles,
    sales = sales,
    rentals = rentals,
    sales_lookup = sales_lookup,
    rental_lookup = rental_lookup
  )
  names(results) <- vapply(
    CONFIG$comparisons, `[[`, character(1), "comparison_id"
  )

  cat("\nScript completed successfully.\n")
  cat("  Measures: Cumulative, ", paste0(CONFIG$windows, "m", collapse = ", "), "\n", sep = "")
  cat(
    "  Comparisons:",
    paste(vapply(CONFIG$comparisons, `[[`, character(1), "comparison_label"), collapse = "; "),
    "\n"
  )

  invisible(
    list(
      measures = c("Cumulative", paste0(CONFIG$windows, "m")),
      comparisons = CONFIG$comparisons,
      results = results
    )
  )
}

if (sys.nframe() == 0) {
  main()
}
