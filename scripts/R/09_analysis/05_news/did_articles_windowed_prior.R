# ==============================================================================
# News/Information DiD Analysis (Windowed Article Counts) - Prior to Sale/Rental
# ==============================================================================
#
# Purpose: Estimate whether recent media coverage amplifies the capitalization
#          of sewage spill exposure into house prices and rents. Uses trailing
#          3-, 6-, and 12-month LexisNexis article counts, inclusive of the
#          transaction month, interacted with spill exposure.
#
# Author: Jacopo Olivieri
# Date: 2026-06-22
#
# Inputs:
#   - data/processed/lexis_nexis/search1_monthly.parquet
#   - data/processed/cross_section/sales/prior_to_sale/
#   - data/processed/cross_section/rentals/prior_to_rental/
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - scripts/R/09_analysis/05_news/extensive_margin_news_utils.R
#
# Outputs:
#   - output/tables/did_articles_windowed_prior_<WIN>m.tex
#   - output/tables/did_articles_windowed_prior_comparison.tex
#   - output/tables/did_articles_windowed_prior_effect_sizes.csv
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
  "tibble",
  "tinytable"
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


# ==============================================================================
# 1. Configuration
# ==============================================================================
CONFIG <- list(
  analysis_start_month_id = 1L,
  analysis_end_month_id = 36L,
  windows = c(3L, 6L, 12L),
  radius = 250L,
  article_path = here::here(
    "data", "processed", "lexis_nexis", "search1_monthly.parquet"
  ),
  sales_cross_section_path = here::here(
    "data", "processed", "cross_section", "sales", "prior_to_sale"
  ),
  rental_cross_section_path = here::here(
    "data", "processed", "cross_section", "rentals", "prior_to_rental"
  ),
  sales_path = here::here("data", "processed", "house_price.parquet"),
  rental_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  output_dir = here::here("output", "tables"),
  effect_size_output_path = here::here(
    "output", "tables", "did_articles_windowed_prior_effect_sizes.csv"
  )
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
# 3. Shared Formatting Helpers
# ==============================================================================
window_label <- function(window) {
  paste0(window, "-month")
}

salience_col_for_window <- function(window) {
  paste0("log_articles_", window, "m")
}

salience_description <- function(window) {
  paste0("log ", window_label(window), " article count")
}

interaction_term <- function(salience_col) {
  paste0("spill_count_weekly_avg:", salience_col)
}

make_formula <- function(rhs, fixed_effects = NULL) {
  stats::as.formula(paste0(
    "log_price ~ ",
    rhs,
    if (!is.null(fixed_effects)) paste0(" | ", fixed_effects) else ""
  ))
}

format_estimate <- function(model, term, fmt = 3L) {
  coef_table <- fixest::coeftable(model)

  if (!term %in% rownames(coef_table)) {
    stop("Required comparison coefficient is missing: ", term, call. = FALSE)
  }

  estimate <- coef_table[term, "Estimate"]
  std_error <- coef_table[term, "Std. Error"]
  p_value <- coef_table[term, "Pr(>|t|)"]

  if (!all(is.finite(c(estimate, std_error, p_value)))) {
    stop("Required comparison coefficient is non-finite: ", term, call. = FALSE)
  }

  stars <- dplyr::case_when(
    p_value < 0.01 ~ "***",
    p_value < 0.05 ~ "**",
    p_value < 0.1 ~ "*",
    TRUE ~ ""
  )

  paste0(
    formatC(estimate, format = "f", digits = fmt),
    stars,
    " (",
    formatC(std_error, format = "f", digits = fmt),
    ")"
  )
}

extract_estimate <- function(model, term) {
  coef_table <- fixest::coeftable(model)

  if (!term %in% rownames(coef_table)) {
    stop("Required comparison coefficient is missing: ", term, call. = FALSE)
  }

  out <- c(
    estimate = coef_table[term, "Estimate"],
    std_error = coef_table[term, "Std. Error"],
    p_value = coef_table[term, "Pr(>|t|)"]
  )

  if (!all(is.finite(out))) {
    stop("Required comparison coefficient is non-finite: ", term, call. = FALSE)
  }

  out
}


# ==============================================================================
# 4. Data Preparation
# ==============================================================================
article_log_columns <- function(articles) {
  grep("^log_(cumulative_articles|articles_[0-9]+m)$", names(articles), value = TRUE)
}

prepare_sales_analysis_data <- function(rad, articles, sales) {
  cat("Loading cross-section sales data...\n")

  dat_cs_sales <- arrow::open_dataset(CONFIG$sales_cross_section_path) |>
    dplyr::filter(.data$radius == rad) |>
    dplyr::filter(.data$n_spill_sites > 0) |>
    dplyr::collect()

  cat(sprintf("  Found %d sales records within %dm\n", nrow(dat_cs_sales), rad))
  cat("Creating sales analysis dataset...\n")

  log_cols <- article_log_columns(articles)

  dat <- dat_cs_sales |>
    dplyr::inner_join(sales, by = "house_id") |>
    dplyr::inner_join(articles, by = "month_id") |>
    dplyr::mutate(log_price = log(.data$price.y)) |>
    dplyr::filter(
      !is.na(.data$spill_count_weekly_avg),
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

  cat(sprintf("  Final sales dataset: %d transactions\n", nrow(dat)))
  cat(sprintf(
    "  Spill count weekly avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
    mean(dat$spill_count_weekly_avg, na.rm = TRUE),
    stats::sd(dat$spill_count_weekly_avg, na.rm = TRUE),
    min(dat$spill_count_weekly_avg, na.rm = TRUE),
    max(dat$spill_count_weekly_avg, na.rm = TRUE)
  ))

  dat
}

prepare_rental_analysis_data <- function(rad, articles, rentals) {
  cat("Loading cross-section rental data...\n")

  dat_cs_rentals <- arrow::open_dataset(CONFIG$rental_cross_section_path) |>
    dplyr::filter(.data$radius == rad) |>
    dplyr::filter(.data$n_spill_sites > 0) |>
    dplyr::collect()

  cat(sprintf("  Found %d rental records within %dm\n", nrow(dat_cs_rentals), rad))
  cat("Creating rental analysis dataset...\n")

  log_cols <- article_log_columns(articles)

  dat_rental <- dat_cs_rentals |>
    dplyr::inner_join(rentals, by = "rental_id") |>
    dplyr::inner_join(articles, by = "month_id") |>
    dplyr::mutate(log_price = log(.data$listing_price.y)) |>
    dplyr::filter(
      !is.na(.data$spill_count_weekly_avg),
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

  cat(sprintf("  Final rental dataset: %d transactions\n", nrow(dat_rental)))
  cat(sprintf(
    "  Spill count weekly avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
    mean(dat_rental$spill_count_weekly_avg, na.rm = TRUE),
    stats::sd(dat_rental$spill_count_weekly_avg, na.rm = TRUE),
    min(dat_rental$spill_count_weekly_avg, na.rm = TRUE),
    max(dat_rental$spill_count_weekly_avg, na.rm = TRUE)
  ))

  dat_rental
}


# ==============================================================================
# 5. Estimation
# ==============================================================================
estimate_models <- function(dat, dat_rental, salience_col) {
  cat("\nEstimating regression models...\n")

  interaction <- interaction_term(salience_col)

  sales_controls <- paste0(
    "spill_count_weekly_avg + ",
    salience_col,
    " + ",
    interaction,
    " + property_type + old_new + duration"
  )
  rental_controls <- paste0(
    "spill_count_weekly_avg + ",
    salience_col,
    " + ",
    interaction,
    " + property_type + bedrooms + bathrooms"
  )

  models <- list(
    model_sale_1 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", salience_col, " + ", interaction)),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_1b = fixest::feols(
      make_formula(sales_controls),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_2 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", interaction), "msoa + month_id"),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_3 = fixest::feols(
      make_formula(
        paste0(
          "spill_count_weekly_avg + ",
          interaction,
          " + property_type + old_new + duration"
        ),
        "msoa + month_id"
      ),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_4 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", interaction), "lsoa + month_id"),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_5 = fixest::feols(
      make_formula(
        paste0(
          "spill_count_weekly_avg + ",
          interaction,
          " + property_type + old_new + duration"
        ),
        "lsoa + month_id"
      ),
      data = dat,
      vcov = ~lsoa
    ),
    model_rent_1 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", salience_col, " + ", interaction)),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_1b = fixest::feols(
      make_formula(rental_controls),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_2 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", interaction), "msoa + month_id"),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_3 = fixest::feols(
      make_formula(
        paste0(
          "spill_count_weekly_avg + ",
          interaction,
          " + property_type + bedrooms + bathrooms"
        ),
        "msoa + month_id"
      ),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_4 = fixest::feols(
      make_formula(paste0("spill_count_weekly_avg + ", interaction), "lsoa + month_id"),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_5 = fixest::feols(
      make_formula(
        paste0(
          "spill_count_weekly_avg + ",
          interaction,
          " + property_type + bedrooms + bathrooms"
        ),
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
# 6. Export Per-Window Tables
# ==============================================================================
export_window_table <- function(models, window, rad, salience_col) {
  cat("\nExporting regression table...\n")

  interaction <- interaction_term(salience_col)
  salience_label <- paste0("$\\log (\\text{Articles}_{", window, "m})$")
  interaction_label <- paste0(
    "{Spills per week (avg.) \\\\ $\\times$ ",
    "$\\log (\\text{Articles}_{",
    window,
    "m})$}"
  )

  coef_labels <- c(
    "spill_count_weekly_avg" = "Spills per week (avg.)",
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
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure, public attention, and property values. The sample includes all properties within ",
    rad,
    "m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or the log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per week (12/24 count) recorded across all overflows within ",
    rad,
    "m from January 2021 to the transaction date. ",
    salience_label,
    " is the natural logarithm of UK news coverage of sewage spills from LexisNexis over the trailing ",
    window,
    " months, inclusive of the transaction month. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Standard errors clustered at the LSOA level are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
      "Effect of Sewage Spills on Property Values: ",
      "Log ",
      window_label(window),
      " Media Coverage (Prior to Transaction)"
    )
  )

  table_latex <- patch_modelsummary_latex(
    table_latex = table_latex,
    label = paste0("tbl:did-articles-windowed-prior-", window, "m"),
    notes = custom_notes
  )

  output_path <- file.path(
    CONFIG$output_dir,
    paste0("did_articles_windowed_prior_", window, "m.tex")
  )
  ensure_output_dir(output_path)
  writeLines(table_latex, output_path)

  cat(sprintf("LaTeX table exported to: %s\n", output_path))

  invisible(output_path)
}


# ==============================================================================
# 7. Comparison Table and Summary
# ==============================================================================
comparison_specs <- function(dat, dat_rental, models_by_window) {
  measures <- c(
    "Cumulative" = "log_cumulative_articles",
    "3m" = "log_articles_3m",
    "6m" = "log_articles_6m",
    "12m" = "log_articles_12m"
  )

  cumulative_models <- estimate_models(dat, dat_rental, measures[["Cumulative"]]) |>
    preferred_models()

  window_models <- models_by_window[names(measures)[-1L]]

  stats::setNames(
    c(list(cumulative_models), window_models),
    names(measures)
  )
}

write_window_comparison_table <- function(models_by_measure) {
  measure_labels <- names(models_by_measure)
  salience_cols <- c(
    "Cumulative" = "log_cumulative_articles",
    "3m" = "log_articles_3m",
    "6m" = "log_articles_6m",
    "12m" = "log_articles_12m"
  )
  terms <- stats::setNames(
    vapply(salience_cols, interaction_term, character(1)),
    names(salience_cols)
  )

  make_row <- function(fe_slot, label) {
    sales_cells <- vapply(measure_labels, function(measure) {
      format_estimate(models_by_measure[[measure]][[paste0("sale_", fe_slot)]], terms[[measure]])
    }, character(1))
    rent_cells <- vapply(measure_labels, function(measure) {
      format_estimate(models_by_measure[[measure]][[paste0("rent_", fe_slot)]], terms[[measure]])
    }, character(1))

    paste(
      c(label, sales_cells, rent_cells),
      collapse = " & "
    )
  }

  table_lines <- c(
    "\\begin{table}[H]",
    "\\centering",
    "\\caption{Media Coverage and Property Values: Cumulative vs Windowed Article Counts}",
    "\\label{tbl:did-articles-windowed-prior-comparison}",
    "\\begin{tblr}{",
    "width=\\linewidth,",
    "colspec={l *{8}{X[c]}},",
    "colsep=2pt,",
    "cells={font=\\fontsize{8pt}{9pt}\\selectfont},",
    "}",
    "\\hline",
    " & \\SetCell[c=4]{c} House Sales & & & & \\SetCell[c=4]{c} House Rentals & & & \\\\",
    "\\hline",
    paste0(
      paste(
        c("", measure_labels, measure_labels),
        collapse = " & "
      ),
      " \\\\"
    ),
    paste0(make_row("msoa", "Property controls + MSOA FE"), " \\\\"),
    paste0(make_row("lsoa", "Property controls + LSOA FE"), " \\\\"),
    "\\hline",
    "\\end{tblr}",
    "\\vspace{0.3em}",
    "\\begin{minipage}{\\linewidth}",
    "\\footnotesize \\textbf{Notes:} Each cell reports the coefficient on the interaction between weekly spill count and the stated public-attention measure, with LSOA-clustered standard errors in parentheses. All models are estimated at the 250m radius on the same 2021--2023 transaction samples and include property controls, the stated location fixed effects, and month fixed effects. Article-count measures differ in scale, so the comparison is intended to read the sign and significance pattern across windows rather than raw magnitudes. *** p<0.01, ** p<0.05, * p<0.1.",
    "\\end{minipage}",
    "\\end{table}"
  )

  output_path <- file.path(
    CONFIG$output_dir,
    "did_articles_windowed_prior_comparison.tex"
  )
  ensure_output_dir(output_path)
  writeLines(table_lines, output_path)

  cat(sprintf("Comparison table exported to: %s\n", output_path))

  invisible(output_path)
}

classify_pattern <- function(estimates, p_values) {
  finite_idx <- is.finite(estimates) & is.finite(p_values)

  if (!all(finite_idx)) {
    return("incomplete estimates; inspect model output before interpretation")
  }

  window_names <- c("3m", "6m", "12m")
  signs_match <- all(sign(estimates[window_names]) == sign(estimates["Cumulative"]))
  all_significant <- all(p_values[c("Cumulative", window_names)] < 0.1)
  none_significant <- all(p_values[c("Cumulative", window_names)] >= 0.1)
  shorter_weakens <- abs(estimates["3m"]) < abs(estimates["12m"]) &&
    p_values["3m"] >= 0.1

  if (none_significant) {
    "no statistically precise salience pattern"
  } else if (signs_match && all_significant) {
    "robust across cumulative and windowed measures"
  } else if (shorter_weakens) {
    "attenuates for the shortest window, consistent with short-lived salience"
  } else {
    "mixed across windows"
  }
}

print_window_comparison_summary <- function(models_by_measure) {
  salience_cols <- c(
    "Cumulative" = "log_cumulative_articles",
    "3m" = "log_articles_3m",
    "6m" = "log_articles_6m",
    "12m" = "log_articles_12m"
  )
  terms <- stats::setNames(
    vapply(salience_cols, interaction_term, character(1)),
    names(salience_cols)
  )

  cat("\nPreferred-spec interaction summary (LSOA FE + controls):\n")

  sales <- purrr::map_dfr(names(salience_cols), function(measure) {
    est <- extract_estimate(models_by_measure[[measure]]$sale_lsoa, terms[[measure]])
    tibble::tibble(
      measure = measure,
      estimate = est[["estimate"]],
      std_error = est[["std_error"]],
      p_value = est[["p_value"]]
    )
  })
  rentals <- purrr::map_dfr(names(salience_cols), function(measure) {
    est <- extract_estimate(models_by_measure[[measure]]$rent_lsoa, terms[[measure]])
    tibble::tibble(
      measure = measure,
      estimate = est[["estimate"]],
      std_error = est[["std_error"]],
      p_value = est[["p_value"]]
    )
  })

  for (measure in names(salience_cols)) {
    sales_row <- sales[sales$measure == measure, ]
    rentals_row <- rentals[rentals$measure == measure, ]

    cat(sprintf(
      "  %-10s sales: %.3f (SE %.3f, p=%.3f); rentals: %.3f (SE %.3f, p=%.3f)\n",
      measure,
      sales_row$estimate,
      sales_row$std_error,
      sales_row$p_value,
      rentals_row$estimate,
      rentals_row$std_error,
      rentals_row$p_value
    ))
  }

  sales_read <- classify_pattern(
    stats::setNames(sales$estimate, sales$measure),
    stats::setNames(sales$p_value, sales$measure)
  )
  rentals_read <- classify_pattern(
    stats::setNames(rentals$estimate, rentals$measure),
    stats::setNames(rentals$p_value, rentals$measure)
  )

  cat("  Read: sales ", sales_read, "; rentals ", rentals_read, ".\n", sep = "")

  invisible(list(sales = sales, rentals = rentals))
}


# ==============================================================================
# 8. Per-Window Workflow
# ==============================================================================
run_for_window <- function(window, dat, dat_rental) {
  cat("\n========================== Window:", window, "months ==========================\n")

  salience_col <- salience_col_for_window(window)

  cat(sprintf(
    "  %s: mean=%.2f, sd=%.2f, min=%.2f, max=%.2f\n",
    salience_description(window),
    mean(dat[[salience_col]], na.rm = TRUE),
    stats::sd(dat[[salience_col]], na.rm = TRUE),
    min(dat[[salience_col]], na.rm = TRUE),
    max(dat[[salience_col]], na.rm = TRUE)
  ))

  models <- estimate_models(dat, dat_rental, salience_col)
  export_window_table(models, window, CONFIG$radius, salience_col)

  preferred_models(models)
}


# ==============================================================================
# 9. Main Workflow
# ==============================================================================
main <- function() {
  initialise_environment()

  dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

  cat("Loading LexisNexis article counts...\n")
  articles <- load_windowed_articles_data(
    path = CONFIG$article_path,
    windows = CONFIG$windows,
    start_month_id = CONFIG$analysis_start_month_id,
    end_month_id = CONFIG$analysis_end_month_id
  )
  cat(sprintf("  Article counts: %d months\n", nrow(articles)))

  cat("Loading house price data...\n")
  sales <- load_sales_transactions(CONFIG$sales_path)
  cat(sprintf("  Loaded %d transactions\n", nrow(sales)))

  cat("Loading rental transactions...\n")
  rentals <- load_rental_transactions(CONFIG$rental_path)
  cat(sprintf("  Loaded %d rental transactions\n", nrow(rentals)))

  dat <- prepare_sales_analysis_data(CONFIG$radius, articles, sales)
  dat_rental <- prepare_rental_analysis_data(CONFIG$radius, articles, rentals)

  models_by_window <- purrr::map(
    CONFIG$windows,
    run_for_window,
    dat = dat,
    dat_rental = dat_rental
  )
  names(models_by_window) <- paste0(CONFIG$windows, "m")

  cat("\nBuilding cumulative-vs-windowed comparison summary...\n")
  models_by_measure <- comparison_specs(dat, dat_rental, models_by_window)
  write_window_comparison_table(models_by_measure)
  print_window_comparison_summary(models_by_measure)
  write_windowed_article_effect_sizes(
    models_by_measure = models_by_measure,
    salience_cols = c(
      "Cumulative" = "log_cumulative_articles",
      "3m" = "log_articles_3m",
      "6m" = "log_articles_6m",
      "12m" = "log_articles_12m"
    ),
    sales_data = dat,
    rental_data = dat_rental,
    interaction_term_fn = interaction_term,
    margin = "intensive",
    output_path = CONFIG$effect_size_output_path,
    spill_col = "spill_count_weekly_avg"
  )

  cat("\nScript completed successfully.\n")
  cat("  Windows:", paste(CONFIG$windows, collapse = ", "), "months\n")
  cat("  Radius:", CONFIG$radius, "m\n")

  invisible(
    list(
      windows = CONFIG$windows,
      radius = CONFIG$radius,
      models_by_window = models_by_window,
      models_by_measure = models_by_measure,
      effect_size_output_path = CONFIG$effect_size_output_path
    )
  )
}

if (sys.nframe() == 0) {
  main()
}
