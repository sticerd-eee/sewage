# ==============================================================================
# News/Information Event Study (Google Trends Peak, Extensive Margin)
# ==============================================================================
#
# Purpose: Estimate the dynamic near-versus-far price gap around the Google
#          Trends peak quarter using a quarter-level event study. Treatment is
#          configurable proximity to the nearest mapped overflow rather than
#          realised spill intensity.
#
# Author: Jacopo Olivieri
# Date: 2026-04-07
# Date Modified: 2026-04-07
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - scripts/R/09_analysis/05_news/extensive_margin_news_utils.R
#
# Outputs:
#   - output/tables/es_trends_prior_extensive.tex
#   - output/figures/es_trends_prior_extensive_sales.pdf
#   - output/figures/es_trends_prior_extensive_rentals.pdf
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
  "ggplot2",
  "glue",
  "here",
  "modelsummary",
  "readxl",
  "rio",
  "stringr",
  "tibble",
  "viridis"
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
  base_year = 2021L,
  google_trends_sheet = "united_kingdom",
  event_min = -4L,
  event_max = 5L,
  event_ref = -1L,
  plot_width_cm = 23,
  plot_height_cm = 14,
  plot_dpi = 300,
  comparison = list(
    comparison_id = "500_vs_1000_2000",
    comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L,
    near_max = 500L,
    far_min = 1000L,
    far_max = 2000L
  ),
  google_trends_path = here::here(
    "data", "raw", "google_trends", "google_trends_uk.xlsx"
  ),
  sales_path = here::here("data", "processed", "house_price.parquet"),
  sales_lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  rental_path = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  rental_lookup_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  table_output_path = here::here("output", "tables", "es_trends_prior_extensive.tex"),
  sales_plot_path = here::here(
    "output", "figures", "es_trends_prior_extensive_sales.pdf"
  ),
  rental_plot_path = here::here(
    "output", "figures", "es_trends_prior_extensive_rentals.pdf"
  ),
  table_label = "tbl:es-trends-prior-extensive"
)


# ==============================================================================
# 2. Package Management
# ==============================================================================
initialise_environment <- function() {
  invisible(lapply(c("dplyr", "ggplot2", "fixest"), function(pkg) {
    suppressPackageStartupMessages(library(pkg, character.only = TRUE))
  }))
}


# ==============================================================================
# 3. Plot Style
# ==============================================================================
theme_pref <- ggplot2::theme_minimal() +
  ggplot2::theme(
    text = ggplot2::element_text(size = 12),
    plot.title = ggplot2::element_text(
      face = "bold",
      size = 12,
      margin = ggplot2::margin(b = 10, unit = "pt")
    ),
    axis.title = ggplot2::element_text(face = "bold", size = 10),
    axis.text = ggplot2::element_text(angle = 45, hjust = 1, vjust = 1, size = 8),
    panel.grid.minor = ggplot2::element_blank(),
    panel.grid.major.x = ggplot2::element_line(color = "gray95"),
    panel.grid.major.y = ggplot2::element_line(color = "gray95"),
    legend.position = "bottom",
    legend.title = ggplot2::element_text(face = "bold", size = 10),
    legend.text = ggplot2::element_text(size = 8),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Data Preparation
# ==============================================================================

#' Prepare the sales estimation sample
#'
#' @param comparison Validated comparison config.
#' @param peak_info List returned by `load_google_trends_peak()`.
#' @return Tibble ready for estimation.
prepare_sales_analysis_data <- function(comparison, peak_info) {
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
    mutate(
      log_price = log(.data$price),
      event_qtr = as.integer(.data$qtr_id - peak_info$peak_qtr_id)
    ) |>
    filter(
      !is.na(.data$lsoa),
      !is.na(.data$qtr_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$old_new),
      !is.na(.data$duration),
      dplyr::between(.data$event_qtr, CONFIG$event_min, CONFIG$event_max),
      is.finite(.data$log_price)
    ) |>
    standardise_sales_estimation_data()

  cat(sprintf(
    "  Google Trends peak: %s (qtr_id = %d)\n",
    peak_info$peak_date,
    peak_info$peak_qtr_id
  ))
  print_extensive_margin_summary(dat, "Sales", comparison)
  cat(sprintf(
    "    Event window [%d, %d] quarters from peak\n",
    CONFIG$event_min,
    CONFIG$event_max
  ))

  dat
}

#' Prepare the rental estimation sample
#'
#' @param comparison Validated comparison config.
#' @param peak_info List returned by `load_google_trends_peak()`.
#' @return Tibble ready for estimation.
prepare_rental_analysis_data <- function(comparison, peak_info) {
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
    mutate(
      log_price = log(.data$listing_price),
      event_qtr = as.integer(.data$qtr_id - peak_info$peak_qtr_id)
    ) |>
    filter(
      !is.na(.data$lsoa),
      !is.na(.data$qtr_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$bedrooms),
      !is.na(.data$bathrooms),
      dplyr::between(.data$event_qtr, CONFIG$event_min, CONFIG$event_max),
      is.finite(.data$log_price)
    ) |>
    standardise_rental_estimation_data()

  print_extensive_margin_summary(dat_rental, "Rentals", comparison)
  cat(sprintf(
    "    Event window [%d, %d] quarters from peak\n",
    CONFIG$event_min,
    CONFIG$event_max
  ))

  dat_rental
}


# ==============================================================================
# 5. Estimation
# ==============================================================================

#' Estimate the extensive-margin event-study models
#'
#' @param dat Sales estimation sample.
#' @param dat_rental Rental estimation sample.
#' @return Named list of fitted `fixest` models.
estimate_models <- function(dat, dat_rental) {
  cat("\nEstimating regression models...\n")

  event_term <- paste0(
    "i(event_qtr, near_bin, ref = ",
    CONFIG$event_ref,
    ")"
  )

  models <- list(
    model_sale_1 = fixest::feols(
      stats::as.formula(paste0("log_price ~ ", event_term)),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_2 = fixest::feols(
      stats::as.formula(paste0("log_price ~ ", event_term, " | lsoa + qtr_id")),
      data = dat,
      vcov = ~lsoa
    ),
    model_sale_3 = fixest::feols(
      stats::as.formula(paste0(
        "log_price ~ ",
        event_term,
        " + property_type + old_new + duration | lsoa + qtr_id"
      )),
      data = dat,
      vcov = ~lsoa
    ),
    model_rent_1 = fixest::feols(
      stats::as.formula(paste0("log_price ~ ", event_term)),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_2 = fixest::feols(
      stats::as.formula(paste0("log_price ~ ", event_term, " | lsoa + qtr_id")),
      data = dat_rental,
      vcov = ~lsoa
    ),
    model_rent_3 = fixest::feols(
      stats::as.formula(paste0(
        "log_price ~ ",
        event_term,
        " + property_type + bedrooms + bathrooms | lsoa + qtr_id"
      )),
      data = dat_rental,
      vcov = ~lsoa
    )
  )

  cat("  Using LSOA-clustered SEs\n")

  models
}


# ==============================================================================
# 6. Export Table
# ==============================================================================

#' Export the regression table
#'
#' @param models Named list of fitted models.
#' @param comparison Validated comparison config.
#' @return Output path, invisibly.
export_table <- function(models, comparison) {
  cat("\nExporting regression table...\n")

  all_models <- unname(models)
  coef_labels <- build_event_coef_map(all_models, event_var = "event_qtr")

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
    "(columns 4--6). Event time is measured in quarters relative to the Google ",
    "Trends peak quarter. Estimates report the interaction between near-bin status ",
    "and event-time dummies, with the quarter immediately before the peak ",
    "(t = ",
    CONFIG$event_ref,
    ") as the reference period. ",
    comparison_note_text(comparison),
    "Near bin is an indicator equal to one for properties in the ",
    comparison$near_band_label,
    " band and zero for properties in the ",
    comparison$far_band_label,
    " band. ",
    sprintf(
      "The event window is [%d, %d] quarters. ",
      CONFIG$event_min,
      CONFIG$event_max
    ),
    "Standard errors clustered at the LSOA level are reported in parentheses. ",
    "Property controls include type (flat, semi-detached, terraced, other), new ",
    "build status, and tenure for sales; and type (bungalow, detached, ",
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
      "Event Study of Overflow Proximity Around Google Trends Peak (Prior to Transaction, ",
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

  ensure_output_dir(CONFIG$table_output_path)
  writeLines(table_latex, CONFIG$table_output_path)

  cat(sprintf("LaTeX table exported to: %s\n", CONFIG$table_output_path))

  invisible(CONFIG$table_output_path)
}


# ==============================================================================
# 7. Plot Export
# ==============================================================================

#' Extract event-study coefficients and confidence intervals for plotting
#'
#' @param model Fitted `fixest` model.
#' @return Tibble with event time, coefficient estimate, and 95% confidence band.
extract_event_plot_data <- function(model) {
  beta <- stats::coef(model)
  vcov_mat <- stats::vcov(model)
  coef_names <- names(beta)
  event_terms <- coef_names[grepl("event_qtr", coef_names)]

  if (length(event_terms) == 0L) {
    stop("No event-study coefficients were found in the fitted model.", call. = FALSE)
  }

  event_time <- as.integer(stringr::str_extract(event_terms, "-?\\d+"))
  std_error <- sqrt(diag(vcov_mat))[event_terms]
  estimate <- beta[event_terms]

  tibble::tibble(
    term = event_terms,
    event_time = event_time,
    estimate = as.numeric(estimate),
    std_error = as.numeric(std_error)
  ) |>
    filter(
      !is.na(.data$event_time),
      dplyr::between(.data$event_time, CONFIG$event_min, CONFIG$event_max)
    ) |>
    mutate(
      ci_low = .data$estimate - 1.96 * .data$std_error,
      ci_high = .data$estimate + 1.96 * .data$std_error,
      group = factor(
        ifelse(.data$event_time < 0, "Pre", "Post"),
        levels = c("Pre", "Post")
      )
    ) |>
    arrange(.data$event_time)
}

#' Build an event-study plot
#'
#' @param model Fitted `fixest` model.
#' @param subtitle Plot subtitle.
#' @param caption Plot caption.
#' @return `ggplot` object.
build_event_plot <- function(model, subtitle, caption) {
  plot_data <- extract_event_plot_data(model)

  ggplot(plot_data, aes(x = .data$event_time, y = .data$estimate, color = .data$group)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_vline(xintercept = CONFIG$event_ref, linetype = "dashed", color = "gray50") +
    geom_errorbar(
      aes(ymin = .data$ci_low, ymax = .data$ci_high),
      width = 0.2
    ) +
    geom_point(size = 1.2) +
    scale_color_manual(
      values = c(
        "Pre" = viridis::viridis(1, option = "magma"),
        "Post" = viridis::viridis(3, option = "magma")[2]
      ),
      breaks = c("Pre", "Post")
    ) +
    labs(
      x = "Time (quarters from peak)",
      y = "Estimate",
      title = NULL,
      subtitle = subtitle,
      caption = caption
    ) +
    theme_pref +
    theme(plot.caption = element_text(hjust = 0))
}

#' Export the preferred-spec event-study plots
#'
#' @param models Named list of fitted models.
#' @param comparison Validated comparison config.
#' @return Named list of plot paths, invisibly.
export_plots <- function(models, comparison) {
  cat("\nGenerating event-study plots...\n")

  sales_caption <- paste0(
    "log_price ~ i(event_qtr, near_bin, ref = ",
    CONFIG$event_ref,
    ") + property controls | lsoa + qtr_id"
  )
  rental_caption <- paste0(
    "log_price ~ i(event_qtr, near_bin, ref = ",
    CONFIG$event_ref,
    ") + property controls | lsoa + qtr_id"
  )

  sales_plot <- build_event_plot(
    model = models$model_sale_3,
    subtitle = paste0(
      "Sales: LSOA + Quarter FE with property controls (",
      comparison$comparison_label,
      ")"
    ),
    caption = sales_caption
  )
  rental_plot <- build_event_plot(
    model = models$model_rent_3,
    subtitle = paste0(
      "Rentals: LSOA + Quarter FE with property controls (",
      comparison$comparison_label,
      ")"
    ),
    caption = rental_caption
  )

  ensure_output_dir(CONFIG$sales_plot_path)
  ensure_output_dir(CONFIG$rental_plot_path)

  ggplot2::ggsave(
    filename = CONFIG$sales_plot_path,
    plot = sales_plot,
    width = CONFIG$plot_width_cm,
    height = CONFIG$plot_height_cm,
    dpi = CONFIG$plot_dpi,
    units = "cm"
  )
  ggplot2::ggsave(
    filename = CONFIG$rental_plot_path,
    plot = rental_plot,
    width = CONFIG$plot_width_cm,
    height = CONFIG$plot_height_cm,
    dpi = CONFIG$plot_dpi,
    units = "cm"
  )

  cat("Event-study plots exported to output/figures\n")

  invisible(
    list(
      sales_plot_path = CONFIG$sales_plot_path,
      rental_plot_path = CONFIG$rental_plot_path
    )
  )
}


# ==============================================================================
# 8. Main Workflow
# ==============================================================================
main <- function() {
  initialise_environment()

  comparison <- validate_comparison_config(CONFIG$comparison)
  peak_info <- load_google_trends_peak(
    path = CONFIG$google_trends_path,
    sheet = CONFIG$google_trends_sheet,
    base_year = CONFIG$base_year
  )

  dat <- prepare_sales_analysis_data(comparison, peak_info)
  dat_rental <- prepare_rental_analysis_data(comparison, peak_info)
  models <- estimate_models(dat, dat_rental)
  export_table(models, comparison)
  export_plots(models, comparison)

  cat("\nScript completed successfully.\n")

  invisible(
    list(
      table_output_path = CONFIG$table_output_path,
      sales_plot_path = CONFIG$sales_plot_path,
      rental_plot_path = CONFIG$rental_plot_path,
      comparison = comparison$comparison_label
    )
  )
}

if (sys.nframe() == 0) {
  main()
}
