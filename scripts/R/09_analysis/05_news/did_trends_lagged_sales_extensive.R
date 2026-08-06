# ==============================================================================
# Lagged Google Trends Peak DiD (Extensive Margin)
# ==============================================================================
#
# Purpose: Re-estimate the headline extensive-margin sales specification using
#          post-peak indicators lagged by 0, 3, 6, and 12 months. The same lag
#          path for rentals is a secondary placebo; only contemporaneous
#          rentals appear in the primary table.
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#
# Outputs:
#   - output/tables/did_trends_lagged_sales_extensive.tex
#   - output/tables/did_trends_lagged_sales_extensive_effect_sizes.csv
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
  "readxl",
  "rio",
  "tibble",
  "tidyr"
)

check_required_packages(REQUIRED_PACKAGES)

source(
  here::here(
    "scripts", "R", "09_analysis", "05_news",
    "extensive_margin_news_utils.R"
  ),
  local = TRUE
)
source(
  here::here(
    "scripts", "R", "09_analysis", "05_news", "news_lag_utils.R"
  ),
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
  base_year = 2021L,
  lags = c(0L, 3L, 6L, 12L),
  google_trends_sheet = "united_kingdom",
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
  sales_lookup_path = here::here(
    "data", "processed", "spill_house_lookup.parquet"
  ),
  rental_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals.parquet"
  ),
  rental_lookup_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  output_path = here::here(
    "output", "tables", "did_trends_lagged_sales_extensive.tex"
  ),
  effect_size_output_path = here::here(
    "output", "tables",
    "did_trends_lagged_sales_extensive_effect_sizes.csv"
  ),
  table_label = "tbl:did-trends-lagged-sales-extensive"
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

#' Build the sales sample once, before assigning lag-specific post indicators
prepare_sales_base_sample <- function(comparison) {
  cat("Loading sales transactions...\n")

  sales <- load_sales_transactions(CONFIG$sales_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    )

  sales_lookup <- load_nearest_distance_lookup(
    path = CONFIG$sales_lookup_path,
    id_col = "house_id",
    max_distance = comparison$far_max
  )

  cat("Creating sales base sample...\n")

  dat <- build_extensive_margin_sample(
    transactions = sales,
    lookup = sales_lookup,
    id_col = "house_id",
    comparison = comparison
  ) |>
    dplyr::mutate(log_price = log(.data$price)) |>
    dplyr::filter(
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

  print_extensive_margin_summary(dat, "Sales", comparison)
  dat
}

#' Build the rental sample once, before assigning lag-specific post indicators
prepare_rental_base_sample <- function(comparison) {
  cat("Loading rental transactions...\n")

  rentals <- load_rental_transactions(CONFIG$rental_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    )

  rental_lookup <- load_nearest_distance_lookup(
    path = CONFIG$rental_lookup_path,
    id_col = "rental_id",
    max_distance = comparison$far_max
  )

  cat("Creating rental base sample...\n")

  dat <- build_extensive_margin_sample(
    transactions = rentals,
    lookup = rental_lookup,
    id_col = "rental_id",
    comparison = comparison
  ) |>
    dplyr::mutate(log_price = log(.data$listing_price)) |>
    dplyr::filter(
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

  print_extensive_margin_summary(dat, "Rentals", comparison)
  dat
}


# ==============================================================================
# 4. Estimation
# ==============================================================================

estimate_lag_path <- function(base_sample, peak_month_id, market) {
  controls <- switch(
    market,
    sales = "property_type + old_new + duration",
    rentals = "property_type + bedrooms + bathrooms",
    stop("Unknown market: ", market, call. = FALSE)
  )

  models <- vector("list", length(CONFIG$lags))
  names(models) <- paste0("lag_", CONFIG$lags)

  for (lag in CONFIG$lags) {
    dat <- base_sample |>
      dplyr::mutate(
        post = shifted_post_indicator(.data$month_id, peak_month_id, lag)
      )

    expected_cut <- peak_month_id + lag
    observed_post_months <- sort(unique(dat$month_id[dat$post == 1L]))
    observed_pre_months <- sort(unique(dat$month_id[dat$post == 0L]))

    stopifnot(
      all(dat$post == as.integer(dat$month_id >= expected_cut)),
      length(observed_post_months) > 0L,
      min(observed_post_months) == expected_cut,
      max(observed_pre_months) == expected_cut - 1L
    )
    if (lag == 12L) {
      # Lag 12 shifts the cut to August 2023, leaving only five post months.
      stopifnot(length(observed_post_months) == 5L)
    }

    # Month FE absorbs the monthly main attention effect; identification comes
    # from the near-bin interaction. This is unchanged by shifting the cutoff.
    model_formula <- stats::as.formula(paste0(
      "log_price ~ near_bin + near_bin:post + ", controls,
      " | lsoa + month_id"
    ))

    cat(sprintf(
      "  %s lag %2d: post starts at month_id %d; N = %s; post months = %d\n",
      tools::toTitleCase(market),
      lag,
      expected_cut,
      format(nrow(dat), big.mark = ","),
      length(observed_post_months)
    ))

    models[[paste0("lag_", lag)]] <- fixest::feols(
      model_formula,
      data = dat,
      vcov = ~lsoa
    )
  }

  model_n <- vapply(models, stats::nobs, numeric(1))
  stopifnot(length(unique(model_n)) == 1L)

  models
}


# ==============================================================================
# 5. Output Helpers
# ==============================================================================

export_table <- function(sales_models, rental_models, comparison, peak_info) {
  cat("\nExporting primary regression table...\n")

  table_models <- list(
    "Sales: lag 0" = sales_models$lag_0,
    "Sales: lag 3" = sales_models$lag_3,
    "Sales: lag 6" = sales_models$lag_6,
    "Sales: lag 12" = sales_models$lag_12,
    "Rentals: lag 0" = rental_models$lag_0
  )

  gof_map <- tibble::tribble(
    ~raw, ~clean, ~fmt,
    "nobs", "Observations", 0,
    "adj.r.squared", "Adj. R-squared", 3
  )
  add_rows <- tibble::tribble(
    ~term, ~`Sales: lag 0`, ~`Sales: lag 3`, ~`Sales: lag 6`,
    ~`Sales: lag 12`, ~`Rentals: lag 0`,
    "Property controls", "Yes", "Yes", "Yes", "Yes", "Yes",
    "Location FE", "LSOA", "LSOA", "LSOA", "LSOA", "LSOA",
    "Time FE", "Month", "Month", "Month", "Month", "Month"
  )
  attr(add_rows, "position") <- "coef_end"

  custom_notes <- paste0(
    "note{}={\\footnotesize{\\textbf{Notes:} This table reports the ",
    "coefficient on Near bin $\\times$ Post from the preferred extensive-margin ",
    "specification. ",
    comparison_note_text(comparison),
    "Post is based on the Google Trends peak in ", peak_info$peak_date,
    " (month_id ", peak_info$peak_month_id,
    "); lag $L$ shifts the post threshold forward by $L$ months. All models ",
    "retain the full January 2021--December 2023 sample. The lag-12 threshold ",
    "leaves only five post months and should be interpreted cautiously. The ",
    "rental column is the contemporaneous benchmark; rental lag paths are ",
    "reported in the component results file. All models include property ",
    "controls, LSOA fixed effects, and month fixed effects. Standard errors are ",
    "clustered by LSOA. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  table_latex <- modelsummary::modelsummary(
    table_models,
    output = "latex",
    escape = FALSE,
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = c(
      "near_bin:post" = "{Near bin \\\\ $\\times$ Post}"
    ),
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = paste0(
      "Lagged Public Attention and Property Values: Extensive Margin (",
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

model_result_row <- function(model, market, lag) {
  coef_stats <- extract_fixest_term(model, "near_bin:post")
  critical_value <- stats::qnorm(0.975)

  tibble::tibble(
    margin = "extensive",
    market = market,
    measure = "post",
    radius = 500L,
    lag = as.integer(lag),
    sample = "full",
    estimate = coef_stats[["estimate"]],
    std_error = coef_stats[["std_error"]],
    conf_low = coef_stats[["estimate"]] -
      critical_value * coef_stats[["std_error"]],
    conf_high = coef_stats[["estimate"]] +
      critical_value * coef_stats[["std_error"]],
    p_value = coef_stats[["p_value"]],
    n = as.integer(stats::nobs(model))
  )
}

export_effect_sizes <- function(sales_models, rental_models) {
  rows <- list()
  row_id <- 0L

  for (market in c("sales", "rentals")) {
    market_models <- if (market == "sales") sales_models else rental_models
    for (lag in CONFIG$lags) {
      row_id <- row_id + 1L
      rows[[row_id]] <- model_result_row(
        model = market_models[[paste0("lag_", lag)]],
        market = market,
        lag = lag
      )
    }
  }

  out <- dplyr::bind_rows(rows)
  key_cols <- c("margin", "market", "measure", "radius", "lag", "sample")
  expected_keys <- tidyr::expand_grid(
    market = c("sales", "rentals"),
    lag = CONFIG$lags
  )

  stopifnot(
    identical(
      names(out),
      c(
        "margin", "market", "measure", "radius", "lag", "sample",
        "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
      )
    ),
    nrow(out) == nrow(expected_keys),
    !anyDuplicated(out[key_cols]),
    all(stats::complete.cases(out)),
    all(is.finite(unlist(out[c(
      "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
    )]))),
    nrow(dplyr::anti_join(expected_keys, out, by = c("market", "lag"))) == 0L,
    all(out$margin == "extensive"),
    all(out$measure == "post"),
    all(out$radius == 500L),
    all(out$sample == "full")
  )

  ensure_output_dir(CONFIG$effect_size_output_path)
  utils::write.csv(
    out,
    CONFIG$effect_size_output_path,
    row.names = FALSE,
    na = ""
  )
  cat(sprintf(
    "Component results exported to: %s\n",
    CONFIG$effect_size_output_path
  ))

  invisible(out)
}


# ==============================================================================
# 6. Main Workflow
# ==============================================================================

main <- function() {
  initialise_environment()

  stopifnot(
    identical(CONFIG$lags, c(0L, 3L, 6L, 12L)),
    identical(
      unname(unlist(CONFIG$comparison[c(
        "near_min", "near_max", "far_min", "far_max"
      )])),
      c(0L, 500L, 1000L, 2000L)
    )
  )

  comparison <- validate_comparison_config(CONFIG$comparison)
  peak_info <- load_google_trends_peak(
    path = CONFIG$google_trends_path,
    sheet = CONFIG$google_trends_sheet,
    base_year = CONFIG$base_year
  )
  stopifnot(peak_info$peak_month_id == 20L)

  cat(sprintf(
    "Google Trends peak: %s (month_id = %d)\n",
    peak_info$peak_date,
    peak_info$peak_month_id
  ))

  sales_base <- prepare_sales_base_sample(comparison)
  rental_base <- prepare_rental_base_sample(comparison)

  cat("\nEstimating full-sample sales lag path...\n")
  sales_models <- estimate_lag_path(sales_base, peak_info$peak_month_id, "sales")
  cat("\nEstimating full-sample rental placebo lag path...\n")
  rental_models <- estimate_lag_path(
    rental_base,
    peak_info$peak_month_id,
    "rentals"
  )

  export_table(sales_models, rental_models, comparison, peak_info)
  effect_sizes <- export_effect_sizes(sales_models, rental_models)

  stopifnot(
    file.exists(CONFIG$output_path),
    file.info(CONFIG$output_path)$size > 0L,
    file.exists(CONFIG$effect_size_output_path),
    file.info(CONFIG$effect_size_output_path)$size > 0L,
    nrow(effect_sizes) == 2L * length(CONFIG$lags)
  )

  cat("\nScript completed successfully.\n")

  invisible(list(
    table_path = CONFIG$output_path,
    effect_size_path = CONFIG$effect_size_output_path,
    sales_models = sales_models,
    rental_models = rental_models
  ))
}

if (sys.nframe() == 0) {
  main()
}
