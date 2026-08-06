# ==============================================================================
# Lagged Cumulative-Article DiD (Extensive Margin)
# ==============================================================================
#
# Purpose: Re-estimate the headline extensive-margin specification using
#          cumulative article coverage lagged by 0, 3, 6, and 12 months. Main
#          sales and rental-placebo paths use a common January 2022--December
#          2023 sample; full-sample contemporaneous estimates are references.
#
# Inputs:
#   - data/processed/lexis_nexis/search1_monthly.parquet
#   - data/processed/house_price.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#
# Outputs:
#   - output/tables/did_articles_lagged_sales_extensive.tex
#   - output/tables/did_articles_lagged_sales_extensive_effect_sizes.csv
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
  lags = c(0L, 3L, 6L, 12L),
  max_lag = 12L,
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
    "output", "tables", "did_articles_lagged_sales_extensive.tex"
  ),
  effect_size_output_path = here::here(
    "output", "tables",
    "did_articles_lagged_sales_extensive_effect_sizes.csv"
  ),
  table_label = "tbl:did-articles-lagged-sales-extensive"
)


# ==============================================================================
# 3. Data Preparation
# ==============================================================================

#' Build the sales sample once, before joining lag-specific article measures
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

#' Build the rental sample once, before joining lag-specific article measures
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

#' Confirm that a joined article path is finite and cumulative over time
validate_lagged_articles <- function(data, lag) {
  article_path <- data |>
    dplyr::distinct(
      .data$lagged_month_id,
      .data$cumulative_articles,
      .data$log_cumulative_articles
    ) |>
    dplyr::arrange(.data$lagged_month_id)

  stopifnot(
    nrow(article_path) > 0L,
    !anyDuplicated(article_path$lagged_month_id),
    all(is.finite(article_path$cumulative_articles)),
    all(is.finite(article_path$log_cumulative_articles)),
    all(diff(article_path$lagged_month_id) > 0L),
    all(diff(article_path$cumulative_articles) >= 0),
    all(diff(article_path$log_cumulative_articles) >= 0),
    all(data$lagged_month_id == data$month_id - lag)
  )

  invisible(data)
}

#' Join articles for one lag, with an optional common-sample restriction
prepare_lagged_sample <- function(base_sample, articles, lag, common_sample) {
  if (common_sample) {
    base_sample <- restrict_to_common_sample(
      sample = base_sample,
      start_month_id = CONFIG$analysis_start_month_id,
      max_lag = CONFIG$max_lag
    )
    stopifnot(min(base_sample$month_id) == 13L)
  }

  dat <- join_lagged_cumulative_articles(
    sample = base_sample,
    articles = articles,
    lag = lag,
    start_month_id = CONFIG$analysis_start_month_id
  )

  validate_lagged_articles(dat, lag)
  stopifnot(all(is.finite(dat$log_cumulative_articles)))

  dat
}


# ==============================================================================
# 4. Estimation
# ==============================================================================

estimate_preferred_model <- function(data, market) {
  controls <- switch(
    market,
    sales = "property_type + old_new + duration",
    rentals = "property_type + bedrooms + bathrooms",
    stop("Unknown market: ", market, call. = FALSE)
  )

  # Month FE absorbs the main cumulative-articles path. The interaction is
  # identified from within-month differences between the near and far bands.
  model_formula <- stats::as.formula(paste0(
    "log_price ~ near_bin + near_bin:log_cumulative_articles + ", controls,
    " | lsoa + month_id"
  ))

  fixest::feols(model_formula, data = data, vcov = ~lsoa)
}

estimate_common_lag_path <- function(base_sample, articles, market) {
  models <- vector("list", length(CONFIG$lags))
  data_n <- integer(length(CONFIG$lags))
  min_month <- integer(length(CONFIG$lags))
  names(models) <- names(data_n) <- names(min_month) <- paste0("lag_", CONFIG$lags)

  for (lag in CONFIG$lags) {
    dat <- prepare_lagged_sample(
      base_sample = base_sample,
      articles = articles,
      lag = lag,
      common_sample = TRUE
    )

    cat(sprintf(
      "  %s common lag %2d: transaction months %d--%d, article months %d--%d, N = %s\n",
      tools::toTitleCase(market),
      lag,
      min(dat$month_id),
      max(dat$month_id),
      min(dat$lagged_month_id),
      max(dat$lagged_month_id),
      format(nrow(dat), big.mark = ",")
    ))

    lag_name <- paste0("lag_", lag)
    data_n[[lag_name]] <- nrow(dat)
    min_month[[lag_name]] <- min(dat$month_id)
    models[[lag_name]] <- estimate_preferred_model(dat, market)
  }

  model_n <- vapply(models, stats::nobs, numeric(1))

  stopifnot(
    length(unique(data_n)) == 1L,
    length(unique(model_n)) == 1L,
    all(min_month == 13L)
  )

  models
}

estimate_full_reference <- function(base_sample, articles, market) {
  dat <- prepare_lagged_sample(
    base_sample = base_sample,
    articles = articles,
    lag = 0L,
    common_sample = FALSE
  )

  stopifnot(min(dat$month_id) == CONFIG$analysis_start_month_id)
  cat(sprintf(
    "  %s full lag-0 reference: transaction months %d--%d, N = %s\n",
    tools::toTitleCase(market),
    min(dat$month_id),
    max(dat$month_id),
    format(nrow(dat), big.mark = ",")
  ))

  estimate_preferred_model(dat, market)
}


# ==============================================================================
# 5. Output Helpers
# ==============================================================================

export_table <- function(
  sales_full_reference,
  sales_common_models,
  rental_full_reference,
  comparison
) {
  cat("\nExporting primary regression table...\n")

  table_models <- list(
    "Sales: full lag 0" = sales_full_reference,
    "Sales: common lag 0" = sales_common_models$lag_0,
    "Sales: common lag 3" = sales_common_models$lag_3,
    "Sales: common lag 6" = sales_common_models$lag_6,
    "Sales: common lag 12" = sales_common_models$lag_12,
    "Rentals: full lag 0" = rental_full_reference
  )

  gof_map <- tibble::tribble(
    ~raw, ~clean, ~fmt,
    "nobs", "Observations", 0,
    "adj.r.squared", "Adj. R-squared", 3
  )
  add_rows <- tibble::tribble(
    ~term, ~`Sales: full lag 0`, ~`Sales: common lag 0`,
    ~`Sales: common lag 3`, ~`Sales: common lag 6`,
    ~`Sales: common lag 12`, ~`Rentals: full lag 0`,
    "Sample", "Full", "Common", "Common", "Common", "Common", "Full",
    "Property controls", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes",
    "Location FE", "LSOA", "LSOA", "LSOA", "LSOA", "LSOA", "LSOA",
    "Time FE", "Month", "Month", "Month", "Month", "Month", "Month"
  )
  attr(add_rows, "position") <- "coef_end"

  custom_notes <- paste0(
    "note{}={\\footnotesize{\\textbf{Notes:} This table reports the ",
    "coefficient on Near bin $\\times$ $\\log(\\text{Articles})$ from the ",
    "preferred extensive-margin specification. ",
    comparison_note_text(comparison),
    "$\\log(\\text{Articles})$ is the natural logarithm of cumulative UK ",
    "LexisNexis sewage coverage since January 2021, evaluated $L$ months before ",
    "the transaction month. Common-sample columns retain January 2022--December ",
    "2023 (month_id 13--36) at every lag; full-reference columns use January ",
    "2021--December 2023 with contemporaneous coverage. The rental common-sample ",
    "placebo path is reported in the component results file. All models include ",
    "property controls, LSOA fixed effects, and month fixed effects. Standard ",
    "errors are clustered by LSOA. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
      "near_bin:log_cumulative_articles" =
        "{Near bin \\\\ $\\times$ $\\log(\\text{Articles})$}"
    ),
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = paste0(
      "Lagged Cumulative Media Coverage and Property Values: Extensive Margin (",
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

model_result_row <- function(model, market, lag, sample) {
  coef_stats <- extract_fixest_term(
    model,
    "near_bin:log_cumulative_articles"
  )
  critical_value <- stats::qnorm(0.975)

  tibble::tibble(
    margin = "extensive",
    market = market,
    measure = "articles",
    radius = 500L,
    lag = as.integer(lag),
    sample = sample,
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

export_effect_sizes <- function(
  sales_full_reference,
  sales_common_models,
  rental_full_reference,
  rental_common_models
) {
  rows <- list(
    model_result_row(
      sales_full_reference, "sales", 0L, "full_reference"
    ),
    model_result_row(
      rental_full_reference, "rentals", 0L, "full_reference"
    )
  )

  for (market in c("sales", "rentals")) {
    market_models <- if (market == "sales") {
      sales_common_models
    } else {
      rental_common_models
    }
    for (lag in CONFIG$lags) {
      rows[[length(rows) + 1L]] <- model_result_row(
        market_models[[paste0("lag_", lag)]],
        market,
        lag,
        "common"
      )
    }
  }

  out <- dplyr::bind_rows(rows)
  output_columns <- c(
    "margin", "market", "measure", "radius", "lag", "sample",
    "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
  )
  key_cols <- c("margin", "market", "measure", "radius", "lag", "sample")
  expected_keys <- dplyr::bind_rows(
    tidyr::expand_grid(
      market = c("sales", "rentals"),
      lag = CONFIG$lags,
      sample = "common"
    ),
    tidyr::expand_grid(
      market = c("sales", "rentals"),
      lag = 0L,
      sample = "full_reference"
    )
  )

  stopifnot(
    identical(names(out), output_columns),
    nrow(out) == nrow(expected_keys),
    !anyDuplicated(out[key_cols]),
    all(stats::complete.cases(out)),
    all(is.finite(unlist(out[c(
      "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
    )]))),
    nrow(dplyr::anti_join(
      expected_keys,
      out,
      by = c("market", "lag", "sample")
    )) == 0L,
    all(out$margin == "extensive"),
    all(out$measure == "articles"),
    all(out$radius == 500L),
    identical(sort(unique(out$sample)), c("common", "full_reference"))
  )

  common_n <- out |>
    dplyr::filter(.data$sample == "common") |>
    dplyr::group_by(.data$market) |>
    dplyr::summarise(n_values = dplyr::n_distinct(.data$n), .groups = "drop")
  stopifnot(all(common_n$n_values == 1L))

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
  stopifnot(
    identical(CONFIG$lags, c(0L, 3L, 6L, 12L)),
    CONFIG$max_lag == max(CONFIG$lags),
    CONFIG$analysis_start_month_id + CONFIG$max_lag == 13L,
    identical(
      unname(unlist(CONFIG$comparison[c(
        "near_min", "near_max", "far_min", "far_max"
      )])),
      c(0L, 500L, 1000L, 2000L)
    )
  )

  comparison <- validate_comparison_config(CONFIG$comparison)
  articles <- load_articles_data(
    path = CONFIG$article_path,
    start_month_id = CONFIG$analysis_start_month_id,
    end_month_id = CONFIG$analysis_end_month_id
  )

  stopifnot(
    min(articles$month_id) == 1L,
    max(articles$month_id) == 36L,
    !anyDuplicated(articles$month_id),
    all(is.finite(articles$cumulative_articles)),
    all(is.finite(articles$log_cumulative_articles)),
    all(diff(articles$cumulative_articles) >= 0),
    all(diff(articles$log_cumulative_articles) >= 0)
  )

  sales_base <- prepare_sales_base_sample(comparison)
  rental_base <- prepare_rental_base_sample(comparison)

  cat("\nEstimating common-sample sales lag path...\n")
  sales_common <- estimate_common_lag_path(sales_base, articles, "sales")
  cat("\nEstimating common-sample rental placebo lag path...\n")
  rental_common <- estimate_common_lag_path(rental_base, articles, "rentals")

  cat("\nEstimating full-sample lag-0 references...\n")
  sales_full <- estimate_full_reference(sales_base, articles, "sales")
  rental_full <- estimate_full_reference(rental_base, articles, "rentals")

  export_table(
    sales_full_reference = sales_full,
    sales_common_models = sales_common,
    rental_full_reference = rental_full,
    comparison = comparison
  )
  effect_sizes <- export_effect_sizes(
    sales_full_reference = sales_full,
    sales_common_models = sales_common,
    rental_full_reference = rental_full,
    rental_common_models = rental_common
  )

  table_text <- paste(readLines(CONFIG$output_path, warn = FALSE), collapse = "\n")
  stopifnot(
    grepl("near_bin:log_cumulative_articles", table_text, fixed = TRUE) ||
      grepl("Near bin", table_text, fixed = TRUE),
    file.exists(CONFIG$output_path),
    file.info(CONFIG$output_path)$size > 0L,
    file.exists(CONFIG$effect_size_output_path),
    file.info(CONFIG$effect_size_output_path)$size > 0L,
    nrow(effect_sizes) == 2L * length(CONFIG$lags) + 2L
  )

  cat("\nScript completed successfully.\n")

  invisible(list(
    table_path = CONFIG$output_path,
    effect_size_path = CONFIG$effect_size_output_path,
    sales_common = sales_common,
    rental_common = rental_common,
    sales_full = sales_full,
    rental_full = rental_full
  ))
}

if (sys.nframe() == 0) {
  main()
}
