# ==============================================================================
# Lagged Cumulative Articles DiD (Intensive-Margin Extension)
# ==============================================================================
#
# Purpose: Estimate the preferred intensive-margin sales specification over a
#          cumulative-article lag-by-radius grid. Main sales comparisons use a
#          common Jan 2022--Dec 2023 sample; full-sample contemporaneous sales
#          and rental estimates are reported as references.
#
# Inputs:
#   - data/processed/lexis_nexis/search1_monthly.parquet
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/cross_section/sales/prior_to_sale/
#   - data/processed/cross_section/rentals/prior_to_rental/
#
# Outputs:
#   - output/tables/did_articles_lagged_sales_grid.tex
#   - output/tables/did_articles_lagged_sales_effect_sizes.csv
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
  "rio",
  "tibble",
  "tidyr"
)

check_required_packages(REQUIRED_PACKAGES)

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
  radii = c(250L, 500L, 1000L),
  lags = c(0L, 3L, 6L, 12L),
  max_lag = 12L,
  analysis_start_month_id = 1L,
  analysis_end_month_id = 36L,
  articles_path = here::here(
    "data", "processed", "lexis_nexis", "search1_monthly.parquet"
  ),
  sales_path = here::here("data", "processed", "house_price.parquet"),
  rental_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals.parquet"
  ),
  sales_cross_section_path = here::here(
    "data", "processed", "cross_section", "sales", "prior_to_sale"
  ),
  rental_cross_section_path = here::here(
    "data", "processed", "cross_section", "rentals", "prior_to_rental"
  ),
  output_path = here::here(
    "output", "tables", "did_articles_lagged_sales_grid.tex"
  ),
  effect_size_output_path = here::here(
    "output", "tables", "did_articles_lagged_sales_effect_sizes.csv"
  ),
  table_label = "tbl:did-articles-lagged-sales-grid"
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

load_articles <- function() {
  articles <- arrow::read_parquet(CONFIG$articles_path) |>
    dplyr::filter(
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    ) |>
    dplyr::arrange(.data$month_id) |>
    dplyr::mutate(
      cumulative_articles = cumsum(.data$article_count),
      log_cumulative_articles = log(.data$cumulative_articles)
    ) |>
    dplyr::select(
      "month_id", "cumulative_articles", "log_cumulative_articles"
    )

  stopifnot(
    nrow(articles) == 36L,
    !anyDuplicated(articles$month_id),
    all(articles$month_id == seq.int(1L, 36L)),
    all(is.finite(articles$cumulative_articles)),
    all(is.finite(articles$log_cumulative_articles)),
    all(diff(articles$cumulative_articles) >= 0),
    all(diff(articles$log_cumulative_articles) >= 0)
  )

  cat(sprintf(
    "Loaded %d article months; cumulative count ranges from %d to %d.\n",
    nrow(articles), min(articles$cumulative_articles),
    max(articles$cumulative_articles)
  ))
  articles
}

load_global_transactions <- function() {
  cat("Loading global sales and rental transactions...\n")

  sales <- rio::import(CONFIG$sales_path, trust = TRUE) |>
    dplyr::mutate(
      property_type = forcats::as_factor(.data$property_type),
      old_new = forcats::as_factor(.data$old_new),
      duration = forcats::as_factor(.data$duration)
    )

  rentals <- rio::import(CONFIG$rental_path, trust = TRUE) |>
    dplyr::mutate(property_type = forcats::as_factor(.data$property_type))

  list(sales = sales, rentals = rentals)
}

prepare_sales_base <- function(radius, sales) {
  cat(sprintf("Loading %dm sales cross-section...\n", radius))

  dat_cs <- arrow::open_dataset(CONFIG$sales_cross_section_path) |>
    dplyr::filter(.data$radius == .env$radius, .data$n_spill_sites > 0) |>
    dplyr::collect()

  dat <- dat_cs |>
    dplyr::inner_join(sales, by = "house_id") |>
    dplyr::mutate(log_price = log(.data$price.y)) |>
    dplyr::filter(
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id,
      !is.na(.data$spill_count_weekly_avg),
      !is.na(.data$lsoa),
      !is.na(.data$month_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$old_new),
      !is.na(.data$duration),
      is.finite(.data$log_price)
    ) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      property_type = forcats::fct_drop(.data$property_type),
      old_new = forcats::fct_drop(.data$old_new),
      duration = forcats::fct_drop(.data$duration)
    )

  if (nrow(dat) == 0L) {
    stop("No complete sales observations at radius ", radius, ".", call. = FALSE)
  }

  cat(sprintf("  Sales base sample: %s observations\n", format(nrow(dat), big.mark = ",")))
  dat
}

prepare_rental_base <- function(radius, rentals) {
  cat(sprintf("Loading %dm rental cross-section...\n", radius))

  dat_cs <- arrow::open_dataset(CONFIG$rental_cross_section_path) |>
    dplyr::filter(.data$radius == .env$radius, .data$n_spill_sites > 0) |>
    dplyr::collect()

  dat <- dat_cs |>
    dplyr::inner_join(rentals, by = "rental_id") |>
    dplyr::mutate(log_price = log(.data$listing_price.y)) |>
    dplyr::filter(
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id,
      !is.na(.data$spill_count_weekly_avg),
      !is.na(.data$lsoa),
      !is.na(.data$month_id),
      !is.na(.data$latitude),
      !is.na(.data$longitude),
      !is.na(.data$property_type),
      !is.na(.data$bedrooms),
      !is.na(.data$bathrooms),
      is.finite(.data$log_price)
    ) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      property_type = forcats::fct_drop(.data$property_type)
    )

  if (nrow(dat) == 0L) {
    stop("No complete rental observations at radius ", radius, ".", call. = FALSE)
  }

  cat(sprintf("  Rental base sample: %s observations\n", format(nrow(dat), big.mark = ",")))
  dat
}

validate_joined_articles <- function(dat) {
  monthly_values <- dat |>
    dplyr::distinct(
      .data$lagged_month_id,
      .data$cumulative_articles,
      .data$log_cumulative_articles
    ) |>
    dplyr::arrange(.data$lagged_month_id)

  stopifnot(
    nrow(monthly_values) > 0L,
    !anyDuplicated(monthly_values$lagged_month_id),
    all(is.finite(monthly_values$cumulative_articles)),
    all(is.finite(monthly_values$log_cumulative_articles)),
    all(diff(monthly_values$cumulative_articles) >= 0),
    all(diff(monthly_values$log_cumulative_articles) >= 0)
  )
  invisible(dat)
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

estimate_sales_model <- function(dat) {
  # The lagged article measure is constant within month and its main effect is
  # absorbed by month FE. Identification comes from its interaction with spill
  # exposure. Month FE are a deliberate departure from did_articles_lag4_prior.R
  # (quarter FE) and match model_sale_5 in did_articles_prior.R.
  fixest::feols(
    log_price ~ spill_count_weekly_avg +
      spill_count_weekly_avg:log_cumulative_articles +
      property_type + old_new + duration | lsoa + month_id,
    data = dat,
    vcov = ~lsoa
  )
}

estimate_rental_model <- function(dat) {
  # As above, month FE absorb the article-measure main effect.
  fixest::feols(
    log_price ~ spill_count_weekly_avg +
      spill_count_weekly_avg:log_cumulative_articles +
      property_type + bedrooms + bathrooms | lsoa + month_id,
    data = dat,
    vcov = ~lsoa
  )
}

estimate_sales_lag_path <- function(base_sample, articles, radius) {
  models <- vector("list", length(CONFIG$lags))
  names(models) <- paste0("lag_", CONFIG$lags)

  for (lag in CONFIG$lags) {
    joined <- join_lagged_cumulative_articles(
      base_sample,
      articles,
      lag = lag,
      start_month_id = CONFIG$analysis_start_month_id
    )
    dat <- restrict_to_common_sample(
      joined,
      start_month_id = CONFIG$analysis_start_month_id,
      max_lag = CONFIG$max_lag
    )

    stopifnot(
      nrow(dat) > 0L,
      min(dat$month_id) == 13L,
      all(dat$month_id >= 13L),
      all(dat$lagged_month_id == dat$month_id - lag)
    )
    validate_joined_articles(dat)

    models[[paste0("lag_", lag)]] <- estimate_sales_model(dat)
    cat(sprintf(
      "  Sales %dm lag %2d common sample: min month = %d, N = %s; dropped %s base rows\n",
      radius, lag, min(dat$month_id), format(nrow(dat), big.mark = ","),
      format(nrow(base_sample) - nrow(dat), big.mark = ",")
    ))
  }

  model_n <- vapply(models, stats::nobs, numeric(1))
  stopifnot(length(unique(model_n)) == 1L)
  models
}

estimate_full_sales_reference <- function(base_sample, articles, radius) {
  dat <- join_lagged_cumulative_articles(
    base_sample,
    articles,
    lag = 0L,
    start_month_id = CONFIG$analysis_start_month_id
  )
  validate_joined_articles(dat)
  model <- estimate_sales_model(dat)
  cat(sprintf(
    "  Sales %dm full-sample contemporaneous reference: N = %s\n",
    radius, format(stats::nobs(model), big.mark = ",")
  ))
  model
}

estimate_full_rental_reference <- function(base_sample, articles, radius) {
  dat <- join_lagged_cumulative_articles(
    base_sample,
    articles,
    lag = 0L,
    start_month_id = CONFIG$analysis_start_month_id
  )
  validate_joined_articles(dat)
  model <- estimate_rental_model(dat)
  cat(sprintf(
    "  Rentals %dm full-sample contemporaneous benchmark: N = %s\n",
    radius, format(stats::nobs(model), big.mark = ",")
  ))
  model
}


# ==============================================================================
# 4. Component Results and Grid Table
# ==============================================================================

model_result_row <- function(model, market, radius, lag, sample) {
  coef_stats <- extract_fixest_term(
    model, "spill_count_weekly_avg:log_cumulative_articles"
  )
  critical_value <- stats::qnorm(0.975)

  tibble::tibble(
    margin = "intensive",
    market = market,
    measure = "articles",
    radius = as.integer(radius),
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

build_component_results <- function(results_by_radius) {
  rows <- list()
  row_id <- 0L

  for (radius in CONFIG$radii) {
    radius_result <- results_by_radius[[paste0(radius, "m")]]
    for (lag in CONFIG$lags) {
      row_id <- row_id + 1L
      rows[[row_id]] <- model_result_row(
        radius_result$sales_common_models[[paste0("lag_", lag)]],
        market = "sales",
        radius = radius,
        lag = lag,
        sample = "common"
      )
    }
    row_id <- row_id + 1L
    rows[[row_id]] <- model_result_row(
      radius_result$sales_full_reference,
      market = "sales",
      radius = radius,
      lag = 0L,
      sample = "full_reference"
    )
    row_id <- row_id + 1L
    rows[[row_id]] <- model_result_row(
      radius_result$rental_full_reference,
      market = "rentals",
      radius = radius,
      lag = 0L,
      sample = "full_reference"
    )
  }

  out <- dplyr::bind_rows(rows)
  key_cols <- c("margin", "market", "measure", "radius", "lag", "sample")
  expected_keys <- dplyr::bind_rows(
    tidyr::expand_grid(
      market = "sales", radius = CONFIG$radii, lag = CONFIG$lags,
      sample = "common"
    ),
    tidyr::expand_grid(
      market = "sales", radius = CONFIG$radii, lag = 0L,
      sample = "full_reference"
    ),
    tidyr::expand_grid(
      market = "rentals", radius = CONFIG$radii, lag = 0L,
      sample = "full_reference"
    )
  )

  stopifnot(
    identical(
      names(out),
      c(
        "margin", "market", "measure", "radius", "lag", "sample",
        "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
      )
    ),
    nrow(out) == 18L,
    sum(out$market == "sales" & out$sample == "common") == 12L,
    sum(out$market == "sales" & out$sample == "full_reference") == 3L,
    sum(out$market == "rentals" & out$sample == "full_reference") == 3L,
    !anyDuplicated(out[key_cols]),
    all(stats::complete.cases(out)),
    all(is.finite(unlist(out[c(
      "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
    )]))),
    nrow(dplyr::anti_join(
      expected_keys, out, by = c("market", "radius", "lag", "sample")
    )) == 0L,
    nrow(dplyr::anti_join(
      out, expected_keys, by = c("market", "radius", "lag", "sample")
    )) == 0L,
    all(out$margin == "intensive"),
    all(out$measure == "articles"),
    all(out$sample %in% c("common", "full_reference")),
    all(out$radius %in% CONFIG$radii)
  )

  out
}

significance_stars <- function(p_value) {
  ifelse(
    p_value < 0.01, "***",
    ifelse(p_value < 0.05, "**", ifelse(p_value < 0.1, "*", ""))
  )
}

format_grid_cell <- function(row) {
  paste0(
    "\\shortstack{",
    sprintf("%.3f", row$estimate), significance_stars(row$p_value),
    " \\\\ ", sprintf("(%.3f)", row$std_error),
    "}"
  )
}

export_grid_table <- function(component_results) {
  cell_for <- function(market, radius, lag, sample) {
    row <- component_results |>
      dplyr::filter(
        .data$market == .env$market,
        .data$radius == .env$radius,
        .data$lag == .env$lag,
        .data$sample == .env$sample
      )
    if (nrow(row) == 0L) return("--")
    stopifnot(nrow(row) == 1L)
    format_grid_cell(row)
  }

  body <- character()
  for (market in c("sales", "rentals")) {
    market_label <- if (market == "sales") "House sales" else "House rentals"
    for (radius in CONFIG$radii) {
      full_reference <- cell_for(
        market, radius, lag = 0L, sample = "full_reference"
      )
      common_cells <- vapply(
        CONFIG$lags,
        function(lag) cell_for(market, radius, lag, sample = "common"),
        FUN.VALUE = character(1)
      )
      body <- c(
        body,
        paste(
          c(market_label, paste0(radius, "m"), full_reference, common_cells),
          collapse = " & "
        )
      )
    }
  }
  body <- paste0(body, " \\\\")

  notes <- paste0(
    "\\multicolumn{7}{p{0.97\\linewidth}}{\\footnotesize \\textit{Notes:} ",
    "This table is the intensive-margin extension. Cells report the coefficient ",
    "on weekly-average spill count $\\times$ log cumulative articles, with ",
    "LSOA-clustered standard errors in parentheses. Common-sample sales models ",
    "retain January 2022--December 2023 (month\\_id $\\geq 13$) at every lag. ",
    "The Full lag-0 column reports contemporaneous January 2021--December 2023 ",
    "references; rentals appear only in that column and are not lagged. All ",
    "models include property controls, LSOA fixed effects, and month fixed ",
    "effects. Month effects absorb the article-measure main effect. Cumulative ",
    "articles originate in January 2021. *** $p<0.01$, ** $p<0.05$, * $p<0.1$.} \\\\")

  latex <- c(
    "\\begin{table}[H]",
    "\\centering",
    "\\caption{Lagged Cumulative Articles and Property Values: Intensive-Margin Extension}",
    paste0("\\label{", CONFIG$table_label, "}"),
    "\\begin{tabular}{llccccc}",
    "\\toprule",
    "Market & Radius & Full lag 0 & Common lag 0 & Common lag 3 & Common lag 6 & Common lag 12 \\\\ ",
    "\\midrule",
    body[1:3],
    "\\addlinespace",
    body[4:6],
    "\\bottomrule",
    "\\end{tabular}",
    notes,
    "\\end{table}"
  )

  stopifnot(
    length(body) == 6L,
    sum(component_results$market == "sales" &
      component_results$sample == "common") == 12L,
    sum(component_results$sample == "full_reference") == 6L,
    grepl("House rentals", paste(latex, collapse = "\n"), fixed = TRUE),
    all(vapply(CONFIG$lags, function(lag) {
      any(grepl(paste0("Common lag ", lag), latex, fixed = TRUE))
    }, logical(1)))
  )

  dir.create(dirname(CONFIG$output_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(latex, CONFIG$output_path)
  cat(sprintf("Grid table exported to: %s\n", CONFIG$output_path))
  invisible(CONFIG$output_path)
}

export_component_results <- function(component_results) {
  dir.create(
    dirname(CONFIG$effect_size_output_path),
    recursive = TRUE,
    showWarnings = FALSE
  )
  utils::write.csv(
    component_results,
    CONFIG$effect_size_output_path,
    row.names = FALSE,
    na = ""
  )
  cat(sprintf(
    "Component results exported to: %s\n",
    CONFIG$effect_size_output_path
  ))
  invisible(CONFIG$effect_size_output_path)
}


# ==============================================================================
# 5. Main Workflow
# ==============================================================================

main <- function() {
  stopifnot(
    identical(CONFIG$radii, c(250L, 500L, 1000L)),
    identical(CONFIG$lags, c(0L, 3L, 6L, 12L)),
    CONFIG$max_lag == max(CONFIG$lags),
    CONFIG$analysis_start_month_id + CONFIG$max_lag == 13L
  )

  articles <- load_articles()
  transactions <- load_global_transactions()
  results_by_radius <- vector("list", length(CONFIG$radii))
  names(results_by_radius) <- paste0(CONFIG$radii, "m")

  for (radius in CONFIG$radii) {
    cat(sprintf("\n================ Radius: %dm ================\n", radius))

    # Build each radius-specific market base once, then reuse it for all lags.
    sales_base <- prepare_sales_base(radius, transactions$sales)
    rental_base <- prepare_rental_base(radius, transactions$rentals)
    results_by_radius[[paste0(radius, "m")]] <- list(
      sales_common_models = estimate_sales_lag_path(
        sales_base, articles, radius
      ),
      sales_full_reference = estimate_full_sales_reference(
        sales_base, articles, radius
      ),
      rental_full_reference = estimate_full_rental_reference(
        rental_base, articles, radius
      )
    )
  }

  component_results <- build_component_results(results_by_radius)
  export_grid_table(component_results)
  export_component_results(component_results)

  stopifnot(
    file.exists(CONFIG$output_path),
    file.info(CONFIG$output_path)$size > 0L,
    file.exists(CONFIG$effect_size_output_path),
    file.info(CONFIG$effect_size_output_path)$size > 0L
  )

  cat("\nScript completed successfully.\n")
  invisible(list(
    results_by_radius = results_by_radius,
    component_results = component_results,
    table_path = CONFIG$output_path,
    effect_size_path = CONFIG$effect_size_output_path
  ))
}

if (sys.nframe() == 0L) {
  main()
}
