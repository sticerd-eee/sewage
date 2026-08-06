# ==============================================================================
# Lagged Google Trends Peak DiD (Intensive-Margin Extension)
# ==============================================================================
#
# Purpose: Estimate the preferred intensive-margin sales specification over a
#          lag-by-radius grid. Rentals enter only as contemporaneous benchmarks.
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/cross_section/sales/prior_to_sale/
#   - data/processed/cross_section/rentals/prior_to_rental/
#
# Outputs:
#   - output/tables/did_trends_lagged_sales_grid.tex
#   - output/tables/did_trends_lagged_sales_effect_sizes.csv
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
  "readxl",
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
  analysis_start_month_id = 1L,
  analysis_end_month_id = 36L,
  base_year = 2021L,
  google_trends_sheet = "united_kingdom",
  google_trends_path = here::here(
    "data", "raw", "google_trends", "google_trends_uk.xlsx"
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
    "output", "tables", "did_trends_lagged_sales_grid.tex"
  ),
  effect_size_output_path = here::here(
    "output", "tables", "did_trends_lagged_sales_effect_sizes.csv"
  ),
  table_label = "tbl:did-trends-lagged-sales-grid"
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

load_google_trends_peak <- function() {
  trends <- readxl::read_excel(
    CONFIG$google_trends_path,
    sheet = CONFIG$google_trends_sheet
  ) |>
    dplyr::filter(.data$Year >= 2021L, .data$Year <= 2023L)

  peak_row <- trends |>
    dplyr::slice_max(
      order_by = .data[["'Sewage Spill' Google Searches"]],
      n = 1L,
      with_ties = FALSE
    )
  peak_month <- as.integer(substr(peak_row$Date[[1]], 6L, 7L))

  list(
    peak_date = as.character(peak_row$Date[[1]]),
    peak_month_id = as.integer(
      (peak_row$Year[[1]] - CONFIG$base_year) * 12L + peak_month
    )
  )
}

load_global_transactions <- function() {
  cat("Loading global sales and rental transactions...\n")

  sales <- arrow::open_dataset(CONFIG$sales_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    ) |>
    dplyr::select(
      "house_id", "price", "month_id", "lsoa", "msoa", "latitude",
      "longitude", "property_type", "old_new", "duration"
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      property_type = forcats::as_factor(.data$property_type),
      old_new = forcats::as_factor(.data$old_new),
      duration = forcats::as_factor(.data$duration)
    )

  rentals <- arrow::open_dataset(CONFIG$rental_path) |>
    dplyr::filter(
      !is.na(.data$month_id),
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id
    ) |>
    dplyr::select(
      "rental_id", "listing_price", "month_id", "lsoa", "msoa",
      "latitude", "longitude", "property_type", "bedrooms", "bathrooms"
    ) |>
    dplyr::collect() |>
    dplyr::mutate(property_type = forcats::as_factor(.data$property_type))

  list(sales = sales, rentals = rentals)
}

prepare_sales_sample <- function(radius, sales) {
  cat(sprintf("Loading %dm sales cross-section...\n", radius))

  dat_cs <- arrow::open_dataset(CONFIG$sales_cross_section_path) |>
    dplyr::filter(.data$radius == .env$radius, .data$n_spill_sites > 0) |>
    dplyr::select("house_id", "price", "spill_count_weekly_avg") |>
    dplyr::collect()

  dat <- dat_cs |>
    dplyr::inner_join(sales, by = "house_id") |>
    dplyr::mutate(log_price = log(.data$price.y)) |>
    dplyr::filter(
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id,
      !is.na(.data$spill_count_weekly_avg),
      !is.na(.data$lsoa),
      !is.na(.data$msoa),
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
      msoa = forcats::fct_drop(forcats::as_factor(.data$msoa)),
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

prepare_rental_sample <- function(radius, rentals) {
  cat(sprintf("Loading %dm rental cross-section...\n", radius))

  dat_cs <- arrow::open_dataset(CONFIG$rental_cross_section_path) |>
    dplyr::filter(.data$radius == .env$radius, .data$n_spill_sites > 0) |>
    dplyr::select("rental_id", "listing_price", "spill_count_weekly_avg") |>
    dplyr::collect()

  dat <- dat_cs |>
    dplyr::inner_join(rentals, by = "rental_id") |>
    dplyr::mutate(log_price = log(.data$listing_price.y)) |>
    dplyr::filter(
      .data$month_id >= CONFIG$analysis_start_month_id,
      .data$month_id <= CONFIG$analysis_end_month_id,
      !is.na(.data$spill_count_weekly_avg),
      !is.na(.data$lsoa),
      !is.na(.data$msoa),
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
      msoa = forcats::fct_drop(forcats::as_factor(.data$msoa)),
      property_type = forcats::fct_drop(.data$property_type)
    )

  if (nrow(dat) == 0L) {
    stop("No complete rental observations at radius ", radius, ".", call. = FALSE)
  }

  cat(sprintf("  Rental base sample: %s observations\n", format(nrow(dat), big.mark = ",")))
  dat
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

estimate_sales_lag_path <- function(base_sample, peak_month_id, radius) {
  models <- vector("list", length(CONFIG$lags))
  names(models) <- paste0("lag_", CONFIG$lags)

  for (lag in CONFIG$lags) {
    dat <- base_sample |>
      dplyr::mutate(
        post = shifted_post_indicator(.data$month_id, peak_month_id, lag)
      )

    expected_cut <- peak_month_id + lag
    post_months <- sort(unique(dat$month_id[dat$post == 1L]))
    pre_months <- sort(unique(dat$month_id[dat$post == 0L]))

    stopifnot(
      all(dat$post == as.integer(dat$month_id >= expected_cut)),
      length(post_months) > 0L,
      min(post_months) == expected_cut,
      max(pre_months) == expected_cut - 1L
    )
    if (lag == 12L) {
      # The August 2023 cutoff leaves only Aug--Dec 2023 (five post months).
      stopifnot(length(post_months) == 5L)
    }

    # Month FE absorbs the monthly post main effect, so identification comes
    # only from spill exposure x post. Month FE (not quarter FE) deliberately
    # matches model_sale_5 in the contemporaneous intensive baseline.
    models[[paste0("lag_", lag)]] <- fixest::feols(
      log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post +
        property_type + old_new + duration | lsoa + month_id,
      data = dat,
      vcov = ~lsoa,
      lean = TRUE
    )

    cat(sprintf(
      "  Sales %dm lag %2d: cut = %d, N = %s, post months = %d\n",
      radius, lag, expected_cut, format(nrow(dat), big.mark = ","),
      length(post_months)
    ))
  }

  model_n <- vapply(models, stats::nobs, numeric(1))
  stopifnot(length(unique(model_n)) == 1L)
  models
}

estimate_rental_benchmark <- function(base_sample, peak_month_id, radius) {
  dat <- base_sample |>
    dplyr::mutate(
      post = shifted_post_indicator(.data$month_id, peak_month_id, lag = 0L)
    )

  stopifnot(
    all(dat$post == as.integer(dat$month_id >= peak_month_id)),
    min(dat$month_id[dat$post == 1L]) == peak_month_id
  )

  # As above, the monthly post main effect is absorbed by month fixed effects.
  model <- fixest::feols(
    log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post +
      property_type + bedrooms + bathrooms | lsoa + month_id,
    data = dat,
    vcov = ~lsoa,
    lean = TRUE
  )

  cat(sprintf(
    "  Rentals %dm contemporaneous benchmark: cut = %d, N = %s\n",
    radius, peak_month_id, format(nrow(dat), big.mark = ",")
  ))
  model
}


# ==============================================================================
# 4. Component Results and Grid Table
# ==============================================================================

model_result_row <- function(model, market, radius, lag) {
  coef_stats <- extract_fixest_term(model, "spill_count_weekly_avg:post")
  critical_value <- stats::qnorm(0.975)

  tibble::tibble(
    margin = "intensive",
    market = market,
    measure = "post",
    radius = as.integer(radius),
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

build_component_results <- function(results_by_radius) {
  rows <- list()
  row_id <- 0L

  for (radius in CONFIG$radii) {
    radius_result <- results_by_radius[[paste0(radius, "m")]]
    for (lag in CONFIG$lags) {
      row_id <- row_id + 1L
      rows[[row_id]] <- model_result_row(
        radius_result$sales_models[[paste0("lag_", lag)]],
        market = "sales",
        radius = radius,
        lag = lag
      )
    }
    row_id <- row_id + 1L
    rows[[row_id]] <- model_result_row(
      radius_result$rental_model,
      market = "rentals",
      radius = radius,
      lag = 0L
    )
  }

  out <- dplyr::bind_rows(rows)
  key_cols <- c("margin", "market", "measure", "radius", "lag", "sample")
  expected_keys <- dplyr::bind_rows(
    tidyr::expand_grid(market = "sales", radius = CONFIG$radii, lag = CONFIG$lags),
    tidyr::expand_grid(market = "rentals", radius = CONFIG$radii, lag = 0L)
  )

  stopifnot(
    identical(
      names(out),
      c(
        "margin", "market", "measure", "radius", "lag", "sample",
        "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
      )
    ),
    nrow(out) == 15L,
    !anyDuplicated(out[key_cols]),
    all(stats::complete.cases(out)),
    all(is.finite(unlist(out[c(
      "estimate", "std_error", "conf_low", "conf_high", "p_value", "n"
    )]))),
    nrow(dplyr::anti_join(
      expected_keys, out, by = c("market", "radius", "lag")
    )) == 0L,
    nrow(dplyr::anti_join(
      out, expected_keys, by = c("market", "radius", "lag")
    )) == 0L,
    all(out$margin == "intensive"),
    all(out$measure == "post"),
    all(out$sample == "full"),
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

export_grid_table <- function(component_results, peak_info) {
  cell_for <- function(market, radius, lag) {
    row <- component_results |>
      dplyr::filter(
        .data$market == .env$market,
        .data$radius == .env$radius,
        .data$lag == .env$lag
      )
    if (nrow(row) == 0L) return("--")
    stopifnot(nrow(row) == 1L)
    format_grid_cell(row)
  }

  body <- character()
  for (market in c("sales", "rentals")) {
    market_label <- if (market == "sales") "House sales" else "House rentals"
    for (radius in CONFIG$radii) {
      cells <- vapply(
        CONFIG$lags,
        function(lag) cell_for(market, radius, lag),
        FUN.VALUE = character(1)
      )
      body <- c(
        body,
        paste(c(market_label, paste0(radius, "m"), cells), collapse = " & ")
      )
    }
  }
  body <- paste0(body, " \\\\")

  notes <- paste0(
    "\\multicolumn{6}{p{0.97\\linewidth}}{\\footnotesize \\textit{Notes:} ",
    "This table is the intensive-margin extension. Cells report the coefficient ",
    "on weekly-average spill count $\\times$ Post, with LSOA-clustered standard ",
    "errors in parentheses. Post begins in ", peak_info$peak_date,
    " (month\\_id ", peak_info$peak_month_id,
    "); sales lag $L$ shifts that threshold forward by $L$ months. All models ",
    "retain January 2021--December 2023, include property controls, LSOA fixed ",
    "effects, and month fixed effects. Month effects absorb the Post main effect. ",
    "Lag 12 has only five post months. Rentals are contemporaneous benchmarks ",
    "only. *** $p<0.01$, ** $p<0.05$, * $p<0.1$.} \\\\"
  )

  latex <- c(
    "\\begin{table}[H]",
    "\\centering",
    "\\caption{Lagged Public Attention and Property Values: Intensive-Margin Extension}",
    paste0("\\label{", CONFIG$table_label, "}"),
    "\\begin{tabular}{llcccc}",
    "\\toprule",
    "Market & Radius & Lag 0 & Lag 3 & Lag 6 & Lag 12 \\\\ ",
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
    sum(component_results$market == "sales") == 12L,
    sum(component_results$market == "rentals") == 3L,
    all(vapply(CONFIG$lags, function(lag) {
      any(grepl(paste0("Lag ", lag), latex, fixed = TRUE))
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
  run_news_lag_sanity_checks()
  stopifnot(
    identical(CONFIG$radii, c(250L, 500L, 1000L)),
    identical(CONFIG$lags, c(0L, 3L, 6L, 12L))
  )

  peak_info <- load_google_trends_peak()
  stopifnot(peak_info$peak_month_id == 20L)
  cat(sprintf(
    "Google Trends peak: %s (month_id = %d)\n",
    peak_info$peak_date, peak_info$peak_month_id
  ))

  transactions <- load_global_transactions()
  results_by_radius <- vector("list", length(CONFIG$radii))
  names(results_by_radius) <- paste0(CONFIG$radii, "m")

  for (radius in CONFIG$radii) {
    cat(sprintf("\n================ Radius: %dm ================\n", radius))

    # Each radius-specific cross-section is built once and reused at every lag.
    sales_base <- prepare_sales_sample(radius, transactions$sales)
    rental_base <- prepare_rental_sample(radius, transactions$rentals)
    results_by_radius[[paste0(radius, "m")]] <- list(
      sales_models = estimate_sales_lag_path(
        sales_base, peak_info$peak_month_id, radius
      ),
      rental_model = estimate_rental_benchmark(
        rental_base, peak_info$peak_month_id, radius
      )
    )
  }

  component_results <- build_component_results(results_by_radius)
  export_grid_table(component_results, peak_info)
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
