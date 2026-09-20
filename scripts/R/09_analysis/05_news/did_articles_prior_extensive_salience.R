# ==============================================================================
# Extensive-Margin Cumulative Articles by Salience Group
# ==============================================================================
#
# Purpose: Estimate the selected paper specification for All Bathing, All Coastal
#          and All Inland, separately for sales and rentals. These groups overlap.
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/site_characteristics/site_group_characteristics.parquet
#   - data/processed/spill_house_lookup.parquet
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - data/processed/lexis_nexis/search1_monthly.parquet
#
# Outputs:
#   - output/tables/did_articles_prior_extensive_salience_groups.tex
#   - output/regs/did_articles_prior_extensive_salience_groups.rds
#   - output/logs/did_articles_prior_extensive_salience_groups_{results,cell_counts}.csv
#
# Run from the repository root with Rscript in the rv environment (R 4.6.0).
# Full exploration: docs/reports/2026-09-03-003-heterogeneity-by-salience-report.qmd
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install project dependencies with `rv sync`.",
       call. = FALSE)
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
REQUIRED_PACKAGES <- c("arrow", "dplyr", "fixest", "forcats", "here", "modelsummary", "rio", "tibble")
check_required_packages(REQUIRED_PACKAGES)
source(here::here("scripts", "R", "utils", "salience_group_utils.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "extensive_margin_news_utils.R"), local = TRUE)


# ==============================================================================
# 1. Configuration
# ==============================================================================

CONFIG <- list(
  coast_rule_m = 2000L,
  output_prefix = "did_articles_prior_extensive_salience_groups",
  title = "Extensive-Margin Cumulative Articles by Salience Group",
  attention = "log_cumulative_articles",
  articles_path = here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet"),
  comparison = list(
    comparison_id = "500_vs_1000_2000", comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L, near_max = 500L, far_min = 1000L, far_max = 2000L
  ),
  groups = c("bathing", "coastal", "inland")
)

MARKETS <- list(
  sales = list(
    id = "house_id", price = "price", end_month_id = 48L,
    controls = c("property_type", "old_new", "duration"),
    transactions = here::here("data", "processed", "house_price.parquet"),
    lookup = here::here("data", "processed", "spill_house_lookup.parquet")
  ),
  rentals = list(
    id = "rental_id", price = "listing_price", end_month_id = 36L,
    controls = c("property_type", "bedrooms", "bathrooms"),
    transactions = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    lookup = here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet")
  )
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

load_attention <- function() {
  attention <- arrow::read_parquet(CONFIG$articles_path) |>
    dplyr::filter(.data$month_id >= 1L, .data$month_id <= 48L) |>
    dplyr::arrange(.data$month_id) |>
    dplyr::transmute(month_id = .data$month_id,
                     log_cumulative_articles = log(cumsum(.data$article_count)))
  stopifnot(nrow(attention) == 48L, !anyDuplicated(attention$month_id),
            all(attention$month_id == 1:48),
            all(is.finite(attention$log_cumulative_articles)))
  attention
}

prepare_analysis_data <- function(market, attention_data, characteristics) {
  spec <- MARKETS[[market]]
  transactions <- if (market == "sales") load_sales_transactions(spec$transactions) else
    load_rental_transactions(spec$transactions)
  transactions <- transactions |>
    dplyr::select(dplyr::all_of(c(
      spec$id, spec$price, "region", "month_id", "lsoa", "latitude", "longitude", spec$controls
    ))) |>
    dplyr::filter(.data$month_id >= 1L, .data$month_id <= spec$end_month_id)
  nearest <- nearest_salience_sites(
    arrow::open_dataset(spec$lookup), characteristics, spec$id
  )
  data <- build_extensive_margin_sample(transactions, nearest, spec$id, CONFIG$comparison) |>
    dplyr::inner_join(attention_data, by = "month_id", relationship = "many-to-one") |>
    dplyr::mutate(log_price = log(.data[[spec$price]])) |>
    dplyr::filter(
      is.finite(.data$log_price), is.finite(.data[[CONFIG$attention]]),
      dplyr::if_all(dplyr::all_of(c(
        "lsoa", "month_id", "latitude", "longitude", spec$controls
      )), ~ !is.na(.x))
    )
  data <- if (market == "sales") standardise_sales_estimation_data(data) else
    standardise_rental_estimation_data(data)
  classify_salience_groups(data, CONFIG$coast_rule_m)
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

fit_salience_extensive <- function(
  data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  controls <- if (market == "sales") "property_type + old_new + duration" else
    "property_type + bedrooms + bathrooms"
  term <- paste0("near_bin:", attention)
  formula <- stats::as.formula(paste(
    "log_price ~ near_bin +", term, "+", controls, "| lsoa + month_id"
  ))
  model <- fixest::feols(formula, data = data, vcov = ~lsoa, lean = TRUE)
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified attention effect: ", market, " / ", term, call. = FALSE)
  }
  model
}

estimate_groups <- function(data, market) {
  counts <- audit_salience_groups(data, market)
  masks <- salience_group_masks(data)
  models <- lapply(CONFIG$groups, function(group) {
    sample <- data[masks[[group]] & !data$london, ]
    cat(sprintf("  %s / %s: %s observations before estimator removals\n",
                market, group, format(nrow(sample), big.mark = ",")))
    fit_salience_extensive(sample, market, CONFIG$attention)
  })
  names(models) <- CONFIG$groups
  counts$nobs <- vapply(models, stats::nobs, numeric(1))
  counts$n_removed_by_estimator <- counts$n_estimation - counts$nobs
  results <- dplyr::bind_rows(lapply(names(models), function(group) {
    salience_group_results(models[[group]]) |>
      dplyr::filter(.data$term %in% c("near_bin", paste0("near_bin:", CONFIG$attention))) |>
      dplyr::mutate(market = .env$market, group = .env$group, .before = 1L)
  }))
  list(models = models, counts = counts, results = results)
}


# ==============================================================================
# 4. Table and Model Export
# ==============================================================================

export_table <- function(result) {
  coefficient_map <- c(near_bin = "Near bin")
  coefficient_map[paste0("near_bin:", CONFIG$attention)] <- "{Near bin \\\\ $\\times$ $\\log (\\text{Articles})$}"
  notes <- paste0(
    "This table presents hedonic estimates of the relationship between proximity to sewage ",
    "overflows, public attention, and property values, estimated separately within each ",
    "salience group. The sample includes properties whose nearest mapped overflow lies either ",
    "within 0-500m (near bin) or 1000-2000m (far bin), excluding Greater London. The sample ",
    "covers 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are ",
    "available). Treatment is proximity to a mapped overflow rather than measured spill ",
    "activity, so annual reporting gaps do not affect treatment classification. ",
    salience_group_notes("nearest", CONFIG$coast_rule_m),
    "The dependent variable is the log transaction price for sales or the log weekly ",
    "asking rent for rentals. ",
    "Near bin is an indicator equal to one for properties in the 0-500m band and zero for ",
    "properties in the 1000-2000m band. ",
    "$\\\\log (\\\\text{Articles})$ is the natural logarithm of cumulative UK news coverage ",
    "of sewage spills from LexisNexis from January 2021 through the transaction month. ",
    "Property controls include type (flat, semi-detached, terraced, other), new build status, ",
    "and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, ",
    "and bathrooms for rentals. ",
    "Standard errors clustered at the LSOA level are reported in parentheses. ",
    "Each column reports a separate regression; differences between groups are not tested. ",
    "*** $p<0.01$, ** $p<0.05$, * $p<0.1$."
  )
  export_salience_group_table(
    result$models, coefficient_map, CONFIG$title, notes,
    here::here("output", "tables", paste0(CONFIG$output_prefix, ".tex"))
  )
}

export_results <- function(result) {
  export_table(result)
  for (folder in c("regs", "logs")) {
    dir.create(here::here("output", folder), recursive = TRUE, showWarnings = FALSE)
  }
  saveRDS(result, here::here("output", "regs", paste0(CONFIG$output_prefix, ".rds")))
  utils::write.csv(result$counts, here::here(
    "output", "logs", paste0(CONFIG$output_prefix, "_cell_counts.csv")
  ), row.names = FALSE)
  utils::write.csv(result$results, here::here(
    "output", "logs", paste0(CONFIG$output_prefix, "_results.csv")
  ), row.names = FALSE)
  invisible(result)
}


# ==============================================================================
# 5. Execution
# ==============================================================================

main <- function() {
  attention_data <- load_attention()
  characteristics <- arrow::read_parquet(here::here(
    "data", "processed", "site_characteristics", "site_group_characteristics.parquet"
  ))
  output <- lapply(names(MARKETS), function(market) {
    data <- prepare_analysis_data(market, attention_data, characteristics)
    estimate_groups(data, market)
  })
  names(output) <- names(MARKETS)
  result <- list(
    models = lapply(output, function(x) x$models),
    counts = dplyr::bind_rows(lapply(output, function(x) x$counts)),
    results = dplyr::bind_rows(lapply(output, function(x) x$results)),
    settings = list(config = CONFIG, london = "excluded", groups_overlap = TRUE,
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
