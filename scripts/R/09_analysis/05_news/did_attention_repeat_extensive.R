# ==============================================================================
# Long-Run Repeat-Property Near-Far Attention Regressions
# ==============================================================================
#
# Purpose: Estimate whether public attention to sewage spills changes the price
#          gap between repeat-observed properties near to and far from the
#          nearest sewage overflow, for sales (2014-2024) and rentals
#          (2014-2023). One sample excludes properties with observed
#          characteristic changes; three attention measures (log cumulative
#          articles, post March 2021, post August 2022) each enter their own
#          regression with property, calendar-month and MSOA x year effects.
#
# Inputs:
#   - data/processed/house_price_long_run.parquet
#   - data/processed/zoopla/zoopla_rentals_long_run.parquet
#   - data/processed/repeated_transactions/repeated_sales.parquet
#   - data/processed/repeated_transactions/repeated_rentals.parquet
#   - data/processed/site_nearest_long_run.parquet
#   - data/processed/lexis_nexis/search1_monthly.parquet
#
# Outputs:
#   - output/tables/did_attention_repeat_extensive.tex
#   - output/regs/did_attention_repeat_extensive.rds (results and settings)
#   - output/logs/did_attention_repeat_extensive_results.csv
#
# Prerequisite: run scripts/R/04_feature_engineering/site_nearest_long_run_match.R
#   by hand first; stage 04 has no runner.
#
# Run from the repository root with Rscript in the rv environment (R 4.6.0).
# Full exploration: docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd
#   (variant `exclude_changes`, fixed effects `property_month_msoa_year`).
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install project dependencies with `rv sync`.",
       call. = FALSE)
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
REQUIRED_PACKAGES <- c(
  "arrow", "data.table", "dplyr", "fixest", "here", "modelsummary", "tibble"
)
check_required_packages(REQUIRED_PACKAGES)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "extensive_margin_news_utils.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "long_run_repeat_panel_utils.R"), local = TRUE)


# ==============================================================================
# 1. Configuration
# ==============================================================================

CONFIG <- list(
  output_prefix = "did_attention_repeat_extensive",
  table_label = "tbl:did-attention-repeat-extensive",
  title = paste0(
    "Effect of Overflow Proximity on Repeat-Property Values: Long-Run Public Attention ",
    "(0-500m vs 1000-2000m)"
  ),
  comparison = list(
    comparison_id = "500_vs_1000_2000", comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L, near_max = 500L, far_min = 1000L, far_max = 2000L
  ),
  lookup = here::here("data", "processed", "site_nearest_long_run.parquet"),
  articles = here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet"),
  calendar_start = as.Date("2014-01-01"),
  calendar_end = as.Date("2024-12-01"),
  # Attention measures in table order; each enters its own regression.
  attention = c("log_cumulative_articles", "post_march_2021", "post_august_2022"),
  event_months = c(post_march_2021 = "2021-03-01", post_august_2022 = "2022-08-01"),
  fixed_effects = "repeat_id + month_index + msoa^year",
  cluster = "lsoa",
  fixef_rm = "perfect_fit"
)

MARKETS <- list(
  sales = list(
    id = "house_id", date = "date_of_transfer", date_type = "timestamp_utc",
    price = "price", start = as.Date("2014-01-01"), end = as.Date("2024-12-31"),
    change_cols = "property_type",
    history = here::here("data", "processed", "house_price_long_run.parquet"),
    mapping = here::here("data", "processed", "repeated_transactions", "repeated_sales.parquet")
  ),
  rentals = list(
    id = "rental_id", date = "rented_est", date_type = "date",
    price = "listing_price", start = as.Date("2014-01-01"), end = as.Date("2023-12-31"),
    change_cols = c("property_type", "bedrooms", "bathrooms"),
    history = here::here("data", "processed", "zoopla", "zoopla_rentals_long_run.parquet"),
    mapping = here::here("data", "processed", "repeated_transactions", "repeated_rentals.parquet")
  )
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

# Months since the calendar start, with January 2014 = 1 (March 2021 = 87,
# August 2022 = 104).
calendar_month_index <- function(dates, start = CONFIG$calendar_start) {
  origin_year <- as.integer(format(start, "%Y"))
  as.integer((data.table::year(dates) - origin_year) * 12L + data.table::month(dates))
}

load_attention <- function() {
  monthly <- as.data.frame(arrow::read_parquet(
    CONFIG$articles, col_select = c("year", "month", "article_count")
  ))
  key <- sprintf("%04d-%02d", as.integer(monthly$year), as.integer(monthly$month))
  if (anyNA(monthly$year) || anyNA(monthly$month) || anyDuplicated(key) > 0L) {
    stop("Article series has missing or duplicate year-month keys.", call. = FALSE)
  }
  if (anyNA(monthly$article_count)) {
    stop("Article series has missing counts.", call. = FALSE)
  }
  months <- seq(CONFIG$calendar_start, CONFIG$calendar_end, by = "month")
  if (max(key) < format(CONFIG$calendar_end, "%Y-%m")) {
    stop("Article series ends at ", max(key), ", before ",
         format(CONFIG$calendar_end, "%Y-%m"), ".", call. = FALSE)
  }
  # The stored series holds only months with at least one article.
  counts <- as.numeric(monthly$article_count[match(format(months, "%Y-%m"), key)])
  counts[is.na(counts)] <- 0
  if (counts[[1L]] <= 0) {
    stop("January 2014 article count is not positive; log stock undefined.", call. = FALSE)
  }
  calendar <- tibble::tibble(
    month_index = calendar_month_index(months),
    month = months,
    article_count = counts,
    cumulative_articles = cumsum(counts),
    log_cumulative_articles = log(cumsum(counts))
  )
  for (measure in names(CONFIG$event_months)) {
    calendar[[measure]] <- as.integer(months >= as.Date(CONFIG$event_months[[measure]]))
  }
  if (!identical(calendar$month_index, seq_len(nrow(calendar)))) {
    stop("Calendar month index is not 1..n from January 2014.", call. = FALSE)
  }
  calendar
}

# Nearest-site lookup at unique-coordinate grain, with distance_m renamed to
# min_distance for build_extensive_margin_sample().
load_long_run_lookup <- function(path) {
  lookup <- as.data.frame(arrow::read_parquet(
    path, col_select = c("easting", "northing", "distance_m")
  ))
  data.table::setDT(lookup)
  data.table::setnames(lookup, "distance_m", "min_distance")
  if (anyDuplicated(lookup, by = c("easting", "northing")) > 0L ||
      any(!is.finite(lookup$min_distance))) {
    stop("Nearest-site lookup is not unique on coordinates or has non-finite distances.",
         call. = FALSE)
  }
  lookup
}

prepare_analysis_data <- function(market, attention, lookup, comparison) {
  dt <- build_long_run_repeat_panel(MARKETS[[market]], lookup, comparison)
  dt[, `:=`(year = data.table::year(date), month_index = calendar_month_index(date))]
  idx <- match(dt$month_index, attention$month_index)
  if (anyNA(idx)) stop(market, ": observations outside the attention calendar.", call. = FALSE)
  for (measure in CONFIG$attention) dt[, (measure) := attention[[measure]][idx]]
  cols <- c("repeat_id", "date", "year", "month_index", "log_price", "near_bin",
            "min_distance", "lsoa", "msoa", CONFIG$attention)
  dt[, cols, with = FALSE]
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

fit_attention <- function(data, market, measure) {
  term <- paste0("near_bin:", measure)
  formula <- stats::as.formula(paste("log_price ~", term, "|", CONFIG$fixed_effects))
  vcov <- stats::as.formula(paste0("~", CONFIG$cluster))
  model <- fixest::feols(formula, data = data, vcov = vcov, fixef.rm = CONFIG$fixef_rm)
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified attention effect: ", market, " / ", term, call. = FALSE)
  }
  model
}

result_row <- function(model, data, market, measure) {
  term <- paste0("near_bin:", measure)
  estimate <- unname(stats::coef(model)[term])
  se <- unname(fixest::se(model)[term])
  n_clusters <- as.numeric(fixest::fitstat(model, "g", simplify = TRUE))
  t_df <- n_clusters - 1
  tibble::tibble(
    market = market, measure = measure, term = term, estimate = estimate, std_error = se,
    t_df = t_df, p_value = 2 * stats::pt(-abs(estimate / se), t_df),
    conf_low = estimate - stats::qt(0.975, t_df) * se,
    conf_high = estimate + stats::qt(0.975, t_df) * se,
    nobs = stats::nobs(model), n_properties = unname(model$fixef_sizes[["repeat_id"]]),
    n_clusters = n_clusters, n_removed_by_estimator = nrow(data) - stats::nobs(model)
  )
}

estimate_market <- function(market, attention, lookup, comparison) {
  data <- prepare_analysis_data(market, attention, lookup, comparison)
  cat(sprintf("  %s sample: %s observations, %s properties\n", market,
              format(nrow(data), big.mark = ","),
              format(data.table::uniqueN(data$repeat_id), big.mark = ",")))
  models <- lapply(CONFIG$attention, function(measure) fit_attention(data, market, measure))
  names(models) <- CONFIG$attention
  results <- dplyr::bind_rows(lapply(CONFIG$attention, function(measure) {
    result_row(models[[measure]], data, market, measure)
  }))
  for (i in seq_len(nrow(results))) {
    cat(sprintf("    %s: %s observations after %s estimator removals\n", results$measure[[i]],
                format(results$nobs[[i]], big.mark = ","),
                format(results$n_removed_by_estimator[[i]], big.mark = ",")))
  }
  list(models = models, results = results)
}


# ==============================================================================
# 4. Table and Model Export
# ==============================================================================

export_table <- function(result, comparison) {
  coefficient_map <- c(
    "near_bin:log_cumulative_articles" = "{Near bin \\\\ $\\times$ $\\log (\\text{Articles})$}",
    "near_bin:post_march_2021" = "{Near bin \\\\ $\\times$ Post March 2021}",
    "near_bin:post_august_2022" = "{Near bin \\\\ $\\times$ Post August 2022}"
  )
  column_names <- sprintf("(%d)", seq_len(6L))
  panels <- list(
    "House Sales" = stats::setNames(unname(result$models$sales), column_names[1:3]),
    "House Rentals" = stats::setNames(unname(result$models$rentals), column_names[4:6])
  )
  results <- result$results
  number <- function(x) format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
  add_rows <- as.data.frame(rbind(
    c("Observations", number(results$nobs)),
    c("Properties", number(results$n_properties)),
    c("LSOA clusters", number(results$n_clusters)),
    c("Property FE", rep("Yes", 6L)),
    c("Month FE", rep("Yes", 6L)),
    c("MSOA $\\times$ year FE", rep("Yes", 6L))
  ), stringsAsFactors = FALSE)
  # Counts come from add_rows (gof_map = NA), so the rows follow the coefficients
  # without a "coef_end" position, which fails when there are no gof rows.
  names(add_rows) <- c("term", column_names)

  notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} ",
    "This table presents repeat-property estimates of how public attention to sewage spills ",
    "changes the price gap between properties near to and far from the nearest mapped ",
    "sewage overflow. The sample covers sales from January 2014 to December 2024 ",
    "(columns 1--3) and rentals from January 2014 to December 2023 (columns 4--6) in ",
    "England, with Greater London retained. A property is a group of transactions or ",
    "listings at the same address, and each property is observed on at least two distinct ",
    "dates. Properties whose type (sales) or type, bedrooms or bathrooms (rentals) take more ",
    "than one observed value are excluded. ",
    comparison_note_text(comparison),
    "The dependent variable is the log transaction price for sales or the log asking rent ",
    "for rentals. Near bin is an indicator equal to one for properties in the ",
    comparison$near_band_label, " band and zero for properties in the ",
    comparison$far_band_label, " band. Each column is a separate regression with one ",
    "attention measure: $\\\\log (\\\\text{Articles})$ is the natural logarithm of cumulative ",
    "UK news coverage of sewage spills from LexisNexis from January 2014 to the observation ",
    "month; Post March 2021 and Post August 2022 are indicators for months from March 2021 ",
    "and from August 2022 inclusive. All regressions include property, calendar-month and ",
    "MSOA-by-year fixed effects, which absorb the near-bin and attention main effects, and ",
    "no property controls. Observations exclude singletons removed by the estimator. ",
    "Standard errors clustered at the LSOA level are reported in parentheses. ",
    "*** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  table_latex <- modelsummary::modelsummary(
    panels, shape = "cbind", output = "latex", escape = FALSE,
    estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01), fmt = fmt_table,
    coef_map = coefficient_map, gof_map = NA, add_rows = add_rows, notes = " ",
    title = CONFIG$title
  )
  table_latex <- patch_modelsummary_latex(table_latex, label = CONFIG$table_label, notes = notes)
  path <- here::here("output", "tables", paste0(CONFIG$output_prefix, ".tex"))
  ensure_output_dir(path)
  writeLines(table_latex, path)
  cat(sprintf("LaTeX table exported to: %s\n", path))
  invisible(path)
}

export_results <- function(result, comparison) {
  export_table(result, comparison)
  for (folder in c("regs", "logs")) {
    dir.create(here::here("output", folder), recursive = TRUE, showWarnings = FALSE)
  }
  # The fitted models hold per-observation vectors for 1.8M rows each, so the RDS
  # keeps the coefficient results and the settings, not the model objects.
  saveRDS(result[c("results", "settings")],
          here::here("output", "regs", paste0(CONFIG$output_prefix, ".rds")))
  utils::write.csv(result$results, here::here(
    "output", "logs", paste0(CONFIG$output_prefix, "_results.csv")
  ), row.names = FALSE)
  invisible(result)
}


# ==============================================================================
# 5. Execution
# ==============================================================================

main <- function() {
  comparison <- validate_comparison_config(CONFIG$comparison)
  attention <- load_attention()
  lookup <- load_long_run_lookup(CONFIG$lookup)
  output <- lapply(names(MARKETS), function(market) {
    cat(sprintf("\nMarket: %s\n", market))
    estimate_market(market, attention, lookup, comparison)
  })
  names(output) <- names(MARKETS)
  # The settings record the validated comparison, with its display labels.
  config <- CONFIG
  config$comparison <- comparison
  result <- list(
    models = lapply(output, function(x) x$models),
    results = dplyr::bind_rows(lapply(output, function(x) x$results)),
    settings = list(config = config, markets = MARKETS, london = "retained",
                    sample = "exclude observed characteristic changes", controls = "none",
                    fixest_version = as.character(utils::packageVersion("fixest")),
                    created_at = Sys.time(), r_version = R.version.string)
  )
  export_results(result, comparison)
}

if (sys.nframe() == 0L) {
  if (length(commandArgs(trailingOnly = TRUE))) {
    stop("This paper script accepts no arguments. ",
         "Run the QMD for exploratory/reproduction analyses.", call. = FALSE)
  }
  main()
}
