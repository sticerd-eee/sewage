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
REQUIRED_PACKAGES <- c("arrow", "data.table", "dplyr", "fixest", "here", "modelsummary")
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
  output_path = here::here("output", "tables", "did_attention_repeat_extensive.tex"),
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

# Monthly attention calendar: the stored series holds only months with at least
# one article, so absent months are zero before the cumulative sum.
load_attention <- function() {
  monthly <- as.data.frame(arrow::read_parquet(
    CONFIG$articles, col_select = c("year", "month", "article_count")
  ))
  key <- sprintf("%04d-%02d", as.integer(monthly$year), as.integer(monthly$month))
  if (anyDuplicated(key) > 0L || anyNA(monthly$article_count)) {
    stop("Article series has duplicate year-month keys or missing counts.", call. = FALSE)
  }
  if (max(key) < format(CONFIG$calendar_end, "%Y-%m")) {
    stop("Article series ends at ", max(key), ", before the calendar end.", call. = FALSE)
  }
  months <- seq(CONFIG$calendar_start, CONFIG$calendar_end, by = "month")
  counts <- as.numeric(monthly$article_count[match(format(months, "%Y-%m"), key)])
  counts[is.na(counts)] <- 0
  if (counts[[1L]] <= 0) {
    stop("January 2014 article count is not positive; log stock undefined.", call. = FALSE)
  }
  calendar <- data.table::data.table(
    month_index = calendar_month_index(months),
    log_cumulative_articles = log(cumsum(counts))
  )
  for (measure in names(CONFIG$event_months)) {
    calendar[, (measure) := as.integer(months >= as.Date(CONFIG$event_months[[measure]]))]
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

# Repeat panel for one market with the calendar-month index and the three
# attention measures attached.
prepare_analysis_data <- function(market, attention, lookup, comparison) {
  dt <- build_long_run_repeat_panel(MARKETS[[market]], lookup, comparison)
  dt[, `:=`(year = data.table::year(date), month_index = calendar_month_index(date))]
  dt <- attention[dt, on = "month_index"]
  if (anyNA(dt$log_cumulative_articles)) {
    stop(market, ": observations outside the attention calendar.", call. = FALSE)
  }
  dt
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

# One regression per attention measure: interaction only, main effects absorbed
# by the property and month fixed effects.
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

estimate_market <- function(market, attention, lookup, comparison) {
  data <- prepare_analysis_data(market, attention, lookup, comparison)
  cat(sprintf("  %s sample: %s observations, %s properties\n", market,
              format(nrow(data), big.mark = ","),
              format(data.table::uniqueN(data$repeat_id), big.mark = ",")))
  models <- lapply(CONFIG$attention, function(measure) fit_attention(data, market, measure))
  names(models) <- CONFIG$attention
  models
}


# ==============================================================================
# 4. Table Export
# ==============================================================================

export_table <- function(models, comparison) {
  coefficient_map <- c(
    "near_bin:log_cumulative_articles" = "{Near bin \\\\ $\\times$ $\\log (\\text{Articles})$}",
    "near_bin:post_march_2021" = "{Near bin \\\\ $\\times$ Post March 2021}",
    "near_bin:post_august_2022" = "{Near bin \\\\ $\\times$ Post August 2022}"
  )
  flat <- c(models$sales, models$rentals)
  column_names <- sprintf("(%d)", seq_along(flat))
  n_sales <- length(models$sales)
  panels <- list(
    "House Sales" = stats::setNames(unname(models$sales), column_names[seq_len(n_sales)]),
    "House Rentals" = stats::setNames(unname(models$rentals), column_names[-seq_len(n_sales)])
  )
  number <- function(x) format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
  yes <- rep("Yes", length(flat))
  add_rows <- as.data.frame(rbind(
    c("Observations", number(vapply(flat, stats::nobs, numeric(1)))),
    c("Properties", number(vapply(flat, function(m) m$fixef_sizes[["repeat_id"]], numeric(1)))),
    c("LSOA clusters", number(vapply(flat, function(m) {
      as.numeric(fixest::fitstat(m, "g", simplify = TRUE))
    }, numeric(1)))),
    c("Property FE", yes),
    c("Month FE", yes),
    c("MSOA $\\times$ year FE", yes)
  ), stringsAsFactors = FALSE)
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
  ensure_output_dir(CONFIG$output_path)
  writeLines(table_latex, CONFIG$output_path)
  cat(sprintf("LaTeX table exported to: %s\n", CONFIG$output_path))
  invisible(CONFIG$output_path)
}


# ==============================================================================
# 5. Execution
# ==============================================================================

main <- function() {
  comparison <- validate_comparison_config(CONFIG$comparison)
  attention <- load_attention()
  lookup <- load_long_run_lookup(CONFIG$lookup)
  models <- lapply(names(MARKETS), function(market) {
    cat(sprintf("\nMarket: %s\n", market))
    estimate_market(market, attention, lookup, comparison)
  })
  names(models) <- names(MARKETS)
  print(fixest::etable(c(models$sales, models$rentals), digits = 4))
  export_table(models, comparison)
}

if (sys.nframe() == 0L) {
  if (length(commandArgs(trailingOnly = TRUE))) {
    stop("This paper script accepts no arguments. ",
         "Run the QMD for exploratory/reproduction analyses.", call. = FALSE)
  }
  main()
}
