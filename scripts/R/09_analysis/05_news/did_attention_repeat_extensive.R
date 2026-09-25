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
#   - output/regs/did_attention_repeat_extensive.rds
#   - output/logs/did_attention_repeat_extensive_{results,cell_counts}.csv
#
# Prerequisite: data/processed/site_nearest_long_run.parquet is written by
#   scripts/R/04_feature_engineering/site_nearest_long_run_match.R, which has no
#   runner and must be run by hand before this script.
#
# Run from the repository root with Rscript in the rv environment (R 4.6.0).
# Full exploration: docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd
# (variant `exclude_changes`, fixed effects `property_month_msoa_year`).
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install project dependencies with `rv sync`.",
       call. = FALSE)
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
REQUIRED_PACKAGES <- c("arrow", "data.table", "dplyr", "fixest", "here", "modelsummary", "tibble")
check_required_packages(REQUIRED_PACKAGES)
source(here::here("scripts", "R", "utils", "repeat_transactions_utils.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "05_news",
                  "extensive_margin_news_utils.R"), local = TRUE)

`%chin%` <- data.table::`%chin%`


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
  comparison = validate_comparison_config(list(
    comparison_id = "500_vs_1000_2000", comparison_label = "0-500m vs 1000-2000m",
    near_min = 0L, near_max = 500L, far_min = 1000L, far_max = 2000L
  )),
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

count_properties <- function(repeat_id) data.table::uniqueN(repeat_id[!is.na(repeat_id)])

record_step <- function(counts, market, step, dt, note) {
  counts[[length(counts) + 1L]] <- tibble::tibble(
    market = market, step_order = length(counts) + 1L, step = step,
    n_rows = nrow(dt), n_properties = count_properties(dt$repeat_id), note = note
  )
  counts
}

# (1) Window rows, filtered inside Arrow; bounds are UTC days for timestamps.
load_window_history <- function(spec) {
  cols <- unique(c(spec$id, spec$date, spec$price, "country", "easting", "northing",
                   "lsoa", "msoa", spec$change_cols))
  lower <- spec$start
  upper <- spec$end + 1L
  if (spec$date_type == "timestamp_utc") {
    lower <- as.POSIXct(format(lower), tz = "UTC")
    upper <- as.POSIXct(format(upper), tz = "UTC")
  }
  history <- arrow::open_dataset(spec$history) |>
    dplyr::select(dplyr::all_of(cols)) |>
    dplyr::filter(.data[[spec$date]] >= lower, .data[[spec$date]] < upper) |>
    dplyr::collect() |>
    as.data.frame()
  data.table::setDT(history)
  history[, date := as_transaction_date(get(spec$date))]
  if (spec$date != "date") history[, (spec$date) := NULL]
  if (anyNA(history$date) || min(history$date) < spec$start || max(history$date) > spec$end) {
    stop("Window rows fall outside ", spec$start, " to ", spec$end, ".", call. = FALSE)
  }
  if (anyDuplicated(history[[spec$id]]) > 0L) {
    stop("History IDs are not unique within the window.", call. = FALSE)
  }
  history
}

# (2) Mapping rows restricted to the window IDs inside Arrow.
attach_repeat_ids <- function(history, spec) {
  ids <- history[[spec$id]]
  mapping <- arrow::open_dataset(spec$mapping) |>
    dplyr::select(dplyr::all_of(c(spec$id, "repeat_id"))) |>
    dplyr::filter(.data[[spec$id]] %in% ids) |>
    dplyr::collect() |>
    as.data.frame()
  if (anyDuplicated(mapping[[spec$id]]) > 0L || anyNA(mapping$repeat_id)) {
    stop("Repeat mapping is not unique on ", spec$id, " or lacks repeat_id.", call. = FALSE)
  }
  history[, repeat_id := mapping$repeat_id[data.table::chmatch(ids, mapping[[spec$id]])]]
  history[, (spec$id) := NULL]
  history
}

# (3) More than one distinct nonmissing value within a Repeat Group; missing-to-
# observed differences are not contradictions.
geography_contradictions <- function(dt) {
  checks <- list(country = "country", coordinates = c("easting", "northing"),
                 lsoa = "lsoa", msoa = "msoa")
  vapply(checks, function(cols) {
    x <- dt[stats::complete.cases(dt[, cols, with = FALSE]), c("repeat_id", cols), with = FALSE]
    as.numeric(nrow(unique(x)[, .N, by = repeat_id][N > 1L]))
  }, numeric(1))
}

# At least two distinct surviving dates per Repeat Group (AE1).
repeat_eligible_ids <- function(dt) {
  unique(dt[, .(repeat_id, date)])[, .N, by = repeat_id][N >= 2L, repeat_id]
}

# Groups where a characteristic takes more than one distinct nonmissing value (AE2).
changed_property_ids <- function(dt, change_cols) {
  unique(unlist(lapply(change_cols, function(col) {
    unique(dt[!is.na(get(col)), c("repeat_id", col), with = FALSE])[
      , .N, by = repeat_id][N > 1L, repeat_id]
  })))
}

prepare_analysis_data <- function(market, attention, lookup) {
  spec <- MARKETS[[market]]
  counts <- list()

  dt <- load_window_history(spec)
  dt <- attach_repeat_ids(dt, spec)
  counts <- record_step(counts, market, "window", dt, sprintf(
    "%s from %s to %s inclusive (properties counted among mapped rows)",
    spec$date, spec$start, spec$end))
  dt <- dt[!is.na(repeat_id)]
  counts <- record_step(counts, market, "mapped", dt, "rows present in the repeat mapping")

  contradictions <- geography_contradictions(dt)
  if (any(contradictions > 0)) {
    stop(market, ": Repeat Groups with contradictory geography: ",
         paste(names(contradictions), contradictions, sep = "=", collapse = ", "), call. = FALSE)
  }
  counts <- record_step(counts, market, "geography_check", dt,
                        "0 Repeat Groups with contradictory country, coordinates, LSOA or MSOA")

  dt <- dt[!is.na(country) & country == "England"]
  counts <- record_step(counts, market, "england", dt, "country England, Greater London retained")
  dt[, log_price := log(as.numeric(get(spec$price)))]
  dt <- dt[is.finite(log_price) & get(spec$price) > 0]
  counts <- record_step(counts, market, "finite_positive_price", dt,
                        paste0("finite positive ", spec$price))
  dt <- dt[!is.na(easting) & !is.na(northing) & !is.na(lsoa) & nzchar(lsoa) &
             !is.na(msoa) & nzchar(msoa)]
  counts <- record_step(counts, market, "coordinates_lsoa_msoa", dt,
                        "finite coordinates and nonmissing nonempty LSOA and MSOA")
  dt[, c(spec$price, "country") := NULL]

  if (nrow(dt[!lookup, on = c("easting", "northing")]) > 0L) {
    stop(market, ": coordinates absent from the long-run nearest-site lookup; ",
         "rerun site_nearest_long_run_match.R.", call. = FALSE)
  }
  data.table::setDF(dt)
  dt <- build_extensive_margin_sample(dt, lookup, c("easting", "northing"), CONFIG$comparison)
  data.table::setDT(dt)
  dt[, c("comparison_id", "comparison_label") := NULL]
  counts <- record_step(counts, market, "distance_bands", dt, sprintf(
    "near [%d, %d] m, far (%d, %d] m", CONFIG$comparison$near_min,
    CONFIG$comparison$near_max, CONFIG$comparison$far_min, CONFIG$comparison$far_max))

  dt <- dt[repeat_id %chin% repeat_eligible_ids(dt)]
  counts <- record_step(counts, market, "two_distinct_dates", dt,
                        "properties observed on at least two distinct dates")
  dt <- dt[!repeat_id %chin% changed_property_ids(dt, spec$change_cols)]
  counts <- record_step(counts, market, "exclude_changes", dt, paste0(
    "drop properties with more than one distinct nonmissing ",
    paste(spec$change_cols, collapse = ", ")))
  dt <- dt[repeat_id %chin% repeat_eligible_ids(dt)]
  counts <- record_step(counts, market, "repeats_recomputed", dt,
                        "at least two distinct dates recomputed after the exclusion")

  dt[, `:=`(year = data.table::year(date), month_index = calendar_month_index(date))]
  idx <- match(dt$month_index, attention$month_index)
  if (anyNA(idx)) stop(market, ": observations outside the attention calendar.", call. = FALSE)
  for (measure in CONFIG$attention) dt[, (measure) := attention[[measure]][idx]]
  cols <- c("repeat_id", "date", "year", "month_index", "log_price", "near_bin",
            "min_distance", "lsoa", "msoa", CONFIG$attention)
  list(data = dt[, cols, with = FALSE], counts = dplyr::bind_rows(counts),
       geography_contradictions = contradictions)
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

fit_attention <- function(data, market, measure) {
  term <- paste0("near_bin:", measure)
  formula <- stats::as.formula(paste("log_price ~", term, "|", CONFIG$fixed_effects))
  model <- fixest::feols(formula, data = data, vcov = stats::as.formula(paste0("~", CONFIG$cluster)),
                         lean = TRUE, fixef.rm = CONFIG$fixef_rm)
  if (!is.finite(stats::coef(model)[term]) || !is.finite(fixest::se(model)[term])) {
    stop("Unidentified attention effect: ", market, " / ", term, call. = FALSE)
  }
  model
}

# fixest::obs() is unavailable under lean = TRUE, so counts come from the fit.
model_counts <- function(model, data) {
  df_t <- as.numeric(attr(model$cov.scaled, "df.t"))
  n_clusters <- df_t + 1
  if (!isTRUE(n_clusters == as.numeric(attr(model$cov.scaled, "G")))) {
    stop("Cluster count from df.t does not match the clustered vcov.", call. = FALSE)
  }
  list(nobs = stats::nobs(model), n_properties = unname(model$fixef_sizes[["repeat_id"]]),
       n_clusters = n_clusters, t_df = df_t,
       n_removed_by_estimator = nrow(data) - stats::nobs(model))
}

estimate_market <- function(market, attention, lookup) {
  prepared <- prepare_analysis_data(market, attention, lookup)
  data <- prepared$data
  cat(sprintf("  %s: %s observations, %s properties before estimator removals\n", market,
              format(nrow(data), big.mark = ","),
              format(count_properties(data$repeat_id), big.mark = ",")))
  models <- lapply(CONFIG$attention, function(measure) fit_attention(data, market, measure))
  names(models) <- CONFIG$attention
  model_rows <- lapply(CONFIG$attention, function(measure) {
    counts <- model_counts(models[[measure]], data)
    term <- paste0("near_bin:", measure)
    estimate <- unname(stats::coef(models[[measure]])[term])
    se <- unname(fixest::se(models[[measure]])[term])
    tibble::tibble(
      market = market, measure = measure, term = term, estimate = estimate, std_error = se,
      t_df = counts$t_df, p_value = 2 * stats::pt(-abs(estimate / se), counts$t_df),
      conf_low = estimate - stats::qt(0.975, counts$t_df) * se,
      conf_high = estimate + stats::qt(0.975, counts$t_df) * se,
      nobs = counts$nobs, n_properties = counts$n_properties, n_clusters = counts$n_clusters,
      n_removed_by_estimator = counts$n_removed_by_estimator
    )
  })
  results <- dplyr::bind_rows(model_rows)
  step_counts <- prepared$counts
  estimator_counts <- tibble::tibble(
    market = market, step_order = nrow(step_counts) + seq_len(nrow(results)),
    step = paste0("estimation:", results$measure), n_rows = results$nobs,
    n_properties = results$n_properties,
    note = sprintf("fixef.rm = %s removed %s observations", CONFIG$fixef_rm,
                   results$n_removed_by_estimator)
  )
  counts <- dplyr::bind_rows(step_counts, estimator_counts) |>
    dplyr::mutate(n_rows_removed = dplyr::lag(.data$n_rows, default = NA) - .data$n_rows,
                  .after = "n_properties")
  counts$n_rows_removed[startsWith(counts$step, "estimation:")] <- results$n_removed_by_estimator
  print(as.data.frame(counts[, c("step", "n_rows", "n_properties", "n_rows_removed")]))
  rm(data, prepared)
  invisible(gc())
  list(models = models, results = results, counts = counts)
}


# ==============================================================================
# 4. Table and Model Export
# ==============================================================================

export_table <- function(result) {
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
    comparison_note_text(CONFIG$comparison),
    "The dependent variable is the log transaction price for sales or the log asking rent ",
    "for rentals. Near bin is an indicator equal to one for properties in the ",
    CONFIG$comparison$near_band_label, " band and zero for properties in the ",
    CONFIG$comparison$far_band_label, " band. Each column is a separate regression with one ",
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
  attention <- load_attention()
  lookup <- as.data.frame(arrow::read_parquet(
    CONFIG$lookup, col_select = c("easting", "northing", "distance_m")
  ))
  data.table::setDT(lookup)
  data.table::setnames(lookup, "distance_m", "min_distance")
  if (anyDuplicated(lookup, by = c("easting", "northing")) > 0L ||
      any(!is.finite(lookup$min_distance))) {
    stop("Nearest-site lookup is not unique on coordinates or has non-finite distances.",
         call. = FALSE)
  }
  output <- lapply(names(MARKETS), function(market) {
    cat(sprintf("\nMarket: %s\n", market))
    estimate_market(market, attention, lookup)
  })
  names(output) <- names(MARKETS)
  result <- list(
    models = lapply(output, function(x) x$models),
    counts = dplyr::bind_rows(lapply(output, function(x) x$counts)),
    results = dplyr::bind_rows(lapply(output, function(x) x$results)),
    calendar = attention,
    settings = list(config = CONFIG, markets = MARKETS, london = "retained",
                    sample = "exclude observed characteristic changes", controls = "none",
                    fixest_version = as.character(utils::packageVersion("fixest")),
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
