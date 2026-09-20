# ==============================================================================
# Intensive-Margin Post August 2022 by Salience Group
# ==============================================================================
#
# Purpose: Estimate the selected paper specification for All Bathing, All Coastal
#          and All Inland, separately for sales and rentals. These groups overlap.
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/cross_section/{sales,rentals}/prior_to_{sale,rental}/
#   - data/processed/cross_section/{sales,rentals}/prior_characteristics/
#
# Outputs:
#   - output/tables/did_trends_prior_salience_groups.tex
#   - output/regs/did_trends_prior_salience_groups.rds
#   - output/logs/did_trends_prior_salience_groups_{results,cell_counts}.csv
#
# Run from the repository root with Rscript in the rv environment (R 4.6.0).
# Full exploration: docs/reports/2026-09-03-003-heterogeneity-by-salience-report.qmd
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required. Install project dependencies with `rv sync`.",
       call. = FALSE)
}
source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
REQUIRED_PACKAGES <- c("arrow", "dplyr", "fixest", "forcats", "here", "modelsummary", "tibble")
check_required_packages(REQUIRED_PACKAGES)
source(here::here("scripts", "R", "utils", "salience_group_utils.R"), local = TRUE)
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"), local = TRUE)


# ==============================================================================
# 1. Configuration
# ==============================================================================

CONFIG <- list(
  coast_rule_m = 2000L,
  output_prefix = "did_trends_prior_salience_groups",
  title = "Intensive-Margin Post August 2022 by Salience Group",
  attention = "post",
  post_month_id = 20L,
  radius = 250L,
  groups = c("bathing", "coastal", "inland")
)

MARKETS <- list(
  sales = list(
    id = "house_id",
    transactions = here::here("data", "processed", "house_price.parquet"),
    exposure = here::here("data", "processed", "cross_section", "sales", "prior_to_sale"),
    companion = here::here("data", "processed", "cross_section", "sales", "prior_characteristics")
  ),
  rentals = list(
    id = "rental_id",
    transactions = here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
    exposure = here::here("data", "processed", "cross_section", "rentals", "prior_to_rental"),
    companion = here::here("data", "processed", "cross_section", "rentals", "prior_characteristics")
  )
)


# ==============================================================================
# 2. Data Preparation
# ==============================================================================

load_attention <- function() {
  # The selected paper specification fixes the break at August 2022, inclusive.
  tibble::tibble(month_id = 1:48, post = as.integer(month_id >= CONFIG$post_month_id))
}

prepare_salience_intensive <- function(
  transactions, exposure, attention_data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  id_col <- if (market == "sales") "house_id" else "rental_id"
  price_col <- if (market == "sales") "price" else "listing_price"
  end_month <- if (market == "sales") 48L else 36L
  controls <- if (market == "sales") c("property_type", "old_new", "duration") else
    c("property_type", "bedrooms", "bathrooms")
  factors <- if (market == "sales") controls else "property_type"
  transactions <- transactions |>
    dplyr::select(dplyr::all_of(c(id_col, price_col, "region", "month_id", "lsoa",
                                  "latitude", "longitude", controls))) |>
    dplyr::mutate(dplyr::across(dplyr::all_of(factors), forcats::as_factor))
  exposure |>
    dplyr::filter(.data$radius == 250L) |>
    dplyr::select(dplyr::all_of(c(id_col, "spill_count_weekly_avg", "n_spill_sites"))) |>
    dplyr::collect() |>
    dplyr::filter(.data$n_spill_sites > 0L) |>
    dplyr::inner_join(transactions, by = id_col, relationship = "one-to-one") |>
    dplyr::inner_join(attention_data, by = "month_id", relationship = "many-to-one") |>
    dplyr::mutate(log_price = log(.data[[price_col]])) |>
    dplyr::filter(
      .data$month_id >= 1L, .data$month_id <= .env$end_month,
      !is.na(.data$spill_count_weekly_avg), is.finite(.data[[attention]]),
      is.finite(.data$log_price),
      dplyr::if_all(dplyr::all_of(c("lsoa", "month_id", "latitude", "longitude", controls)),
                    ~ !is.na(.x))
    ) |>
    dplyr::mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(.data$lsoa)),
      dplyr::across(dplyr::all_of(factors), forcats::fct_drop)
    )
}

prepare_analysis_data <- function(market, attention_data) {
  spec <- MARKETS[[market]]
  data <- prepare_salience_intensive(
    arrow::read_parquet(spec$transactions), arrow::open_dataset(spec$exposure),
    attention_data, market, CONFIG$attention
  )
  join_salience_group_companion(
    data, arrow::open_dataset(spec$companion), spec$id,
    radius = CONFIG$radius, coast_rule_m = CONFIG$coast_rule_m
  )
}


# ==============================================================================
# 3. Estimation
# ==============================================================================

fit_salience_intensive <- function(
  data, market = c("sales", "rentals"),
  attention = c("post", "log_cumulative_articles")
) {
  market <- match.arg(market)
  attention <- match.arg(attention)
  controls <- if (market == "sales") "property_type + old_new + duration" else
    "property_type + bedrooms + bathrooms"
  terms <- c("spill_count_weekly_avg", paste0("spill_count_weekly_avg:", attention))
  formula <- stats::as.formula(paste(
    "log_price ~", paste(terms, collapse = " + "), "+", controls, "| lsoa + month_id"
  ))
  model <- fixest::feols(formula, data = data, vcov = ~lsoa, lean = TRUE)
  if (any(!is.finite(stats::coef(model)[terms])) || any(!is.finite(fixest::se(model)[terms]))) {
    stop("Unidentified spill/attention effect: ", market, " / ", attention, call. = FALSE)
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
    fit_salience_intensive(sample, market, CONFIG$attention)
  })
  names(models) <- CONFIG$groups
  counts$nobs <- vapply(models, stats::nobs, numeric(1))
  counts$n_removed_by_estimator <- counts$n_estimation - counts$nobs
  results <- dplyr::bind_rows(lapply(names(models), function(group) {
    salience_group_results(models[[group]]) |>
      dplyr::filter(.data$term %in% c("spill_count_weekly_avg", paste0("spill_count_weekly_avg:", CONFIG$attention))) |>
      dplyr::mutate(market = .env$market, group = .env$group, .before = 1L)
  }))
  list(models = models, counts = counts, results = results)
}


# ==============================================================================
# 4. Table and Model Export
# ==============================================================================

export_table <- function(result) {
  coefficient_map <- c(spill_count_weekly_avg = "Spills per week (avg.)")
  coefficient_map[paste0("spill_count_weekly_avg:", CONFIG$attention)] <- "{Spills per week (avg.) \\\\ $\\times$ Post}"
  notes <- paste0(
    "This table presents hedonic estimates of the relationship between sewage spill exposure, ",
    "public attention, and property values, estimated separately within each salience group. ",
    "The sample includes all properties within 250m of a storm overflow in England, excluding ",
    "Greater London, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are ",
    "available). ",
    salience_group_notes("radius", CONFIG$coast_rule_m),
    "The dependent variable is the log transaction price for sales or the log weekly ",
    "asking rent for rentals. ",
    "Spill exposure is measured as the average number of spill events per week (12/24 count) ",
    "recorded across all overflows within 250m from January 2021 to the transaction date. ",
    "Post is an indicator equal to one for transactions occurring on or after August 2022, ",
    "the peak month for Google Trends searches and news coverage of sewage spills. ",
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
  output <- lapply(names(MARKETS), function(market) {
    data <- prepare_analysis_data(market, attention_data)
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
