# ==============================================================================
# Daily Spill Logit Estimates and Probability Effects by Lag
# ==============================================================================
#
# Purpose: Estimate daily spill-incidence logit models using lags 0--7 for
#          continuous rainfall and dry-day indicators, then export one appendix
#          table with raw logit coefficients and probability-scale effects.
#
# Author: Jacopo Olivieri
# Date: 2026-03-08
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_daily.parquet
#
# Outputs:
#   - output/tables/dry_spill_logit_ame.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
TABLE_CAPTION <- "Daily Spill Logit Estimates by Lag"
TABLE_LABEL <- "tbl:dry-spill-logit-ame"
OUTPUT_FILENAME <- "dry_spill_logit_ame.tex"
LAGS <- 0:7L
DRY_DAY_THRESHOLD_MM <- 0.25
RAINFALL_AME_SCALE_MM <- 10
TABLE_COEF_DIGITS <- 3L
TABLE_EFFECT_DIGITS <- 4L
RAIN_TERMS <- vapply(
  LAGS,
  function(lag) if (lag == 0L) "rain_t" else paste0("rain_l", lag),
  character(1L)
)
DRY_TERMS <- vapply(
  LAGS,
  function(lag) if (lag == 0L) "dry_t" else paste0("dry_l", lag),
  character(1L)
)
LAG_LABELS <- ifelse(LAGS == 0L, "t", paste0("t-", LAGS))
TABLE_COLUMN_LABELS <- paste0("(", seq_len(4L), ")")
TABLE_HEADER_LABELS <- c(
  "Logit \\\\ coefficient",
  "AME",
  "Logit \\\\ coefficient",
  "Probability \\\\ effect"
)


# ==============================================================================
# 2. Package Management
# ==============================================================================
required_packages <- c(
  "arrow",
  "data.table",
  "fixest",
  "here"
)

missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1L), quietly = TRUE)
]

if (length(missing_packages) > 0L) {
  stop(
    "Missing required packages: ",
    paste(missing_packages, collapse = ", "),
    ". Run renv::restore() before running this script.",
    call. = FALSE
  )
}


# ==============================================================================
# 3. Setup
# ==============================================================================
output_dir <- here::here("output", "tables")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

output_file <- file.path(output_dir, OUTPUT_FILENAME)

format_num <- function(x, digits) {
  formatted <- formatC(x, format = "f", digits = digits)
  negative_zero <- grepl("^-0(?:\\.0+)?$", formatted, perl = TRUE)
  formatted[negative_zero] <- sub("^-", "", formatted[negative_zero])

  paste0("\\num{", formatted, "}")
}

significance_stars <- function(p_value) {
  out <- rep.int("", length(p_value))
  ok <- !is.na(p_value)

  out[ok & p_value < 0.1] <- "*"
  out[ok & p_value < 0.05] <- "**"
  out[ok & p_value < 0.01] <- "***"

  out
}

format_estimate <- function(x, p_value, digits = 3L) {
  out <- rep.int("", length(x))
  ok <- !is.na(x)

  out[ok] <- paste0(
    format_num(x[ok], digits = digits),
    significance_stars(p_value[ok])
  )

  out
}

format_statistic <- function(x, digits = 3L) {
  out <- rep.int("", length(x))
  ok <- !is.na(x)

  out[ok] <- paste0("(", format_num(x[ok], digits = digits), ")")

  out
}

extract_fitstat_scalar <- function(model, stat) {
  value <- fixest::fitstat(model, stat)

  if (is.list(value)) {
    value <- unlist(value, recursive = TRUE, use.names = FALSE)
  }

  as.numeric(value[1L])
}

delta_se <- function(gradient, vcov_mat) {
  missing_terms <- setdiff(colnames(vcov_mat), names(gradient))

  if (length(missing_terms) > 0L) {
    stop(
      "Gradient is missing terms required by the variance-covariance matrix: ",
      paste(missing_terms, collapse = ", "),
      call. = FALSE
    )
  }

  gradient <- gradient[colnames(vcov_mat)]
  variance <- as.numeric(t(gradient) %*% vcov_mat %*% gradient)
  sqrt(pmax(variance, 0))
}

align_design_matrix <- function(design_matrix, beta) {
  coef_names <- names(beta)
  missing_terms <- setdiff(coef_names, colnames(design_matrix))

  if (length(missing_terms) > 0L) {
    stop(
      "Design matrix is missing model terms: ",
      paste(missing_terms, collapse = ", "),
      call. = FALSE
    )
  }

  design_matrix[, coef_names, drop = FALSE]
}

build_estimate_row <- function(model, terms, design_matrix, effect_builder) {
  beta <- stats::coef(model)
  vcov_mat <- stats::vcov(model)
  coef_se <- sqrt(diag(vcov_mat))
  missing_terms <- setdiff(terms, names(beta))

  if (length(missing_terms) > 0L) {
    stop(
      "Requested terms are missing from the model coefficients: ",
      paste(missing_terms, collapse = ", "),
      call. = FALSE
    )
  }

  design_matrix <- align_design_matrix(design_matrix, beta)

  rows <- vector("list", length(terms))

  for (i in seq_along(terms)) {
    term <- terms[i]
    effect <- effect_builder(
      design_matrix = design_matrix,
      beta = beta,
      term = term
    )

    effect_est <- effect$estimate
    effect_se <- delta_se(effect$gradient, vcov_mat)

    coef_p <- 2 * stats::pnorm(abs(beta[term] / coef_se[term]), lower.tail = FALSE)
    effect_p <- 2 * stats::pnorm(abs(effect_est / effect_se), lower.tail = FALSE)

    rows[[i]] <- data.table::data.table(
      term = term,
      coef_estimate = unname(beta[term]),
      coef_se = unname(coef_se[term]),
      coef_p = coef_p,
      effect_estimate = unname(effect_est),
      effect_se = effect_se,
      effect_p = effect_p
    )
  }

  data.table::rbindlist(rows)
}

continuous_ame_builder <- function(design_matrix, beta, term) {
  term_index <- match(term, colnames(design_matrix))
  beta_term <- unname(beta[[term]])

  if (is.na(term_index)) {
    stop("Could not locate term in design matrix: ", term, call. = FALSE)
  }

  eta <- as.numeric(design_matrix %*% beta)
  prob <- stats::plogis(eta)
  weight <- prob * (1 - prob)
  slope_weight <- weight * (1 - 2 * prob)

  estimate <- mean(weight) * beta_term
  gradient <- beta_term * colMeans(design_matrix * slope_weight)
  gradient[term] <- gradient[term] + mean(weight)

  list(
    estimate = estimate,
    gradient = gradient
  )
}

discrete_change_builder <- function(design_matrix, beta, term) {
  term_index <- match(term, colnames(design_matrix))

  if (is.na(term_index)) {
    stop("Could not locate term in design matrix: ", term, call. = FALSE)
  }

  design_zero <- design_matrix
  design_one <- design_matrix
  design_zero[, term_index] <- 0
  design_one[, term_index] <- 1

  eta_one <- as.numeric(design_one %*% beta)
  eta_zero <- as.numeric(design_zero %*% beta)
  prob_one <- stats::plogis(eta_one)
  prob_zero <- stats::plogis(eta_zero)
  weight_one <- prob_one * (1 - prob_one)
  weight_zero <- prob_zero * (1 - prob_zero)

  list(
    estimate = mean(prob_one - prob_zero),
    gradient = colMeans(design_one * weight_one - design_zero * weight_zero)
  )
}


# ==============================================================================
# 4. Data Preparation
# ==============================================================================
cat("Loading daily spill panel...\n")

daily_path <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_daily.parquet"
)

dat_daily <- arrow::read_parquet(
  daily_path,
  col_select = c(
    "site_id",
    "date",
    "spill_count",
    "rainfall_max_9cell_d0_na_rm",
    "site_missing"
  )
) |>
  data.table::as.data.table()

dat_daily[, `:=`(
  date = as.Date(date),
  spill_any = data.table::fifelse(
    is.na(spill_count),
    NA_integer_,
    as.integer(spill_count == 1L)
  )
)]

data.table::setorder(dat_daily, site_id, date)

date_gaps <- dat_daily[, .(
  date = date,
  prev_date = data.table::shift(date),
  date_gap = as.integer(date - data.table::shift(date))
), by = site_id][!is.na(date_gap) & date_gap != 1L]

if (nrow(date_gaps) > 0L) {
  example_gap <- date_gaps[1L]
  stop(
    sprintf(
      paste0(
        "Detected %s non-consecutive site-date rows in agg_spill_daily.parquet. ",
        "First example: site_id=%s, prev_date=%s, date=%s, gap=%s."
      ),
      format(nrow(date_gaps), big.mark = ","),
      example_gap$site_id,
      example_gap$prev_date,
      example_gap$date,
      example_gap$date_gap
    ),
    call. = FALSE
  )
}

dat_daily[, (RAIN_TERMS) := data.table::shift(
  rainfall_max_9cell_d0_na_rm,
  n = LAGS,
  type = "lag"
), by = site_id]

dat_daily[, (DRY_TERMS) := lapply(.SD, function(x) {
  as.integer(x < DRY_DAY_THRESHOLD_MM)
}), .SDcols = RAIN_TERMS]

estimation_pool <- dat_daily[
  site_missing == FALSE &
    !is.na(date) &
    !is.na(spill_count)
]

dat_daily_lag7 <- estimation_pool[
  stats::complete.cases(estimation_pool[, ..RAIN_TERMS])
]

sample_start_year <- format(min(dat_daily_lag7$date, na.rm = TRUE), "%Y")
sample_end_year <- format(max(dat_daily_lag7$date, na.rm = TRUE), "%Y")
sample_period <- if (identical(sample_start_year, sample_end_year)) {
  sample_start_year
} else {
  paste0(sample_start_year, "--", sample_end_year)
}

n_obs <- nrow(dat_daily_lag7)
n_sites <- data.table::uniqueN(dat_daily_lag7$site_id)
mean_dep_var <- mean(dat_daily_lag7$spill_any, na.rm = TRUE)

cat(sprintf("  Retained %s site-days\n", format(n_obs, big.mark = ",")))
cat(sprintf("  Distinct overflow sites: %s\n", format(n_sites, big.mark = ",")))
cat(sprintf("  Sample period: %s\n", sample_period))


# ==============================================================================
# 5. Model Estimation
# ==============================================================================
cat("Estimating rainfall logit model...\n")

mod_rain_lag7 <- fixest::feglm(
  stats::reformulate(RAIN_TERMS, response = "spill_any"),
  data = dat_daily_lag7,
  family = "binomial",
  vcov = ~site_id
)

cat("Estimating dry-day logit model...\n")

mod_dry_lag7 <- fixest::feglm(
  stats::reformulate(DRY_TERMS, response = "spill_any"),
  data = dat_daily_lag7,
  family = "binomial",
  vcov = ~site_id
)

rain_design <- stats::model.matrix(
  stats::reformulate(RAIN_TERMS),
  data = dat_daily_lag7
)

dry_design <- stats::model.matrix(
  stats::reformulate(DRY_TERMS),
  data = dat_daily_lag7
)


# ==============================================================================
# 6. Effect Computation
# ==============================================================================
cat("Computing probability-scale effects...\n")

rain_results <- build_estimate_row(
  model = mod_rain_lag7,
  terms = RAIN_TERMS,
  design_matrix = rain_design,
  effect_builder = continuous_ame_builder
)

rain_results[, `:=`(
  effect_estimate = effect_estimate * RAINFALL_AME_SCALE_MM,
  effect_se = effect_se * RAINFALL_AME_SCALE_MM
)]

dry_results <- build_estimate_row(
  model = mod_dry_lag7,
  terms = DRY_TERMS,
  design_matrix = dry_design,
  effect_builder = discrete_change_builder
)

table_rows <- data.table::data.table(
  lag = LAG_LABELS,
  rain_coef = format_estimate(rain_results$coef_estimate, rain_results$coef_p, digits = TABLE_COEF_DIGITS),
  rain_coef_se = format_statistic(rain_results$coef_se, digits = TABLE_COEF_DIGITS),
  rain_ame = format_estimate(rain_results$effect_estimate, rain_results$effect_p, digits = TABLE_EFFECT_DIGITS),
  rain_ame_se = format_statistic(rain_results$effect_se, digits = TABLE_EFFECT_DIGITS),
  dry_coef = format_estimate(dry_results$coef_estimate, dry_results$coef_p, digits = TABLE_COEF_DIGITS),
  dry_coef_se = format_statistic(dry_results$coef_se, digits = TABLE_COEF_DIGITS),
  dry_effect = format_estimate(dry_results$effect_estimate, dry_results$effect_p, digits = TABLE_EFFECT_DIGITS),
  dry_effect_se = format_statistic(dry_results$effect_se, digits = TABLE_EFFECT_DIGITS)
)

body_lines <- unlist(lapply(seq_len(nrow(table_rows)), function(i) {
  estimate_line <- paste(
    table_rows$lag[i],
    table_rows$rain_coef[i],
    table_rows$rain_ame[i],
    table_rows$dry_coef[i],
    table_rows$dry_effect[i],
    sep = " & "
  )

  se_line <- paste(
    "",
    table_rows$rain_coef_se[i],
    table_rows$rain_ame_se[i],
    table_rows$dry_coef_se[i],
    table_rows$dry_effect_se[i],
    sep = " & "
  )

  c(
    paste0(estimate_line, " \\\\"),
    paste0(se_line, " \\\\")
  )
}))

table_note <- paste0(
  "\\footnotesize{\\textbf{Notes:} This table presents logit estimates of ",
  "daily spill incidence for storm overflow sites in England, ",
  sample_period,
  ". Columns (1) and (2) use daily rainfall in millimetres on day $t$ and ",
  "its seven daily lags; columns (3) and (4) use indicators for rainfall ",
  "below ",
  formatC(DRY_DAY_THRESHOLD_MM, format = "f", digits = 2L),
  " mm on day $t$ and each of the previous seven days. Columns (1) and (3) ",
  "report logit coefficients. Column (2) reports average marginal effects for ",
  "a ",
  formatC(RAINFALL_AME_SCALE_MM, format = "f", digits = 0L),
  " mm increase in rainfall at the indicated lag, and column (4) reports ",
  "average discrete changes in predicted spill probability from switching the ",
  "dry-day indicator from 0 to 1 at the indicated lag. Standard errors ",
  "clustered at the site level are reported in parentheses. *** p<0.01, ",
  "** p<0.05, * p<0.1.}"
)

summary_table <- data.table::data.table(
  label = c("Mean Dep. Var.", "Observations", "Adj. Pseudo R2"),
  `(1)` = c(
    format_num(mean_dep_var, digits = 3L),
    format_num(stats::nobs(mod_rain_lag7), digits = 0L),
    format_num(extract_fitstat_scalar(mod_rain_lag7, "apr2"), digits = 3L)
  ),
  `(2)` = c("", "", ""),
  `(3)` = c(
    format_num(mean_dep_var, digits = 3L),
    format_num(stats::nobs(mod_dry_lag7), digits = 0L),
    format_num(extract_fitstat_scalar(mod_dry_lag7, "apr2"), digits = 3L)
  ),
  `(4)` = c("", "", "")
)

summary_lines <- paste0(
  do.call(paste, c(as.list(summary_table), sep = " & ")),
  " \\\\"
)

header_rows <- 3L
body_separator_row <- header_rows + length(body_lines) + 1L
bottom_rule_row <- header_rows + length(body_lines) + length(summary_lines) + 1L


# ==============================================================================
# 7. LaTeX Export
# ==============================================================================
cat("Writing LaTeX table...\n")

latex_lines <- c(
  "\\begin{table}[H]",
  "\\centering",
  "\\begin{talltblr}[",
  paste0("caption={", TABLE_CAPTION, "},"),
  paste0("label={", TABLE_LABEL, "},"),
  paste0("note{}={", table_note, "},"),
  "]{",
  "width=0.98\\linewidth,",
  "colsep=3pt,",
  "rowsep=0pt,",
  "cells={font=\\fontsize{11pt}{12pt}\\selectfont, valign=m},",
  "colspec={Q[l,0.18\\linewidth]Q[c,0.205\\linewidth]Q[c,0.205\\linewidth]Q[c,0.205\\linewidth]Q[c,0.205\\linewidth]},",
  "hline{1}={1-5}{solid, black, 0.1em},",
  "hline{2}={2-3}{solid, black, 0.05em},",
  "hline{2}={4-5}{solid, black, 0.05em},",
  paste0("hline{", header_rows + 1L, "}={1-5}{solid, black, 0.05em},"),
  paste0("hline{", body_separator_row, "}={1-5}{solid, black, 0.05em},"),
  paste0("hline{", bottom_rule_row, "}={1-5}{solid, black, 0.1em},"),
  "column{1}={}{halign=l},",
  "column{2-5}={}{halign=c}",
  "}",
  "& \\SetCell[c=2]{c} Panel A: Rainfall model & & \\SetCell[c=2]{c} Panel B: Dry-day model & \\\\",
  paste0("& ", paste(TABLE_COLUMN_LABELS, collapse = " & "), " \\\\"),
  paste0("{Lag} & {", paste(TABLE_HEADER_LABELS, collapse = "} & {"), "} \\\\"),
  body_lines,
  summary_lines,
  "\\end{talltblr}",
  "\\end{table}"
)

writeLines(latex_lines, con = output_file)


# ==============================================================================
# 8. Summary
# ==============================================================================
cat(sprintf("Wrote table to %s\n", output_file))
