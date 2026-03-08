# ==============================================================================
# Spill Days and Spill Hours by Rainfall Category
# ==============================================================================
#
# Purpose: Summarise daily spill incidence and spill duration across rainfall
#          categories using the site-day panel underlying the dry-spill
#          analysis. Spill-day totals are annualised, spill-day shares are
#          computed within rainfall category, and spill-hour moments are shown
#          both unconditionally and conditional on spill days.
#
# Author: Jacopo Olivieri
# Date: 2026-03-07
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_daily.parquet
#
# Outputs:
#   - output/tables/dry_spill_daily_rain_regimes.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAINFALL_LABELS <- c(
  "{Dry \\\\ ($<0.25$ mm)}",
  "{Moderate \\\\ (0.25--10 mm)}",
  "{Heavy \\\\ (10--20 mm)}",
  "{Very heavy \\\\ ($>20$ mm)}"
)

TABLE_CAPTION <- "Daily Spill Statistics by Rainfall Category"
TABLE_LABEL <- "tbl:dry-spill-daily-rain-regimes"


# ==============================================================================
# 2. Package Management
# ==============================================================================
required_packages <- c(
  "arrow",
  "data.table",
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

output_file <- file.path(output_dir, "dry_spill_daily_rain_regimes.tex")

format_latex_num <- function(x, digits = 2L, scale = 1, suffix = "") {
  out <- rep.int("", length(x))
  ok <- !is.na(x)

  if (any(ok)) {
    out[ok] <- paste0(
      "\\num{",
      formatC(scale * x[ok], format = "f", digits = digits),
      "}",
      suffix
    )
  }

  out
}

format_latex_total <- function(x) {
  out <- rep.int("", length(x))
  ok <- !is.na(x)

  if (!any(ok)) {
    return(out)
  }

  x_ok <- x[ok]
  x_round <- round(x_ok)
  is_int <- abs(x_ok - x_round) < 1e-9

  out[ok] <- ifelse(
    is_int,
    format_latex_num(x_round, digits = 0L),
    format_latex_num(x_ok, digits = 1L)
  )

  out
}

format_latex_pct <- function(x, digits = 1L) {
  format_latex_num(x, digits = digits, scale = 100, suffix = "\\%")
}

safe_quantile <- function(x, probs) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  as.numeric(stats::quantile(x, probs = probs, na.rm = TRUE, names = FALSE))
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
    "spill_hrs",
    "rainfall_max_9cell_d01_na_rm",
    "site_missing"
  )
) |>
  data.table::as.data.table()

dat_daily <- dat_daily[
  site_missing == FALSE &
    !is.na(date) &
    !is.na(spill_count) &
    !is.na(rainfall_max_9cell_d01_na_rm)
]

dat_daily[, `:=`(
  date = as.Date(date),
  year = data.table::year(date)
)]

dat_daily[, rainfall_category := data.table::fcase(
  rainfall_max_9cell_d01_na_rm < 0.25, RAINFALL_LABELS[1L],
  rainfall_max_9cell_d01_na_rm <= 10, RAINFALL_LABELS[2L],
  rainfall_max_9cell_d01_na_rm <= 20, RAINFALL_LABELS[3L],
  default = RAINFALL_LABELS[4L]
)]

dat_daily[, rainfall_category := factor(
  rainfall_category,
  levels = RAINFALL_LABELS
)]

sample_start_year <- format(min(dat_daily$date, na.rm = TRUE), "%Y")
sample_end_year <- format(max(dat_daily$date, na.rm = TRUE), "%Y")
sample_period <- if (identical(sample_start_year, sample_end_year)) {
  sample_start_year
} else {
  paste0(sample_start_year, "--", sample_end_year)
}

cat(sprintf("  Retained %s site-days\n", format(nrow(dat_daily), big.mark = ",")))
cat(sprintf("  Sample period: %s\n", sample_period))


# ==============================================================================
# 5. Table Preparation
# ==============================================================================
cat("Computing rainfall-category summary...\n")

n_site_years <- data.table::uniqueN(dat_daily[, .(site_id, year)])

category_days_summary <- dat_daily[
  ,
  .(
    days_per_overflow_year = .N / n_site_years,
    spill_days_per_overflow_year = sum(spill_count, na.rm = TRUE) / n_site_years,
    spill_days_share = sum(spill_count, na.rm = TRUE) / .N
  ),
  by = rainfall_category
]

category_hours_summary <- dat_daily[
  ,
  {
    spill_hours_all_days <- data.table::fifelse(spill_count == 1L, spill_hrs, 0)
    spill_hours_on_spill_days <- spill_hrs[spill_count == 1L & !is.na(spill_hrs)]

    list(
      spill_hours_uncond_mean = mean(spill_hours_all_days, na.rm = TRUE),
      spill_hours_uncond_p25 = safe_quantile(spill_hours_all_days, 0.25),
      spill_hours_uncond_p50 = safe_quantile(spill_hours_all_days, 0.50),
      spill_hours_uncond_p75 = safe_quantile(spill_hours_all_days, 0.75),
      spill_hours_uncond_p90 = safe_quantile(spill_hours_all_days, 0.90),
      spill_hours_cond_mean = if (length(spill_hours_on_spill_days) > 0L) {
        mean(spill_hours_on_spill_days)
      } else {
        NA_real_
      },
      spill_hours_cond_p25 = safe_quantile(spill_hours_on_spill_days, 0.25),
      spill_hours_cond_p50 = safe_quantile(spill_hours_on_spill_days, 0.50),
      spill_hours_cond_p75 = safe_quantile(spill_hours_on_spill_days, 0.75),
      spill_hours_cond_p90 = safe_quantile(spill_hours_on_spill_days, 0.90)
    )
  },
  by = rainfall_category
]

category_summary <- data.table::data.table(
  rainfall_category = factor(RAINFALL_LABELS, levels = RAINFALL_LABELS)
)

category_summary <- merge(
  category_summary,
  category_days_summary,
  by = "rainfall_category",
  all.x = TRUE,
  sort = FALSE
)

category_summary <- merge(
  category_summary,
  category_hours_summary,
  by = "rainfall_category",
  all.x = TRUE,
  sort = FALSE
)

data.table::setorder(category_summary, rainfall_category)

table_rows <- category_summary[, .(
  rainfall = as.character(rainfall_category),
  days_per_overflow_year = format_latex_total(days_per_overflow_year),
  spill_days_per_overflow_year = format_latex_total(spill_days_per_overflow_year),
  spill_days_share = format_latex_pct(spill_days_share, digits = 1L),
  spill_hours_uncond_mean = format_latex_num(spill_hours_uncond_mean, digits = 1L),
  spill_hours_uncond_p25 = format_latex_num(spill_hours_uncond_p25, digits = 1L),
  spill_hours_uncond_p50 = format_latex_num(spill_hours_uncond_p50, digits = 1L),
  spill_hours_uncond_p75 = format_latex_num(spill_hours_uncond_p75, digits = 1L),
  spill_hours_uncond_p90 = format_latex_num(spill_hours_uncond_p90, digits = 1L),
  spill_hours_cond_mean = format_latex_num(spill_hours_cond_mean, digits = 1L),
  spill_hours_cond_p25 = format_latex_num(spill_hours_cond_p25, digits = 1L),
  spill_hours_cond_p50 = format_latex_num(spill_hours_cond_p50, digits = 1L),
  spill_hours_cond_p75 = format_latex_num(spill_hours_cond_p75, digits = 1L),
  spill_hours_cond_p90 = format_latex_num(spill_hours_cond_p90, digits = 1L)
)]

table_note <- paste0(
  "\\footnotesize{\\textbf{Notes:} This table presents daily spill statistics ",
  "by rainfall category for storm overflow sites in England, ",
  sample_period,
  ". For each overflow on each calendar day, rainfall is measured as the ",
  "maximum daily rainfall recorded within the surrounding 3~$\\times$~3\\,km ",
  "grid on that day or the previous day. Total days reports the average ",
  "number of days per year in each rainfall category; spill days the average ",
  "number of spill days,and Share the proportion of those days with a spill.",
  "Spill hours reports daily spill duration across all days in the category, ",
  "including zeroes on days without a spill. Spill hours $\\mid$ spill day ",
  "reports daily spill duration conditional on a spill occurring.}"
)

body_lines <- paste0(
  do.call(paste, c(as.list(table_rows), sep = " & ")),
  " \\\\"
)


# ==============================================================================
# 6. LaTeX Export
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
  "colsep=4pt,",
  "rowsep=0pt,",
  "cells={font=\\fontsize{11pt}{12pt}\\selectfont, valign=m},",
  "colspec={Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]},",
  "hline{1}={1-14}{solid, black, 0.1em},",
  "hline{2}={2-4}{solid, black, 0.05em},",
  "hline{2}={5-9}{solid, black, 0.05em},",
  "hline{2}={10-14}{solid, black, 0.05em},",
  "hline{2}={4}{solid, black, 0.05em, r=-0.5},",
  "hline{2}={5}{solid, black, 0.05em, l=-0.5},",
  "hline{2}={9}{solid, black, 0.05em, r=-0.5},",
  "hline{2}={10}{solid, black, 0.05em, l=-0.5},",
  "hline{3}={1-14}{solid, black, 0.05em},",
  "hline{7}={1-14}{solid, black, 0.1em},",
  "column{1}={}{halign=l},",
  "column{2-14}={}{halign=c}",
  "}",
  "& \\SetCell[c=3]{c} Days / overflow & & & \\SetCell[c=5]{c} Spill hours & & & & & \\SetCell[c=5]{c} Spill hours $\\mid$ spill day & & & & \\\\",
  "{Rainfall \\\\ Category} & {Total \\\\ Days} & {Spill \\\\ Days} & Share & Mean & P25 & P50 & P75 & P90 & Mean & P25 & P50 & P75 & P90 \\\\",
  body_lines,
  "\\end{talltblr}",
  "\\end{table}"
)

writeLines(latex_lines, con = output_file)


# ==============================================================================
# 7. Summary
# ==============================================================================
cat(sprintf("Wrote table to %s\n", output_file))
