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
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "data.table",
  "here"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)


# ==============================================================================
# 3. Setup
# ==============================================================================
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

output_file <- file.path(output_dir, "dry_spill_daily_rain_regimes.tex")

format_latex_int <- function(x) {
  if (length(x) != 1L) {
    return(vapply(x, format_latex_int, character(1L)))
  }

  if (is.na(x)) {
    return("")
  }

  paste0("\\num{", formatC(round(x), format = "f", digits = 0), "}")
}

format_latex_num <- function(x, digits = 2L) {
  if (length(x) != 1L) {
    return(vapply(x, format_latex_num, character(1L), digits = digits))
  }

  if (is.na(x)) {
    return("")
  }

  paste0("\\num{", formatC(x, format = "f", digits = digits), "}")
}

format_latex_pct <- function(x, digits = 1L) {
  if (length(x) != 1L) {
    return(vapply(x, format_latex_pct, character(1L), digits = digits))
  }

  if (is.na(x)) {
    return("")
  }

  paste0("\\num{", formatC(100 * x, format = "f", digits = digits), "}\\%")
}

format_latex_total <- function(x) {
  if (length(x) != 1L) {
    return(vapply(x, format_latex_total, character(1L)))
  }

  if (is.na(x)) {
    return("")
  }

  if (isTRUE(all.equal(x, round(x), tolerance = 1e-9))) {
    return(format_latex_int(round(x)))
  }

  format_latex_num(x, digits = 1L)
}

safe_quantile <- function(x, prob) {
  x <- x[!is.na(x)]

  if (length(x) == 0L) {
    return(NA_real_)
  }

  as.numeric(stats::quantile(x, probs = prob, na.rm = TRUE, names = FALSE))
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
    "rainfall_r9_weak",
    "site_missing"
  )
) |>
  data.table::as.data.table()

dat_daily <- dat_daily[
  site_missing == FALSE &
    !is.na(date) &
    !is.na(spill_count) &
    !is.na(rainfall_r9_weak)
]

dat_daily[, `:=`(
  date = as.Date(date),
  spill_any = as.integer(spill_count == 1L)
)]

dat_daily[, rainfall_category := data.table::fcase(
  rainfall_r9_weak < 0.25, RAINFALL_LABELS[1L],
  rainfall_r9_weak <= 10, RAINFALL_LABELS[2L],
  rainfall_r9_weak <= 20, RAINFALL_LABELS[3L],
  default = RAINFALL_LABELS[4L]
)]

dat_daily[, rainfall_category := factor(
  rainfall_category,
  levels = RAINFALL_LABELS
)]

sample_start_year <- format(min(dat_daily$date, na.rm = TRUE), "%Y")
sample_end_year <- format(max(dat_daily$date, na.rm = TRUE), "%Y")
sample_year_count <- data.table::uniqueN(format(dat_daily$date, "%Y"))
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

category_summary <- dat_daily[
  !is.na(rainfall_category),
  {
    spill_hours_all_days <- data.table::fifelse(spill_any == 1L, spill_hrs, 0)
    spill_hours_on_spill_days <- spill_hrs[spill_any == 1L & !is.na(spill_hrs)]

    list(
      spill_days_annual_total = sum(spill_any, na.rm = TRUE) / sample_year_count,
      spill_days_share = mean(spill_any, na.rm = TRUE),
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

category_summary[, rainfall_category := factor(
  rainfall_category,
  levels = RAINFALL_LABELS
)]
data.table::setorder(category_summary, rainfall_category)

table_rows <- category_summary[, .(
  rainfall = as.character(rainfall_category),
  spill_days_annual_total = format_latex_total(spill_days_annual_total),
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
  ". The sample includes overflow-days with usable daily spill and rainfall ",
  "information. For each overflow-day, rainfall is defined as the highest ",
  "daily rainfall observed across the 3x3 km grid surrounding the overflow. ",
  "Annual total reports the average number of spill days per year in each ",
  "rainfall category. Share reports the fraction of days in that rainfall ",
  "category on which a spill occurs. Columns under Spill hours summarise ",
  "spill duration across all overflow-days in the category, so days without ",
  "a spill enter as zeroes. Columns under Spill hours $\\mid$ spill day ",
  "summarise spill duration only on days when a spill occurs. Dry, moderate, ",
  "heavy, and very heavy rainfall categories correspond to $<0.25$ mm, ",
  "0.25--10 mm, 10--20 mm, and $>20$ mm respectively.}"
)

body_lines <- apply(table_rows, 1L, function(row) {
  paste(
    row[["rainfall"]],
    row[["spill_days_annual_total"]],
    row[["spill_days_share"]],
    row[["spill_hours_uncond_mean"]],
    row[["spill_hours_uncond_p25"]],
    row[["spill_hours_uncond_p50"]],
    row[["spill_hours_uncond_p75"]],
    row[["spill_hours_uncond_p90"]],
    row[["spill_hours_cond_mean"]],
    row[["spill_hours_cond_p25"]],
    row[["spill_hours_cond_p50"]],
    row[["spill_hours_cond_p75"]],
    row[["spill_hours_cond_p90"]],
    sep = " & "
  )
})
body_lines <- paste0(body_lines, " \\\\")


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
  "colspec={Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]},",
  "hline{1}={1-13}{solid, black, 0.1em},",
  "hline{2}={2-3}{solid, black, 0.05em},",
  "hline{2}={4-8}{solid, black, 0.05em},",
  "hline{2}={9-13}{solid, black, 0.05em},",
  "hline{2}={3}{solid, black, 0.05em, r=-0.5},",
  "hline{2}={4}{solid, black, 0.05em, l=-0.5},",
  "hline{2}={8}{solid, black, 0.05em, r=-0.5},",
  "hline{2}={9}{solid, black, 0.05em, l=-0.5},",
  "hline{3}={1-13}{solid, black, 0.05em},",
  "hline{7}={1-13}{solid, black, 0.1em},",
  "column{1}={}{halign=l},",
  "column{2-13}={}{halign=c}",
  "}",
  "& \\SetCell[c=2]{c} Spill days & & \\SetCell[c=5]{c} Spill hours & & & & & \\SetCell[c=5]{c} Spill hours $\\mid$ spill day & & & & \\\\",
  "{Rainfall \\\\ Category} & {Annual \\\\ total} & Share & Mean & P25 & P50 & P75 & P90 & Mean & P25 & P50 & P75 & P90 \\\\",
  body_lines,
  "\\end{talltblr}",
  "\\end{table}"
)

writeLines(latex_lines, con = output_file)


# ==============================================================================
# 7. Summary
# ==============================================================================
cat(sprintf("Wrote table to %s\n", output_file))
