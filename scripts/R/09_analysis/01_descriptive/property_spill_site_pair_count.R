# ==============================================================================
# 
# ==============================================================================
#
# Purpose: Generate a descriptive statistics table on the number of properties,
#          spill sites, and share of spill sites per property.  
#
# Author: Alina Zeltikova
# Date: 2026-02-23
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/house_site - Cross-sectional sales (house - spill site level)
#
# Outputs:
#   - output/tables/property_spill_site_pair_count.tex
#
# ==============================================================================

# ==============================================================================
# 1. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "tidyverse",
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
# 2. Setup
# ==============================================================================

# Output Directory -------------------------------------------------------------

output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# ==============================================================================
# 3. Data Loading and Preparation
# ==============================================================================

# 3.1 Panel A: Sales -----------------------------------------------------------

# Load Sales Data --------------------------------------------------------------
cat("Loading sales - spill sites data...\n")

# Cross-section data with spill metrics (prior to sale)
## Filter for houses with at least one spill site within radius
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale_house_site")
) |>
  collect()

# 3.2 Panel B: Rentals ---------------------------------------------------------

# Load Rental Data -------------------------------------------------------------
cat("Loading rentals - spill sites data...\n")

# Cross-section data with spill metrics (prior to rental)
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental_rental_site")
) |>
  collect()

# ==============================================================================
# 4. Create Table
# ==============================================================================
cat("Creating table...\n")

compute_summary <- function(dat, id_col) {
  id_col <- rlang::sym(id_col)
  
  counts <- dat |>
    summarise(
      n_properties  = n_distinct(!!id_col),
      n_sites       = n_distinct(site_id),
      mean_distance = mean(distance_m),
      .by = radius
    )
  
  site_per_prop <- dat |>
    summarise(n_sites = n_distinct(site_id), .by = c(radius, !!id_col)) |>
    summarise(
      mean_sites     = mean(n_sites),
      median_sites   = median(n_sites),
      share_one      = mean(n_sites == 1),
      share_two      = mean(n_sites == 2),
      share_three    = mean(n_sites == 3),
      share_gt_three = mean(n_sites >  3),
      .by = radius
    )
  
  counts |>
    left_join(site_per_prop, by = "radius") |>
    arrange(radius) |>
    mutate(across(where(is.numeric), ~ round(.x, 3)))
}

summary_sales    <- compute_summary(dat_cs_sales,    id_col = "house_id")
summary_rentals  <- compute_summary(dat_cs_rentals,  id_col = "rental_id")

# ==============================================================================
# 5. Export Property - Spill Site Pairs Descriptive Statistics 
# ==============================================================================
fmt_f <- function(x, d = 3) formatC(as.numeric(x), format = "f", digits = d)
fmt_i <- function(x)        formatC(as.numeric(x), format = "d", big.mark = ",")

make_rows <- function(df) {
  apply(df, 1, function(r) {
    paste0(
      "\\quad ", r["radius"],       " & ",
      fmt_f(r["mean_distance"]),    " & ",
      fmt_i(r["n_properties"]),     " & ",
      fmt_i(r["n_sites"]),          " & ",
      fmt_f(r["mean_sites"]),       " & ",
      fmt_f(r["median_sites"]),     " & ",
      fmt_f(r["share_one"]),        " & ",
      fmt_f(r["share_two"]),        " & ",
      fmt_f(r["share_three"]),      " & ",
      fmt_f(r["share_gt_three"]),
      " \\\\"
    )
  })
}

rows_sales   <- make_rows(summary_sales)
rows_rentals <- make_rows(summary_rentals)

property_spill_site_pair_count <- c(
  "\\begin{table}[H]",
  "\\centering",
  "\\begin{talltblr}[",
  "caption={Property - Spill Site Pairs (2021--2023)},",
  "label={tab:property_site_counts},",
  "]{",
  "colsep=3pt,",
  "rowsep=0.5pt,",
  "cells={font=\\fontsize{11pt}{12pt}\\selectfont},",
  "colspec={Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]},",
  "hline{1}={1-10}{solid, black, 0.1em},",
  "hline{3}={3-4}{solid, black, 0.05em},",
  "hline{3}={5-6}{solid, black, 0.05em},",
  "hline{3}={7-10}{solid, black, 0.05em},",
  "hline{3}={4}{solid, black, 0.05em, r=-0.5},",
  "hline{3}={5}{solid, black, 0.05em, l=-0.5},",
  "hline{3}={6}{solid, black, 0.05em, r=-0.5},",
  "hline{3}={7}{solid, black, 0.05em, l=-0.5},",
  "hline{4}={1-10}{solid, black, 0.05em},",
  "hline{5}={1-10}{solid, black, 0.05em},",
  "hline{8}={1-10}{solid, black, 0.05em},",
  "hline{9}={1-10}{solid, black, 0.05em},",
  "hline{12}={1-10}{solid, black, 0.1em},",
  "column{1-2}={}{halign=l},",
  "column{3-10}={}{halign=c}",
  "}",
  "& & \\SetCell[c=2, r=2]{c} Observations & & \\SetCell[c=2]{c} Spill Site Count & & \\SetCell[c=4]{c} Share of Properties & & & \\\\",
  "& Mean & & & \\SetCell[c=2]{c} per Property & & \\SetCell[c=4]{c} (\\# of Spill Sites) & & & \\\\",
  "Threshold (m) & Distance (m) & \\# Properties & \\# Spill Sites & Mean & Median & One & Two & Three & {$>$Three} \\\\",
  "\\textbf{Panel A: Sales} & & & & & & & & & \\\\",
  rows_sales,
  "\\textbf{Panel B: Rentals} & & & & & & & & & \\\\",
  rows_rentals,
  "\\end{talltblr}",
  "\\end{table}"
)

output_path_pairs_descriptives <- file.path(output_dir, "property_spill_site_pair_count.tex")
writeLines(property_spill_site_pair_count, output_path_pairs_descriptives)
