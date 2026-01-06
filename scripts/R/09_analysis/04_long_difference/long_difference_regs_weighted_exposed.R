# ==============================================================================
# Long Difference Regression Analysis (Weighted, Exposed Grid Cells Only)
# ==============================================================================
#
# Purpose: Estimate the relationship between changes in property values and
#          changes in sewage spill exposure between 2021 and 2023 using
#          250m x 250m grid-level aggregated data.
#
# Methodology: Long difference approach where each grid cell's 2023 value is
#              differenced from its 2021 value, removing time-invariant
#              unobserved characteristics of locations.
#
#              This version:
#              - Weights regressions by total transactions in each grid cell
#                to reduce noise from cells with few observations.
#              - Restricts the sample to grid cells with at least one nearby
#                spill site in at least one year (2021 or 2023).
#
# Author: Jacopo Olivieri
# Date: 2026-01-06
#
# Inputs:
#   - data/processed/long_difference/long_diff_grid_house_sales.parquet
#   - data/processed/long_difference/long_diff_grid_rentals.parquet
#
# Outputs:
#   - output/tables/long_difference_weighted_exposed.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
YEAR_START <- 2021L
YEAR_END <- 2023L


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "here",
  "modelsummary",
  "fixest"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)


# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}


# ==============================================================================
# 3. Load Data
# ==============================================================================
cat("Loading grid-level data...\n")

# Sales grid data
dat_sales_grid <- rio::import(
  here::here("data", "processed", "long_difference", "long_diff_grid_house_sales.parquet"),
  trust = TRUE
)
cat("  Sales grid-cell-year rows:", nrow(dat_sales_grid), "\n")

# Rentals grid data
dat_rental_grid <- rio::import(
  here::here("data", "processed", "long_difference", "long_diff_grid_rentals.parquet"),
  trust = TRUE
)
cat("  Rental grid-cell-year rows:", nrow(dat_rental_grid), "\n")


# ==============================================================================
# 4. Compute Long Differences: Sales
# ==============================================================================
cat("\nComputing long differences for sales...\n")

# Filter to start and end years
dat_sales_start <- dat_sales_grid |>
  dplyr::filter(year == YEAR_START) |>
  dplyr::select(
    grid_cell_id,
    mean_log_price_start = mean_log_price,
    mean_spill_count_start = mean_spill_count,
    mean_spill_hrs_start = mean_spill_hrs,
    n_transactions_start = n_transactions
  )

dat_sales_end <- dat_sales_grid |>
  dplyr::filter(year == YEAR_END) |>
  dplyr::select(
    grid_cell_id,
    mean_log_price_end = mean_log_price,
    mean_spill_count_end = mean_spill_count,
    mean_spill_hrs_end = mean_spill_hrs,
    n_transactions_end = n_transactions
  )

# Join and compute differences
dat_sales_diff <- dat_sales_start |>
  dplyr::inner_join(dat_sales_end, by = "grid_cell_id") |>
  dplyr::mutate(
    delta_log_price = mean_log_price_end - mean_log_price_start,
    delta_spill_count = mean_spill_count_end - mean_spill_count_start,
    delta_spill_hrs = mean_spill_hrs_end - mean_spill_hrs_start,
    total_transactions = n_transactions_start + n_transactions_end
  ) |>
  # Keep only cells with valid price observations in both years
  dplyr::filter(!is.na(delta_log_price)) |>
  # Restrict to grid cells with at least one nearby spill site in at least one year
  dplyr::filter(mean_spill_count_start > 0 | mean_spill_count_end > 0)

cat("  Grid cells with valid prices in both years (exposed only):", nrow(dat_sales_diff), "\n")
cat("  Grid cells with valid spill exposure:", sum(!is.na(dat_sales_diff$delta_spill_count)), "\n")
cat("  Total transactions (sum of weights):", sum(dat_sales_diff$total_transactions), "\n")


# ==============================================================================
# 5. Compute Long Differences: Rentals
# ==============================================================================
cat("\nComputing long differences for rentals...\n")

# Filter to start and end years
dat_rental_start <- dat_rental_grid |>
  dplyr::filter(year == YEAR_START) |>
  dplyr::select(
    grid_cell_id,
    mean_log_price_start = mean_log_price,
    mean_spill_count_start = mean_spill_count,
    mean_spill_hrs_start = mean_spill_hrs,
    n_transactions_start = n_transactions
  )

dat_rental_end <- dat_rental_grid |>
  dplyr::filter(year == YEAR_END) |>
  dplyr::select(
    grid_cell_id,
    mean_log_price_end = mean_log_price,
    mean_spill_count_end = mean_spill_count,
    mean_spill_hrs_end = mean_spill_hrs,
    n_transactions_end = n_transactions
  )

# Join and compute differences
dat_rental_diff <- dat_rental_start |>
  dplyr::inner_join(dat_rental_end, by = "grid_cell_id") |>
  dplyr::mutate(
    delta_log_price = mean_log_price_end - mean_log_price_start,
    delta_spill_count = mean_spill_count_end - mean_spill_count_start,
    delta_spill_hrs = mean_spill_hrs_end - mean_spill_hrs_start,
    total_transactions = n_transactions_start + n_transactions_end
  ) |>
  # Keep only cells with valid price observations in both years
  dplyr::filter(!is.na(delta_log_price)) |>
  # Restrict to grid cells with at least one nearby spill site in at least one year
  dplyr::filter(mean_spill_count_start > 0 | mean_spill_count_end > 0)

cat("  Grid cells with valid prices in both years (exposed only):", nrow(dat_rental_diff), "\n")
cat("  Grid cells with valid spill exposure:", sum(!is.na(dat_rental_diff$delta_spill_count)), "\n")
cat("  Total transactions (sum of weights):", sum(dat_rental_diff$total_transactions), "\n")


# ==============================================================================
# 6. Summary Statistics
# ==============================================================================
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("Summary Statistics: Sales (Weighted, Exposed Cells Only)\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

# Sales summary (filter to valid spill observations for spill stats)
dat_sales_valid <- dat_sales_diff |> dplyr::filter(!is.na(delta_spill_count))

cat("\nPrice changes (all exposed cells):\n")
cat("  N:", nrow(dat_sales_diff), "\n")
cat("  Mean delta_log_price:", round(mean(dat_sales_diff$delta_log_price, na.rm = TRUE), 4), "\n")
cat("  Weighted mean delta_log_price:",
    round(weighted.mean(dat_sales_diff$delta_log_price, dat_sales_diff$total_transactions, na.rm = TRUE), 4), "\n")
cat("  SD delta_log_price:", round(sd(dat_sales_diff$delta_log_price, na.rm = TRUE), 4), "\n")

cat("\nSpill changes (exposed cells only):\n")
cat("  N:", nrow(dat_sales_valid), "\n")
cat("  Mean delta_spill_count:", round(mean(dat_sales_valid$delta_spill_count, na.rm = TRUE), 2), "\n")
cat("  Weighted mean delta_spill_count:",
    round(weighted.mean(dat_sales_valid$delta_spill_count, dat_sales_valid$total_transactions, na.rm = TRUE), 2), "\n")
cat("  SD delta_spill_count:", round(sd(dat_sales_valid$delta_spill_count, na.rm = TRUE), 2), "\n")

# Weight distribution
cat("\nWeight distribution (total_transactions):\n")
cat("  Min:", min(dat_sales_diff$total_transactions), "\n")
cat("  Median:", median(dat_sales_diff$total_transactions), "\n")
cat("  Mean:", round(mean(dat_sales_diff$total_transactions), 1), "\n")
cat("  Max:", max(dat_sales_diff$total_transactions), "\n")

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("Summary Statistics: Rentals (Weighted, Exposed Cells Only)\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

# Rentals summary
dat_rental_valid <- dat_rental_diff |> dplyr::filter(!is.na(delta_spill_count))

cat("\nPrice changes (all exposed cells):\n")
cat("  N:", nrow(dat_rental_diff), "\n")
cat("  Mean delta_log_price:", round(mean(dat_rental_diff$delta_log_price, na.rm = TRUE), 4), "\n")
cat("  Weighted mean delta_log_price:",
    round(weighted.mean(dat_rental_diff$delta_log_price, dat_rental_diff$total_transactions, na.rm = TRUE), 4), "\n")
cat("  SD delta_log_price:", round(sd(dat_rental_diff$delta_log_price, na.rm = TRUE), 4), "\n")

cat("\nSpill changes (exposed cells only):\n")
cat("  N:", nrow(dat_rental_valid), "\n")
cat("  Mean delta_spill_count:", round(mean(dat_rental_valid$delta_spill_count, na.rm = TRUE), 2), "\n")
cat("  Weighted mean delta_spill_count:",
    round(weighted.mean(dat_rental_valid$delta_spill_count, dat_rental_valid$total_transactions, na.rm = TRUE), 2), "\n")
cat("  SD delta_spill_count:", round(sd(dat_rental_valid$delta_spill_count, na.rm = TRUE), 2), "\n")

# Weight distribution
cat("\nWeight distribution (total_transactions):\n")
cat("  Min:", min(dat_rental_diff$total_transactions), "\n")
cat("  Median:", median(dat_rental_diff$total_transactions), "\n")
cat("  Mean:", round(mean(dat_rental_diff$total_transactions), 1), "\n")
cat("  Max:", max(dat_rental_diff$total_transactions), "\n")


# ==============================================================================
# 7. Estimate Models (Weighted)
# ==============================================================================
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("Estimating weighted long difference models (exposed cells only)...\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

# Sales Models (Weighted)
model_sales_count <- fixest::feols(
  delta_log_price ~ delta_spill_count,
  data = dat_sales_diff,
  weights = ~total_transactions,
  vcov = "hetero"
)

model_sales_hrs <- fixest::feols(
  delta_log_price ~ delta_spill_hrs,
  data = dat_sales_diff,
  weights = ~total_transactions,
  vcov = "hetero"
)

# Rental Models (Weighted)
model_rental_count <- fixest::feols(
  delta_log_price ~ delta_spill_count,
  data = dat_rental_diff,
  weights = ~total_transactions,
  vcov = "hetero"
)

model_rental_hrs <- fixest::feols(
  delta_log_price ~ delta_spill_hrs,
  data = dat_rental_diff,
  weights = ~total_transactions,
  vcov = "hetero"
)

cat("  Models estimated successfully.\n")


# ==============================================================================
# 8. Export LaTeX Table
# ==============================================================================
cat("\nExporting LaTeX table...\n")

# Coefficient labels
coef_labels <- c(
  "(Intercept)" = "Constant",
  "delta_spill_count" = "$\\Delta$ Spill count",
  "delta_spill_hrs" = "$\\Delta$ Spill hours"
)

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Combined models for joint table
panels <- list(
  "House Sales" = list(
    "(1)" = model_sales_count,
    "(2)" = model_sales_hrs
  ),
  "House Rentals" = list(
    "(3)" = model_rental_count,
    "(4)" = model_rental_hrs
  )
)

# Custom notes
custom_notes <- paste0(
  "\\\\begin{tablenotes}[flushleft]\n",
  "       \\\\item \\\\hspace{-0.25cm} \\\\protect\\\\footnotesize{\\\\textbf{Notes:} ",
  "Weighted long difference regression (2023 $-$ 2021). Unit of observation is a 250m $\\\\times$ 250m grid cell. ",
  "Sample restricted to grid cells within 250m of at least one sewage outlet in either 2021 or 2023. ",
  "Regressions weighted by total number of transactions in each grid cell (2021 + 2023). ",
  "Dependent variable is the change in mean log price within the grid cell. ",
  "$\\\\Delta$ Spill count is the change in mean annual spill count across properties within 250m of a sewage outlet. ",
  "$\\\\Delta$ Spill hours is the corresponding change in total spill duration. ",
  "Heteroskedasticity-robust standard errors in parentheses. ",
  "\\\\sym{***} \\\\(p<0.01\\\\), \\\\sym{**} \\\\(p<0.05\\\\), \\\\sym{*} \\\\(p<0.1\\\\).}\n",
  "    \\\\end{tablenotes}"
)

# Export table
table_latex <- modelsummary::modelsummary(
  panels,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 3,
  coef_map = coef_labels,
  gof_map = gof_map,
  notes = " ",
  title = "Long Difference: Effect of Changes in Sewage Spills on Property Values (Weighted, Exposed Cells)"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:long-difference-weighted-exposed},",
  table_latex
)

# Replace tablenotes with custom one-liner
table_latex <- sub(
  "(?s)\\\\begin\\{tablenotes\\}.*?\\\\end\\{tablenotes\\}",
  custom_notes,
  table_latex,
  perl = TRUE
)

# Write to file
output_path <- file.path(output_dir, "long_difference_weighted_exposed.tex")
writeLines(table_latex, output_path)


# ==============================================================================
# 9. Summary
# ==============================================================================
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("Script completed successfully.\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("\nLaTeX table exported to:", output_path, "\n")
