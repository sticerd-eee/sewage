# ==============================================================================
# Cross-Sectional Regression Analysis: Spill Count and Hours
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          cumulative spill measures (count and hours).
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, MSOA FE, MSOA FE +
#          Controls, LSOA FE, and LSOA FE + Controls.
#
# Author: Jacopo Olivieri
# Date: 2024-10-15
# Date Modified: 2026-08-17
#
# Inputs:
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/cross_section/sales/study_period/ - Study-period exposure
#   - data/processed/cross_section/rentals/study_period/ - Study-period exposure
#
# Outputs:
#   - output/tables/hedonic_count_bins_full.tex
#   - output/tables/hedonic_hrs_bins_full.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L


# ==============================================================================
# 2. Package Management
# ==============================================================================

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "purrr",
  "here",
  "janitor",
  "modelsummary",
  "sandwich",
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

# Shared table formatting helpers
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))


# ==============================================================================
# 3. Setup
# ==============================================================================

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Helper Function --------------------------------------------------------------
# Helper to bucket spill metrics into "0 spills" plus four quartiles
bin_spill_measure <- function(x) {
  x_for_ntile <- dplyr::if_else(x == 0, NA_real_, x, missing = NA_real_)
  quartile <- dplyr::ntile(x_for_ntile, 4)
  bins <- dplyr::case_when(
    is.na(x) ~ NA_character_,
    x == 0 ~ "0 spills",
    TRUE ~ paste0("Q", quartile)
  )
  factor(bins, levels = c("0 spills", paste0("Q", 1:4)))
}

# ==============================================================================
# Panel A: Sales
# ==============================================================================

# Load Sales Data --------------------------------------------------------------
cat("Loading sales data...\n")

path_sales <- here::here("data", "processed", "house_price.parquet")

path_cross_section_sales <- here::here(
  "data",
  "processed",
  "cross_section",
  "sales",
  "study_period"
)

sales <- import(path_sales, trust = TRUE) |>
  select(
    -date_of_transfer,
    -quality,
    -paon,
    -saon,
    -street,
    -locality,
    -town_city,
    -district,
    -county,
    -ppd_category,
    -record_status
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type),
    old_new = forcats::as_factor(old_new),
    duration = forcats::as_factor(duration)
  )

# Prepare Sales Data -----------------------------------------------------------
cat("Preparing sales data...\n")

# Study-period exposure: full-window spill totals per transaction. Exposure is
# NA when any overflow within the radius has unreported annual data for part of
# the window (has_missing_site), so binned samples are complete-window sums.
spill_sales_collapsed <- arrow::open_dataset(path_cross_section_sales) |>
  filter(radius == RAD) |>
  select(house_id, spill_count, spill_hrs, n_spill_sites, spatially_eligible) |>
  collect() |>
  filter(spatially_eligible, n_spill_sites > 0L) |>
  select(house_id, spill_count, spill_hrs)

dat_sales_clean <- sales |>
  left_join(spill_sales_collapsed, by = join_by(house_id)) |>
  mutate(
    spill_count_bin = bin_spill_measure(spill_count),
    spill_hrs_bin = bin_spill_measure(spill_hrs),
    log_price = log(price)
  ) |>
  filter(
    !is.na(spill_count_bin),
    !is.na(spill_hrs_bin),
    !is.na(lsoa),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration)
  ) |>
  mutate(
    spill_count_bin = forcats::fct_relevel(spill_count_bin, "0 spills"),
    spill_count_bin = forcats::fct_drop(spill_count_bin),
    spill_hrs_bin = forcats::fct_relevel(spill_hrs_bin, "0 spills"),
    spill_hrs_bin = forcats::fct_drop(spill_hrs_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

cat("  Sales observations:", nrow(dat_sales_clean), "\n")


# ==============================================================================
# Panel B: Rentals
# ==============================================================================

# Load Rental Data -------------------------------------------------------------
cat("Loading rental data...\n")

path_rent <- here::here("data", "processed", "zoopla", "zoopla_rentals.parquet")

path_cross_section_rental <- here::here(
  "data",
  "processed",
  "cross_section",
  "rentals",
  "study_period"
)

rentals <- import(path_rent, trust = TRUE) |>
  select(
    -postcode,
    -listing_created,
    -latest_to_rent,
    -rented,
    -rented_est,
    -address_line_01,
    -address_line_02,
    -address_line_03
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type)
  )

# Prepare Rental Data ----------------------------------------------------------
cat("Preparing rental data...\n")

# Study-period exposure for rentals (2021--2023 window; see
# scripts/R/06_analysis_datasets/cross_section_rental.R).
spill_rental_collapsed <- arrow::open_dataset(path_cross_section_rental) |>
  filter(radius == RAD) |>
  select(
    rental_id, spill_count, spill_hrs, n_spill_sites, spatially_eligible
  ) |>
  collect() |>
  filter(spatially_eligible, n_spill_sites > 0L) |>
  select(rental_id, spill_count, spill_hrs)

dat_rental_clean <- rentals |>
  left_join(spill_rental_collapsed, by = join_by(rental_id)) |>
  mutate(
    spill_count_bin = bin_spill_measure(spill_count),
    spill_hrs_bin = bin_spill_measure(spill_hrs),
    log_price = log(listing_price)
  ) |>
  filter(
    !is.na(spill_count_bin),
    !is.na(spill_hrs_bin),
    !is.na(lsoa),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms)
  ) |>
  mutate(
    spill_count_bin = forcats::fct_relevel(spill_count_bin, "0 spills"),
    spill_count_bin = forcats::fct_drop(spill_count_bin),
    spill_hrs_bin = forcats::fct_relevel(spill_hrs_bin, "0 spills"),
    spill_hrs_bin = forcats::fct_drop(spill_hrs_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat("  Rental observations:", nrow(dat_rental_clean), "\n")


# ==============================================================================
# Estimate Models: Spill Count
# ==============================================================================
cat("Estimating spill count models...\n")

# Sales Models
model_sales_count_1 <- fixest::feols(
  log_price ~ spill_count_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_1b <- fixest::feols(
  log_price ~ spill_count_bin + property_type + old_new + duration,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_2 <- fixest::feols(
  log_price ~ spill_count_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_3 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_4 <- fixest::feols(
  log_price ~ spill_count_bin | msoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_5 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + old_new + duration | msoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_count_1 <- fixest::feols(
  log_price ~ spill_count_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_1b <- fixest::feols(
  log_price ~ spill_count_bin + property_type + bedrooms + bathrooms,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_2 <- fixest::feols(
  log_price ~ spill_count_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_3 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_4 <- fixest::feols(
  log_price ~ spill_count_bin | msoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_5 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + bedrooms + bathrooms | msoa,
  data = dat_rental_clean,
  vcov = "hetero"
)


# ==============================================================================
# Estimate Models: Spill Hours
# ==============================================================================
cat("Estimating spill hours models...\n")

# Sales Models
model_sales_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + old_new + duration,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_4 <- fixest::feols(
  log_price ~ spill_hrs_bin | msoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_5 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + old_new + duration | msoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + bedrooms + bathrooms,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_4 <- fixest::feols(
  log_price ~ spill_hrs_bin | msoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_5 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + bedrooms + bathrooms | msoa,
  data = dat_rental_clean,
  vcov = "hetero"
)


# ==============================================================================
# Export Tables: Spill Count
# ==============================================================================
cat("Exporting spill count table...\n")

# Coefficient labels
coef_labels_count <- c(
  "(Intercept)" = "Constant",
  "spill_count_binQ1" = "Spill count Q1",
  "spill_count_binQ2" = "Spill count Q2",
  "spill_count_binQ3" = "Spill count Q3",
  "spill_count_binQ4" = "Spill count Q4"
)

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Combined models for joint table
panels_count <- list(
  "House Sales" = list(
    "(1)" = model_sales_count_1,
    "(2)" = model_sales_count_1b,
    "(3)" = model_sales_count_4,
    "(4)" = model_sales_count_5,
    "(5)" = model_sales_count_2,
    "(6)" = model_sales_count_3
  ),
  "House Rentals" = list(
    "(7)" = model_rental_count_1,
    "(8)" = model_rental_count_1b,
    "(9)" = model_rental_count_4,
    "(10)" = model_rental_count_5,
    "(11)" = model_rental_count_2,
    "(12)" = model_rental_count_3
  )
)

# Add rows for fixed effects and controls
add_rows <- tibble::tribble(
  ~term                , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` , ~`(9)` , ~`(10)`, ~`(11)`, ~`(12)`,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" ,
  "Time FE"            , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"
)
attr(add_rows, "position") <- "coef_end"

# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the total number of spill events (12/24 count) recorded across all overflows within 250m over the entire study window (2021--2024 for sales, 2021--2023 for rentals), classified into quartiles (Q1--Q4) based on the distribution of strictly positive exposure; the reference category is properties near overflows with zero recorded spills. Properties are excluded where any overflow within 250m lacks reported annual spill data for part of the window (including overflows that stopped reporting and subsequently left the register), so exposure is always a complete-window total. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export table
table_latex_count <- modelsummary::modelsummary(
  panels_count,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table,
  coef_map = coef_labels_count,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Count) on Property Values"
)

table_latex_count <- fit_tblr_latex(
  table_latex_count,
  label = "tbl:hedonic-count-bins-full",
  notes = custom_notes_count
)

output_path_count <- file.path(output_dir, "hedonic_count_bins_full.tex")
writeLines(table_latex_count, output_path_count)


# ==============================================================================
# Export Tables: Spill Hours
# ==============================================================================
cat("Exporting spill hours table...\n")

# Coefficient labels
coef_labels_hrs <- c(
  "(Intercept)" = "Constant",
  "spill_hrs_binQ1" = "Spill duration Q1",
  "spill_hrs_binQ2" = "Spill duration Q2",
  "spill_hrs_binQ3" = "Spill duration Q3",
  "spill_hrs_binQ4" = "Spill duration Q4"
)

# Combined models for joint table
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_1,
    "(2)" = model_sales_hrs_1b,
    "(3)" = model_sales_hrs_4,
    "(4)" = model_sales_hrs_5,
    "(5)" = model_sales_hrs_2,
    "(6)" = model_sales_hrs_3
  ),
  "House Rentals" = list(
    "(7)" = model_rental_hrs_1,
    "(8)" = model_rental_hrs_1b,
    "(9)" = model_rental_hrs_4,
    "(10)" = model_rental_hrs_5,
    "(11)" = model_rental_hrs_2,
    "(12)" = model_rental_hrs_3
  )
)

# Notes
custom_notes_hrs <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the total spill duration in hours recorded across all overflows within 250m over the entire study window (2021--2024 for sales, 2021--2023 for rentals), classified into quartiles (Q1--Q4) based on the distribution of strictly positive exposure; the reference category is properties near overflows with zero recorded spills. Properties are excluded where any overflow within 250m lacks reported annual spill data for part of the window (including overflows that stopped reporting and subsequently left the register), so exposure is always a complete-window total. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export table
table_latex_hrs <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table,
  coef_map = coef_labels_hrs,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Hours) on Property Values"
)

table_latex_hrs <- fit_tblr_latex(
  table_latex_hrs,
  label = "tbl:hedonic-hrs-bins-full",
  notes = custom_notes_hrs
)

output_path_hrs <- file.path(output_dir, "hedonic_hrs_bins_full.tex")
writeLines(table_latex_hrs, output_path_hrs)


# ==============================================================================
# Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_count_bins_full.tex\n")
cat("  - hedonic_hrs_bins_full.tex\n")
