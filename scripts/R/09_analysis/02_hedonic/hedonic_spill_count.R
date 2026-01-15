# ==============================================================================
# Cross-Sectional Regression Analysis: Spill Count
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spill count on property values.
#          Panel A: Sales (log house prices)
#          Panel B: Rentals (log rental prices)
#          Each panel includes OLS, Controls, FE, and FE + Controls specifications.
#
# Author: Jacopo Olivieri
# Date: 2024-10-15
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/general_panel/sales/ - General panel (Arrow dataset)
#   - data/processed/general_panel/rentals/ - General panel (Arrow dataset)
#
# Outputs:
#   - output/tables/hedonic_spill_count.tex - Combined LaTeX table
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
BASE_YEAR <- 2021

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


# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Output Directory ---------------------------------------------------------
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.2 Helper Function ----------------------------------------------------------
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

# 3.3 Data Loading - Common ----------------------------------------------------
# Sewage Spills
path_spill_yr <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_yr.parquet"
)

spills <- import(path_spill_yr, trust = TRUE)

# ==============================================================================
# 4. Panel A: Sales
# ==============================================================================

# 4.1 Load Sales Data ----------------------------------------------------------

# House Prices
path_sales <- here::here("data", "processed", "house_price.parquet")

# Panel Data - General
path_general_panel_sales <- here::here(
  "data",
  "processed",
  "general_panel",
  "sales"
)

gen_panel_sales <- arrow::open_dataset(path_general_panel_sales) |>
  filter(radius == RAD) |>
  collect() |>
  mutate(
    year = (qtr_id - 1) %/% 4 + BASE_YEAR,
    year_transfer = qtr_id_transfer %/% 4 + BASE_YEAR
  ) |>
  distinct(house_id, site_id, year, year_transfer, distance_m, within_radius)

sales <- import(path_sales, trust = TRUE) |>
  mutate(year = (qtr_id - 1) %/% 4 + 1) |>
  select(
    -transaction_id,
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

# 4.2 Prepare Sales Data -------------------------------------------------------

spill_sales_collapsed <- gen_panel_sales |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(house_id) |>
  summarise(
    spill_count = sum(spill_count_yr)
  )

dat_sales_clean <- sales |>
  left_join(spill_sales_collapsed, by = join_by(house_id)) |>
  mutate(
    spill_count_bin = bin_spill_measure(spill_count),
    log_price = log(price)
  ) |>
  filter(
    !is.na(spill_count),
    !is.na(lsoa)
  ) |>
  mutate(
    spill_count_bin = forcats::fct_relevel(spill_count_bin, "0 spills"),
    spill_count_bin = forcats::fct_drop(spill_count_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

# 4.3 Estimate Sales Models ----------------------------------------------------

model_sales_1 <- fixest::feols(
  log_price ~ spill_count_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_1b <- fixest::feols(
  log_price ~ spill_count_bin + property_type + old_new + duration,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_2 <- fixest::feols(
  log_price ~ spill_count_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_3 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# ==============================================================================
# 5. Panel B: Rentals
# ==============================================================================

# 5.1 Load Rental Data ---------------------------------------------------------

# Rental Prices
path_rent <- here::here("data", "processed", "zoopla", "zoopla_rentals.parquet")

# Panel Data - General
path_general_panel_rental <- here::here(
  "data",
  "processed",
  "general_panel",
  "rentals"
)

gen_panel_rental <- arrow::open_dataset(path_general_panel_rental) |>
  filter(radius == RAD) |>
  collect() |>
  mutate(
    year = (qtr_id - 1) %/% 4 + BASE_YEAR,
    year_transfer = qtr_id_transfer %/% 4 + BASE_YEAR
  ) |>
  distinct(rental_id, site_id, year, year_transfer, distance_m, within_radius)

rentals <- import(path_rent, trust = TRUE) |>
  mutate(year = (qtr_id - 1) %/% 4 + 2021) |>
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

# 5.2 Prepare Rental Data ------------------------------------------------------

spill_rental_collapsed <- gen_panel_rental |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(rental_id) |>
  summarise(
    spill_count = sum(spill_count_yr)
  )

dat_rental_clean <- rentals |>
  left_join(spill_rental_collapsed, by = join_by(rental_id)) |>
  mutate(
    spill_count_bin = bin_spill_measure(spill_count),
    log_price = log(listing_price)
  ) |>
  filter(
    !is.na(spill_count),
    !is.na(lsoa)
  ) |>
  mutate(
    spill_count_bin = forcats::fct_relevel(spill_count_bin, "0 spills"),
    spill_count_bin = forcats::fct_drop(spill_count_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type)
  )

# 5.3 Estimate Rental Models ---------------------------------------------------

model_rental_1 <- fixest::feols(
  log_price ~ spill_count_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_1b <- fixest::feols(
  log_price ~ spill_count_bin + property_type + bedrooms + bathrooms,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_2 <- fixest::feols(
  log_price ~ spill_count_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_3 <- fixest::feols(
  log_price ~ spill_count_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# 6. Export Tables
# ==============================================================================

# Coefficient labels
coef_labels <- c(
  "(Intercept)" = "Constant",
  "spill_count_binQ1" = "Spill count Q1",
  "spill_count_binQ2" = "Spill count Q2",
  "spill_count_binQ3" = "Spill count Q3",
  "spill_count_binQ4" = "Spill count Q4"
)

# Goodness of fit map (use adjusted R-squared)
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Combined models for joint table with spanning headers via nested list
panels <- list(
  "House Sales" = list(
    "(1)" = model_sales_1,
    "(2)" = model_sales_1b,
    "(3)" = model_sales_2,
    "(4)" = model_sales_3
  ),
  "House Rentals" = list(
    "(5)" = model_rental_1,
    "(6)" = model_rental_1b,
    "(7)" = model_rental_2,
    "(8)" = model_rental_3
  )
)

# Add rows for fixed effects and controls
add_rows <- tibble::tribble(
  ~term                , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` ,
  "LSOA FE"            , "No"   , "No"   , "Yes"  , "Yes"  , "No"   , "No"   , "Yes"  , "Yes"  ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
)
attr(add_rows, "position") <- "coef_end"

# One-line custom notes (tabularray format)
custom_notes <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--4) or log weekly asking rent for rentals (columns 5--8). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m over the entire 2021--2023 period, classified into quartiles (Q1--Q4) based on the distribution of strictly positive exposure; the reference category is properties near overflows with zero recorded spills. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export Combined Table
table_latex <- modelsummary::modelsummary(
  panels,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 2,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Count) on Property Values"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label for referencing
table_latex <- sub(
  "(caption\\s*=\\s*\\{[^}]*\\})",
  "\\1,\n  label = {tbl:hedonic-spill-count}",
  table_latex
)

# Replace empty note with custom notes (tabularray format)
table_latex <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes,
  table_latex
)

# Add colsep and font size for tighter column spacing
table_latex <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1colsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex
)

output_path <- file.path(output_dir, "hedonic_spill_count.tex")
writeLines(table_latex, output_path)

cat("LaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_spill_count.tex\n")
