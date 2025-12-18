# ==============================================================================
# Cross-Sectional Regression Analysis: Spill Hours
# ==============================================================================
# Purpose: Estimate the effect of sewage spill duration (hours) on property values
#          Panel A: Sales (log house prices)
#          Panel B: Rentals (log rental prices)
#          Each panel includes OLS, FE, and FE + Controls specifications
#
# Author: Jacopo Olivieri
# Date: 2024-10-15
#
# Outputs: LaTeX regression table saved to output/tables/
#          - hedonic_spill_hrs.tex (combined table with both panels)
# ==============================================================================

# Configuration ----------------------------------------------------------------
RAD <- 250L
BASE_YEAR <- 2021

# Package Management -----------------------------------------------------------
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

# Run manual renv commands if needed
# renv::activate() # Activate the project library
# renv::restore()  # Restore the environment if running for first time
# renv::snapshot()  # After adding/updating packages, snapshot the state

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

# Data Loading -----------------------------------------------------------------

# Sewage Spills
path_spill_yr <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_yr.parquet"
)

spills <- import(path_spill_yr, trust = TRUE)

# ==============================================================================
# Panel A: Sales
# ==============================================================================

# Load Sales Data --------------------------------------------------------------

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

# Prepare Sales Data -----------------------------------------------------------

spill_sales_collapsed <- gen_panel_sales |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(house_id) |>
  summarise(
    spill_hrs = sum(spill_hrs_yr, na.rm = TRUE)
  )

dat_sales_clean <- sales |>
  left_join(spill_sales_collapsed, by = join_by(house_id)) |>
  mutate(
    spill_hrs_bin = bin_spill_measure(spill_hrs),
    log_price = log(price)
  ) |>
  filter(
    !is.na(spill_hrs),
    !is.na(lsoa)
  ) |>
  mutate(
    spill_hrs_bin = forcats::fct_relevel(spill_hrs_bin, "0 spills"),
    spill_hrs_bin = forcats::fct_drop(spill_hrs_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

# Estimate Sales Models --------------------------------------------------------

model_sales_1 <- fixest::feols(
  log_price ~ spill_hrs_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_2 <- fixest::feols(
  log_price ~ spill_hrs_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_3 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# ==============================================================================
# Panel B: Rentals
# ==============================================================================

# Load Rental Data -------------------------------------------------------------

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

# Prepare Rental Data ----------------------------------------------------------

spill_rental_collapsed <- gen_panel_rental |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(rental_id) |>
  summarise(
    spill_hrs = sum(spill_hrs_yr, na.rm = TRUE)
  )

# Trim outliers at 2.5 and 97.5 percentiles
price_quantiles <- quantile(
  rentals$listing_price,
  c(0.025, 0.975),
  na.rm = TRUE
)

dat_rental_clean <- rentals |>
  filter(
    listing_price >= price_quantiles[1],
    listing_price <= price_quantiles[2]
  ) |>
  left_join(spill_rental_collapsed, by = join_by(rental_id)) |>
  mutate(
    spill_hrs_bin = bin_spill_measure(spill_hrs),
    log_price = log(listing_price)
  ) |>
  filter(
    !is.na(spill_hrs),
    !is.na(lsoa)
  ) |>
  mutate(
    spill_hrs_bin = forcats::fct_relevel(spill_hrs_bin, "0 spills"),
    spill_hrs_bin = forcats::fct_drop(spill_hrs_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type)
  )

# Estimate Rental Models -------------------------------------------------------

model_rental_1 <- fixest::feols(
  log_price ~ spill_hrs_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_2 <- fixest::feols(
  log_price ~ spill_hrs_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_3 <- fixest::feols(
  log_price ~ spill_hrs_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# Export Tables
# ==============================================================================

# Coefficient labels
coef_labels <- c(
  "(Intercept)" = "Constant (zero spills)",
  "spill_hrs_binQ1" = "Spill duration Q1",
  "spill_hrs_binQ2" = "Spill duration Q2",
  "spill_hrs_binQ3" = "Spill duration Q3",
  "spill_hrs_binQ4" = "Spill duration Q4"
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
    "(2)" = model_sales_2,
    "(3)" = model_sales_3
  ),
  "House Rentals" = list(
    "(4)" = model_rental_1,
    "(5)" = model_rental_2,
    "(6)" = model_rental_3
  )
)

# Add rows for fixed effects and controls
add_rows <- tibble::tribble(
  ~term                , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` ,
  "LSOA FE"            , "No"   , "Yes"  , "Yes"  , "No"   , "Yes"  , "Yes"  ,
  "Property controls"  , "No"   , "No"   , "Yes"  , "No"   , "No"   , "Yes"
)
attr(add_rows, "position") <- "coef_end"

# One-line custom notes
custom_notes <- paste0(
  "\\\\begin{tablenotes}[flushleft]\n",
  "       \\\\item \\\\hspace{-0.25cm} \\\\protect\\\\footnotesize{\\\\textbf{Notes:} Dependent variables are log house price (cols 1-3) and log rental price (cols 4-6). Heteroskedasticity-robust standard errors in parentheses. Spill duration quartiles (Q1-Q4) represent increasing sewage spill hours. LSOA FE denotes Lower Layer Super Output Area fixed effects. Property controls include property type, new-build status, and tenure for sales; property type, bedrooms, and bathrooms for rentals. \\\\sym{***} \\\\(p<0.01\\\\), \\\\sym{**} \\\\(p<0.05\\\\), \\\\sym{*} \\\\(p<0.1\\\\).}\n",
  "    \\\\end{tablenotes}"
)

# Export Combined Table
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
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Hours) on Property Values"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label for referencing
table_latex <- sub(
  "(\\\\caption\\{[^}]*\\})",
  "\\\\1\n\\\\label{tbl:hedonic-spill-hrs}",
  table_latex
)

# Replace tablenotes with custom one-liner
table_latex <- sub(
  "(?s)\\\\begin\\{tablenotes\\}.*?\\\\end\\{tablenotes\\}",
  custom_notes,
  table_latex,
  perl = TRUE
)

output_path <- file.path(output_dir, "hedonic_spill_hrs.tex")
writeLines(table_latex, output_path)

cat("LaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_spill_hrs.tex\n")