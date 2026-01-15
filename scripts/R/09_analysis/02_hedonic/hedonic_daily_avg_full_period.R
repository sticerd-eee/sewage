# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average (Full Period)
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous daily average measures calculated over the entire
#          2021-2023 period (not just prior to transaction date).
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, FE, and FE + Controls.
#
# Author: Jacopo Olivieri
# Date: 2025-01-15
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill data
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/general_panel/sales/ - General panel (Arrow dataset)
#   - data/processed/general_panel/rentals/ - General panel (Arrow dataset)
#
# Outputs:
#   - output/tables/hedonic_spill_count_daily_avg_full.tex
#   - output/tables/hedonic_spill_hrs_daily_avg_full.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
BASE_YEAR <- 2021
N_DAYS_FULL_PERIOD <- 1095L  # 365 * 3 (2021-2023)


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

# 3.2 Data Loading - Common ----------------------------------------------------
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
cat("Loading sales data...\n")

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
cat("Preparing sales data...\n")

spill_sales_collapsed <- gen_panel_sales |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(house_id) |>
  summarise(
    spill_count = sum(spill_count_yr),
    spill_hrs = sum(spill_hrs_yr)
  )

dat_sales_clean <- sales |>
  left_join(spill_sales_collapsed, by = join_by(house_id)) |>
  mutate(
    spill_count_daily_avg = spill_count / N_DAYS_FULL_PERIOD,
    spill_hrs_daily_avg = spill_hrs / N_DAYS_FULL_PERIOD,
    log_price = log(price)
  ) |>
  filter(
    !is.na(spill_count),
    !is.na(lsoa)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

cat("  Sales observations:", nrow(dat_sales_clean), "\n")

# ==============================================================================
# 5. Panel B: Rentals
# ==============================================================================

# 5.1 Load Rental Data ---------------------------------------------------------
cat("Loading rental data...\n")

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
cat("Preparing rental data...\n")

spill_rental_collapsed <- gen_panel_rental |>
  filter(!is.na(site_id)) |>
  left_join(spills, by = join_by(site_id, year)) |>
  group_by(rental_id) |>
  summarise(
    spill_count = sum(spill_count_yr),
    spill_hrs = sum(spill_hrs_yr)
  )

dat_rental_clean <- rentals |>
  left_join(spill_rental_collapsed, by = join_by(rental_id)) |>
  mutate(
    spill_count_daily_avg = spill_count / N_DAYS_FULL_PERIOD,
    spill_hrs_daily_avg = spill_hrs / N_DAYS_FULL_PERIOD,
    log_price = log(listing_price)
  ) |>
  filter(
    !is.na(spill_count),
    !is.na(lsoa)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat("  Rental observations:", nrow(dat_rental_clean), "\n")

# ==============================================================================
# 6. Estimate Models: Spill Count Daily Average
# ==============================================================================
cat("Estimating spill count models...\n")

# Sales Models
model_sales_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + old_new + duration,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + bedrooms + bathrooms,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# 7. Estimate Models: Spill Hours Daily Average
# ==============================================================================
cat("Estimating spill hours models...\n")

# Sales Models
model_sales_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + old_new + duration,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + bedrooms + bathrooms,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# 8. Export Tables: Spill Count Daily Average
# ==============================================================================
cat("Exporting spill count table...\n")

# Coefficient labels
coef_labels_count <- c(
  "(Intercept)" = "Constant",
  "spill_count_daily_avg" = "Daily spill count"
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
    "(3)" = model_sales_count_2,
    "(4)" = model_sales_count_3
  ),
  "House Rentals" = list(
    "(5)" = model_rental_count_1,
    "(6)" = model_rental_count_1b,
    "(7)" = model_rental_count_2,
    "(8)" = model_rental_count_3
  )
)

# Add rows for fixed effects and controls
add_rows <- tibble::tribble(
  ~term                , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` ,
  "LSOA FE"            , "No"   , "No"   , "Yes"  , "Yes"  , "No"   , "No"   , "Yes"  , "Yes"  ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--4) or log weekly asking rent for rentals (columns 5--8). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m over the entire 2021--2023 period. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export table
table_latex_count <- modelsummary::modelsummary(
  panels_count,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_decimal(2),
  coef_map = coef_labels_count,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Count) on Property Values: Full Period"
)

# Force table environment to [H]
table_latex_count <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_count)

# Add label in tabularray format
table_latex_count <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-spill-count-daily-full},",
  table_latex_count
)

# Add colsep and font size for tighter column spacing
table_latex_count <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1colsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_count
)

# Replace empty note with custom notes (tabularray format)
table_latex_count <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_count,
  table_latex_count
)

output_path_count <- file.path(output_dir, "hedonic_spill_count_daily_avg_full.tex")
writeLines(table_latex_count, output_path_count)

# ==============================================================================
# 9. Export Tables: Spill Hours Daily Average
# ==============================================================================
cat("Exporting spill hours table...\n")

# Coefficient labels (including controls)
coef_labels_hrs <- c(
  "(Intercept)" = "Constant",
  "spill_hrs_daily_avg" = "Daily spill hours"
)

# Combined models for joint table
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_1,
    "(2)" = model_sales_hrs_1b,
    "(3)" = model_sales_hrs_2,
    "(4)" = model_sales_hrs_3
  ),
  "House Rentals" = list(
    "(5)" = model_rental_hrs_1,
    "(6)" = model_rental_hrs_1b,
    "(7)" = model_rental_hrs_2,
    "(8)" = model_rental_hrs_3
  )
)

# Notes
custom_notes_hrs <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--4) or log weekly asking rent for rentals (columns 5--8). Spill exposure is measured as the average total number of spill hours per day recorded across all overflows within 250m over the entire 2021--2023 period. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export table
table_latex_hrs <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 2,
  coef_map = coef_labels_hrs,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Hours) on Property Values: Full Period"
)

# Force table environment to [H]
table_latex_hrs <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_hrs)

# Add label in tabularray format
table_latex_hrs <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-spill-hrs-daily-full},",
  table_latex_hrs
)

# Add colsep and font size for tighter column spacing
table_latex_hrs <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1colsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_hrs
)

# Replace empty note with custom notes (tabularray format)
table_latex_hrs <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_hrs,
  table_latex_hrs
)

output_path_hrs <- file.path(output_dir, "hedonic_spill_hrs_daily_avg_full.tex")
writeLines(table_latex_hrs, output_path_hrs)

# ==============================================================================
# 10. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_spill_count_daily_avg_full.tex\n")
cat("  - hedonic_spill_hrs_daily_avg_full.tex\n")
