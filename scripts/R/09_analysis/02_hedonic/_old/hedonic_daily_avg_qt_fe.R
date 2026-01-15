# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average Spill Measures (Quarter FE)
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous daily average measures (spill count and hours) with
#          quarter fixed effects added to all specifications.
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes Quarter FE, Controls + Quarter FE,
#          LSOA + Quarter FE, and LSOA + Controls + Quarter FE.
#
# Author: Jacopo Olivieri
# Date: 2024-12-24
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#
# Outputs:
#   - output/tables/hedonic_spill_count_daily_avg3.tex
#   - output/tables/hedonic_spill_hrs_daily_avg3.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L


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

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# ==============================================================================
# Panel A: Sales
# ==============================================================================

# Load Sales Data --------------------------------------------------------------
cat("Loading sales data...\n")

# Cross-section data with spill metrics (prior to sale)
## Filter for houses with at least one spill site within radius
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
) |>
  filter(radius == RAD) |>
  filter(n_spill_sites > 0) |>
  collect()

# House price data for property characteristics and LSOA
sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  select(
    house_id,
    price,
    property_type,
    old_new,
    duration,
    postcode,
    lsoa,
    msoa,
    qtr_id  # Keep qtr_id for fixed effects
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type),
    old_new = forcats::as_factor(old_new),
    duration = forcats::as_factor(duration)
  )

# Trim sales at 2.5/97.5 percentiles
price_quantiles_sales <- quantile(sales$price, c(0.025, 0.975), na.rm = TRUE)
sales_trimmed <- sales |>
  filter(
    price >= price_quantiles_sales[1],
    price <= price_quantiles_sales[2]
  )

# Prepare Sales Data -----------------------------------------------------------
cat("Preparing sales data...\n")

dat_sales_clean <- dat_cs_sales |>
  select(-any_of("price")) |>
  inner_join(sales_trimmed, by = "house_id") |>
  mutate(log_price = log(price)) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(qtr_id)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration),
    qtr_id = forcats::as_factor(qtr_id)  # Convert to factor for FE
  )

cat("  Sales observations:", nrow(dat_sales_clean), "\n")

# ==============================================================================
# Panel B: Rentals
# ==============================================================================

# Load Rental Data -------------------------------------------------------------
cat("Loading rental data...\n")

# Cross-section data with spill metrics (prior to rental)
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
) |>
  filter(radius == RAD) |>
  filter(n_spill_sites > 0) |>
  collect()

# Rental price data for property characteristics and LSOA
rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
) |>
  select(
    rental_id,
    listing_price,
    property_type,
    bedrooms,
    bathrooms,
    lsoa,
    msoa,
    qtr_id  # Keep qtr_id for fixed effects
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type)
  )

# Trim rentals at 2.5/97.5 percentiles
price_quantiles_rent <- quantile(rentals$listing_price, c(0.025, 0.975), na.rm = TRUE)
rentals_trimmed <- rentals |>
  filter(
    listing_price >= price_quantiles_rent[1],
    listing_price <= price_quantiles_rent[2]
  )

# Prepare Rental Data ----------------------------------------------------------
cat("Preparing rental data...\n")

dat_rental_clean <- dat_cs_rentals |>
  select(-any_of("listing_price")) |>
  inner_join(rentals_trimmed, by = "rental_id") |>
  mutate(log_price = log(listing_price)) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(qtr_id)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    qtr_id = forcats::as_factor(qtr_id)  # Convert to factor for FE
  )

cat("  Rental observations:", nrow(dat_rental_clean), "\n")

# ==============================================================================
# Estimate Models: Spill Count Daily Average (with Quarter FE)
# ==============================================================================
cat("Estimating spill count models with quarter FE...\n")

# Sales Models
model_sales_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg | qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + old_new + duration | qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg | lsoa + qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + old_new + duration | lsoa + qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg | qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + bedrooms + bathrooms | qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg | lsoa + qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + property_type + bedrooms + bathrooms | lsoa + qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# Estimate Models: Spill Hours Daily Average (with Quarter FE)
# ==============================================================================
cat("Estimating spill hours models with quarter FE...\n")

# Sales Models
model_sales_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + old_new + duration | qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | lsoa + qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + old_new + duration | lsoa + qtr_id,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_1b <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + bedrooms + bathrooms | qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg | lsoa + qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg + property_type + bedrooms + bathrooms | lsoa + qtr_id,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# Export Tables: Spill Count Daily Average
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
  "Quarter FE"         , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  ,
  "LSOA FE"            , "No"   , "No"   , "Yes"  , "Yes"  , "No"   , "No"   , "Yes"  , "Yes"  ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--4) or log weekly asking rent for rentals (columns 5--8). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m from January 2021 to the transaction date. All specifications include quarter fixed effects. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Export table
table_latex_count <- modelsummary::modelsummary(
  panels_count,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 2,
  coef_map = coef_labels_count,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Count) on Property Values (Quarter FE)"
)

# Force table environment to [H]
table_latex_count <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_count)

# Add label in tabularray format
table_latex_count <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-spill-count-daily-qtr},",
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

output_path_count <- file.path(output_dir, "hedonic_spill_count_daily_avg3.tex")
writeLines(table_latex_count, output_path_count)

# ==============================================================================
# Export Tables: Spill Hours Daily Average
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
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--4) or log weekly asking rent for rentals (columns 5--8). Spill exposure is measured as the average total spill duration in hours per day recorded across all overflows within 250m from January 2021 to the transaction date. All specifications include quarter fixed effects. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
  title = "Effect of Sewage Spills (Hours) on Property Values (Quarter FE)"
)

# Force table environment to [H]
table_latex_hrs <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_hrs)

# Add label in tabularray format
table_latex_hrs <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-spill-hrs-daily-qtr},",
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

output_path_hrs <- file.path(output_dir, "hedonic_spill_hrs_daily_avg3.tex")
writeLines(table_latex_hrs, output_path_hrs)

# ==============================================================================
# Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_spill_count_daily_avg3.tex\n")
cat("  - hedonic_spill_hrs_daily_avg3.tex\n")
