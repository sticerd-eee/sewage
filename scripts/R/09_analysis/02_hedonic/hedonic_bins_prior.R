# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average Spill Measures (Quartiles)
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          quartile-binned daily average measures (spill count and hours).
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, FE, and FE + Controls.
#
# Author: Jacopo Olivieri
# Date: 2024-12-24
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#
# Outputs:
#   - output/tables/hedonic_count_bins_prior.tex
#   - output/tables/hedonic_hrs_bins_prior.tex
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

# Cross-section data with spill metrics (prior to sale)
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
cat("Preparing sales data...\n")

dat_sales_clean <- dat_cs_sales |>
  inner_join(sales, by = "house_id") |>
  mutate(
    log_price = log(price.y),
    spill_count_daily_avg_bin = bin_spill_measure(spill_count_daily_avg),
    spill_hrs_daily_avg_bin = bin_spill_measure(spill_hrs_daily_avg)
  ) |>
  filter(
    !is.na(spill_count_daily_avg_bin),
    !is.na(spill_hrs_daily_avg_bin),
    !is.na(lsoa),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration)
  ) |>
  mutate(
    spill_count_daily_avg_bin = forcats::fct_relevel(spill_count_daily_avg_bin, "0 spills"),
    spill_count_daily_avg_bin = forcats::fct_drop(spill_count_daily_avg_bin),
    spill_hrs_daily_avg_bin = forcats::fct_relevel(spill_hrs_daily_avg_bin, "0 spills"),
    spill_hrs_daily_avg_bin = forcats::fct_drop(spill_hrs_daily_avg_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
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

dat_rental_clean <- dat_cs_rentals |>
  inner_join(rentals, by = "rental_id") |>
  mutate(
    log_price = log(listing_price.y),
    spill_count_daily_avg_bin = bin_spill_measure(spill_count_daily_avg),
    spill_hrs_daily_avg_bin = bin_spill_measure(spill_hrs_daily_avg)
  ) |>
  filter(
    !is.na(spill_count_daily_avg_bin),
    !is.na(spill_hrs_daily_avg_bin),
    !is.na(lsoa),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms)
  ) |>
  mutate(
    spill_count_daily_avg_bin = forcats::fct_relevel(spill_count_daily_avg_bin, "0 spills"),
    spill_count_daily_avg_bin = forcats::fct_drop(spill_count_daily_avg_bin),
    spill_hrs_daily_avg_bin = forcats::fct_relevel(spill_hrs_daily_avg_bin, "0 spills"),
    spill_hrs_daily_avg_bin = forcats::fct_drop(spill_hrs_daily_avg_bin),
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat("  Rental observations:", nrow(dat_rental_clean), "\n")

# ==============================================================================
# Estimate Models: Spill Count Daily Average (Quartiles)
# ==============================================================================
cat("Estimating spill count models...\n")

# Sales Models
model_sales_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_count_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_count_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# Estimate Models: Spill Hours Daily Average (Quartiles)
# ==============================================================================
cat("Estimating spill hours models...\n")

# Sales Models
model_sales_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

model_sales_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin + property_type + old_new + duration | lsoa,
  data = dat_sales_clean,
  vcov = "hetero"
)

# Rental Models
model_rental_hrs_1 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_2 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

model_rental_hrs_3 <- fixest::feols(
  log_price ~ spill_hrs_daily_avg_bin + property_type + bedrooms + bathrooms | lsoa,
  data = dat_rental_clean,
  vcov = "hetero"
)

# ==============================================================================
# Export Tables: Spill Count Daily Average (Quartiles)
# ==============================================================================
cat("Exporting spill count table...\n")

# Coefficient labels (including controls)
coef_labels_count <- c(
  "(Intercept)" = "Constant (zero spills)",
  "spill_count_daily_avg_binQ1" = "Spill count Q1",
  "spill_count_daily_avg_binQ2" = "Spill count Q2",
  "spill_count_daily_avg_binQ3" = "Spill count Q3",
  "spill_count_daily_avg_binQ4" = "Spill count Q4",
  # Sales controls
  "property_typeF" = "Flat",
  "property_typeO" = "Other",
  "property_typeS" = "Semi-detached",
  "property_typeT" = "Terraced",
  "old_newY" = "New build",
  "durationL" = "Leasehold",
  # Rental controls
  "property_typeB" = "Bungalow",
  "property_typeD" = "Detached",
  "bedrooms" = "Bedrooms",
  "bathrooms" = "Bathrooms"
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
    "(2)" = model_sales_count_2,
    "(3)" = model_sales_count_3
  ),
  "House Rentals" = list(
    "(4)" = model_rental_count_1,
    "(5)" = model_rental_count_2,
    "(6)" = model_rental_count_3
  )
)

# Add rows for fixed effects
add_rows <- tibble::tribble(
  ~term     , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` ,
  "LSOA FE" , "No"   , "Yes"  , "Yes"  , "No"   , "Yes"  , "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Export table
table_latex_count <- modelsummary::modelsummary(
  panels_count,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 3,
  coef_map = coef_labels_count,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Count) on Property Values"
)

# Force table environment to [H]
table_latex_count <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_count)

# Add label in tabularray format
table_latex_count <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-count-bins-prior},",
  table_latex_count
)

output_path_count <- file.path(output_dir, "hedonic_count_bins_prior.tex")
writeLines(table_latex_count, output_path_count)

# ==============================================================================
# Export Tables: Spill Hours Daily Average (Quartiles)
# ==============================================================================
cat("Exporting spill hours table...\n")

# Coefficient labels (including controls)
coef_labels_hrs <- c(
  "(Intercept)" = "Constant (zero spills)",
  "spill_hrs_daily_avg_binQ1" = "Spill hours Q1",
  "spill_hrs_daily_avg_binQ2" = "Spill hours Q2",
  "spill_hrs_daily_avg_binQ3" = "Spill hours Q3",
  "spill_hrs_daily_avg_binQ4" = "Spill hours Q4",
  # Sales controls
  "property_typeF" = "Flat",
  "property_typeO" = "Other",
  "property_typeS" = "Semi-detached",
  "property_typeT" = "Terraced",
  "old_newY" = "New build",
  "durationL" = "Leasehold",
  # Rental controls
  "property_typeB" = "Bungalow",
  "property_typeD" = "Detached",
  "bedrooms" = "Bedrooms",
  "bathrooms" = "Bathrooms"
)

# Combined models for joint table
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_1,
    "(2)" = model_sales_hrs_2,
    "(3)" = model_sales_hrs_3
  ),
  "House Rentals" = list(
    "(4)" = model_rental_hrs_1,
    "(5)" = model_rental_hrs_2,
    "(6)" = model_rental_hrs_3
  )
)

# Export table
table_latex_hrs <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 3,
  coef_map = coef_labels_hrs,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills (Hours) on Property Values"
)

# Force table environment to [H]
table_latex_hrs <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_hrs)

# Add label in tabularray format
table_latex_hrs <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-hrs-bins-prior},",
  table_latex_hrs
)

output_path_hrs <- file.path(output_dir, "hedonic_hrs_bins_prior.tex")
writeLines(table_latex_hrs, output_path_hrs)

# ==============================================================================
# Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
cat("  - hedonic_count_bins_prior.tex\n")
cat("  - hedonic_hrs_bins_prior.tex\n")
