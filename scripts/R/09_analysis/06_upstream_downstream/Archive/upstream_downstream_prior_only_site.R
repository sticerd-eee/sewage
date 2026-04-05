# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average Spill Measures
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous daily average measures (spill count and hours) for
#          sites located in either upstream or downstream river waters of the
#          property.
#          Panel A: unweighted spill counts and hours. Panel B: spill counts and
#          hours are weighted by inverse river distance from spill site to 
#          property. Each panel includes OLS, Controls, FE, and FE + Controls.
#
# Author: Alina Zeltikova
# Date: 2026-01-30
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/house_site - Cross-sectional sales (house - spill site level)
#   - data/processed/cross_section/rentals/prior_to_rental/rental_site - Cross-sectional rentals (rental - spill site level)
#   - upstream_downstream/output/18-03/river_filter/spill_house_signed - Cross-section of house - upstream/downstream spill site pairs
#   - upstream_downstream/output/18-03/river_filter/spill_rental_signed - Cross-section of rental - upstream/downstream spill site pairs
#
# Outputs:
#   - output/tables/hedonic_count_continuous_prior_direction.tex
#   - output/tables/hedonic_count_continuous_prior_direction_weighted.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction.tex
#   - output/hedonic_hrs_continuous_prior_direction_weighted.tex
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

# ==============================================================================
# 3. Setup
# ==============================================================================

# Output Directory -------------------------------------------------------------

output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

exported_files <- character()
record_export <- function(path) {
  exported_files <<- c(exported_files, basename(path))
}

# ==============================================================================
# Panel A: Sales
# ==============================================================================

# Load Sales Data --------------------------------------------------------------
cat("Loading sales data...\n")

# Cross-section data with spill metrics (prior to sale)
## Filter for houses with at least one spill site within radius
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale_house_site")
) |>
  filter(radius == RAD) |>
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

# Load Upstream/Downstream Data ------------------------------------------------
cat("Preparing upstream/downstream data...\n")

path_upstream_downstream_sales <- here::here(
  "upstream_downstream",
  "output",
  "18-03",
  "river_filter",
  "spill_house_signed.csv"
)

upstream_downstream_sales <- import(path_upstream_downstream_sales)

upstream_downstream_sales <- upstream_downstream_sales |>
  mutate(direction=ifelse(direction==-1, 1,0),
         dist_river_m = abs(signed_dist_m))|>
  filter(spill_lateral_m <= 250 & house_lateral_m <= 250 & dist_river_m <= 1000  & spill_house_euclid_m <= RAD) |>
  distinct()

# Prepare Sales Data -----------------------------------------------------------
cat("Preparing sales data...\n")

dat_sales_clean <- dat_cs_sales |>
  select(-any_of("price")) |>
  inner_join(sales, by = "house_id") |>
  inner_join(upstream_downstream_sales, by = c("house_id", "site_id")) |>
  mutate(log_price = log(price)) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(spill_hrs_daily_avg),
    !is.na(lsoa),
    !is.na(msoa),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration),
    !is.na(direction)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

cat("  Sales observations (house-site pairs):", nrow(dat_sales_clean), "\n")

# Keep properties with only one spill site -------------------------------------
dat_one_site_sales <- dat_sales_clean |> 
  group_by(house_id) |>
  filter(n() == 1) |>
  ungroup() |>
  mutate(log_price = log(price)) 

# ==============================================================================
# Panel B: Rentals
# ==============================================================================

# Load Rental Data -------------------------------------------------------------
cat("Loading rental data...\n")

# Cross-section data with spill metrics (prior to rental)
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental_rental_site")
) |>
  filter(radius == RAD) |>
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

# Load Upstream/Downstream Data ------------------------------------------------
cat("Preparing upstream/downstream data...\n")

path_upstream_downstream_rentals <- here::here(
  "upstream_downstream",
  "output",
  "18-03",
  "river_filter",
  "spill_rental_signed.csv"
)

upstream_downstream_rentals <- import(path_upstream_downstream_rentals)

upstream_downstream_rentals <- upstream_downstream_rentals |>
  mutate(direction=ifelse(direction==-1, 1,0), # Upstream/Downstream to binary
         dist_river_m = abs(signed_dist_m))|>
  filter(spill_lateral_m <= 250 & rental_lateral_m <= 250 & dist_river_m <= 1000 & spill_house_euclid_m <= RAD) |>
  distinct()

# Prepare Rental Data ----------------------------------------------------------
cat("Preparing rental data...\n")

dat_rental_clean <- dat_cs_rentals |>
  select(-any_of("listing_price")) |>
  inner_join(rentals, by = "rental_id") |>
  inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id")) |>
  mutate(log_price = log(listing_price)) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(spill_hrs_daily_avg),
    !is.na(lsoa),
    !is.na(msoa),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms),
    !is.na(direction)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat("  Rental observations (rental-site pairs):", nrow(dat_rental_clean), "\n")

# Keep properties with only one spill site -------------------------------------
dat_one_site_rentals <- dat_rental_clean |>
  group_by(rental_id) |> 
  filter(n() == 1) |>
  ungroup() |>
  mutate(log_price = log(listing_price)) 

# ==============================================================================
# Export Tables: Spill Count Daily Average, One Site 
# ==============================================================================

cat("Exporting spill count one site table...\n")

# Coefficient labels
coef_labels_count <- c(
  "(Intercept)" = "Constant",
  "spill_count_daily_avg" = "Daily spill count", 
  "direction" = "Upstream spill site",
  "spill_count_daily_avg:direction" = "{Daily count \\\\ $\\times$ Upstream}"
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
    "(1)" = model_sales_count_ns_1_one,
    "(2)" = model_sales_count_ns_1b_one,
    "(3)" = model_sales_count_ns_2b_one,
    "(4)" = model_sales_count_ns_3b_one,
    "(5)" = model_sales_count_ns_2_one,
    "(6)" = model_sales_count_ns_3_one
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_count_ns_1_one,
    "(8)" = model_rentals_count_ns_1b_one,
    "(9)" = model_rentals_count_ns_2b_one,
    "(10)" = model_rentals_count_ns_3b_one,
    "(11)" = model_rentals_count_ns_2_one,
    "(12)" = model_rentals_count_ns_3_one
  )
)
# Add rows for location fixed effects and property controls
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  
)
attr(add_rows, "position") <- "coef_end"
# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample is restricted to properties with exactly one storm overflow within 250m in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded at the single overflow within 250m from January 2021 to the transaction date. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
  escape = FALSE,  
  title = "Effect of Sewage Spills (Count) on Property Values by Direction: One Site Sample"
)
# Force table environment to [H]
table_latex_count <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_count)
# Add label in tabularray format
table_latex_count <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-count-continuous-prior-one-site},",
  table_latex_count
)
# Add colsep, rowsep, hspan and font size for tighter column spacing
table_latex_count <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1hspan = even,\ncolsep=2pt,\nrowsep=0.1pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_count
)
# Replace empty note with custom notes (tabularray format)
table_latex_count <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_count,
  table_latex_count
)
output_path_count <- file.path(output_dir, "hedonic_count_continuous_prior_one_site.tex")
writeLines(table_latex_count, output_path_count)
record_export(output_path_count)

# ==============================================================================
# Export Tables: Spill Count Daily Average, One Site + Distance Control
# ==============================================================================

cat("Exporting spill count one site + distance table...\n")

# Coefficient labels
coef_labels_count <- c(
  "(Intercept)" = "Constant",
  "spill_count_daily_avg" = "Daily spill count", 
  "direction" = "Upstream spill site",
  "spill_count_daily_avg:direction" = "{Daily count \\\\ $\\times$ Upstream}",
  "dist_river_m" = "River distance"
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
    "(1)" = model_sales_count_ns_4_one,
    "(2)" = model_sales_count_ns_4b_one,
    "(3)" = model_sales_count_ns_5b_one,
    "(4)" = model_sales_count_ns_6b_one,
    "(5)" = model_sales_count_ns_5_one,
    "(6)" = model_sales_count_ns_6_one
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_count_ns_4_one,
    "(8)" = model_rentals_count_ns_4b_one,
    "(9)" = model_rentals_count_ns_5b_one,
    "(10)" = model_rentals_count_ns_6b_one,
    "(11)" = model_rentals_count_ns_5_one,
    "(12)" = model_rentals_count_ns_6_one
  )
)
# Add rows for location fixed effects and property controls
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"   
)
attr(add_rows, "position") <- "coef_end"
# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample is restricted to properties with exactly one storm overflow within 250m in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded at the single overflow within 250m from January 2021 to the transaction date. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
  escape = FALSE,  
  title = "Effect of Sewage Spills (Count) on Property Values by Direction and Distance: One Site Sample"
)
# Force table environment to [H]
table_latex_count <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_count)
# Add label in tabularray format
table_latex_count <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-count-continuous-prior-one-site-distance},",
  table_latex_count
)
# Add colsep, rowsep, hspan and font size for tighter column spacing
table_latex_count <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1hspan = even,\ncolsep=2pt,\nrowsep=0.1pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_count
)
# Replace empty note with custom notes (tabularray format)
table_latex_count <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_count,
  table_latex_count
)
output_path_count <- file.path(output_dir, "hedonic_count_continuous_prior_one_site_distance.tex")
writeLines(table_latex_count, output_path_count)
record_export(output_path_count)

# ==============================================================================
# Export Tables: Spill Hours Daily Average, One Site 
# ==============================================================================

cat("Exporting spill hours one site table...\n")

# Coefficient labels
coef_labels_hrs <- c(
  "(Intercept)" = "Constant",
  "spill_hrs_daily_avg" = "Daily spill hours", 
  "direction" = "Upstream spill site",
  "spill_hrs_daily_avg:direction" = "{Daily hours \\\\ $\\times$ Upstream}"
)
# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)
# Combined models for joint table
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_ns_1_one,
    "(2)" = model_sales_hrs_ns_1b_one,
    "(3)" = model_sales_hrs_ns_2b_one,
    "(4)" = model_sales_hrs_ns_3b_one,
    "(5)" = model_sales_hrs_ns_2_one,
    "(6)" = model_sales_hrs_ns_3_one
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_hrs_ns_1_one,
    "(8)" = model_rentals_hrs_ns_1b_one,
    "(9)" = model_rentals_hrs_ns_2b_one,
    "(10)" = model_rentals_hrs_ns_3b_one,
    "(11)" = model_rentals_hrs_ns_2_one,
    "(12)" = model_rentals_hrs_ns_3_one
  )
)
# Add rows for location fixed effects and property controls
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  
)
attr(add_rows, "position") <- "coef_end"
# Notes
custom_notes_hrs <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample is restricted to properties with exactly one storm overflow within 250m in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per day recorded at the single overflow within 250m from January 2021 to the transaction date. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)
# Export table
table_latex_hrs <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_decimal(2),
  coef_map = coef_labels_hrs,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  escape = FALSE,  
  title = "Effect of Sewage Spills (Hours) on Property Values by Direction: One Site Sample"
)
# Force table environment to [H]
table_latex_hrs <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_hrs)
# Add label in tabularray format
table_latex_hrs <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-hrs-continuous-prior-one-site},",
  table_latex_hrs
)
# Add colsep, rowsep, hspan and font size for tighter column spacing
table_latex_hrs <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1hspan = even,\ncolsep=2pt,\nrowsep=0.1pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_hrs
)
# Replace empty note with custom notes (tabularray format)
table_latex_hrs <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_hrs,
  table_latex_hrs
)
output_path_hrs <- file.path(output_dir, "hedonic_hrs_continuous_prior_one_site.tex")
writeLines(table_latex_hrs, output_path_hrs)
record_export(output_path_hrs)

# ==============================================================================
# Export Tables: Spill Hours Daily Average, One Site + Distance Control
# ==============================================================================

cat("Exporting spill hours one site + distance table...\n")

# Coefficient labels
coef_labels_hrs <- c(
  "(Intercept)" = "Constant",
  "spill_hrs_daily_avg" = "Daily spill hours", 
  "direction" = "Upstream spill site",
  "spill_hrs_daily_avg:direction" = "{Daily hours \\\\ $\\times$ Upstream}",
  "dist_river_m" = "River distance"
)
# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)
# Combined models for joint table
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_ns_4_one,
    "(2)" = model_sales_hrs_ns_4b_one,
    "(3)" = model_sales_hrs_ns_5b_one,
    "(4)" = model_sales_hrs_ns_6b_one,
    "(5)" = model_sales_hrs_ns_5_one,
    "(6)" = model_sales_hrs_ns_6_one
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_hrs_ns_4_one,
    "(8)" = model_rentals_hrs_ns_4b_one,
    "(9)" = model_rentals_hrs_ns_5b_one,
    "(10)" = model_rentals_hrs_ns_6b_one,
    "(11)" = model_rentals_hrs_ns_5_one,
    "(12)" = model_rentals_hrs_ns_6_one
  )
)
# Add rows for location fixed effects and property controls
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"   
)
attr(add_rows, "position") <- "coef_end"
# Notes
custom_notes_hrs <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample is restricted to properties with exactly one storm overflow within 250m in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per day recorded at the single overflow within 250m from January 2021 to the transaction date. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)
# Export table
table_latex_hrs <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_decimal(2),
  coef_map = coef_labels_hrs,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  escape = FALSE,  
  title = "Effect of Sewage Spills (Hours) on Property Values by Direction and Distance: One Site Sample"
)
# Force table environment to [H]
table_latex_hrs <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex_hrs)
# Add label in tabularray format
table_latex_hrs <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:hedonic-hrs-continuous-prior-one-site-distance},",
  table_latex_hrs
)
# Add colsep, rowsep, hspan and font size for tighter column spacing
table_latex_hrs <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1hspan = even,\ncolsep=2pt,\nrowsep=0.1pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex_hrs
)
# Replace empty note with custom notes (tabularray format)
table_latex_hrs <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes_hrs,
  table_latex_hrs
)
output_path_hrs <- file.path(output_dir, "hedonic_hrs_continuous_prior_one_site_distance.tex")
writeLines(table_latex_hrs, output_path_hrs)
record_export(output_path_hrs)

# ==============================================================================
# Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (file in exported_files) {
  cat("  - ", file, "\n", sep = "")
}