# ==============================================================================
# News/Information DiD Analysis (Google Trends Peak)
# ==============================================================================
#
# Purpose: Estimate whether the capitalization of sewage spill exposure into
#          house prices changes after public attention peaks, using a pre/post
#          DiD where treatment is the canonical Stage-2 weekly spill count
#          within 250m and post starts at the Google Trends peak month
#          (inclusive).
#
# Author: Jacopo Olivieri
# Date: 2025-01-08
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx - Google Trends search data
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/cross_section/sales/study_period/ - Sales exposure
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/cross_section/rentals/study_period/ - Rental exposure
#
# Outputs:
#   - output/tables/did_trends_full.tex - LaTeX regression table
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
BASE_YEAR <- 2021L
SALES_END_YEAR <- 2024L
RENTALS_END_YEAR <- 2023L


# ==============================================================================
# 2. Package Management
# ==============================================================================

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "here",
  "readxl",
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

# Shared table formatting helpers
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))


# ==============================================================================
# 3. Setup
# ==============================================================================
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}


# ==============================================================================
# 4. Data Preparation
# ==============================================================================

# 4.1 Load Google Trends and find peak month ----------------------------------
cat("Loading Google Trends data...\n")

google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
) |>
  filter(Year >= BASE_YEAR, Year <= SALES_END_YEAR)

# Find peak month (earliest if ties)
peak_row <- google_trends |>
  slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)

# Convert YYYY-MM to month_id (Jan 2021 = 1)
peak_year <- peak_row$Year
peak_month <- as.integer(substr(peak_row$Date, 6, 7))
PEAK_MONTH_ID <- (peak_year - BASE_YEAR) * 12 + peak_month

cat(sprintf("  Google Trends peak: %s (month_id = %d)\n", peak_row$Date, PEAK_MONTH_ID))

# 4.2 Load canonical Stage-2 sales exposure (250m radius) ---------------------
cat("Loading canonical Stage-2 sales exposure...\n")

path_cross_section_sales <- here::here(
  "data", "processed", "cross_section", "sales", "study_period"
)

spill_sales <- arrow::open_dataset(path_cross_section_sales) |>
  filter(radius == RAD) |>
  select(house_id, spill_count_weekly_avg, spatially_eligible) |>
  collect() |>
  filter(spatially_eligible, !is.na(spill_count_weekly_avg)) |>
  distinct(house_id, spill_count_weekly_avg)

if (anyDuplicated(spill_sales$house_id)) {
  stop("Stage-2 sales exposure must be unique on house_id at the selected radius.",
       call. = FALSE)
}

cat(sprintf(
  "  Found %d sales with complete spatially eligible exposure within %dm\n",
  nrow(spill_sales), RAD
))
cat(sprintf(
  "  Weekly spill count: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
  mean(spill_sales$spill_count_weekly_avg),
  sd(spill_sales$spill_count_weekly_avg),
  min(spill_sales$spill_count_weekly_avg),
  max(spill_sales$spill_count_weekly_avg)
))

# 4.3 Load sales transactions ---------------------------------------------------
cat("Loading sales transactions...\n")

sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
)

cat(sprintf("  Loaded %d transactions\n", nrow(sales)))

# 4.4 Merge and create analysis variables -------------------------------------
cat("Creating analysis dataset...\n")

dat <- sales |>
  left_join(spill_sales, by = "house_id") |>
  mutate(
    log_price = log(price),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
    !is.na(spill_count_weekly_avg),
    !is.na(month_id),
    month_id <= 12L * (SALES_END_YEAR - BASE_YEAR + 1L),
    !is.na(lsoa),
    !is.na(qtr_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration),
    !is.na(post),
    is.finite(log_price)
  )

cat(sprintf("  Final dataset: %d transactions\n", nrow(dat)))
cat(sprintf("  Pre-period (month_id < %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat$post == 0)))
cat(sprintf("  Post-period (month_id >= %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat$post == 1)))

# 4.5 Load canonical Stage-2 rental exposure (250m radius) --------------------
cat("Loading canonical Stage-2 rental exposure...\n")

path_cross_section_rental <- here::here(
  "data", "processed", "cross_section", "rentals", "study_period"
)

spill_rentals <- arrow::open_dataset(path_cross_section_rental) |>
  filter(radius == RAD) |>
  select(rental_id, spill_count_weekly_avg, spatially_eligible) |>
  collect() |>
  filter(spatially_eligible, !is.na(spill_count_weekly_avg)) |>
  distinct(rental_id, spill_count_weekly_avg)

if (anyDuplicated(spill_rentals$rental_id)) {
  stop("Stage-2 rental exposure must be unique on rental_id at the selected radius.",
       call. = FALSE)
}

cat(sprintf(
  "  Found %d rentals with complete spatially eligible exposure within %dm\n",
  nrow(spill_rentals), RAD
))
cat(sprintf(
  "  Weekly spill count: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
  mean(spill_rentals$spill_count_weekly_avg),
  sd(spill_rentals$spill_count_weekly_avg),
  min(spill_rentals$spill_count_weekly_avg),
  max(spill_rentals$spill_count_weekly_avg)
))

# 4.6 Load rental transactions ------------------------------------------------
cat("Loading rental transactions...\n")

rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
)

cat(sprintf("  Loaded %d rental transactions\n", nrow(rentals)))

# 4.7 Merge and create rental analysis variables -------------------------------
cat("Creating rental analysis dataset...\n")

dat_rental <- rentals |>
  left_join(spill_rentals, by = "rental_id") |>
  mutate(
    log_price = log(listing_price),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
    !is.na(spill_count_weekly_avg),
    !is.na(month_id),
    month_id <= 12L * (RENTALS_END_YEAR - BASE_YEAR + 1L),
    !is.na(lsoa),
    !is.na(qtr_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms),
    !is.na(post),
    is.finite(log_price)
  )

cat(sprintf("  Final rental dataset: %d transactions\n", nrow(dat_rental)))
cat(sprintf("  Pre-period (month_id < %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat_rental$post == 0)))
cat(sprintf("  Post-period (month_id >= %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat_rental$post == 1)))


# ==============================================================================
# 5. Estimation
# ==============================================================================
cat("\nEstimating regression models...\n")

# 5.1 Sales models ------------------------------------------------------------

# Model 1: No controls, no FE
model_sale_1 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + post + spill_count_weekly_avg:post,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 1 (no controls, no FE) estimated\n")

# Model 2: LSOA + Quarter FE only
model_sale_2 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post | lsoa + qtr_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 2 (FE only) estimated\n")

# Model 3: LSOA + Quarter FE + property controls
model_sale_3 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post +
    property_type + old_new + duration | lsoa + qtr_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 3 (FE + controls) estimated\n")

# 5.2 Rental models -----------------------------------------------------------

# Model 4: No controls, no FE
model_rent_1 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + post + spill_count_weekly_avg:post,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 1 (no controls, no FE) estimated\n")

# Model 5: LSOA + Quarter FE only
model_rent_2 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post | lsoa + qtr_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 2 (FE only) estimated\n")

# Model 6: LSOA + Quarter FE + property controls
model_rent_3 <- fixest::feols(
  log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post +
    property_type + bedrooms + bathrooms | lsoa + qtr_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 3 (FE + controls) estimated\n")

cat("  Using LSOA-clustered SEs\n")


# ==============================================================================
# 6. Export Table
# ==============================================================================
cat("\nExporting regression table...\n")

# Coefficient labels
coef_labels <- c(
  "spill_count_weekly_avg" = "Weekly spill count",
  "post" = "Post",
  "spill_count_weekly_avg:post" = "{Weekly spill count \\\\ $\\times$ Post}"
)

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw, ~clean, ~fmt,
  "nobs", "Observations", 0,
  "adj.r.squared", "Adj. R-squared", 3
)

# Add rows for fixed effects
add_rows <- tibble::tribble(
  ~term, ~`(1)`, ~`(2)`, ~`(3)`, ~`(4)`, ~`(5)`, ~`(6)`,
  "Property controls", "No", "No", "Yes", "No", "No", "Yes",
  "LSOA FE", "No", "Yes", "Yes", "No", "Yes", "Yes",
  "Quarter FE", "No", "Yes", "Yes", "No", "Yes", "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Set option to avoid siunitx wrapping
# options("modelsummary_format_numeric_latex" = "plain")

# Notes
custom_notes <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure, public attention, and property values. The sample includes properties with spatially eligible canonical Stage-2 exposure within 250m in England over 2021--2024 (sales) / 2021--2023 (rentals). The dependent variable is the log transaction price for sales (columns 1--3) or the log weekly asking rent for rentals (columns 4--6). Exposure is the canonical Stage-2 study-period weekly spill count (12/24 count) across all Site Groups within 250m. Annual evidence gaps, including the standard annual_returns_na_then_absent reporting-gap exclusion, are encoded as missing exposure and excluded; true spatially eligible zero-exposure observations are retained. Post is an indicator equal to one for transactions occurring on or after August 2022 (the peak month for Google Trends searches and news coverage of sewage spills). Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Standard errors clustered at the LSOA level are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)


# Structure models into panels
panels <- list(
  "House Sales" = list(
    "(1)" = model_sale_1,
    "(2)" = model_sale_2,
    "(3)" = model_sale_3
  ),
  "House Rentals" = list(
    "(4)" = model_rent_1,
    "(5)" = model_rent_2,
    "(6)" = model_rent_3
  )
)

# Generate table
table_latex <- modelsummary::modelsummary(
  panels,
  shape = "cbind",
  output = "latex",
  escape = FALSE,
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills on Property Values: Pre/Post Google Trends Peak"
)

table_latex <- fit_tblr_latex(
  table_latex,
  label = "tbl:did-trends-full",
  notes = custom_notes,
  width = "0.9\\linewidth"
)

# Write to file
output_path <- file.path(output_dir, "did_trends_full.tex")
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))
cat("\nScript completed successfully.\n")
