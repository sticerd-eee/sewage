# ==============================================================================
# News/Information DiD Analysis (Google Trends Peak) - Prior to Sale/Rental
# ==============================================================================
#
# Purpose: Estimate whether the capitalization of sewage spill exposure into
#          house prices changes after public attention peaks, using a pre/post
#          DiD where treatment is daily average spill count (aggregated from
#          Jan 2021 to transaction date) within 250m and post starts at the
#          Google Trends peak month (inclusive).
#
# Author: Jacopo Olivieri
# Date: 2025-01-08
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx - Google Trends search data
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#
# Outputs:
#   - output/tables/did_trends_prior.tex - LaTeX regression table
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
CONLEY_CUTOFF <- 0.5  # Conley SE cutoff in km (500m)


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
  filter(Year >= 2021, Year <= 2023)

# Find peak month (earliest if ties)
peak_row <- google_trends |>
  slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)

# Convert YYYY-MM to month_id (Jan 2021 = 1)
peak_year <- peak_row$Year
peak_month <- as.integer(substr(peak_row$Date, 6, 7))
PEAK_MONTH_ID <- (peak_year - 2021) * 12 + peak_month

cat(sprintf("  Google Trends peak: %s (month_id = %d)\n", peak_row$Date, PEAK_MONTH_ID))

# 4.2 Load cross-section sales data (prior to sale) ---------------------------
cat("Loading cross-section sales data...\n")

dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
) |>
  filter(radius == RAD) |>
  filter(n_spill_sites > 0) |>
  collect()

cat(sprintf("  Found %d sales records within %dm\n", nrow(dat_cs_sales), RAD))

# 4.3 Load house price data for property characteristics ----------------------
cat("Loading house price data...\n")

sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  mutate(
    property_type = forcats::as_factor(property_type),
    old_new = forcats::as_factor(old_new),
    duration = forcats::as_factor(duration)
  )

cat(sprintf("  Loaded %d transactions\n", nrow(sales)))

# 4.4 Merge and create sales analysis variables -------------------------------
cat("Creating sales analysis dataset...\n")

dat <- dat_cs_sales |>
  inner_join(sales, by = "house_id") |>
  mutate(
    log_price = log(price.y),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(month_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration),
    !is.na(post),
    is.finite(log_price)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

cat(sprintf("  Final sales dataset: %d transactions\n", nrow(dat)))
cat(sprintf("  Pre-period (month_id < %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat$post == 0)))
cat(sprintf("  Post-period (month_id >= %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat$post == 1)))
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat$spill_count_daily_avg, na.rm = TRUE),
            sd(dat$spill_count_daily_avg, na.rm = TRUE),
            min(dat$spill_count_daily_avg, na.rm = TRUE),
            max(dat$spill_count_daily_avg, na.rm = TRUE)))

# 4.5 Load cross-section rental data (prior to rental) ------------------------
cat("Loading cross-section rental data...\n")

dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
) |>
  filter(radius == RAD) |>
  filter(n_spill_sites > 0) |>
  collect()

cat(sprintf("  Found %d rental records within %dm\n", nrow(dat_cs_rentals), RAD))

# 4.6 Load rental data for property characteristics ---------------------------
cat("Loading rental transactions...\n")

rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
) |>
  mutate(
    property_type = forcats::as_factor(property_type)
  )

cat(sprintf("  Loaded %d rental transactions\n", nrow(rentals)))

# 4.7 Merge and create rental analysis variables ------------------------------
cat("Creating rental analysis dataset...\n")

dat_rental <- dat_cs_rentals |>
  inner_join(rentals, by = "rental_id") |>
  mutate(
    log_price = log(listing_price.y),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(month_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms),
    !is.na(post),
    is.finite(log_price)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat(sprintf("  Final rental dataset: %d transactions\n", nrow(dat_rental)))
cat(sprintf("  Pre-period (month_id < %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat_rental$post == 0)))
cat(sprintf("  Post-period (month_id >= %d): %d transactions\n",
            PEAK_MONTH_ID, sum(dat_rental$post == 1)))
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            sd(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            min(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            max(dat_rental$spill_count_daily_avg, na.rm = TRUE)))


# ==============================================================================
# 5. Estimation
# ==============================================================================
cat("\nEstimating regression models...\n")

# 5.1 Sales models ------------------------------------------------------------

# Model 1: No controls, no FE
model_sale_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg + post + spill_count_daily_avg:post,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 1 (no controls, no FE) estimated\n")

# Model 1b: Property controls, no FE
model_sale_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + post + spill_count_daily_avg:post +
    property_type + old_new + duration,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 1b (controls, no FE) estimated\n")

# Model 2: MSOA + Month FE only
model_sale_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post | msoa + month_id,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 2 (MSOA FE + month FE) estimated\n")

# Model 3: MSOA + Month FE + property controls
model_sale_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post +
    property_type + old_new + duration | msoa + month_id,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 3 (MSOA FE + month FE + controls) estimated\n")

# Model 4: LSOA + Month FE only
model_sale_4 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post | lsoa + month_id,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 4 (LSOA FE + month FE) estimated\n")

# Model 5: LSOA + Month FE + property controls
model_sale_5 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post +
    property_type + old_new + duration | lsoa + month_id,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 5 (LSOA FE + month FE + controls) estimated\n")

# 5.2 Rental models -----------------------------------------------------------

# Model 4: No controls, no FE
model_rent_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg + post + spill_count_daily_avg:post,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 1 (no controls, no FE) estimated\n")

# Model 4b: Property controls, no FE
model_rent_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + post + spill_count_daily_avg:post +
    property_type + bedrooms + bathrooms,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 1b (controls, no FE) estimated\n")

# Model 2: MSOA + Month FE only
model_rent_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post | msoa + month_id,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 2 (MSOA FE + month FE) estimated\n")

# Model 3: MSOA + Month FE + property controls
model_rent_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post +
    property_type + bedrooms + bathrooms | msoa + month_id,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 3 (MSOA FE + month FE + controls) estimated\n")

# Model 4: LSOA + Month FE only
model_rent_4 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post | lsoa + month_id,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 4 (LSOA FE + month FE) estimated\n")

# Model 5: LSOA + Month FE + property controls
model_rent_5 <- fixest::feols(
  log_price ~ spill_count_daily_avg + spill_count_daily_avg:post +
    property_type + bedrooms + bathrooms | lsoa + month_id,
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 5 (LSOA FE + month FE + controls) estimated\n")

cat(sprintf("  Using Conley SEs with %.0fm cutoff\n", CONLEY_CUTOFF * 1000))


# ==============================================================================
# 6. Export Table
# ==============================================================================
cat("\nExporting regression table...\n")

# Coefficient labels
coef_labels <- c(
  "spill_count_daily_avg" = "Daily spill count",
  "post" = "Post",
  "spill_count_daily_avg:post" = "{Daily spill count \\\\ $\\times$ Post}"
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
  ~`(7)`, ~`(8)`, ~`(9)`, ~`(10)`, ~`(11)`, ~`(12)`,
  "Property controls", "No", "Yes", "No", "Yes", "No", "Yes",
  "No", "Yes", "No", "Yes", "No", "Yes",
  "Location FE", "No", "No", "MSOA", "MSOA", "LSOA", "LSOA",
  "No", "No", "MSOA", "MSOA", "LSOA", "LSOA",
  "Time FE", "No", "No", "Month", "Month", "Month", "Month",
  "No", "No", "Month", "Month", "Month", "Month"
)
attr(add_rows, "position") <- "coef_end"

# Notes
custom_notes <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure, public attention, and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or the log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m from January 2021 to the transaction date. Post is an indicator equal to one for transactions occurring on or after August 2022 (the peak month for Google Trends searches and news coverage of sewage spills). Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Conley spatial standard errors (500m cutoff) are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Set option to avoid siunitx wrapping
# options("modelsummary_format_numeric_latex" = "plain")

# Structure models into panels
panels <- list(
  "House Sales" = list(
    "(1)" = model_sale_1,
    "(2)" = model_sale_1b,
    "(3)" = model_sale_2,
    "(4)" = model_sale_3,
    "(5)" = model_sale_4,
    "(6)" = model_sale_5
  ),
  "House Rentals" = list(
    "(7)" = model_rent_1,
    "(8)" = model_rent_1b,
    "(9)" = model_rent_2,
    "(10)" = model_rent_3,
    "(11)" = model_rent_4,
    "(12)" = model_rent_5
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
  fmt = 2,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills on Property Values: Pre/Post Google Trends Peak (Prior to Transaction)"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:did-trends-prior},",
  table_latex
)

# Add colsep and font size for tighter column spacing
table_latex <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1colsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex
)

# Replace empty note with custom notes (tabularray format)
table_latex <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes,
  table_latex
)

# Distribute available width among columns (X[] instead of Q[])
table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)
table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)

# Write to file
output_path <- file.path(output_dir, "did_trends_prior.tex")
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))
cat("\nScript completed successfully.\n")
