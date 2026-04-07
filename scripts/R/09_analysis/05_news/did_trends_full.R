# ==============================================================================
# News/Information DiD Analysis (Google Trends Peak)
# ==============================================================================
#
# Purpose: Estimate whether the capitalization of sewage spill exposure into
#          house prices changes after public attention peaks, using a pre/post
#          DiD where treatment is spill intensity (raw spill counts aggregated
#          across 2021-2023) within 250m and post starts at the Google Trends
#          peak month (inclusive).
#
# Author: Jacopo Olivieri
# Date: 2025-01-08
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx - Google Trends search data
#   - data/processed/agg_spill_stats/agg_spill_yr.parquet - Yearly spill counts
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/general_panel/sales/ - House-site distance mapping
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/zoopla/spill_rental_lookup.parquet - Rental-spill mapping
#
# Outputs:
#   - output/tables/did_trends_full.tex - LaTeX regression table
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

# 4.2 Load and aggregate spill intensity by site (2021-2023) ------------------
cat("Loading spill data...\n")

spills <- import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_yr.parquet"),
  trust = TRUE
)

spill_intensity_by_site <- spills |>
  filter(year >= 2021, year <= 2023) |>
  group_by(site_id) |>
  summarise(spill_intensity = sum(spill_count_yr, na.rm = TRUE))

cat(sprintf("  Computed spill intensity for %d sites\n", nrow(spill_intensity_by_site)))
cat(sprintf("  Spill intensity: mean=%.2f, sd=%.2f, min=%d, max=%d\n",
            mean(spill_intensity_by_site$spill_intensity, na.rm = TRUE),
            sd(spill_intensity_by_site$spill_intensity, na.rm = TRUE),
            min(spill_intensity_by_site$spill_intensity, na.rm = TRUE),
            max(spill_intensity_by_site$spill_intensity, na.rm = TRUE)))

# 4.3 Load house-site mapping (250m radius) -----------------------------------
cat("Loading house-site mapping...\n")

gen_panel_sales <- arrow::open_dataset(
  here::here("data", "processed", "general_panel", "sales")
) |>
  filter(radius == RAD) |>
  collect() |>
  distinct(house_id, site_id, distance_m)

cat(sprintf("  Found %d house-site pairs within %dm\n", nrow(gen_panel_sales), RAD))

# 4.4 Map spill intensity to houses -------------------------------------------
cat("Mapping spill intensity to houses...\n")

house_spill_intensity <- gen_panel_sales |>
  filter(!is.na(site_id)) |>
  left_join(spill_intensity_by_site, by = "site_id") |>
  group_by(house_id) |>
  summarise(spill_intensity = sum(spill_intensity, na.rm = TRUE))

cat(sprintf("  Mapped spill intensity to %d houses\n", nrow(house_spill_intensity)))

# 4.5 Load sales transactions -------------------------------------------------
cat("Loading sales transactions...\n")

sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
)

cat(sprintf("  Loaded %d transactions\n", nrow(sales)))

# 4.6 Merge and create analysis variables -------------------------------------
cat("Creating analysis dataset...\n")

dat <- sales |>
  left_join(house_spill_intensity, by = "house_id") |>
  mutate(
    spill_intensity = replace_na(spill_intensity, 0),
    log_price = log(price),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
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

# 4.7 Load rental-spill lookup ------------------------------------------------
cat("Loading rental-spill lookup...\n")

spill_rental_lookup <- import(
  here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
  trust = TRUE
) |>
  filter(distance_m <= RAD) |>
  select(rental_id, site_id, distance_m)

cat(sprintf("  Found %d rental-site pairs within %dm\n", nrow(spill_rental_lookup), RAD))

# 4.8 Map spill intensity to rentals ------------------------------------------
cat("Mapping spill intensity to rentals...\n")

rental_spill_intensity <- spill_rental_lookup |>
  filter(!is.na(site_id)) |>
  left_join(spill_intensity_by_site, by = "site_id") |>
  group_by(rental_id) |>
  summarise(spill_intensity = sum(spill_intensity, na.rm = TRUE))

cat(sprintf("  Mapped spill intensity to %d rentals\n", nrow(rental_spill_intensity)))

# 4.9 Load rental transactions ------------------------------------------------
cat("Loading rental transactions...\n")

rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
)

cat(sprintf("  Loaded %d rental transactions\n", nrow(rentals)))

# 4.10 Merge and create rental analysis variables -----------------------------
cat("Creating rental analysis dataset...\n")

dat_rental <- rentals |>
  left_join(rental_spill_intensity, by = "rental_id") |>
  mutate(
    spill_intensity = replace_na(spill_intensity, 0),
    log_price = log(listing_price),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(
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
  log_price ~ spill_intensity + post + spill_intensity:post,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 1 (no controls, no FE) estimated\n")

# Model 2: LSOA + Quarter FE only
model_sale_2 <- fixest::feols(
  log_price ~ spill_intensity + spill_intensity:post | lsoa + qtr_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 2 (FE only) estimated\n")

# Model 3: LSOA + Quarter FE + property controls
model_sale_3 <- fixest::feols(
  log_price ~ spill_intensity + spill_intensity:post +
    property_type + old_new + duration | lsoa + qtr_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 3 (FE + controls) estimated\n")

# 5.2 Rental models -----------------------------------------------------------

# Model 4: No controls, no FE
model_rent_1 <- fixest::feols(
  log_price ~ spill_intensity + post + spill_intensity:post,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 1 (no controls, no FE) estimated\n")

# Model 5: LSOA + Quarter FE only
model_rent_2 <- fixest::feols(
  log_price ~ spill_intensity + spill_intensity:post | lsoa + qtr_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 2 (FE only) estimated\n")

# Model 6: LSOA + Quarter FE + property controls
model_rent_3 <- fixest::feols(
  log_price ~ spill_intensity + spill_intensity:post +
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
  "spill_intensity" = "Spill intensity",
  "post" = "Post",
  "spill_intensity:post" = "{Spill intensity \\\\ $\\times$ Post}"
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
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure, public attention, and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--3) or the log weekly asking rent for rentals (columns 4--6). Spill intensity is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m from January 2021 up to the transaction date. Post is an indicator equal to one for transactions occurring on or after August 2022 (the peak month for Google Trends searches and news coverage of sewage spills). Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Standard errors clustered at the LSOA level are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
  fmt = 3,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills on Property Values: Pre/Post Google Trends Peak"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:did-trends-full},",
  table_latex
)

# Add colsep and font size for tighter column spacing
table_latex <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1width=0.9\\\\linewidth,\ncolsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
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
output_path <- file.path(output_dir, "did_trends_full.tex")
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))
cat("\nScript completed successfully.\n")
