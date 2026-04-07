# ==============================================================================
# News/Information DiD Analysis (Cumulative Article Count) - Prior to Sale/Rental
# ==============================================================================
#
# Purpose: Estimate whether cumulative media coverage amplifies the capitalization
#          of sewage spill exposure into house prices. Uses cumulative LexisNexis
#          article counts (from Jan 2021 to transaction month) as a continuous
#          measure of public attention, interacted with spill exposure.
#
# Author: Jacopo Olivieri
# Date: 2026-01-09
#
# Inputs:
#   - data/processed/lexis_nexis/search1_monthly.parquet - Monthly article counts
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#
# Outputs:
#   - output/tables/did_articles_prior.tex - LaTeX regression table
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

# 4.1 Load LexisNexis article counts and compute cumulative -----------------------
cat("Loading LexisNexis article counts...\n")

articles <- arrow::read_parquet(
  here::here("data", "processed", "lexis_nexis", "search1_monthly.parquet")
) |>
  filter(month_id >= 1, month_id <= 36) |>  # Jan 2021 - Dec 2023
  arrange(month_id) |>
  mutate(
    cumulative_articles = cumsum(article_count),
    log_cumulative_articles = log(cumulative_articles)
  )

cat(sprintf("  Article counts: %d months\n", nrow(articles)))
cat(sprintf("  Cumulative range: %d to %d articles\n",
            min(articles$cumulative_articles),
            max(articles$cumulative_articles)))
cat(sprintf("  Log cumulative range: %.2f to %.2f\n",
            min(articles$log_cumulative_articles),
            max(articles$log_cumulative_articles)))

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
  inner_join(
    articles |> select(month_id, cumulative_articles, log_cumulative_articles),
    by = "month_id"
  ) |>
  mutate(log_price = log(price.y)) |>
  filter(
    !is.na(spill_count_daily_avg),
    is.finite(log_cumulative_articles),
    !is.na(lsoa),
    !is.na(month_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration),
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
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat$spill_count_daily_avg, na.rm = TRUE),
            sd(dat$spill_count_daily_avg, na.rm = TRUE),
            min(dat$spill_count_daily_avg, na.rm = TRUE),
            max(dat$spill_count_daily_avg, na.rm = TRUE)))
cat(sprintf("  Cumulative articles: mean=%.1f, sd=%.1f, min=%d, max=%d\n",
            mean(dat$cumulative_articles, na.rm = TRUE),
            sd(dat$cumulative_articles, na.rm = TRUE),
            min(dat$cumulative_articles, na.rm = TRUE),
            max(dat$cumulative_articles, na.rm = TRUE)))
cat(sprintf("  Log cumulative articles: mean=%.2f, sd=%.2f, min=%.2f, max=%.2f\n",
            mean(dat$log_cumulative_articles, na.rm = TRUE),
            sd(dat$log_cumulative_articles, na.rm = TRUE),
            min(dat$log_cumulative_articles, na.rm = TRUE),
            max(dat$log_cumulative_articles, na.rm = TRUE)))

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
  inner_join(
    articles |> select(month_id, cumulative_articles, log_cumulative_articles),
    by = "month_id"
  ) |>
  mutate(log_price = log(listing_price.y)) |>
  filter(
    !is.na(spill_count_daily_avg),
    is.finite(log_cumulative_articles),
    !is.na(lsoa),
    !is.na(month_id),
    !is.na(latitude),
    !is.na(longitude),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms),
    is.finite(log_price)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat(sprintf("  Final rental dataset: %d transactions\n", nrow(dat_rental)))
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            sd(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            min(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            max(dat_rental$spill_count_daily_avg, na.rm = TRUE)))
cat(sprintf("  Cumulative articles: mean=%.1f, sd=%.1f, min=%d, max=%d\n",
            mean(dat_rental$cumulative_articles, na.rm = TRUE),
            sd(dat_rental$cumulative_articles, na.rm = TRUE),
            min(dat_rental$cumulative_articles, na.rm = TRUE),
            max(dat_rental$cumulative_articles, na.rm = TRUE)))
cat(sprintf("  Log cumulative articles: mean=%.2f, sd=%.2f, min=%.2f, max=%.2f\n",
            mean(dat_rental$log_cumulative_articles, na.rm = TRUE),
            sd(dat_rental$log_cumulative_articles, na.rm = TRUE),
            min(dat_rental$log_cumulative_articles, na.rm = TRUE),
            max(dat_rental$log_cumulative_articles, na.rm = TRUE)))


# ==============================================================================
# 5. Estimation
# ==============================================================================
cat("\nEstimating regression models...\n")

# 5.1 Sales models ------------------------------------------------------------

# Model 1: No controls, no FE
model_sale_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg + log_cumulative_articles +
              spill_count_daily_avg:log_cumulative_articles,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 1 (no controls, no FE) estimated\n")

# Model 1b: Property controls, no FE
model_sale_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + log_cumulative_articles +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + old_new + duration,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 1b (controls, no FE) estimated\n")

# Model 2: MSOA + Month FE only
model_sale_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles | msoa + month_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 2 (MSOA FE + month FE) estimated\n")

# Model 3: MSOA + Month FE + property controls
model_sale_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + old_new + duration | msoa + month_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 3 (MSOA FE + month FE + controls) estimated\n")

# Model 4: LSOA + Month FE only
model_sale_4 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles | lsoa + month_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 4 (LSOA FE + month FE) estimated\n")

# Model 5: LSOA + Month FE + property controls
model_sale_5 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + old_new + duration | lsoa + month_id,
  data = dat,
  vcov = ~lsoa
)
cat("  Sales Model 5 (LSOA FE + month FE + controls) estimated\n")

# 5.2 Rental models -----------------------------------------------------------

# Model 4: No controls, no FE
model_rent_1 <- fixest::feols(
  log_price ~ spill_count_daily_avg + log_cumulative_articles +
              spill_count_daily_avg:log_cumulative_articles,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 1 (no controls, no FE) estimated\n")

# Model 4b: Property controls, no FE
model_rent_1b <- fixest::feols(
  log_price ~ spill_count_daily_avg + log_cumulative_articles +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + bedrooms + bathrooms,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 1b (controls, no FE) estimated\n")

# Model 2: MSOA + Month FE only
model_rent_2 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles | msoa + month_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 2 (MSOA FE + month FE) estimated\n")

# Model 3: MSOA + Month FE + property controls
model_rent_3 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + bedrooms + bathrooms | msoa + month_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 3 (MSOA FE + month FE + controls) estimated\n")

# Model 4: LSOA + Month FE only
model_rent_4 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles | lsoa + month_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 4 (LSOA FE + month FE) estimated\n")

# Model 5: LSOA + Month FE + property controls
model_rent_5 <- fixest::feols(
  log_price ~ spill_count_daily_avg +
              spill_count_daily_avg:log_cumulative_articles +
              property_type + bedrooms + bathrooms | lsoa + month_id,
  data = dat_rental,
  vcov = ~lsoa
)
cat("  Rental Model 5 (LSOA FE + month FE + controls) estimated\n")

cat("  Using LSOA-clustered SEs\n")


# ==============================================================================
# 6. Export Table
# ==============================================================================
cat("\nExporting regression table...\n")

# Coefficient labels
coef_labels <- c(
  "spill_count_daily_avg" = "Daily spill count",
  "log_cumulative_articles" = "$\\log (\\text{Articles})$",
  "spill_count_daily_avg:log_cumulative_articles" = "{Daily spill count \\\\ $\\times$ $\\log (\\text{Articles})$}"
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
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure, public attention, and property values. The sample includes all properties within 250m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or the log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within 250m from January 2021 to the transaction date. $\\\\log (\\\\text{Articles})$ is the natural logarithm of cumulative UK news coverage of sewage spills from LexisNexis from January 2021 to the transaction month. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Standard errors clustered at the LSOA level are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
  fmt = 3,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills on Property Values: Log Cumulative Media Coverage (Prior to Transaction)"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:did-articles-prior},",
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
output_path <- file.path(output_dir, "did_articles_prior.tex")
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))
cat("\nScript completed successfully.\n")
