# ==============================================================================
# Repeat Sales & Rentals Regression Analysis (Palmquist 1982 Specification)
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          repeat sales/rentals methodology following Palmquist (1982).
#          - Dependent variable: Depreciation-adjusted log price/rent ratio
#          - Treatment: Change in spill exposure between consecutive transactions
#          - Standard errors: Conley (1999) spatial SEs
#          Sample: Repeat sales/rentals within 250m of spill sites.
#
# Reference: Palmquist, R.B. (1982). "Measuring Environmental Effects on
#            Property Values without Hedonic Regressions." Journal of Urban
#            Economics, 11(3), 333-347.
#
# Author: Jacopo Olivieri
# Date: 2025-12-26
#
# Inputs:
#   - data/processed/repeated_transactions/repeated_sales.parquet - Repeat sales
#   - data/processed/repeated_transactions/repeated_rentals.parquet - Repeat rentals
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/spill_house_lookup.parquet - House-spill distance lookup
#   - data/processed/zoopla/spill_rental_lookup.parquet - Rental-spill lookup
#   - data/processed/agg_spill_stats/agg_spill_qtr.parquet - Quarterly spills
#
# Outputs:
#   - output/tables/repeat_sales_palmquist.tex - LaTeX regression table
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
BASE_YEAR <- 2021
SPILL_WINDOW <- 4L  # Number of quarters for rolling spill window (set to 2 or 4)
WARMUP_QUARTERS <- SPILL_WINDOW  # Quarters to drop due to rolling window
DEPRECIATION_RATE <- 0.01  # 1% annual depreciation rate (UK literature)
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
  "data.table",
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

# 4.1 Load repeat sales with transaction details -------------------------------
cat("Loading repeat sales data...\n")

repeat_ids <- import(

  here::here("data", "processed", "repeated_transactions", "repeated_sales.parquet"),
  trust = TRUE
)

house_prices <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  select(house_id, transaction_id, price, qtr_id, latitude, longitude)

# Join and filter to repeat sales only (repeat_id appears 2+ times)
repeat_sales <- repeat_ids |>
  inner_join(house_prices, by = "house_id") |>
  group_by(repeat_id) |>
  filter(n() > 1) |>
  ungroup()

cat(sprintf("  Found %d transactions from %d repeat-sale properties\n",
            nrow(repeat_sales), n_distinct(repeat_sales$repeat_id)))

# 4.2 Load repeat rentals with transaction details -----------------------------
cat("Loading repeat rentals data...\n")

repeat_rental_ids <- import(
  here::here("data", "processed", "repeated_transactions", "repeated_rentals.parquet"),
  trust = TRUE
)

rental_prices <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
) |>
  select(rental_id, listing_price, qtr_id, latitude, longitude)

# Join and filter to repeat rentals only (repeat_id appears 2+ times)
repeat_rentals <- repeat_rental_ids |>
  inner_join(rental_prices, by = "rental_id") |>
  group_by(repeat_id) |>
  filter(n() > 1) |>
  ungroup()

cat(sprintf("  Found %d transactions from %d repeat-rental properties\n",
            nrow(repeat_rentals), n_distinct(repeat_rentals$repeat_id)))

# 4.3 Link to nearby spill sites (250m) ----------------------------------------
cat("Loading spill-house lookup...\n")

spill_lookup <- import(
  here::here("data", "processed", "spill_house_lookup.parquet"),
  trust = TRUE
) |>
  filter(distance_m <= RAD) |>
  select(house_id, site_id, distance_m)

cat(sprintf("  Found %d house-site pairs within %dm\n", nrow(spill_lookup), RAD))

cat("Loading spill-rental lookup...\n")

spill_rental_lookup <- import(
  here::here("data", "processed", "zoopla", "spill_rental_lookup.parquet"),
  trust = TRUE
) |>
  filter(distance_m <= RAD) |>
  select(rental_id, site_id, distance_m)

cat(sprintf("  Found %d rental-site pairs within %dm\n", nrow(spill_rental_lookup), RAD))

# 4.4 Compute 4-quarter rolling spill exposure ---------------------------------
cat("Computing 4-quarter rolling spill exposure...\n")

agg_spill <- import(
  here::here("data", "processed", "agg_spill_stats", "agg_spill_qtr.parquet"),
  trust = TRUE
) |>
  select(site_id, qtr_id, spill_count_qt, spill_hrs_qt)

# Convert to data.table for frollsum
agg_spill_dt <- as.data.table(agg_spill)
setorder(agg_spill_dt, site_id, qtr_id)

# Compute rolling sum, then lag by 1 (previous quarters)
agg_spill_dt[, `:=`(
  spill_count_roll_raw = frollsum(spill_count_qt, n = SPILL_WINDOW, align = "right"),
  spill_hrs_roll_raw = frollsum(spill_hrs_qt, n = SPILL_WINDOW, align = "right")
), by = site_id]

agg_spill_dt[, `:=`(
  spill_count_roll = shift(spill_count_roll_raw, n = 1, type = "lag"),
  spill_hrs_roll = shift(spill_hrs_roll_raw, n = 1, type = "lag")
), by = site_id]

# Keep only necessary columns
agg_spill_clean <- agg_spill_dt[, .(site_id, qtr_id, spill_count_roll, spill_hrs_roll)]

cat(sprintf("  Computed rolling exposure for %d site-quarter observations\n",
            nrow(agg_spill_clean)))

# 1.4 Join and aggregate to house-transaction level ----------------------------
cat("Building panel dataset...\n")

# First, filter repeat_sales to houses within 250m of any site
houses_near_sites <- repeat_sales |>
  semi_join(spill_lookup, by = "house_id")

cat(sprintf("  %d repeat-sale transactions are within %dm of spill sites\n",
            nrow(houses_near_sites), RAD))

# Join with spill lookup and aggregated spills
dat_panel <- houses_near_sites |>
  inner_join(spill_lookup, by = "house_id", relationship = "many-to-many") |>
  inner_join(agg_spill_clean, by = c("site_id", "qtr_id")) |>
  group_by(house_id, transaction_id, repeat_id, qtr_id, price, latitude, longitude) |>
  summarise(
    spill_count_roll = sum(spill_count_roll, na.rm = TRUE),
    spill_hrs_roll = sum(spill_hrs_roll, na.rm = TRUE),
    min_dist_m = min(distance_m),
    n_sites = n_distinct(site_id),
    .groups = "drop"
  ) |>
  # Filter out warmup quarters (incomplete rolling window)
  filter(qtr_id > WARMUP_QUARTERS)

cat(sprintf("  Final panel: %d transactions from %d properties\n",
            nrow(dat_panel), n_distinct(dat_panel$repeat_id)))

# 4.5 Build rental panel -------------------------------------------------------
cat("Building rental panel dataset...\n")

# Filter repeat_rentals to rentals within 250m of any site
rentals_near_sites <- repeat_rentals |>
  semi_join(spill_rental_lookup, by = "rental_id")

cat(sprintf("  %d repeat-rental transactions are within %dm of spill sites\n",
            nrow(rentals_near_sites), RAD))

# Join with spill lookup and aggregated spills
dat_panel_rental <- rentals_near_sites |>
  inner_join(spill_rental_lookup, by = "rental_id", relationship = "many-to-many") |>
  inner_join(agg_spill_clean, by = c("site_id", "qtr_id")) |>
  group_by(rental_id, repeat_id, qtr_id, listing_price, latitude, longitude) |>
  summarise(
    spill_count_roll = sum(spill_count_roll, na.rm = TRUE),
    spill_hrs_roll = sum(spill_hrs_roll, na.rm = TRUE),
    min_dist_m = min(distance_m),
    n_sites = n_distinct(site_id),
    .groups = "drop"
  ) |>
  # Filter out warmup quarters (incomplete rolling window)
  filter(qtr_id > WARMUP_QUARTERS)

cat(sprintf("  Final rental panel: %d transactions from %d properties\n",
            nrow(dat_panel_rental), n_distinct(dat_panel_rental$repeat_id)))

# ==============================================================================
# 4.6 Build Consecutive Sale Pairs (Palmquist 1982)
# ==============================================================================
cat("\nBuilding consecutive sale pairs...\n")

# Order by property and time, compute consecutive pairs
repeat_pairs <- dat_panel |>
  arrange(repeat_id, qtr_id) |>
  group_by(repeat_id) |>
  mutate(
    # Lagged values from previous sale
    lag_price = lag(price),
    lag_qtr_id = lag(qtr_id),
    lag_spill_count_roll = lag(spill_count_roll),
    lag_spill_hrs_roll = lag(spill_hrs_roll)
  ) |>
  filter(!is.na(lag_price)) |>  # Keep only pairs (drops first sale)
  ungroup()

cat(sprintf("  Created %d consecutive sale pairs from %d properties\n",
            nrow(repeat_pairs), n_distinct(repeat_pairs$repeat_id)))

# ==============================================================================
# 4.6 Compute Depreciation-Adjusted Log Price Ratio
# ==============================================================================
# From Palmquist Eq. (4): r = ln(P_t'/P_t) + δ(A_t' - A_t)
# where δ is the depreciation rate and A is age

repeat_pairs <- repeat_pairs |>
  mutate(
    # Time between sales in years
    years_between = (qtr_id - lag_qtr_id) / 4,

    # Log price ratio
    log_price_ratio = log(price) - log(lag_price),

    # Depreciation-adjusted ratio (Palmquist's r)
    r = log_price_ratio + DEPRECIATION_RATE * years_between
  )

# ==============================================================================
# 4.7 Compute First-Differenced Treatment (Change in Spill Exposure)
# ==============================================================================
# From Palmquist Eq. (5): Treatment is N_itt' = (N_t' - N_t)

repeat_pairs <- repeat_pairs |>
  mutate(
    delta_spill_count = spill_count_roll - lag_spill_count_roll,
    delta_spill_hrs = spill_hrs_roll - lag_spill_hrs_roll
  )

# Summary statistics
cat("\nSummary of treatment variables (changes in spill exposure):\n")
cat(sprintf("  Delta spill count: mean=%.2f, sd=%.2f, min=%d, max=%d\n",
            mean(repeat_pairs$delta_spill_count, na.rm = TRUE),
            sd(repeat_pairs$delta_spill_count, na.rm = TRUE),
            min(repeat_pairs$delta_spill_count, na.rm = TRUE),
            max(repeat_pairs$delta_spill_count, na.rm = TRUE)))
cat(sprintf("  Delta spill hours: mean=%.2f, sd=%.2f, min=%.0f, max=%.0f\n",
            mean(repeat_pairs$delta_spill_hrs, na.rm = TRUE),
            sd(repeat_pairs$delta_spill_hrs, na.rm = TRUE),
            min(repeat_pairs$delta_spill_hrs, na.rm = TRUE),
            max(repeat_pairs$delta_spill_hrs, na.rm = TRUE)))
cat(sprintf("  Years between sales: mean=%.2f, sd=%.2f\n",
            mean(repeat_pairs$years_between, na.rm = TRUE),
            sd(repeat_pairs$years_between, na.rm = TRUE)))

# ==============================================================================
# 4.8 Build Consecutive Rental Pairs (Palmquist 1982)
# ==============================================================================
cat("\nBuilding consecutive rental pairs...\n")

# Order by property and time, compute consecutive pairs
repeat_rental_pairs <- dat_panel_rental |>
  arrange(repeat_id, qtr_id) |>
  group_by(repeat_id) |>
  mutate(
    # Lagged values from previous rental
    lag_price = lag(listing_price),
    lag_qtr_id = lag(qtr_id),
    lag_spill_count_roll = lag(spill_count_roll),
    lag_spill_hrs_roll = lag(spill_hrs_roll)
  ) |>
  filter(!is.na(lag_price)) |>  # Keep only pairs (drops first rental)
  ungroup()

cat(sprintf("  Created %d consecutive rental pairs from %d properties\n",
            nrow(repeat_rental_pairs), n_distinct(repeat_rental_pairs$repeat_id)))

# Compute depreciation-adjusted log rent ratio
repeat_rental_pairs <- repeat_rental_pairs |>
  mutate(
    # Time between rentals in years
    years_between = (qtr_id - lag_qtr_id) / 4,

    # Log rent ratio
    log_price_ratio = log(listing_price) - log(lag_price),

    # Depreciation-adjusted ratio (Palmquist's r)
    r = log_price_ratio + DEPRECIATION_RATE * years_between
  )

# Compute first-differenced treatment
repeat_rental_pairs <- repeat_rental_pairs |>
  mutate(
    delta_spill_count = spill_count_roll - lag_spill_count_roll,
    delta_spill_hrs = spill_hrs_roll - lag_spill_hrs_roll
  )

# Summary statistics for rentals
cat("\nSummary of rental treatment variables (changes in spill exposure):\n")
cat(sprintf("  Delta spill count: mean=%.2f, sd=%.2f, min=%d, max=%d\n",
            mean(repeat_rental_pairs$delta_spill_count, na.rm = TRUE),
            sd(repeat_rental_pairs$delta_spill_count, na.rm = TRUE),
            min(repeat_rental_pairs$delta_spill_count, na.rm = TRUE),
            max(repeat_rental_pairs$delta_spill_count, na.rm = TRUE)))
cat(sprintf("  Delta spill hours: mean=%.2f, sd=%.2f, min=%.0f, max=%.0f\n",
            mean(repeat_rental_pairs$delta_spill_hrs, na.rm = TRUE),
            sd(repeat_rental_pairs$delta_spill_hrs, na.rm = TRUE),
            min(repeat_rental_pairs$delta_spill_hrs, na.rm = TRUE),
            max(repeat_rental_pairs$delta_spill_hrs, na.rm = TRUE)))
cat(sprintf("  Years between rentals: mean=%.2f, sd=%.2f\n",
            mean(repeat_rental_pairs$years_between, na.rm = TRUE),
            sd(repeat_rental_pairs$years_between, na.rm = TRUE)))

# ==============================================================================
# Part 2: Regressions (Palmquist 1982 Specification)
# ==============================================================================
cat("\nEstimating Palmquist (1982) regression models...\n")

# ==============================================================================
# Construct BMN Time Dummies (Palmquist 1982, Eq. 5-6)
# ==============================================================================
# Each row has -1 for initial sale quarter, +1 for final sale quarter, 0 elsewhere
# This is the Bailey-Muth-Nourse approach used by Palmquist

# Ensure numeric quarter IDs
repeat_pairs <- repeat_pairs |>
  mutate(
    qtr_id_num = as.integer(qtr_id),
    lag_qtr_id_num = as.integer(lag_qtr_id)
  )

# Get all unique quarters and set reference (earliest quarter omitted)
all_qtrs <- sort(unique(c(repeat_pairs$qtr_id_num, repeat_pairs$lag_qtr_id_num)))
ref_qtr <- all_qtrs[1]
cat(sprintf("  Reference quarter (omitted): %d\n", ref_qtr))
cat(sprintf("  Total time dummies: %d\n", length(all_qtrs) - 1))

# Create BMN time dummy columns (+1 for final sale, -1 for initial sale)
for (q in all_qtrs[-1]) {
  col_name <- paste0("time_", q)
  repeat_pairs[[col_name]] <- case_when(
    repeat_pairs$qtr_id_num == q ~ 1L,       # +1 for final sale in this quarter
    repeat_pairs$lag_qtr_id_num == q ~ -1L,  # -1 for initial sale in this quarter
    TRUE ~ 0L
  )
}

# Build formula with time dummies
time_vars <- paste0("time_", all_qtrs[-1])
time_formula <- paste(time_vars, collapse = " + ")

# Model 1: Spill count with BMN time dummies
# Note: conley() auto-detects lat/lon from columns named latitude/longitude
model_count <- fixest::feols(
  as.formula(paste("r ~ delta_spill_count +", time_formula)),
  data = repeat_pairs,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)

# Model 2: Spill hours with BMN time dummies
model_hrs <- fixest::feols(
  as.formula(paste("r ~ delta_spill_hrs +", time_formula)),
  data = repeat_pairs,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)

cat("  Sales models estimated successfully\n")
cat(sprintf("  Using Conley SEs with %.0fm cutoff\n", CONLEY_CUTOFF * 1000))

# ==============================================================================
# Construct BMN Time Dummies for Rentals
# ==============================================================================
cat("\nConstructing BMN time dummies for rentals...\n")

# Ensure numeric quarter IDs
repeat_rental_pairs <- repeat_rental_pairs |>
  mutate(
    qtr_id_num = as.integer(qtr_id),
    lag_qtr_id_num = as.integer(lag_qtr_id)
  )

# Get all unique quarters for rentals and set reference
all_qtrs_rental <- sort(unique(c(repeat_rental_pairs$qtr_id_num, repeat_rental_pairs$lag_qtr_id_num)))
ref_qtr_rental <- all_qtrs_rental[1]
cat(sprintf("  Reference quarter for rentals (omitted): %d\n", ref_qtr_rental))
cat(sprintf("  Total time dummies for rentals: %d\n", length(all_qtrs_rental) - 1))

# Create BMN time dummy columns for rentals
for (q in all_qtrs_rental[-1]) {
  col_name <- paste0("time_", q)
  repeat_rental_pairs[[col_name]] <- case_when(
    repeat_rental_pairs$qtr_id_num == q ~ 1L,
    repeat_rental_pairs$lag_qtr_id_num == q ~ -1L,
    TRUE ~ 0L
  )
}

# Build formula with time dummies for rentals
time_vars_rental <- paste0("time_", all_qtrs_rental[-1])
time_formula_rental <- paste(time_vars_rental, collapse = " + ")

# Model 3: Rental spill count with BMN time dummies
model_rent_count <- fixest::feols(
  as.formula(paste("r ~ delta_spill_count +", time_formula_rental)),
  data = repeat_rental_pairs,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)

# Model 4: Rental spill hours with BMN time dummies
model_rent_hrs <- fixest::feols(
  as.formula(paste("r ~ delta_spill_hrs +", time_formula_rental)),
  data = repeat_rental_pairs,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)

cat("  Rental models estimated successfully\n")

# ==============================================================================
# Part 3: Export Tables
# ==============================================================================
cat("\nExporting regression table...\n")

# Coefficient labels (dynamic based on SPILL_WINDOW)
coef_labels <- c(
  "delta_spill_count" = sprintf("$\\Delta$ Spill count (%dQ)", SPILL_WINDOW),
  "delta_spill_hrs" = sprintf("$\\Delta$ Spill hours (%dQ)", SPILL_WINDOW)
)

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw, ~clean, ~fmt,
  "nobs", "Observations", 0,
  "adj.r.squared", "Adj. R-squared", 3
)

# Add rows for fixed effects and parameters
add_rows <- tibble::tribble(
  ~term, ~`(1)`, ~`(2)`, ~`(3)`, ~`(4)`,
  "Time FE (BMN)", "Yes", "Yes", "Yes", "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Set option to avoid siunitx wrapping
options("modelsummary_format_numeric_latex" = "plain")

# Notes text (dynamic based on SPILL_WINDOW)
notes_text <- paste(
  "Dependent variable is the depreciation-adjusted log price/rent ratio between",
  "consecutive transactions of the same property (Palmquist, 1982).",
  "Sample: repeat sales/rentals within 250m of sewage spill sites.",
  sprintf("Treatment: change in cumulative %d-quarter spill exposure between transactions.", SPILL_WINDOW),
  "Time FE: Bailey-Muth-Nourse dummies (+1 final, -1 initial transaction).",
  sprintf("Conley (1999) spatial SEs (%.0fm cutoff) in parentheses.", CONLEY_CUTOFF * 1000),
  "Depreciation rate: 1 percent annually."
)

# Structure models into panels (like hedonic_daily_avg.R)
panels <- list(
  "Sales" = list(
    "(1)" = model_count,
    "(2)" = model_hrs
  ),
  "Rentals" = list(
    "(3)" = model_rent_count,
    "(4)" = model_rent_hrs
  )
)

# Generate table
table_latex <- modelsummary::modelsummary(
  panels,
  shape = "cbind",
  output = "latex",
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 5,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Effect of Sewage Spills on Property Values: Palmquist (1982) Repeat Transactions"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:repeat-transactions-palmquist},",
  table_latex
)

# Write to file (dynamic filename based on SPILL_WINDOW)
output_path <- file.path(output_dir, sprintf("repeat_sales_palmquist_%dq.tex", SPILL_WINDOW))
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))
cat("\nScript completed successfully.\n")
