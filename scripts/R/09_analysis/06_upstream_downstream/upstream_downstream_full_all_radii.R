# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average Spill Measures
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous daily average measures (spill count and hours) aggregated
#          across all spill sites within radius, split by upstream/downstream
#          direction.
#          Panel A: unweighted spill counts and hours. Panel B: spill counts and
#          hours are weighted by inverse river distance from spill site to
#          property. Each panel includes OLS, Controls, FE, and FE + Controls.
#          Loop over RAD = 250, 500, 1000.
#
# Author: Alina Zeltikova
# Date: 2026-03-25
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale_house_site
#   - data/processed/house_price.parquet
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - upstream_downstream/output/18-03/river_filter/spill_house_signed.csv
#   - upstream_downstream/output/18-03/river_filter/spill_rental_signed.csv
#
# Outputs (per radius):
#   - output/tables/hedonic_count_continuous_prior_direction_{RAD}m.tex
#   - output/tables/hedonic_count_continuous_prior_direction_weighted_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction_weighted_{RAD}m.tex
#   - output/tables/hedonic_count_continuous_prior_direction_msoa_{RAD}m.tex
#   - output/tables/hedonic_count_continuous_prior_direction_weighted_msoa_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction_msoa_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction_weighted_msoa_{RAD}m.tex
#   - output/tables/hedonic_count_continuous_prior_direction_msoa_both_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_direction_msoa_both_{RAD}m.tex
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RADII          <- c(250L, 500L, 1000L)
RIVER_DIST_CAP <- 1000L

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
# 4. Load data 
# ==============================================================================

# House price data for property characteristics and LSOA ----------------------
cat("Loading house price data...\n")
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

# Rental price data for property characteristics and LSOA --------------------
cat("Loading rental data...\n")
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

# Upstream/downstream data  ----------------------------------------------------
cat("Loading upstream/downstream data...\n")

ud_sales_raw <- import(
  here::here("upstream_downstream", "output", "18-03",
             "river_filter", "spill_house_signed.csv")
)

ud_rentals_raw <- import(
  here::here("upstream_downstream", "output", "18-03",
             "river_filter", "spill_rental_signed.csv")
)

# ==============================================================================
# 5. Estimate all models for a given radius
# ==============================================================================

run_all_models <- function(RAD) {
  
  LATERAL    <- RAD
  RIVER_DIST <- max(RAD, RIVER_DIST_CAP)
  
  cat("\n========================================================\n")
  cat("  Running models for RAD =", RAD, "m\n")
  cat("    Lateral filter :", LATERAL, "m\n")
  cat("    River distance :", RIVER_DIST, "m\n")
  cat("========================================================\n")
  
  # ------------------------------------------------------------------
  # Panel A: Sales
  # ------------------------------------------------------------------
  cat("  Loading sales cross-section (radius =", RAD, ")...\n")
  
  dat_cs_sales <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "sales",
               "prior_to_sale_house_site")
  ) |>
    filter(radius == RAD) |>
    collect()
  
  upstream_downstream_sales <- ud_sales_raw |>
    mutate(
      direction    = ifelse(direction == -1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m      <= LATERAL,
      house_lateral_m      <= LATERAL,
      dist_river_m         <= RIVER_DIST,
      spill_house_euclid_m <= RAD
    ) |>
    distinct()
  
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
      lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new       = forcats::fct_drop(old_new),
      duration      = forcats::fct_drop(duration)
    )
  
  cat("    Sales observations (house-site pairs):", nrow(dat_sales_clean), "\n")
  
  # Generate exposure dataset (aggregated across all sites) ----------------
  cat("  Generating sales exposure dataset...\n")
  
  alpha <- 1
  
  dat_exposure_sales <- dat_sales_clean |>
    group_by(house_id) |>
    mutate(
      inv_dist_weight_raw = (dist_river_m + 0.01) ^(-alpha)
    ) |>
    summarise(
      upstream_count = sum(spill_count_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_count = sum(spill_count_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_count_exposure = sum(inv_dist_weight_raw * spill_count_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_count_exposure = sum(inv_dist_weight_raw * spill_count_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs = sum(spill_hrs_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs = sum(spill_hrs_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs_exposure = sum(inv_dist_weight_raw * spill_hrs_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs_exposure = sum(inv_dist_weight_raw * spill_hrs_daily_avg * (direction == 0), na.rm = TRUE)
    ) |>
    ungroup() |>
    inner_join(sales, by = "house_id") |>
    mutate(log_price = log(price))
  
  cat("    Sales observations (exposure):", nrow(dat_exposure_sales), "\n")
  
  # ------------------------------------------------------------------
  # Panel B: Rentals
  # ------------------------------------------------------------------
  cat("  Loading rental cross-section (radius =", RAD, ")...\n")
  
  dat_cs_rentals <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "rentals",
               "prior_to_rental_rental_site")
  ) |>
    filter(radius == RAD) |>
    collect()
  
  upstream_downstream_rentals <- ud_rentals_raw |>
    mutate(
      direction    = ifelse(direction == -1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m       <= LATERAL,
      rental_lateral_m      <= LATERAL,
      dist_river_m          <= RIVER_DIST,
      spill_rental_euclid_m <= RAD
    ) |>
    distinct()
  
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
      lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )
  
  cat("    Rental observations (rental-site pairs):", nrow(dat_rental_clean), "\n")
  
  # Generate exposure dataset (aggregated across all sites) ----------------
  cat("  Generating rental exposure dataset...\n")
  
  dat_exposure_rentals <- dat_rental_clean |>
    group_by(rental_id) |>
    mutate(
      inv_dist_weight_raw = (dist_river_m + 0.01) ^(-alpha)
    ) |>
    summarise(
      upstream_count = sum(spill_count_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_count = sum(spill_count_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_count_exposure = sum(inv_dist_weight_raw * spill_count_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_count_exposure = sum(inv_dist_weight_raw * spill_count_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs = sum(spill_hrs_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs = sum(spill_hrs_daily_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs_exposure = sum(inv_dist_weight_raw * spill_hrs_daily_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs_exposure = sum(inv_dist_weight_raw * spill_hrs_daily_avg * (direction == 0), na.rm = TRUE)
    ) |>
    ungroup() |>
    inner_join(rentals, by = "rental_id") |>
    mutate(log_price = log(listing_price))
  
  cat("    Rental observations (exposure):", nrow(dat_exposure_rentals), "\n")
  
  # ==================================================================
  # Estimate Models: Spill Count Daily Average
  # ==================================================================
  cat("  Estimating spill count models...\n")
  
  # Sales — unweighted, no distance control
  
  model_sales_count_1 <- feols(
    log_price ~ upstream_count + downstream_count,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_1b <- feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_2 <- fixest::feols(
    log_price ~ upstream_count + downstream_count | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_2b <- fixest::feols(
    log_price ~ upstream_count + downstream_count | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_3 <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_3b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  # Sales — weighted, distance control
  
  model_sales_count_4 <- feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_4b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_5 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_5b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_6 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_count_6b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  # Rentals — unweighted, no distance control
  
  model_rentals_count_1 <- feols(
    log_price ~ upstream_count + downstream_count,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_1b <- feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_2 <- fixest::feols(
    log_price ~ upstream_count + downstream_count | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_2b <- fixest::feols(
    log_price ~ upstream_count + downstream_count | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_3 <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_3b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  # Rentals — weighted, distance control
  
  model_rentals_count_4 <- feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_4b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_5 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_5b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_6 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_count_6b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  # ==================================================================
  # Estimate Models: Spill Hours Daily Average
  # ==================================================================
  cat("  Estimating spill hours models...\n")
  
  # Sales — unweighted, no distance control
  
  model_sales_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  # Sales — weighted, distance control
  
  model_sales_hrs_4 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_4b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_5 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_5b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_6 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration | lsoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  model_sales_hrs_6b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration | msoa,
    data = dat_exposure_sales,
    vcov = "hetero"
  )
  
  # Rentals — unweighted, no distance control
  
  model_rentals_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  # Rentals — weighted, distance control
  
  model_rentals_hrs_4 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_4b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_5 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_5b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_6 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms | lsoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  model_rentals_hrs_6b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms | msoa,
    data = dat_exposure_rentals,
    vcov = "hetero"
  )
  
  # ==================================================================
  # Export Tables
  # ==================================================================
  
  rad_label <- paste0(RAD, "m")
  
  # Shared GOF map
  gof_map <- tibble::tribble(
    ~raw           , ~clean          , ~fmt ,
    "nobs"         , "Observations"  ,    0 ,
    "adj.r.squared", "Adj. R-squared",    3
  )
  
  # Post-process modelsummary LaTeX output
  postprocess_table <- function(latex_str, label, notes) {
    # Force [H] placement
    latex_str <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", latex_str)
    # Add label
    latex_str <- sub(
      "caption=\\{([^}]*)\\},",
      paste0("caption={\\1},\nlabel={", label, "},"),
      latex_str
    )
    # Add colsep, rowsep, hspan and font size
    latex_str <- sub(
      "(\\{\\s*%% tabularray inner open\\n)",
      "\\1hspan = even,\ncolsep=2pt,\nrowsep=0.1pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
      latex_str
    )
    # Replace empty note with custom notes
    latex_str <- sub(
      "note\\{\\}=\\{\\s*\\},",
      notes,
      latex_str
    )
    # Add indentation
    latex_str <- gsub("Upstream &", "\\\\quad Upstream &", latex_str)
    latex_str <- gsub("Downstream &", "\\\\quad Downstream &", latex_str)
    latex_str
  }
  
  # Build notes string
  make_notes <- function(measure_text) {
    paste0(
      "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes properties within ",
      RAD,
      "m of a storm overflow in England, 2021--2023. Properties and spill sites are within ",
      RAD,
      "m of a river, and river distance between property and spill site is less than 1 km. The dependent variable is the log transaction price for sales or log weekly asking rent for rentals. ",
      measure_text,
      " Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
    )
  }
  
  notes_count <- make_notes(
    paste0("Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within ", RAD, "m from January 2021 to the transaction date.")
  )
  notes_hrs <- make_notes(
    paste0("Spill exposure is measured as the average total number of spill hours per day recorded across all overflows within ", RAD, "m from January 2021 to the transaction date.")
  )
  
  # ---- Shared add_rows for 8-column tables (LSOA only) ----
  add_rows_8 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" ,
    "Location FE"        , "No"   , "No"   , "LSOA" , "LSOA" , "No"   , "No"   , "LSOA" , "LSOA" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
  )
  attr(add_rows_8, "position") <- "coef_end"
  
  # ---- Shared add_rows for 12-column tables (MSOA + LSOA) ----
  add_rows_12 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"
  )
  attr(add_rows_12, "position") <- "coef_end"
  
  # ==================================================================
  # Table 1: Spill Count, Unweighted 
  # ==================================================================
  cat("  Exporting spill count unweighted table...\n")
  
  coef_labels_count <- c(
    "(Intercept)" = "Constant",
    "upstream_count" = "Upstream",
    "downstream_count" = "Downstream"
  )
  
  add_rows_count_8 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" ,
    "Daily spill count"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     ,
    "Location FE"        , "No"   , "No"   , "LSOA" , "LSOA" , "No"   , "No"   , "LSOA" , "LSOA" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
  )
  attr(add_rows_count_8, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_1,
      "(2)" = model_sales_count_1b,
      "(3)" = model_sales_count_2,
      "(4)" = model_sales_count_3
    ),
    "House Rentals" = list(
      "(5)" = model_rentals_count_1,
      "(6)" = model_rentals_count_1b,
      "(7)" = model_rentals_count_2,
      "(8)" = model_rentals_count_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_count,
    gof_map = gof_map,
    add_rows = add_rows_count_8,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Unweighted Count) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-direction-", rad_label),
    notes = notes_count
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_direction_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 2: Spill Count, Weighted 
  # ==================================================================
  cat("  Exporting spill count weighted table...\n")
  
  coef_labels_count_w <- c(
    "(Intercept)" = "Constant",
    "upstream_count_exposure" = "Upstream",
    "downstream_count_exposure" = "Downstream"
  )
  
  add_rows_count_w_8 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" ,
    "Daily spill count"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     ,
    "Location FE"        , "No"   , "No"   , "LSOA" , "LSOA" , "No"   , "No"   , "LSOA" , "LSOA" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
  )
  attr(add_rows_count_w_8, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_4,
      "(2)" = model_sales_count_4b,
      "(3)" = model_sales_count_5,
      "(4)" = model_sales_count_6
    ),
    "House Rentals" = list(
      "(5)" = model_rentals_count_4,
      "(6)" = model_rentals_count_4b,
      "(7)" = model_rentals_count_5,
      "(8)" = model_rentals_count_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_count_w,
    gof_map = gof_map,
    add_rows = add_rows_count_w_8,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Weighted Count) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-direction-weighted-", rad_label),
    notes = notes_count
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_direction_weighted_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 3: Spill Count, Unweighted + MSOA 
  # ==================================================================
  cat("  Exporting spill count unweighted + MSOA table...\n")
  
  add_rows_count_12 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Daily spill count"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""      , ""      , ""      ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"
  )
  attr(add_rows_count_12, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_1,
      "(2)" = model_sales_count_1b,
      "(3)" = model_sales_count_2b,
      "(4)" = model_sales_count_3b,
      "(5)" = model_sales_count_2,
      "(6)" = model_sales_count_3
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_count_1,
      "(8)" = model_rentals_count_1b,
      "(9)" = model_rentals_count_2b,
      "(10)" = model_rentals_count_3b,
      "(11)" = model_rentals_count_2,
      "(12)" = model_rentals_count_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_count,
    gof_map = gof_map,
    add_rows = add_rows_count_12,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Unweighted Count) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-direction-msoa-", rad_label),
    notes = notes_count
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_direction_msoa_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 4: Spill Count, Weighted + MSOA 
  # ==================================================================
  cat("  Exporting spill count weighted + MSOA table...\n")
  
  add_rows_count_w_12 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Daily spill count"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""      , ""      , ""      ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"
  )
  attr(add_rows_count_w_12, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_4,
      "(2)" = model_sales_count_4b,
      "(3)" = model_sales_count_5b,
      "(4)" = model_sales_count_6b,
      "(5)" = model_sales_count_5,
      "(6)" = model_sales_count_6
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_count_4,
      "(8)" = model_rentals_count_4b,
      "(9)" = model_rentals_count_5b,
      "(10)" = model_rentals_count_6b,
      "(11)" = model_rentals_count_5,
      "(12)" = model_rentals_count_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_count_w,
    gof_map = gof_map,
    add_rows = add_rows_count_w_12,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Weighted Count) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-direction-weighted-msoa-", rad_label),
    notes = notes_count
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_direction_weighted_msoa_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 5: Spill Hours, Unweighted 
  # ==================================================================
  cat("  Exporting spill hours unweighted table...\n")
  
  coef_labels_hrs <- c(
    "(Intercept)" = "Constant",
    "upstream_hrs" = "Upstream",
    "downstream_hrs" = "Downstream"
  )
  
  add_rows_hrs_8 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" ,
    "Daily spill hours"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     ,
    "Location FE"        , "No"   , "No"   , "LSOA" , "LSOA" , "No"   , "No"   , "LSOA" , "LSOA" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
  )
  attr(add_rows_hrs_8, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_1,
      "(2)" = model_sales_hrs_1b,
      "(3)" = model_sales_hrs_2,
      "(4)" = model_sales_hrs_3
    ),
    "House Rentals" = list(
      "(5)" = model_rentals_hrs_1,
      "(6)" = model_rentals_hrs_1b,
      "(7)" = model_rentals_hrs_2,
      "(8)" = model_rentals_hrs_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_hrs,
    gof_map = gof_map,
    add_rows = add_rows_hrs_8,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Unweighted Hours) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-direction-", rad_label),
    notes = notes_hrs
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_direction_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 6: Spill Hours, Weighted 
  # ==================================================================
  cat("  Exporting spill hours weighted table...\n")
  
  coef_labels_hrs_w <- c(
    "(Intercept)" = "Constant",
    "upstream_hrs_exposure" = "Upstream",
    "downstream_hrs_exposure" = "Downstream"
  )
  
  add_rows_hrs_w_8 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" ,
    "Daily spill hours"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     ,
    "Location FE"        , "No"   , "No"   , "LSOA" , "LSOA" , "No"   , "No"   , "LSOA" , "LSOA" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"
  )
  attr(add_rows_hrs_w_8, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_4,
      "(2)" = model_sales_hrs_4b,
      "(3)" = model_sales_hrs_5,
      "(4)" = model_sales_hrs_6
    ),
    "House Rentals" = list(
      "(5)" = model_rentals_hrs_4,
      "(6)" = model_rentals_hrs_4b,
      "(7)" = model_rentals_hrs_5,
      "(8)" = model_rentals_hrs_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_hrs_w,
    gof_map = gof_map,
    add_rows = add_rows_hrs_w_8,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Weighted Hours) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-direction-weighted-", rad_label),
    notes = notes_hrs
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_direction_weighted_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 7: Spill Hours, Unweighted + MSOA
  # ==================================================================
  cat("  Exporting spill hours unweighted + MSOA table...\n")
  
  add_rows_hrs_12 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Daily spill hours"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""      , ""      , ""      ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"
  )
  attr(add_rows_hrs_12, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_1,
      "(2)" = model_sales_hrs_1b,
      "(3)" = model_sales_hrs_2b,
      "(4)" = model_sales_hrs_3b,
      "(5)" = model_sales_hrs_2,
      "(6)" = model_sales_hrs_3
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_hrs_1,
      "(8)" = model_rentals_hrs_1b,
      "(9)" = model_rentals_hrs_2b,
      "(10)" = model_rentals_hrs_3b,
      "(11)" = model_rentals_hrs_2,
      "(12)" = model_rentals_hrs_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_hrs,
    gof_map = gof_map,
    add_rows = add_rows_hrs_12,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Unweighted Hours) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-direction-msoa-", rad_label),
    notes = notes_hrs
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_direction_msoa_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 8: Spill Hours, Weighted + MSOA
  # ==================================================================
  cat("  Exporting spill hours weighted + MSOA table...\n")
  
  add_rows_hrs_w_12 <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Daily spill hours"  , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""      , ""      , ""      ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"  ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"
  )
  attr(add_rows_hrs_w_12, "position") <- c(3, 9, 10)
  
  panels <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_4,
      "(2)" = model_sales_hrs_4b,
      "(3)" = model_sales_hrs_5b,
      "(4)" = model_sales_hrs_6b,
      "(5)" = model_sales_hrs_5,
      "(6)" = model_sales_hrs_6
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_hrs_4,
      "(8)" = model_rentals_hrs_4b,
      "(9)" = model_rentals_hrs_5b,
      "(10)" = model_rentals_hrs_6b,
      "(11)" = model_rentals_hrs_5,
      "(12)" = model_rentals_hrs_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_hrs_w,
    gof_map = gof_map,
    add_rows = add_rows_hrs_w_12,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Weighted Hours) on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-direction-weighted-msoa-", rad_label),
    notes = notes_hrs
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_direction_weighted_msoa_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==========================================================================
  # Table 9: Combined Panel A (Unweighted) + Panel B (Weighted) — Count, MSOA
  # ==========================================================================
  cat("  Exporting combined count MSOA panel table...\n")
  
  # Extract coefficients from each model
  extract_coefs <- function(model, coef_names, label_map) {
    s <- summary(model, vcov = "hetero")
    ct <- coeftable(s)
    rows <- list()
    for (cname in coef_names) {
      if (cname %in% rownames(ct)) {
        est <- ct[cname, "Estimate"]
        se  <- ct[cname, "Std. Error"]
        pv  <- ct[cname, "Pr(>|t|)"]
        stars <- ""
        if (!is.na(pv)) {
          if (pv < 0.01) stars <- "***"
          else if (pv < 0.05) stars <- "**"
          else if (pv < 0.1) stars <- "*"
        }
        rows[[label_map[cname]]] <- list(
          est = paste0("\\num{", formatC(est, format = "f", digits = 2), "}", stars),
          se  = paste0("(\\num{", formatC(se, format = "f", digits = 2), "})")
        )
      } else {
        rows[[label_map[cname]]] <- list(est = "", se = "")
      }
    }
    rows[["nobs"]] <- format(nobs(model), big.mark = "")
    rows[["adj_r2"]] <- formatC(
      fixest::r2(model, type = "ar2"),
      format = "f", digits = 3
    )
    rows
  }
  
  # Model lists for Panel A (unweighted MSOA)
  panel_a_sales <- list(
    model_sales_count_1, model_sales_count_1b,
    model_sales_count_2b, model_sales_count_3b,
    model_sales_count_2, model_sales_count_3
  )
  panel_a_rentals <- list(
    model_rentals_count_1, model_rentals_count_1b,
    model_rentals_count_2b, model_rentals_count_3b,
    model_rentals_count_2, model_rentals_count_3
  )
  
  # Model lists for Panel B (weighted MSOA)
  panel_b_sales <- list(
    model_sales_count_4, model_sales_count_4b,
    model_sales_count_5b, model_sales_count_6b,
    model_sales_count_5, model_sales_count_6
  )
  panel_b_rentals <- list(
    model_rentals_count_4, model_rentals_count_4b,
    model_rentals_count_5b, model_rentals_count_6b,
    model_rentals_count_5, model_rentals_count_6
  )
  
  coef_names_a <- c("(Intercept)", "upstream_count", "downstream_count")
  label_map_a  <- c("(Intercept)" = "Constant", "upstream_count" = "Upstream", "downstream_count" = "Downstream")
  
  coef_names_b <- c("(Intercept)", "upstream_count_exposure", "downstream_count_exposure")
  label_map_b  <- c("(Intercept)" = "Constant", "upstream_count_exposure" = "Upstream", "downstream_count_exposure" = "Downstream")
  
  all_models_a <- c(panel_a_sales, panel_a_rentals)
  all_models_b <- c(panel_b_sales, panel_b_rentals)
  
  coefs_a <- lapply(all_models_a, extract_coefs, coef_names = coef_names_a, label_map = label_map_a)
  coefs_b <- lapply(all_models_b, extract_coefs, coef_names = coef_names_b, label_map = label_map_b)
  
  # Helper to build a row of 12 cells
  make_row <- function(coefs_list, field, key) {
    vals <- sapply(coefs_list, function(x) {
      v <- x[[key]]
      if (is.list(v)) v[[field]] else if (field == "est") v else ""
    })
    paste(vals, collapse = " & ")
  }
  
  make_gof_row <- function(coefs_list, key) {
    vals <- sapply(coefs_list, function(x) {
      v <- x[[key]]
      if (is.list(v)) "" else paste0("\\num{", v, "}")
    })
    paste(vals, collapse = " & ")
  }
  
  # Location FE rows
  loc_fe    <- "No & No & Yes & Yes & No & No & No & No & Yes & Yes & No & No"
  loc_fe_l  <- "No & No & No & No & Yes & Yes & No & No & No & No & Yes & Yes"
  
  notes_count_both <- paste0(
    "note{}={\\footnotesize{\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes properties within ",
    RAD,
    "m of a storm overflow in England, 2021--2023. Properties and spill sites are within ",
    RAD,
    "m of a river, and river distance between property and spill site is less than 1 km. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per day (12/24 count) recorded across all overflows within ",
    RAD,
    "m from January 2021 to the transaction date. Panel A uses unweighted spill counts. Panel B weights spills by inverse river distance from spill site to property. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )
  
  prop_ctrl <- "No & Yes & No & Yes & No & Yes & No & Yes & No & Yes & No & Yes"
  
  combined_tex <- paste0(
    "\\begin{longtblr}[\n",
    "caption={Effect of Sewage Spills (Count) on Property Values by Direction (", rad_label, ")},\n",
    "label={tbl:hedonic-count-continuous-prior-direction-msoa-both-", rad_label, "},\n",
    notes_count_both, "\n",
    "]{\n",
    "hspan = even,\n",
    "colsep=2pt,\n",
    "rowsep=0.1pt,\n",
    "cells   = {font = \\fontsize{11pt}{12pt}\\selectfont},\n",
    "colspec={Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]},\n",
    "hline{1}={1-13}{solid, black, 0.1em},\n",
    "hline{2}={2-7,8-13}{solid, black, 0.05em},\n",
    "hline{3}={1-13}{solid, black, 0.05em},\n",
    "hline{4}={1-13}{solid, black, 0.05em},\n",
    "hline{11}={1-13}{solid, black, 0.05em},\n",
    "hline{16}={1-13}{solid, black, 0.05em},\n",
    "hline{17}={1-13}{solid, black, 0.05em},\n",
    "hline{24}={1-13}{solid, black, 0.05em},\n",
    "hline{29}={1-13}{solid, black, 0.1em},\n",
    "hline{2}={2,8}{solid, black, 0.05em, l=-0.5},\n",
    "hline{2}={7}{solid, black, 0.05em, r=-0.5},\n",
    "column{2-7,8-13}={}{halign=c},\n",
    "cell{1}{1}={}{halign=c},\n",
    "cell{1}{2}={c=6}{halign=c},\n",
    "cell{1}{8}={c=6}{halign=c},\n",
    "cell{2-29}{1}={}{halign=l},\n",
    "cell{2-29}{2}={}{halign=c},\n",
    "cell{2-29}{6}={}{halign=c},\n",
    "cell{3}{1}={c=9}{halign=l, font=\\bfseries},\n",
    "cell{16}{1}={c=9}{halign=l, font=\\bfseries},\n",
    "}\n",
    "& House Sales &  & &  &  &  & House Rentals &  &  &  &  &\\\\\n",
    "& (1) & (2) & (3) & (4) & (5) & (6) & (7) & (8) & (9) & (10) & (11) & (12) \\\\\n",
    "\\textbf{Panel A: Unweighted} &  &  &  &  &  &  &  &  &  &  &  & \\\\\n",
    "Constant & ", make_row(coefs_a, "est", "Constant"), " \\\\\n",
    "& ", make_row(coefs_a, "se", "Constant"), " \\\\\n",
    "Daily spill count &  &  &  &  &  &  &  &  &  &  &  &  \\\\\n",
    "\\quad Upstream & ", make_row(coefs_a, "est", "Upstream"), " \\\\\n",
    "& ", make_row(coefs_a, "se", "Upstream"), " \\\\\n",
    "\\quad Downstream & ", make_row(coefs_a, "est", "Downstream"), " \\\\\n",
    "& ", make_row(coefs_a, "se", "Downstream"), " \\\\\n",
    "Property controls & ", prop_ctrl, " \\\\\n",
    "MSOA FE & ", loc_fe, " \\\\\n",
    "LSOA FE & ", loc_fe_l, " \\\\\n",
    "Observations & ", make_gof_row(coefs_a, "nobs"), " \\\\\n",
    "Adj. R-squared & ", make_gof_row(coefs_a, "adj_r2"), " \\\\\n",
    "\\textbf{Panel B: Weighted by river distance}  &  &  &  &  &  &  &  &  &  &  &  &\\\\\n",
    "Constant & ", make_row(coefs_b, "est", "Constant"), " \\\\\n",
    "& ", make_row(coefs_b, "se", "Constant"), " \\\\\n",
    "Daily spill count &  &  &  &  &  &  &  &  &  &  &  &  \\\\\n",
    "\\quad Upstream & ", make_row(coefs_b, "est", "Upstream"), " \\\\\n",
    "& ", make_row(coefs_b, "se", "Upstream"), " \\\\\n",
    "\\quad Downstream & ", make_row(coefs_b, "est", "Downstream"), " \\\\\n",
    "& ", make_row(coefs_b, "se", "Downstream"), " \\\\\n",
    "Property controls & ", prop_ctrl, " \\\\\n",
    "MSOA FE & ", loc_fe, " \\\\\n",
    "LSOA FE & ", loc_fe_l, "\\\\\n",
    "Observations & ", make_gof_row(coefs_b, "nobs"), " \\\\\n",
    "Adj. R-squared & ", make_gof_row(coefs_b, "adj_r2"), " \\\\\n",
    "\\end{longtblr}"
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_direction_msoa_both_", rad_label, ".tex"))
  writeLines(combined_tex, out_path)
  record_export(out_path)
  
  # ===========================================================================
  # Table 10: Combined Panel A (Unweighted) + Panel B (Weighted) — Hours, MSOA
  # ===========================================================================
  cat("  Exporting combined hours MSOA panel table...\n")
  
  coef_names_a_hrs <- c("(Intercept)", "upstream_hrs", "downstream_hrs")
  label_map_a_hrs  <- c("(Intercept)" = "Constant", "upstream_hrs" = "Upstream", "downstream_hrs" = "Downstream")
  
  coef_names_b_hrs <- c("(Intercept)", "upstream_hrs_exposure", "downstream_hrs_exposure")
  label_map_b_hrs  <- c("(Intercept)" = "Constant", "upstream_hrs_exposure" = "Upstream", "downstream_hrs_exposure" = "Downstream")
  
  # Panel A models (unweighted hours, MSOA)
  panel_a_hrs_sales <- list(
    model_sales_hrs_1, model_sales_hrs_1b,
    model_sales_hrs_2b, model_sales_hrs_3b,
    model_sales_hrs_2, model_sales_hrs_3
  )
  panel_a_hrs_rentals <- list(
    model_rentals_hrs_1, model_rentals_hrs_1b,
    model_rentals_hrs_2b, model_rentals_hrs_3b,
    model_rentals_hrs_2, model_rentals_hrs_3
  )
  
  # Panel B models (weighted hours, MSOA)
  panel_b_hrs_sales <- list(
    model_sales_hrs_4, model_sales_hrs_4b,
    model_sales_hrs_5b, model_sales_hrs_6b,
    model_sales_hrs_5, model_sales_hrs_6
  )
  panel_b_hrs_rentals <- list(
    model_rentals_hrs_4, model_rentals_hrs_4b,
    model_rentals_hrs_5b, model_rentals_hrs_6b,
    model_rentals_hrs_5, model_rentals_hrs_6
  )
  
  all_models_a_hrs <- c(panel_a_hrs_sales, panel_a_hrs_rentals)
  all_models_b_hrs <- c(panel_b_hrs_sales, panel_b_hrs_rentals)
  
  coefs_a_hrs <- lapply(all_models_a_hrs, extract_coefs, coef_names = coef_names_a_hrs, label_map = label_map_a_hrs)
  coefs_b_hrs <- lapply(all_models_b_hrs, extract_coefs, coef_names = coef_names_b_hrs, label_map = label_map_b_hrs)
  
  notes_hrs_both <- paste0(
    "note{}={\\footnotesize{\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes properties within ",
    RAD,
    "m of a storm overflow in England, 2021--2023. Properties and spill sites are within ",
    RAD,
    "m of a river, and river distance between property and spill site is less than 1 km. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per day recorded across all overflows within ",
    RAD,
    "m from January 2021 to the transaction date. Panel A uses unweighted spill hours. Panel B weights spills by inverse river distance from spill site to property. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )
  
  combined_hrs_tex <- paste0(
    "\\begin{longtblr}[\n",
    "caption={Effect of Sewage Spills (Hours) on Property Values by Direction (", rad_label, ")},\n",
    "label={tbl:hedonic-hrs-continuous-prior-direction-msoa-both-", rad_label, "},\n",
    notes_hrs_both, "\n",
    "]{\n",
    "hspan = even,\n",
    "colsep=2pt,\n",
    "rowsep=0.1pt,\n",
    "cells   = {font = \\fontsize{11pt}{12pt}\\selectfont},\n",
    "colspec={Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]Q[]},\n",
    "hline{1}={1-13}{solid, black, 0.1em},\n",
    "hline{2}={2-7,8-13}{solid, black, 0.05em},\n",
    "hline{3}={1-13}{solid, black, 0.05em},\n",
    "hline{4}={1-13}{solid, black, 0.05em},\n",
    "hline{11}={1-13}{solid, black, 0.05em},\n",
    "hline{16}={1-13}{solid, black, 0.05em},\n",
    "hline{17}={1-13}{solid, black, 0.05em},\n",
    "hline{24}={1-13}{solid, black, 0.05em},\n",
    "hline{29}={1-13}{solid, black, 0.1em},\n",
    "hline{2}={2,8}{solid, black, 0.05em, l=-0.5},\n",
    "hline{2}={7}{solid, black, 0.05em, r=-0.5},\n",
    "column{2-7,8-13}={}{halign=c},\n",
    "cell{1}{1}={}{halign=c},\n",
    "cell{1}{2}={c=6}{halign=c},\n",
    "cell{1}{8}={c=6}{halign=c},\n",
    "cell{2-29}{1}={}{halign=l},\n",
    "cell{2-29}{2}={}{halign=c},\n",
    "cell{2-29}{6}={}{halign=c},\n",
    "cell{3}{1}={c=9}{halign=l, font=\\bfseries},\n",
    "cell{16}{1}={c=9}{halign=l, font=\\bfseries},\n",
    "}\n",
    "& House Sales &  & &  &  &  & House Rentals &  &  &  &  &\\\\\n",
    "& (1) & (2) & (3) & (4) & (5) & (6) & (7) & (8) & (9) & (10) & (11) & (12) \\\\\n",
    "\\textbf{Panel A: Unweighted} &  &  &  &  &  &  &  &  &  &  &  & \\\\\n",
    "Constant & ", make_row(coefs_a_hrs, "est", "Constant"), " \\\\\n",
    "& ", make_row(coefs_a_hrs, "se", "Constant"), " \\\\\n",
    "Daily spill hours &  &  &  &  &  &  &  &  &  &  &  &  \\\\\n",
    "\\quad Upstream & ", make_row(coefs_a_hrs, "est", "Upstream"), " \\\\\n",
    "& ", make_row(coefs_a_hrs, "se", "Upstream"), " \\\\\n",
    "\\quad Downstream & ", make_row(coefs_a_hrs, "est", "Downstream"), " \\\\\n",
    "& ", make_row(coefs_a_hrs, "se", "Downstream"), " \\\\\n",
    "Property controls & ", prop_ctrl, " \\\\\n",
    "MSOA FE & ", loc_fe, " \\\\\n",
    "LSOA FE & ", loc_fe_l, " \\\\\n",
    "Observations & ", make_gof_row(coefs_a_hrs, "nobs"), " \\\\\n",
    "Adj. R-squared & ", make_gof_row(coefs_a_hrs, "adj_r2"), " \\\\\n",
    "\\textbf{Panel B: Weighted by river distance}  &  &  &  &  &  &  &  &  &  &  &  &\\\\\n",
    "Constant & ", make_row(coefs_b_hrs, "est", "Constant"), " \\\\\n",
    "& ", make_row(coefs_b_hrs, "se", "Constant"), " \\\\\n",
    "Daily spill hours &  &  &  &  &  &  &  &  &  &  &  &  \\\\\n",
    "\\quad Upstream & ", make_row(coefs_b_hrs, "est", "Upstream"), " \\\\\n",
    "& ", make_row(coefs_b_hrs, "se", "Upstream"), " \\\\\n",
    "\\quad Downstream & ", make_row(coefs_b_hrs, "est", "Downstream"), " \\\\\n",
    "& ", make_row(coefs_b_hrs, "se", "Downstream"), " \\\\\n",
    "Property controls & ", prop_ctrl, " \\\\\n",
    "MSOA FE & ", loc_fe, " \\\\\n",
    "LSOA FE & ", loc_fe_l, "\\\\\n",
    "Observations & ", make_gof_row(coefs_b_hrs, "nobs"), " \\\\\n",
    "Adj. R-squared & ", make_gof_row(coefs_b_hrs, "adj_r2"), " \\\\\n",
    "\\end{longtblr}"
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_direction_msoa_both_", rad_label, ".tex"))
  writeLines(combined_hrs_tex, out_path)
  record_export(out_path)
  
  cat("  Done for RAD =", RAD, "m\n")
}

# ==============================================================================
# 6. Run the loop
# ==============================================================================

for (r in RADII) {
  run_all_models(r)
}

# ==============================================================================
# 7. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (file in exported_files) {
  cat("  - ", file, "\n", sep = "")
}