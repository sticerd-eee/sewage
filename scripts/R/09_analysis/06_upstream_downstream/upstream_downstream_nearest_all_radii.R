# ==============================================================================
# Cross-Sectional Regression Analysis: Daily Average Spill Measures
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous daily average measures (spill count and hours) from the
#          nearest spill site, incorporating upstream/downstream direction.
#          Models are estimated with and without river distance controls.
#          Each set includes OLS, Controls, MSOA FE, MSOA FE + Controls,
#          LSOA FE, and LSOA FE + Controls. Limit sample to spill sites 
#          within RAD meters of a property. Loop over RAD = 250, 500, 1000.
# 
# Author: Alina Zeltikova
# Date: 2026-03-20
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
#   - output/tables/hedonic_count_continuous_prior_nearest_site_{RAD}m.tex
#   - output/tables/hedonic_count_continuous_prior_nearest_site_distance_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_nearest_site_{RAD}m.tex
#   - output/tables/hedonic_hrs_continuous_prior_nearest_site_distance_{RAD}m.tex
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
# 4. Load data that does NOT depend on radius
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
  
  dat_nearest_sales <- dat_sales_clean |>
    arrange(house_id, distance_m) |>
    group_by(house_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    mutate(log_price = log(price))
  
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
  
  dat_nearest_rentals <- dat_rental_clean |>
    arrange(rental_id, distance_m) |>
    group_by(rental_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    mutate(log_price = log(listing_price))
  
  # ------------------------------------------------------------------
  # Estimate Models: Spill Count Daily Average, Nearest Spill Site
  # ------------------------------------------------------------------
  cat("  Estimating spill count models...\n")
  
  # Sales — no distance control
  model_sales_count_ns_1 <- feols(
    log_price ~ spill_count_daily_avg * direction,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_1b <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + old_new + duration,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_2 <- feols(
    log_price ~ spill_count_daily_avg * direction | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_2b <- feols(
    log_price ~ spill_count_daily_avg * direction | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_3 <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_3b <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + old_new + duration | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  
  # Sales — with distance control
  model_sales_count_ns_4 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_4b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + old_new + duration,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_5 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_5b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_6 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_count_ns_6b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + old_new + duration | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  
  # Rentals — no distance control
  model_rentals_count_ns_1 <- feols(
    log_price ~ spill_count_daily_avg * direction,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_1b <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_2 <- feols(
    log_price ~ spill_count_daily_avg * direction | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_2b <- feols(
    log_price ~ spill_count_daily_avg * direction | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_3 <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_3b <- feols(
    log_price ~ spill_count_daily_avg * direction + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  
  # Rentals — with distance control
  model_rentals_count_ns_4 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_4b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_5 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_5b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_6 <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_count_ns_6b <- feols(
    log_price ~ spill_count_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  
  # ------------------------------------------------------------------
  # Estimate Models: Spill Hours Daily Average, Nearest Spill Site
  # ------------------------------------------------------------------
  cat("  Estimating spill hours models...\n")
  
  # Sales — no distance control
  model_sales_hrs_ns_1 <- feols(
    log_price ~ spill_hrs_daily_avg * direction,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_1b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + old_new + duration,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_2 <- feols(
    log_price ~ spill_hrs_daily_avg * direction | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_2b <- feols(
    log_price ~ spill_hrs_daily_avg * direction | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_3 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_3b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + old_new + duration | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  
  # Sales — with distance control
  model_sales_hrs_ns_4 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_4b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + old_new + duration,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_5 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_5b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_6 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  model_sales_hrs_ns_6b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + old_new + duration | msoa,
    data = dat_nearest_sales, 
    vcov = "hetero"
  )
  
  # Rentals — no distance control
  model_rentals_hrs_ns_1 <- feols(
    log_price ~ spill_hrs_daily_avg * direction,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_1b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_2 <- feols(
    log_price ~ spill_hrs_daily_avg * direction | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_2b <- feols(
    log_price ~ spill_hrs_daily_avg * direction | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_3 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_3b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  
  # Rentals — with distance control
  model_rentals_hrs_ns_4 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_4b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_5 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_5b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_6 <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  model_rentals_hrs_ns_6b <- feols(
    log_price ~ spill_hrs_daily_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals, 
    vcov = "hetero"
  )
  
  # ------------------------------------------------------------------
  # Export Tables
  # ------------------------------------------------------------------
  
  rad_label <- paste0(RAD, "m")
  
  # Shared GOF map
  gof_map <- tibble::tribble(
    ~raw           , ~clean          , ~fmt ,
    "nobs"         , "Observations"  ,    0 ,
    "adj.r.squared", "Adj. R-squared",    3
  )
  
  # Shared add_rows
  add_rows <- tibble::tribble(
    ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"
  )
  attr(add_rows, "position") <- "coef_end"
  
  # Post-process modelsummary LaTeX output
  postprocess_table <- function(latex_str, label, notes, rad_label) {
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
    latex_str
  }
  
  # Build notes string
  make_notes <- function(measure_text) {
    paste0(
      "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure from the nearest spill site and property values. The sample includes all properties within ",
      RAD,
      "m of a storm overflow in England, 2021--2023. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). ",
      measure_text,
      " Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
    )
  }
  
  notes_count <- make_notes(
    paste0("Spill exposure is measured as the average number of spill events per day (12/24 count) recorded at the nearest overflow within ", RAD, "m from January 2021 to the transaction date.")
  )
  notes_hrs <- make_notes(
    paste0("Spill exposure is measured as the average total number of spill hours per day recorded at the nearest overflow within ", RAD, "m from January 2021 to the transaction date.")
  )
  
  # ==================================================================
  # Table 1: Spill Count, Nearest Site (no distance control)
  # ==================================================================
  cat("  Exporting spill count nearest site table...\n")
  
  coef_labels_count <- c(
    "(Intercept)" = "Constant",
    "spill_count_daily_avg" = "Daily spill count",
    "direction" = "Upstream spill site",
    "spill_count_daily_avg:direction" = "{Daily count \\\\ $\\times$ Upstream}"
  )
  
  panels_count <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_ns_1,
      "(2)" = model_sales_count_ns_1b,
      "(3)" = model_sales_count_ns_2b,
      "(4)" = model_sales_count_ns_3b,
      "(5)" = model_sales_count_ns_2,
      "(6)" = model_sales_count_ns_3
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_count_ns_1,
      "(8)" = model_rentals_count_ns_1b,
      "(9)" = model_rentals_count_ns_2b,
      "(10)" = model_rentals_count_ns_3b,
      "(11)" = model_rentals_count_ns_2,
      "(12)" = model_rentals_count_ns_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
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
    title = paste0("Effect of Sewage Spills (Count) from the Nearest Site on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-nearest-site-", rad_label),
    notes = notes_count,
    rad_label = rad_label
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_nearest_site_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 2: Spill Count, Nearest Site + Distance Control
  # ==================================================================
  cat("  Exporting spill count nearest site + distance table...\n")
  
  coef_labels_count_dist <- c(
    "(Intercept)" = "Constant",
    "spill_count_daily_avg" = "Daily spill count",
    "direction" = "Upstream spill site",
    "spill_count_daily_avg:direction" = "{Daily count \\\\ $\\times$ Upstream}",
    "dist_river_m" = "River distance"
  )
  
  panels_count_dist <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_ns_4,
      "(2)" = model_sales_count_ns_4b,
      "(3)" = model_sales_count_ns_5b,
      "(4)" = model_sales_count_ns_6b,
      "(5)" = model_sales_count_ns_5,
      "(6)" = model_sales_count_ns_6
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_count_ns_4,
      "(8)" = model_rentals_count_ns_4b,
      "(9)" = model_rentals_count_ns_5b,
      "(10)" = model_rentals_count_ns_6b,
      "(11)" = model_rentals_count_ns_5,
      "(12)" = model_rentals_count_ns_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels_count_dist,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_count_dist,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Count) from the Nearest Site on Property Values by Direction and Distance (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-nearest-site-distance-", rad_label),
    notes = notes_count,
    rad_label = rad_label
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_nearest_site_distance_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 3: Spill Hours, Nearest Site (no distance control)
  # ==================================================================
  cat("  Exporting spill hours nearest site table...\n")
  
  coef_labels_hrs <- c(
    "(Intercept)" = "Constant",
    "spill_hrs_daily_avg" = "Daily spill hours",
    "direction" = "Upstream spill site",
    "spill_hrs_daily_avg:direction" = "{Daily hours \\\\ $\\times$ Upstream}"
  )
  
  panels_hrs <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_ns_1,
      "(2)" = model_sales_hrs_ns_1b,
      "(3)" = model_sales_hrs_ns_2b,
      "(4)" = model_sales_hrs_ns_3b,
      "(5)" = model_sales_hrs_ns_2,
      "(6)" = model_sales_hrs_ns_3
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_hrs_ns_1,
      "(8)" = model_rentals_hrs_ns_1b,
      "(9)" = model_rentals_hrs_ns_2b,
      "(10)" = model_rentals_hrs_ns_3b,
      "(11)" = model_rentals_hrs_ns_2,
      "(12)" = model_rentals_hrs_ns_3
    )
  )
  
  tbl <- modelsummary::modelsummary(
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
    title = paste0("Effect of Sewage Spills (Hours) from the Nearest Site on Property Values by Direction (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-nearest-site-", rad_label),
    notes = notes_hrs,
    rad_label = rad_label
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_nearest_site_", rad_label, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)
  
  # ==================================================================
  # Table 4: Spill Hours, Nearest Site + Distance Control
  # ==================================================================
  cat("  Exporting spill hours nearest site + distance table...\n")
  
  coef_labels_hrs_dist <- c(
    "(Intercept)" = "Constant",
    "spill_hrs_daily_avg" = "Daily spill hours",
    "direction" = "Upstream spill site",
    "spill_hrs_daily_avg:direction" = "{Daily hours \\\\ $\\times$ Upstream}",
    "dist_river_m" = "River distance"
  )
  
  panels_hrs_dist <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_ns_4,
      "(2)" = model_sales_hrs_ns_4b,
      "(3)" = model_sales_hrs_ns_5b,
      "(4)" = model_sales_hrs_ns_6b,
      "(5)" = model_sales_hrs_ns_5,
      "(6)" = model_sales_hrs_ns_6
    ),
    "House Rentals" = list(
      "(7)" = model_rentals_hrs_ns_4,
      "(8)" = model_rentals_hrs_ns_4b,
      "(9)" = model_rentals_hrs_ns_5b,
      "(10)" = model_rentals_hrs_ns_6b,
      "(11)" = model_rentals_hrs_ns_5,
      "(12)" = model_rentals_hrs_ns_6
    )
  )
  
  tbl <- modelsummary::modelsummary(
    panels_hrs_dist,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_decimal(2),
    coef_map = coef_labels_hrs_dist,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = paste0("Effect of Sewage Spills (Hours) from the Nearest Site on Property Values by Direction and Distance (", rad_label, ")")
  )
  
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-nearest-site-distance-", rad_label),
    notes = notes_hrs,
    rad_label = rad_label
  )
  
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_nearest_site_distance_", rad_label, ".tex"))
  writeLines(tbl, out_path)
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