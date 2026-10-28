# ==============================================================================
# Upstream/Downstream Regression Analysis: Cumulative Spill Exposure
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous weekly average measures (spill count and hours) summed
#          separately over upstream and downstream overflows on the same river.
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, MSOA FE, MSOA FE +
#          Controls, LSOA FE, and LSOA FE + Controls.
#
#          The analysis is run for every radius in RADII (see Configuration);
#          each radius writes its own radius-suffixed tables.
#
# Author: Alina Zeltikova
# Date: 2026-10-06
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale_house_site/ - House-site pairs
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site/ - Rental-site pairs
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - upstream_downstream/output/spill_house_signed.csv - Upstream/downstream house-site pairs
#   - upstream_downstream/output/spill_rental_signed.csv - Upstream/downstream rental-site pairs
#
# Outputs (one per radius in RADII):
#   - output/tables/ud_full_count_<RAD>m.tex
#   - output/tables/ud_full_hrs_<RAD>m.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
# Radii (m) to run. Each radius writes its own suffixed tables; edit to restrict.
RADII        <- c(250L, 500L, 1000L)
LATERAL_DIST <- 250L


# ==============================================================================
# 2. Package Management
# ==============================================================================

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

# Shared table formatting helpers
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

# Output Directory Setup -------------------------------------------------------
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

exported_files <- character()
record_export <- function(path) {
  exported_files <<- c(exported_files, basename(path))
}


# ==============================================================================
# 3. Property and river data (radius-independent; loaded once)
# ==============================================================================

# House price data for property characteristics and LSOA
sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  select(
    -any_of("transaction_id"),
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

# Upstream/downstream spill site pairs
ud_sales_raw <- import(
  here::here("upstream_downstream", "output", "spill_house_signed.csv")
)
ud_rentals_raw <- import(
  here::here("upstream_downstream", "output", "spill_rental_signed.csv")
)

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)


# ==============================================================================
# 4. Per-radius analysis
# ==============================================================================
run_for_radius <- function(RAD) {

  cat("\n========================== Radius:", RAD, "m ==========================\n")

  # Along-river distance cut equals the radius
  RIVER_DIST <- RAD

  # ============================================================================
  # Panel A: Sales
  # ============================================================================

  # Load Sales Data ------------------------------------------------------------
  cat("Loading sales data...\n")

  # Cross-section data with spill metrics (prior to sale)
  dat_cs_sales <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "sales", "prior_to_sale_house_site")
  ) |>
    filter(radius == RAD) |>
    collect()

  # Upstream/downstream pairs within the lateral and along-river cuts
  upstream_downstream_sales <- ud_sales_raw |>
    mutate(
      direction = ifelse(direction == 1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m <= LATERAL_DIST &
        house_lateral_m <= LATERAL_DIST &
        dist_river_m <= RIVER_DIST &
        spill_house_euclid_m <= RAD
    )

  # Properties with missing spill data at any overflow within the radius
  houses_spill_na <- dat_cs_sales |>
    filter(is.na(spill_count_weekly_avg) | is.na(spill_hrs_weekly_avg)) |>
    distinct(house_id)

  # Sentence added to every table note
  na_note <- paste0(
    "Properties with any storm overflow within ", RAD, "m whose spill record is ",
    "missing are dropped in full, as in the hedonic tables. "
  )

  # Prepare Sales Data ---------------------------------------------------------
  cat("Preparing sales data...\n")

  dat_sales_pairs <- dat_cs_sales |>
    anti_join(houses_spill_na, by = "house_id") |>
    inner_join(upstream_downstream_sales, by = c("house_id", "site_id")) |>
    filter(!is.na(direction))

  # Exposure summed by direction
  dat_sales_clean <- dat_sales_pairs |>
    group_by(house_id) |>
    summarise(
      upstream_count = sum(spill_count_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_count = sum(spill_count_weekly_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs = sum(spill_hrs_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs = sum(spill_hrs_weekly_avg * (direction == 0), na.rm = TRUE)
    ) |>
    ungroup() |>
    inner_join(sales, by = "house_id") |>
    mutate(log_price = log(price)) |>
    filter(
      !is.na(lsoa),
      !is.na(property_type),
      !is.na(old_new),
      !is.na(duration)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new = forcats::fct_drop(old_new),
      duration = forcats::fct_drop(duration)
    )

  cat("  Sales observations:", nrow(dat_sales_clean), "\n")


  # ============================================================================
  # Panel B: Rentals
  # ============================================================================

  # Load Rental Data -----------------------------------------------------------
  cat("Loading rental data...\n")

  # Cross-section data with spill metrics (prior to rental)
  dat_cs_rentals <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "rentals", "prior_to_rental_rental_site")
  ) |>
    filter(radius == RAD) |>
    collect()

  # Upstream/downstream pairs within the lateral and along-river cuts
  upstream_downstream_rentals <- ud_rentals_raw |>
    mutate(
      direction = ifelse(direction == 1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m <= LATERAL_DIST &
        rental_lateral_m <= LATERAL_DIST &
        dist_river_m <= RIVER_DIST &
        spill_rental_euclid_m <= RAD
    )

  # Properties with missing spill data at any overflow within the radius
  rentals_spill_na <- dat_cs_rentals |>
    filter(is.na(spill_count_weekly_avg) | is.na(spill_hrs_weekly_avg)) |>
    distinct(rental_id)

  # Prepare Rental Data --------------------------------------------------------
  cat("Preparing rental data...\n")

  dat_rental_pairs <- dat_cs_rentals |>
    anti_join(rentals_spill_na, by = "rental_id") |>
    inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id")) |>
    filter(!is.na(direction))

  # Exposure summed by direction
  dat_rental_clean <- dat_rental_pairs |>
    group_by(rental_id) |>
    summarise(
      upstream_count = sum(spill_count_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_count = sum(spill_count_weekly_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs = sum(spill_hrs_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs = sum(spill_hrs_weekly_avg * (direction == 0), na.rm = TRUE)
    ) |>
    ungroup() |>
    inner_join(rentals, by = "rental_id") |>
    mutate(log_price = log(listing_price)) |>
    filter(
      !is.na(lsoa),
      !is.na(property_type),
      !is.na(bedrooms),
      !is.na(bathrooms)
    ) |>
    mutate(
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )

  cat("  Rental observations:", nrow(dat_rental_clean), "\n")


  # ============================================================================
  # Estimate Models: Spill Count Weekly Average
  # ============================================================================
  cat("Estimating spill count models...\n")

  # Sales Models
  model_sales_count_1 <- fixest::feols(
    log_price ~ upstream_count + downstream_count,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_count_1b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_count_2 <- fixest::feols(
    log_price ~ upstream_count + downstream_count | lsoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_count_2b <- fixest::feols(
    log_price ~ upstream_count + downstream_count | msoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_count_3 <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_count_3b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  # Rental Models
  model_rentals_count_1 <- fixest::feols(
    log_price ~ upstream_count + downstream_count,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_count_1b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_count_2 <- fixest::feols(
    log_price ~ upstream_count + downstream_count | lsoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_count_2b <- fixest::feols(
    log_price ~ upstream_count + downstream_count | msoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_count_3 <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_count_3b <- fixest::feols(
    log_price ~ upstream_count + downstream_count + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )


  # ============================================================================
  # Estimate Models: Spill Hours Weekly Average
  # ============================================================================
  cat("Estimating spill hrs models...\n")

  # Sales Models
  model_sales_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | lsoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | msoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  model_sales_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = spatial_vcov()
  )

  # Rental Models
  model_rentals_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | lsoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs | msoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs + downstream_hrs + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = spatial_vcov()
  )


  # ============================================================================
  # Export Tables: Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "upstream_count" = "\\quad Upstream",
    "downstream_count" = "\\quad Downstream"
  )

  # Combined models for joint table
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

  # Add rows for fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                         , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` , ~`(9)` , ~`(10)` , ~`(11)` , ~`(12)`,
    "Spills per week (avg.)"      , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""    ,
    "Property controls"           , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes" ,
    "Location FE"                 , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 8, 9)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available), with properties and spill sites within ", LATERAL_DIST, "m of a river and within ", RIVER_DIST, "m of each other along the river. ", na_note, "The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per week (12/24 count) recorded at each overflow from January 2021 to the transaction date, summed separately over upstream and downstream overflows. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  # Export table
  table_latex <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = coef_labels,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = "Effect of Sewage Spills (Unweighted Count) on Property Values by Direction"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-count-", RAD, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_count_", RAD, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "upstream_hrs" = "\\quad Upstream",
    "downstream_hrs" = "\\quad Downstream"
  )

  # Combined models for joint table
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

  # Add rows for fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                         , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` , ~`(9)` , ~`(10)` , ~`(11)` , ~`(12)`,
    "Spill hours per week (avg.)" , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""     , ""    ,
    "Property controls"           , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes" ,
    "Location FE"                 , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 8, 9)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available), with properties and spill sites within ", LATERAL_DIST, "m of a river and within ", RIVER_DIST, "m of each other along the river. ", na_note, "The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per week recorded at each overflow from January 2021 to the transaction date, summed separately over upstream and downstream overflows. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  # Export table
  table_latex <- modelsummary::modelsummary(
    panels,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = coef_labels,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = "Effect of Sewage Spills (Unweighted Hours) on Property Values by Direction"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-hrs-", RAD, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_hrs_", RAD, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)
}


# ==============================================================================
# 5. Run for all radii
# ==============================================================================
purrr::walk(RADII, run_for_radius)


# ==============================================================================
# 6. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (file in exported_files) {
  cat("  - ", file, "\n", sep = "")
}
