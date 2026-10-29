# ==============================================================================
# Upstream/Downstream Regression Analysis: Cumulative Spill Exposure (Weighted)
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous weekly average measures (spill count and hours) summed
#          separately over upstream and downstream overflows on the same river,
#          each overflow weighted by the inverse of its along-river distance.
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, MSOA FE, MSOA FE +
#          Controls, LSOA FE, and LSOA FE + Controls. For each coefficient
#          table, a second table reports t-tests that the upstream and
#          downstream coefficients are equal.
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
#   - output/tables/ud_full_count_weighted_<RAD>m.tex
#   - output/tables/ud_full_count_weighted_<RAD>m_ttest.tex
#   - output/tables/ud_full_hrs_weighted_<RAD>m.tex
#   - output/tables/ud_full_hrs_weighted_<RAD>m_ttest.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
# Radii (m) to run. Each radius writes its own suffixed tables; edit to restrict.
RADII        <- c(250L, 500L, 1000L)
LATERAL_DIST <- 250L
ALPHA        <- 1  # inverse river distance weight, d^(-ALPHA)


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
  "fixest",
  "marginaleffects"
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

# Goodness of fit map (radius-independent)
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Goodness of fit map for t-test tables (radius-independent)
gof_map_ttest <- tibble::tribble(
  ~raw   , ~clean         , ~fmt ,
  "nobs" , "Observations" ,    0
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

  # Exposure summed by direction, weighted by inverse river distance
  dat_sales_clean <- dat_sales_pairs |>
    group_by(house_id) |>
    mutate(inv_dist_weight = dist_river_m^(-ALPHA)) |>
    summarise(
      upstream_count_exposure = sum(inv_dist_weight * spill_count_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_count_exposure = sum(inv_dist_weight * spill_count_weekly_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs_exposure = sum(inv_dist_weight * spill_hrs_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs_exposure = sum(inv_dist_weight * spill_hrs_weekly_avg * (direction == 0), na.rm = TRUE)
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

  # Exposure summed by direction, weighted by inverse river distance
  dat_rental_clean <- dat_rental_pairs |>
    group_by(rental_id) |>
    mutate(inv_dist_weight = dist_river_m^(-ALPHA)) |>
    summarise(
      upstream_count_exposure = sum(inv_dist_weight * spill_count_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_count_exposure = sum(inv_dist_weight * spill_count_weekly_avg * (direction == 0), na.rm = TRUE),
      upstream_hrs_exposure = sum(inv_dist_weight * spill_hrs_weekly_avg * (direction == 1), na.rm = TRUE),
      downstream_hrs_exposure = sum(inv_dist_weight * spill_hrs_weekly_avg * (direction == 0), na.rm = TRUE)
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
  # Estimate Models: Spill Count Weekly Average (Weighted)
  # ============================================================================
  cat("Estimating spill count models...\n")

  # Sales Models
  model_sales_count_1 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_1b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_2 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_2b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_3 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_3b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  # Rental Models
  model_rentals_count_1 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_count_1b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_count_2 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_count_2b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_count_3 <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_count_3b <- fixest::feols(
    log_price ~ upstream_count_exposure + downstream_count_exposure + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )


  # ============================================================================
  # Estimate Models: Spill Hours Weekly Average (Weighted)
  # ============================================================================
  cat("Estimating spill hrs models...\n")

  # Sales Models
  model_sales_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  # Rental Models
  model_rentals_hrs_1 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_hrs_1b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_hrs_2 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_hrs_2b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_hrs_3 <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rentals_hrs_3b <- fixest::feols(
    log_price ~ upstream_hrs_exposure + downstream_hrs_exposure + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )


  # ============================================================================
  # Export Tables: Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "upstream_count_exposure" = "\\quad Upstream",
    "downstream_count_exposure" = "\\quad Downstream"
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
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available), with properties and spill sites within ", LATERAL_DIST, "m of a river and within ", RIVER_DIST, "m of each other along the river. ", na_note, "The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per week (12/24 count) recorded at each overflow from January 2021 to the transaction date, weighted by the inverse of its along-river distance to the property in meters and summed separately over upstream and downstream overflows. A unit of exposure is one additional weekly spill at an overflow 1m away along the river. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = "Effect of Sewage Spills (Weighted Count) on Property Values by Direction"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-count-weighted-", RAD, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_count_weighted_", RAD, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: t-test, Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count t-test table...\n")

  # Null hypothesis: equal upstream and downstream coefficients
  null_hypothesis <- "upstream_count_exposure - downstream_count_exposure = 0"

  # Combined t-tests for joint table
  panels <- list(
    "House Sales" = list(
      "(1)" = marginaleffects::hypotheses(model_sales_count_1, hypothesis = null_hypothesis),
      "(2)" = marginaleffects::hypotheses(model_sales_count_1b, hypothesis = null_hypothesis),
      "(3)" = marginaleffects::hypotheses(model_sales_count_2b, hypothesis = null_hypothesis),
      "(4)" = marginaleffects::hypotheses(model_sales_count_3b, hypothesis = null_hypothesis),
      "(5)" = marginaleffects::hypotheses(model_sales_count_2, hypothesis = null_hypothesis),
      "(6)" = marginaleffects::hypotheses(model_sales_count_3, hypothesis = null_hypothesis)
    ),
    "House Rentals" = list(
      "(7)" = marginaleffects::hypotheses(model_rentals_count_1, hypothesis = null_hypothesis),
      "(8)" = marginaleffects::hypotheses(model_rentals_count_1b, hypothesis = null_hypothesis),
      "(9)" = marginaleffects::hypotheses(model_rentals_count_2b, hypothesis = null_hypothesis),
      "(10)" = marginaleffects::hypotheses(model_rentals_count_3b, hypothesis = null_hypothesis),
      "(11)" = marginaleffects::hypotheses(model_rentals_count_2, hypothesis = null_hypothesis),
      "(12)" = marginaleffects::hypotheses(model_rentals_count_3, hypothesis = null_hypothesis)
    )
  )

  # p-values, with those below 0.001 shown as <0.001
  p_values <- sapply(unlist(panels, recursive = FALSE), function(test) test$p.value)
  p_cells <- ifelse(
    p_values < 0.001,
    "$<$\\num{0.001}",
    paste0("\\num{", fmt_table(p_values), "}")
  )

  # Coefficient labels
  coef_labels <- c(
    "upstream_count_exposure-downstream_count_exposure=0" = "Upstream $-$ downstream"
  )

  # Add rows for p-values, fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                                , ~`(1)`     , ~`(2)`     , ~`(3)`     , ~`(4)`     , ~`(5)`     , ~`(6)`     , ~`(7)`     , ~`(8)`     , ~`(9)`     , ~`(10)`     , ~`(11)`     , ~`(12)`    ,
    "$p$-value, upstream $=$ downstream" , p_cells[1] , p_cells[2] , p_cells[3] , p_cells[4] , p_cells[5] , p_cells[6] , p_cells[7] , p_cells[8] , p_cells[9] , p_cells[10] , p_cells[11] , p_cells[12],
    "Property controls"                  , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"       , "No"        , "Yes"      ,
    "Location FE"                        , "No"       , "No"       , "MSOA"     , "MSOA"     , "LSOA"     , "LSOA"     , "No"       , "No"       , "MSOA"     , "MSOA"      , "LSOA"      , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 4, 5)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table tests whether spills summed over upstream overflows and over downstream overflows have equal effects on property values, for each specification of the corresponding coefficient table (radius ", RAD, "m), with the same sample, controls and fixed effects. ", na_note, "Spill exposure is the average number of spill events per week (12/24 count) recorded at each overflow from January 2021 to the transaction date, weighted by the inverse of its along-river distance to the property in meters and summed separately over upstream and downstream overflows. Upstream $-$ downstream is the difference between the two spill coefficients, both estimated in the same regression; its standard error uses the full variance matrix, including the covariance between the two coefficients. The $p$-value is from a two-sided t-test that the two coefficients are equal. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    gof_map = gof_map_ttest,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = "t-test of Equal Upstream and Downstream Effects of Sewage Spills (Weighted Count) on Property Values"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-count-weighted-", RAD, "m-ttest"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_count_weighted_", RAD, "m_ttest.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "upstream_hrs_exposure" = "\\quad Upstream",
    "downstream_hrs_exposure" = "\\quad Downstream"
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
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available), with properties and spill sites within ", LATERAL_DIST, "m of a river and within ", RIVER_DIST, "m of each other along the river. ", na_note, "The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per week recorded at each overflow from January 2021 to the transaction date, weighted by the inverse of its along-river distance to the property in meters and summed separately over upstream and downstream overflows. A unit of exposure is one additional weekly spill hour at an overflow 1m away along the river. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = "Effect of Sewage Spills (Weighted hours) on Property Values by Direction"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-hrs-weighted-", RAD, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_hrs_weighted_", RAD, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: t-test, Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours t-test table...\n")

  # Null hypothesis: equal upstream and downstream coefficients
  null_hypothesis <- "upstream_hrs_exposure - downstream_hrs_exposure = 0"

  # Combined t-tests for joint table
  panels <- list(
    "House Sales" = list(
      "(1)" = marginaleffects::hypotheses(model_sales_hrs_1, hypothesis = null_hypothesis),
      "(2)" = marginaleffects::hypotheses(model_sales_hrs_1b, hypothesis = null_hypothesis),
      "(3)" = marginaleffects::hypotheses(model_sales_hrs_2b, hypothesis = null_hypothesis),
      "(4)" = marginaleffects::hypotheses(model_sales_hrs_3b, hypothesis = null_hypothesis),
      "(5)" = marginaleffects::hypotheses(model_sales_hrs_2, hypothesis = null_hypothesis),
      "(6)" = marginaleffects::hypotheses(model_sales_hrs_3, hypothesis = null_hypothesis)
    ),
    "House Rentals" = list(
      "(7)" = marginaleffects::hypotheses(model_rentals_hrs_1, hypothesis = null_hypothesis),
      "(8)" = marginaleffects::hypotheses(model_rentals_hrs_1b, hypothesis = null_hypothesis),
      "(9)" = marginaleffects::hypotheses(model_rentals_hrs_2b, hypothesis = null_hypothesis),
      "(10)" = marginaleffects::hypotheses(model_rentals_hrs_3b, hypothesis = null_hypothesis),
      "(11)" = marginaleffects::hypotheses(model_rentals_hrs_2, hypothesis = null_hypothesis),
      "(12)" = marginaleffects::hypotheses(model_rentals_hrs_3, hypothesis = null_hypothesis)
    )
  )

  # p-values, with those below 0.001 shown as <0.001
  p_values <- sapply(unlist(panels, recursive = FALSE), function(test) test$p.value)
  p_cells <- ifelse(
    p_values < 0.001,
    "$<$\\num{0.001}",
    paste0("\\num{", fmt_table(p_values), "}")
  )

  # Coefficient labels
  coef_labels <- c(
    "upstream_hrs_exposure-downstream_hrs_exposure=0" = "Upstream $-$ downstream"
  )

  # Add rows for p-values, fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                                , ~`(1)`     , ~`(2)`     , ~`(3)`     , ~`(4)`     , ~`(5)`     , ~`(6)`     , ~`(7)`     , ~`(8)`     , ~`(9)`     , ~`(10)`     , ~`(11)`     , ~`(12)`    ,
    "$p$-value, upstream $=$ downstream" , p_cells[1] , p_cells[2] , p_cells[3] , p_cells[4] , p_cells[5] , p_cells[6] , p_cells[7] , p_cells[8] , p_cells[9] , p_cells[10] , p_cells[11] , p_cells[12],
    "Property controls"                  , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"       , "No"        , "Yes"      ,
    "Location FE"                        , "No"       , "No"       , "MSOA"     , "MSOA"     , "LSOA"     , "LSOA"     , "No"       , "No"       , "MSOA"     , "MSOA"      , "LSOA"      , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 4, 5)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table tests whether spills summed over upstream overflows and over downstream overflows have equal effects on property values, for each specification of the corresponding coefficient table (radius ", RAD, "m), with the same sample, controls and fixed effects. ", na_note, "Spill exposure is the average total number of spill hours per week recorded at each overflow from January 2021 to the transaction date, weighted by the inverse of its along-river distance to the property in meters and summed separately over upstream and downstream overflows. Upstream $-$ downstream is the difference between the two spill coefficients, both estimated in the same regression; its standard error uses the full variance matrix, including the covariance between the two coefficients. The $p$-value is from a two-sided t-test that the two coefficients are equal. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    gof_map = gof_map_ttest,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = "t-test of Equal Upstream and Downstream Effects of Sewage Spills (Weighted Hours) on Property Values"
  )

  table_latex <- fit_tblr_latex(
    table_latex,
    label = paste0("tbl:ud-full-hrs-weighted-", RAD, "m-ttest"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_full_hrs_weighted_", RAD, "m_ttest.tex"))
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
