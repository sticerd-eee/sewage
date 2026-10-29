# ==============================================================================
# Upstream/Downstream Regression Analysis: Nearest Upstream and Downstream Site
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous weekly average measures (spill count and hours) at the
#          nearest upstream and the nearest downstream overflow, entered as two
#          regressors and controlling for the along-river distance to each.
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, MSOA FE, MSOA FE +
#          Controls, LSOA FE, and LSOA FE + Controls. For each coefficient
#          table, a second table reports t-tests that the upstream and
#          downstream coefficients are equal.
#
#          The analysis is run for every along-river distance in RIVER_DISTS
#          (see Configuration); each writes its own suffixed tables.
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
# Outputs (one per river distance in RIVER_DISTS):
#   - output/tables/ud_both_dir_count_dist_river_<RIVER_DIST>m.tex
#   - output/tables/ud_both_dir_count_dist_river_<RIVER_DIST>m_ttest.tex
#   - output/tables/ud_both_dir_hrs_dist_river_<RIVER_DIST>m.tex
#   - output/tables/ud_both_dir_hrs_dist_river_<RIVER_DIST>m_ttest.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
# Along-river distances (m) to run. Each writes its own suffixed tables.
RIVER_DISTS  <- c(250L, 500L, 1000L)
RAD          <- 1000L
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

# tabularray layout for the 12-column tables
postprocess_table <- function(latex_str, label, notes) {
  fit_tblr_latex(
    latex_str,
    label = label,
    notes = notes,
    hspan = "even",
    rowsep = "0.1pt"
  )
}


# ==============================================================================
# 3. Property and river data (loaded once)
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

# Goodness of fit map (river-distance-independent)
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Goodness of fit map for t-test tables (river-distance-independent)
gof_map_ttest <- tibble::tribble(
  ~raw   , ~clean         , ~fmt ,
  "nobs" , "Observations" ,    0
)

# Cross-section data with spill metrics, at the Euclidean radius
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale_house_site")
) |>
  filter(radius == RAD) |>
  collect()

dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental_rental_site")
) |>
  filter(radius == RAD) |>
  collect()


# ==============================================================================
# 4. Per-river-distance analysis
# ==============================================================================
run_for_river_dist <- function(RIVER_DIST) {

  cat("\n====================== River distance:", RIVER_DIST, "m ======================\n")

  # ============================================================================
  # Panel A: Sales
  # ============================================================================

  # Upstream/downstream pairs within the distance limits
  upstream_downstream_sales <- ud_sales_raw |>
    mutate(
      direction = ifelse(direction == 1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m <= LATERAL_DIST,
      house_lateral_m <= LATERAL_DIST,
      dist_river_m <= RIVER_DIST,
      spill_house_euclid_m <= RAD
    )

  dat_sales_pairs <- dat_cs_sales |>
    inner_join(upstream_downstream_sales, by = c("house_id", "site_id"))

  # Nearest site on each side; a site at 0m counts on both sides
  nearest_up_sales <- dat_sales_pairs |>
    filter(direction == 1 | dist_river_m == 0) |>
    arrange(house_id, dist_river_m, distance_m) |>
    group_by(house_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    select(
      house_id,
      site_id_up = site_id,
      count_up = spill_count_weekly_avg,
      hrs_up = spill_hrs_weekly_avg,
      dist_river_up_m = dist_river_m
    )

  nearest_down_sales <- dat_sales_pairs |>
    filter(direction == 0 | dist_river_m == 0) |>
    arrange(house_id, dist_river_m, distance_m) |>
    group_by(house_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    select(
      house_id,
      site_id_down = site_id,
      count_down = spill_count_weekly_avg,
      hrs_down = spill_hrs_weekly_avg,
      dist_river_down_m = dist_river_m
    )

  # Prepare Sales Data ---------------------------------------------------------
  # Properties with a site on each side, dropping those whose nearest site is at
  # 0m or has missing spill data
  dat_pair_sales <- nearest_up_sales |>
    inner_join(nearest_down_sales, by = "house_id") |>
    filter(
      dist_river_up_m > 0,
      dist_river_down_m > 0,
      !is.na(count_up), !is.na(hrs_up),
      !is.na(count_down), !is.na(hrs_down)
    ) |>
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

  cat("  Sales observations:", nrow(dat_pair_sales), "\n")


  # ============================================================================
  # Panel B: Rentals
  # ============================================================================

  # Upstream/downstream pairs within the distance limits
  upstream_downstream_rentals <- ud_rentals_raw |>
    mutate(
      direction = ifelse(direction == 1, 1, 0),
      dist_river_m = abs(signed_dist_m)
    ) |>
    filter(
      spill_lateral_m <= LATERAL_DIST,
      rental_lateral_m <= LATERAL_DIST,
      dist_river_m <= RIVER_DIST,
      spill_rental_euclid_m <= RAD
    )

  dat_rental_pairs <- dat_cs_rentals |>
    inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id"))

  # Nearest site on each side; a site at 0m counts on both sides
  nearest_up_rentals <- dat_rental_pairs |>
    filter(direction == 1 | dist_river_m == 0) |>
    arrange(rental_id, dist_river_m, distance_m) |>
    group_by(rental_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    select(
      rental_id,
      site_id_up = site_id,
      count_up = spill_count_weekly_avg,
      hrs_up = spill_hrs_weekly_avg,
      dist_river_up_m = dist_river_m
    )

  nearest_down_rentals <- dat_rental_pairs |>
    filter(direction == 0 | dist_river_m == 0) |>
    arrange(rental_id, dist_river_m, distance_m) |>
    group_by(rental_id) |>
    slice_head(n = 1) |>
    ungroup() |>
    select(
      rental_id,
      site_id_down = site_id,
      count_down = spill_count_weekly_avg,
      hrs_down = spill_hrs_weekly_avg,
      dist_river_down_m = dist_river_m
    )

  # Prepare Rental Data --------------------------------------------------------
  # Properties with a site on each side, dropping those whose nearest site is at
  # 0m or has missing spill data
  dat_pair_rentals <- nearest_up_rentals |>
    inner_join(nearest_down_rentals, by = "rental_id") |>
    filter(
      dist_river_up_m > 0,
      dist_river_down_m > 0,
      !is.na(count_up), !is.na(hrs_up),
      !is.na(count_down), !is.na(hrs_down)
    ) |>
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

  cat("  Rental observations:", nrow(dat_pair_rentals), "\n")


  # ============================================================================
  # Estimate Models: Spill Count Weekly Average
  # ============================================================================
  cat("Estimating spill count models...\n")

  # Sales Models
  model_sales_count_1 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_count_1b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_count_2 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m | lsoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_count_2b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m | msoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_count_3 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration | lsoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_count_3b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration | msoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  # Rental Models
  model_rentals_count_1 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_count_1b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_count_2 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m | lsoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_count_2b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m | msoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_count_3 <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_count_3b <- fixest::feols(
    log_price ~ count_up + count_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )


  # ============================================================================
  # Estimate Models: Spill Hours Weekly Average
  # ============================================================================
  cat("Estimating spill hrs models...\n")

  # Sales Models
  model_sales_hrs_1 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_hrs_1b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_hrs_2 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m | lsoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_hrs_2b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m | msoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_hrs_3 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration | lsoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  model_sales_hrs_3b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + old_new + duration | msoa,
    data = dat_pair_sales,
    vcov = spatial_vcov()
  )

  # Rental Models
  model_rentals_hrs_1 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_1b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_2 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m | lsoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_2b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m | msoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_3 <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )

  model_rentals_hrs_3b <- fixest::feols(
    log_price ~ hrs_up + hrs_down + dist_river_up_m + dist_river_down_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_pair_rentals,
    vcov = spatial_vcov()
  )


  # ============================================================================
  # Export Tables: Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "count_up" = "\\quad Nearest upstream site",
    "count_down" = "\\quad Nearest downstream site"
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
    "River distance to each site" , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes" ,
    "Property controls"           , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes" ,
    "Location FE"                 , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 8, 9, 10)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values, in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). Each property is matched to its nearest upstream and its nearest downstream overflow, both the closest of their direction measured along the river, and the sample is restricted to properties that have at least one overflow in each direction. Properties and spill sites are within ", LATERAL_DIST, "m (lateral) of a river, within ", RIVER_DIST, "m of each other along the river, and within ", RAD, "m Euclidean. Properties whose nearest upstream or nearest downstream overflow is snapped to the same point on the river or has no spill record are dropped. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is the average number of spill events per week (12/24 count) recorded at an overflow within ", RIVER_DIST, "m along the river from January 2021 to the transaction date. The two overflows enter as separate regressors, so no restriction is imposed linking their coefficients. Every specification in this table also controls for the along-river distance from the property to each of the two retained sites, in metres; those two coefficients are not reported. Property controls include type, new-build status and tenure for sales; and type, bedrooms and bathrooms for rentals. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = paste0(
      "Effect of Sewage Spills (Count) on Property Values, Nearest Upstream ",
      "and Nearest Downstream Site, Controlling for River Distance (river ",
      RIVER_DIST, "m)"
    )
  )

  table_latex <- postprocess_table(
    table_latex,
    label = paste0("tbl:ud-both-dir-count-dist-river-", RIVER_DIST, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_both_dir_count_dist_river_", RIVER_DIST, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: t-test, Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count t-test table...\n")

  # Null hypothesis: equal upstream and downstream coefficients
  null_hypothesis <- "count_up - count_down = 0"

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
    "count_up-count_down=0" = "Upstream $-$ downstream"
  )

  # Add rows for p-values, fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                                , ~`(1)`     , ~`(2)`     , ~`(3)`     , ~`(4)`     , ~`(5)`     , ~`(6)`     , ~`(7)`     , ~`(8)`     , ~`(9)`     , ~`(10)`     , ~`(11)`     , ~`(12)`    ,
    "$p$-value, upstream $=$ downstream" , p_cells[1] , p_cells[2] , p_cells[3] , p_cells[4] , p_cells[5] , p_cells[6] , p_cells[7] , p_cells[8] , p_cells[9] , p_cells[10] , p_cells[11] , p_cells[12],
    "River distance to each site"        , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"       , "Yes"       , "Yes"      ,
    "Property controls"                  , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"       , "No"        , "Yes"      ,
    "Location FE"                        , "No"       , "No"       , "MSOA"     , "MSOA"     , "LSOA"     , "LSOA"     , "No"       , "No"       , "MSOA"     , "MSOA"      , "LSOA"      , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 4, 5, 6)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table tests whether spills at the nearest upstream and at the nearest downstream overflow have equal effects on property values, for each specification of the corresponding coefficient table (river ", RIVER_DIST, "m), with the same sample, controls and fixed effects. Spill exposure is the average number of spill events per week (12/24 count) recorded at each overflow from January 2021 to the transaction date. Upstream $-$ downstream is the difference between the two spill coefficients, both estimated in the same regression; its standard error uses the full variance matrix, including the covariance between the two coefficients. The $p$-value is from a two-sided t-test that the two coefficients are equal. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = paste0(
      "t-test of Equal Upstream and Downstream Effects of Sewage Spills (Count), ",
      "Nearest Upstream and Nearest Downstream Site, Controlling for River ",
      "Distance (river ", RIVER_DIST, "m)"
    )
  )

  table_latex <- postprocess_table(
    table_latex,
    label = paste0("tbl:ud-both-dir-count-dist-river-", RIVER_DIST, "m-ttest"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_both_dir_count_dist_river_", RIVER_DIST, "m_ttest.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours table...\n")

  # Coefficient labels
  coef_labels <- c(
    "(Intercept)" = "Constant",
    "hrs_up" = "\\quad Nearest upstream site",
    "hrs_down" = "\\quad Nearest downstream site"
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
    "River distance to each site" , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes"  , "Yes" ,
    "Property controls"           , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes" ,
    "Location FE"                 , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 8, 9, 10)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values, in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). Each property is matched to its nearest upstream and its nearest downstream overflow, both the closest of their direction measured along the river, and the sample is restricted to properties that have at least one overflow in each direction. Properties and spill sites are within ", LATERAL_DIST, "m (lateral) of a river, within ", RIVER_DIST, "m of each other along the river, and within ", RAD, "m Euclidean. Properties whose nearest upstream or nearest downstream overflow is snapped to the same point on the river or has no spill record are dropped. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is the average total spill hours per week recorded at an overflow within ", RIVER_DIST, "m along the river from January 2021 to the transaction date. The two overflows enter as separate regressors, so no restriction is imposed linking their coefficients. Every specification in this table also controls for the along-river distance from the property to each of the two retained sites, in metres; those two coefficients are not reported. Property controls include type, new-build status and tenure for sales; and type, bedrooms and bathrooms for rentals. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = paste0(
      "Effect of Sewage Spills (Hours) on Property Values, Nearest Upstream ",
      "and Nearest Downstream Site, Controlling for River Distance (river ",
      RIVER_DIST, "m)"
    )
  )

  table_latex <- postprocess_table(
    table_latex,
    label = paste0("tbl:ud-both-dir-hrs-dist-river-", RIVER_DIST, "m"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_both_dir_hrs_dist_river_", RIVER_DIST, "m.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)


  # ============================================================================
  # Export Tables: t-test, Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours t-test table...\n")

  # Null hypothesis: equal upstream and downstream coefficients
  null_hypothesis <- "hrs_up - hrs_down = 0"

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
    "hrs_up-hrs_down=0" = "Upstream $-$ downstream"
  )

  # Add rows for p-values, fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                                , ~`(1)`     , ~`(2)`     , ~`(3)`     , ~`(4)`     , ~`(5)`     , ~`(6)`     , ~`(7)`     , ~`(8)`     , ~`(9)`     , ~`(10)`     , ~`(11)`     , ~`(12)`    ,
    "$p$-value, upstream $=$ downstream" , p_cells[1] , p_cells[2] , p_cells[3] , p_cells[4] , p_cells[5] , p_cells[6] , p_cells[7] , p_cells[8] , p_cells[9] , p_cells[10] , p_cells[11] , p_cells[12],
    "River distance to each site"        , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"      , "Yes"       , "Yes"       , "Yes"      ,
    "Property controls"                  , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"      , "No"       , "Yes"       , "No"        , "Yes"      ,
    "Location FE"                        , "No"       , "No"       , "MSOA"     , "MSOA"     , "LSOA"     , "LSOA"     , "No"       , "No"       , "MSOA"     , "MSOA"      , "LSOA"      , "LSOA"
  )
  attr(add_rows, "position") <- c(3, 4, 5, 6)

  # Notes
  custom_notes <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table tests whether spills at the nearest upstream and at the nearest downstream overflow have equal effects on property values, for each specification of the corresponding coefficient table (river ", RIVER_DIST, "m), with the same sample, controls and fixed effects. Spill exposure is the average total spill hours per week recorded at each overflow from January 2021 to the transaction date. Upstream $-$ downstream is the difference between the two spill coefficients, both estimated in the same regression; its standard error uses the full variance matrix, including the covariance between the two coefficients. The $p$-value is from a two-sided t-test that the two coefficients are equal. ", spatial_se_note(), " *** p<0.01, ** p<0.05, * p<0.1.}},"
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
    title = paste0(
      "t-test of Equal Upstream and Downstream Effects of Sewage Spills (Hours), ",
      "Nearest Upstream and Nearest Downstream Site, Controlling for River ",
      "Distance (river ", RIVER_DIST, "m)"
    )
  )

  table_latex <- postprocess_table(
    table_latex,
    label = paste0("tbl:ud-both-dir-hrs-dist-river-", RIVER_DIST, "m-ttest"),
    notes = custom_notes
  )

  output_path <- file.path(output_dir, paste0("ud_both_dir_hrs_dist_river_", RIVER_DIST, "m_ttest.tex"))
  writeLines(table_latex, output_path)
  record_export(output_path)
}


# ==============================================================================
# 5. Run for all river distances
# ==============================================================================
purrr::walk(RIVER_DISTS, run_for_river_dist)


# ==============================================================================
# 6. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (file in exported_files) {
  cat("  - ", file, "\n", sep = "")
}
