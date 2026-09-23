# ==============================================================================
# Cross-Sectional Regression Analysis: Inverse-Distance-Weighted Spill Measures
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values using
#          continuous weekly average measures (spill count and hours) that are
#          weighted by the inverse straight-line distance to each overflow.
#          Panel A: Sales (log house prices), Panel B: Rentals (log rental
#          prices). Each panel includes OLS, Controls, MSOA FE, MSOA FE +
#          Controls, LSOA FE, and LSOA FE + Controls.
#
#          This is the distance-weighted counterpart of
#          scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R. The
#          estimation samples are identical; only the exposure regressor
#          differs. Exposure is built from the transaction-by-site pair tables
#          as the sum over overflows within the radius of each overflow's
#          average weekly spill exposure divided by its straight-line distance
#          in kilometres (unnormalised, i.e. a weighted sum, not a mean).
#
#          The analysis is run for every radius in RADII (see Configuration);
#          each radius writes its own radius-suffixed tables.
#
# Author: Jacopo Olivieri
# Date: 2026-09-23
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#   - data/processed/cross_section/sales/prior_to_sale_house_site/ - House-site pairs
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site/ - Rental-site pairs
#
# Outputs (one per radius in RADII):
#   - output/tables/hedonic_count_continuous_prior_idw_<RAD>m.tex
#   - output/tables/hedonic_hrs_continuous_prior_idw_<RAD>m.tex
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
# Radii (m) to run. Each radius writes its own suffixed tables; edit to restrict.
RADII <- c(250L, 500L, 1000L)


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


# ==============================================================================
# 3. Property characteristics (radius-independent; loaded once)
# ==============================================================================

# House price data for property characteristics and LSOA
sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  select(
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

# Goodness of fit map (radius-independent)
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Inverse-distance-weighted exposure -------------------------------------------
# The weight is 1 / (distance_m / 1000): the inverse straight-line distance in
# kilometres, with no offset and no floor. The aggregation stays lazy in arrow
# so the pair tables (~5.5M sales rows at 1000m) are never collected in full,
# and `sum()` keeps its default `na.rm = FALSE` so the NA-propagation convention
# carries through to the weighted measure unchanged.
build_idw_exposure <- function(pair_path, id_col, RAD) {
  arrow::open_dataset(
    here::here("data", "processed", "cross_section", pair_path[1], pair_path[2])
  ) |>
    filter(radius == RAD) |>
    mutate(idw_weight = 1 / (distance_m / 1000)) |>
    group_by(.data[[id_col]]) |>
    summarise(
      spill_count_weekly_avg_idw = sum(idw_weight * spill_count_weekly_avg),
      spill_hrs_weekly_avg_idw   = sum(idw_weight * spill_hrs_weekly_avg)
    ) |>
    collect() |>
    as.data.frame()
}

# Console diagnostic on the near-zero distances that drive the weights.
report_idw_diagnostics <- function(pair_path, id_col, RAD, keep_ids, label) {
  diag <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", pair_path[1], pair_path[2])
  ) |>
    filter(radius == RAD) |>
    filter(.data[[id_col]] %in% keep_ids) |>
    mutate(
      idw_weight = 1 / (distance_m / 1000),
      under_10m = distance_m < 10
    ) |>
    summarise(
      n_pairs          = n(),
      min_distance_m   = min(distance_m),
      n_pairs_under10  = sum(as.integer(under_10m)),
      total_weighted   = sum(idw_weight * spill_count_weekly_avg, na.rm = TRUE),
      under10_weighted = sum(
        idw_weight * spill_count_weekly_avg * as.integer(under_10m),
        na.rm = TRUE
      )
    ) |>
    collect() |>
    as.data.frame()

  cat("  ", label, " inverse-distance diagnostics:\n", sep = "")
  cat("    Pairs:", diag$n_pairs, "\n")
  cat("    Minimum distance (m):", format(diag$min_distance_m, digits = 6), "\n")
  cat("    Pairs under 10 m:", diag$n_pairs_under10, "\n")
  cat(
    "    Share of weighted count exposure from pairs under 10 m:",
    format(diag$under10_weighted / diag$total_weighted, digits = 4), "\n"
  )

  invisible(diag)
}


# ==============================================================================
# 4. Per-radius analysis
# ==============================================================================
run_for_radius <- function(RAD) {

  cat("\n========================== Radius:", RAD, "m ==========================\n")

  # ============================================================================
  # Panel A: Sales
  # ============================================================================

  # Load Sales Data ------------------------------------------------------------
  cat("Loading sales data...\n")

  # Cross-section data with spill metrics (prior to sale)
  ## Filter for houses with at least one spill site within radius
  dat_cs_sales <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
  ) |>
    filter(radius == RAD) |>
    filter(n_spill_sites > 0) |>
    collect()

  # Prepare Sales Data ---------------------------------------------------------
  cat("Preparing sales data...\n")

  dat_sales_clean <- dat_cs_sales |>
    select(-any_of("price")) |>
    inner_join(sales, by = "house_id") |>
    mutate(log_price = log(price)) |>
    filter(
      !is.na(spill_count_weekly_avg),
      !is.na(spill_hrs_weekly_avg),
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

  n_sales_unweighted <- nrow(dat_sales_clean)
  cat("  Sales observations:", n_sales_unweighted, "\n")

  # Attach Distance-Weighted Sales Exposure ------------------------------------
  cat("Building distance-weighted sales exposure...\n")

  idw_sales <- build_idw_exposure(
    c("sales", "prior_to_sale_house_site"), "house_id", RAD
  )

  dat_sales_clean <- dat_sales_clean |>
    inner_join(idw_sales, by = "house_id")

  if (nrow(dat_sales_clean) != n_sales_unweighted) {
    stop(
      "Distance-weighted sales sample (", nrow(dat_sales_clean),
      ") differs from the unweighted sample (", n_sales_unweighted,
      ") at ", RAD, "m.",
      call. = FALSE
    )
  }
  if (
    anyNA(dat_sales_clean$spill_count_weekly_avg_idw) ||
      anyNA(dat_sales_clean$spill_hrs_weekly_avg_idw)
  ) {
    stop(
      "Distance-weighted sales exposure is NA where the unweighted measure is not.",
      call. = FALSE
    )
  }

  report_idw_diagnostics(
    c("sales", "prior_to_sale_house_site"), "house_id", RAD,
    dat_sales_clean$house_id, "Sales"
  )

  # ============================================================================
  # Panel B: Rentals
  # ============================================================================

  # Load Rental Data -----------------------------------------------------------
  cat("Loading rental data...\n")

  # Cross-section data with spill metrics (prior to rental)
  dat_cs_rentals <- arrow::open_dataset(
    here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
  ) |>
    filter(radius == RAD) |>
    filter(n_spill_sites > 0) |>
    collect()

  # Prepare Rental Data --------------------------------------------------------
  cat("Preparing rental data...\n")

  dat_rental_clean <- dat_cs_rentals |>
    select(-any_of("listing_price")) |>
    inner_join(rentals, by = "rental_id") |>
    mutate(log_price = log(listing_price)) |>
    filter(
      !is.na(spill_count_weekly_avg),
      !is.na(spill_hrs_weekly_avg),
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

  n_rentals_unweighted <- nrow(dat_rental_clean)
  cat("  Rental observations:", n_rentals_unweighted, "\n")

  # Attach Distance-Weighted Rental Exposure -----------------------------------
  cat("Building distance-weighted rental exposure...\n")

  idw_rentals <- build_idw_exposure(
    c("rentals", "prior_to_rental_rental_site"), "rental_id", RAD
  )

  dat_rental_clean <- dat_rental_clean |>
    inner_join(idw_rentals, by = "rental_id")

  if (nrow(dat_rental_clean) != n_rentals_unweighted) {
    stop(
      "Distance-weighted rental sample (", nrow(dat_rental_clean),
      ") differs from the unweighted sample (", n_rentals_unweighted,
      ") at ", RAD, "m.",
      call. = FALSE
    )
  }
  if (
    anyNA(dat_rental_clean$spill_count_weekly_avg_idw) ||
      anyNA(dat_rental_clean$spill_hrs_weekly_avg_idw)
  ) {
    stop(
      "Distance-weighted rental exposure is NA where the unweighted measure is not.",
      call. = FALSE
    )
  }

  report_idw_diagnostics(
    c("rentals", "prior_to_rental_rental_site"), "rental_id", RAD,
    dat_rental_clean$rental_id, "Rentals"
  )

  # ============================================================================
  # Estimate Models: Distance-Weighted Spill Count Weekly Average
  # ============================================================================
  cat("Estimating spill count models...\n")

  # Sales Models
  model_sales_count_idw_1 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_idw_1b <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_idw_2 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_idw_3 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_idw_4 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_count_idw_5 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  # Rental Models
  model_rental_count_idw_1 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_count_idw_1b <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_count_idw_2 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_count_idw_3 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_count_idw_4 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_count_idw_5 <- fixest::feols(
    log_price ~ spill_count_weekly_avg_idw + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  # ============================================================================
  # Estimate Models: Distance-Weighted Spill Hours Weekly Average
  # ============================================================================
  cat("Estimating spill hours models...\n")

  # Sales Models
  model_sales_hrs_idw_1 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_idw_1b <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + old_new + duration,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_idw_2 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_idw_3 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + old_new + duration | lsoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_idw_4 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  model_sales_hrs_idw_5 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + old_new + duration | msoa,
    data = dat_sales_clean,
    vcov = "hetero"
  )

  # Rental Models
  model_rental_hrs_idw_1 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_hrs_idw_1b <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + bedrooms + bathrooms,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_hrs_idw_2 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_hrs_idw_3 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + bedrooms + bathrooms | lsoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_hrs_idw_4 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  model_rental_hrs_idw_5 <- fixest::feols(
    log_price ~ spill_hrs_weekly_avg_idw + property_type + bedrooms + bathrooms | msoa,
    data = dat_rental_clean,
    vcov = "hetero"
  )

  # ============================================================================
  # Export Tables: Distance-Weighted Spill Count Weekly Average
  # ============================================================================
  cat("Exporting spill count table...\n")

  # Coefficient labels
  coef_labels_count <- c(
    "(Intercept)" = "Constant",
    "spill_count_weekly_avg_idw" = "Spills per week (avg., dist.-weighted)"
  )

  # Combined models for joint table
  panels_count <- list(
    "House Sales" = list(
      "(1)" = model_sales_count_idw_1,
      "(2)" = model_sales_count_idw_1b,
      "(3)" = model_sales_count_idw_4,
      "(4)" = model_sales_count_idw_5,
      "(5)" = model_sales_count_idw_2,
      "(6)" = model_sales_count_idw_3
    ),
    "House Rentals" = list(
      "(7)" = model_rental_count_idw_1,
      "(8)" = model_rental_count_idw_1b,
      "(9)" = model_rental_count_idw_4,
      "(10)" = model_rental_count_idw_5,
      "(11)" = model_rental_count_idw_2,
      "(12)" = model_rental_count_idw_3
    )
  )

  # Add rows for fixed effects and controls
  add_rows <- tibble::tribble(
    ~term                , ~`(1)` , ~`(2)` , ~`(3)` , ~`(4)` , ~`(5)` , ~`(6)` , ~`(7)` , ~`(8)` , ~`(9)` , ~`(10)`, ~`(11)`, ~`(12)`,
    "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  ,
    "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" ,
    "Time FE"            , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"   , "No"
  )
  attr(add_rows, "position") <- "coef_end"

  # Notes
  custom_notes_count <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). Properties are excluded where any overflow within the radius has an incomplete spill record over the exposure window, since measured spill exposure would otherwise be understated. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average number of spill events per week (12/24 count) recorded across all overflows within ", RAD, "m from January 2021 to the transaction date. Exposure weights each overflow's average weekly spills by the inverse of its straight-line distance in kilometres and sums across overflows within the radius; the coefficient is the effect of one additional weekly spill at an overflow 1 km away, and $\\\\beta/d$ at distance $d$ km. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  # Export table
  table_latex_count <- modelsummary::modelsummary(
    panels_count,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = coef_labels_count,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = "Effect of Sewage Spills (Count, Distance-Weighted) on Property Values"
  )

  table_latex_count <- fit_tblr_latex(
    table_latex_count,
    label = paste0("tbl:hedonic-count-continuous-prior-idw-", RAD, "m"),
    notes = custom_notes_count
  )

  output_path_count <- file.path(output_dir, paste0("hedonic_count_continuous_prior_idw_", RAD, "m.tex"))
  writeLines(table_latex_count, output_path_count)

  # ============================================================================
  # Export Tables: Distance-Weighted Spill Hours Weekly Average
  # ============================================================================
  cat("Exporting spill hours table...\n")

  # Coefficient labels (including controls)
  coef_labels_hrs <- c(
    "(Intercept)" = "Constant",
    "spill_hrs_weekly_avg_idw" = "Spill hours per week (avg., dist.-weighted)"
  )

  # Combined models for joint table
  panels_hrs <- list(
    "House Sales" = list(
      "(1)" = model_sales_hrs_idw_1,
      "(2)" = model_sales_hrs_idw_1b,
      "(3)" = model_sales_hrs_idw_4,
      "(4)" = model_sales_hrs_idw_5,
      "(5)" = model_sales_hrs_idw_2,
      "(6)" = model_sales_hrs_idw_3
    ),
    "House Rentals" = list(
      "(7)" = model_rental_hrs_idw_1,
      "(8)" = model_rental_hrs_idw_1b,
      "(9)" = model_rental_hrs_idw_4,
      "(10)" = model_rental_hrs_idw_5,
      "(11)" = model_rental_hrs_idw_2,
      "(12)" = model_rental_hrs_idw_3
    )
  )
  # Notes
  custom_notes_hrs <- paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic estimates of the relationship between sewage spill exposure and property values. The sample includes all properties within ", RAD, "m of a storm overflow in England, 2021--2024 for sales and 2021--2023 for rentals (no 2024 rental data are available). Properties are excluded where any overflow within the radius has an incomplete spill record over the exposure window, since measured spill exposure would otherwise be understated. The dependent variable is the log transaction price for sales (columns 1--6) or log weekly asking rent for rentals (columns 7--12). Spill exposure is measured as the average total number of spill hours per week recorded across all overflows within ", RAD, "m from January 2021 to the transaction date. Exposure weights each overflow's average weekly spill hours by the inverse of its straight-line distance in kilometres and sums across overflows within the radius; the coefficient is the effect of one additional weekly spill hour at an overflow 1 km away, and $\\\\beta/d$ at distance $d$ km. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )

  # Export table
  table_latex_hrs <- modelsummary::modelsummary(
    panels_hrs,
    shape = "cbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = coef_labels_hrs,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    title = "Effect of Sewage Spills (Hours, Distance-Weighted) on Property Values"
  )

  table_latex_hrs <- fit_tblr_latex(
    table_latex_hrs,
    label = paste0("tbl:hedonic-hrs-continuous-prior-idw-", RAD, "m"),
    notes = custom_notes_hrs
  )

  output_path_hrs <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_idw_", RAD, "m.tex"))
  writeLines(table_latex_hrs, output_path_hrs)

  cat(
    "  Wrote:", basename(output_path_count), "and",
    basename(output_path_hrs), "\n"
  )

  invisible(NULL)
}


# ==============================================================================
# 5. Run for all radii
# ==============================================================================
purrr::walk(RADII, run_for_radius)

cat("\nLaTeX tables exported to:", output_dir, "\n")
cat("  Radii:", paste(RADII, collapse = ", "), "m\n")
