# ==============================================================================
# Cross-Sectional Regression Analysis: Near-vs-Far Difference-in-Differences
# ==============================================================================
#
# Purpose: Estimate the effect of sewage spills on property values by comparing,
#          for each near distance ring, the upstream/downstream spill gradient
#          against a common far reference ring (750-1000m). Three separate
#          nearest-site regressions are estimated, one per near ring:
#          0-250m, 250-500m and 500-750m, each versus the 750-1000m reference.
#          The coefficient of interest is the triple interaction
#          spill x direction x near ring. Each panel includes OLS, Controls,
#          MSOA FE, and MSOA FE + Controls.
#          Panel A: house sales. Panel B: rentals.
#
# Author: Alina Zeltikova
# Date: 2026-06-05
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale_house_site - Cross-sectional sales (house - spill site level)
#   - data/processed/house_price.parquet - House prices and property characteristics
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site - Cross-sectional rentals (rental - spill site level)
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental listings and property characteristics
#   - upstream_downstream/output/18-03/river_filter/spill_house_signed.csv - House - upstream/downstream spill site pairs
#   - upstream_downstream/output/18-03/river_filter/spill_rental_signed.csv - Rental - upstream/downstream spill site pairs
#
# Outputs:
#   - output/tables/ud_did_count.tex
#   - output/tables/ud_did_count_distance.tex
#   - output/tables/ud_did_hrs.tex
#   - output/tables/ud_did_hrs_distance.tex
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RING_BREAKS    <- c(0, 250, 500, 750, 1000)
RING_LABELS    <- c("b0_250", "b250_500", "b500_750", "b750_1000")
REF_RING       <- "b750_1000"
NEAR_LABELS    <- c("b0_250", "b250_500", "b500_750")
MAX_DIST       <- 1000L
LATERAL_CAP    <- 250L
RIVER_DIST_CAP <- 1000L

# Column-group headers for the three near rings
bin_title <- c(
  "b0_250"   = "0--250m vs 750--1000m",
  "b250_500" = "250--500m vs 750--1000m",
  "b500_750" = "500--750m vs 750--1000m"
)

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
  "tinytable",
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
# Panel A: Sales
# ==============================================================================

# Load Sales Data --------------------------------------------------------------
cat("Loading sales data...\n")

# Cross-section data with spill metrics (prior to sale), all pairs within 1km
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale_house_site")
) |>
  filter(radius == MAX_DIST) |>
  collect()

# House price data for property characteristics and LSOA
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

# Load Upstream/Downstream Data ------------------------------------------------
cat("Preparing upstream/downstream data...\n")

ud_sales_raw <- import(
  here::here("upstream_downstream", "output", "18-03", "river_filter", "spill_house_signed.csv")
)

upstream_downstream_sales <- ud_sales_raw |>
  mutate(
    direction = ifelse(direction == -1, 1, 0),
    dist_river_m = abs(signed_dist_m)
  ) |>
  filter(
    spill_lateral_m <= LATERAL_CAP,
    house_lateral_m <= LATERAL_CAP,
    dist_river_m <= RIVER_DIST_CAP,
    spill_house_euclid_m <= MAX_DIST
  ) |>
  distinct()

# Prepare Sales Data -----------------------------------------------------------
cat("Preparing sales data...\n")

# House-site pairs with non-missing spill metrics, geography and characteristics
dat_sales_clean <- dat_cs_sales |>
  select(-any_of("price")) |>
  inner_join(sales, by = "house_id") |>
  inner_join(upstream_downstream_sales, by = c("house_id", "site_id")) |>
  mutate(log_price = log(price)) |>
  filter(
    !is.na(spill_count_weekly_avg),
    !is.na(spill_hrs_weekly_avg),
    !is.na(lsoa),
    !is.na(msoa),
    !is.na(property_type),
    !is.na(old_new),
    !is.na(duration),
    !is.na(direction),
    !is.na(distance_m)
  )

# Keep the single nearest spill site per property and assign its distance ring
dat_nearest_sales <- dat_sales_clean |>
  arrange(house_id, distance_m) |>
  group_by(house_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(
    log_price = log(price),
    ring = as.character(cut(distance_m, breaks = RING_BREAKS, labels = RING_LABELS,
                            right = TRUE, include.lowest = TRUE))
  ) |>
  filter(!is.na(ring))

cat("  Sales observations (nearest-site properties):", nrow(dat_nearest_sales), "\n")

# ==============================================================================
# Panel B: Rentals
# ==============================================================================

# Load Rental Data -------------------------------------------------------------
cat("Loading rental data...\n")

# Cross-section data with spill metrics (prior to rental), all pairs within 1km
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental_rental_site")
) |>
  filter(radius == MAX_DIST) |>
  collect()

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

# Load Upstream/Downstream Data ------------------------------------------------
cat("Preparing upstream/downstream data...\n")

ud_rentals_raw <- import(
  here::here("upstream_downstream", "output", "18-03", "river_filter", "spill_rental_signed.csv")
)

upstream_downstream_rentals <- ud_rentals_raw |>
  mutate(
    direction = ifelse(direction == -1, 1, 0),
    dist_river_m = abs(signed_dist_m)
  ) |>
  filter(
    spill_lateral_m <= LATERAL_CAP,
    rental_lateral_m <= LATERAL_CAP,
    dist_river_m <= RIVER_DIST_CAP,
    spill_rental_euclid_m <= MAX_DIST
  ) |>
  distinct()

# Prepare Rental Data ----------------------------------------------------------
cat("Preparing rental data...\n")

# Rental-site pairs with non-missing spill metrics, geography and characteristics
dat_rental_clean <- dat_cs_rentals |>
  select(-any_of("listing_price")) |>
  inner_join(rentals, by = "rental_id") |>
  inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id")) |>
  mutate(log_price = log(listing_price)) |>
  filter(
    !is.na(spill_count_weekly_avg),
    !is.na(spill_hrs_weekly_avg),
    !is.na(lsoa),
    !is.na(msoa),
    !is.na(property_type),
    !is.na(bedrooms),
    !is.na(bathrooms),
    !is.na(direction),
    !is.na(distance_m)
  )

# Keep the single nearest spill site per property and assign its distance ring
dat_nearest_rentals <- dat_rental_clean |>
  arrange(rental_id, distance_m) |>
  group_by(rental_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(
    log_price = log(listing_price),
    ring = as.character(cut(distance_m, breaks = RING_BREAKS, labels = RING_LABELS,
                            right = TRUE, include.lowest = TRUE))
  ) |>
  filter(!is.na(ring))

cat("  Rental observations (nearest-site properties):", nrow(dat_nearest_rentals), "\n")

# ==============================================================================
# Estimate Models
# ==============================================================================
# For each near ring, pool it with the 750-1000m reference ring and estimate the
# difference-in-differences with a binary near-ring indicator. Returns the four
# specifications (OLS, Controls, MSOA FE, MSOA FE + Controls) for sales and
# rentals, for the spill count and spill hours measures, both without and with
# a continuous river-distance control.

estimate_bin <- function(near_label) {
  
  cat("Estimating models for", bin_title[[near_label]], "...\n")
  
  dat_sales_pair <- dat_nearest_sales |>
    filter(ring %in% c(near_label, REF_RING)) |>
    mutate(
      bin_near = as.integer(ring == near_label),
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new = forcats::fct_drop(old_new),
      duration = forcats::fct_drop(duration)
    )
  
  dat_rental_pair <- dat_nearest_rentals |>
    filter(ring %in% c(near_label, REF_RING)) |>
    mutate(
      bin_near = as.integer(ring == near_label),
      lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )
  
  list(
    # Sales: spill count - no control by distance
    sales_count_1  = feols(log_price ~ spill_count_weekly_avg * direction * bin_near, data = dat_sales_pair, vcov = "hetero"),
    sales_count_1b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + property_type + old_new + duration, data = dat_sales_pair, vcov = "hetero"),
    sales_count_2b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near | msoa, data = dat_sales_pair, vcov = "hetero"),
    sales_count_3b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + property_type + old_new + duration | msoa, data = dat_sales_pair, vcov = "hetero"),
    # Sales: spill count - control for river distance
    sales_count_4  = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m, data = dat_sales_pair, vcov = "hetero"),
    sales_count_4b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m + property_type + old_new + duration, data = dat_sales_pair, vcov = "hetero"),
    sales_count_5b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m | msoa, data = dat_sales_pair, vcov = "hetero"),
    sales_count_6b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m + property_type + old_new + duration | msoa, data = dat_sales_pair, vcov = "hetero"),
    # Sales: spill hours - no control by distance
    sales_hrs_1  = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_1b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + property_type + old_new + duration, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_2b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near | msoa, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_3b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + property_type + old_new + duration | msoa, data = dat_sales_pair, vcov = "hetero"),
    # Sales: spill hours - control for river distance
    sales_hrs_4  = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_4b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m + property_type + old_new + duration, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_5b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m | msoa, data = dat_sales_pair, vcov = "hetero"),
    sales_hrs_6b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m + property_type + old_new + duration | msoa, data = dat_sales_pair, vcov = "hetero"),
    # Rentals: spill count - no control by distance
    rentals_count_1  = feols(log_price ~ spill_count_weekly_avg * direction * bin_near, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_1b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + property_type + bedrooms + bathrooms, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_2b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near | msoa, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_3b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + property_type + bedrooms + bathrooms | msoa, data = dat_rental_pair, vcov = "hetero"),
    # Rentals: spill count - control for river distance
    rentals_count_4  = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_4b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m + property_type + bedrooms + bathrooms, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_5b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m | msoa, data = dat_rental_pair, vcov = "hetero"),
    rentals_count_6b = feols(log_price ~ spill_count_weekly_avg * direction * bin_near + dist_river_m + property_type + bedrooms + bathrooms | msoa, data = dat_rental_pair, vcov = "hetero"),
    # Rentals: spill hours - no control by distance
    rentals_hrs_1  = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_1b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + property_type + bedrooms + bathrooms, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_2b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near | msoa, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_3b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + property_type + bedrooms + bathrooms | msoa, data = dat_rental_pair, vcov = "hetero"),
    # Rentals: spill hours - control for river distance
    rentals_hrs_4  = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_4b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m + property_type + bedrooms + bathrooms, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_5b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m | msoa, data = dat_rental_pair, vcov = "hetero"),
    rentals_hrs_6b = feols(log_price ~ spill_hrs_weekly_avg * direction * bin_near + dist_river_m + property_type + bedrooms + bathrooms | msoa, data = dat_rental_pair, vcov = "hetero")
  )
}

models <- lapply(NEAR_LABELS, estimate_bin)
names(models) <- NEAR_LABELS

b1 <- NEAR_LABELS[1]; b2 <- NEAR_LABELS[2]; b3 <- NEAR_LABELS[3]

# Goodness of fit map (shared across tables)
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Specification rows: 3 near rings x 4 specifications
add_rows <- tibble::tribble(
  ~term               , ~"(1)", ~"(2)", ~"(3)", ~"(4)", ~"(5)", ~"(6)", ~"(7)", ~"(8)", ~"(9)", ~"(10)", ~"(11)", ~"(12)",
  "Location FE"       , "No"  , "No"  , "MSOA", "MSOA", "No"  , "No"  , "MSOA", "MSOA", "No"  , "No"   , "MSOA" , "MSOA" ,
  "Property controls" , "No"  , "Yes" , "No"  , "Yes" , "No"  , "Yes" , "No"  , "Yes" , "No"  , "Yes"  , "No"   , "Yes"
)

# Render a combined two-panel table: Panel A (sales) over Panel B (rentals),
# with the three near rings as column groups, then apply the shared formatting.
export_did_table <- function(models_sales, models_rentals, coef_map, title, label, notes, fname) {
  
  panels <- list(
    "Panel A: House Sales"   = models_sales,
    "Panel B: House Rentals" = models_rentals
  )
  
  out <- modelsummary::modelsummary(
    panels,
    shape = "rbind",
    output = "latex",
    estimate = "{estimate}{stars}",
    statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table,
    coef_map = coef_map,
    gof_map = gof_map,
    add_rows = add_rows,
    notes = " ",
    escape = FALSE,
    title = title
  )
  
  # Group the twelve columns under the three near-ring headers
  out <- tinytable::group_tt(
    out,
    j = setNames(list(2:5, 6:9, 10:13), unname(bin_title[c(b1, b2, b3)]))
  )
  
  table_latex <- as.character(out)
  table_latex <- fit_tblr_latex(
    table_latex,
    label = label,
    notes = notes
  )
  
  output_path <- file.path(output_dir, fname)
  writeLines(table_latex, output_path)
  record_export(output_path)
}

# ==============================================================================
# Export Tables: Spill Count Daily Average
# ==============================================================================
cat("Exporting spill count table...\n")

# Coefficient labels
coef_labels_count <- c(
  "(Intercept)"                              = "Constant",
  "spill_count_weekly_avg"                    = "Spills per week (avg.)",
  "direction"                                = "Upstream",
  "bin_near"                                 = "Near ring",
  "spill_count_weekly_avg:direction"          = "Count $\\times$ Upstream",
  "spill_count_weekly_avg:bin_near"           = "Count $\\times$ Near",
  "direction:bin_near"                       = "Upstream $\\times$ Near",
  "spill_count_weekly_avg:direction:bin_near" = "{Count $\\times$ Upstream \\\\ $\\times$ Near}"
)

# Notes
custom_notes_count <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents nearest-site difference-in-differences estimates of the relationship between sewage spill exposure and property values in England, 2021--2023. Each column group pools one near ring (0--250m, 250--500m, 500--750m) with the common 750--1000m reference ring; ``Near ring'' equals one for properties in the near ring and zero in the reference. Each property is matched to its single nearest overflow within 250m (lateral) of a river and 1000m of each other along the river. The dependent variable is the log transaction price in Panel A and the log weekly asking rent in Panel B. Spill exposure is measured as the average number of spill events per week (12/24 count) recorded at the nearest overflow from January 2021 to the transaction date. ``Upstream'' equals one when the site is upstream of the property along the river; the triple interaction (count $\\\\times$ upstream $\\\\times$ near ring) gives the difference in the upstream spill gradient between the near ring and the reference. Property controls include type, new build status and tenure for sales; and type, bedrooms and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Models without river-distance control
export_did_table(
  models_sales = list(
    "(1)" = models[[b1]]$sales_count_1, "(2)" = models[[b1]]$sales_count_1b, "(3)" = models[[b1]]$sales_count_2b, "(4)" = models[[b1]]$sales_count_3b,
    "(5)" = models[[b2]]$sales_count_1, "(6)" = models[[b2]]$sales_count_1b, "(7)" = models[[b2]]$sales_count_2b, "(8)" = models[[b2]]$sales_count_3b,
    "(9)" = models[[b3]]$sales_count_1, "(10)" = models[[b3]]$sales_count_1b, "(11)" = models[[b3]]$sales_count_2b, "(12)" = models[[b3]]$sales_count_3b
  ),
  models_rentals = list(
    "(1)" = models[[b1]]$rentals_count_1, "(2)" = models[[b1]]$rentals_count_1b, "(3)" = models[[b1]]$rentals_count_2b, "(4)" = models[[b1]]$rentals_count_3b,
    "(5)" = models[[b2]]$rentals_count_1, "(6)" = models[[b2]]$rentals_count_1b, "(7)" = models[[b2]]$rentals_count_2b, "(8)" = models[[b2]]$rentals_count_3b,
    "(9)" = models[[b3]]$rentals_count_1, "(10)" = models[[b3]]$rentals_count_1b, "(11)" = models[[b3]]$rentals_count_2b, "(12)" = models[[b3]]$rentals_count_3b
  ),
  coef_map = coef_labels_count,
  title = "Near-vs-Far Difference-in-Differences of Sewage Spills (Count) on Property Values",
  label = "tbl:ud-did-count",
  notes = custom_notes_count,
  fname = "ud_did_count.tex"
)

# Models with river-distance control
export_did_table(
  models_sales = list(
    "(1)" = models[[b1]]$sales_count_4, "(2)" = models[[b1]]$sales_count_4b, "(3)" = models[[b1]]$sales_count_5b, "(4)" = models[[b1]]$sales_count_6b,
    "(5)" = models[[b2]]$sales_count_4, "(6)" = models[[b2]]$sales_count_4b, "(7)" = models[[b2]]$sales_count_5b, "(8)" = models[[b2]]$sales_count_6b,
    "(9)" = models[[b3]]$sales_count_4, "(10)" = models[[b3]]$sales_count_4b, "(11)" = models[[b3]]$sales_count_5b, "(12)" = models[[b3]]$sales_count_6b
  ),
  models_rentals = list(
    "(1)" = models[[b1]]$rentals_count_4, "(2)" = models[[b1]]$rentals_count_4b, "(3)" = models[[b1]]$rentals_count_5b, "(4)" = models[[b1]]$rentals_count_6b,
    "(5)" = models[[b2]]$rentals_count_4, "(6)" = models[[b2]]$rentals_count_4b, "(7)" = models[[b2]]$rentals_count_5b, "(8)" = models[[b2]]$rentals_count_6b,
    "(9)" = models[[b3]]$rentals_count_4, "(10)" = models[[b3]]$rentals_count_4b, "(11)" = models[[b3]]$rentals_count_5b, "(12)" = models[[b3]]$rentals_count_6b
  ),
  coef_map = c(coef_labels_count, "dist_river_m" = "River distance"),
  title = "Near-vs-Far Difference-in-Differences of Sewage Spills (Count) on Property Values, with River Distance",
  label = "tbl:ud-did-count-distance",
  notes = custom_notes_count,
  fname = "ud_did_count_distance.tex"
)

# ==============================================================================
# Export Tables: Spill Hours Daily Average
# ==============================================================================
cat("Exporting spill hours table...\n")

# Coefficient labels
coef_labels_hrs <- c(
  "(Intercept)"                            = "Constant",
  "spill_hrs_weekly_avg"                    = "Spill hours per week (avg.)",
  "direction"                              = "Upstream",
  "bin_near"                               = "Near ring",
  "spill_hrs_weekly_avg:direction"          = "Hours $\\times$ Upstream",
  "spill_hrs_weekly_avg:bin_near"           = "Hours $\\times$ Near",
  "direction:bin_near"                     = "Upstream $\\times$ Near",
  "spill_hrs_weekly_avg:direction:bin_near" = "{Hours $\\times$ Upstream \\\\ $\\times$ Near}"
)

# Notes
custom_notes_hrs <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents nearest-site difference-in-differences estimates of the relationship between sewage spill exposure and property values in England, 2021--2023. Each column group pools one near ring (0--250m, 250--500m, 500--750m) with the common 750--1000m reference ring; ``Near ring'' equals one for properties in the near ring and zero in the reference. Each property is matched to its single nearest overflow within 250m (lateral) of a river and 1000m of each other along the river. The dependent variable is the log transaction price in Panel A and the log weekly asking rent in Panel B. Spill exposure is measured as the average total number of spill hours per week recorded at the nearest overflow from January 2021 to the transaction date. ``Upstream'' equals one when the site is upstream of the property along the river; the triple interaction (hours $\\\\times$ upstream $\\\\times$ near ring) gives the difference in the upstream spill gradient between the near ring and the reference. Property controls include type, new build status and tenure for sales; and type, bedrooms and bathrooms for rentals. Heteroskedasticity-robust standard errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Models without river-distance control
export_did_table(
  models_sales = list(
    "(1)" = models[[b1]]$sales_hrs_1, "(2)" = models[[b1]]$sales_hrs_1b, "(3)" = models[[b1]]$sales_hrs_2b, "(4)" = models[[b1]]$sales_hrs_3b,
    "(5)" = models[[b2]]$sales_hrs_1, "(6)" = models[[b2]]$sales_hrs_1b, "(7)" = models[[b2]]$sales_hrs_2b, "(8)" = models[[b2]]$sales_hrs_3b,
    "(9)" = models[[b3]]$sales_hrs_1, "(10)" = models[[b3]]$sales_hrs_1b, "(11)" = models[[b3]]$sales_hrs_2b, "(12)" = models[[b3]]$sales_hrs_3b
  ),
  models_rentals = list(
    "(1)" = models[[b1]]$rentals_hrs_1, "(2)" = models[[b1]]$rentals_hrs_1b, "(3)" = models[[b1]]$rentals_hrs_2b, "(4)" = models[[b1]]$rentals_hrs_3b,
    "(5)" = models[[b2]]$rentals_hrs_1, "(6)" = models[[b2]]$rentals_hrs_1b, "(7)" = models[[b2]]$rentals_hrs_2b, "(8)" = models[[b2]]$rentals_hrs_3b,
    "(9)" = models[[b3]]$rentals_hrs_1, "(10)" = models[[b3]]$rentals_hrs_1b, "(11)" = models[[b3]]$rentals_hrs_2b, "(12)" = models[[b3]]$rentals_hrs_3b
  ),
  coef_map = coef_labels_hrs,
  title = "Near-vs-Far Difference-in-Differences of Sewage Spills (Hours) on Property Values",
  label = "tbl:ud-did-hrs",
  notes = custom_notes_hrs,
  fname = "ud_did_hrs.tex"
)

# Models with river-distance control
export_did_table(
  models_sales = list(
    "(1)" = models[[b1]]$sales_hrs_4, "(2)" = models[[b1]]$sales_hrs_4b, "(3)" = models[[b1]]$sales_hrs_5b, "(4)" = models[[b1]]$sales_hrs_6b,
    "(5)" = models[[b2]]$sales_hrs_4, "(6)" = models[[b2]]$sales_hrs_4b, "(7)" = models[[b2]]$sales_hrs_5b, "(8)" = models[[b2]]$sales_hrs_6b,
    "(9)" = models[[b3]]$sales_hrs_4, "(10)" = models[[b3]]$sales_hrs_4b, "(11)" = models[[b3]]$sales_hrs_5b, "(12)" = models[[b3]]$sales_hrs_6b
  ),
  models_rentals = list(
    "(1)" = models[[b1]]$rentals_hrs_4, "(2)" = models[[b1]]$rentals_hrs_4b, "(3)" = models[[b1]]$rentals_hrs_5b, "(4)" = models[[b1]]$rentals_hrs_6b,
    "(5)" = models[[b2]]$rentals_hrs_4, "(6)" = models[[b2]]$rentals_hrs_4b, "(7)" = models[[b2]]$rentals_hrs_5b, "(8)" = models[[b2]]$rentals_hrs_6b,
    "(9)" = models[[b3]]$rentals_hrs_4, "(10)" = models[[b3]]$rentals_hrs_4b, "(11)" = models[[b3]]$rentals_hrs_5b, "(12)" = models[[b3]]$rentals_hrs_6b
  ),
  coef_map = c(coef_labels_hrs, "dist_river_m" = "River distance"),
  title = "Near-vs-Far Difference-in-Differences of Sewage Spills (Hours) on Property Values, with River Distance",
  label = "tbl:ud-did-hrs-distance",
  notes = custom_notes_hrs,
  fname = "ud_did_hrs_distance.tex"
)

# ==============================================================================
# Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (file in exported_files) {
  cat("  - ", file, "\n", sep = "")
}
