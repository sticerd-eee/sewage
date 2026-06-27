# ==============================================================================
# Sewage Spill Capitalization at the Nearest Site, by Euclidean Distance Bin
# ==============================================================================
#
# Purpose: Estimate the upstream/downstream nearest-site specification of
#          upstream_downstream_nearest_all_radii.R, but instead of looping over
#          a cumulative radius, split the nearest-site sample into FOUR
#          NON-OVERLAPPING Euclidean distance bins and run every model
#          SEPARATELY within each bin so the effect can decay in distance:
#            0-250m, 250-500m, 500-750m, 750-1000m.
#
#          The 1km cross-section (all property-site pairs within 1000m) is read
#          once; the nearest site per property is kept (as in the nearest-site
#          script); each bin is then a filter on the nearest site's Euclidean
#          distance (distance_m).
#
#          Specification is IDENTICAL to upstream_downstream_nearest_all_radii.R:
#            log_price ~ spill_measure * direction [+ dist_river_m] [+ controls]
#                        [| LSOA or MSOA]
#          estimated on the single nearest spill site with heteroskedasticity-
#          robust standard errors. River distance (|signed_dist_m|) is an
#          ENTIRELY SEPARATE object from the Euclidean ring and enters as a
#          continuous control in the "+ distance" tables.
#
#          Four tables are exported separately per bin:
#            Table 1 — Count (no distance control)
#            Table 2 — Count + river distance
#            Table 3 — Hours (no distance control)
#            Table 4 — Hours + river distance
#
# Author: Alina Zeltikova
# Date: 2026-06-04
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale_house_site
#   - data/processed/house_price.parquet
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - upstream_downstream/output/18-03/river_filter/spill_house_signed.csv
#   - upstream_downstream/output/18-03/river_filter/spill_rental_signed.csv
#
# Outputs (per bin):
#   - output/tables/hedonic_count_continuous_prior_nearest_site_bin_{bin}.tex
#   - output/tables/hedonic_count_continuous_prior_nearest_site_distance_bin_{bin}.tex
#   - output/tables/hedonic_hrs_continuous_prior_nearest_site_bin_{bin}.tex
#   - output/tables/hedonic_hrs_continuous_prior_nearest_site_distance_bin_{bin}.tex
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RING_BREAKS    <- c(0, 250, 500, 750, 1000)
RING_LABELS    <- c("0-250m", "250-500m", "500-750m", "750-1000m")
MAX_DIST       <- 1000L   # read the 1km cross-section (radius is cumulative)
LATERAL_CAP    <- 250L # used to be 1000
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

# Shared table formatting helpers
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

# ==============================================================================
# 3. Setup
# ==============================================================================
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

exported_files <- character()
record_export <- function(path) {
  exported_files <<- c(exported_files, basename(path))
}

# Post-process modelsummary LaTeX output (tabularray styling, as in the earlier
# upstream/downstream scripts).
postprocess_table <- function(latex_str, label, notes) {
  fit_tblr_latex(
    latex_str,
    label = label,
    notes = notes,
    hspan = "even",
    rowsep = "0.1pt"
  )
}

make_notes <- function(bin_lab, measure_text) {
  paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents hedonic ",
    "estimates of the relationship between sewage spill exposure from the ",
    "nearest spill site and property values, restricted to properties whose ",
    "nearest storm overflow lies in the ", bin_lab, " ring (Euclidean ",
    "distance), in England, 2021--2023. Each property is matched to its single ",
    "nearest overflow; properties and spill sites are within ", LATERAL_CAP,
    "m of a river and within ", RIVER_DIST_CAP, "m of each other along the ",
    "river. The dependent variable is the log transaction price for sales ",
    "(columns 1--6) or log weekly asking rent for rentals (columns 7--12). ",
    measure_text, " ``Upstream spill site'' equals one when the site is ",
    "upstream of the property along the river. River distance is the ",
    "along-river distance between property and site. Property controls include ",
    "type (flat, semi-detached, terraced, other), new build status, and tenure ",
    "for sales; and type (bungalow, detached, semi-detached, terraced), ",
    "bedrooms, and bathrooms for rentals. Heteroskedasticity-robust standard ",
    "errors are reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )
}

# Shared GOF map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Shared add_rows (12-column: 6 sales + 6 rentals), as in the nearest-site script.
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"
)
attr(add_rows, "position") <- "coef_end"

# ==============================================================================
# 4. Load data that does NOT depend on the bin
# ==============================================================================
# `sales`, `rentals`, `ud_sales_raw`, `ud_rentals_raw`, `dat_cs_sales`,
# `dat_cs_rentals`, `upstream_downstream_sales`, `upstream_downstream_rentals`,
# `dat_sales_clean`, `dat_rental_clean` keep the names used in the earlier
# upstream/downstream scripts.

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
    old_new       = forcats::as_factor(old_new),
    duration      = forcats::as_factor(duration)
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
# 5. Build the nearest-site sample (1km cross-section, read ONCE)
# ==============================================================================
# Read all property-site pairs within MAX_DIST (radius is cumulative, so
# radius == MAX_DIST returns every pair with distance_m <= MAX_DIST), keep the
# single nearest site per property, then tag the nearest site's Euclidean
# distance with a non-overlapping ring. Each bin is later a filter on `ring`.

# ---- Panel A: Sales --------------------------------------------------------
cat("Loading sales cross-section (radius =", MAX_DIST, ")...\n")
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales",
             "prior_to_sale_house_site")
) |>
  filter(radius == MAX_DIST) |>
  collect()

upstream_downstream_sales <- ud_sales_raw |>
  mutate(
    direction    = ifelse(direction == -1, 1, 0),  # 1 = upstream, 0 = downstream
    dist_river_m = abs(signed_dist_m)
  ) |>
  filter(
    spill_lateral_m      <= LATERAL_CAP,
    house_lateral_m      <= LATERAL_CAP,
    dist_river_m         <= RIVER_DIST_CAP,
    spill_house_euclid_m <= MAX_DIST
  ) |>
  distinct()

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

# Keep the single nearest spill site per property, then assign the ring.
dat_nearest_sales <- dat_sales_clean |>
  arrange(house_id, distance_m) |>
  group_by(house_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(
    log_price = log(price),
    ring = cut(distance_m, breaks = RING_BREAKS, labels = RING_LABELS,
               right = TRUE, include.lowest = TRUE)
  ) |>
  filter(!is.na(ring))

cat("  Sales nearest-site properties (<=", MAX_DIST, "m):",
    nrow(dat_nearest_sales), "\n")

# ---- Panel B: Rentals ------------------------------------------------------
cat("Loading rental cross-section (radius =", MAX_DIST, ")...\n")
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals",
             "prior_to_rental_rental_site")
) |>
  filter(radius == MAX_DIST) |>
  collect()

upstream_downstream_rentals <- ud_rentals_raw |>
  mutate(
    direction    = ifelse(direction == -1, 1, 0),
    dist_river_m = abs(signed_dist_m)
  ) |>
  filter(
    spill_lateral_m       <= LATERAL_CAP,
    rental_lateral_m      <= LATERAL_CAP,
    dist_river_m          <= RIVER_DIST_CAP,
    spill_rental_euclid_m <= MAX_DIST
  ) |>
  distinct()

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

dat_nearest_rentals <- dat_rental_clean |>
  arrange(rental_id, distance_m) |>
  group_by(rental_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(
    log_price = log(listing_price),
    ring = cut(distance_m, breaks = RING_BREAKS, labels = RING_LABELS,
               right = TRUE, include.lowest = TRUE)
  ) |>
  filter(!is.na(ring))

cat("  Rental nearest-site properties (<=", MAX_DIST, "m):",
    nrow(dat_nearest_rentals), "\n")

# ==============================================================================
# 6. Estimate all models and export all tables for a given bin
# ==============================================================================

run_all_models <- function(bin_lab) {

  bin_slug <- gsub("[^0-9]", "_", bin_lab)   # e.g. "0_250"

  cat("\n========================================================\n")
  cat("  Running models for bin:", bin_lab, "\n")
  cat("========================================================\n")

  # Restrict to the bin and drop unused factor levels so FE match the subsample.
  dat_nearest_sales_bin <- dat_nearest_sales |>
    filter(ring == bin_lab) |>
    mutate(
      lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type),
      old_new       = forcats::fct_drop(old_new),
      duration      = forcats::fct_drop(duration)
    )

  dat_nearest_rentals_bin <- dat_nearest_rentals |>
    filter(ring == bin_lab) |>
    mutate(
      lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
      msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
      property_type = forcats::fct_drop(property_type)
    )

  cat("    Sales properties:", nrow(dat_nearest_sales_bin), "\n")
  cat("    Rental properties:", nrow(dat_nearest_rentals_bin), "\n")

  # ------------------------------------------------------------------
  # Estimate Models: Spill Count Daily Average, Nearest Spill Site
  # ------------------------------------------------------------------
  cat("  Estimating spill count models...\n")

  # Sales — no distance control
  model_sales_count_ns_1 <- feols(
    log_price ~ spill_count_weekly_avg * direction,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_1b <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + old_new + duration,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_2 <- feols(
    log_price ~ spill_count_weekly_avg * direction | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_2b <- feols(
    log_price ~ spill_count_weekly_avg * direction | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_3 <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_3b <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + old_new + duration | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )

  # Sales — with distance control
  model_sales_count_ns_4 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_4b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + old_new + duration,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_5 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_5b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_6 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_count_ns_6b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + old_new + duration | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )

  # Rentals — no distance control
  model_rentals_count_ns_1 <- feols(
    log_price ~ spill_count_weekly_avg * direction,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_1b <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_2 <- feols(
    log_price ~ spill_count_weekly_avg * direction | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_2b <- feols(
    log_price ~ spill_count_weekly_avg * direction | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_3 <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_3b <- feols(
    log_price ~ spill_count_weekly_avg * direction + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )

  # Rentals — with distance control
  model_rentals_count_ns_4 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_4b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_5 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_5b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_6 <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_count_ns_6b <- feols(
    log_price ~ spill_count_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )

  # ------------------------------------------------------------------
  # Estimate Models: Spill Hours Daily Average, Nearest Spill Site
  # ------------------------------------------------------------------
  cat("  Estimating spill hours models...\n")

  # Sales — no distance control
  model_sales_hrs_ns_1 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_1b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + old_new + duration,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_2 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_2b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_3 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_3b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + old_new + duration | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )

  # Sales — with distance control
  model_sales_hrs_ns_4 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_4b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + old_new + duration,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_5 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_5b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_6 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + old_new + duration | lsoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )
  model_sales_hrs_ns_6b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + old_new + duration | msoa,
    data = dat_nearest_sales_bin,
    vcov = "hetero"
  )

  # Rentals — no distance control
  model_rentals_hrs_ns_1 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_1b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_2 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_2b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_3 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_3b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )

  # Rentals — with distance control
  model_rentals_hrs_ns_4 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_4b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_5 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_5b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_6 <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )
  model_rentals_hrs_ns_6b <- feols(
    log_price ~ spill_hrs_weekly_avg * direction + dist_river_m + property_type + bedrooms + bathrooms | msoa,
    data = dat_nearest_rentals_bin,
    vcov = "hetero"
  )

  # ==================================================================
  # Table 1: Spill Count, Nearest Site (no distance control)
  # ==================================================================
  cat("  Exporting spill count nearest site table...\n")

  coef_labels_count <- c(
    "(Intercept)"                     = "Constant",
    "spill_count_weekly_avg"           = "Spills per week (avg.)",
    "direction"                       = "Upstream spill site",
    "spill_count_weekly_avg:direction" = "{Count per week \\\\ $\\times$ Upstream}"
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
      "(7)"  = model_rentals_count_ns_1,
      "(8)"  = model_rentals_count_ns_1b,
      "(9)"  = model_rentals_count_ns_2b,
      "(10)" = model_rentals_count_ns_3b,
      "(11)" = model_rentals_count_ns_2,
      "(12)" = model_rentals_count_ns_3
    )
  )

  tbl <- modelsummary::modelsummary(
    panels_count,
    shape     = "cbind",
    output    = "latex",
    estimate  = "{estimate}{stars}",
    statistic = "({std.error})",
    stars     = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt       = fmt_table,
    coef_map  = coef_labels_count,
    gof_map   = gof_map,
    add_rows  = add_rows,
    notes     = " ",
    escape    = FALSE,
    title     = paste0("Effect of Sewage Spills (Count) from the Nearest Site on Property Values by Direction (", bin_lab, ")")
  )
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-nearest-site-bin-", bin_slug),
    notes = make_notes(bin_lab, "Spill exposure is measured as the average number of spill events per week (12/24 count) recorded at the nearest overflow in the ring from January 2021 to the transaction date.")
  )
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_nearest_site_bin_", bin_slug, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)

  # ==================================================================
  # Table 2: Spill Count, Nearest Site + Distance Control
  # ==================================================================
  cat("  Exporting spill count nearest site + distance table...\n")

  coef_labels_count_dist <- c(
    "(Intercept)"                     = "Constant",
    "spill_count_weekly_avg"           = "Spills per week (avg.)",
    "direction"                       = "Upstream spill site",
    "spill_count_weekly_avg:direction" = "{Count per week \\\\ $\\times$ Upstream}",
    "dist_river_m"                    = "River distance"
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
      "(7)"  = model_rentals_count_ns_4,
      "(8)"  = model_rentals_count_ns_4b,
      "(9)"  = model_rentals_count_ns_5b,
      "(10)" = model_rentals_count_ns_6b,
      "(11)" = model_rentals_count_ns_5,
      "(12)" = model_rentals_count_ns_6
    )
  )

  tbl <- modelsummary::modelsummary(
    panels_count_dist,
    shape     = "cbind",
    output    = "latex",
    estimate  = "{estimate}{stars}",
    statistic = "({std.error})",
    stars     = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt       = fmt_table,
    coef_map  = coef_labels_count_dist,
    gof_map   = gof_map,
    add_rows  = add_rows,
    notes     = " ",
    escape    = FALSE,
    title     = paste0("Effect of Sewage Spills (Count) from the Nearest Site on Property Values by Direction and Distance (", bin_lab, ")")
  )
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-count-continuous-prior-nearest-site-distance-bin-", bin_slug),
    notes = make_notes(bin_lab, "Spill exposure is measured as the average number of spill events per week (12/24 count) recorded at the nearest overflow in the ring from January 2021 to the transaction date.")
  )
  out_path <- file.path(output_dir, paste0("hedonic_count_continuous_prior_nearest_site_distance_bin_", bin_slug, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)

  # ==================================================================
  # Table 3: Spill Hours, Nearest Site (no distance control)
  # ==================================================================
  cat("  Exporting spill hours nearest site table...\n")

  coef_labels_hrs <- c(
    "(Intercept)"                   = "Constant",
    "spill_hrs_weekly_avg"           = "Spill hours per week (avg.)",
    "direction"                     = "Upstream spill site",
    "spill_hrs_weekly_avg:direction" = "{Hours per week \\\\ $\\times$ Upstream}"
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
      "(7)"  = model_rentals_hrs_ns_1,
      "(8)"  = model_rentals_hrs_ns_1b,
      "(9)"  = model_rentals_hrs_ns_2b,
      "(10)" = model_rentals_hrs_ns_3b,
      "(11)" = model_rentals_hrs_ns_2,
      "(12)" = model_rentals_hrs_ns_3
    )
  )

  tbl <- modelsummary::modelsummary(
    panels_hrs,
    shape     = "cbind",
    output    = "latex",
    estimate  = "{estimate}{stars}",
    statistic = "({std.error})",
    stars     = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt       = fmt_table,
    coef_map  = coef_labels_hrs,
    gof_map   = gof_map,
    add_rows  = add_rows,
    notes     = " ",
    escape    = FALSE,
    title     = paste0("Effect of Sewage Spills (Hours) from the Nearest Site on Property Values by Direction (", bin_lab, ")")
  )
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-nearest-site-bin-", bin_slug),
    notes = make_notes(bin_lab, "Spill exposure is measured as the average total number of spill hours per week recorded at the nearest overflow in the ring from January 2021 to the transaction date.")
  )
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_nearest_site_bin_", bin_slug, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)

  # ==================================================================
  # Table 4: Spill Hours, Nearest Site + Distance Control
  # ==================================================================
  cat("  Exporting spill hours nearest site + distance table...\n")

  coef_labels_hrs_dist <- c(
    "(Intercept)"                   = "Constant",
    "spill_hrs_weekly_avg"           = "Spill hours per week (avg.)",
    "direction"                     = "Upstream spill site",
    "spill_hrs_weekly_avg:direction" = "{Hours per week \\\\ $\\times$ Upstream}",
    "dist_river_m"                  = "River distance"
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
      "(7)"  = model_rentals_hrs_ns_4,
      "(8)"  = model_rentals_hrs_ns_4b,
      "(9)"  = model_rentals_hrs_ns_5b,
      "(10)" = model_rentals_hrs_ns_6b,
      "(11)" = model_rentals_hrs_ns_5,
      "(12)" = model_rentals_hrs_ns_6
    )
  )

  tbl <- modelsummary::modelsummary(
    panels_hrs_dist,
    shape     = "cbind",
    output    = "latex",
    estimate  = "{estimate}{stars}",
    statistic = "({std.error})",
    stars     = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt       = fmt_table,
    coef_map  = coef_labels_hrs_dist,
    gof_map   = gof_map,
    add_rows  = add_rows,
    notes     = " ",
    escape    = FALSE,
    title     = paste0("Effect of Sewage Spills (Hours) from the Nearest Site on Property Values by Direction and Distance (", bin_lab, ")")
  )
  tbl <- postprocess_table(
    tbl,
    label = paste0("tbl:hedonic-hrs-continuous-prior-nearest-site-distance-bin-", bin_slug),
    notes = make_notes(bin_lab, "Spill exposure is measured as the average total number of spill hours per week recorded at the nearest overflow in the ring from January 2021 to the transaction date.")
  )
  out_path <- file.path(output_dir, paste0("hedonic_hrs_continuous_prior_nearest_site_distance_bin_", bin_slug, ".tex"))
  writeLines(tbl, out_path)
  record_export(out_path)

  cat("  Done for bin:", bin_lab, "\n")
}

# ==============================================================================
# 7. Run the loop over bins
# ==============================================================================
for (b in RING_LABELS) {
  run_all_models(b)
}

# ==============================================================================
# 8. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (f in exported_files) cat("  - ", f, "\n", sep = "")
