# ==============================================================================
# Sewage Spill Capitalization: Pooled Distance-Decay (Triple Interaction)
# ==============================================================================
#
# Purpose: Specification B. One pooled nearest-site regression in which the
#          Count(/Hours) x Upstream effect is allowed to differ across four
#          Euclidean distance rings, with 750-1000m as the OMITTED REFERENCE
#          category. The variable of interest is the triple interaction
#          spill_measure x direction x ring, which gives, for each near ring,
#          the difference in the upstream spill gradient relative to the
#          750-1000m reference ring (a difference-in-differences in space).
#
#          The full saturated set of lower-order terms is included so the triple
#          interaction is interpretable, and ALL interaction terms are reported
#          in the output tables.
#
#          Rings (cut on distance_m, the Euclidean property-site distance):
#            0-250m, 250-500m, 500-750m, 750-1000m (reference).
#          They are entered as three mutually-exclusive ring dummies
#          (near0 = 0-250m, near1 = 250-500m, near2 = 500-750m); the reference
#          (750-1000m) is all three equal to zero.
#
#          Sample filters (held fixed):
#            lateral distance to river <= 250m,
#            along-river distance       <= 1000m,
#            Euclidean property-site    <= 1000m,
#          keeping the single nearest spill site per property. River distance
#          (|signed_dist_m|) is an ENTIRELY SEPARATE object and enters as a
#          continuous control in the "+ distance" tables.
#
#          Four tables are exported (12 columns each: 6 sales + 6 rentals):
#            Table 1 — Count (no distance control)
#            Table 2 — Count + river distance
#            Table 3 — Hours (no distance control)
#            Table 4 — Hours + river distance
#
# Author: Alina Zeltikova
# Date: 2026-06-05
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale_house_site
#   - data/processed/house_price.parquet
#   - data/processed/cross_section/rentals/prior_to_rental_rental_site
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - upstream_downstream/output/18-03/river_filter/spill_house_signed.csv
#   - upstream_downstream/output/18-03/river_filter/spill_rental_signed.csv
#
# Outputs:
#   - output/tables/ud_decay_ring_triple_count.tex
#   - output/tables/ud_decay_ring_triple_count_distance.tex
#   - output/tables/ud_decay_ring_triple_hrs.tex
#   - output/tables/ud_decay_ring_triple_hrs_distance.tex
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RING_BREAKS    <- c(0, 250, 500, 750, 1000)
RING_LABELS    <- c("b0_250", "b250_500", "b500_750", "b750_1000")
REF_RING       <- "b750_1000"
MAX_DIST       <- 1000L   # Euclidean radius (cumulative cross-section)
LATERAL_CAP    <- 250L    # lateral distance to the river
RIVER_DIST_CAP <- 1000L   # along-river distance

# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow", "rio", "tidyverse", "purrr", "here", "janitor",
  "modelsummary", "sandwich", "fixest"
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

postprocess_table <- function(latex_str, label, notes) {
  fit_tblr_latex(
    latex_str,
    label = label,
    notes = notes,
    hspan = "even",
    rowsep = "0.1pt"
  )
}

make_notes <- function(measure_text) {
  paste0(
    "note{}={\\\\footnotesize{\\\\textbf{Notes:} This table presents a pooled ",
    "nearest-site hedonic regression in which the sewage-spill gradient is ",
    "allowed to vary across Euclidean distance rings, with the 750--1000m ring ",
    "as the omitted reference category, in England, 2021--2023. Each property is ",
    "matched to its single nearest overflow; properties and spill sites are ",
    "within ", LATERAL_CAP, "m (lateral) of a river and within ", RIVER_DIST_CAP,
    "m of each other along the river. The dependent variable is the log ",
    "transaction price for sales (columns 1--6) or log weekly asking rent for ",
    "rentals (columns 7--12). ", measure_text, " ``Upstream'' equals one when ",
    "the site is upstream of the property along the river. The ring dummies ",
    "(0--250m, 250--500m, 500--750m) are relative to the 750--1000m reference; ",
    "the triple interactions give the difference in the upstream spill gradient ",
    "in each near ring relative to that reference. River distance is the ",
    "along-river distance between property and site. Property controls include ",
    "type, new-build status and tenure for sales; and type, bedrooms and ",
    "bathrooms for rentals. Heteroskedasticity-robust standard errors are ",
    "reported in parentheses. *** p<0.01, ** p<0.05, * p<0.1.}},"
  )
}

# Shared GOF map
gof_map <- tibble::tribble(
  ~raw           , ~clean          , ~fmt ,
  "nobs"         , "Observations"  ,    0 ,
  "adj.r.squared", "Adj. R-squared",    3
)

# Shared add_rows (12-column: 6 sales + 6 rentals)
add_rows <- tibble::tribble(
  ~term                , ~"(1)" , ~"(2)" , ~"(3)" , ~"(4)" , ~"(5)" , ~"(6)" , ~"(7)" , ~"(8)" , ~"(9)" , ~"(10)" , ~"(11)" , ~"(12)" ,
  "Property controls"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"  , "No"   , "Yes"   , "No"    , "Yes"   ,
  "Location FE"        , "No"   , "No"   , "MSOA" , "MSOA" , "LSOA" , "LSOA" , "No"   , "No"   , "MSOA" , "MSOA"  , "LSOA"  , "LSOA"
)
attr(add_rows, "position") <- "coef_end"

# ==============================================================================
# 4. Load data
# ==============================================================================
cat("Loading house price data...\n")
sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  select(
    -transaction_id, -date_of_transfer, -quality, -paon, -saon, -street,
    -locality, -town_city, -district, -county, -ppd_category, -record_status
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type),
    old_new       = forcats::as_factor(old_new),
    duration      = forcats::as_factor(duration)
  )

cat("Loading rental data...\n")
rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
) |>
  select(
    -postcode, -listing_created, -latest_to_rent, -rented, -rented_est,
    -address_line_01, -address_line_02, -address_line_03
  ) |>
  mutate(
    property_type = forcats::as_factor(property_type)
  )

cat("Loading upstream/downstream (river) data...\n")
ud_sales_raw <- import(
  here::here("upstream_downstream", "output", "18-03",
             "river_filter", "spill_house_signed.csv")
)
ud_rentals_raw <- import(
  here::here("upstream_downstream", "output", "18-03",
             "river_filter", "spill_rental_signed.csv")
)

# ==============================================================================
# 5. Build the nearest-site sample with ring dummies (read 1km cross-section)
# ==============================================================================
# `near0`, `near1`, `near2` are mutually-exclusive ring dummies; the reference
# 750-1000m ring is all three equal to zero. Writing the formula as
#   spill * direction * near0 + spill * direction * near1 + spill * direction * near2
# reproduces the fully-saturated ring interaction with predictable coefficient
# names (e.g. spill_count_weekly_avg:direction:near0).

add_ring_dummies <- function(df) {
  df |>
    mutate(
      ring  = cut(distance_m, breaks = RING_BREAKS, labels = RING_LABELS,
                  right = TRUE, include.lowest = TRUE)
    ) |>
    filter(!is.na(ring)) |>
    mutate(
      near0 = as.integer(ring == "b0_250"),
      near1 = as.integer(ring == "b250_500"),
      near2 = as.integer(ring == "b500_750")
    )
}

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

dat_nearest_sales <- dat_cs_sales |>
  select(-any_of("price")) |>
  inner_join(sales, by = "house_id") |>
  inner_join(upstream_downstream_sales, by = c("house_id", "site_id")) |>
  mutate(log_price = log(price)) |>
  filter(
    !is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg),
    !is.na(lsoa), !is.na(msoa), !is.na(property_type), !is.na(old_new),
    !is.na(duration), !is.na(direction), !is.na(distance_m)
  ) |>
  arrange(house_id, distance_m) |>
  group_by(house_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(log_price = log(price)) |>
  add_ring_dummies() |>
  mutate(
    lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type),
    old_new       = forcats::fct_drop(old_new),
    duration      = forcats::fct_drop(duration)
  )

cat("  Sales nearest-site properties:", nrow(dat_nearest_sales), "\n")

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

dat_nearest_rentals <- dat_cs_rentals |>
  select(-any_of("listing_price")) |>
  inner_join(rentals, by = "rental_id") |>
  inner_join(upstream_downstream_rentals, by = c("rental_id", "site_id")) |>
  mutate(log_price = log(listing_price)) |>
  filter(
    !is.na(spill_count_weekly_avg), !is.na(spill_hrs_weekly_avg),
    !is.na(lsoa), !is.na(msoa), !is.na(property_type), !is.na(bedrooms),
    !is.na(bathrooms), !is.na(direction), !is.na(distance_m)
  ) |>
  arrange(rental_id, distance_m) |>
  group_by(rental_id) |>
  slice_head(n = 1) |>
  ungroup() |>
  mutate(log_price = log(listing_price)) |>
  add_ring_dummies() |>
  mutate(
    lsoa          = forcats::fct_drop(forcats::as_factor(lsoa)),
    msoa          = forcats::fct_drop(forcats::as_factor(msoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat("  Rental nearest-site properties:", nrow(dat_nearest_rentals), "\n")

# ==============================================================================
# 6. Estimate models
# ==============================================================================

# ---- Spill Count: Sales ----------------------------------------------------
cat("Estimating spill count models...\n")

model_sales_count_1 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_1b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + old_new + duration,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_2 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_2b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_3 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + old_new + duration | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_3b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + old_new + duration | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
# with river-distance control
model_sales_count_4 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_4b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_5 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_5b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_6 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_count_6b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)

# ---- Spill Count: Rentals --------------------------------------------------
model_rentals_count_1 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_1b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_2 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_2b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_3 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_3b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_4 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_4b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_5 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_5b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_6 <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_count_6b <- feols(
  log_price ~ spill_count_weekly_avg * direction * near0 + spill_count_weekly_avg * direction * near1 + spill_count_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)

# ---- Spill Hours: Sales ----------------------------------------------------
cat("Estimating spill hours models...\n")

model_sales_hrs_1 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_1b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + old_new + duration,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_2 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_2b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_3 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + old_new + duration | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_3b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + old_new + duration | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_4 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_4b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_5 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_5b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_6 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration | lsoa,
  data = dat_nearest_sales, vcov = "hetero"
)
model_sales_hrs_6b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + old_new + duration | msoa,
  data = dat_nearest_sales, vcov = "hetero"
)

# ---- Spill Hours: Rentals --------------------------------------------------
model_rentals_hrs_1 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_1b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_2 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_2b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_3 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_3b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + property_type + bedrooms + bathrooms | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_4 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_4b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_5 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_5b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_6 <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms | lsoa,
  data = dat_nearest_rentals, vcov = "hetero"
)
model_rentals_hrs_6b <- feols(
  log_price ~ spill_hrs_weekly_avg * direction * near0 + spill_hrs_weekly_avg * direction * near1 + spill_hrs_weekly_avg * direction * near2 + dist_river_m + property_type + bedrooms + bathrooms | msoa,
  data = dat_nearest_rentals, vcov = "hetero"
)

# ==============================================================================
# 7. Coefficient maps (all interaction terms reported)
# ==============================================================================
# Ring dummies: near0 = 0-250m, near1 = 250-500m, near2 = 500-750m
# (reference = 750-1000m). fixest names continuous:dummy interactions as
# "spill...:near0" and the triple as "spill...:direction:near0".

build_coef_map <- function(spill_var, spill_label, with_dist) {
  cm <- c(
    spill_var, "direction",
    "near0", "near1", "near2",
    paste0(spill_var, ":direction"),
    paste0(spill_var, ":near0"), paste0(spill_var, ":near1"), paste0(spill_var, ":near2"),
    paste0("direction:near0"), paste0("direction:near1"), paste0("direction:near2"),
    paste0(spill_var, ":direction:near0"),
    paste0(spill_var, ":direction:near1"),
    paste0(spill_var, ":direction:near2")
  )
  labs <- c(
    spill_label, "Upstream",
    "0--250m", "250--500m", "500--750m",
    paste0(spill_label, " $\\times$ Upstream"),
    paste0(spill_label, " $\\times$ 0--250m"),
    paste0(spill_label, " $\\times$ 250--500m"),
    paste0(spill_label, " $\\times$ 500--750m"),
    "Upstream $\\times$ 0--250m",
    "Upstream $\\times$ 250--500m",
    "Upstream $\\times$ 500--750m",
    paste0("{", spill_label, " $\\times$ Upstream \\\\ $\\times$ 0--250m}"),
    paste0("{", spill_label, " $\\times$ Upstream \\\\ $\\times$ 250--500m}"),
    paste0("{", spill_label, " $\\times$ Upstream \\\\ $\\times$ 500--750m}")
  )
  out <- setNames(labs, cm)
  if (with_dist) out <- c(out, "dist_river_m" = "River distance")
  out
}

# ==============================================================================
# 8. Export tables
# ==============================================================================

# ---- Table 1: Count (no distance control) ----------------------------------
cat("Exporting Table 1 (count)...\n")
coef_labels_count <- build_coef_map("spill_count_weekly_avg", "Spills per week (avg.)", with_dist = FALSE)
panels_count <- list(
  "House Sales" = list(
    "(1)" = model_sales_count_1, "(2)" = model_sales_count_1b,
    "(3)" = model_sales_count_2b, "(4)" = model_sales_count_3b,
    "(5)" = model_sales_count_2,  "(6)" = model_sales_count_3
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_count_1, "(8)" = model_rentals_count_1b,
    "(9)" = model_rentals_count_2b, "(10)" = model_rentals_count_3b,
    "(11)" = model_rentals_count_2, "(12)" = model_rentals_count_3
  )
)
tbl <- modelsummary::modelsummary(
  panels_count,
  shape = "cbind", output = "latex",
  estimate = "{estimate}{stars}", statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table, coef_map = coef_labels_count,
  gof_map = gof_map, add_rows = add_rows, notes = " ", escape = FALSE,
  title = "Distance Decay of Sewage Spills (Count) on Property Values: Triple Interaction with Distance Ring (ref. 750--1000m)"
)
tbl <- postprocess_table(
  tbl,
  label = "tbl:ud-decay-ring-triple-count",
  notes = make_notes("Spill exposure is the average number of spill events per week (12/24 count) at the nearest overflow from January 2021 to the transaction date.")
)
out_path <- file.path(output_dir, "ud_decay_ring_triple_count.tex")
writeLines(tbl, out_path)
record_export(out_path)

# ---- Table 2: Count + river distance ---------------------------------------
cat("Exporting Table 2 (count + river distance)...\n")
coef_labels_count_dist <- build_coef_map("spill_count_weekly_avg", "Spills per week (avg.)", with_dist = TRUE)
panels_count_dist <- list(
  "House Sales" = list(
    "(1)" = model_sales_count_4, "(2)" = model_sales_count_4b,
    "(3)" = model_sales_count_5b, "(4)" = model_sales_count_6b,
    "(5)" = model_sales_count_5,  "(6)" = model_sales_count_6
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_count_4, "(8)" = model_rentals_count_4b,
    "(9)" = model_rentals_count_5b, "(10)" = model_rentals_count_6b,
    "(11)" = model_rentals_count_5, "(12)" = model_rentals_count_6
  )
)
tbl <- modelsummary::modelsummary(
  panels_count_dist,
  shape = "cbind", output = "latex",
  estimate = "{estimate}{stars}", statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table, coef_map = coef_labels_count_dist,
  gof_map = gof_map, add_rows = add_rows, notes = " ", escape = FALSE,
  title = "Distance Decay of Sewage Spills (Count) on Property Values: Triple Interaction with Distance Ring and River Distance (ref. 750--1000m)"
)
tbl <- postprocess_table(
  tbl,
  label = "tbl:ud-decay-ring-triple-count-distance",
  notes = make_notes("Spill exposure is the average number of spill events per week (12/24 count) at the nearest overflow from January 2021 to the transaction date.")
)
out_path <- file.path(output_dir, "ud_decay_ring_triple_count_distance.tex")
writeLines(tbl, out_path)
record_export(out_path)

# ---- Table 3: Hours (no distance control) ----------------------------------
cat("Exporting Table 3 (hours)...\n")
coef_labels_hrs <- build_coef_map("spill_hrs_weekly_avg", "Spill hours per week (avg.)", with_dist = FALSE)
panels_hrs <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_1, "(2)" = model_sales_hrs_1b,
    "(3)" = model_sales_hrs_2b, "(4)" = model_sales_hrs_3b,
    "(5)" = model_sales_hrs_2,  "(6)" = model_sales_hrs_3
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_hrs_1, "(8)" = model_rentals_hrs_1b,
    "(9)" = model_rentals_hrs_2b, "(10)" = model_rentals_hrs_3b,
    "(11)" = model_rentals_hrs_2, "(12)" = model_rentals_hrs_3
  )
)
tbl <- modelsummary::modelsummary(
  panels_hrs,
  shape = "cbind", output = "latex",
  estimate = "{estimate}{stars}", statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table, coef_map = coef_labels_hrs,
  gof_map = gof_map, add_rows = add_rows, notes = " ", escape = FALSE,
  title = "Distance Decay of Sewage Spills (Hours) on Property Values: Triple Interaction with Distance Ring (ref. 750--1000m)"
)
tbl <- postprocess_table(
  tbl,
  label = "tbl:ud-decay-ring-triple-hrs",
  notes = make_notes("Spill exposure is the average total spill hours per week at the nearest overflow from January 2021 to the transaction date.")
)
out_path <- file.path(output_dir, "ud_decay_ring_triple_hrs.tex")
writeLines(tbl, out_path)
record_export(out_path)

# ---- Table 4: Hours + river distance ---------------------------------------
cat("Exporting Table 4 (hours + river distance)...\n")
coef_labels_hrs_dist <- build_coef_map("spill_hrs_weekly_avg", "Spill hours per week (avg.)", with_dist = TRUE)
panels_hrs_dist <- list(
  "House Sales" = list(
    "(1)" = model_sales_hrs_4, "(2)" = model_sales_hrs_4b,
    "(3)" = model_sales_hrs_5b, "(4)" = model_sales_hrs_6b,
    "(5)" = model_sales_hrs_5,  "(6)" = model_sales_hrs_6
  ),
  "House Rentals" = list(
    "(7)" = model_rentals_hrs_4, "(8)" = model_rentals_hrs_4b,
    "(9)" = model_rentals_hrs_5b, "(10)" = model_rentals_hrs_6b,
    "(11)" = model_rentals_hrs_5, "(12)" = model_rentals_hrs_6
  )
)
tbl <- modelsummary::modelsummary(
  panels_hrs_dist,
  shape = "cbind", output = "latex",
  estimate = "{estimate}{stars}", statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = fmt_table, coef_map = coef_labels_hrs_dist,
  gof_map = gof_map, add_rows = add_rows, notes = " ", escape = FALSE,
  title = "Distance Decay of Sewage Spills (Hours) on Property Values: Triple Interaction with Distance Ring and River Distance (ref. 750--1000m)"
)
tbl <- postprocess_table(
  tbl,
  label = "tbl:ud-decay-ring-triple-hrs-distance",
  notes = make_notes("Spill exposure is the average total spill hours per week at the nearest overflow from January 2021 to the transaction date.")
)
out_path <- file.path(output_dir, "ud_decay_ring_triple_hrs_distance.tex")
writeLines(tbl, out_path)
record_export(out_path)

# ==============================================================================
# 9. Summary
# ==============================================================================
cat("\nLaTeX tables exported to:", output_dir, "\n")
for (f in exported_files) cat("  - ", f, "\n", sep = "")
