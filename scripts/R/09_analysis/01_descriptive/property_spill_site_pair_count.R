# ==============================================================================
# 
# ==============================================================================
#
# Purpose: Generate a descriptive statistics table on the number of properties,
#          spill sites, and share of spill sites per property.  
#
# Author: Alina Zeltikova
# Date: 2026-02-23
#
# Inputs:
#   - data/processed/cross_section/sales/prior_to_sale/house_site - Cross-sectional sales (house - spill site level)
#
# Outputs:
#   - output/tables/property_spill_site_pair_count.tex
#
# ==============================================================================

# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- list(250L, 500L, 1000L)
RAD <- 250L

# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "tidyverse",
  "here"
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

# ==============================================================================
# 4. Data Loading and Preparation
# ==============================================================================

# 5.1 Panel A: Sales -----------------------------------------------------------

# Load Sales Data --------------------------------------------------------------
cat("Loading sales - spill sites data...\n")

# Cross-section data with spill metrics (prior to sale)
## Filter for houses with at least one spill site within radius
dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale", "house_site")
) |>
  filter(radius == RAD) |>
  collect()

# 5.2 Panel B: Rentals ---------------------------------------------------------

# Load Rental Data -------------------------------------------------------------
cat("Loading rentals - spill sites data...\n")

# Cross-section data with spill metrics (prior to rental)
dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental", "rental_site")
) |>
  filter(radius == RAD) |>
  collect()

# ==============================================================================
# 5. Create and Save Table
# ==============================================================================
cat("Creating table...\n")

sites_houses <-  dat_cs_sales |>
  summarize(total_houses = n_distinct(house_id),
            total_sites = n_distinct(site_id),
            mean_distance = mean(distance_m))

site_summary_sales <- dat_cs_sales |>
  group_by(house_id) |>
  summarise(n_sites = n_distinct(site_id), .groups = "drop") |>
  summarise(
    min_sites_per_house    = min(n_sites),
    max_sites_per_house    = max(n_sites),
    mean_sites_per_house    = mean(n_sites),
    median_sites_per_house = median(n_sites),
    share_one_site         = mean(n_sites == 1),
    share_two_sites        = mean(n_sites == 2),
    share_three_sites      = mean(n_sites == 3),
    share_more_than_three  = mean(n_sites >  3)
  )

sites_rentals <-  dat_cs_sales |>
  summarize(total_houses = n_distinct(house_id),
            total_sites = n_distinct(site_id),
            mean_distance = mean(distance_m))

site_summary_rentals <- dat_cs_rentals |>
  group_by(rental_id) |>
  summarise(n_sites = n_distinct(site_id), .groups = "drop") |>
  summarise(
    min_sites_per_rental    = min(n_sites),
    max_sites_per_rental    = max(n_sites),
    mean_sites_per_rental    = mean(n_sites),
    median_sites_per_rental = median(n_sites),
    share_one_site         = mean(n_sites == 1),
    share_two_sites        = mean(n_sites == 2),
    share_three_sites      = mean(n_sites == 3),
    share_more_than_three  = mean(n_sites >  3)
  )

sites_rentals <-  dat_cs_rentals |>
  summarize(total_rentals = n_distinct(rental_id),
            total_sites = n_distinct(site_id),
            mean_distance = mean(distance_m))
