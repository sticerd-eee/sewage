# ==============================================================================
# Sewage Data Load
# ==============================================================================
#
# Purpose: Load and assemble panel datasets for sewage spill analysis. Creates
#          general panel (house-level) and within-radius panel (site-level)
#          datasets for both sales and rental transactions.
#
# Author: Jacopo Olivieri
# Date: 2025-10-02
#
# Inputs:
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#   - data/processed/agg_spill_stats/agg_spill_dry_qtr.parquet - Quarterly spills
#   - data/processed/agg_spill_stats/agg_spill_dry_mo.parquet - Monthly spills
#   - data/processed/general_panel/sales/ - General panel (Arrow dataset)
#   - data/processed/general_panel/rentals/ - General panel (Arrow dataset)
#   - data/processed/within_radius_panel/sales/ - Within panel (Arrow dataset)
#   - data/processed/within_radius_panel/rentals/ - Within panel (Arrow dataset)
#
# Outputs:
#   - dat_sales: General panel sales data (in memory)
#   - dat_rent: General panel rental data (in memory)
#   - dat_sales_within: Within-radius panel sales data (in memory)
#   - dat_rent_within: Within-radius panel rental data (in memory)
#
# ==============================================================================


# ==============================================================================
# 1. Package Management
# ==============================================================================

required_packages <- c("arrow", "here", "rio", "dplyr")

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)



# ==============================================================================
# 2. Define Data Paths
# ==============================================================================

## House Prices
path_sales <- here::here("data", "processed", "house_price.parquet")
path_rent <- here::here("data", "processed", "zoopla", "zoopla_rentals.parquet")

## Sewage Spills

### Aggregated Sewage Spills
path_spill_qtr <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_dry_qtr.parquet"
)
path_spill_mo <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_dry_mo.parquet"
)

### Aggregated Sewage Spill Statistics
path_spill_stats_qtr <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_stats_qtr.parquet"
)
path_spill_stats_mo <- here::here(
  "data",
  "processed",
  "agg_spill_stats",
  "agg_spill_stats_mo.parquet"
)

## Panel Data

### General
path_general_panel_rental <- here::here(
  "data",
  "processed",
  "general_panel",
  "rentals"
)
path_general_panel_sales <- here::here(
  "data",
  "processed",
  "general_panel",
  "sales"
)

### Within Radius
path_within_panel_rental <- here::here(
  "data",
  "processed",
  "within_radius_panel",
  "rentals"
)
path_within_panel_sales <- here::here(
  "data",
  "processed",
  "within_radius_panel",
  "sales"
)



# ==============================================================================
# 3. General Panel Data Load
# ==============================================================================
# Panel unit: house; observation unit: house transaction

## Define radius
rad <- 250L # 500, 1000

# 3.1 General Panel - Sales ------------------------------------------------

## Load sales data
gen_panel_sales <- arrow::open_dataset(path_general_panel_sales) |>
  filter(radius == rad) |>
  collect()
sales <- import(path_sales, trust = TRUE)

## Load sewage spills
spills <- import(path_spill_qtr, trust = TRUE)

## Assemble sales data
dat_sales <- gen_panel_sales |>
  left_join(sales, by = join_by(house_id, qtr_id)) |>
  left_join(spills, by = join_by(site_id, qtr_id))



# 3.2 General Panel - Rental -----------------------------------------------

## Define radius
rad <- 250L # 500, 1000

## Load rent data
gen_panel_rent <- arrow::open_dataset(path_general_panel_rental) |>
  filter(radius == rad) |>
  collect()
rent <- import(path_rent, trust = TRUE)

## Load sewage spills
spills <- import(path_spill_qtr, trust = TRUE)

## Assemble rent data
dat_rent <- gen_panel_rent |>
  left_join(rent, by = join_by(rental_id, qtr_id)) |>
  left_join(spills, by = join_by(site_id, qtr_id))



# ==============================================================================
# 4. Within Panel Data Load
# ==============================================================================
# Panel unit: spill site; observation unit: house transaction

# 4.1 Within Panel - Sales -------------------------------------------------

## Define radius and time-period
rad <- 250L # 500, 1000
period <- "quarterly" #monthly

## Load sales data
within_panel_sales <- arrow::open_dataset(path_within_panel_sales) |>
  filter(radius == rad, period_type == period) |>
  select(-month_id) |> 
  collect()
sales <- import(path_sales, trust = TRUE)

## Load sewage spills
spills <- import(path_spill_qtr, trust = TRUE)

## Assemble sales data
dat_sales_within <- within_panel_sales |>
  left_join(sales, by = join_by(house_id, qtr_id)) |> #month_id
  left_join(spills, by = join_by(site_id, qtr_id)) #month_id


# 4.2 Within Panel - Rental ------------------------------------------------

## Define radius and time-period
rad <- 250L # 500, 1000
period <- "quarterly" #monthly

## Load rent data
within_panel_rent <- arrow::open_dataset(path_within_panel_rental) |>
  filter(radius == rad, period_type == period) |>
  select(-month_id) |> 
  collect()
rent <- import(path_rent, trust = TRUE)

## Load sewage spills
spills <- import(path_spill_qtr, trust = TRUE)

## Assemble rent data
dat_rent_within <- within_panel_rent |>
  left_join(rent, by = join_by(rental_id, qtr_id)) |> #month_id
  left_join(spills, by = join_by(site_id, qtr_id)) #month_id