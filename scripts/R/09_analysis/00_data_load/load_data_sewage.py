################################################################################
# Project: Sewage in Our Waters
# Script: Sewage Data Load
# Date: 2025-10-02
# Author: Jacopo Olivieri
################################################################################

################################################################################
# Load Packages
################################################################################

from pyprojroot import here
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq


################################################################################
# Define Data Paths
################################################################################

## House Prices
path_sales = here("data/processed/house_price.parquet")
path_rent = here("data/processed/zoopla/zoopla_rentals.parquet")

## Sewage Spills

### Aggregated Sewage Spills
path_spill_qtr = here("data/processed/agg_spill_stats/agg_spill_dry_qtr.parquet")
path_spill_mo = here("data/processed/agg_spill_stats/agg_spill_dry_mo.parquet")

### Aggregated Sewage Spill Statistics
path_spill_stats_qtr = here(
    "data/processed/agg_spill_stats/agg_spill_stats_qtr.parquet"
)
path_spill_stats_mo = here("data/processed/agg_spill_stats/agg_spill_stats_mo.parquet")

## Panel Data

### General
path_general_panel_rental = here("data/processed/general_panel/rentals")
path_general_panel_sales = here("data/processed/general_panel/sales")

### Within Radius
path_within_panel_rental = here("data/processed/within_radius_panel/rentals")
path_within_panel_sales = here("data/processed/within_radius_panel/sales")


################################################################################
# 1. General Panel Data Load
# (panel unit: house; observation unit: house transaction)
################################################################################

## Define radius
rad = 250  # 500, 1000

# 1.1 General Panel - Sales
################################################################################

## Load sales data
gen_panel_sales = (
    ds.dataset(path_general_panel_sales, format="parquet", partitioning="hive")
    .to_table(filter=ds.field("radius") == rad)
    .to_pandas()
)
sales = pd.read_parquet(path_sales)

## Load sewage spills
spills = pd.read_parquet(path_spill_qtr)

## Assemble sales data
dat_sales = gen_panel_sales.merge(sales, on=["house_id", "qtr_id"], how="left").merge(
    spills, on=["site_id", "qtr_id"], how="left"
)


# 1.2 General Panel - Rental
################################################################################

## Define radius
rad = 250  # 500, 1000

## Load rent data
gen_panel_rent = (
    ds.dataset(path_general_panel_rental, format="parquet", partitioning="hive")
    .to_table(filter=ds.field("radius") == rad)
    .to_pandas()
)
rent = pd.read_parquet(path_rent)

## Load sewage spills
spills = pd.read_parquet(path_spill_qtr)

## Assemble rent data
dat_rent = gen_panel_rent.merge(rent, on=["rental_id", "qtr_id"], how="left").merge(
    spills, on=["site_id", "qtr_id"], how="left"
)


################################################################################
# 2. Within Panel Data Load
# (panel unit: spill site; observation unit: house transaction)
################################################################################

# 2.1 Within Panel - Sales
################################################################################

## Define radius and time-period
rad = 250  # 500, 1000
period = "quarterly"  # monthly

## Load sales data
within_panel_sales = (
    ds.dataset(path_within_panel_sales, format="parquet", partitioning="hive")
    .to_table(filter=(ds.field("radius") == rad) & (ds.field("period_type") == period))
    .to_pandas()
    .drop(columns=["month_id"])  # qtr_id
)
sales = pd.read_parquet(path_sales)

## Load sewage spills
spills = pd.read_parquet(path_spill_qtr)

## Assemble sales data
dat_sales_within = within_panel_sales.merge(
    sales, on=["house_id", "qtr_id"], how="left"
).merge(spills, on=["site_id", "qtr_id"], how="left")  # month_id


# 2.2 Within Panel - Rental
################################################################################

## Define radius and time-period
rad = 250  # 500, 1000
period = "quarterly"  # monthly

## Load rent data
within_panel_rent = (
    ds.dataset(path_within_panel_rental, format="parquet", partitioning="hive")
    .to_table(filter=(ds.field("radius") == rad) & (ds.field("period_type") == period))
    .to_pandas()
    .drop(columns=["month_id"])  # qtr_id
)
rent = pd.read_parquet(path_rent)

## Load sewage spills
spills = pd.read_parquet(path_spill_qtr)

## Assemble rent data
dat_rent_within = within_panel_rent.merge(
    rent, on=["rental_id", "qtr_id"], how="left"
).merge(spills, on=["site_id", "qtr_id"], how="left")  # month_id
