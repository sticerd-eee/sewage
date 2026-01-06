# Long Difference Dataset: Data Pipeline Implementation

## Overview

This document provides a complete, implementable specification for creating a long-difference dataset that examines how changes in house prices are associated with changes in sewage spill frequency between 2021 and 2023 in England.

### Research Question

Do geographic areas that experienced increases in sewage spill frequency also experience relative declines in house prices?

### Purpose

This analysis serves as motivating evidence that the cross-sectional correlation between spill exposure and house prices (documented in the hedonic regression) also holds in changes. By differencing, we remove time-invariant unobserved characteristics of locations.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Spatial unit** | 250m × 250m grid cells | Matches spill exposure radius; creates comparable geographic units |
| **Grid extent** | Full England bounding box | Ensures consistent grid across analyses |
| **Spill window** | Calendar year totals (2021, 2022, 2023) | Creates distinct periods for differencing |
| **NA treatment** | Spills = NA if no sites within 250m | Distinguishes "no exposure possible" from "zero spills" |
| **Zero treatment** | Spills = 0 if site exists but no spills | Site exists but no events recorded |
| **Min transactions** | None (include `n_transactions` variable) | Allows flexible weighting in regression |
| **2022 data** | Included | Enables robustness checks (2021-22, 2022-23) |
| **LSOA** | Not included | Grid cell ID sufficient for identification |

---

## Input Data

### 1. House Price Data

**Path**: `data/processed/house_price.parquet`

| Variable | Type | Description |
|----------|------|-------------|
| `house_id` | character | Unique house identifier |
| `price` | numeric | Transaction price (GBP) |
| `date_of_transfer` | POSIXct | Transaction date |
| `easting` | numeric | British National Grid x-coordinate (CRS 27700) |
| `northing` | numeric | British National Grid y-coordinate (CRS 27700) |
| `property_type` | character | D(etached), S(emi), T(erraced), F(lat), Other |
| `old_new` | character | Y(es) = new build, N(o) = existing |
| `duration` | character | F(reehold), L(easehold) |
| `lsoa` | character | Lower Layer Super Output Area code |

### 2. House-Site Lookup

**Path**: `data/processed/spill_house_lookup.parquet`

| Variable | Type | Description |
|----------|------|-------------|
| `house_id` | character | Links to house price data |
| `site_id` | character | Spill site identifier |
| `distance_m` | numeric | Euclidean distance from house to site (meters) |

**Note**: Contains all house-site pairs within 10km. Filter to `distance_m <= 250` for 250m radius.

### 3. Annual Spill Statistics

**Path**: `data/processed/agg_spill_stats/agg_spill_yr.parquet`

| Variable | Type | Description |
|----------|------|-------------|
| `site_id` | character | Spill site identifier |
| `year` | integer | Calendar year (2021, 2022, 2023) |
| `water_company` | character | Water company identifier |
| `spill_count_yr` | integer | Total spill count for the year (EA methodology) |
| `spill_hrs_yr` | numeric | Total spill duration hours for the year |

### 4. Unique Spill Sites (Reference)

**Path**: `data/processed/unique_spill_sites.parquet`

| Variable | Type | Description |
|----------|------|-------------|
| `site_id` | character | Unique site identifier |
| `easting` | numeric | Site x-coordinate (CRS 27700) |
| `northing` | numeric | Site y-coordinate (CRS 27700) |
| `water_company` | character | Water company |

---

## Output Dataset (Long, Canonical)

**Path**: `data/processed/long_difference/long_diff_grid_house_sales.parquet`

**Unit of observation**: 250m × 250m grid cell × year

### Output Variables

#### Identifiers
| Variable | Type | Description |
|----------|------|-------------|
| `grid_cell_id` | character | Unique cell ID, format: `E{easting}_N{northing}` |
| `year` | integer | Calendar year (2021, 2022, 2023) |
| `centroid_easting` | numeric | Grid cell centroid easting (SW corner + 125m) |
| `centroid_northing` | numeric | Grid cell centroid northing (SW corner + 125m) |

#### Price Variables
| Variable | Type | Description |
|----------|------|-------------|
| `mean_log_price` | numeric | Mean of log(price) for transactions in the year |
| `median_log_price` | numeric | Median of log(price) for transactions in the year |
| `sd_log_price` | numeric | Standard deviation of log(price) in the year |

#### Spill Exposure Variables
| Variable | Type | Description |
|----------|------|-------------|
| `mean_spill_count` | numeric | Mean spill count across houses with nearby sites. NA if no nearby sites |
| `mean_spill_hrs` | numeric | Mean spill hours across houses with nearby sites. NA if no nearby sites |

#### Transaction Counts
| Variable | Type | Description |
|----------|------|-------------|
| `n_transactions` | integer | Number of transactions in cell in the year |
| `n_exposed_transactions` | integer | Transactions in cell within 250m of any spill site |

#### Composition Controls
| Variable | Type | Description |
|----------|------|-------------|
| `pct_detached` | numeric | Share of detached houses (0-1) |
| `pct_semi` | numeric | Share of semi-detached houses (0-1) |
| `pct_terraced` | numeric | Share of terraced houses (0-1) |
| `pct_flat` | numeric | Share of flats (0-1) |
| `pct_new_build` | numeric | Share of new builds (0-1) |
| `pct_freehold` | numeric | Share of freehold properties (0-1) |

---

## Implementation Steps

### Step 1: Load Data
Load the three inputs: house transactions, house-to-site lookup, and annual spill stats. Filter the lookup to `distance_m <= 250` so that only house transaction-to-site pairs within 250m are included. Keep only the fields needed for downstream joins (house IDs, site IDs, distances, years, spill counts/hours).

### Step 2: Prepare House Data
Derive a `year` from `date_of_transfer` and filter to 2021–2023. Create a `has_nearby_site` flag by checking whether each `house_id` appears in the filtered lookup. Keep all transactions, including those without nearby sites, so later aggregation can distinguish “no exposure possible” from “zero spills.”

### Step 3: Assign Grid Cells
Define a 250m grid and compute each transaction’s grid cell using the SW corner (`cell_easting`, `cell_northing`), then create `grid_cell_id` as `E{easting}_N{northing}`. Compute `log_price`. Filter to the England bounding box to ensure consistent coverage. Centroids are fixed to the grid (SW + 125m), not transaction-weighted.

### Step 4: Compute House-Level Spill Exposure by Calendar Year
For houses with nearby sites, join to spill stats by `site_id` and `year` and sum spill counts and hours across all sites within 250m for each house-year. When summing, ignore missing site-year values (explicit zeros stay zero). Also compute `n_sites` and `min_distance` for diagnostics. For houses without nearby sites, set spill measures to `NA` and `n_sites` to 0. Combine both sets into a complete house-year exposure table.

### Step 5: Aggregate to Grid-Cell × Year
Join house-year exposure back to transactions and aggregate by `grid_cell_id` and `year`. Compute price statistics (`mean_log_price`, `median_log_price`, `sd_log_price`). Compute `mean_spill_count` and `mean_spill_hrs` across exposed houses only, and ensure they are `NA` if no houses in the cell-year are exposed. Add `n_exposed_transactions` (count of non-NA spill exposure), total `n_transactions`, and composition shares for property type, new build, and freehold. Carry forward fixed `cell_easting` and `cell_northing` per grid cell.

### Step 6: Finalize Long Dataset
Compute fixed centroids from the grid (SW corner + 125m) and attach them to each grid-cell-year row. Ensure the long dataset has one row per `grid_cell_id` and `year`, with spill means set to `NA` when no houses in that cell-year are exposed.

### Step 7: Export Dataset(s)
Write the long canonical dataset to `data/processed/long_difference/long_diff_grid_house_sales.parquet`, creating the output directory if needed.

---

## Validation Checks

Run these checks after building the dataset to confirm the pipeline behaved as intended:

- Coverage by year: count of grid-cell-year rows and unique grid cells; verify expected counts for 2021–2023.
- Spill exposure coverage: share of grid-cell-year rows with `NA` spill exposure, and consistency with `n_exposed_transactions == 0`.
- Transaction coverage: distribution (mean/median/max) of `n_transactions` by year for plausibility.
- Exposure prevalence: verify `n_exposed_transactions <= n_transactions` and inspect its distribution.
- Coordinate ranges: ensure centroids fall within the England bounding box and match grid spacing.
- Price and spill sanity: summary stats by year for `mean_log_price`, `mean_spill_count`, and `mean_spill_hrs`, checking zeros vs `NA`.

---

## Expected Output Summary

After running the pipeline, you should expect:

- **Total grid-cell-year rows**: Depends on transaction coverage across England
- **Unique grid cells**: Subset of the grid where at least one transaction occurs in 2021–2023
- **Spill exposure coverage by year**: Share of grid-cell-years with at least one exposed transaction

---

## Usage Notes

1. **Coordinate System**: All coordinates use British National Grid (EPSG:27700) with eastings/northings in meters.

2. **Grid Cell ID**: The format `E{easting}_N{northing}` uses the SW corner coordinates. For example, `E500000_N200000` represents the cell with SW corner at (500000, 200000). The centroid uses the grid cell center (SW + 125m).

3. **Spill Exposure**: Uses 250m radius buffer consistent with the hedonic specification, but aggregates by calendar year rather than cumulative prior-to-sale.

4. **Missing Data**:
   - Houses with no spill sites within 250m: `spill_count = NA`, `spill_hrs = NA`
   - Houses near sites but no recorded spills: `spill_count = 0`, `spill_hrs = 0`

---

## File Structure
- `scripts/R/06_analysis_datasets/long_difference_grid.R`: Main implementation script.
- `data/processed/long_difference/long_diff_grid_house_sales.parquet`: Long canonical dataset.
