# Implementation Plan: News/Information DiD (Google Trends Peak)

## Objective
Estimate whether the capitalization of sewage spill exposure into house prices changes after public attention peaks, using a pre/post DiD where treatment is spill intensity (raw spill counts aggregated across 2021-2023) within 250m and post starts at the Google Trends peak month (inclusive).

---

## 1. Identification Strategy

### 1.1 DiD Design (Pre/Post)
**Outcome**: Log sale price (`log(price)`) for transactions in 2021-2023.
**Treatment**: Spill intensity = sum of 2021-2023 spill counts at nearby sites within 250m.
**Post**: Indicator for transactions on or after August 2022 (the Google Trends peak).

**Interpretation**: The interaction `spill_intensity × post` captures whether the spill-price gradient steepens after public awareness peaks.

### 1.2 Baseline Specification
```
log_price ~ spill_intensity + spill_intensity:post + controls | lsoa + month_id
```
Note: The main effect of `post` is absorbed by `month_id` fixed effects. We should use fixest to estimate this.

---

## 2. Data Inputs

### 2.1 Google Trends (peak month)
- **File**: `data/raw/google_trends/google_trends_uk.xlsx`
- **Sheet**: `united_kingdom`
- **Columns**:
  - `Date` (string, format "YYYY-MM")
  - `Year` (numeric)
  - `'Sewage Spill' Google Searches` (numeric, 0-100 scale)
- **Known peak**: August 2022 (value = 100). This corresponds to `month_id = 20`.
- **Reference code**: `scripts/R/09_analysis/01_descriptive/google_trends_sewage_spill.R`

**Reading pattern:**
```r
google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
) |>
  filter(Year >= 2021, Year <= 2023)

peak_month <- google_trends |>
  slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)
# Result: 2022-08 (August 2022)
```

### 2.2 Spill Intensity (2021-2023)
- **File**: `data/processed/agg_spill_stats/agg_spill_yr.parquet`
- **Key columns**: `site_id`, `year`, `spill_count_yr`
- Sum `spill_count_yr` across years 2021-2023 by `site_id` to get total site-level intensity.

**Reference pattern** (from `hedonic_spill_count.R:145-151`):
```r
spills <- import(here::here("data", "processed", "agg_spill_stats", "agg_spill_yr.parquet"))

spill_intensity_by_site <- spills |>
  filter(year >= 2021, year <= 2023) |>
  group_by(site_id) |>
  summarise(spill_intensity = sum(spill_count_yr, na.rm = TRUE))
```

### 2.3 Sales Transactions
- **File**: `data/processed/house_price.parquet`
- **Key columns**:
  - `house_id` - property identifier
  - `price` - sale price (GBP)
  - `month_id` - month index (1-36, where 1 = Jan 2021, 20 = Aug 2022)
  - `qtr_id` - quarter index (1-12)
  - `lsoa` - Lower Super Output Area (34,752 unique values)
  - `latitude`, `longitude` - for Conley SEs
  - `property_type`, `old_new`, `duration` - controls
- **Codebook**: `data/codebook_sewage.xlsx` (sheet: house_price)

### 2.4 House-Site Mapping (250m radius)
- **Path**: `data/processed/general_panel/sales/` (partitioned parquet by radius)
- **Key columns**: `house_id`, `site_id`, `distance_m`, `radius`, `within_radius`
- **Filter**: `radius == 250` (efficient partition filter)

**Reference pattern** (from `hedonic_spill_count.R:112-119`):
```r
gen_panel_sales <- arrow::open_dataset(
  here::here("data", "processed", "general_panel", "sales")
) |>
  filter(radius == 250L) |>
  collect() |>
  distinct(house_id, site_id, distance_m)
```

---

## 3. Construction Steps

### Step 1: Compute peak month from Google Trends data
```r
# Load and find peak month programmatically
google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
) |>
  filter(Year >= 2021, Year <= 2023)

# Find peak month (earliest if ties)
peak_row <- google_trends |>
  slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)

# Convert YYYY-MM to month_id (Jan 2021 = 1)
peak_year <- peak_row$Year
peak_month <- as.integer(substr(peak_row$Date, 6, 7))
PEAK_MONTH_ID <- (peak_year - 2021) * 12 + peak_month
# Expected result: 20 (August 2022)

cat(sprintf("Google Trends peak: %s (month_id = %d)\n", peak_row$Date, PEAK_MONTH_ID))
```

### Step 2: Aggregate spill intensity by site
```r
spill_intensity_by_site <- spills |>
  filter(year >= 2021, year <= 2023) |>
  group_by(site_id) |>
  summarise(spill_intensity = sum(spill_count_yr, na.rm = TRUE))
```

### Step 3: Map to houses via 250m panel
```r
house_spill_intensity <- gen_panel_sales |>
  filter(!is.na(site_id)) |>
  left_join(spill_intensity_by_site, by = "site_id") |>
  group_by(house_id) |>
  summarise(spill_intensity = sum(spill_intensity, na.rm = TRUE))
```

### Step 4: Merge with transactions and create variables
```r
dat <- sales |>
  left_join(house_spill_intensity, by = "house_id") |>
  mutate(
    spill_intensity = replace_na(spill_intensity, 0),
    log_price = log(price),
    post = as.integer(month_id >= PEAK_MONTH_ID)
  ) |>
  filter(!is.na(lsoa), !is.na(latitude), !is.na(longitude))
```

---

## 4. Estimation Plan

### 4.1 Main Model
```r
CONLEY_CUTOFF <- 0.5  # 500m

model_main <- fixest::feols(
  log_price ~ spill_intensity + spill_intensity:post +
              property_type + old_new + duration | lsoa + month_id,
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
```

**Reference**: Conley SE implementation in `repeat_sales_regs.R:408-419`

### 4.2 Alternative specifications
1. **No controls**: Drop property controls to check robustness
2. **Continuous treatment**: Use raw spill_intensity (baseline)
3. **Binned treatment**: Quartiles of spill_intensity for non-linearity

### 4.3 Standard Errors
- Conley spatial SEs with 0.5 km cutoff (consistent with other scripts)
- `fixest::conley()` auto-detects `latitude`/`longitude` columns

---

## 5. Validation and Checks

- [ ] Confirm post-period has adequate observations (~17 months: Aug 2022 - Dec 2023)
- [ ] Inspect spill_intensity distribution (zeros, outliers, quartiles)
- [ ] Verify coordinate missingness (expect ~0.4% missing based on house_price data)
- [ ] Check balance: compare pre/post sample sizes and property characteristics

---

## 6. Outputs

- **Primary table**: `output/tables/did_google_trends.tex`
- **Diagnostic**: Print pre/post observation counts and intensity summary stats

### Table export pattern (from `hedonic_spill_count.R:342-376`):
```r
modelsummary::modelsummary(
  models,
  output = "latex",
  coef_map = c("spill_intensity" = "Spill intensity",
               "spill_intensity:post" = "Spill intensity × Post"),
  gof_map = tibble::tribble(
    ~raw, ~clean, ~fmt,
    "nobs", "Observations", 0,
    "adj.r.squared", "Adj. R-squared", 3
  )
)
```

---

## 7. Required Packages
```r
required_packages <- c("arrow", "rio", "tidyverse", "here",
                       "readxl", "modelsummary", "fixest")
```
