# Sewage

## Overview
This project estimates the impact of sewage spills in the UK.

## Project Structure

The project is organised as follows:

```
├── _old/ # Material from previous RAs
│
├── book/ # Underlying files used to produce the project website
│
├── data/ # All project data
│   ├── final/ # Analysis-ready datasets
│   ├── processed/ # Intermediate processed data files
│   ├── raw/ # Original immutable data
│   └── temp/ # Intermediate processing files
│
├── docs/ # Documentation
│   ├── conferences/ # Conference materials
│   ├── data_requests/ # Data requests for external data
│   ├── lit_review/ # Literature review materials
│   └── reports/ # Project reports
│
├── output/ # Analysis outputs
│   ├── figures/ # Generated graphics
│   ├── html_plots/ # HTML plots and figures
│   └── tables/ # Generated tables
│
├── renv/ # R environment files
│   ├── library/ # Local R package library
│   ├── staging/ # Staging area for renv
│   ├── activate.R # Activate renv environment
│   └── settings.json # renv settings
│
├── scripts/ # Analysis code
│   ├── config/ # Configuration files
│   ├── python/ # Python scripts
│   ├── R/ # R scripts
│   └── stata/ # Stata scripts
│
├── sources/ # Reference materials
│   ├── articles/ # Published articles
│   ├── data_documentation/ # Dataset documentation
│   └── papers/ # Working papers
│
├── renv.lock # renv lockfile for package versions
├── sewage.Rproj # RStudio project file
└── ReadMe.md # Project README

```

## Data

### Dataset List

| Data Directory               | Source                                | Notes                                                                                                       | Citation |
|------------------------------|----------------------------------------|-------------------------------------------------------------------------------------------------------------|----------|
| `data/raw/edm_data/`         | UK Government: Environment Agency     | **Event Duration Monitoring (2021–2023):** Logs sewage spill event durations across all 10 WaSCs in England | n/a      |
| `data/raw/ea_consents/`      | UK Government: Environment Agency     | **Consented Discharges to Controlled Waters with Conditions:** Covers permit details under the Environmental Permit Regulations | n/a      |
| `data/raw/lr_house_price/`   | UK Government: HM Land Registry       | **Land Registry House Prices:** Price-paid data for property sales in England and Wales                     | n/a      |


#### Event Duration Monitoring

- **Data Files:** `data/raw/edm_data/`  
- **Source:** [UK Government: Environment Agency](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)  
- **Format:** XLSX, XLSB, CSV  
- **Notes:** Logs event durations for water treatment sites across all 10 Water and Sewerage Companies (WaSCs) in England, covering 2021 to 2023. The raw data includes individual event log files and annual aggregated data files. The aggregated data contains additional details—such as location—that are not included in the individual event logs.

#### Consented Discharges to Controlled Waters with Conditions

- **Data Files:** `data/raw/ea_consents/`  
- **Source:** [UK Government: Environment Agency](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109/consented-discharges-to-controlled-waters-with-conditions)  
- **Format:** ACCDB (exported to CSV)  
- **Notes:** Provides details of permit requirements under the Environmental Permit Regulations. Includes site information, consent holders, the date of permit issue or revocation, effluent types (e.g. sewage effluent, overflow), and location data in OS National Grid Reference format.

#### Land Registry House Prices

- **Data Files:** `data/raw/lr_house_price/`  
- **Source:** [UK Government: HM Land Registry](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file)  
- **Format:** CSV  
- **Notes:** Contains price-paid data for property sales in England and Wales that are sold for value and lodged for registration, covering 2021 to 2023. Includes sale date, address, property type, and sale price.

## Computational Requirements

### Software Requirements

- **R (version 4.4.3)**  
  - Packages:
    - Run `renv::status()` within R.
  - Installation:
    - Copy your project (including the `renv.lock` file) to a new machine.
    - Run `renv::restore()` to install the required packages.

## Scripts

### Data Cleaning and Preparation

1. **`01_edm_individ_data_standardisation.R`**  
   - **Input:** Original yearly zip archives (`EDM_{year}.zip`) from `data/raw/edm_data/` containing individual company EDM spill data files (XLSX, XLSB, CSV).
   - Unzips individual EDM data files.  
   - Standardises filenames to `{yyyy}_{company_name}`.  
   - **Output:** Unzipped individual EDM files with standardised names (`{yyyy}_{company_name}_edm.ext`) saved directly in `data/raw/edm_data/`.

2. **`02_convert_individ_raw_data_to_rdata.R`**  
   - **Input:** Standardised individual EDM data files (`{yyyy}_{company_name}_edm.ext`) from `data/raw/edm_data/`.
   - Reads the various file formats of individual EDM data.  
   - Converts these files to RData format.  
   - **Output:** Saves a single RData file (`data/processed/individual_edm_by_company.RData`) containing a list object where each element is a raw data frame for a specific company and year.

3. **`03_combine_individ_edm_data.R`**  
   - **Input:** The RData list object (`data/processed/individual_edm_by_company.RData`) containing raw individual company spill data.
   - Cleans and combines the individual sewage spill data (2021–2023) from multiple water companies.  
   - Standardises column names, water company names, and spill start/end dates to POSIXct.  
   - **Output:** A single, cleaned, and consolidated data frame of all individual spill events saved to `data/processed/combined_edm_data.RData` (and a `.csv` version).

4. **`04_combine_annual_return_data.R`**  
   - **Input:** Raw annual return Excel files (`{year}_annual_return_edm.xlsx`) from `data/raw/edm_data/`. These contain aggregated annual statistics per site.
   - Cleans and combines the annual sewage spill data for the years 2021–2023.  
   - Standardises column and company names.  
   - **Output:** A single, cleaned, and consolidated data frame of the annual return data saved to `data/processed/annual_return_edm.rds` (and a `.csv` version).

5. **`05_merge_individ_annual_location.R`**  
   - **Input:** The cleaned individual spill data (`data/processed/combined_edm_data.RData`) and the cleaned annual return data (`data/processed/annual_return_edm.rds`).
   - Merges location data (like NGR) from the annual aggregated files into the cleaned individual sewage spill dataset.  
   - **Output:** The individual spill data now augmented with location information, saved to `data/processed/merged_edm_data.RData` (and a `.csv` version). Optionally includes summary statistics in `merged_edm_data_with_summary.RData`.

6. **`06_clean_consented_discharges_database.R`**  
   - **Input:** The raw EA Consented Discharges database (`data/raw/ea_consents/consents_all.csv`).
   - Processes the Environment Agency's consented discharges data.  
   - Standardises column names and water company names.  
   - **Output:** The cleaned and standardised consented discharges database saved to `data/processed/consent_discharges_db.RData` (and a `.csv` version).

7. **`07_aggregate_spill_stats.R`**  
   - **Input:** The individual spill data merged with location information (`data/processed/merged_edm_data_with_summary.RData` or `merged_edm_data.RData`).
   - Aggregates individual sewage overflow spill data into total spill counts and durations for a given catchment area.  
   - Splits events that cross month boundaries and applies a 12/24 counting method for spill events.  
   - **Output:** Aggregated site-level spill statistics (counts and hours using 12/24 method), prepared for both yearly and monthly analysis, saved as an RData list object in `data/processed/individual_edm_location.RData` (with separate `.csv` files for yearly and monthly data).

8. **`08_clean_lr_house_price_data.R`**  
   - **Input:** Raw Land Registry Price Paid Data CSV files (`pp-{year}.csv`) from `data/raw/lr_house_price/`. Optionally uses cached postcode data (`data/raw/lr_house_price/postcode_data.rds`).
   - Cleans and combines the Land Registry Price Paid Data (2021–2023).
   - Merges postcodes with coordinate data (easting, northing, lat/lon) fetched from Postcodes.io to enable geospatial analysis.
   - **Output:** Exports the final tidy house price dataset with coordinates and a unique `house_id` to `data/processed/house_price.rds` (and `.csv`). Updates/creates postcode cache `data/raw/lr_house_price/postcode_data.rds`.

9. **`09_create_unique_spill_sites.R`**  
   - **Input:** Merged individual spill data (`data/processed/merged_edm_data.RData` or `data/processed/merged_edm_data_with_summary.RData`).
   - Creates unique spill sites from the merged EDM data by cleaning location data.
   - Filters invalid entries and standardises NGR format.
   - Converts grid references to eastings/northings coordinates using `rnrfa::osg_parse`.
   - **Output:** Exports a dataset of unique spill sites with `site_id`, `outlet_discharge_ngr`, easting/northing coordinates, and year availability flags to `data/processed/unique_spill_sites.rds`.

10. **`10_10km_site_house_sale_match.R`**  
    - **Input:** Cleaned house price data (`data/processed/house_price.rds`) with coordinates and unique spill sites data (`data/processed/unique_spill_sites.rds`) with coordinates.
    - Performs a spatial join using `sf::st_is_within_distance` to find all spill sites (`site_id`) located within a 10km radius of each house sale (`house_id`).
    - Calculates the exact straight-line distance (`distance_m`, `distance_km`) between each house and the identified nearby spill sites.
    - Counts the total number of discharge outlets (`n_discharge_outlet`) within the 10km radius for each house.
    - **Output:** Exports a lookup table containing pairs of `house_id` and `site_id` for all matches within 10km, along with the calculated distances. Saved to `data/processed/spill_house_lookup.rds` (and `.csv`).

11. **`11_cross_section_db.R`**  
    - **Input:** Loads `house_price.rds`, `spill_house_lookup.rds`, and the monthly spill data (from `individual_edm_location.RData`) into a DuckDB database (`data/duckdb.duckdb`). Uses tables `house_price_data`, `spill_lookup`, and `dat_mo`.
    - Creates two cross-sectional datasets aggregated at the **house level** (`house_id`) for different timeframes:
        1.  **All Years:** Aggregates total spill counts (`spill_count`), total spill hours (`spill_hrs`), number of unique spill sites (`n_spill_sites`), and mean/min distance (`mean_distance`, `min_distance`) across all available years (2021-2023) for various radii (250m to 10km) around each house.
        2.  **Prior 12 Months:** Aggregates the same statistics but only considers spills occurring in the 12 months *before* the house sale date (`transfer_date`). Includes sales from 2022 onwards.
    - **Output:** Exports the two aggregated cross-sectional datasets, partitioned by radius, into Parquet format:
        -   `data/processed/agg_all_yrs_cross_section/`
        -   `data/processed/agg_12mo_cross_section/`

12. **`12_site_mo_panel_db.R`**  
    - **Input:** Reads `house_price.rds`, `spill_house_lookup.rds`, and the monthly part of `individual_edm_location.RData` from `data/processed/` into a DuckDB database (`data/duckdb.duckdb`).
    - Constructs a monthly panel dataset at the **site level** (`site_id`) for regression analysis.
    - Aggregates house price data within various radii (250m to 10km) of each site for each month, calculating median/mean prices (weighted and unweighted by inverse distance).
    - Merges aggregated house prices with monthly site-level spillage metrics (raw counts/hours, log-transformed).
    - Creates treatment indicators based on whether a site's spillage exceeds monthly distribution thresholds (p50, p75, p90).
    - **Output:** Exports radius-specific site-level panel datasets, partitioned by radius, as Parquet files to `data/processed/dat_panel_site_mo/`.

13. **`13_house_mo_panel_db.R`**
    - **Input:** Reads `house_price.rds`, `spill_house_lookup.rds`, and the monthly part of `individual_edm_location.RData` from `data/processed/` into a DuckDB database (`data/duckdb.duckdb`). Also reads the site-level panel data created by script 12 (`data/processed/dat_panel_site_mo/`).
    - Constructs monthly panel datasets at the **house level** (`house_id`). Links each house sale to spill events occurring in the preceding 12 months.
    - Calculates two types of house-level exposure metrics for various radii (250m to 5km):
        1.  **Nearest Site:** Identifies the closest spill site within the radius and uses its spill data.
        2.  **Distance Weighted:** Aggregates spill data from all sites within the radius, weighted by inverse distance.
    - **Output:** Exports radius-specific house-level panel datasets, partitioned by radius, as Parquet files:
        -   `data/processed/dat_panel_house_nearest/`: Panel using nearest site spill data.
        -   `data/processed/dat_panel_house_weighted/`: Panel using distance-weighted spill data.

## Book Website

The project website, built using Quarto, documents the analysis process and findings. Key pages include:

-   **`index.qmd`**: The main landing page, providing recent updates and navigation.
-   **`data_limitations.qmd`**: Discusses challenges and limitations encountered during data processing, particularly regarding the merging of individual and annual EDM data and potential inaccuracies in spill duration records.
-   **`spill_data_exploration.qmd`**: Presents exploratory data analysis of the sewage spill data, covering geographic distribution, frequency/duration patterns, temporal trends (seasonality), and variations across different water companies.
-   **`house_data_exploration.qmd`**: Details the exploratory analysis of the HM Land Registry house price data, including descriptive statistics, price distributions, temporal patterns, and initial cross-sectional analysis linking house prices to nearby spill site characteristics (distance, spill severity).
-   **`event_study.qmd`**: Outlines the event study methodology used to estimate the causal impact of sewage spills on house prices, including the model setup (using `DIDmultiplegtDYN`), treatment definitions (binary and continuous), and results from various specifications.

## Contact

- Jacopo Olivieri, LSE, j.olivieri@lse.ac.uk

## References

1. 


---

Last updated: 01 April 2025