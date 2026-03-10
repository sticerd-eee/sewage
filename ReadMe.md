# Sewage in Our Waters

## Overview
This project estimates the causal impact of sewage spills on house prices in England using comprehensive event duration monitoring data (2021-2024+) and Land Registry property transactions. The analysis employs modern difference-in-differences methodologies to identify the effect of sewage overflow events on local property values, with particular attention to spatial and temporal heterogeneity in treatment effects.

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
│   ├── html_plots/ # Interactive HTML plots and figures
│   ├── regs/ # Regression results and model outputs
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
|   |   ├── setup.py # Creates Python venv
|   |   └── RiverNetwork.py # Creates River data with direction
│   ├── R/ # R scripts organised into 6 pipeline layers
│   │   ├── 01_data_ingestion/ # Raw data collection scripts
│   │   ├── 02_data_cleaning/ # Data cleaning and standardisation scripts
│   │   ├── 03_data_enrichment/ # Data aggregation and enrichment scripts
│   │   ├── 04_feature_engineering/ # Spatial analysis and feature engineering scripts
│   │   ├── 05_data_integration/ # Data integration and merging scripts
│   │   └── 06_analysis_datasets/ # Final dataset assembly scripts
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

| Data Directory                  | Source                            | Notes                                                                                                                                                                                                                                                                           | Citation                                                                                                        |
| ------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `data/raw/edm_data/`            | UK Government: Environment Agency | **Event Duration Monitoring (2021–2024+):** Historical company EDM files (2021-2023) plus live API snapshots for the nine England companies in the National Storm Overflows Hub feed (2024+)                                                                                      | [Environment Agency EDM](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)          |
| `data/raw/ea_consents/`         | UK Government: Environment Agency | **Consented Discharges to Controlled Waters with Conditions:** Site locations, permit details, and discharge consent information under Environmental Permit Regulations                                                                                                         | [EA Consents Data](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109)                        |
| `data/raw/haduk_rainfall_data/` | UK Government: Met Office         | **HadUK-Grid Rainfall Data (2020–2023):** Daily precipitation totals on 60km grid across UK from nationally consistent observational dataset. Used to identify "dry spills" occurring during minimal rainfall periods. Monthly NetCDF files with transverse mercator projection | [Met Office HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) |
| `data/raw/lr_house_price/`      | UK Government: HM Land Registry   | **Land Registry House Prices:** Complete property transaction records for England and Wales (2021-2024+) including sale prices, addresses, and property characteristics                                                                                                         | [Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)                |


#### Event Duration Monitoring

- **Data Files:** `data/raw/edm_data/`  
- **Source:** [UK Government: Environment Agency](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)  
- **Format:** XLSX, XLSB, CSV (historical), JSON (API)  
- **Coverage:** 2021-2024+ individual sewage overflow events  
- **Notes:** Historical data (2021-2023) comes from annual EDM archives containing company files. Live API snapshots (2024+) come from the England-only National Storm Overflows Hub feed for nine companies and are stored under `data/raw/edm_data/raw_api_responses/`; Welsh Water's separate public map is not part of this live API pipeline. Location data is merged from annual return files, and the project standardises formats across years before integration.

#### Consented Discharges to Controlled Waters with Conditions

- **Data Files:** `data/raw/ea_consents/`  
- **Source:** [UK Government: Environment Agency](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109/consented-discharges-to-controlled-waters-with-conditions)  
- **Format:** ACCDB (exported to CSV)  
- **Notes:** Provides details of permit requirements under the Environmental Permit Regulations. Includes site information, consent holders, the date of permit issue or revocation, effluent types (e.g. sewage effluent, overflow), and location data in OS National Grid Reference format.

#### Land Registry House Prices

- **Data Files:** `data/raw/lr_house_price/`  
- **Source:** [UK Government: HM Land Registry](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file)  
- **Format:** CSV  
- **Coverage:** 2021-2024+ property transactions  
- **Notes:** Complete price-paid dataset for property sales in England and Wales. Includes transaction date, full address, property type, sale price, and tenure information. Geocoded using Postcodes.io API to enable spatial analysis. Each property assigned unique identifier for panel analysis.

### Potential Data Sources

- **CoStar:**
  - **Source:** CoStar Group
  - **Data Type:** Commercial real estate data, including prices, analytics, and market information for the UK.
  - **Notes:** Could be used to extend the analysis to the commercial property sector or as a point of comparison for residential market impacts. Requires subscription or data purchase. The Geography & Environment Department at LSE may have access to fine-grained CoStar data for the UK (Contact: Christian Hilber).

## Computational Requirements

### Software Requirements

- **R (version 4.5.0+)**  
  - **Package Management:** Uses `renv` for reproducible environment
  - **Key Dependencies:** 
    - **Spatial Analysis:** `sf`, `terra`, `rnrfa` for geographic operations
    - **Data Processing:** `arrow`, `dplyr`, `DuckDB` for large dataset handling  
    - **Econometrics:** `DIDmultiplegtDYN`, `fixest` for causal inference
    - **Visualization:** `ggplot2`, `plotly`, `tinytable` for output generation
  - **Installation:**
    - Clone repository with `renv.lock` file
    - Run `renv::restore()` to install 100+ required packages

### Hardware Requirements

- **Memory:** Minimum 16GB RAM recommended for large dataset processing
- **Storage:** ~50GB for complete data pipeline (raw + processed + outputs)
- **Processing:** Multi-core CPU beneficial for spatial joins and model estimation

## Scripts

### Data Processing Pipeline

The project uses a pipeline organised into 6 layers to transform raw administrative data into analysis-ready datasets. Each layer contains scripts that handle specific aspects of the data processing workflow:

#### 01_data_ingestion/ - Raw Data Ingestion Layer

Scripts for collecting and standardising raw EDM and API inputs before cleaning.

**`edm_individ_data_standardisation_2021-2023.R`**  
   - **Input:** Raw FOI EDM zip archives (`EDM*.zip`) from `data/raw/edm_data/`.
   - **Output:** Extracted company files with standardised names (`{year}_{company}_edm.{extension}`) saved in `data/raw/edm_data/`.
   - **Purpose:** Unzips historical EDM archives and renames supported files into a consistent format for downstream ingestion.

**`fetch_edm_api_data_2024_onwards.R`**  
   - **Input:** Environment Agency ArcGIS EDM endpoints configured in `scripts/config/api_config.R`.
   - **Output:** Timestamped raw JSON API snapshots saved in `data/raw/edm_data/raw_api_responses/` by company.
   - **Purpose:** Downloads 2024+ EDM API data for the England-only live feed and stores raw responses for downstream processing.

#### 02_data_cleaning/ - Data Cleaning & Standardisation Layer

Scripts for cleaning, standardising, and combining raw inputs into consistent intermediate datasets.

**`clean_consented_discharges_database.R`**  
    - **Input:** Raw EA Consented Discharges database (`data/raw/ea_consents/consents_all.xlsx`).  
    - **Output:** Cleaned consented discharges database saved to `data/processed/consent_discharges_db.RData` and `data/processed/consent_discharges_db.csv`.
    - **Purpose:** Cleans the Environment Agency consented discharges database for downstream matching and analysis.

**`clean_lr_house_price_data.R`**  
    - **Input:** Raw Land Registry Price Paid Data CSV files (`pp-{year}.csv`) from `data/raw/lr_house_price/`.
    - **Output:** Canonical house price parquet saved to `data/processed/house_price.parquet`.
    - **Purpose:** Cleans and combines Land Registry sales data (currently 2021–2023) with local ONS postcode data.

**`clean_zoopla_data.R`**  
    - **Input:** WhenFresh/CDRC safeguarded Zoopla rentals CSVs from `data/raw/zoopla/` (`rentals_safeguarded_2014-2022.csv`, `rentals_safeguarded_2023.csv`).
    - **Output:** Canonical rentals parquet saved to `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Purpose:** Cleans and combines Zoopla rental data (currently 2021–2023) with local ONS postcode data.

**`combine_annual_return_data.R`**  
   - **Input:** Raw annual return workbooks (`{year}_annual_return_edm.xlsx`) from `data/raw/edm_data/`.
   - **Output:** Combined annual return parquet saved to `data/processed/annual_return_edm.parquet`.
   - **Purpose:** Combines and standardises annual return EDM workbooks for 2021–2024.

**`combine_api_edm_data_2024_onwards.R`**  
   - **Input:** Company-level API parquet files in `data/processed/edm_api_data/`.
   - **Output:** Combined API parquet saved to `data/processed/edm_api_data/combined_api_data.parquet`.
   - **Purpose:** Combines and the per-company 2024+ EDM API parquet files into one dataset.

**`combine_individ_edm_data_2021-2023.R`**  
   - **Input:** Company-level historical EDM parquet files in `data/processed/edm_data_2021_2023/`.
   - **Output:** Combined individual-EDM parquet saved to `data/processed/combined_edm_data.parquet`.
   - **Purpose:** Combines 2021–2023 individual EDM parquet files and standardises schema and datetime fields.

**`convert_individ_raw_data_to_rdata_2021-2023.R`**  
   - **Input:** Standardised raw EDM company files (`{year}_{company}_edm.xlsx/.xlsb/.csv`) from `data/raw/edm_data/`.
   - **Output:** Company-level and year-level parquet files saved in `data/processed/edm_data_2021_2023/`.
   - **Purpose:** Converts raw historical EDM files into standardised parquet outputs for downstream combination.

**`process_edm_api_json_to_parquet_2024_onwards.R`**  
   - **Input:** Raw API JSON snapshots from `data/raw/edm_data/raw_api_responses/` by company.
   - **Output:** Company-level API parquet files saved in `data/processed/edm_api_data/`.
   - **Purpose:** Processes raw 2024+ EDM API JSON snapshots into incremental parquet files.

**`clean_rainfall_data.R`**  
    - **Input:** HadUK-Grid NetCDF rainfall files from `data/raw/haduk_rainfall_data/` and unique spill sites from `data/processed/unique_spill_sites.parquet`.
    - **Output:** Cleaned rainfall parquet and spill-site lookup tables saved in `data/processed/rainfall/`.
    - **Purpose:** Extracts rainfall data for grid cells near spill sites and prepares rainfall inputs for spill-weather analysis.

#### 03_data_enrichment/ - Data Enrichment & Aggregation Layer

Scripts for aggregating raw data into statistics, creating lookup tables, and enriching datasets with additional information. Scripts handle temporal aggregation, site linkage, and statistical summarisation.

**`aggregate_spill_stats.R`**  
    - **Input:** Individual spill data merged with location information.
    - Aggregates individual sewage overflow events into site-level statistics (counts and durations).  
    - Implements 12/24 counting methodology and handles events crossing month boundaries.  
    - **Output:** Aggregated site-level spill statistics for yearly, monthly, and quarterly analysis saved to `data/processed/agg_spill_stats/`.

**`create_annual_return_lookup.R`**  
   - **Input:** Combined annual return data from data cleaning layer.
   - Creates lookup tables linking site identifiers across different data sources.
   - **Output:** Site lookup tables enabling data integration across datasets.

**`create_unique_spill_sites.R`**  
    - **Input:** Merged individual spill data with location information.
    - Creates unique spill sites dataset by cleaning and validating location data.
    - Converts National Grid References to eastings/northings coordinates.
    - **Output:** Dataset of unique spill sites with coordinates saved to `data/processed/unique_spill_sites.parquet`.

**`aggregate_rainfall_stats.R`**  
    - **Input:** Unique spill sites (`data/processed/unique_spill_sites.parquet`), cleaned rainfall (`data/processed/rainfall/rainfall_data_cleaned.parquet`), and site-to-grid lookup (`data/processed/rainfall/spill_site_grid_lookup.parquet`).
    - Builds a complete site-day panel and aggregates rainfall by site-year, site-month, and site-quarter using center-cell and 9-cell neighbourhood indicators.
    - Chunked processing for memory efficiency; logs to `output/log/aggregate_rainfall_stats.log`.
    - **Output:** `data/processed/rainfall/rainfall_agg_yr.parquet`, `rainfall_agg_mo.parquet`, `rainfall_agg_qtr.parquet`.

**`identify_dry_spills.R`**
    - **Input:** Individual spill data (`data/processed/matched_events_annual_data/matched_events_annual_data.parquet`), cleaned rainfall data (`data/processed/rainfall/rainfall_data_cleaned.parquet`), and spill site to grid cell lookup table (`data/processed/rainfall/spill_site_grid_lookup.parquet`).
    - Builds 12/24 counted spill blocks for yearly, monthly, and quarterly periods, then matches each block to rainfall using its start date.
    - **Output:** Block-level parquets with rainfall indicators saved to `data/processed/rainfall/spill_blocks_rainfall_{yr|mo|qt}.parquet`.

**`aggregate_dry_spill_stats.R`**
    - **Input:** Block-level parquets with rainfall indicators (`data/processed/rainfall/spill_blocks_rainfall_{yr|mo|qt}.parquet`) and existing spill aggregation files (`data/processed/agg_spill_stats/agg_spill_{yr|mo|qtr}.parquet`).
    - Classifies each counted block as dry/wet per indicator, then aggregates by counting dry blocks and summing their raw hours.
    - Creates standardised column naming convention and integrates dry spill metrics with existing aggregation datasets.
    - Implements zero imputation for periods with no dry spills and maintains compatibility with general spill analysis workflows.
    - **Output:** Integrated aggregation datasets with dry spill statistics saved to `data/processed/agg_spill_stats/agg_spill_dry_{yr|mo|qtr}.parquet`.

#### 04_feature_engineering/ - Feature Engineering & Spatial Processing Layer

Scripts for spatial analysis, distance calculations, and feature engineering. Scripts handle geographic matching, spatial joins, and statistical calculations for analysis preparation.

**`10km_site_house_sale_match.R`**  
    - **Input:** Geocoded house price data and unique spill sites with coordinates.
    - Performs spatial join to identify spill sites within 10km of each property.
    - Calculates exact distances and counts discharge outlets within radius for each house.
    - **Output:** Spatial lookup table with house-site pairs and distances saved to `data/processed/spill_house_lookup.parquet`.

**`10km_site_rental_match.R`**  
    - **Input:** Geocoded Zoopla rental data (`data/processed/zoopla/zoopla_rentals.parquet`) and unique spill sites with coordinates.
    - Performs spatial join to identify spill sites within 10km of each rental listing.
    - Calculates exact distances and counts discharge outlets within radius for each rental.
    - **Output:** Spatial lookup table with rental–site pairs and distances saved to `data/processed/zoopla/spill_rental_lookup.parquet`.

**`compute_spill_stats.R`**
    - **Input:** Dry spill aggregation files (`agg_spill_dry_mo.parquet`, `agg_spill_dry_qtr.parquet`).
    - Computes spill statistics including dry weather spill metrics, cross-site thresholds, and treatment indicators for monthly and quarterly periods.
    - Calculates period-specific, yearly, and all-time thresholds for both total spills and dry spills with binary indicator creation.
    - **Output:** Enhanced spill statistics with dry weather indicators saved to `agg_spill_stats_mo.parquet` and `agg_spill_stats_qtr.parquet`.

#### 05_data_integration/ - Integration Layer

Scripts for merging and integrating data from different sources and time periods. Scripts handle data matching, location integration, and harmonisation of datasets with different schemas.

**`combine_2021-2023_and_api_edm_data.R`**  
   - **Input:** Historical combined data (2021-2023) and API data (2024+).
   - Merges historical and modern datasets to create complete time series.
   - Ensures consistency across data collection methodologies and formats.
   - **Output:** Complete EDM dataset spanning 2021-2024+ saved to `data/processed/combined_edm_data.parquet`.

**`merge_individ_annual_location.R`**  
    - **Input:** Individual spill data and annual return data with location information.
    - Merges location data (National Grid References) from annual files into individual spill records.  
    - **Output:** Individual spill data augmented with location information saved to `data/processed/merged_edm_data.RData`.

#### 06_analysis_datasets/ - Master Dataset Assembly Layer

Scripts for creating final analysis-ready datasets for econometric analysis. Scripts handle panel construction, treatment variable creation, and final dataset optimisation for different analytical approaches.

**`cross_section_sales.R`**
    - **Input:** House price data, spill-house lookup table, and monthly spill data loaded into DuckDB.
    - Creates cross-sectional datasets aggregated at house level for multiple timeframes and spatial radii.
    - Calculates spill exposure metrics: total counts, total hours, number of sites, mean/minimum distances.
    - **Output:** Partitioned by `radius` under `data/processed/cross_section/sales/` in two directories: `all_years/` and `prior_12mo/`.

**`cross_section_rental.R`**  
    - **Input:** Zoopla rentals data (`zoopla_rentals.parquet`), rental–spill lookup (`spill_rental_lookup.parquet`), and monthly spill data loaded into DuckDB.
    - Creates cross-sectional datasets aggregated at rental level for multiple timeframes and spatial radii (12‑month window anchored on `rented_est`).
    - Calculates spill exposure metrics and distance summaries as per sales.
    - **Output:** Partitioned by `radius` under `data/processed/cross_section/rentals/` in two directories: `all_years/` and `prior_12mo/`.

**`cross_section_prior_to_sale.R`**
    - **Input:** House price data (`house_price.parquet`), spill-house lookup table (`spill_house_lookup.parquet`), and matched spill events (`matched_events_annual_data.parquet`).
    - Creates cross-sectional datasets aggregated from January 1, 2021 to the day before each house sale.
    - Calculates spill exposure metrics: total counts, total hours, daily averages, number of sites, mean/minimum distances.
    - Handles houses with no nearby sites (zeros, NA distances) and sites with no spill events (zeros, actual distances).
    - **Output:** Partitioned by `radius` under `data/processed/cross_section/sales/prior_to_sale/`.

**`cross_section_prior_to_rental.R`**
    - **Input:** Zoopla rentals data (`zoopla_rentals.parquet`), rental-spill lookup (`spill_rental_lookup.parquet`), and matched spill events (`matched_events_annual_data.parquet`).
    - Creates cross-sectional datasets aggregated from January 1, 2021 to the day before each rental.
    - Calculates spill exposure metrics: total counts, total hours, daily averages, number of sites, mean/minimum distances.
    - Handles rentals with no nearby sites (zeros, NA distances) and sites with no spill events (zeros, actual distances).
    - **Output:** Partitioned by `radius` under `data/processed/cross_section/rentals/prior_to_rental/`.

**`sale_panel_exp.R`**  
    - **Input:** House-level panel data from sale panel scripts.
    - Creates panel datasets linking house sale transactions to nearby sewage spill sites across different radii. The datasets can be joined with the house_price data (for house price data), and spill statistics data.
    - **Output:** Final analysis datasets saved to `data/final/dat_event/` and `data/final/dat_hedonic/`.

**`rental_panel_exp.R`**
    - **Input:** Zoopla rental listings (`zoopla_rentals.parquet`), rental–spill lookup (`spill_rental_lookup.parquet`), and quarterly spill statistics.
    - Creates panel datasets linking rental transactions to nearby sewage spill sites across different radii. The datasets can be joined with the zoopla_rentals data, and spill statistics data.
    - **Output:** Partitioned by `radius` under `data/processed/general_panel/rentals/`.

**`house_panel_within_radius.R`**  
    - **Input:** House price data, spill lookup table, and site-level panel data.
    - Constructs house-level panel datasets linking property transactions to preceding 12-month spill exposure.
    - Implements two exposure metrics: nearest site and distance-weighted aggregation.
    - **Output:** House-level panel datasets for various radii saved to `data/processed/within_radius_panel/sales/`.

**`rental_panel_within_radius.R`**  
    - **Input:** Zoopla rental listings, rental spill lookup tables, and site-level panel data.
    - Mirrors the sales workflow to build rental-level panels linking lets to nearby spill sites.
    - Implements the same nearest-site and distance-weighted exposure measures.
    - **Output:** Rental-level panel datasets saved to `data/processed/within_radius_panel/rentals/`.

**`grid_long_difference_sales.R`**
    - **Input:** House price data (`house_price.parquet`), house-spill lookup (`spill_house_lookup.parquet`), annual spill statistics (`agg_spill_yr.parquet`).
    - Creates 250m × 250m grid-level dataset for long-difference analysis of house prices and sewage spill exposure.
    - Aggregates transactions and spill exposure by grid cell and calendar year (2021–2023).
    - Computes mean log prices, spill counts/hours, transaction counts, and composition controls.
    - **Output:** `data/processed/long_difference/long_diff_grid_house_sales.parquet`.

**`grid_long_difference_rentals.R`**
    - **Input:** Zoopla rental data (`zoopla_rentals.parquet`), rental-spill lookup (`spill_rental_lookup.parquet`), annual spill statistics (`agg_spill_yr.parquet`).
    - Creates 250m × 250m grid-level dataset for long-difference analysis of rental prices and sewage spill exposure.
    - Aggregates rentals and spill exposure by grid cell and calendar year (2021–2023).
    - Computes mean log prices, spill counts/hours, transaction counts, and composition controls.
    - **Output:** `data/processed/long_difference/long_diff_grid_rentals.parquet`.

**`site_panel_sales.R`**  
    - **Input:** House price data, spill lookup table, and monthly spill data in DuckDB database.
    - Constructs monthly/quarterly panel datasets at site level for regression analysis.
    - Aggregates house prices within various radii of each site using distance weighting.
    - **Output:** Site-level panel datasets partitioned by radius saved to `data/processed/dat_panel_site/sales/`.

**`site_panel_rental.R`**  
    - **Input:** Zoopla rental listings and spill-to-rental lookup tables stored in DuckDB.
    - Mirrors the sales workflow to build site-level rental panels with monthly and quarterly aggregations.
    - Aggregates advertised rents within the same distance radii using distance-weighted statistics.
    - **Output:** Rental site-level panel datasets saved to `data/processed/dat_panel_site/rentals/`.

### Utilities

- `scripts/R/utils/postcode_processing_utils.R`: Shared postcode utilities. Sales and rentals now use local ONS postcode and code-name lookups (`load_local_postcode_lookup`, `build_local_postcode_label_lookup`, `get_local_postcode_data_for_sales`) instead of the old Postcodes.io/cache workflow.
- `scripts/R/utils/spill_aggregation_utils.R`: Shared spill aggregation helpers including `split_monthly_records`, `split_quarterly_records`, `prepare_spill_data`, 12/24 counting via `count_spills`, block construction via `build_spill_blocks`, and `calculate_spill_hours`.

### Analysis Scripts (09_analysis)

All analysis scripts estimate effects on both house sales and rental prices unless otherwise noted.

#### 01_descriptive/ - Descriptive figures

Scripts for generating descriptive visualisations of spill patterns, price relationships, and media coverage trends.

**`cross_sectional_plots.R`**
    - **Input:** `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/cross_section/sales/all_years/`, `data/processed/cross_section/rentals/all_years/`.
    - **Description:** Bivariate scatter plots of log prices against spill metrics (count, hours, distance) at 250m, 500m, and 1km radii for sales and rentals.
    - **Output:** `output/figures/sales_{variable}_lm.pdf`, `output/figures/rental_{variable}_lm.pdf`.

**`google_trends_sewage_spill.R`**
    - **Input:** `data/raw/google_trends/google_trends_uk.xlsx`.
    - **Description:** Time series of Google Trends "Sewage spill" search interest (2018-2024) with event annotations for key media coverage dates.
    - **Output:** `output/figures/google_trends_sewage_spill_uk.pdf`.

**`google_trends_article_counts_combined.R`**
    - **Input:** `data/raw/google_trends/google_trends_uk.xlsx`, `data/processed/lexis_nexis/search1_monthly.parquet`.
    - **Description:** Dual y-axis time series combining Google Trends and LexisNexis article counts with aligned scales and event annotations.
    - **Output:** `output/figures/google_trends_article_counts_combined.pdf`.

**`spill_maps.R`**
    - **Input:** `data/processed/agg_spill_stats/agg_spill_yr.parquet`, `data/processed/agg_spill_stats/agg_spill_dry_yr.parquet`, `data/processed/unique_spill_sites.parquet`, `data/raw/shapefiles/msoa_bcg_2021/`, `data/raw/shapefiles/msoa_population_2021/sapemsoasyoatablefinal.xlsx`.
    - **Description:** MSOA-level choropleth maps of log spill counts (total and dry) for 2021-2023.
    - **Output:** `output/figures/maps/spill_total_count_2021_2023.pdf`, `output/figures/maps/dry_spill_total_count_2021_2023.pdf`.

**`spill_maps_inset.R`**
    - **Input:** Same as `spill_maps.R`.
    - **Description:** MSOA-level choropleth maps with circular Greater London inset.
    - **Output:** `output/figures/maps/spill_total_count_2021_2023_inset.pdf`, `output/figures/maps/dry_spill_total_count_2021_2023_inset.pdf`.

**`spill_phase_diagrams.R`**
    - **Input:** `data/processed/agg_spill_stats/agg_spill_yr.parquet`.
    - **Description:** Year-to-year transition probability heatmaps showing persistence of spill severity across sites (5 states: 0, Q1-Q4).
    - **Output:** `output/figures/spill_count_persistence.pdf`, `output/figures/spill_hours_persistence.pdf`.

#### 02_hedonic/ - Hedonic price regressions

Cross-sectional hedonic regressions of log prices on spill exposure. Scripts vary by:
- **Time period**: `prior` = exposure from 1 Jan 2021 to day before transaction; `full` = exposure over full 2021-2023 period
- **Functional form**: `continuous` = linear treatment; `bins` = quartile indicators
- **Treatment measure**: All scripts estimate both spill count and spill hours

**`hedonic_spill_count.R`**
    - **Input:** `data/processed/agg_spill_stats/agg_spill_yr.parquet`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/general_panel/sales/`, `data/processed/general_panel/rentals/`.
    - **Specification:** Full period, continuous, count only. Includes OLS, controls, and fixed effects variants.
    - **Output:** `output/tables/hedonic_spill_count.tex`.

**`hedonic_continuous_prior.R`**
    - **Input:** `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** Prior-to-transaction period, continuous, count and hours.
    - **Output:** `output/tables/hedonic_count_continuous_prior.tex`, `output/tables/hedonic_hrs_continuous_prior.tex`.

**`hedonic_bins_prior.R`**
    - **Input:** `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** Prior-to-transaction period, quartile bins, count and hours.
    - **Output:** `output/tables/hedonic_count_bins_prior.tex`, `output/tables/hedonic_hrs_bins_prior.tex`.

**`hedonic_continuous_full.R`**
    - **Input:** `data/processed/agg_spill_stats/agg_spill_yr.parquet`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/general_panel/sales/`, `data/processed/general_panel/rentals/`.
    - **Specification:** Full 2021-2023 period, continuous, count and hours.
    - **Output:** `output/tables/hedonic_count_continuous_full.tex`, `output/tables/hedonic_hrs_continuous_full.tex`.

**`hedonic_bins_full.R`**
    - **Input:** `data/processed/agg_spill_stats/agg_spill_yr.parquet`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/general_panel/sales/`, `data/processed/general_panel/rentals/`.
    - **Specification:** Full 2021-2023 period, quartile bins, count and hours.
    - **Output:** `output/tables/hedonic_count_bins_full.tex`, `output/tables/hedonic_hrs_bins_full.tex`.

#### 03_repeat_sales/ - Repeat sales and rentals

Repeat-transaction regressions exploiting within-property price variation.

**`repeat_sales.R`**
    - **Input:** `data/processed/repeated_transactions/repeated_sales.parquet`, `data/processed/repeated_transactions/repeated_rentals.parquet`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/spill_house_lookup.parquet`, `data/processed/zoopla/spill_rental_lookup.parquet`, `data/processed/agg_spill_stats/agg_spill_qtr.parquet`.
    - **Specification:** Palmquist (1982) approach with rolling 4-quarter spill exposure within 250m radius.
    - **Output:** `output/tables/repeat_sales.tex`.

#### 04_long_difference/ - Long-difference grid regressions

Long-difference regressions at the 250m grid-cell level. Scripts vary by:
- **Sample**: `all` = all grid cells; `exposed` = cells with nearby spill sites only
- **Weighting**: `unweighted` = equal weights; `weighted` = transaction-count weights

**`longdiff_unweighted_all.R`**
    - **Input:** `data/processed/long_difference/long_diff_grid_house_sales.parquet`, `data/processed/long_difference/long_diff_grid_rentals.parquet`.
    - **Specification:** All grid cells, unweighted.
    - **Output:** `output/tables/longdiff_unweighted_all.tex`.

**`longdiff_unweighted_exposed.R`**
    - **Input:** `data/processed/long_difference/long_diff_grid_house_sales.parquet`, `data/processed/long_difference/long_diff_grid_rentals.parquet`.
    - **Specification:** Exposed grid cells only, unweighted.
    - **Output:** `output/tables/longdiff_unweighted_exposed.tex`.

**`longdiff_weighted_all.R`**
    - **Input:** `data/processed/long_difference/long_diff_grid_house_sales.parquet`, `data/processed/long_difference/long_diff_grid_rentals.parquet`.
    - **Specification:** All grid cells, transaction-count weighted.
    - **Output:** `output/tables/longdiff_weighted_all.tex`.

**`longdiff_weighted_exposed.R`**
    - **Input:** `data/processed/long_difference/long_diff_grid_house_sales.parquet`, `data/processed/long_difference/long_diff_grid_rentals.parquet`.
    - **Specification:** Exposed grid cells only, transaction-count weighted.
    - **Output:** `output/tables/longdiff_weighted_exposed.tex`.

#### 05_news/ - News and information effects

Difference-in-differences and event-study designs exploiting variation in public attention to sewage issues. Scripts vary by:
- **Information shock**: Google Trends peak vs cumulative LexisNexis article coverage
- **Estimation**: `did` = two-period DiD; `es` = event study with quarter-by-quarter coefficients
- **Treatment measure**: `full` = full-period spill exposure; `prior` = prior-to-transaction exposure
- **Lag**: `lag4` = 4-month lagged media coverage (default = contemporaneous)

**`did_trends_full.R`**
    - **Input:** `data/raw/google_trends/google_trends_uk.xlsx`, `data/processed/agg_spill_stats/agg_spill_yr.parquet`, `data/processed/house_price.parquet`, `data/processed/general_panel/sales/`, `data/processed/zoopla/zoopla_rentals.parquet`, `data/processed/zoopla/spill_rental_lookup.parquet`.
    - **Specification:** Google Trends shock, two-period DiD, full-period spill exposure.
    - **Output:** `output/tables/did_trends_full.tex`.

**`did_trends_prior.R`**
    - **Input:** `data/raw/google_trends/google_trends_uk.xlsx`, `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** Google Trends shock, two-period DiD, prior-to-transaction spill exposure.
    - **Output:** `output/tables/did_trends_prior.tex`.

**`es_trends_prior.R`**
    - **Input:** `data/raw/google_trends/google_trends_uk.xlsx`, `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** Google Trends shock, event study, prior-to-transaction spill exposure.
    - **Output:** `output/tables/es_trends_prior.tex`, `output/figures/es_trends_prior_sales.pdf`, `output/figures/es_trends_prior_rentals.pdf`.

**`did_articles_prior.R`**
    - **Input:** `data/processed/lexis_nexis/search1_monthly.parquet`, `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** LexisNexis cumulative coverage, two-period DiD, prior-to-transaction spill exposure, contemporaneous.
    - **Output:** `output/tables/did_articles_prior.tex`.

**`did_articles_lag4_prior.R`**
    - **Input:** `data/processed/lexis_nexis/search1_monthly.parquet`, `data/processed/cross_section/sales/prior_to_sale/`, `data/processed/cross_section/rentals/prior_to_rental/`, `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet`.
    - **Specification:** LexisNexis cumulative coverage, two-period DiD, prior-to-transaction spill exposure, 4-month lag.
    - **Output:** `output/tables/did_articles_lag4_prior.tex`.

### Script Execution Order

The following sequence removes circular dependencies and includes all scripts in layers 01–06:

#### Layer 01: Data Ingestion
1. **`edm_individ_data_standardisation_2021-2023.R`** — Standardises historical EDM archive files
2. **`fetch_edm_api_data_2024_onwards.R`** — Downloads raw 2024+ API snapshots (can run in parallel with step 1)

#### Layer 02: Data Cleaning
3. **`clean_consented_discharges_database.R`** — Cleans the consented discharges database (independent)
4. **`clean_lr_house_price_data.R`** — Cleans LR house prices with ONS postcode data
5. **`clean_zoopla_data.R`** — Cleans Zoopla rentals with ONS postcode data
6. **`combine_annual_return_data.R`** — Combines annual return workbooks (independent)
7. **`convert_individ_raw_data_to_rdata_2021-2023.R`** — Converts raw historical EDM files to parquet (requires step 1)
8. **`process_edm_api_json_to_parquet_2024_onwards.R`** — Processes raw API JSON to parquet (requires step 2)
9. **`combine_individ_edm_data_2021-2023.R`** — Combines 2021–2023 individual EDM parquet (requires step 7)
10. **`combine_api_edm_data_2024_onwards.R`** — Combines 2024+ API parquet (requires step 8)

#### Layer 03: Data Enrichment
11. **`merge_individ_annual_location.R`** — Merge location into individual spills (requires steps 6, 9)
12. **`combine_2021-2023_and_api_edm_data.R`** — Combine historical and API data (requires steps 9, 10)
13. **`create_annual_return_lookup.R`** — Site lookup tables (requires step 6)
14. **`create_unique_spill_sites.R`** — Unique sites dataset (requires step 11)
15. **`aggregate_spill_stats.R`** — General spill aggregations (requires step 11)
16. **`clean_rainfall_data.R`** — Clean rainfall NetCDFs (requires step 14; file lives in `02_data_cleaning/`)
17. **`aggregate_rainfall_stats.R`** — Aggregate rainfall by year/month/qtr (requires steps 14, 16)
18. **`identify_dry_spills.R`** — Detect dry spills (requires steps 11, 14, 16)
19. **`aggregate_dry_spill_stats.R`** — Aggregate dry-spill statistics (requires steps 15, 18)

#### Layer 04: Feature Engineering
20. **`10km_site_house_sale_match.R`** — House-to-site spatial matching (requires steps 4, 14)
21. **`10km_site_rental_match.R`** — Rental-to-site spatial matching (requires steps 5, 14)
22. **`compute_spill_stats.R`** — Enhanced spill statistics (requires step 19)

#### Layer 05: Data Integration
• Note: Integration scripts are executed earlier for dependency reasons — see steps 11–12.

#### Layer 06: Analysis Datasets
23. **`cross_section_sales.R`** — Cross-sectional datasets (sales; requires steps 4, 15, 20)
24. **`cross_section_prior_to_sale.R`** — Prior-to-sale cross-sectional datasets (requires steps 4, 11, 20)
25. **`cross_section_rental.R`** — Cross-sectional datasets (rentals; requires steps 5, 15, 20 & 22)
26. **`cross_section_prior_to_rental.R`** — Prior-to-rental cross-sectional datasets (requires steps 5, 11, 21)
27. **`site_panel_sales.R`** — Site-level panels (requires steps 4, 15, 20)
28. **`site_panel_rental.R`** — Rental site-level panels (requires steps 5, 15, 21)
29. **`house_panel_within_radius.R`** — House-level panels (requires steps 4, 15, 20)
30. **`rental_panel_within_radius.R`** — Rental-level panels (requires steps 5, 15, 21)
31. **`sale_panel_exp.R`** — Sales general panel export (requires steps 29–30)
32. **`rental_panel_exp.R`** — Rental general panel export (requires steps 27, 30)
33. **`grid_long_difference_sales.R`** — Grid-level long-difference dataset for sales (requires steps 4, 15, 20)
34. **`grid_long_difference_rentals.R`** — Grid-level long-difference dataset for rentals (requires steps 5, 15, 21)

**Dependencies Notes:**
- Steps 1–2 can run in parallel.
- Steps 3–6 are independent and can run in parallel; step 5 (Zoopla) is optional until used downstream.
- Steps 7–10 depend on ingestion outputs.
- Step 16 runs after unique spill sites (step 14) to avoid circularity; steps 17–19 form the rainfall/dry-spill sub-pipeline.
- Layer 06 scripts build on spill aggregations and spatial matching; keep order 23/24/25/26 → 27/28/29/30 → 31/32.

### Key Data Flows

The pipeline follows a dependency structure across 6 layers:

1. **Data Ingestion Layer (`01_data_ingestion/`)**: Raw data collection from EDM archives, API endpoints, and external sources
2. **Data Cleaning Layer (`02_data_cleaning/`)**: Format standardisation, data validation, and consolidation of multiple sources
3. **Data Enrichment Layer (`03_data_enrichment/`)**: Temporal aggregation, lookup table creation, and statistical summarisation
4. **Feature Engineering Layer (`04_feature_engineering/`)**: Spatial analysis, distance calculations, and feature creation
5. **Integration Layer (`05_data_integration/`)**: Cross-source data merging and harmonisation across time periods
6. **Analysis Datasets Layer (`06_analysis_datasets/`)**: Final dataset assembly for econometric analysis

**Layer Dependencies:**
- Each layer depends on outputs from previous layers
- Data flows sequentially through the pipeline from raw ingestion to final analysis datasets
- Spatial processing (Layer 4) enables the integration of house prices with spill site data
- Final layer creates multiple analysis-ready datasets optimised for different econometric approaches

## Analysis Documentation

For detailed analysis results, methodology, and interactive visualisations, visit the project website: **https://jacopo-olivieri.github.io/sewage/**

## Contact

- Jacopo Olivieri, LSE, j.olivieri@lse.ac.uk

## References

1. 

---

Last updated: 6 January 2026
