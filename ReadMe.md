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

| Data Directory               | Source                                | Notes                                                                                                       | Citation |
|------------------------------|----------------------------------------|-------------------------------------------------------------------------------------------------------------|----------|
| `data/raw/edm_data/`         | UK Government: Environment Agency     | **Event Duration Monitoring (2021–2024+):** Individual sewage spill events across all 10 WaSCs in England. Includes historical Excel files (2021-2023) and live API data (2024+) | [Environment Agency EDM](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac) |
| `data/raw/ea_consents/`      | UK Government: Environment Agency     | **Consented Discharges to Controlled Waters with Conditions:** Site locations, permit details, and discharge consent information under Environmental Permit Regulations | [EA Consents Data](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109) |
| `data/raw/lr_house_price/`   | UK Government: HM Land Registry       | **Land Registry House Prices:** Complete property transaction records for England and Wales (2021-2024+) including sale prices, addresses, and property characteristics | [Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) |


#### Event Duration Monitoring

- **Data Files:** `data/raw/edm_data/`  
- **Source:** [UK Government: Environment Agency](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)  
- **Format:** XLSX, XLSB, CSV (historical), JSON (API)  
- **Coverage:** 2021-2024+ individual sewage overflow events  
- **Notes:** Comprehensive logs of sewage spill events across all 10 Water and Sewerage Companies (WaSCs) in England. Historical data (2021-2023) sourced from annual zip archives containing individual company files. Current data (2024+) obtained via live API. Location data merged from annual return files. The project includes robust data processing pipeline to standardise formats across companies and time periods.

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

Scripts for collecting and standardising raw data from historical EDM files, live API endpoints, and external databases. Scripts handle file format variations, API pagination, and initial data validation.

**`edm_individ_data_standardisation_2021-2023.R`**  
   - **Input:** Original yearly zip archives (`EDM_{year}.zip`) from `data/raw/edm_data/` containing individual company EDM spill data files (XLSX, XLSB, CSV).
   - Unzips individual EDM data files and standardises filenames to `{yyyy}_{company_name}`.  
   - **Output:** Unzipped individual EDM files with standardised names (`{yyyy}_{company_name}_edm.ext`) saved directly in `data/raw/edm_data/`.

**`fetch_edm_api_data_2024_onwards.R`**  
   - **Input:** Environment Agency real-time API endpoints for each water company.
   - Fetches live sewage overflow data via REST API calls for all 10 WaSCs.  
   - Handles pagination, rate limiting, and error recovery for robust data collection.
   - **Output:** Raw JSON API responses saved to `data/processed/edm_api_data/` by company.

#### 02_data_cleaning/ - Data Cleaning & Standardisation Layer

Scripts for cleaning, standardising, and converting raw data into consistent formats. Scripts handle format conversion, column standardisation, data validation, and consolidation of multiple data sources into unified datasets.

**`clean_consented_discharges_database.R`**  
    - **Input:** Raw EA Consented Discharges database (`data/raw/ea_consents/consents_all.csv`).
    - Processes Environment Agency consented discharges data for site validation.  
    - Standardises column names, coordinates, and water company identifiers.  
    - **Output:** Cleaned consented discharges database saved to `data/processed/consent_discharges_db.RData`.

**`clean_lr_house_price_data.R`**  
    - **Input:** Raw Land Registry Price Paid Data CSV files (`pp-{year}.csv`) from `data/raw/lr_house_price/`.
    - Cleans and combines Land Registry data (2021-2024+) with geocoding via Postcodes.io API.
    - Creates unique house identifiers and validates property transaction data.
    - **Output:** Complete geocoded house price dataset with unique `house_id` saved to `data/processed/house_price.parquet`.

**`combine_annual_return_data.R`**  
   - **Input:** Raw annual return Excel files (`{year}_annual_return_edm.xlsx`) containing aggregated site-level statistics.
   - Cleans and combines annual sewage spill data for 2021–2023.  
   - Standardises column and company names across years.  
   - **Output:** Consolidated annual return dataset saved to `data/processed/annual_return_edm.parquet`.

**`combine_api_edm_data_2024_onwards.R`**  
   - **Input:** Processed API data Parquet files for all companies.
   - Combines all company API data into single dataset with consistent schema.
   - **Output:** Unified API dataset saved to `data/processed/combined_api_data.parquet`.

**`combine_individ_edm_data_2021-2023.R`**  
   - **Input:** RData list object (`data/processed/individual_edm_by_company.RData`) containing raw individual company spill data.
   - Cleans and combines individual sewage spill data (2021–2023) from all water companies.  
   - Standardises column names, water company names, and spill start/end dates to POSIXct format.  
   - **Output:** Single, cleaned, consolidated data frame of all individual spill events saved to `data/processed/combined_edm_data.RData`.

**`convert_individ_raw_data_to_rdata_2021-2023.R`**  
   - **Input:** Standardised individual EDM data files (`{yyyy}_{company_name}_edm.ext`) from `data/raw/edm_data/`.
   - Reads various file formats (XLSX, XLSB, CSV) of individual EDM data and converts to RData format.  
   - **Output:** Single RData file (`data/processed/individual_edm_by_company.RData`) containing list object with raw data frames for each company and year.

**`process_edm_api_json_to_parquet_2024_onwards.R`**  
   - **Input:** Raw JSON API responses from `data/processed/edm_api_data/` directory.
   - Processes and standardises JSON data to match historical data schema.
   - Converts timestamps, standardises location formats, and validates data quality.
   - **Output:** Processed API data in Parquet format for efficient analysis.

#### 03_data_enrichment/ - Data Enrichment & Aggregation Layer

Scripts for aggregating raw data into statistics, creating lookup tables, and enriching datasets with additional information. Scripts handle temporal aggregation, site linkage, and statistical summarisation.

**`aggregate_spill_stats.R`**  
    - **Input:** Individual spill data merged with location information.
    - Aggregates individual sewage overflow events into site-level statistics (counts and durations).  
    - Implements 12/24 counting methodology and handles events crossing month boundaries.  
    - **Output:** Aggregated site-level spill statistics for yearly and monthly analysis saved to `data/processed/agg_spill_stats/`.

**`create_annual_return_lookup.R`**  
   - **Input:** Combined annual return data from data cleaning layer.
   - Creates lookup tables linking site identifiers across different data sources.
   - **Output:** Site lookup tables enabling data integration across datasets.

**`create_unique_spill_sites.R`**  
    - **Input:** Merged individual spill data with location information.
    - Creates unique spill sites dataset by cleaning and validating location data.
    - Converts National Grid References to eastings/northings coordinates using `rnrfa::osg_parse`.
    - **Output:** Dataset of unique spill sites with coordinates saved to `data/processed/unique_spill_sites.parquet`.

#### 04_feature_engineering/ - Feature Engineering & Spatial Processing Layer

Scripts for spatial analysis, distance calculations, and feature engineering. Scripts handle geographic matching, spatial joins, and statistical calculations for analysis preparation.

**`10km_site_house_sale_match.R`**  
    - **Input:** Geocoded house price data and unique spill sites with coordinates.
    - Performs spatial join using `sf::st_is_within_distance` to identify spill sites within 10km of each property.
    - Calculates exact distances and counts discharge outlets within radius for each house.
    - **Output:** Spatial lookup table with house-site pairs and distances saved to `data/processed/spill_house_lookup.parquet`.

**`compute_spill_stats.R`**  
    - **Input:** Combined spill data with location information.
    - Computes comprehensive spill statistics and implements refined counting methodologies.
    - **Output:** Enhanced spill statistics for analysis preparation.

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

**`cross_section_db.R`**  
    - **Input:** House price data, spill-house lookup table, and monthly spill data loaded into DuckDB database.
    - Creates cross-sectional datasets aggregated at house level for multiple timeframes and spatial radii.
    - Calculates spill exposure metrics: total counts, total hours, number of sites, mean/minimum distances.
    - **Output:** Cross-sectional datasets partitioned by radius saved to `data/processed/agg_all_yrs_cross_section/` and `data/processed/agg_12mo_cross_section/`.

**`house_panel_exp.R`**  
    - **Input:** House-level panel data from house panel scripts.
    - Creates final analysis-ready datasets for event study and hedonic regressions.
    - Implements multiple treatment definitions and temporal aggregations.
    - **Output:** Final analysis datasets saved to `data/final/dat_event/` and `data/final/dat_hedonic/`.

**`house_panel_within_radius.R`**  
    - **Input:** House price data, spill lookup table, and site-level panel data.
    - Constructs house-level panel datasets linking property transactions to preceding 12-month spill exposure.
    - Implements two exposure metrics: nearest site and distance-weighted aggregation.
    - **Output:** House-level panel datasets for various radii saved to `data/processed/dat_panel_house_*/`.

**`site_panel.R`**  
    - **Input:** House price data, spill lookup table, and monthly spill data in DuckDB database.
    - Constructs monthly/quarterly panel datasets at site level for regression analysis.
    - Aggregates house prices within various radii (250m-10km) of each site using distance weighting.
    - Creates treatment indicators based on spill distribution thresholds (p50, p75, p90).
    - **Output:** Site-level panel datasets partitioned by radius saved to `data/processed/dat_panel_site_*/`.

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

Last updated: 18 July 2025