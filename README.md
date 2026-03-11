# Sewage in Our Waters

## Overview

This repository contains the data pipeline, analysis code, and supporting documentation for an economics research project on the effect of sewage spills on house prices and rents in England.

## Project Structure

The project is organised as follows:

```text
├── book/ # Quarto book source for the project website and data-cleaning notes
│
├── data/ # All project data
│   ├── final/ # Analysis-ready datasets
│   ├── processed/ # Intermediate processed data files
│   ├── raw/ # Original data inputs
│   └── temp/ # Temporary processing files
│
├── docs/ # Project documentation, plans, reports, and manuscript-side assets
│
├── output/ # Generated tables, figures, maps, logs, and model artefacts
│
├── scripts/
│   ├── config/ # Configuration files
│   ├── python/ # Optional Python utilities, managed with uv
│   ├── R/
│   │   ├── 01_data_ingestion/ # Raw data collection scripts
│   │   ├── 02_data_cleaning/ # Data cleaning and standardisation scripts
│   │   ├── 03_data_enrichment/ # Data aggregation and enrichment scripts
│   │   ├── 04_feature_engineering/ # Spatial analysis and feature engineering scripts
│   │   ├── 05_data_integration/ # Data integration and merging scripts
│   │   ├── 06_analysis_datasets/ # Final dataset assembly scripts
│   │   ├── 09_analysis/ # Descriptive, regression, and auxiliary analysis scripts
│   │   ├── testing/ # Validation notebooks and targeted checks
│   │   └── utils/ # Shared helpers
│   └── stata/ # Stata scripts
│
├── sources/ # Reference materials and data documentation
│
├── AGENTS.md # Repository guidance
├── README.md # Project README
├── renv.lock # R dependency lockfile
└── sewage.Rproj # RStudio project file
```

## Data

The only restricted datasets currently used in this project are `data/raw/zoopla/` and `data/raw/lexis_nexis/`. All other active project data inputs are public and should be treated as such.

### Dataset List

| Data Directory | Source | Access | Notes | Citation |
| --- | --- | --- | --- | --- |
| `data/raw/edm_data/` | UK Government: Environment Agency | Public | Historical company EDM files (2021-2023) plus live API snapshots for the nine England companies in the National Storm Overflows Hub feed (2024+). | [Environment Agency EDM](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac) |
| `data/raw/ea_consents/` | UK Government: Environment Agency | Public | Site locations, permit details, and discharge consent information under the Environmental Permit Regulations. | [EA Consents Data](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109) |
| `data/raw/haduk_rainfall_data/` | UK Government: Met Office | Public | Daily precipitation data used to construct rainfall indicators and identify dry spills. | [Met Office HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) |
| `data/raw/lr_house_price/` | UK Government: HM Land Registry | Public | Property transaction records for England and Wales used for the sales-side analysis. | [Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) |
| `data/raw/zoopla/` | WhenFresh / CDRC / Zoopla | Restricted | Safeguarded rental listings used for the rental-side analysis. | Internal safeguarded access |
| `data/raw/lexis_nexis/` | LexisNexis | Restricted | News coverage exports used in the information and media-attention analysis. | Subscription access |

#### Event Duration Monitoring

- **Data Files:** `data/raw/edm_data/`
- **Source:** [UK Government: Environment Agency](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)
- **Format:** XLSX, XLSB, CSV (historical), JSON (API)
- **Coverage:** 2021-2024+ individual sewage overflow events
- **Notes:** Historical data (2021-2023) comes from annual EDM archives containing company files. Live API snapshots (2024+) come from the England-only National Storm Overflows Hub feed for nine companies and are stored under `data/raw/edm_data/raw_api_responses/`. Welsh Water's separate public map is not part of this live API pipeline.

#### Consented Discharges to Controlled Waters with Conditions

- **Data Files:** `data/raw/ea_consents/`
- **Source:** [UK Government: Environment Agency](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109/consented-discharges-to-controlled-waters-with-conditions)
- **Format:** ACCDB (exported to CSV)
- **Notes:** Provides permit requirements, consent-holder information, effluent types, and location data in OS National Grid Reference format.

#### HadUK-Grid Rainfall Data

- **Data Files:** `data/raw/haduk_rainfall_data/`
- **Source:** [UK Government: Met Office](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid)
- **Format:** NetCDF
- **Coverage:** 2020-2023 daily rainfall data
- **Notes:** Used to construct site-level rainfall indicators and dry-spill classifications.

#### Land Registry House Prices

- **Data Files:** `data/raw/lr_house_price/`
- **Source:** [UK Government: HM Land Registry](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file)
- **Format:** CSV
- **Coverage:** 2021-2024+ property transactions
- **Notes:** Property transactions are enriched using local ONS postcode data and provide the core house-sales outcome measure.

#### Zoopla Rental Data

- **Data Files:** `data/raw/zoopla/`
- **Source:** WhenFresh / CDRC / Zoopla
- **Format:** CSV
- **Coverage:** Current pipeline uses safeguarded rental data for 2021-2023
- **Notes:** This is a restricted dataset and should not be treated as publicly shareable project input.

#### LexisNexis News Data

- **Data Files:** `data/raw/lexis_nexis/`
- **Source:** LexisNexis
- **Format:** Raw search exports and derived files
- **Notes:** This is a restricted dataset used for the news and information-attention analyses.

## Computational Requirements

### Software Requirements

- **R (version 4.5.0+)**
  - Uses `renv` for reproducible package management.
  - Restore the environment with `R -q -e "renv::restore()"`.
- **Python**
  - Uses `uv` for package management:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r scripts/python/requirements.txt
```

- **Quarto**
  - Build the project website locally with `quarto render book`.

## Further Documentation

- Extended pipeline and script documentation: [`docs/pipeline_documentation.md`](docs/pipeline_documentation.md)
- Project website: <https://jacopo-olivieri.github.io/sewage/>

## Contact

- Jacopo Olivieri, LSE, `j.olivieri@lse.ac.uk`
