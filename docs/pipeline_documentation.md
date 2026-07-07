# Pipeline and Scripts Documentation

This document provides the extended documentation for the R pipeline and the main script structure in this repository. It is intended to sit behind the root `README.md`, which provides the higher-level project overview.

## Pipeline Overview

The core R workflow is organised into six ordered layers:

1. **`01_data_ingestion/`**: acquire and standardise raw EDM inputs.
2. **`02_data_cleaning/`**: clean and harmonise public and restricted source data.
3. **`03_data_enrichment/`**: build spill aggregations, rainfall joins, and dry-spill indicators.
4. **`04_feature_engineering/`**: create spatial matches and derived treatment variables.
5. **`05_data_integration/`**: combine historical and API-era EDM data and align location information.
6. **`06_analysis_datasets/`**: assemble the final cross-sections and panels used in estimation.

The main analysis scripts live separately in `scripts/R/09_analysis/`, while validation notebooks and targeted checks live in `scripts/R/testing/`.

## Main Pipeline Scripts

### 01_data_ingestion

- `edm_individ_data_standardisation.R`: standardises historical EDM archive files.
- `fetch_edm_api_data_2024_onwards.R`: downloads timestamped EDM API snapshots for the live England-only feed.

### 02_data_cleaning

- `clean_consented_discharges_database.R`: cleans the consented discharges database.
- `clean_lr_house_price_data.R`: cleans Land Registry price-paid data using local ONS postcode data.
- `clean_zoopla_data.R`: cleans safeguarded Zoopla rental data.
- `combine_annual_return_data.R`: combines annual return workbooks.
- `convert_individ_raw_data_to_rdata.R`: converts historical EDM files into parquet outputs.
- `process_edm_api_json_to_parquet_2024_onwards.R`: processes raw API snapshots into parquet outputs.
- `combine_individ_edm_data.R`: combines cleaned historical EDM parquet files.
- `combine_api_edm_data_2024_onwards.R`: combines cleaned API parquet files.
- `clean_rainfall_data.R`: prepares rainfall inputs and site-grid lookup files.
- `clean_lexis_nexis_search1.R`: converts LexisNexis search_1 PDFs to article-level data and aggregates to monthly counts (`search1_monthly.parquet`), consumed by the `05_news` analysis. Sources the helper `nexis_pdf_conversion.R`, which is not run standalone.

### 03_data_enrichment

- `aggregate_spill_stats.R`: builds the main yearly, monthly, and quarterly spill aggregations.
- `create_annual_return_lookup.R`: constructs cross-year site lookup tables. The orchestration lives in the numbered script; graph resolution, conflict audits, and the optional (off-by-default) random-forest matching live in `scripts/R/utils/annual_return_lookup_{graph_utils,audit_utils,rf_matching}.R`. Outputs under `data/processed/`:
  - `annual_return_lookup.parquet` / `annual_return_lookup.xlsx`: canonical cross-year lookup (one row per canonical site).
  - `annual_return_lookup_edges.parquet`: kept match edges behind the lookup.
  - Pre-resolution conflict audit (refreshed every run, zero-row when clean): `annual_return_lookup_conflict_summary.parquet`, `annual_return_lookup_conflict_records.parquet`, `annual_return_lookup_conflict_edges.parquet`, `annual_return_lookup_resolution_kept_edges.parquet`, `annual_return_lookup_resolution_dropped_edges.parquet`, `annual_return_lookup_conflicts.xlsx`.
  - Post-resolution diagnostics (`annual_return_lookup_post_resolution_*.parquet`, `annual_return_lookup_post_resolution_conflicts.xlsx`): written only when the final same-year safety net trips; a healthy run deletes any stale copies, so their absence is the expected state.
- `create_unique_spill_sites.R`: builds the canonical spill-sites dataset.
- `aggregate_rainfall_stats.R`: aggregates rainfall to site-period level.
- `identify_dry_spills.R`: identifies dry spills using spill and rainfall information.
- `aggregate_dry_spill_stats.R`: integrates dry-spill metrics into the main aggregation outputs.
- `aggregate_daily_spill_rainfall.R`: constructs a balanced site-day spill and rainfall panel.

### 04_feature_engineering

- `10km_site_house_sale_match.R`: spatially matches house sales to nearby spill sites.
- `10km_site_rental_match.R`: spatially matches rental listings to nearby spill sites.
- `compute_spill_stats.R`: builds enhanced spill statistics and treatment indicators.

### 05_data_integration

- `combine_2021-2023_and_api_edm_data.R`: combines historical and API-era EDM records.
- `merge_individ_annual_location.R`: links spill events to annual-return works identities, locations, and EA totals through a works-register crosswalk. Outputs under `data/processed/matched_events_annual_data/`: `site_works_crosswalk.parquet` (canonical works-year artefact), `matched_events_annual_data.parquet` (pure event grain), `events_unmatched.parquet` (reason-coded), `annual_unmatched.parquet`, and `near_miss_report.parquet`. Manual match decisions live in `data/processed/matched_events_annual_data_manual_overrides.csv`.

### 06_analysis_datasets

- `cross_section_sales.R` and `cross_section_prior_to_sale.R`: build sales cross-sections.
- `cross_section_rental.R` and `cross_section_prior_to_rental.R`: build rental cross-sections.
- `house_spill_prior_to_sale.R` and `rental_spill_prior_to_rental.R`: build sale- and rental-spill prior-exposure datasets from raw matched events.
- `site_panel_sales.R` and `site_panel_rental.R`: build site-level panels.
- `house_panel_within_radius.R` and `rental_panel_within_radius.R`: build within-radius property panels.
- `sale_panel_exp.R` and `rental_panel_exp.R`: export the general panel datasets.
- `grid_long_difference_sales.R` and `grid_long_difference_rentals.R`: build grid-level long-difference datasets.
- `repeat_sales.R` and `repeat_rentals.R`: build repeated-sales and repeated-rentals identifiers and summaries.

## Analysis Scripts

The `scripts/R/09_analysis/` folder contains the main descriptive, hedonic, repeat-sales, long-difference, news, and dry-spill analysis scripts. These are organised by empirical approach rather than by pipeline layer.

## Detailed Execution Order

### Layer 01: Data Ingestion

1. `edm_individ_data_standardisation.R` — standardise historical EDM archive files.
2. `fetch_edm_api_data_2024_onwards.R` — download raw 2024+ EDM API snapshots.

### Layer 02: Data Cleaning

3. `clean_consented_discharges_database.R` — clean the consented discharges database.
4. `clean_lr_house_price_data.R` — clean Land Registry house prices with local ONS postcode data.
5. `clean_zoopla_data.R` — clean safeguarded Zoopla rentals with local ONS postcode data.
6. `combine_annual_return_data.R` — combine annual return workbooks.
7. `convert_individ_raw_data_to_rdata.R` — convert standardised historical EDM files to parquet.
8. `process_edm_api_json_to_parquet_2024_onwards.R` — process raw API JSON snapshots into parquet.
9. `combine_individ_edm_data.R` — combine the 2021–2024 individual EDM parquet files.
10. `combine_api_edm_data_2024_onwards.R` — combine the 2024+ API parquet files.
11. `clean_lexis_nexis_search1.R` — convert LexisNexis search_1 PDFs to article-level data and monthly counts (sources the `nexis_pdf_conversion.R` helper); feeds the `05_news` analysis.

### Layer 03: Data Enrichment

12. `combine_2021-2023_and_api_edm_data.R` — combine historical and API EDM data.
13. `create_annual_return_lookup.R` — build cross-year site lookup tables.
14. `merge_individ_annual_location.R` — merge location data into individual spill records (reads the combined EDM data and the lookup; produces the works crosswalk consumed by the next two steps).
15. `create_unique_spill_sites.R` — create the canonical spill-sites dataset.
16. `aggregate_spill_stats.R` — produce the main spill aggregations.
17. `clean_rainfall_data.R` — clean rainfall inputs and site-grid lookups.
18. `aggregate_rainfall_stats.R` — aggregate rainfall by year, month, and quarter.
19. `identify_dry_spills.R` — identify and classify dry spills.
20. `aggregate_dry_spill_stats.R` — integrate dry-spill metrics into the main spill aggregations.
21. `aggregate_daily_spill_rainfall.R` — construct the balanced site-day spill-and-rainfall panel; feeds the `07_dry_spills` and `01_descriptive` analysis.

### Layer 04: Feature Engineering

22. `10km_site_house_sale_match.R` — create house-to-site spatial matches.
23. `10km_site_rental_match.R` — create rental-to-site spatial matches.
24. `compute_spill_stats.R` — build enhanced spill statistics and treatment indicators.

### Layer 05: Data Integration

Integration scripts are executed earlier for dependency reasons; see steps 12 and 13 above.

### Layer 06: Analysis Datasets

25. `cross_section_sales.R` — build sales cross-sections.
26. `cross_section_rental.R` — build rental cross-sections.
27. `cross_section_prior_to_sale.R` — build prior-to-sale sales cross-sections.
28. `cross_section_prior_to_rental.R` — build prior-to-rental rental cross-sections.
29. `house_spill_prior_to_sale.R` — build the sale-spill prior-exposure dataset.
30. `rental_spill_prior_to_rental.R` — build the rental-spill prior-exposure dataset.
31. `site_panel_sales.R` — build site-level sales panels.
32. `site_panel_rental.R` — build site-level rental panels.
33. `house_panel_within_radius.R` — build within-radius house panels.
34. `rental_panel_within_radius.R` — build within-radius rental panels.
35. `sale_panel_exp.R` — export the general sales panel.
36. `rental_panel_exp.R` — export the general rental panel.
37. `grid_long_difference_sales.R` — build the sales long-difference grid dataset.
38. `grid_long_difference_rentals.R` — build the rental long-difference grid dataset.
39. `repeat_sales.R` — build repeated-sales identifiers and summaries.
40. `repeat_rentals.R` — build repeated-rentals identifiers and summaries.

## Dependency Notes

- Steps 1 and 2 can run in parallel.
- Steps 3 to 6 are independent; step 5 is only needed for rental workflows.
- Steps 7 to 10 depend on the ingestion outputs.
- Step 11 is independent and only required for the `05_news` analysis.
- Steps 17 to 21 form the rainfall and dry-spill sub-pipeline.
- The layer-06 scripts build on spill aggregations and spatial matching outputs.
