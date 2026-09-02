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
- `clean_lr_house_price_data.R`: cleans one same-vintage 2014–2024 Land Registry
  download, assigns content-stable string `house_id` values, and writes paired
  long-run and 2021–2024 study-window candidates. The study window is always a
  pure filter of the long-run table from the same run.
- `clean_zoopla_data.R`: cleans safeguarded 2014–2023 Zoopla rental data,
  removes exact duplicates, assigns content-stable string `rental_id` values,
  and writes paired long-run and 2021–2023 study-window candidates. The study
  window is always a pure filter of the long-run table from the same run.
- `combine_annual_return_data.R`: combines annual return workbooks.
- `convert_individ_raw_data_to_rdata.R`: converts historical EDM files into parquet outputs.
- `process_edm_api_json_to_parquet_2024_onwards.R`: processes raw API snapshots into parquet outputs.
- `combine_individ_edm_data.R`: combines cleaned historical EDM parquet files.
- `combine_api_edm_data_2024_onwards.R`: combines cleaned API parquet files.
- `clean_rainfall_data.R`: prepares rainfall inputs and site-grid lookup files.
- `clean_lexis_nexis_search1.R`: converts LexisNexis search_1 PDFs to article-level data and aggregates to monthly counts (`search1_monthly.parquet`), consumed by the `05_news` analysis. Sources the helper `nexis_pdf_conversion.R`, which is not run standalone.

### 03_data_enrichment

- `aggregate_spill_stats.R`: reads the matched event feed and Site Group-year crosswalk,
  applies the Environment Agency 12/24 counting method at Site Group grain, and writes
  `agg_spill_yr.parquet`, `agg_spill_mo.parquet`, and `agg_spill_qtr.parquet` under
  `data/processed/agg_spill_stats/`. Monthly and quarterly outputs retain both
  explicit calendar columns and the stable `month_id` / `qtr_id` keys.
- `create_annual_return_lookup.R`: constructs cross-year site lookup tables. The orchestration lives in the numbered script; graph resolution, conflict audits, and the optional (off-by-default) random-forest matching live in `scripts/R/utils/annual_return_lookup_{graph_utils,audit_utils,rf_matching}.R`. Outputs under `data/processed/`:
  - `annual_return_lookup.parquet` / `annual_return_lookup.xlsx`: canonical cross-year lookup (one row per canonical site).
  - `annual_return_lookup_edges.parquet`: kept match edges behind the lookup.
  - Pre-resolution conflict audit (refreshed every run, zero-row when clean): `annual_return_lookup_conflict_summary.parquet`, `annual_return_lookup_conflict_records.parquet`, `annual_return_lookup_conflict_edges.parquet`, `annual_return_lookup_resolution_kept_edges.parquet`, `annual_return_lookup_resolution_dropped_edges.parquet`, `annual_return_lookup_conflicts.xlsx`.
  - Post-resolution diagnostics (`annual_return_lookup_post_resolution_*.parquet`, `annual_return_lookup_post_resolution_conflicts.xlsx`): written only when the final same-year safety net trips; a healthy run deletes any stale copies, so their absence is the expected state.
- `create_unique_spill_sites.R`: builds one row per Canonical Spill Site in the Annual Return Lookup, keyed by `site_id_canonical`. The repeated `site_id` column records Site Group membership; availability, location, operation, closure, and commissioning fields remain canonical metadata.
- `build_site_group_characteristics.R`: publishes
  `data/processed/site_characteristics/site_group_characteristics.parquet`,
  keyed uniquely by Site Group `site_id`. It combines 2021–2024 bathing- and
  shellfish-designation histories with continuous Site Coast Distance. The
  coastline is the dissolved England, Wales, and Scotland boundary from the
  ONS December 2024 Countries BGC product (EPSG:27700), licensed under the Open
  Government Licence v3.0; contains OS data © Crown copyright and database
  right 2024. `distance_to_coast_m` measures the nearest Mean High Water tidal
  line—including tidal reaches of rivers such as the Thames and Severn. It is
  neither distance to open sea nor a receiving-water classification.
- `aggregate_rainfall_stats.R`: aggregates rainfall to site-period level.
- `identify_dry_spills.R`: identifies dry spills using spill and rainfall information.
- `aggregate_dry_spill_stats.R`: integrates dry-spill metrics into the main aggregation outputs.
- `aggregate_daily_spill_rainfall.R`: constructs a balanced site-day spill and rainfall panel.

### 04_feature_engineering

- `site_house_sale_match.R`: spatially matches house sales to the unique Site Group projection using a configurable radius (currently 10 km) and writes `n_site_groups` as the nearby-group count.
- `site_rental_match.R`: spatially matches rental listings to the unique Site Group projection using a configurable radius (currently 10 km) and writes `n_site_groups` as the nearby-group count.
- `compute_spill_stats.R`: builds enhanced spill statistics and treatment indicators.

### 05_data_integration

- `combine_2021-2023_and_api_edm_data.R`: combines historical and API-era EDM records.
- `merge_individ_annual_location.R`: links spill events to Site Groups and annual-return evidence through the Site Group Register. Outputs under `data/processed/matched_events_annual_data/`: `site_group_crosswalk.parquet` (one row per Site Group, year, and company), `matched_events_annual_data.parquet` (pure event grain), `events_unmatched.parquet` (reason-coded), `annual_unmatched.parquet`, and `near_miss_report.parquet`. The crosswalk retains membership, annual status, group spill totals, representative location, and event-match evidence; canonical commissioning and operation metadata are deliberately excluded. Manual match decisions live in `data/processed/matched_events_annual_data_manual_overrides.csv`.

### 06_analysis_datasets

- `house_site_spills.R` and `rental_site_spills.R`: publish the unmasked measurement layer — one row per eligible transaction × nearby Site Group within the maximum radius, carrying the window-clipped spill hours, the 12/24-rule spill count, the pair distance, and the four atomic evidence flags, with no missingness masking. The published prior-exposure datasets are derivations over these tables; the tables publish through the staged gate but stay off the public enumerated list, with their schemas pinned in `test_prior_exposure_contracts.R`.
- `cross_section_sales.R` and `cross_section_rental.R`: publish the fixed-window `study_period` sales (2021–2024) and rental (2021–2023) cross-sections from matched individual EDM events at 250, 500, and 1,000 m. Events are clipped to the window and spill counts recomputed under the 12/24 rule, the same measurement the prior-to-transaction builders use. They retain every source transaction and distinguish eligible zero exposure, unknown annual evidence, and spatial ineligibility.
- `cross_section_sales_ea.R` and `cross_section_rental_ea.R`: publish the same cross-sections to `study_period_ea` from EA annual-return evidence instead. Both families share one pipeline in `cross_section_study_period_utils.R` and one missingness rule, so their outputs differ only in the exposure numbers; `scripts/R/testing/verify_study_period_exposure_sources.R` reports the difference.
- `cross_section_prior_to_sale.R` and `cross_section_prior_to_rental.R`: build prior-to-transaction exposure at property-radius grain. This remains a separate estimand from the fixed-period product, now differing from it only in window.
- `build_prior_characteristics.R`: reads the closed prior-to-transaction
  products and existing property–Site Group lookups without rebuilding either.
  It publishes joinable Hive-partitioned companions keyed by transaction ID and
  `radius` at
  `data/processed/cross_section/sales/prior_characteristics/` (`house_id`) and
  `data/processed/cross_section/rentals/prior_characteristics/` (`rental_id`).
  These companions contain nearby-Site-Group coast ranges, designation
  summaries, and market/radius/measure-specific spill-intensity bands. Their
  cutoffs and reconciliation counts are keyed by `market`, `radius`, and
  `measure` in
  `data/processed/cross_section/prior_intensity_cutoffs.parquet`. The companion
  boundary keeps the existing prior-exposure schemas and paths unchanged.
- `house_spill_prior_to_sale.R` and `rental_spill_prior_to_rental.R`: build sale- and rental-spill prior-exposure datasets from raw matched events.
- `site_panel_sales.R` and `site_panel_rental.R`: build site-level panels.
- `house_panel_within_radius.R` and `rental_panel_within_radius.R`: build within-radius property panels.
- `sale_panel_exp.R` and `rental_panel_exp.R`: export the general panel datasets.
- `grid_long_difference_sales.R` and `grid_long_difference_rentals.R`: build grid-level long-difference datasets.
- `repeat_sales.R` and `repeat_rentals.R`: thin market-specific entries over
  `scripts/R/utils/repeat_transactions_utils.R`. They build deterministic
  transaction-to-`repeat_id` mappings from the long-run cleaned supersets and
  publish large-group, extreme-price-ratio, and same-day review tables. Two
  transactions sharing an address and a date contradict each other, so every
  conflicting row is excluded from the mapping and routed to the same-day
  review; `repeat_count` is counted only over the survivors. The manifest
  reports address-key completeness (`key_coverage`, `keyed_count`) separately
  from same-day exclusion (`same_day_excluded_count`, `mapped_count`), because
  the two measure different data-quality failures and carry different floors.
  Persisted `repeat_count` is defined over the full long-run input; consumers
  that filter the date window must regroup after filtering. Both entries publish
  their own outputs: each run stages `*_candidate.parquet` files and, once its
  checks pass, promotes all four onto the canonical paths, so a successful run
  leaves the live datasets current with no manual move. A failed run promotes
  nothing and leaves the previous generation in place, which is also the only
  copy of it — publication keeps no backup.

## Analysis Scripts

The `scripts/R/09_analysis/` folder contains the main descriptive, hedonic, repeat-sales, long-difference, news, and dry-spill analysis scripts. These are organised by empirical approach rather than by pipeline layer.

### Analysis runner

From the repository root, use R 4.6.0 with the `rv` project library activated
by `.Rprofile`. The runner invokes plain `Rscript`; do not add `--vanilla`
or install packages at runtime.

```bash
bash scripts/R/09_analysis/run_all_analysis.sh --dry-run
bash scripts/R/09_analysis/run_all_analysis.sh
```

`--dry-run` prints the active script order and checks that each script exists.
The default run stops at the first failure; `--keep-going` continues and returns
a nonzero status if any script fails. Data ingestion and dataset construction
are separate prerequisites. The five Local Salience entry points run immediately
after their parents, in this relative order (other analyses run between pairs):

| Parent, relative to `scripts/R/09_analysis/` | Immediately following script |
|---|---|
| `02_hedonic/hedonic_continuous_prior.R` | `02_hedonic/hedonic_continuous_prior_salience.R` |
| `05_news/did_trends_prior.R` | `05_news/did_trends_prior_salience.R` |
| `05_news/did_articles_prior.R` | `05_news/did_articles_prior_salience.R` |
| `05_news/did_trends_prior_extensive.R` | `05_news/did_trends_prior_extensive_salience.R` |
| `05_news/did_articles_prior_extensive.R` | `05_news/did_articles_prior_extensive_salience.R` |

Each salience script can also be run directly with `Rscript` from the root.
Its optional `--reproduce` flag disables the Salience Stratum filter and retains
London to check the unrestricted model against the parent's saturated table
column. This mode writes a separate reproduction model bundle and refreshes the
reproduction CSV; it does not publish stratum tables or replace their model bundle.
Normal runs perform this check before publishing the stratum results. The parent
table must exist; coefficients and standard errors must match at published
precision and N must match exactly.

### Heterogeneity by Local Salience

These scripts re-estimate the existing saturated specifications within Salience
Strata, using the vocabulary in [CONCEPTS.md](../CONCEPTS.md) and the
[locked plan](plans/2026-09-02-001-feat-heterogeneity-by-salience-regressions-plan.md).
Sales cover 2021–2024 and rentals 2021–2023. No new data build is required.

| Analysis | Specification and classification |
|---|---|
| Extensive Margin, `did_{trends,articles}_prior_extensive_salience.R` | Log price on Near and Near × Public Attention, property controls, LSOA and month fixed effects; LSOA-clustered SEs. Near is 0–500 m inclusive; far is >1,000 m and ≤2,000 m. Coast/bathing uses the nearest Site Group; intensity uses the near property's 500 m companion. |
| Intensive Margin, `did_{trends,articles}_prior_salience.R` | Log price on average weekly spill count and count × Public Attention within 250 m, property controls, LSOA and month fixed effects; LSOA-clustered SEs. Both classifications use the 250 m companion. |
| Baseline hedonic, `hedonic_continuous_prior_salience.R` | Log price on average weekly spill count within 250 m, property controls and LSOA fixed effects, no time fixed effects; heteroskedasticity-robust SEs. Coast/bathing uses the nearest Site Group; no intensity split. |

`trends` denotes the Post indicator beginning in August 2022, including that
month, based on the Google Trends Search Interest peak. `articles` denotes log
cumulative UK Media Article Count. Both measure Public Attention over time;
Local Salience defines the cross-sectional subsamples. Spill hours, windowed
articles and additional near-band or radius sweeps are outside this analysis.
Controls are property type, new-build status and tenure for sales; property type,
bedrooms and bathrooms for rentals. The baseline hedonic retains its parent's
joint count/hours availability restriction even though only spill count is
estimated. Intensive Margin requires observed spill count and its parent's
coordinate, month, attention and price filters.

#### Shared utility and input keys

`scripts/R/09_analysis/utils_salience_strata.R` owns nearest-Site-Group selection,
coast/bathing classification, radius-companion joins, London flags, ordered
stratum filters and cell-count validation. It selects the minimum `distance_m`
within 2,000 m and breaks ties by `site_id`, without a new spatial match.
The nearest lookup is also used for the Intensive Margin's coverage audit,
but does not define its regression strata.

Under `scripts/R/09_analysis/05_news/`, `extensive_margin_salience_utils.R` and
`intensive_margin_salience_utils.R` prepare the parent samples, fit the models,
and publish results for their two attention entry points.
`salience_attention_table_utils.R` supplies their common table export and
parent-reproduction checks. The hedonic entry point reuses the shared stratum
utility and `utils_table_formatting.R`. Helpers are sourced, not runner entries.

| Published input | Key and role |
|---|---|
| `data/processed/site_characteristics/site_group_characteristics.parquet` | `site_id`; Site Coast Distance and Ever-Observed Designated-Water Indicator (`bath_ever_2124`, `bath_unknown_2124`). |
| `data/processed/spill_house_lookup.parquet` | `(house_id, site_id)`; candidate nearest Site Groups and `distance_m`. |
| `data/processed/zoopla/spill_rental_lookup.parquet` | `(rental_id, site_id)`; candidate nearest Site Groups and `distance_m`. |
| `data/processed/cross_section/sales/prior_characteristics/` | `(house_id, radius)`; `min_coast_dist_m`, `any_bath_2124`, `bath_unknown_2124`, `spill_count_band`. |
| `data/processed/cross_section/rentals/prior_characteristics/` | `(rental_id, radius)`; same companion fields. |
| `data/processed/house_price.parquet` | `house_id`; sales price, controls, location and `region`. |
| `data/processed/zoopla/zoopla_rentals.parquet` | `rental_id`; asking rent, controls, location and `region`. |
| `data/processed/cross_section/sales/prior_to_sale/` | `(house_id, radius)`; published Prior-to-Transaction Spill Exposure for the 250 m models. |
| `data/processed/cross_section/rentals/prior_to_rental/` | `(rental_id, radius)`; corresponding rental exposure. |
| `data/raw/google_trends/google_trends_uk.xlsx`, sheet `united_kingdom` | Monthly `Date` and `Year`; identifies the August 2022 peak. |
| `data/processed/lexis_nexis/search1_monthly.parquet` | `month_id`; `article_count` accumulates into log cumulative articles. |

#### Stratum definitions and variants

The coast/bathing family crosses two independent flags, in this fixed order:
`coastal_bathing`, `coastal_not_bathing`, `inland_bathing`,
`inland_not_bathing`. Coastal means Site Coast Distance ≤2,000 m (nearest-site
`distance_to_coast_m`, or companion `min_coast_dist_m`); inland means greater
than the threshold. Bathing means a positive designation in any year of
2021–2024 (`bath_ever_2124`, or `any_bath_2124` across nearby Site Groups).
Inland freshwater bathing locations therefore remain inland bathing.

Positive designation takes precedence over unknown evidence in other years or
other nearby Site Groups. Unknown or missing evidence without a positive is
unresolved (`bath_unresolved`); the headline counts it as not bathing. Published
unknown flags remain available for audit. Missing coast distance is excluded
from the coast/bathing family. Properties without a Site Group within 2 km are
excluded from nearest-site strata.

The intensity family uses the published Property Spill-Intensity Band:
`spill_le_p50` (positive exposure at or below the positive-exposure median)
and `spill_gt_p50` (above it). Cutoffs are fixed by the published market/radius
companion and are not recomputed within the analysis sample. Unknown and zero
bands enter neither stratum. Extensive Margin splits only the near group by its
500 m band and uses the full far group in both regressions; those samples overlap
in the far controls. A far property must have band `no_site`. Intensive Margin
splits the 250 m sample by its own band, without shared far controls.

Greater London (`region == "London"`) is dropped in all headline and intensity
results. Site Coast Distance is measured to the tidal Mean High Water line,
which makes the tidal Thames through London read as coast. Dropping London
addresses that specific concern; it does not redefine distance as open-sea
proximity or remove every tidal-river location elsewhere.

| Variant key | Coast rule | London | Unresolved bathing evidence | Strata |
|---|---|---|---|---|
| `coast_bathing` | 2,000 m | Dropped | Counts as not bathing | Four coast/bathing strata |
| `intensity` | Not used for selection | Dropped | Not used for selection | Two intensity strata; attention analyses only |
| `robust_coast10km` | 10,000 m | Dropped | Counts as not bathing | Four coast/bathing strata |
| `robust_london` | 2,000 m | Retained | Counts as not bathing | Four coast/bathing strata |
| `robust_dropunknown` | 2,000 m | Dropped | Excluded unless a positive is observed | Four coast/bathing strata |

#### Published tables: complete path inventory

Each table contains both markets and one saturated model per Salience Stratum.
Its column key is `(market, stratum)`; the file identifies analysis, attention
measure and variant. Markets are ordered sales, rentals; strata use the order
above. The headline rows are Near × Attention, weekly spill count × Attention,
or weekly spill count for the baseline hedonic; each column also reports its
standard error and final estimation N.

| Output path | Variant key |
|---|---|
| `output/tables/did_trends_prior_extensive_salience_coast_bathing.tex` | `coast_bathing` |
| `output/tables/did_trends_prior_extensive_salience_intensity.tex` | `intensity` |
| `output/tables/did_trends_prior_extensive_salience_robust_coast10km.tex` | `robust_coast10km` |
| `output/tables/did_trends_prior_extensive_salience_robust_london.tex` | `robust_london` |
| `output/tables/did_trends_prior_extensive_salience_robust_dropunknown.tex` | `robust_dropunknown` |
| `output/tables/did_articles_prior_extensive_salience_coast_bathing.tex` | `coast_bathing` |
| `output/tables/did_articles_prior_extensive_salience_intensity.tex` | `intensity` |
| `output/tables/did_articles_prior_extensive_salience_robust_coast10km.tex` | `robust_coast10km` |
| `output/tables/did_articles_prior_extensive_salience_robust_london.tex` | `robust_london` |
| `output/tables/did_articles_prior_extensive_salience_robust_dropunknown.tex` | `robust_dropunknown` |
| `output/tables/did_trends_prior_salience_coast_bathing.tex` | `coast_bathing` |
| `output/tables/did_trends_prior_salience_intensity.tex` | `intensity` |
| `output/tables/did_trends_prior_salience_robust_coast10km.tex` | `robust_coast10km` |
| `output/tables/did_trends_prior_salience_robust_london.tex` | `robust_london` |
| `output/tables/did_trends_prior_salience_robust_dropunknown.tex` | `robust_dropunknown` |
| `output/tables/did_articles_prior_salience_coast_bathing.tex` | `coast_bathing` |
| `output/tables/did_articles_prior_salience_intensity.tex` | `intensity` |
| `output/tables/did_articles_prior_salience_robust_coast10km.tex` | `robust_coast10km` |
| `output/tables/did_articles_prior_salience_robust_london.tex` | `robust_london` |
| `output/tables/did_articles_prior_salience_robust_dropunknown.tex` | `robust_dropunknown` |
| `output/tables/hedonic_count_continuous_prior_salience_coast_bathing.tex` | `coast_bathing` |
| `output/tables/hedonic_count_continuous_prior_salience_robust_coast10km.tex` | `robust_coast10km` |
| `output/tables/hedonic_count_continuous_prior_salience_robust_london.tex` | `robust_london` |
| `output/tables/hedonic_count_continuous_prior_salience_robust_dropunknown.tex` | `robust_dropunknown` |

#### Model bundles and audit outputs

The following five exact prefixes each generate all four normal-run paths in
the second table (20 files); the fifth path is written only by `--reproduce`.

| Analysis / attention | Prefix `P` |
|---|---|
| Extensive Margin / Post | `did_trends_prior_extensive_salience` |
| Extensive Margin / articles | `did_articles_prior_extensive_salience` |
| Intensive Margin / Post | `did_trends_prior_salience` |
| Intensive Margin / articles | `did_articles_prior_salience` |
| Baseline hedonic | `hedonic_count_continuous_prior_salience` |

| Path, substituting each prefix for `P` | Key and contents |
|---|---|
| `output/regs/P.rds` | `models[[variant]][[market]][[stratum]]`: compact `fixest` models with stored inference; also `unrestricted[[market]]`, `reproduction`, `counts`, `variants`. |
| `output/logs/P_cell_counts.csv` | `(market, variant, stratum)`; stratum family, London policy, before/after counts, estimation input and final N, estimator removals, exclusions and missing-coast share. Attention CSVs also carry `attention`. |
| `output/logs/P_nearest_site_coverage.csv` | `market`; counts and characteristic-match shares over lookup pairs and properties within 2 km, before analysis-sample filters. |
| `output/logs/P_reproduction.csv` | `(market, term)`; unrestricted estimate, SE and N compared with the parent reference; attention CSVs also carry `attention`. |
| `output/regs/P_reproduction.rds` | Reproduction-only bundle; `unrestricted[[market]]` and reproduction audit, with no fitted stratum models. |

`n_before_london_drop` and `n_after_london_drop` always describe the hypothetical
London exclusion, including for `robust_london`. `n_estimation` follows the
selected policy, and `nobs` is the model's final N after estimator removals.
Extensive-margin logs also contain near/far counts and, for Post, all four
near/far × pre/post cells, separately after dropping London and under the actual
estimation policy. Empty strata or missing required support are hard failures.
`nearest_site_missing_coast_share` uses distinct nearest Site Groups represented
in the prepared market sample before the London drop; it is not a transaction
share. Exclusion totals such as `n_unclassified` are repeated across stratum
rows and must not be summed. Intensive-margin logs additionally record missing
companions/coast, unresolved bathing and unknown/zero/no-site/missing bands;
hedonic logs additionally record missing nearest coast and unresolved bathing.

The [results memo](reports/2026-09-03-001-heterogeneity-by-salience-results-memo.md)
collects the 176 headline coefficients, SEs and counts from these 24 tables'
saved models and logs, including robustness results. It is a dated snapshot,
not automatically rewritten by the runner. After a new analysis run, refresh it
from the saved artifacts and reconcile its N and coefficients with the logs and
tables; do not refit models merely to edit the memo.

## Detailed Execution Order

### Layer 01: Data Ingestion

1. `edm_individ_data_standardisation.R` — standardise historical EDM archive files.
2. `fetch_edm_api_data_2024_onwards.R` — download raw 2024+ EDM API snapshots.

### Layer 02: Data Cleaning

3. `clean_consented_discharges_database.R` — clean the consented discharges database.
4. `clean_lr_house_price_data.R` — create paired 2014–2024 long-run and
   2021–2024 study-window Land Registry candidates in one run; promote the
   validated candidates to `house_price_long_run.parquet` and
   `house_price.parquet` together.
5. `clean_zoopla_data.R` — create paired 2014–2023 long-run and 2021–2023
   study-window Zoopla candidates in one run; promote the validated candidates
   to `zoopla_rentals_long_run.parquet` and `zoopla_rentals.parquet` together.
6. `combine_annual_return_data.R` — combine annual return workbooks.
7. `convert_individ_raw_data_to_rdata.R` — convert standardised historical EDM files to parquet.
8. `process_edm_api_json_to_parquet_2024_onwards.R` — process raw API JSON snapshots into parquet.
9. `combine_individ_edm_data.R` — combine the 2021–2024 individual EDM parquet files.
10. `combine_api_edm_data_2024_onwards.R` — combine the 2024+ API parquet files.
11. `clean_lexis_nexis_search1.R` — convert LexisNexis search_1 PDFs to article-level data and monthly counts (sources the `nexis_pdf_conversion.R` helper); feeds the `05_news` analysis.

### Layer 03: Data Enrichment

12. `combine_2021-2023_and_api_edm_data.R` — combine historical and API EDM data.
13. `create_annual_return_lookup.R` — build cross-year site lookup tables.
14. `merge_individ_annual_location.R` — attach event records to Site Groups and publish the Site Group crosswalk.
15. `create_unique_spill_sites.R` — resolve canonical metadata and create the one-row-per-`site_id_canonical` inventory.
16. `build_site_group_characteristics.R` — publish Site Group coast and designated-water characteristics after canonical membership and the Site Group projection are available.
17. `aggregate_spill_stats.R` — produce Site Group-keyed spill aggregations from matched events and group-year status.
18. `clean_rainfall_data.R` — clean rainfall inputs and site-grid lookups.
19. `aggregate_rainfall_stats.R` — aggregate rainfall by year, month, and quarter.
20. `identify_dry_spills.R` — identify and classify dry spills.
21. `aggregate_dry_spill_stats.R` — integrate dry-spill metrics into the main spill aggregations.
22. `aggregate_daily_spill_rainfall.R` — construct the balanced site-day spill-and-rainfall panel; feeds the `07_dry_spills` and `01_descriptive` analysis.

### Layer 04: Feature Engineering

23. `site_house_sale_match.R` — create house-to-site spatial matches using the configured radius (currently 10 km).
24. `site_rental_match.R` — create rental-to-site spatial matches using the configured radius (currently 10 km).
25. `compute_spill_stats.R` — build enhanced spill statistics and treatment indicators.

### Layer 05: Data Integration

Integration scripts are executed earlier for dependency reasons; see steps 12 and 13 above.

### Layer 06: Analysis Datasets

26. `house_site_spills.R` — publish the unmasked sales measurement layer at transaction–Site Group grain.
27. `rental_site_spills.R` — publish the unmasked rental measurement layer at transaction–Site Group grain.
28. `cross_section_sales.R` — build the fixed 2021–2024 sales `study_period` cross-section from matched individual EDM events.
29. `cross_section_rental.R` — build the fixed 2021–2023 rental `study_period` cross-section from matched individual EDM events.
30. `cross_section_sales_ea.R` — build the fixed 2021–2024 sales `study_period_ea` cross-section from EA annual-return evidence.
31. `cross_section_rental_ea.R` — build the fixed 2021–2023 rental `study_period_ea` cross-section from EA annual-return evidence.
32. `cross_section_prior_to_sale.R` — derive prior-to-sale sales cross-sections from the measurement layer.
33. `cross_section_prior_to_rental.R` — derive prior-to-rental rental cross-sections from the measurement layer.
34. `build_prior_characteristics.R` — publish sales and rental property-radius companion characteristics and the combined cutoff audit after both prior-exposure products exist.
35. `house_spill_prior_to_sale.R` — derive the sale-spill prior-exposure dataset from the measurement layer.
36. `rental_spill_prior_to_rental.R` — derive the rental-spill prior-exposure dataset from the measurement layer.
37. `site_panel_sales.R` — build site-level sales panels.
38. `site_panel_rental.R` — build site-level rental panels.
39. `house_panel_within_radius.R` — build within-radius house panels.
40. `rental_panel_within_radius.R` — build within-radius rental panels.
41. `sale_panel_exp.R` — export the general sales panel.
42. `rental_panel_exp.R` — export the general rental panel.
43. `grid_long_difference_sales.R` — build the sales long-difference grid dataset.
44. `grid_long_difference_rentals.R` — build the rental long-difference grid dataset.
45. `repeat_sales.R` — build the long-run sales repeat mapping and review tables.
46. `repeat_rentals.R` — build the long-run rental repeat mapping and review tables.

## Dependency Notes

- Steps 1 and 2 can run in parallel.
- Steps 3 to 6 are independent; step 5 is only needed for rental workflows.
- Steps 7 to 10 depend on the ingestion outputs.
- Step 11 is independent and only required for the `05_news` analysis.
- Steps 18 to 22 form the rainfall and dry-spill sub-pipeline.
- The layer-06 scripts build on spill aggregations and spatial matching outputs.
- The cleaned long-run/study pairs are one atomic data generation: never rebuild
  or promote one member independently. All ID-keyed artifacts must regenerate
  after either pair changes.
