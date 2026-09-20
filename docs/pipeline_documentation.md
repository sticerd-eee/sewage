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

Each salience script also runs directly with `Rscript` from the root.
The main runner estimates only the selected paper groups. The executable report
owns historical reproductions and exploratory variants.

### Heterogeneity by Local Salience

The paper uses **overlapping Salience Groups**, as confirmed in the
[refactor plan](plans/2026-09-03-001-refactor-salience-report-and-paper-scripts-plan.md)
and defined in [CONCEPTS.md](../CONCEPTS.md):

- **All Bathing:** positive reported bathing-water association in any year 2021–2024, coastal or inland.
- **All Coastal:** site coast distance at most 2 km, including bathing locations.
- **All Inland:** site coast distance above 2 km, including bathing locations.

Group counts must not be added together. Missing coast excludes Coastal/Inland
membership but does not negate an observed bathing-water association. Positive bathing
evidence prevails over other unknown evidence; unresolved evidence does not
establish Bathing membership. Coast distance follows the tidal Mean High Water
line, including estuarine and tidal-river banks. Greater London is excluded using
transaction `region == "London"`. Recreational use and the value of nearby water
are hypothesised salience mechanisms. These proxies do not establish individual
swimming, direct discharge, continuous designation or designation at transaction date.

#### Paper specifications and outputs

All five scripts run sales 2021–2024 and rentals 2021–2023. Paths in the first
column are relative to `scripts/R/09_analysis/`.

| Script | Focal coefficient | Output prefix P |
|---|---|---|
| `05_news/did_trends_prior_extensive_salience.R` | Near × Post | `did_trends_prior_extensive_salience_groups` |
| `05_news/did_articles_prior_extensive_salience.R` | Near × log cumulative articles | `did_articles_prior_extensive_salience_groups` |
| `05_news/did_trends_prior_salience.R` | Weekly spill count × Post | `did_trends_prior_salience_groups` |
| `05_news/did_articles_prior_salience.R` | Weekly spill count × log cumulative articles | `did_articles_prior_salience_groups` |
| `02_hedonic/hedonic_continuous_prior_salience.R` | Weekly spill count | `hedonic_count_continuous_prior_salience_groups` |

Extensive Near is 0–500 m inclusive; Far is >1,000 m and ≤2,000 m.
Intensive and hedonic use properties within 250 m. Extensive and hedonic groups
use the nearest Site Group; intensive groups use any bathing site and minimum
coast distance in the 250 m companion. Attention models include LSOA and month
FE, with LSOA-clustered SEs. Hedonic includes LSOA FE, no time FE, and
heteroskedasticity-robust SEs; it retains the parent's joint count/hours
availability restriction. All models retain parent property controls.
Post begins in August 2022, inclusive; Articles is log cumulative UK coverage.

For each prefix P:

| Path | Contents |
|---|---|
| `output/tables/P.tex` | Six columns: Inland, Coastal, Bathing water for each market. Same format as `did_trends_prior_extensive.tex`: three decimals, significance stars, SEs in parentheses, controls/FE rows, N and adjusted R-squared. Geography labels retain the overlapping All Inland/All Coastal/All Bathing definitions. |
| `output/regs/P.rds` | `models[[market]][[group]]`, `counts`, `results` and `settings`. Group keys: `bathing`, `coastal`, `inland`. |
| `output/logs/P_cell_counts.csv` | Before/after London counts, estimation input, final N, estimator removals and extensive support cells, keyed by market/group. |
| `output/logs/P_results.csv` | Exposure/attention estimates, SEs, 95% CIs, p-values and N. |

Each script contains configuration, preparation, estimation, export and execution
sections. Shared nearest-site selection, companion joins, group membership,
sample audits and table formatting live in
`scripts/R/utils/salience_group_utils.R`. Estimation remains in each script.
No exploratory variants or historical model dependencies remain in these
entry points. They do not accept the retired `--reproduce` option.

#### Inputs

The data builders remain in layers 03 and 06. Analyses consume:
`site_group_characteristics.parquet` (coast/designation by `site_id`);
`spill_house_lookup.parquet` and `zoopla/spill_rental_lookup.parquet` (nearest
Site Group within 2 km, ties broken by `site_id`); transaction prices, controls
and regions from `house_price.parquet` and `zoopla/zoopla_rentals.parquet`;
prior exposure from `cross_section/{sales,rentals}/prior_to_{sale,rental}/`;
and the 250 m `prior_characteristics/` companions for intensive groups.
All these paths are under `data/processed/`. Articles use
`data/processed/lexis_nexis/search1_monthly.parquet`.

#### Executable report and historical outputs

[The QMD report](reports/2026-09-03-003-heterogeneity-by-salience-report.qmd)
loads saved results by default and embeds the full historical exploration.
It renders definitions, coefficient plots, results, sample counts and audits:

```bash
quarto render docs/reports/2026-09-03-003-heterogeneity-by-salience-report.qmd --to html
```

Append `-P reestimate:true` to explicitly rerun all five paper scripts and the
exploration. Missing saved bundles fail clearly without triggering a fit.
The rendered report states which mode was used.

The report reads a saved original-classification map with Coastal sites inside
and outside Greater London distinguished. Regenerate it separately:

```bash
Rscript docs/reports/2026-09-03-003-heterogeneity-by-salience-report/map_coastal_classification.R
```

The map uses the original site distances, representative crosswalk coordinates,
ONS 2024 country boundaries and the union of the 33 E090 local authorities from
May 2025. Its site categories do not represent the property-level London exclusion.

The original five prefixes are the paper prefixes with `_groups` removed.
For those original prefixes, the report preserves:

- `output/regs/P.rds`: `models[[variant]][[market]][[stratum]]`, unrestricted
  models, parent-reproduction checks, counts and variants.
- `output/tables/P_{coast_bathing,intensity,robust_coast10km,robust_london,robust_dropunknown}.tex`:
  five tables per attention specification; hedonic omits intensity.
- `output/logs/P_{cell_counts,nearest_site_coverage,reproduction}.csv`:
  sample support, characteristic coverage and unrestricted reproduction.

The four-way family is coastal bathing, coastal not bathing, inland bathing,
inland not bathing. Intensity uses published positive-exposure median bands at
500 m for extensive Near and 250 m for intensive; zero and unknown bands are
excluded. Both extensive intensity comparisons contain the full Far group.
Robustness changes the coast rule to 10 km, retains London, or drops unresolved
bathing evidence. Historical strata retain their original common missing-coast
exclusion. Before/After counts describe the hypothetical London drop; final N
follows the actual estimation policy.

The earlier **exclusive** three-way comparison (pooled Bathing, Coastal
non-bathing, Inland non-bathing) was removed from the report; the
[exclusive three-way memo](reports/2026-09-03-002-salience-three-way-results.md)
is the retained record of its definitions and results, and its saved
`salience_three_way` artifacts under `output/` are no longer rewritten.

The [four-way memo](reports/2026-09-03-001-heterogeneity-by-salience-results-memo.md)
and the exclusive three-way memo remain dated historical snapshots. Contract
tests source the QMD's definition chunks through
`scripts/R/testing/salience_report_test_setup.R` without fitting the full
model grid.

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
