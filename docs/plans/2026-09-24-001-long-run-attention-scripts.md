---
title: Long-run repeat-property attention regression as paper scripts - Plan
type: feat
date: 2026-09-24
artifact_contract: ce-unified-plan/v1
product_contract_source: grill-with-docs
execution: code
issue: https://github.com/sticerd-eee/sewage/issues/14
---

# Long-run repeat-property attention regression as paper scripts - Plan

## Goal Capsule

- **Objective:** Promote the long-run repeat-property attention regression from the exploratory report into the paper's analysis scripts, producing one six-column table: sales and rentals, each with three attention measures, on the sample that excludes observed characteristic changes.
- **Means:** One feature-engineering script that writes a long-run nearest-Site-Group lookup at unique-coordinate grain, and one analysis script in `scripts/R/09_analysis/05_news/` that builds the repeat panel, fits six regressions and exports the table. Both follow the structure of their existing neighbours.
- **Authority:** The requirements record the user's choices from the planning session of 24 September 2026 and the independent review of the same day. Technical decisions govern implementation within them.
- **Execution profile:** Implementation on a new branch `jo/long-run-attention-scripts` off `jo/long-run-repeat-sales`. The exploratory report and its saved bundle are not modified.
- **Stop conditions:** Stop if a required input is unreadable, a mapping fails its uniqueness checks, a window Repeat Group carries contradictory geography, the article series does not reach December 2024, the January 2014 article stock is not positive, or a focal coefficient is not finite. Do not substitute another sample, window or estimator.
- **Completion and handoff:** The implementer runs both scripts, checks the two reference columns and the sample-step counts against the report's saved bundle, and reports the comparison. Converting the paper table into a slide table, committing, pushing and opening a pull request are separate instructions.

---

## Product Contract

### Summary

Write `scripts/R/04_feature_engineering/site_nearest_long_run_match.R`, which computes the distance from every unique property coordinate in the long-run histories to the nearest usable Site Group and writes one lookup keyed on the coordinate pair.
Write `scripts/R/09_analysis/05_news/did_attention_repeat_extensive.R`, which loads the 2014-onward histories, the repeat mappings and the lookup, builds the near–far repeat panel excluding properties with observed characteristic changes, fits two markets by three attention measures with property, calendar-month and MSOA × year fixed effects, and exports one table, one RDS and two CSV logs.
Wire the analysis script into `run_all_analysis.sh` and add the new vocabulary to `CONCEPTS.md`.

### Problem Frame

The exploratory report `docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd` established the design and estimated a 40-cell grid with heavy validation. The paper needs one specification as a script that reads like the other news scripts. The report cannot serve that role: it rebuilds its panel with a validation layer sized for exploration, loads the full histories before filtering (its run log records an 18 GB peak R heap on sales), and any edit to its computational chunks forces a twenty-minute re-estimation.

The existing nearest-distance lookups in `data/processed` were built from the study-window transactions, so pre-2021 sales and lettings carry no distance to an overflow. The long-run histories already carry British National Grid coordinates as integers, and `scripts/R/utils/site_group_utils.R` already provides the latest-valid Site Group location projection, so the missing feature is a lookup, not a new spatial method.

### Key Decisions

- **One sample, one fixed-effect structure, three attention measures.** The paper script estimates only the report's "exclude observed changes" sample under property, calendar-month and MSOA × year effects, with log cumulative articles, a post-March-2021 indicator and a post-August-2022 indicator. Governs R3–R5. (session-settled: user-directed — chosen over promoting the headline sample, both fixed-effect structures or the full variant grid.)
- **Distance is a pipeline feature, not an analysis-time computation.** A 04 script writes the lookup; the analysis script only joins it. Governs R1. (session-settled: user-approved — chosen over computing nearest distance inside the analysis script: features belong in 04, the lookup is reusable, and the analysis script keeps the shape of its siblings.)
- **Lookup at unique-coordinate grain.** One row per distinct coordinate pair, not per transaction. Governs R1. (review-settled 2026-09-24: user-approved — chosen over a transaction-keyed lookup mirroring `spill_house_lookup.parquet`: a 30M-row lookup would make the consumer collect and group 20M rows to compute a no-op minimum, needs an Arrow join with matching key types to build, and must be rebuilt for every history vintage; the coordinate table is an order of magnitude smaller and the existing helpers accept a two-column key.)
- **One script, one table.** Six columns in one table from one script rather than one script per attention measure. Governs R2, R6. (session-settled: user-directed — chosen over mirroring the trends/articles script pair: per-measure tables would have two columns each.)
- **The report stays frozen.** The scripts are a fresh lean implementation of the same method; the report and its bundle are the reference for the quick check. Governs R7. (session-settled: user-accepted default.)
- **No annual profiles and no testing script now.** Only the regression table is promoted; verification is a manual comparison after the run. Governs R7, Scope Boundaries. (session-settled: user-directed.)

### Requirements

**Feature-engineering script**

- R1. `site_nearest_long_run_match.R` reads the distinct finite integer `(easting, northing)` pairs from `data/processed/house_price_long_run.parquet` and `data/processed/zoopla/zoopla_rentals_long_run.parquet` through Arrow, unions them, matches each pair to the nearest usable Site Group under the latest-valid projection over 2021–2024, and writes `data/processed/site_nearest_long_run.parquet` with columns `easting` (int32), `northing` (int32), `site_id` and `distance_m`, unique on the coordinate pair. It logs unusable sites and the count of history rows without finite coordinates, and publishes atomically, following `site_house_sale_match.R`.

**Analysis script**

- R2. `did_attention_repeat_extensive.R` follows the structure of `did_trends_prior_extensive_salience.R`: header with Purpose, Inputs, Outputs and the report pointer; `script_setup.R`, `check_required_packages()`, sourced `utils_table_formatting.R` and `extensive_margin_news_utils.R`; one `CONFIG` list and one `MARKETS` list; numbered sections for configuration, data preparation, estimation, export and execution; `main()` behind `sys.nframe() == 0L`, refusing command-line arguments.
- R3. Sample per market, in this order, reproducing the report's ordered sample contract: (1) sales January 2014 to December 2024 and rentals January 2014 to December 2023, dated by `date_of_transfer` converted with `as_transaction_date()` from `repeat_transactions_utils.R` and by `rented_est`; (2) rows present in the repeat mapping, joined on the ID with a uniqueness stop; (3) stop if any window Repeat Group carries more than one distinct nonmissing country, coordinate pair, LSOA or MSOA (the report removed zero such groups; the check is cheap and keeps the definition complete); (4) country England, Greater London retained; (5) finite positive price; (6) finite coordinates and nonmissing, nonempty LSOA and MSOA; (7) nearest overflow within 0–500 m inclusive (near) or more than 1,000 m and at most 2,000 m (far); (8) properties observed on at least two distinct dates; (9) whole properties dropped when a characteristic takes more than one distinct nonmissing value across their surviving observations (sales: property type; rentals: property type, bedrooms, bathrooms), missing-to-observed not counting as a change; (10) repeats recomputed. A property is a Repeat Group. No property controls.
- R4. Three attention measures, each its own regression: log cumulative LexisNexis search-1 articles from January 2014 through the observation month; an indicator for months from March 2021 inclusive; an indicator for months from August 2022 inclusive. The calendar index is computed from the observation date with January 2014 as month 1 (March 2021 is 87, August 2022 is 104), not read from the stored 2021-based `month_id`.
- R5. Each fit is `log_price ~ near_bin:<attention> | repeat_id + month_index + msoa^year`, interaction only and no main effects, with `fixest::feols`, `vcov = ~lsoa`, `lean = TRUE`, and `fixef.rm = "perfect_fit"` passed explicitly (the installed default the report ran under, so a future default change cannot move the sample). Stop if the focal coefficient or its standard error is not finite.
- R6. Export `output/tables/did_attention_repeat_extensive.tex` (label `tbl:did-attention-repeat-extensive`) as one `modelsummary` table with sales and rentals groups of three columns each, three coefficient rows (one per measure), then observations, properties, LSOA clusters and fixed-effect tick rows; `output/regs/did_attention_repeat_extensive.rds` with models, counts and settings; `output/logs/did_attention_repeat_extensive_{results,cell_counts}.csv`, the counts CSV recording rows and properties after every R3 step and the estimator's removals.

**Integration and vocabulary**

- R7. Add the analysis script to `run_all_analysis.sh` after `did_articles_lag4_prior_extensive.R`; name the 04 script as a manual prerequisite in both headers. Leave the report, its bundle and its artifacts unchanged.
- R8. Add to `CONCEPTS.md`, in the glossary's style and without implementation detail: Near–Far Distance Bands (cross-referencing Near-Overflow Radius and Extensive Margin), Attention Event Indicators, Cumulative Article Stock (cross-referencing UK Media Article Count and Windowed Article Count, and noting the January 2014 origin against the January 2021 origin of the study-window scripts), and Long-Run Nearest Site Lookup.

### Scope Boundaries

No annual profiles, no other sample variants, no second fixed-effect structure, no address re-verification against the mapping, no input hashing, no fixtures and no testing script.
Two report checks are deliberately not re-implemented because they passed on the report inputs: the article-export coverage evidence (replaced by the R4 series-reach check in KTD4) and the rental listing-price unit-break check. The table note must not describe the rental outcome as weekly rent, since the provider does not define the unit; say log asking rent.
Slide-table conversion through `scripts/python/convert_paper_tables_to_beamer.py`, copying into Overleaf, committing and pushing are release steps outside this plan.

### Deferred to Follow-Up Work

A year-by-year script and figure (Near × year, reference 2019) on the same panel, if the paper wants the pre-2022 gap path.
Promotion of the other sample variants if a referee asks for them.

### Acceptance Examples

- AE1. Covers R3. A property with two sales in the same calendar month meets the two-distinct-dates rule; a property with two history rows on one date does not.
- AE2. Covers R3. A rental with bathrooms missing on one date and 2 on another is not flagged; bathrooms 1 and 2 across dates drops the whole property, after which a partner property left with one date also drops.
- AE3. Covers R4. A sale in February 2021 has both indicators equal to zero; a sale in July 2022 has Post March 2021 = 1 and Post August 2022 = 0; a sale in August 2022 has both equal to one.
- AE4. Covers R1. A Site Group whose latest valid location is missing is logged as unusable and matched against by no property; a property nearer to it than to any usable site is matched to the nearest usable site.
- AE5. Covers R3. A sale stamped 31 December 2024 23:00 UTC stays in the sales window; a bare local-time conversion that moved it to 2025 would be a defect.

---

## Planning Contract

### Key Technical Decisions

- KTD1. **Lookup on unique coordinates, matched once.** The 04 script pulls distinct `(easting, northing)` pairs from each history with an Arrow query (`select`, `filter` on finiteness, `distinct`, `collect`), unions the two markets, and matches with `sf::st_nearest_feature` plus `sf::st_distance(by_element = TRUE)` in EPSG:27700 on the usable projection, as the report's `nearest_site_distance()` does. Coordinates are cast to double only for `sf` and written back as int32 so the analysis script joins on the histories' native integer columns. No transaction-grain expansion and no Arrow join. Governs R1.
- KTD2. **Projection and atomic publication as in the existing 04 scripts.** Projection through `read_site_group_projection(crosswalk_path, 2021:2024)`; `usable = is.finite(easting) & is.finite(northing)` as the report's `project_sites()` defines it, with a stop if no site is usable; unusable sites written to `output/log/site_nearest_long_run_match_dropped_sites.csv`. Stage the parquet, validate schema, uniqueness on the coordinate pair and finiteness of distance, then rename into place; a failed validation leaves any previous lookup intact. Governs R1.
- KTD3. **Panel construction inline in the analysis script, bounded memory, reusing 05_news helpers.** Per market, in order: read the history through `arrow::open_dataset()` with `select` of the needed columns and `filter` to the window pushed into Arrow, then `collect()`, `as.data.frame()` and only then `data.table::setDT()`, per `docs/solutions/logic-errors/arrow-altrep-data-table-join-nondeterminism.md`; read the mapping restricted to the window IDs inside Arrow (semi-join or `filter(id %in% ...)`) rather than all 30M rows; read the lookup in full (small) and join on the two integer coordinate columns; `build_extensive_margin_sample()` for the bands (its near `[0, 500]` and far `(1000, 2000]` rule matches the report's `assign_bands()`); distinct-date counts, change flags and the final regroup as small functions in the script's data-preparation section; `rm()` and `gc()` between markets. No new utils file: the report is frozen, and this script is the only consumer. Peak memory is an implementation result to report. Governs R2, R3.
- KTD4. **Attention calendar built in the script.** A complete monthly calendar from January 2014 to December 2024, joined to `search1_monthly.parquet` on year-month with duplicate keys and missing counts as errors, absent months filled with zero (the stored series holds only months with at least one article), cumulative sum and log. Stop if the stored series' last month is before December 2024 or the January 2014 count is not positive. Indicators from the calendar index. Governs R4.
- KTD5. **Counts from a lean fit.** `fixest::obs()` is unavailable under `lean = TRUE`, so: observations from `nobs(fit)`; properties from `fit$fixef_sizes[["repeat_id"]]`; clusters from `attr(fit$cov.scaled, "df.t") + 1` (the report's reference cells show `t_df` one below the cluster count); estimator removals from `nrow(data) - nobs(fit)`, with removed rows available in `fit$obs_selection$obsRemoved` if a check needs them. Governs R5, R6.
- KTD6. **Table through `modelsummary(shape = "cbind")` and `patch_modelsummary_latex()`, following `did_articles_prior_extensive.R` lines 370–414.** Six lean fixest models in two named panels; a three-row `coef_map` (blanks where a term is absent); `add_rows` for properties, clusters and fixed-effect ticks; notes stating the sample windows, the bands, the three measures, the exclusion of properties with observed characteristic changes, that London is retained, no controls, and LSOA clustering. The salience script's exporter and note are not the pattern: they assume London excluded and controls present. Governs R6.

### Model

For property $i$ observed in calendar month $m$,

$$
\log p_{im} = \alpha_i + \lambda_m + \gamma_{g(i),\,y(m)} + \beta\,(\mathrm{Near}_i \times A_m) + \varepsilon_{im},
$$

with $A_m$ one of $\log(\text{cumulative articles}_m)$, $\mathbb{1}[m \geq \text{March 2021}]$ or $\mathbb{1}[m \geq \text{August 2022}]$. The property effect absorbs the near main effect; the month effect absorbs the attention main effect.

### Readiness and cost

Inputs are the ones the report validated on 19–20 September 2026. Both long-run histories carry int32 `easting` and `northing`, `lsoa`, `msoa` and `country`; `date_of_transfer` is a UTC timestamp and `rented_est` a date. The report's run log records an 18 GB peak R heap on sales because it materialised the full 30M-row history and mapping before filtering; the scripts push the window filter and the ID restriction into Arrow (KTD3) and never hold more than one market. Peak memory and runtime remain implementation-time results on a 16 GB machine; do not run alongside other heavy R jobs.

---

## Implementation Units

### U0. Branch and vocabulary

**Goal:** Start the work where the user asked and record the terms.

**Files:** `CONCEPTS.md`.

**Approach:** Create `jo/long-run-attention-scripts` from `jo/long-run-repeat-sales`. Add the four R8 entries in the glossary's style with the stated cross-references, near the existing Public Attention and Repeat Group entries. Commit as the first commit on the branch when the user asks for commits.

### U1. Long-run nearest-Site-Group lookup

**Goal:** Give every long-run property coordinate a distance to the nearest usable overflow.

**Requirements:** R1; KTD1, KTD2.

**Files:** Create `scripts/R/04_feature_engineering/site_nearest_long_run_match.R`.

**Patterns to follow:** `scripts/R/04_feature_engineering/site_house_sale_match.R` for header, `CONFIG`, logging through `setup_logging()`, stage-validate-publish and `main()`; the report's `nearest_site_distance()` for the matching call; `read_site_group_projection()` in `scripts/R/utils/site_group_utils.R` for the projection.

**Verification:** The lookup exists, is unique on the coordinate pair, covers every distinct finite coordinate in both histories, and every distance is finite. The log lists unusable sites and the count of rows without coordinates. Spot-check three `house_id`s of 2021–2024 sales present in `house_price.parquet`: the new distance at the sale's coordinates equals the minimum `distance_m` for that `house_id` in `spill_house_lookup.parquet` when that minimum is within 10 km (same crosswalk vintage, same projection years, same coordinate-eligibility rule, so equality should be exact).

### U2. Analysis script

**Goal:** Fit the six regressions and export the table.

**Requirements:** R2–R6; KTD3–KTD6.

**Dependencies:** U1.

**Files:** Create `scripts/R/09_analysis/05_news/did_attention_repeat_extensive.R`.

**Patterns to follow:** `did_trends_prior_extensive_salience.R` for header, configuration lists, section numbering, `main()` guard and RDS/CSV export; `did_articles_prior_extensive.R` for the article regression and the table export; `extensive_margin_news_utils.R` for `build_extensive_margin_sample()` and `patch_modelsummary_latex()`; `repeat_transactions_utils.R` for `as_transaction_date()`.

**Verification:** The script runs from the repository root under `rv` R 4.6.0 with no arguments and writes the table, the RDS and the two CSVs. The counts CSV records, per market, rows and properties after each R3 step and the estimator's removals, in the order of the Appendix ledger.

### U3. Runner and quick check

**Goal:** Make the script part of the paper run and confirm it reproduces the report.

**Requirements:** R7.

**Dependencies:** U2.

**Files:** `scripts/R/09_analysis/run_all_analysis.sh`.

**Approach:** Add the script after `did_articles_lag4_prior_extensive.R`. Run it once and compare, first, the sample-step counts with the Appendix ledger and pre-estimator targets, then the log-articles and post-August-2022 columns with the report's saved cells. Report the comparison in the handoff, with any difference localised to the sample or estimator step that causes it.

---

## Verification Contract

Manual, as the user asked: no testing script. The quick check is the U3 comparison of step counts, then coefficients, standard errors, observations, properties and clusters. Agreement to the report's third decimal is the expectation; any larger difference is a finding to explain before handoff, not to tune away. Inspect the rendered `.tex` table once in the paper build or a scratch document.

---

## Definition of Done

- Both scripts exist and run without arguments. Each has the header fields Purpose, Inputs, Outputs and the report pointer; the analysis script has the five numbered sections, `CONFIG` and `MARKETS`, `main()` behind the frame guard and the no-argument refusal; the 04 script has `setup_logging()`, stage-validate-publish and `main()`.
- The lookup, the table, the RDS and the CSV logs are written to the stated paths.
- The counts CSV reproduces the Appendix ledger and the two reference columns agree with the report's `exclude_changes` cells, or each difference is explained.
- The runner lists the analysis script; the report, its bundle and its artifacts are unchanged.
- `CONCEPTS.md` carries the four new entries.

---

## Appendix

### Reference cells from the report bundle

`output/tables/long_run_public_attention/cells.csv`, variant `exclude_changes`, fixed effects `property_month_msoa_year`, fingerprint `479cfd88…`. Counts are after the estimator's singleton removals.

| Market | Measure | Estimate | SE | Rows | Properties | Clusters |
|:--|:--|--:|--:|--:|--:|--:|
| Sales | Post August 2022 | −0.00506 | 0.00173 | 1,826,214 | 848,488 | 26,667 |
| Sales | log cumulative articles | −0.00223 | 0.00067 | 1,826,214 | 848,488 | 26,667 |
| Rentals | Post August 2022 | −0.00415 | 0.00148 | 1,597,619 | 557,198 | 25,573 |
| Rentals | log cumulative articles | −0.00270 | 0.00094 | 1,597,619 | 557,198 | 25,573 |

The March 2021 indicator has no reference cell; check sign and magnitude for plausibility only.

### Pre-estimator targets

From `sample_support.csv`, `variant_ledger.csv` and `exclusion_ledger.csv` in the same directory.

| Market | Step | Rows | Properties |
|:--|:--|--:|--:|
| Sales | In window (2014–2024) | 11,207,136 | |
| Sales | Mapped | 11,068,790 | |
| Sales | Geography contradictions removed | 0 groups | |
| Sales | England | 10,508,441 | |
| Sales | In near or far band | 5,226,959 | |
| Sales | Two distinct dates (headline) | 1,909,500 | 885,487 |
| Sales | Exclude observed changes, regrouped | 1,827,945 | 849,299 |
| Sales | Estimator singleton removals | 1,731 | |
| Rentals | In window (2014–2023) | 5,483,171 | |
| Rentals | Mapped | 5,483,171 | |
| Rentals | Geography contradictions removed | 0 groups | |
| Rentals | England | 5,305,821 | |
| Rentals | In near or far band | 2,600,802 | |
| Rentals | Two distinct dates (headline) | 1,944,100 | 656,391 |
| Rentals | Exclude observed changes, regrouped | 1,600,295 | 558,142 |
| Rentals | Estimator singleton removals | 2,676 | |

The report's ledger folds the price, coordinate and LSOA/MSOA requirements into the England-to-band steps; the counts CSV may report them separately as long as the band and two-date totals match.

### Facts established during planning and review

- Stages 01 to 06 have no runner; each pipeline script is run by hand. `run_all_analysis.sh` covers stage 09 only.
- The existing lookups `spill_house_lookup.parquet` and `spill_rental_lookup.parquet` are pair tables within 10 km built from the study-window transactions; `load_nearest_distance_lookup()` takes the per-property minimum within 2 km. It and `build_extensive_margin_sample()` accept a character vector as the key, so a two-column coordinate key works if that loader is reused.
- `derive_site_group_projection()` returns `site_id`, `water_company`, `ngr`, `easting`, `northing` and no usability flag; the report adds `usable` itself.
- The report's singleton removals arise from fixest 0.14.1's default `fixef.rm = "perfect_fit"`; the report passes nothing.
- The Overleaf slide tables are generated from `output/tables` by `scripts/python/convert_paper_tables_to_beamer.py`; no R script writes to Overleaf.
- `CONCEPTS.md` defines Repeat Group, Repeat-Transactions Mapping, Long-Run Transaction History, Extensive Margin, Public Attention, UK Media Article Count and Windowed Article Count, but not the bands, the event indicators or the article stock.
- Independent review on 24 September 2026: approve with changes; all sixteen findings are incorporated above.

---

## Addendum: simplification pass (2026-09-25)

After the implementation and its review, the user directed a simplification pass so that both scripts read like the cleaner producers in the pipeline rather than like the exploratory report. Decisions from the grilling session of 25 September 2026; each supersedes the earlier text where they conflict.

- **AD1. Shared publication.** The 04 script publishes through `publish_validated_file()` from `scripts/R/utils/dataset_publication_utils.R`, as the 03 and 06 producers do, with a validate function that checks the schema, uniqueness on the coordinate pair and finite non-negative distances. No hand-rolled staging, no `promote_stage` hook, no post-promotion re-verification. Supersedes KTD2's mechanism, not its checks.
- **AD2. Checks dropped from the 04 script.** The brute-force oracle sample, the usable-set membership check on the output and the staged row-count check are removed. The int32 stop on the histories' coordinate columns becomes a cast to int32 after collect.
- **AD3. One matching call.** Matching runs once over the union of coordinates; measured at 6 seconds and 1.1 GB, identical to the chunked result. The chunk loop and its `gc()` are removed. Functions return only what their caller uses.
- **AD4. Panel builder in a utils file.** The window load, mapping join, geography-contradiction check, England and price and coordinate filters, band assignment, two-distinct-dates rule, characteristic-change exclusion and regroup move to `scripts/R/09_analysis/05_news/long_run_repeat_panel_utils.R`, sourced by the analysis script like `extensive_margin_news_utils.R`. The deferred year-by-year script reuses it. Supersedes KTD3's "no new utils file".
- **AD5. No sample ledger.** The step-by-step counts and `output/logs/did_attention_repeat_extensive_cell_counts.csv` are dropped; no sibling script produces one and the order-dependent counts are not informative on their own. The script prints the final rows and properties per market to the console, as the siblings do. Supersedes the counts CSV in R6 and the ledger comparison in U2 and U3.
- **AD6. Fits without `lean`.** `lean = TRUE` and the cluster-count recovery from `cov.scaled` attributes are removed; observations, properties and clusters come from fixest's standard accessors. Without `lean` the six model objects weigh 1.5 GB, so the RDS keeps the results table and the settings and not the model objects; nothing reads them. Supersedes KTD5 and the `lean = TRUE` clause of R5. `fixef.rm = "perfect_fit"` stays explicit.
- **AD7. Acceptance for the pass.** The lookup parquet and the `.tex` table are byte-identical to the pre-refactor outputs, and the results CSV agrees to 1e-12 on every numeric column, with identical observations, properties and clusters. Both scripts are re-run.
- **AD8. Glossary.** `CONCEPTS.md` remains the only glossary; no `CONTEXT.md`.

### Style alignment (2026-09-25, second grilling)

- **AD9. House skeleton for the 04 script.** `site_nearest_long_run_match.R` follows the producer skeleton of `scripts/R/02_data_cleaning/clean_zoopla_data.R` and the 03 builder: `here` guard, `script_setup.R`, `REQUIRED_PACKAGES` one per line, `LOG_FILE`, `check_required_packages()`, `CONFIG`, `initialise_environment()`, `initialise_logging()` with "Script started" and "Script finished", `# Title` banners over sixty hashes, block-form frame guard. Publication stays on `publish_validated_file()`. The dropped-sites CSV is removed; the log warning naming the unusable sites stays.
- **AD10. 05_news convention for the analysis script.** `did_attention_repeat_extensive.R` keeps the 05_news convention (numbered sections, `cat()` progress, direct writes, no logger). Its `CONFIG` is a raw list with `validate_comparison_config()` applied in `main()`, and the lookup is loaded and checked by its own function. data.table stays in the panel builder and the script, by the user's direction; the top-level `%chin%` binding goes in favour of `data.table::%chin%`.
- **AD11. Documentation.** The utils file documents every function with full roxygen blocks like `extensive_margin_news_utils.R`; the two scripts use plain `#` comments.
- Acceptance as AD7.
- **AD12. Table only (2026-09-25).** The analysis script writes only `output/tables/did_attention_repeat_extensive.tex`, like 24 of its 29 siblings; the RDS and the results CSV are dropped, and the fixest coefficient table is printed to the console so the runner log records the estimates. Counts for the table come straight from the model objects. Supersedes the RDS and CSV items of R6 and AD6.
