# Code review — `cross_section_sales.R` and `cross_section_rental.R`

- **Date:** 2026-07-07
- **Scope:** whole-file sanity check of `scripts/R/06_analysis_datasets/cross_section_sales.R` and `scripts/R/06_analysis_datasets/cross_section_rental.R`, verified against the upstream producers (`clean_lr_house_price_data.R`, `clean_zoopla_data.R`, `10km_site_house_sale_match.R`, `10km_site_rental_match.R`, `aggregate_spill_stats.R`) and downstream consumers under `scripts/R/09_analysis/`.
- **Method:** four parallel review agents (correctness, adversarial data-integrity, upstream-contracts, pipeline-consistency). The highest-impact findings were verified empirically: the reviewers ran read-only DuckDB queries against the actual parquet files and reproduced the critical bug on a minimal fixture; the orchestrator independently confirmed the load-bearing facts (rental radius config, table-name collision, file timestamps) by reading the code and file metadata.
- **Verdict:** two critical findings. Finding 1 silently removes about 5% of sales and rentals — precisely the zero-exposure control properties — from every exported dataset. Finding 2 (upstream, re-flagging todo 012) makes the sales and rental samples structurally asymmetric. Three high findings concern the trailing-window definition and stale data. The scripts' internal join logic is otherwise sound: keys were verified unique, the month-id formula matches upstream exactly, and no join fan-out exists.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## Critical

### 1. `[ ]` Properties with no spill site nearby are silently dropped from every output (SQL NULL semantics)

- **Where:** `cross_section_sales.R:208` and `:231-232` (all-years), `:300` and `:323-324` (prior-12mo); identical code in `cross_section_rental.R:203`, `:226-227`, `:295`, `:318-319`. The prior-12mo variants also drop these rows earlier, at the window filter (`cross_section_sales.R:277-282`, `cross_section_rental.R:272-277`).
- **Problem:** these pipes run as DuckDB SQL, not R. A house or rental with no site in the lookup carries a single row with `site_id = NA` and `distance_m = NA` after the left joins. For that group, every `if_else(distance_m <= radius, ...)` branch evaluates to SQL `NULL`, and `SUM` over an all-`NULL` group returns `NULL` (not 0). So `n_site_months` is `NULL`, `prop_spill_count_na` is `NULL`, and the quality filter `!(prop_spill_count_na == 1 & n_site_months > 0)` evaluates to `NULL` — and SQL `WHERE` drops `NULL` rows. In the prior-12mo functions the same properties are eliminated even earlier because `month_id >= (transfer_month_id - 11)` is `NULL` when `month_id` is `NULL`.
- **Evidence (verified against the shipped files):**
  - `spill_house_lookup.parquet` has 148,426 house ids with a NULL site row, and a further ~9,300 house ids absent from the lookup entirely (postcode-coordinate misses are filtered out before the spatial join, so they never get a NULL row).
  - The exported `all_years/radius=250` sales output contains 3,015,365 distinct house ids versus 3,175,951 in `house_price.parquet` — 160,586 sales (5.1%) missing. Rentals: 1,381,810 versus 1,450,255 — 68,445 (4.7%) missing.
  - Reproduced on a minimal DuckDB fixture: a property with a NULL lookup row vanishes at every radius, while a property whose nearest site is outside the radius but inside the lookup correctly survives with `spill_count = 0`.
- **Why it matters:** the dropped observations are the properties farthest from any sewage infrastructure — the cleanest controls in a hedonic design. The exclusion is non-random (correlated with rurality and plausibly with price), undocumented, and internally inconsistent: a house 9.9 km from its nearest site is kept with zero exposure, a house 10.1 km away is dropped entirely.
- **Suggested fix:** wrap the aggregates in `coalesce(..., 0)` (and add `na.rm = TRUE` to the `n_site_months` sum), make the prior-12mo window filter NULL-safe (`is.na(month_id) | (month_id >= ... & month_id <= ...)`), and restrict the quality filter to groups that actually have in-radius site-months. Then decide explicitly whether beyond-lookup-radius properties belong in the sample, and add an assertion that distinct property counts in equal counts out (minus logged, justified exclusions). The fix must be applied in all four places (two functions × two scripts).
- **Flagged by:** correctness, adversarial, and upstream-contracts independently; all three verified it empirically.

### 2. `[ ]` Rental spill lookup is built at 5 km while the sales lookup is built at 10 km (re-ships todo 012 finding 1)

- **Where:** `scripts/R/04_feature_engineering/10km_site_rental_match.R:52` (`radius_km = 5`, contradicting the filename and the function docstrings at lines 129 and 174 which say 10). Consumed at `cross_section_rental.R:58`.
- **Problem:** `spill_rental_lookup.parquet` was regenerated on 6 July 2026 — after the works-crosswalk rebuild — still at 5 km (verified: maximum `distance_m` is 5,000 in the rental lookup versus 10,000 in the sales lookup). Combined with finding 1, this makes the sample-selection boundary differ between the two markets: at the 5,000 m radius the rental sample contains no property without a site inside the radius (every retained rental has at least one site within 5 km by construction), while the sales sample retains controls whose nearest site is 5–10 km away. Sales-versus-rental comparisons at the wider radii are not like-for-like.
- **Suggested fix:** set the radius to 10 in the rental match script, regenerate the rental lookup, drop the affected DuckDB tables, and re-run `cross_section_rental.R` with `refresh_db = TRUE` — sequenced together with the fixes in finding 1 and the regeneration in finding 5. Implement the radius contract test proposed in `todos/012-pending-p0-review-10km-site-match-scripts.md` (finding 6) so this cannot ship a third time.
- **Flagged by:** adversarial and upstream-contracts independently; confirmed by the orchestrator in the source (`radius_km = 5` at line 52) and on disk.

---

## High

### 3. `[ ]` Trailing-window definition is internally inconsistent: the window includes the transaction month, but the eligibility cutoff assumes it does not

- **Where:** `cross_section_sales.R:278-282` and `:330-334`; `cross_section_rental.R:273-277` and `:325-329`.
- **Problem:** the window `month_id` in `[transfer_month_id - 11, transfer_month_id]` spans twelve months *including* the sale or rental month, so spill days occurring after the transaction within that month count as "prior" exposure — contradicting the docstring ("the 12 months prior"). Separately, the cutoff `transfer_month_id >= 13` is only necessary for a window that *excludes* the transaction month: a December 2021 sale (`month_id = 12`) has a complete inclusive window over months 1–12 (the monthly panel is a verified complete grid over months 1–48), yet it is excluded. The two lines encode different definitions; one of them is wrong.
- **Suggested fix:** pick one definition. For twelve complete months strictly before the transaction: `month_id >= transfer_month_id - 12 & month_id <= transfer_month_id - 1`, keeping the `>= 13` cutoff. For a trailing window including the transaction month: keep the window and relax the cutoff to `>= 12` so December 2021 transactions are retained. Update the comments and docstring to match, in both scripts.
- **Flagged by:** correctness.

### 4. `[ ]` Load-if-absent tables in the shared `duckdb.duckdb` serve silently stale data; `dat_mo` collides across scripts with different sources

- **Where:** `cross_section_sales.R:93-130` and `:443` (`main(refresh_db = FALSE)`); `cross_section_rental.R:97-124` and `:446`; collision with `scripts/R/04_feature_engineering/compute_spill_stats.R:85-105`, which loads a *different* file (`agg_spill_dry_mo.parquet`) into the same table name `dat_mo` in the same database file.
- **Problem:** `load_data_to_db()` loads a table only if its name is absent, with no freshness check against the source parquet, and the entry point hardcodes `refresh_db = FALSE`. After any upstream regeneration — such as the works-crosswalk rebuild just completed — a rerun silently computes from the old snapshot. Worse, whichever of `cross_section_sales.R`, `cross_section_rental.R`, or `compute_spill_stats.R` runs first pins both the vintage *and the source dataset* of `dat_mo` for the others (the cross-section scripts expect `agg_spill_mo.parquet`; `compute_spill_stats.R` loads the dry-spill variant). The panel scripts additionally cache the same rental lookup under two different table names, which can drift to different vintages. This staleness also blocks the fix for the critical upstream bug in `todos/2026-07-07-review-aggregate_spill_stats.md` (finding 1) from propagating here.
- **Suggested fix:** at minimum, use distinct table names per source dataset (e.g. `dat_dry_mo` in `compute_spill_stats.R`) and log the source file's modification time when a load is skipped. Better: record the source parquet's modification time or hash in a metadata table at load time and reload when it changes, or switch to `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_parquet('...')`, which is cheap enough to run unconditionally and also removes the memory-heavy `rio::import` + `copy_to` round-trip through R (the sales lookup is roughly 208 million rows).
- **Flagged by:** correctness, adversarial, and pipeline-consistency independently; collision confirmed by the orchestrator.

### 5. `[ ]` The exported cross-section outputs on disk predate the works-crosswalk merge rebuild

- **Where:** `data/processed/cross_section/{sales,rentals}/{all_years,prior_12mo}/` — partition directories dated 15–19 September 2025, while `agg_spill_mo.parquet` (6 July 2026), `spill_house_lookup.parquet` (6 July 2026), and `spill_rental_lookup.parquet` (6 July 2026) were all regenerated on the new works grain.
- **Problem:** `scripts/R/09_analysis/01_descriptive/cross_sectional_plots.R` (lines 293 and 350) reads these directories directly, so any analysis run today mixes new-grain upstream artifacts with old-grain cross-sections. Commit `aa039a0` (regenerate site-keyed artifacts on works grain) did not cover these two outputs.
- **Suggested fix:** re-run both scripts with `refresh_db = TRUE` — but only after fixing finding 1 (otherwise the row loss is baked in again), finding 2 (otherwise the 5 km rental lookup is baked in again), and the upstream `aggregate_spill_stats.R` critical finding (otherwise the annual-total contamination of `agg_spill_mo` is baked in again). Add these outputs to the post-rebuild regeneration checklist.
- **Flagged by:** adversarial; timestamps confirmed by the orchestrator.

---

## Moderate

### 6. `[ ]` All-years exposure includes months after each transaction, through December 2024

- **Where:** `cross_section_sales.R:190-232`; `cross_section_rental.R:185-227`.
- **Problem:** transactions span 2021–2023 (verified) but the monthly spill panel runs through month 48 (December 2024), and `create_all_years_db()` applies no time restriction. A 2021 sale's "exposure" therefore includes up to two years of spills the buyer could not have observed. Defensible if the variable is interpreted as a time-invariant site-quality proxy; contaminated if interpreted as experienced exposure.
- **Suggested fix:** document the intended interpretation in the script header and the paper's data section, or cap at `month_id <= transfer_month_id` and name the variable accordingly.

### 7. `[ ]` `write_dataset()` can leave stale partition files from previous runs

- **Where:** `cross_section_sales.R:376-383`; `cross_section_rental.R:371-378`.
- **Problem:** arrow's default `existing_data_behavior = "overwrite"` only replaces files whose names collide. If `radius_thresholds` ever changes, or a run writes fewer part files than its predecessor, leftover files remain and `open_dataset()` downstream silently unions stale rows with fresh ones. Currently benign (single `part-0.parquet` per partition, stable radius set) but a latent duplication hazard. Same defect class as finding 6 in `todos/2026-07-07-review-prior-to-sale-rental-spill-scripts.md`.
- **Suggested fix:** pass `existing_data_behavior = "delete_matching"` in both `export_data()` functions.

### 8. `[ ]` Non-market Land Registry transactions enter the sales cross-section unfiltered

- **Where:** upstream in `scripts/R/02_data_cleaning/clean_lr_house_price_data.R` (no `ppd_category` filter); consumed at `cross_section_sales.R:147-151`.
- **Problem:** 500,577 of 3,175,951 rows (15.8%) carry `ppd_category = "B"` (additional price-paid entries: repossessions, buy-to-lets, below-market transfers), and prices range down to £1. This is a modeling decision rather than a bug in the reviewed scripts, but nothing in the pipeline currently makes the decision explicitly.
- **Suggested fix:** filter or flag category-B and implausibly low-price transactions in the cleaning script (or as a robustness check downstream), and document the choice either way.

---

## Low

### 9. `[ ]` Sales `main()` lacks the error-logging wrapper the rental script has

- **Where:** `cross_section_sales.R:402` versus `cross_section_rental.R:397-442`.
- **Problem:** the rental script wraps `main()` in `tryCatch` with a fatal-error log line and a `finally` timestamp; the sales script does not. No correctness impact (the `on.exit` disconnect still runs), but fatal errors in overnight sales runs never reach the log file, and the twin scripts drift when edited in parallel. A normalized diff found no other semantic divergence between the two scripts beyond the intended renaming.
- **Suggested fix:** add the same wrapper to the sales script.

### 10. `[ ]` `n_site_months` sum lacks `na.rm = TRUE`

- **Where:** `cross_section_sales.R:208` and `:300`; `cross_section_rental.R:203` and `:295`.
- **Problem:** computationally irrelevant in SQL (SUM always skips NULLs; dbplyr just emits a warning), but the omission signals the R-semantics expectation that produced finding 1. Fold into the finding 1 fix.

### 11. `[ ]` The rental timing variable is a listing-removal proxy for 71.6% of observations

- **Where:** upstream `scripts/R/02_data_cleaning/clean_zoopla_data.R:203` (`rented_est = coalesce(rented, latest_to_rent)`); consumed at `cross_section_rental.R:144`.
- **Problem:** 1,038,849 of 1,450,255 rentals have no actual rented date, so their prior-twelve-month window is anchored on the last listing date instead — measurement error in window placement, absent from the sales side (registered transfer dates, no missing values). No rentals are dropped for missing dates (`rented_est` has zero NULLs, verified).
- **Suggested fix:** carry a `has_actual_rented_date` flag into the cross-section outputs so analyses can test robustness on the roughly 28% with true dates.

### 12. `[ ]` The `prior_12mo` outputs appear to have no live downstream consumer

- **Where:** `data/processed/cross_section/{sales,rentals}/prior_12mo/`.
- **Problem:** a grep across `scripts/R/09_analysis/` and `scripts/R/06_analysis_datasets/` found every "prior exposure" consumer reading the `prior_to_sale` / `prior_to_rental` outputs (the newer daily-precision pipeline) instead. The prior-12mo halves of these two scripts may be dead computation.
- **Suggested fix:** confirm, then either delete the `create_prior_12mo_db()` paths or document why they are kept (for example, as a monthly-grain robustness alternative).

---

## Verified non-issues (checked so they need not be re-litigated)

- `house_id` and `rental_id` are per-row `row_number()` identifiers (`clean_lr_house_price_data.R:219`, `clean_zoopla_data.R:262`) and are unique in the shipped parquet files, so the `group_by` calls cannot merge repeat sales and the final price/rent joins cannot fan out. Note the name `house_id` misleadingly suggests a property identifier; repeat sales of the same property appear as independent observations.
- `site_id` is globally unique across water companies, and `agg_spill_mo.parquet` is exactly one row per site and month (13,990 sites × 48 months, zero duplicates, verified), so the join on `site_id` alone is safe and the 48-fold expansion is the intended month grid.
- The `month_id` formula, `(year - 2021) * 12 + month`, is identical between these scripts and `aggregate_spill_stats.R`, and the site universe is consistent across `unique_spill_sites.parquet`, `agg_spill_mo.parquet`, and both lookups after the works-crosswalk rebuild — no dangling site keys in either direction.
- `n_distinct(if_else(...), na.rm = TRUE)` translates to `COUNT(DISTINCT CASE WHEN ...)`, which correctly ignores NULLs; `NA_integer_` matches the int32 `site_id` column type.
- The `cross_join` with the radius table does not double count: each radius aggregate is computed within its own group, and the two `PRAGMA` memory settings are aliases (redundant but harmless).

## Testing gaps

- No assertion that distinct property counts in the exports match the input tables per radius — a one-line check that would have caught the ~5% silent loss in finding 1.
- No fixture test for the three NULL-semantics corner cases (property with zero matched sites; property with all sites outside the smallest radius; property with all-NA spill counts). The minimal DuckDB repro built during this review would make a good permanent fixture.
- No test pinning the window definition (a fixture sale in December 2021 and January 2022 with known monthly spills would expose finding 3 immediately).
- No contract test on `(site_id, month_id)` uniqueness or on lookup radius alignment between the sales and rental match scripts (the latter is todo 012 finding 6, still unimplemented — it would have caught finding 2's re-ship).
- No freshness check tying DuckDB table vintage to source parquet modification times (finding 4), and no post-export read-back validation.

## Suggested fix order

1. Fix the upstream monthly-panel contamination (`todos/2026-07-07-review-aggregate_spill_stats.md`, finding 1) and the rental lookup radius (finding 2 here) — both poison any regeneration.
2. Fix finding 1 (NULL-safe aggregation and filters, all four call sites) and finding 3 (window definition), plus the cheap items 7, 9, 10.
3. Re-run both scripts with `refresh_db = TRUE` (finding 5), after resolving the `dat_mo` naming collision (finding 4).
4. Add the count-reconciliation and fixture tests from the testing gaps.
