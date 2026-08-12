# Code review — `cross_section_sales.R` and `cross_section_rental.R`

- **Original review:** 2026-07-07
- **Comprehensive update:** 2026-08-12
- **Scope:** whole-file review of `scripts/R/06_analysis_datasets/cross_section_sales.R` and `scripts/R/06_analysis_datasets/cross_section_rental.R`. Current upstream producers, downstream consumers, persistent DuckDB tables, and published Parquet artifacts were checked where they determine correctness.
- **Method:** independent correctness, data-integrity, reliability, performance, testing, maintainability, project-standards, and institutional-learnings passes, followed by an independent validation pass over every high or directly actionable finding. Read-only DuckDB queries checked current row counts, missingness, lookup radii, cache contents, and artifact vintages. An additional cross-model adversarial pass was attempted but returned no usable result because its execution context could not refresh authentication; it is not counted as corroboration.
- **Verdict:** **not clean yet**. The rental lookup radius is fixed, and several structural invariants remain sound, but the scripts still silently change the estimation sample, reuse stale/colliding persistent tables, and publish outputs that are already behind their inputs. The current query plan also materializes hundreds of millions of lookup rows in R and expresses a very large site-month-by-radius expansion.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` explicit policy/design decision required

## Status of the 2026-07-07 findings

| Finding | Status on 2026-08-12 | Current evidence |
|---|---|---|
| 1. NULL semantics drop controls | `[ ]` Still current | Current all-years exports retain fewer IDs at every radius than their inputs; NULL lookup sentinels are still dropped. |
| 2. Rental lookup only 5 km | `[x]` Fixed | `site_rental_match.R` now uses 10 km, its contract test covers the radius, and the current lookup reaches 10,000 m. |
| 3. Inconsistent trailing window | `[-]` Still current | The code includes the transaction month but keeps the cutoff for a strictly prior window. Choose one estimand. |
| 4. Stale/colliding DuckDB tables | `[ ]` Still current | The cache predates current sources; `dat_mo` is also shared with a dry-spill consumer that requires extra columns. |
| 5. Stale exported outputs | `[ ]` Stale again | Cross-sections are dated 10 August; the monthly panel is dated 11 August and both spatial lookups 12 August. |
| 6. Post-transaction exposure | `[-]` Still current | The all-years measure includes spill months after transactions. This needs an explicit interpretation. |
| 7. Stale Arrow partitions | `[ ]` Still current, broadened | Direct writes to live roots can leave stale or mixed generations; exact replacement needs staging and promotion. |
| 8. Category-B sales included | `[-]` Still current | 500,577 of 3,175,951 sales are category B, and the cross-section output does not carry the flag. |
| 9. Missing sales error wrapper | `[ ]` Still current | Rental logs and rethrows fatal errors; sales does not have the equivalent top-level wrapper. |
| 10. Missing `na.rm` | `[ ]` Still current, subsumed by finding 1 | All four `n_site_months` sums omit `na.rm = TRUE`; the necessary fix is broader than adding the argument. |
| 11. Rental-date proxy | `[-]` Still current | 1,038,849 of 1,450,255 rentals use `latest_to_rent` because the actual rented date is absent. |
| 12. Apparently unused `prior_12mo` | `[-]` Still current | No live consumer was found under `scripts/R/09_analysis/`; active prior-exposure work uses the daily-precision outputs. |

## P1 — High

### 1. `[ ]` NULL lookup rows remove zero-exposure controls from every output

- **Where:** `cross_section_sales.R:208,231-232,278-282,300,323-324`; the corresponding rental lines are `203,226-227,273-277,295,318-319`.
- **Problem:** the left joins preserve unmatched properties with `site_id` and `distance_m` equal to SQL `NULL`, but the conditional `n_site_months` aggregate and subsequent quality predicates are not NULL-safe. SQL `WHERE` then removes those rows. The prior-window filter drops them even earlier because `month_id` is NULL.
- **Current evidence:** the sales input has 3,175,951 IDs. The current sales lookup contains 148,426 NULL-sentinel IDs and omits another 9,280 IDs; all-years exports contain only 2,977,129–3,011,810 IDs, depending on radius. The rental input has 1,450,255 IDs and its lookup contains 40,921 NULL sentinels; exports contain only 1,361,063–1,383,525 IDs. An independent validator retraced the SQL path and confirmed the loss.
- **Why it matters:** these are properties beyond the 10 km lookup boundary or without usable coordinates. They are not missing at random, and many are the cleanest zero-exposure controls. A property just inside the lookup boundary can survive with zero exposure, while one just outside it disappears.
- **Fix:** build an explicit eligible property-by-radius baseline; distinguish valid no-match sentinels from genuinely ungeocoded records; make the aggregates, quality filters, and prior-window filters NULL-safe in both scripts; and assert input/output ID reconciliation for every radius.

### 3. `[-]` The prior-12-month window includes the transaction month but uses the cutoff for an exclusive window

- **Where:** `cross_section_sales.R:278-282,330-334`; `cross_section_rental.R:273-277,325-329`.
- **Problem:** the code selects offsets `-11` through `0`, so it includes the sale/rental month. The `>= 13` cutoff instead assumes twelve complete months strictly before the transaction. A December 2021 transaction has twelve months under the inclusive definition but is excluded. Including the transaction month can also count spills after the transaction date within that month.
- **Fix options:**
  1. For twelve complete months strictly before the transaction, select offsets `-12` through `-1` and retain the `>= 13` cutoff.
  2. For a twelve-month window including the transaction month, retain offsets `-11` through `0`, relax the cutoff to `>= 12`, and rename/document the measure accurately.
- **Required test:** use distinct spill values in December 2021 and January 2022 fixtures so both the eligibility boundary and included months are pinned.

### 4. `[ ]` The shared DuckDB cache has no source provenance and `dat_mo` has incompatible owners

- **Where:** `cross_section_sales.R:86-130,402-443`; `cross_section_rental.R:90-124,397-446`; `scripts/R/04_feature_engineering/compute_spill_stats.R:78-106`.
- **Problem A — stale values:** each table is loaded only when its name is absent. The direct entry points hardcode `main(refresh_db = FALSE)`, and no path, mtime, hash, schema, or producer version is checked before reuse.
- **Current evidence:** `data/duckdb.duckdb` is dated 7 July 2026. Its cached `dat_mo` totals are 5,230,885 spills and 38,900,559 hours, versus 1,559,620 and 11,803,351 in the current `agg_spill_mo.parquet`. A default run therefore produces plausible-looking results from obsolete values.
- **Problem B — table collision:** the cross-section scripts can create `dat_mo` with five common columns from `agg_spill_mo.parquet`. `compute_spill_stats.R` uses the same persistent name for `agg_spill_dry_mo.parquet` and requires two additional dry-spill columns. If a cross-section refresh owns the table first, `compute_spill_stats.R` fails on missing columns. If the dry-spill script owns it first, the cross-sections silently inherit that table's vintage.
- **Problem C — partial generations:** `refresh_db` removes tables one at a time. A failure during reload can leave a mixture of old and new persistent inputs. Positional `house_id`/`rental_id` keys make cross-vintage mixing especially dangerous if an upstream rebuild changes row order.
- **Fix:** use source-qualified Parquet views or source-qualified table names, including separate normal and dry monthly tables. Centralize loading and record a manifest with source path, fingerprint, schema, and producer version. Validate or rebuild the full input generation together; do not use table existence as a freshness contract.

### 5. `[ ]` Published cross-section outputs are stale again

- **Where:** `data/processed/cross_section/{sales,rentals}/{all_years,prior_12mo}/`.
- **Current evidence:** all four outputs were written on 10 August 2026. `agg_spill_mo.parquet` was rebuilt on 11 August; `spill_house_lookup.parquet` and `spill_rental_lookup.parquet` were rebuilt on 12 August. The published cross-sections therefore do not represent the current upstream generation.
- **Why it matters:** `scripts/R/09_analysis/01_descriptive/cross_sectional_plots.R` reads the all-years directories directly. A current analysis run can combine fresh source files with stale derived exposures without any warning.
- **Fix:** regenerate only after findings 1, 3, 4, 7, 13, and 14 are resolved. Publish from a verified cache/source generation, then read back and check source vintage, expected partitions, unique `(property_id, radius)` grain, and per-radius ID counts.

### 15. `[ ]` Refreshing the cache materializes the full spatial lookups in R before copying them to DuckDB

- **Where:** `cross_section_sales.R:93-128`; `cross_section_rental.R:97-122`.
- **Problem:** `rio::import()` first expands each full Parquet lookup into an R object, then `copy_to()` serializes it back into DuckDB. The current sales lookup is 4.08 GB compressed and 207,927,037 rows; the rental lookup is 1.28 GB and 112,159,617 rows. Peak memory is much larger than the Parquet sizes and competes with DuckDB's configured 16 GB limit.
- **Why it matters:** the safe recovery path for finding 4 (`refresh_db = TRUE`) is also the path most likely to exhaust memory before DuckDB can spill to disk.
- **Fix:** load directly inside DuckDB with `CREATE OR REPLACE TABLE ... AS SELECT <required columns> FROM read_parquet(...)`, or use views if repeated scan cost is acceptable. Record peak RSS and output signatures on both current artifacts.

### 16. `[ ]` The query plan multiplies the property-site-month relation by all five radii before aggregation

- **Where:** `cross_section_sales.R:161-164,190-192,236-250,277-284,329-348`; equivalent rental blocks.
- **Problem:** the spatial lookup is joined to the 48-row site-month panel before `cross_join(radius_tbl)`. The all-years sales relation can represent roughly 208 million property-site pairs × 48 months before the five-radius expansion. DuckDB may optimize or stream parts of the plan, but the script still asks it to evaluate the radius condition over an enormous repeated relation, twice for spill metrics and distance metrics, then again for the prior window.
- **Fix:** inspect `EXPLAIN ANALYZE` and benchmark alternatives. A likely design is to pre-aggregate all-years data at site level, use rolling site-period totals for transaction windows, and calculate all five nested-radius outputs in one property scan. Reconcile every output column against small fixtures and a sample of production IDs before adopting the rewrite.

## P2 — Moderate

### 6. `[-]` All-years exposure includes spill months after the transaction

- **Where:** `cross_section_sales.R:190-232`; `cross_section_rental.R:185-227`.
- **Problem:** the spill panel runs through December 2024, while the transactions are from 2021–2023. The all-years functions impose no transaction-time restriction.
- **Decision:** this is valid only if `all_years` is explicitly a time-invariant nearby-site quality measure. If it is described as exposure experienced before a transaction, cap it at the transaction date/month and rename it. Document the chosen interpretation in the scripts and paper.

### 7. `[ ]` Direct writes to live dataset roots can publish stale or mixed generations

- **Where:** `cross_section_sales.R:367-393`; `cross_section_rental.R:362-388`.
- **Problem:** the scripts write the all-years and prior-12-month roots sequentially. A failure between them exposes different generations. Arrow's default `existing_data_behavior = "overwrite"` replaces colliding file names but is not an exact dataset replacement contract; obsolete partitions or part files can survive when the radius set or file count changes.
- **Current state:** each current radius partition contains one `part-0.parquet`, so no duplicate part file was observed in the present outputs. The failure mode remains reachable on a changed partition set or interrupted run.
- **Fix:** write both outputs to a fresh generation/staging root, read them back, validate expected radii and grain, then atomically promote the complete generation. `delete_matching` can help within present partitions but does not by itself remove partitions absent from the new run.

### 8. `[-]` Category-B Land Registry transactions remain in the sales sample

- **Where:** upstream `scripts/R/02_data_cleaning/clean_lr_house_price_data.R`; consumed at `cross_section_sales.R:146-151`.
- **Current evidence:** 500,577 of 3,175,951 rows (15.8%) have `ppd_category = "B"`. The cross-section selects only `house_id`, `price`, and date, so the category cannot be filtered or used for robustness checks after export.
- **Decision:** either exclude category B in the canonical cleaner, carry the flag through the cross-section and make it a downstream sample choice, or document why these non-standard transactions belong in the primary sample. Inspect implausibly low prices at the same time.

### 11. `[-]` Most rental windows use a listing-removal date proxy

- **Where:** upstream `scripts/R/02_data_cleaning/clean_zoopla_data.R:200-208`; consumed at `cross_section_rental.R:140-146`.
- **Current evidence:** 1,038,849 of 1,450,255 rentals (71.6%) have no actual `rented` date and therefore use `latest_to_rent` through `rented_est`.
- **Decision:** carry a `has_actual_rented_date` flag into the output and report sensitivity on the 411,406 observations with an actual rented date. The proxy is especially important if the prior-window output is retained.

### 13. `[ ]` The quality filter creates different, non-monotonic samples at each radius

- **Where:** `cross_section_sales.R:220-232,312-324`; `cross_section_rental.R:215-227,307-319`.
- **Problem:** a property with no site inside a radius has `n_site_months = 0` and is retained with zero exposure. Once an in-radius site appears, the property is dropped if all of that site's monthly measures are missing. It can re-enter at a larger radius if another site has observed data. The nested-radius outputs therefore do not describe a fixed sample.
- **Current evidence:** sales all-years ID counts move 3,011,810 → 2,999,417 → 2,978,763 → 2,977,129 → 3,004,218 as radius increases from 250 m to 5 km. Rental counts show the same non-monotonic pattern. The current monthly panel has 523 sites with all 48 months missing.
- **Why it matters:** radius comparisons conflate exposure geography with sample selection, and a property can be treated as a valid zero at one radius but become unobservable at the next.
- **Fix:** retain every eligible property-radius row, encode wholly unobserved exposure as `NA`, preserve coverage fields, and make any complete-case or balanced-sample restriction explicit downstream.

### 14. `[ ]` Partial monthly coverage is silently converted into a lower spill total

- **Where:** the `spill_count` and `spill_hrs` sums at `cross_section_sales.R:195-202,287-294` and the equivalent rental lines.
- **Problem:** `na.rm = TRUE` is correct for aggregation mechanics but treats an incomplete observed history as if missing site-months contributed zero. The quality filter removes only 100% missing groups; partially observed groups keep understated canonical totals.
- **Current evidence:** `agg_spill_mo.parquet` has 80,675 missing site-month rows. In the current 5 km outputs, 2,488,579 sales rows and 1,214,789 rental rows have partial count coverage. The live descriptive consumer uses the totals but does not apply a coverage policy.
- **Fix:** choose and document a missing-data policy. Keep observed totals and coverage measures, but set canonical totals to `NA` when coverage is incomplete unless a justified threshold or adjustment is adopted. Apply the same policy to count and hours.

### 17. `[ ]` Both large result sets are collected and retained in R before export

- **Where:** `cross_section_sales.R:253-258,351-356,432-437`; `cross_section_rental.R:248-253,346-351,428-433`.
- **Problem:** each lazy DuckDB result is `collect()`ed, and `main()` retains both the all-years and prior-window data frames until export. This adds avoidable peak memory after the expensive query phase.
- **Fix:** export each relation directly from DuckDB, validate it, release it, and only then compute the next result. If Arrow remains the writer, process one radius/generation at a time without retaining both full outputs.

### 18. `[ ]` Runtime package installation bypasses the project's `rv` environment

- **Where:** both `initialise_environment()` functions, lines 18-34.
- **Problem:** the scripts say package management is handled by `rv` but call `install.packages()` when a dependency is missing. This can install an unpinned version, trigger network mutation during a pipeline run, or produce a machine-specific environment.
- **Project rule:** `AGENTS.md` states that R package management uses `rv`. The repository already provides `scripts/R/utils/script_setup.R::check_required_packages()`, which stops with an instruction to run `rv sync`.
- **Fix:** source `script_setup.R`, replace the installer loop with `check_required_packages()`, and use explicit namespace calls or the established package-loading pattern.

## P3 — Low / cleanup

### 9. `[ ]` The sales script lacks the rental script's fatal-error wrapper

- **Where:** `cross_section_sales.R:402-439` versus `cross_section_rental.R:397-442`.
- **Problem:** the rental entry point logs fatal errors and a final timestamp; the sales entry point does not. `on.exit()` still disconnects the sales connection, so this is observability rather than data correctness.
- **Fix:** use the same top-level error/finally behavior in both scripts. Prefer one shared, parameterized builder or parity tests so the near-duplicate pipelines cannot continue to drift silently.

### 10. `[ ]` `n_site_months` omits `na.rm = TRUE`

- **Where:** `cross_section_sales.R:208,300`; `cross_section_rental.R:203,295`.
- **Problem:** this is not an isolated style issue. It exposes the R-semantics assumption behind finding 1. Adding `na.rm = TRUE` is necessary, but it does not repair the prior-window filter or distinguish zero-match from ungeocoded records.
- **Fix:** address as part of finding 1 and retain an explicit NULL fixture.

### 12. `[-]` The monthly `prior_12mo` outputs have no live repository consumer

- **Where:** `data/processed/cross_section/{sales,rentals}/prior_12mo/` and both `create_prior_12mo_db()` paths.
- **Current evidence:** the only live cross-section consumer reads `all_years`. Active prior-exposure analyses use the newer daily-precision `prior_to_sale` and `prior_to_rental` datasets.
- **Decision:** either remove the monthly path and its duplicate compute cost, or label it as a supported monthly-grain robustness dataset and add a real consumer/test. Do not preserve it solely because it already exists.

## Verified non-issues and completed items

- **Rental radius is fixed:** `scripts/R/04_feature_engineering/site_rental_match.R` sets `radius_km = 10`; the current lookup reaches 10,000 m and `scripts/R/testing/test_property_site_match_contracts.R` covers the contract.
- `house_id` and `rental_id` are unique per input row, so the current `group_by()` calls do not merge repeat transactions and the final price/rent joins do not fan out. The names still describe transaction/listing rows, not stable properties.
- `site_id` is currently globally unique, and the current monthly panel has one row per site-month over 13,990 sites × 48 months. Joining on `site_id` alone does not presently fan out.
- The month-ID formula is consistent with the current monthly producer.
- The common `spill_count_mo` and `spill_hrs_mo` columns in the current normal and dry monthly Parquet files are identical. This does **not** clear finding 4: the shared `dat_mo` schema and vintage still differ, and `compute_spill_stats.R` requires dry columns that a cross-section refresh removes.
- `n_distinct(if_else(...), na.rm = TRUE)` correctly ignores SQL NULLs.
- The radius cross join does not logically double-count within a radius; finding 16 concerns execution scale, not the mathematical grouping.
- `PRAGMA memory_limit` and `PRAGMA max_memory` are redundant aliases but harmless.

## Testing and validation gaps

- No production test sources the current scripts and exercises their functions. `scripts/R/testing/test_cross_section.Rmd` embeds an old copy of the pipeline instead, so it can stay green while production code changes.
- No fixture distinguishes: observed in-radius sites; all sites outside a radius; a valid NULL no-match sentinel; an ID absent from the lookup; wholly missing site histories; and partially missing histories.
- No boundary fixture pins the transaction-month window policy.
- No contract verifies that every eligible input ID appears once per radius, or records explicit justified exclusions.
- No integration test changes a source Parquet between runs and proves that the persistent DuckDB table refreshes or fails clearly.
- No test runs `cross_section_*` and `compute_spill_stats.R` sequentially against one temporary database to expose the `dat_mo` collision.
- No post-publication read-back verifies radii, `(property_id, radius)` uniqueness, source vintage, row counts, and absence of stale parts.
- No interrupted-publication test proves that readers see either the old complete generation or the new complete generation, never a mixture.
- No production-scale benchmark records peak R memory, DuckDB temporary-disk use, and elapsed time for a full refresh.

## Relevant repository learnings

- `docs/solutions/best-practices/edm-api-combine-hardening-20260310.md`: validate a candidate before replacing canonical Parquet; atomic promotion is the relevant next hardening step here.
- `docs/solutions/best-practices/lr-house-price-local-ons-postcode-lookup-20260310.md` and `zoopla-local-ons-postcode-lookup-20260310.md`: lookup coverage and core geography completeness are source contracts. Preserve the distinction between source missingness and downstream sample loss.
- No existing solution document establishes a DuckDB cache-provenance or SQL-NULL policy for these scripts; the current findings therefore come from present code and artifact evidence, not inherited assumptions.

## Recommended fix order

1. Decide the estimands and missing-data policy: findings 3, 6, 8, 11, 13, and 14.
2. Add the NULL/window/cache fixtures and ID-reconciliation checks before changing the query implementation.
3. Replace the shared implicit cache and `dat_mo` collision (finding 4), then load Parquet directly in DuckDB (finding 15).
4. Fix NULL/sample-retention behavior (findings 1 and 13) and the chosen window/coverage semantics (findings 3 and 14) in both scripts.
5. Replace direct live-root writes with staged, validated publication (finding 7) and reduce result materialization (findings 16 and 17).
6. Rebuild both cross-sections from the verified current source generation and close finding 5 only after read-back checks pass.
7. Apply cleanup items 9, 10, and 18; then decide whether `prior_12mo` remains a supported output.
