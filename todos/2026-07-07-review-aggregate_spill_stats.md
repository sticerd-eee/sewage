# Code review — `aggregate_spill_stats.R`

- **Date:** 2026-07-07
- **Scope:** whole-file sanity check of `scripts/R/03_data_enrichment/aggregate_spill_stats.R`, including the shared utilities it depends on (`scripts/R/utils/spill_aggregation_utils.R`), verified against the upstream merge outputs and the downstream consumers.
- **Method:** four parallel review agents (correctness, adversarial edge-case, pipeline contract, maintainability), with the highest-impact findings verified directly against the actual parquet files and, for the counting logic, by executing the utility functions on constructed inputs.
- **Verdict:** one critical bug (finding 1) that inflates the monthly and quarterly panels and flows into the hedonic cross-section; two high findings; the rest are moderate/low robustness and hygiene items. The yearly output is broadly sound.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## Critical

### 1. `[x]` Event-free months and quarters are filled with the site's ANNUAL Environment Agency total

- **Where:** `scripts/R/03_data_enrichment/aggregate_spill_stats.R:248-257` (monthly) and `:280-289` (quarterly), inside `complete_data_observations()`.
- **Problem:** after the completion grid is built, any month (or quarter) with no matched events has `NA` for `spill_count_mo` / `spill_hrs_mo`, and the `dplyr::coalesce()` then fills that cell with `spill_count_ea_crosswalk` / `spill_hrs_ea_crosswalk` — which are **annual** totals. Every event-free month of a site-year therefore carries the whole year's spill figure. This is correct at the yearly grain (lines 217–226, where the fallback covers site-years with no matched events at all) but is a grain mismatch at monthly and quarterly frequency.
- **Evidence (verified against the written output files):**
  - 216,361 of 671,520 rows in `agg_spill_mo.parquet` (about a third) carry a value exactly equal to a positive annual EA total.
  - For 2022, 9,171 of 13,990 sites have summed monthly hours exceeding their own yearly total; the worst cases are exactly 12 × the annual figure (e.g. site 5984, South West Water: 41,752 summed monthly hours vs 3,479 yearly hours).
  - Aggregate monthly sums are roughly 3.4 × the yearly sums in every year 2021–2024.
- **Downstream impact:** no consumer undoes this. `cross_section_sales.R:194-210` joins `agg_spill_mo` to houses by the month of sale and sums with `na.rm = TRUE`; `cross_section_rental.R`, `population_exposure.R`, `repeat_sales.R` (quarterly), and `aggregate_dry_spill_stats.R` all read these files without touching `annual_status` or the `_ea_crosswalk` columns.
- **Suggested fix:** keep the EA fallback at yearly grain only. At monthly/quarterly grain: fill event-free periods with **0** for site-years that report (`annual_status` of `reported_positive` or `reported_zero` with event coverage), keep `NA` for `reported_na`/`absent`, and if an EA-only fallback is wanted at sub-annual grain, apportion (total ÷ 12 or ÷ 4) and add an explicit imputation flag column. Then regenerate every downstream artifact built from `agg_spill_mo` / `agg_spill_qtr`.
- **Resolution (2026-08-10):** annual EA totals remain a yearly fallback only. Missing subperiods are zero-filled for `reported_zero` years and event-covered `reported_positive` years; EA-only positive, `reported_na`, and `absent` years remain `NA`. Contract tests and regenerated-output checks confirm unchanged schemas and keys, with aggregate monthly/yearly hours falling from 3.254 to 0.987 and quarterly/yearly hours from 1.223 to 0.987.
- **Flagged by:** correctness, adversarial, pipeline-contract (independently); confirmed empirically by the orchestrator.

---

## High

### 2. `[x]` Year-boundary clamp creates a phantom spill counted in January of the wrong year

- **Where:** `scripts/R/utils/spill_aggregation_utils.R:60-67` (`split_monthly_records()` has no output-side `end_time > start_time` filter, unlike `split_daily_records()` at line 116), interacting with the year clamp in `prepare_spill_data()` (lines 139–148).
- **Problem:** a spill that runs across New Year is clamped to end exactly at 1 January 00:00:00 of the following year. The interval-overlap join then matches the following-January window and produces a zero-duration piece. That piece is not dropped, its `month` becomes 1, but its `year` column is still the original label year — so `month_id` places it in January of the **previous** year. `count_spills()` counts a zero-duration record as one spill (verified by executing the function).
- **Evidence:** 2,767 spills in the current data are clamped at a year end and each produces one phantom January record.
- **Suggested fix:** add `out <- out[end_time > start_time]` after the clamp in `split_monthly_records()`, mirroring `split_daily_records()`. Then re-run and diff January/Q1 counts.
- **Resolution (2026-08-10):** year boundaries are now constructed explicitly in UTC through the shared `clamp_spill_records_to_year()` helper. Monthly windows end at the exact next-month boundary, and the overlap output drops every non-positive slice before counting. Focused UTC/Europe-Rome contract tests pass. On the rebuilt input, all 535 cross-year events reconcile to their 573 monthly slices with zero non-positive slices, zero wrong-label-year slices, and a maximum duration difference of `9.1e-13` hours.
- **Flagged by:** adversarial (verified by execution); magnitude verified by the orchestrator.

### 3. `[ ]` Exact-duplicate event rows double spill hours but not spill counts

- **Where:** `scripts/R/03_data_enrichment/aggregate_spill_stats.R:95-96` (`load_data()` applies no deduplication to the event data) together with `calculate_spill_hours()` (`spill_aggregation_utils.R:250-255`), which sums durations, while `count_spills()` effectively absorbs duplicates into the same block.
- **Problem:** identical `(site_id, start_time, end_time)` rows are summed twice in hours but merge into one counted block, so hours and counts are computed from inconsistent record sets.
- **Evidence:** 2,979 duplicated keys / 3,284 surplus rows in `matched_events_annual_data.parquet` (small relative to 7.27 million rows, but concentrated on specific sites).
- **Suggested fix:** deduplicate exact `(site_id, start_time, end_time)` rows in `load_data()` with a logged drop count. Separately decide whether overlapping (non-identical) intervals at the same site should be union-merged for the hours measure; the duplicate source is worth tracing upstream in the merge script.
- **Flagged by:** adversarial; confirmed empirically by the orchestrator.

---

## Moderate

### 4. `[ ]` `month_id` / `qtr_id` computed twice from two independently hardcoded base years

- **Where:** `spill_aggregation_utils.R:159-161` (`base_year <- 2021` hardcoded inside `prepare_spill_data()`) vs `aggregate_spill_stats.R:129-130` (recomputed from `CONFIG$base_year`).
- **Problem:** the recomputation silently overwrites identical values today; if either constant is ever changed alone, the two grains diverge with no error. `aggregate_dry_spill_stats.R` uses the utils values with its own separate config entry.
- **Suggested fix:** add a `base_year` parameter to `prepare_spill_data()`, pass `CONFIG$base_year`, delete the recomputation in this script.

### 5. `[ ]` Misleading default argument `metadata = data$metadata`

- **Where:** `aggregate_spill_stats.R:181-182`.
- **Problem:** the `data` argument at the call site is `aggregate_spills()`'s output, which has no `$metadata` element, so the default silently evaluates to `NULL` and would fail later with a confusing `select()` error. Every current call passes `metadata` explicitly, so this is a latent footgun only.
- **Suggested fix:** make `metadata` a required argument (drop the default).

### 6. `[ ]` Dead configuration entry `CONFIG$data_path_annual`

- **Where:** `aggregate_spill_stats.R:63-65`.
- **Problem:** `annual_return_edm.parquet` is never read anywhere in the script (single grep hit is the assignment itself). It misstates the script's input surface; the EA totals now come from the crosswalk.
- **Suggested fix:** delete the entry.

### 7. `[ ]` No guards for inverted or open-ended events at the yearly grain (latent)

- **Where:** `spill_aggregation_utils.R:137-153` (`prepare_spill_data()` filters `NA` `site_id`/`start_time` but not `NA` `end_time` or `end_time <= start_time`); `count_spills()` line 221 adds +1 for any record and returns `NA` for the whole site-year if any `end_time` is `NA`, after which the coalesce in the main script silently substitutes the EA total.
- **Problem:** the yearly path counts inverted records and sums negative hours; a single `NA` end poisons the count and is then invisibly replaced by a different measurement instrument. **Current data is clean** (zero `NA` timestamps, zero inverted rows), so this is robustness hardening, not a live bug.
- **Suggested fix:** after the year clamp, drop or clamp bad rows with a logged count, and log whenever the EA fallback replaces a computed value.

---

## Low

### 8. `[ ]` `count_spills()` overcounts when a spill starts exactly at a block boundary

- **Where:** `spill_aggregation_utils.R:214` (`gap > 0`).
- **Problem:** a spill starting exactly at `block_end` (gap of exactly zero) takes the within-block branch and measures from `block_start`, counting an empty elapsed block: spills `[0h,1h]` and `[36h,37h]` return 3 where the Environment Agency 12/24 method gives 2 (verified by execution). Measure-zero in continuous timestamps, so practical impact is negligible. The comments at lines 172–175 and 213 also misstate the reset rule (">24h gap" vs the actual `gap > 0` after pre-advanced blocks — the code's rule is otherwise equivalent to the EA convention).
- **Suggested fix:** change the condition to `gap >= 0` and fix the comments; add unit tests pinning EA worked examples.

### 9. `[x]` One-second undercount of hours per month-crossing spill

- **Where:** `spill_aggregation_utils.R:52` (`month_end` is next month start minus 1 second).
- **Problem:** the boundary second is lost for every month-crossing spill, so summed monthly hours are systematically (trivially) below yearly hours even after fixing finding 1.
- **Suggested fix:** clamp piece ends to the true boundary and drop zero-duration rows (same edit as finding 2).
- **Resolution (2026-08-10):** monthly windows now use the exact start of the following month. Focused tests confirm that ordinary month crossings and exact-boundary endings retain their full duration, while the production-data reconciliation described in finding 2 confirms the invariant across all current cross-year events.

### 10. `[ ]` Monthly/quarterly outputs drop calendar columns

- **Where:** `aggregate_spill_stats.R:258-262` and `:290-294`.
- **Problem:** only `month_id` / `qtr_id` survive; consumers must re-derive calendar time with their own hardcoded base year (e.g. `compute_spill_stats.R:155` derives a 1–4 index). Consistent today, fragile tomorrow.
- **Suggested fix:** also write `year` and `month` / `quarter`.

### 11. `[ ]` No input-contract or grain assertions

- **Where:** `aggregate_spill_stats.R:95-106` and the three metadata joins (lines 213–216, 244–247, 276–279).
- **Problem:** grain safety currently rests entirely on the upstream `stop()` check in `merge_outputs_utils.R:520-523`. (Verified: the crosswalk is unique at site/year/company — 55,960 rows, zero duplicate keys — so there is **no live join fanout**.) A local assertion would fail fast if the upstream invariant is ever weakened.
- **Suggested fix:** assert crosswalk uniqueness on `(site_id, year, water_company)` and required columns before the joins, mirroring the preflight checks in `merge_individ_annual_location.R`.

### 12. `[ ]` Logging: stale numeric prefix and colour codes written to file

- **Where:** `aggregate_spill_stats.R:41-48`.
- **Problem:** the log name `12_aggregate_spill_stats.log` uses a stale pipeline-position prefix (siblings use the plain script basename), and `layout_glue_colors` writes ANSI escape codes into a file log.
- **Suggested fix:** rename to `aggregate_spill_stats.log`, use a plain layout, remove the orphaned old log file.

### 13. `[ ]` Documentation drift (cluster)

- `aggregate_spill_stats.R:8-9` — header sentence is missing its verb ("This script individual sewage overflow spills into…") and says aggregation is by "catchment area" when the code groups by water company and site.
- `aggregate_spill_stats.R:74-79` — `load_data()` return doc omits `year` and `water_company`, which are selected.
- `aggregate_spill_stats.R:169-180` — `complete_data_observations()` docs omit the quarterly component in both `@param` and `@return`.
- `aggregate_spill_stats.R:153` — the comment "(D13: uses quarter-split data)" is wrong: the quarterly result uses month-split data grouped by quarter.
- `aggregate_spill_stats.R:304` — `export_results()` doc mentions only `$yearly` and `$monthly`; it also writes quarterly.
- `aggregate_spill_stats.R:338-340` — the "imported from shared utilities" notes sit before `main()` although `split_monthly_records()` is never called directly in this file; move next to the `source()` call or delete.

---

## Related scripts (out of scope, surfaced during contract checks)

### R1. `[ ]` Long-difference exposure sums propagate `NA` without handling

- **Where:** `scripts/R/06_analysis_datasets/grid_long_difference_sales.R:245-251` (and the analogous line in `grid_long_difference_rentals.R`).
- **Problem:** house-level exposure sums `spill_count_yr` / `spill_hrs_yr` **without** `na.rm = TRUE`. The aggregation deliberately keeps `NA` for `reported_na` and `absent` site-years, so one such site near a house poisons that house-year's exposure to `NA`, silently changing regression sample composition.
- **Suggested fix:** filter on `annual_status` (or use `na.rm = TRUE` with a logged count of `NA` contributors) and document the intended treatment.

### R2. `[ ]` Dry-spill aggregation fabricates zeros for 2024

- **Where:** `scripts/R/03_data_enrichment/aggregate_dry_spill_stats.R:56` (years `2021:2023`) and its zero-fill of unmatched rows when integrating with the 2021–2024 spill panel.
- **Problem:** 2024 rows have no dry-spill classification, but the unconditional zero-fill records them as "zero dry spills" rather than "not classified". These feed `compute_spill_stats.R` and `load_data_sewage.R`.
- **Suggested fix:** restrict the zero-fill to the dry-classification coverage window, or extend the classification to 2024.

---

## Verified non-issues (checked and cleared)

- **Crosswalk join fanout:** the crosswalk is unique at (site, year, water company) — 55,960 rows, zero duplicate keys — and the upstream row-accounting `stop()` enforces this at write time. The three left joins are safe.
- **Company-label mismatch:** no site carries more than one water company label in either the events or the crosswalk, and every event (site, company) pair exists in the crosswalk, so the completion grid does not fabricate company-year rows in the current data.
- **Timestamps:** stored as UTC POSIXct; zero `NA` values; zero inverted intervals in the current merged events.
- **`gap` variable staleness in `count_spills()`:** safe — the first iteration short-circuits on `is.na(block_end)` before `gap` is read, and `gap` is reassigned whenever `block_end` is set.
- **Quarterly counting of month-split pieces:** traced cases show counting the split pieces reproduces the count for the unsplit event, apart from the boundary cases in findings 2 and 8.
- **`spill_count_ea` / `spill_hrs_ea` semantics:** confirmed works-year annual totals summed across member outlets with an all-`NA` guard (so they cannot silently differ across rows).

---

## Testing gaps

1. No reconciliation test asserting that summed monthly (and quarterly) counts/hours are consistent with the yearly figures per site-year — this single assertion would have caught finding 1 immediately. `diff_aggregate_spill_stats_ch9.R` only checks row-count multipliers and status membership.
2. No unit tests for `count_spills()` against Environment Agency worked examples (exact-boundary starts, long spills spanning several blocks, month-split slices vs unsplit events, `NA` / inverted ends).
3. No uniqueness assertions on the completed outputs (one row per site/company/period).
4. No test that year labels bracket event timestamps after `prepare_spill_data()`, or that the yearly grain contains no non-positive durations.
5. No end-to-end test of `NA` propagation through the long-difference exposure sums (related finding R1).

---

## Coverage notes

- Reviewers: correctness and adversarial on the session model; pipeline-contract and maintainability on the mid-tier model. All four completed; the pipeline-contract agent needed one resume nudge.
- Findings below confidence threshold or refuted by direct data checks were dropped (notably: crosswalk join fanout, multi-company sites, timezone mismatch inside the R pipeline — though whether the *raw* EDM sources record British Summer Time as UTC upstream was not checked and remains an open question for Layer 01/02).
- Run artifacts: `/tmp/compound-engineering/ce-code-review/20260706-234730-1175f9cb/`.
