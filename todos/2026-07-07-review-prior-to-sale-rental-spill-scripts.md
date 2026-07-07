# Code review — `house_spill_prior_to_sale.R` and `rental_spill_prior_to_rental.R`

- **Date:** 2026-07-07
- **Scope:** whole-file sanity check of `scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R` and `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R` (near-identical twins), including their use of `count_spills()` from `scripts/R/utils/spill_aggregation_utils.R`, verified against the actual parquet inputs and the shipped outputs under `data/processed/cross_section/`.
- **Method:** eight parallel review agents (correctness, adversarial, performance, testing, maintainability, project standards, agent-native, learnings research), with every load-bearing finding verified by the orchestrator through direct execution against the real data — including a small end-to-end rerun of the rental join path and targeted probes of R's type-dispatch behaviour.
- **Verdict:** no finding invalidates the currently shipped outputs at scale. The most important discovery is that the rental script produces correct results **only because the lubridate package happens to repair a broken Date-versus-POSIXct comparison** (finding 1) — a silent, catastrophic failure mode one package-list edit away. Two small but real data defects are present in the shipped artifacts (finding 2), and one copy-paste slip is waiting to corrupt a future rerun (finding 3). The missingness design (findings 4 and 5) deserves a coauthor discussion rather than a mechanical fix.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## High

### 1. `[ ]` Rental window filter compares POSIXct with Date and works only because lubridate is attached

- **Where:** `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R:187` (window filter `start_time < rented_est`) and `:195` (`pmin(end_time, rented_est)`); root type comes from `data/processed/zoopla/zoopla_rentals.parquet`, where `rented_est` is stored as `date32[day]` and collects to R class `Date`, while event times are `POSIXct`. The house twin is unaffected because `date_of_transfer` is stored as a UTC timestamp.
- **Problem:** in base R, comparing a `POSIXct` with a `Date` does not coerce; it emits an "Incompatible methods" warning and falls back to comparing raw numbers — seconds since 1970 against days since 1970 — so `start_time < rented_est` is always `FALSE`. Had that path executed, every rental would have received zero spill exposure with nothing but a console warning. The script survives because `initialise_environment()` attaches lubridate, which registers comparison methods that treat the `Date` as UTC midnight, making the comparison (and the `pmin()` clamp) correct.
- **Evidence (verified by execution):**
  - Without lubridate: `as.POSIXct("2021-06-01 12:00", tz="UTC") < as.Date("2021-06-02")` returns `FALSE` with a warning; a data.table filter using it returns zero rows.
  - With lubridate attached (as the script does): the same comparison returns `TRUE`, and a 5,000-rental end-to-end rerun of the script's exact join logic returns 62,937 filtered rows — exactly matching an explicitly type-correct comparison.
  - The shipped rental artifact is consistent with the correct path: 64.6% of rows have positive spill hours.
- **Why it still matters:** correctness rests on an invisible side effect of a package attachment. Removing lubridate from `required_packages`, running the functions in a fresh session, or copying this code into a script that doesn't attach lubridate would silently zero out the entire rental exposure measure. Two independent review agents reproduced the broken behaviour and reported it as an active critical bug precisely because the guard is undiscoverable.
- **Suggested fix:** make the type explicit. In `load_data()`, immediately after collecting `rental_dt`, add `rental_dt[, rented_est := as.POSIXct(rented_est, tz = "UTC")]`. Better still, also store `rented_est` as a UTC timestamp upstream in `clean_zoopla_data.R` so the two transaction datasets share one type contract, and add a `stopifnot(inherits(..., "POSIXct"))` guard after load in both twins.
- **Flagged by:** correctness and adversarial (independently, both at maximum confidence); impact corrected and mechanism pinned down empirically by the orchestrator.

---

## Moderate

### 2. `[ ]` Transactions dated exactly 2021-01-01 get a zero-day window and NaN averages — present in shipped outputs

- **Where:** `house_spill_prior_to_sale.R:102` (filter `>= CONFIG$window_start`) with the divisions at `:373-378`; same pattern in `rental_spill_prior_to_rental.R:102` and `:372-377`; denominator built in `get_house_metadata()` / `get_rental_metadata()` (`n_days_in_window`).
- **Problem:** a sale or rental dated exactly on the window start passes the `>=` filter with `n_days_in_window = 0`. No event can survive the clamp for such a transaction (any overlapping fragment has zero length), so the averages evaluate `0 / 0 = NaN`, which is written to the output.
- **Evidence (verified in shipped artifacts):** 63 sales and 373 rentals are dated 2021-01-01. The sales output contains 118 rows with `n_days_in_window == 0`, all with `NaN` daily averages; the rentals output contains 820 such rows, 661 `NaN` and the rest `NA` via the missing-site mask.
- **Suggested fix:** either make the load filter strict (`> CONFIG$window_start`) and document that day-one transactions are excluded, or keep them and guard the four average columns with `fifelse(n_days_in_window > 0, metric / n_days_in_window, NA_real_)`, logging the affected count. Apply identically in both twins.
- **Flagged by:** adversarial, correctness, and testing (independently); counts confirmed empirically by the orchestrator.

### 3. `[ ]` Empty-chunk fallback schema diverges from the real output — wrong column name and wrong id type (latent)

- **Where:** `rental_spill_prior_to_rental.R:307-318` (declares `price = numeric()` although the script produces `listing_price`, and `rental_id = character()` although real ids are integers); `house_spill_prior_to_sale.R:307-318` (declares `house_id = character()` although real ids are integers).
- **Problem:** if any chunk of 10,000 ids ever has no site within 1,000 m, `process_chunk()` returns this fallback table, and `rbindlist(..., fill = TRUE)` merges its schema with the real chunks. Verified by execution: the id column of the **entire** result is promoted to character, and the rentals output gains a phantom all-`NA` `price` column alongside `listing_price`. Everything keyed on integer ids downstream then misbehaves or slows.
- **Evidence:** direct `rbindlist` test reproduces both effects; the July 6 runs had no empty chunk (output schemas are clean), so this is latent, not active. It is also the one substantive divergence between the twins — a copy-paste slip from the sales script.
- **Suggested fix:** in the rental fallback rename `price` to `listing_price` and declare `rental_id = integer()`; in the house fallback declare `house_id = integer()`. Cheaper and safer still: build the fallback once from the real schema (e.g. an empty prototype constant) so it cannot drift.
- **Flagged by:** maintainability, correctness, adversarial, testing, and project standards (independently); mechanism verified by the orchestrator.

### 4. `[ ]` `site_missing` requires availability over the whole 2021–2023 range instead of each transaction's own window

- **Where:** `derive_site_missing_flags()` in both scripts (lines 65-90), driven by `sample_years = seq(min, max)` of all transaction years (lines 108-116).
- **Problem:** a site is flagged missing — and all its pair metrics set `NA` — if it lacks EDM availability in **any** year of the global sample range, even for transactions whose exposure window ends before the gap. A 2021 sale next to a site with full 2021 coverage is discarded because the site's monitor was down in 2023, two years after the window closed.
- **Evidence (quantified at the 1,000 m radius):** 385,547 sale rows are flagged missing; 38,371 of them (about 10%, including 32,864 rows from 2021 sales) belong to transactions whose own window years are fully covered. Overall, 8.8% of sale rows and 12.8% of rental rows carry `NA` metrics.
- **Suggested fix:** make the flag transaction-specific: for each site, precompute the largest year Y such that 2021 through Y are all available, and flag a pair missing only when the transaction year exceeds Y. This is a methodological choice about measurement error, so decide it with coauthors before changing the estimation sample.
- **Flagged by:** correctness and adversarial; magnitude quantified by the orchestrator.

### 5. `[ ]` A single transaction beyond the availability columns silently voids the entire sample (latent)

- **Where:** `derive_site_missing_flags()` lines 70-79 in both scripts: absent `available_year_YYYY` columns are created as all-`FALSE` with only a file-log warning.
- **Problem:** `unique_spill_sites.parquet` carries availability flags for 2021–2024 only. The moment the transaction data extends into 2025 (or one mis-dated row does), `sample_years` widens, the missing column is filled `FALSE` for every site, and **every** site in the sample is flagged missing — the scripts then export an all-`NA` dataset while exiting successfully.
- **Evidence:** simulated with the real sites table: a 2021–2025 window flags 13,990 of 13,990 sites (100%) as missing, versus 885 (6.3%) for 2021–2024. Current data (transactions through 2023-12-31) does not trigger it.
- **Suggested fix:** replace the fill-with-`FALSE` fallback with `stop()` (or at minimum a console warning plus a logged share of flagged sites, aborting if it exceeds a sanity threshold). When the data refresh to 2024+ happens, regenerate the availability columns first.
- **Flagged by:** adversarial; simulated and confirmed by the orchestrator.

### 6. `[ ]` Re-running after a radius change leaves stale partitions in the output dataset

- **Where:** `export_data()` — `house_spill_prior_to_sale.R:401-406`, `rental_spill_prior_to_rental.R:399-404` (`arrow::write_dataset(..., partitioning = "radius")`).
- **Problem:** `write_dataset()`'s default `existing_data_behavior` overwrites only the partitions it writes. If `radius_thresholds` is ever narrowed or changed (the repo already uses different radius sets elsewhere, e.g. 2,000/5,000 m in the `prior_12mo` outputs), old `radius=` directories survive and `open_dataset()` downstream silently unions stale and fresh rows.
- **Suggested fix:** pass `existing_data_behavior = "delete_matching"` in both scripts.
- **Flagged by:** adversarial; current outputs verified clean by the orchestrator.

### 7. `[ ]` No output contract check for either script

- **Where:** project-level gap; the repo's convention is ad-hoc validation scripts under `scripts/R/testing/`.
- **Problem:** none of the defects above would announce themselves: finding 1 zeroes a regressor silently, finding 2 ships NaN rows, finding 3 flips a column type, finding 5 voids the sample — all with a green exit. A single cheap contract script would catch every one of them.
- **Suggested fix:** add `scripts/R/testing/test_prior_exposure_contracts.R` asserting, for both outputs: unique grain on (id, site_id, radius); no NaN/Inf in the four average columns; `site_missing` rows are exactly the `NA`-metric rows; radius values match `CONFIG$radius_thresholds`; id columns are integer; and a non-degenerate positive-exposure share (e.g. between 30% and 90% of non-missing rows).
- **Flagged by:** testing (with near-identical suggestions from correctness and adversarial).

### 8. `[ ]` The two scripts are hand-maintained ~440-line twins

- **Where:** both files in full; a mechanical diff confirms they differ only in the id/price/date renames plus finding 3's slip.
- **Problem:** every future fix (including the ones in this review) must be applied twice, and finding 3 demonstrates the failure mode. A single parameterized pipeline (id column, price column, date column, input and output paths) called by two thin wrappers would eliminate the class of bug.
- **Suggested fix:** extract the shared body into `scripts/R/utils/` when convenient — ideally as part of applying this review's fixes, so they land once.
- **Flagged by:** maintainability; twin equivalence verified by the learnings researcher.

---

## Low

### 9. `[ ]` Leftover debug markers, dead code, and stale comments

- **Where (both scripts unless noted):** `# TEST: REMOVE LATER` sitting above the *production* radius loop (house:244, rental:244); the superseded commented-out `rbindlist(lapply(...))` block (house:257-263, rental:257-263); commented-out `head(all_house_ids, 50000)` test lines (house:337, rental:336); the inaccurate `# Shallow copy (copies structure, not data)` comment on a plain subset (house:249, rental:249); the unresolved `# CHANGE: Add site_id to partitioning or adjust as needed` comment (house:400 only); the house log file named `house_site_prior_to_sale.log` while every sibling logs under its own script name (house:37).
- **Problem:** the `TEST: REMOVE LATER` marker in particular invites someone to delete live production code; the rest is drift that misleads the next reader.
- **Suggested fix:** one cleanup pass over both files: delete the markers and dead blocks, fix or remove the stale comments, rename the log file.
- **Flagged by:** maintainability, project standards, and agent-native (in passing).

### 10. `[ ]` `count_spills()` is an R-level loop called once per house-site pair

- **Where:** `calculate_metrics_by_radius()` at line 223 of both scripts, calling `spill_aggregation_utils.R:181-240`.
- **Problem:** the per-group interpreted loop is the pipeline's dominant CPU cost. It completed fine on the July 6 runs, so this is a scaling note for wider radii or longer samples, not a defect.
- **Suggested fix:** none needed now; if runtimes grow, vectorise the block-counting or add timing instrumentation first.
- **Flagged by:** performance.

---

## Pre-existing upstream issues that reach these scripts (tracked elsewhere, not counted in the verdict)

- **Duplicate event rows inflate spill hours:** 3,284 exact duplicate (site_id, start_time, end_time) rows among 7.27 million events (0.05% of rows, 0.48% of total hours) are summed twice into `spill_hrs` while `count_spills()` absorbs them. Known from the `aggregate_spill_stats.R` review (finding 3 there); fix by deduplicating in `load_data()` or upstream.
- **`count_spills()` block-boundary overcount:** the `gap > 0` test at `spill_aggregation_utils.R:214` should arguably be `gap >= 0`; known from the same review (finding 8 there). Measure-zero on continuous timestamps.
- **Rental lookup radius asymmetry:** `todos/012-pending-p0-review-10km-site-match-scripts.md` records that the rental site lookup was built at 5 km versus 10 km for houses. Invisible here because both scripts filter to 1,000 m, but a trap if `radius_thresholds` is ever widened.
- **Overlapping simultaneous events:** `spill_hrs` sums wall-clock hours of overlapping events at the same site without union-merging intervals; a site with two parallel monitored outlets can log more spill hours than elapsed hours. Methodological convention — worth one deliberate decision.
- **Provenance note:** the July 6 run logs show the scripts executed from a `/private/tmp/sewage-ch10` worktree. The artifacts now in `data/processed/cross_section/` were verified consistent with the current tree's code and inputs (a fresh partial rerun matches), so this is documentation, not a defect.

---

## Review coverage notes

- All eight reviewers returned; no failures. One maintainability finding ("documentation says day before the sale but code clamps to the sale timestamp") was **dropped as a false positive** during validation: both transaction date columns are midnight-precision, so clamping to the timestamp is exactly "through the end of the previous day" and documentation and behaviour agree.
- Both session-model reviewers independently reported finding 1 as an active critical bug ("all rental exposure is zero and the shipped artifact is not reproducible"). The orchestrator's end-to-end rerun with the script's own package environment refuted the impact claim — lubridate's attachment makes the shipped output correct and reproducible — and the finding was re-scoped to the latent hazard described above.
- Validation used direct execution against the real parquet data (type-dispatch probes, a 5,000-rental pipeline rerun, output scans, an rbindlist schema test, and a missingness simulation) in place of a separate validator wave; every finding above carries its empirical evidence inline.
- Per the review request, no fixes were applied and no branches were created; this document is the deliverable.
