# Code review — `aggregate_rainfall_stats.R`

- **Date:** 2026-08-17
- **Scope:** whole-file audit of `scripts/R/03_data_enrichment/aggregate_rainfall_stats.R` (chunked aggregation of grid-cell daily rainfall to Site Group level, exporting yearly/monthly/quarterly parquet panels), reviewed with `clean_lr_house_price_data.R` as the style and fail-hard benchmark and `aggregate_spill_stats.R` as the stage sibling, verified against the published parquet artifacts and the downstream consumer.
- **Method:** two independent reviews, merged. Review 1 — seven parallel Claude review agents (correctness, adversarial, testing, maintainability, project-standards, performance, learnings) plus a cross-model adversarial pass via Codex; every surviving finding went through an independent per-finding validator (10 confirmed, 1 rejected). Review 2 — a separate Codex multi-agent audit with its own validator batch. Findings carry `[codex]` where the second review independently reached the same conclusion.
- **Verdict:** the script can complete successfully while publishing wrong data, and one such path is already live in the published artifacts: 1,251 site-years across 417 sites carry a fabricated zero for center-cell rainfall (A3). It also inherits, and then conceals, the upstream cleaner's tolerance of partial inputs — see the sibling review `2026-08-17-review-clean_rainfall_data.md`. Not ready for a trusted rebuild until A1–A4 land.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## Cross-cutting themes (shared with the cleaner review)

| Theme | Findings here | Cleaner findings | Decision or mechanical |
|-------|---------------|------------------|------------------------|
| T1. The 2024 horizon | residual (below) | C1 | **Decision first**, owned by the cleaner: rainfall stops 2023-12-31 while `site_group_years` and the spill panel cover 2021–2024. |
| T2. Fail-closed computation | A1 | C2, C3 | Mechanical. A chunk failure must stop the run, exactly as a missing month must upstream. |
| T3. Validate before publication | A2, A3, A4, A8 | C5, C10, C11 | Mechanical. Assert expected site-period keys and calendar coverage before export; make missingness explicit rather than zero-filled. |
| T4. Atomic generation publication | A6 | C4 | Mechanical. Stage the full output set and promote together. |
| T5. Shared bootstrap migration | A5 | C6, C8, C16 | Mechanical. Both scripts auto-install packages at runtime and bypass `script_setup.R`. |

---

## Critical

### A1. `[ ]` A failed chunk is silently dropped and a truncated panel is published as success

- **Where:** `aggregate_rainfall_stats.R:296-299` (error handler in `process_complete_chunk`), `:305-328` (`combine_chunk_results`).
- **Problem:** any chunk exception becomes `NULL`, the combiner filters the `NULL`s and only `stop()`s when *every* chunk fails, and the truncated panel is exported while the run logs "completed successfully" (`:512`). Up to 500 sites vanish per failed chunk. Sites are chunked in their existing order, so the omission can be geographically or company-clustered and systematically bias downstream estimates. Same fail-open pattern as C3, one stage downstream.
- **Evidence:** validator-confirmed; no assertion in this script or its sole consumer reconciles exported site counts against the site universe.
- **Suggested fix:** rethrow chunk errors (or `stop()` when `length(successful_chunks) != length(chunk_results)`), and assert all expected Site Group-period keys before export. Fault-injection test required.
- **Flagged by:** adversarial, testing, correctness, adversarial-codex, `[codex]` (independently in both reviews).

---

## High

### A2. `[ ]` Missing source dates silently shrink rainfall totals

- **Where:** `aggregate_rainfall_stats.R:256`.
- **Problem:** the join is `nomatch = 0`, so any site-day whose grid cells lack rainfall rows is deleted outright, defeating the complete CJ site-day grid built at `:182-186`. The upstream cleaner permits exactly this kind of hole (C2/C3); period sums at `:362-396` then label the surviving remainder as a complete period. Wrong-but-plausible totals with no coverage count anywhere in the output.
- **Evidence:** validator confirmed the inner-join semantics with a live `data.table` test, and that no coverage assertion exists between the join and export (unlike the metadata join at `:191`, which does use `assert_left_row_count`).
- **Suggested fix:** preflight complete date coverage, or preserve the grid through the join (left-join direction) and export observed/expected-day diagnostics. Define one missingness/coverage contract together with A3.
- **Flagged by:** adversarial, testing, adversarial-codex, `[codex]`.

### A3. `[ ]` All-missing rainfall is published as zero — live in the current artifacts

- **Where:** `aggregate_rainfall_stats.R:363` (also `:379`, `:395`).
- **Problem:** `rainfall_r1` is `NA` when a site's center grid cell has no rainfall row (`:261`, `:268`), and `sum(..., na.rm = TRUE)` over an all-NA vector returns 0 — so a site whose center cell is entirely unobserved becomes a permanently "dry" site, indistinguishable from genuine zero rainfall. The source holds 2,581,918 missing values (sea/coastal cells, by design), yet the aggregate files contain no missing values at all. Dry-spill classification consumes this.
- **Evidence (verified twice, independently):** `rainfall_agg_yr.parquet` contains 1,251 site-years across 417 sites with `rainfall_r1_yr == 0` while `rainfall_r9_yr > 0`; those sites show zero center-cell totals across all 36 monthly records. A synthetic all-NA center-cell period reproduced the zero.
- **Suggested fix:** a coverage-aware sum (`if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)`) plus explicit `n_days_r1` / `n_days_r9` coverage fields at every grain; regenerate all six outputs and re-check the consumer. `rainfall_r9`'s `-Inf` guard at `:269` correctly prevents a different collapse route, but shares the same weakness when all nine cells are missing every day.
- **Flagged by:** correctness, adversarial, adversarial-codex, `[codex]`; empirical check reproduced by an independent validator.

### A4. `[ ]` Sites missing or stale in the grid lookup silently vanish from every output

- **Where:** `aggregate_rainfall_stats.R:216`.
- **Problem:** the `(site_id, ngr)` inner join against `spill_site_grid_lookup.parquet` drops any site absent from the lookup, or present under a different NGR than the crosswalk projection's current representative. The lookup is generated by a separate script from its own crosswalk read (`clean_rainfall_data.R:91-118`), so the two artifacts drift whenever one is regenerated without the other; a remapped site loses its entire rainfall history with no error while the run logs success.
- **Suggested fix:** `stop()` on non-empty `setdiff(chunk_sites$site_id, unique(chunk_lookup$site_id))` (modulo any NA-NGR sites, pending the C12 contract decision); optionally assert nine cells per site.
- **Flagged by:** adversarial, correctness, adversarial-codex; validator-confirmed (severity disagreement P1/P2 — kept P1).

### A5. `[ ]` Bootstrap bypasses the rv dependency contract

- **Where:** `aggregate_rainfall_stats.R:33-38` (`install.packages` at `:35`).
- **Problem:** runtime `install.packages()` can mutate the rv-managed library and select versions outside `rv.lock`, against AGENTS.md ("R package management uses `rv`") and the documented fail-fast bootstrap contract. Two aggravators: `CONFIG` calls `here::here()` at source time (`:57`), before `main()` could ever install a missing `here`, so that path is unreachable anyway; and the local `setup_logging()` uses `layout_glue_colors` rather than the shared helper's plain layout. The sibling `aggregate_spill_stats.R` already uses the shared bootstrap.
- **Suggested fix:** same migration as C6 — `requireNamespace("here")` guard first, `source(script_setup.R)`, `REQUIRED_PACKAGES` + `check_required_packages()` with `rv sync` guidance, shared `setup_logging()`.
- **Flagged by:** project-standards, learnings, `[codex]`; validator-confirmed on all three sub-claims.

---

## Moderate

### A6. `[ ]` Direct writes can publish mixed output generations

- **Where:** `aggregate_rainfall_stats.R:437-457` (write loop), `:454`.
- **Problem:** six outputs are overwritten sequentially with no generation-level validation or rollback boundary. An interruption leaves a mixed-vintage set (fresh yearly, stale monthly) that downstream reads as consistent. Same class as C4.
- **Suggested fix:** stage the complete output set and promote together, reusing the existing `publish_validated_dataset()` utility or the staged `tempfile()` + `file.rename()` pattern in `clean_lr_house_price_data.R:334-351`.
- **Flagged by:** reliability, adversarial `[codex]`.

### A7. `[ ]` The 54.3M-row rainfall window is join-indexed roughly 28 times

- **Where:** `aggregate_rainfall_stats.R:223`.
- **Problem:** the full date-filtered rainfall table is held in memory for the whole run and re-indexed by the per-chunk `on = c("x_idx","y_idx")` join once per chunk, repeating a full-table sort's worth of work ~28 times.
- **Suggested fix:** build one reusable key/index after loading (`setkey` on the rainfall table) and benchmark runtime and peak RSS; consider `arrow::open_dataset()` predicate pushdown at load (`:114-118` currently reads the whole file then filters in R) if the source range widens.
- **Flagged by:** performance `[codex]` (confidence 75). **Severity disagreement:** the Codex review rated this P1 ("adds avoidable full-table work on every normal run"); it is kept Moderate here because the cost is runtime and memory on every run rather than wrong output. Promote it if a rebuild turns out to be time-bound.

### A8. `[ ]` Grid lookup loaded without uniqueness or center-cell assertions

- **Where:** `aggregate_rainfall_stats.R:126-134`.
- **Problem:** no assertion of uniqueness on `(site_id, x_idx, y_idx)` or exactly one `is_center` row per site, unlike `load_spill_sites()` (`:105`, `assert_unique_site_groups`) and unlike the upstream generator, which validates this exact table at write time (`clean_rainfall_data.R:335-337`). Duplicates would silently multiply rows through the `allow.cartesian` join at `:249`, and `rainfall[is_center == TRUE][1]` (`:261`) would silently pick the first — corruption, not an error. Current artifacts satisfy all three properties, asserted nowhere.
- **Suggested fix:** assert both invariants at load, mirroring the upstream write-time check.
- **Flagged by:** testing; validator-confirmed. Also a residual in the Codex review.

### A9. `[ ]` Legacy `spill_blocks_rainfall_*` dual exports that nothing in the repo reads — decision

- **Where:** `aggregate_rainfall_stats.R:71-75` and the dual-write loop at `:437-457`.
- **Problem:** every aggregate is written twice. A repo-wide search (verified twice, across `.R`/`.do`/`.py`/`.md`/`.qmd`) finds no reader of the `spill_blocks_rainfall_*` names; git shows the alias was added in `aa039a0` for consumers migrated off in `53b8f31`. The sole live consumer, `dry_spill_rainfall_monthly_per_site_combined.R:184`, reads `rainfall_agg_mo.parquet`.
- **Decision needed:** confirm no out-of-repo consumer (Stata code, collaborators), then delete the compat block and collapse the export loop; otherwise leave a comment naming the consumer and a removal date.
- **Flagged by:** maintainability; validator-confirmed with git evidence. Also a residual in the Codex review ("aliases may have external consumers even though no in-repository reader was found").

### A10. `[ ]` Chunk-sizing comment understates the real expansion by ~9x

- **Where:** `aggregate_rainfall_stats.R:83`.
- **Problem:** the comment claims ~547K records per chunk, but that is the pre-lookup site-day grid; the actual per-chunk peak (`expanded_grid` / `rainfall_joined`) is 500 sites x 1,095 days x 9 cells. The two reviews measured the peak slightly differently — ~4.9M rows arithmetically, ~4.25M observed on the current site distribution — either way an order of magnitude above the comment. Future tuning of `sites_per_chunk` from this comment would misjudge memory.
- **Suggested fix:** correct the comment to state the true peak (and measure peak RSS on the intended 16GB host).

### A11. `[ ]` Dead configuration entry `CONFIG$n_cores`

- **Where:** `aggregate_rainfall_stats.R:84-88`.
- **Problem:** computed via `parallel::detectCores()` and never referenced anywhere (grep-verified); chunk processing is deliberately sequential (`:482`, `:490`). Dead config that could mask a future accidental parallel switch without the intended core cap. Precedent for removal: `docs/solutions/best-practices/individ-edm-yearly-combined-parquet-fix-20260310.md`.
- **Suggested fix:** delete the entry, or wire it into a real parallel chunk loop if that is wanted.

### A12. `[ ]` Full-table `copy()` made only to read a row count

- **Where:** `aggregate_rainfall_stats.R:189`.
- **Problem:** `data.table::copy()` duplicates the ~547K-row chunk grid solely so `assert_left_row_count()` can call `nrow()` on it after the join — the helper reads nothing else from the table (`site_group_utils.R:30-38`).
- **Suggested fix:** capture `nrow(chunk_grid)` into a scalar before the join and compare directly, or add an nrow-based overload of the helper.

### A13. `[ ]` Three copy-paste aggregation functions

- **Where:** `aggregate_rainfall_stats.R:358-402`.
- **Problem:** `aggregate_yearly` / `aggregate_monthly` / `aggregate_quarterly` differ only in by-columns and output suffix, so the sum expressions carry triple maintenance — which matters more once A2/A3 change them.
- **Suggested fix:** one parameterized helper (`by_cols`, `suffix`) called three times.

### A14. `[ ]` Redundant `chunk_datasets` wrapper list

- **Where:** `aggregate_rainfall_stats.R:484-487`.
- **Problem:** each chunk is wrapped as `list(chunk_sites = ...)` citing an "established pattern", then only `chunk_sites` is ever read (`:496`). Iterating `imap(site_chunks, ...)` directly is behavior-identical — validator-verified, including index semantics over the named list from `split()`.
- **Suggested fix:** drop the wrapper and its stale comments.

---

## Low

### A15. `[ ]` Comments claim parallelism that does not exist, plus small cleanups in the same pass

- **Where:** `aggregate_rainfall_stats.R:141` ("Load all required datasets in parallel") and `:413` ("Perform aggregation for each temporal scale in parallel"); both are plain sequential calls, with no parallel machinery anywhere in the file (validator-verified).
- **Suggested fix:** reword both to describe sequential behavior. While in the area: `rainfall_indicators[is.na(rainfall_r1), rainfall_r1 := NA_real_]` at `:268` is a no-op, and the bare `select()` / `map()` / `imap()` / `rbindlist()` calls (`:129`, `:163`, `:319-321`, `:491`) should take `pkg::` prefixes to match the repo's namespacing discipline.

---

## Known patterns from docs/solutions

- [`edm-api-combine-hardening-20260310.md`](../docs/solutions/best-practices/edm-api-combine-hardening-20260310.md) — validate schemas, propagate fatal failures, and reject invalid canonical publications. The established pattern for A1 and A6.
- [`script-setup-runtime-package-cleanup-ingestion-20260310.md`](../docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md) and [`data-cleaning-script-header-bootstrap-standardisation-20260310.md`](../docs/solutions/best-practices/data-cleaning-script-header-bootstrap-standardisation-20260310.md) — use `script_setup.R`, fail with `rv sync` guidance, never install at runtime. A5 is a documented violation, not a style preference.
- [`individ-edm-yearly-combined-parquet-fix-20260310.md`](../docs/solutions/best-practices/individ-edm-yearly-combined-parquet-fix-20260310.md) — test every named period output with a synthetic fixture (shapes the A3 regeneration checks), and precedent for deleting unused parallel scaffolding (A11).
- [`data-enrichment-readme-standardisation-20260310.md`](../docs/solutions/best-practices/data-enrichment-readme-standardisation-20260310.md) — this script is named explicitly; documented filenames must stay aligned with actual exports, so any change to `CONFIG$output_files` or the compat block (A9) updates the README in the same commit.

---

## Residual risks

- **The 2024 horizon (T1, owned by C1 in the cleaner review):** `end_date = 2023-12-31` at `:79` while `site_group_years = 2021:2024` at `:60` and the spill panel covers 2021–2024. Checked from this side: no current consumer of these aggregates joins them to 2024 sales/hedonic data, so the cutoff does not bite here today — the live loss is in the dry-spill path (C1). Whichever way T1 is decided, add an assertion that `max(rainfall_dt$date) >= CONFIG$end_date` after `load_rainfall_data()` so a configured window exceeding actual coverage fails loudly instead of truncating silently.
- `qtr_id` is created numeric (`as.numeric`, `:344`) while `month_id` and sibling panels use integer IDs; joins coerce today, but a strict schema comparison downstream would see a type mismatch.
- `rbindlist(..., fill = TRUE)` (`:319-321`) would silently NA-fill cross-chunk schema drift instead of erroring.
- Doc drift: `docs/ideas/run2/context/data-asset-inventory.md:71` describes `rainfall_r9_*` as "averaged over the nine-cell block"; the code takes the maximum (`:264`), matching the `rainfall_max_9cell_*` convention in `spill_aggregation_utils.R`.
- Input contracts do not assert unique cell-date keys on the rainfall table (the lookup half is A8); current artifacts satisfy the property.

---

## Verified non-issues (checked and cleared)

- **Stage write conventions:** direct `arrow::write_parquet()` without `_candidate` staging matches every stage-03 sibling; the candidate/atomic pattern is specific to `02_data_cleaning`. A6 therefore proposes atomicity on its own merits, not as a convention violation.
- **Consumer contract:** the only in-repo consumer reads `rainfall_agg_mo.parquet` and averages `rainfall_r9_mo` by calendar month, so neither the 2023 cutoff nor the compat aliases currently break a live reader.
- **Crosswalk/lookup properties:** unique cell-date keys, nine lookup cells per site, exactly one center cell — all satisfied by the current artifacts (see A8: asserted nowhere).
- **The 21 spatially-unkeyed Site Groups:** this review's validator rejected the candidate as an intentional, logged design choice currently excluding zero matched events. The cleaner review reached the opposite conclusion — the contract decision is tracked as C12 there, not duplicated here.
- **Contract test:** `test_site_group_consumer_contracts.R` passes; it covers `create_chunk_site_day_grid` row conservation and Site Group uniqueness only.

---

## Testing gaps

1. A 9-cell fixture covering observed, partially missing, all-missing, and omitted-date rainfall (catches A2 and A3).
2. Fault injection: force one chunk to fail and prove no canonical output is written (A1).
3. Reconciliation between the crosswalk Site Group count and distinct `site_id` counts in the exported parquets (catches A1 and A4).
4. A per-period day-count/coverage invariant (365 / 28-31 / 90-92 days) on the aggregated outputs.
5. Lookup input-contract assertions exercised by a duplicate-row fixture (A8).
6. A byte-identity or equality check between primary and compatibility exports while both exist (A9).
7. A benchmark proving the rainfall index is built once (A7), plus peak-RSS measurement against the sizing comment (A10).
8. A round-trip check that a fully-observed site-year's `rainfall_r1_yr` equals the sum of daily center-cell values in `rainfall_data_cleaned.parquet`.

---

## Coverage notes

- Review 1 (Claude multi-agent): seven reviewers plus a Codex cross-model adversarial pass; 11 per-finding validators ran — 10 confirmed, 1 rejected (the 2024 end-date item as a live P1, rerouted to residual risks above), and 3 mechanical anchor-100 findings verified by direct inspection rather than a validator. The cross-model pass contributed 4 findings, all deduplicating onto A1/A2/A3/A4 — the strongest agreement signal in the set. Run artifacts: `/tmp/compound-engineering/ce-code-review/20260817-115338-40c377e1/`.
- Review 2 (Codex multi-agent): fresh validator batch confirmed its findings, which map onto A1, A2, A3, A5, A6, A7 here; synthetic checks reproduced the all-NA-to-zero and partial-chunk behaviors. A7 is the finding only that review surfaced. One candidate rejected (the 21 coordinate-ineligible Site Groups — kept open as C12 in the cleaner review, where the opposite verdict was reached). Run artifacts: `/tmp/compound-engineering-501/ce-code-review/20260817-103327-856326c2/`.
- Suggested execution order: A1 fail-closed, then A2+A3 as one coverage/missingness contract (regenerating all six outputs once at the end), then A4+A8 lookup guards, then A5 bootstrap, then A6, then the cleanup block A10–A15, with A7 measured alongside the regeneration. A9 needs your decision first.
