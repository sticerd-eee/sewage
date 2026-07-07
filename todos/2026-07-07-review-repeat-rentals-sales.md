# Code review — `repeat_rentals.R` and `repeat_sales.R`

- **Date:** 2026-07-07
- **Scope:** whole-file sanity check of `scripts/R/06_analysis_datasets/repeat_rentals.R` and `scripts/R/06_analysis_datasets/repeat_sales.R` (near-identical twins), verified against the actual parquet inputs (`zoopla_rentals.parquet`, `house_price.parquet`, `spill_rental_lookup.parquet`, `spill_house_lookup.parquet`), the shipped mapping outputs under `data/processed/repeated_transactions/`, and the downstream consumer `scripts/R/09_analysis/03_repeat_sales/repeat_sales.R`.
- **Method:** eight parallel review agents (correctness, adversarial, performance, testing, maintainability, project standards, agent-native, learnings research), followed by seven independent validator agents that re-verified every load-bearing finding by direct execution against the real data. One finding was rejected in validation and is recorded at the bottom.
- **Verdict:** the core repeat-identification logic (ordering, grouping, sequencing) is sound, but the pipeline around it has one active data-integrity failure and one corrupted diagnostic. The shipped rentals mapping is **provably misaligned with the current input data** (finding 1) — any repeat-rental result produced from the current on-disk files is joining repeat groups to the wrong transactions. The distance summary silently misclassifies every property that has no spill site within the lookup radius (finding 2) and, on the next rerun, will pool 31,836 unmatched rentals into one fake property (finding 3). The planned rebuild is well justified; findings 4–10 should shape it rather than be patched piecemeal.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## High

### 1. `[ ]` Shipped rentals mapping is stale and provably misaligned with the regenerated input — active data corruption

- **Where:** `repeat_rentals.R:180-211` (`export_repeat_ids`) writes `(rental_id, repeat_id)` keyed on a positional row number; consumer `scripts/R/09_analysis/03_repeat_sales/repeat_sales.R:99,123` inner-joins by that id. Root cause: `rental_id`/`house_id` are `row_number()` ids (`clean_zoopla_data.R:262`, `clean_lr_house_price_data.R:219`) with no provenance guard anywhere.
- **Problem:** the mapping parquets on disk predate the March 2026 regeneration of both inputs (`repeated_rentals.parquet` is from October 2025, `repeated_sales.parquet` from December 2025; both inputs from March 2026). Because the ids are positional, any change in upstream row count or order silently re-labels every transaction.
- **Evidence (verified by execution):** `repeated_rentals.parquet` has a maximum `rental_id` of 1,451,521 while the current `zoopla_rentals.parquet` has only 1,450,255 rows; 1,266 mapped ids do not exist in the current input at all. The upstream row set demonstrably changed after the mapping was built. The sales side reconciles exactly (maximum `house_id` equals the current row count), so it is latent there, not proven broken.
- **Suggested fix:** regenerate both mappings immediately after any upstream rebuild (and now). In the rebuild, export a content-stable key alongside `repeat_id` (sales already has the Land Registry `transaction_id` string; rentals can carry the address key or a hash of it), or write the input row count into the parquet metadata and make consumers assert it.
- **Flagged by:** adversarial; proven active by validator execution.

### 2. `[ ]` Properties with no spill site within the lookup radius silently vanish from every distance band in the summary

- **Where:** `repeat_rentals.R:220-232` (`load_spill_lookup`) and `repeat_sales.R:215-227`; consumed by `generate_summary` in both twins.
- **Problem:** the spill lookups contain one row for **every** property; those with no site within the radius carry `NA` distance (not absent rows). Arrow's `summarise(min(distance_m, na.rm = TRUE))` returns `NA` for an all-`NA` group — silently, with no warning, unlike base R which returns `Inf` with a warning. All four distance flags then evaluate to `NA`, and `any(flag, na.rm = TRUE)` in the summary turns them into `FALSE` in every band. Those properties are counted in `n_properties` but belong to no band, so every `share_*` column is silently deflated.
- **Evidence (verified by execution):** 62,369 of 1,450,255 rentals and 148,426 of 3,166,671 houses have all-`NA` distance. The `NA`-return behaviour of arrow's `min` was reproduced on a synthetic table.
- **Suggested fix:** after `collect()`, map `NA` minimum distance to an explicit `beyond_radius` flag (or `Inf`) so the truncation is visible, and make the four bands plus `beyond_radius` a complete partition. Decide in the rebuild whether this diagnostic survives at all (see finding 8).
- **Flagged by:** adversarial (empirically), correctness and maintainability (same phenomenon, different mechanism guess); mechanism pinned down and confirmed by validator execution.

### 3. `[ ]` Right join in `generate_summary` pools every mapping-absent id into one fake property — active for rentals on next rerun

- **Where:** `repeat_rentals.R:255-267` and `repeat_sales.R:250-262`: `rentals_dt[spill_lookup, on = "rental_id", nomatch = NA]` keeps every lookup row; lookup ids missing from the mapping survive with `repeat_id = NA` and the `by = repeat_id` aggregation pools them into a single pseudo-property.
- **Problem:** transactions whose address key is `NA` are excluded from the exported mapping, but they are present in the spill lookup. Every such id lands in one `NA` group whose transaction count is their total number, polluting the top of the repeat-count distribution.
- **Evidence (verified by execution):** with the current on-disk files, 1,266 rental ids (31,836 lookup rows) are absent from the rentals mapping; rerunning `generate_summary` today produces a fake property with a rental count of 31,836, versus 6 for the largest real property. Sales are dormant today (zero absent ids) purely by coincidence — nothing enforces that.
- **Suggested fix:** drive the join from the mapping side (or use `nomatch = NULL`) and log the excluded count; add `stopifnot(!anyNA(summary_dt$repeat_id))` after the join in both twins.
- **Flagged by:** correctness, testing, adversarial (independently); activation counts established by validator execution.

### 4. `[ ]` The two scripts are one script written twice — parameterize in the rebuild

- **Where:** both files, whole-script.
- **Problem:** the twins are structurally identical (same eight functions, same control flow); only about 18% of combined lines differ, all renames, column names, and paths. Every defect in this review exists twice and every fix must be applied twice; this directory already demonstrates the fork-and-diverge failure mode (see the sibling review of 2026-07-07, finding 3, a one-twin-only copy-paste slip).
- **Evidence:** `diff` of the two files; validator quantified 123 differing lines of ~685 combined, none behavioural.
- **Suggested fix:** one shared pipeline function parameterized by a small config (id column, date column, price column, address columns, paths, log name), following the repo's own documented convention (`docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md`) and the existing sourced-utils precedent (`scripts/R/utils/spill_aggregation_utils.R`, sourced by four sibling scripts). Each twin becomes a config plus a call.
- **Flagged by:** maintainability; convention and precedent confirmed by validator.

## Moderate

### 5. `[ ]` `max()` on an empty repeat set produces `-Inf` repeat ids for every single-transaction property (latent edge case)

- **Where:** `repeat_rentals.R:192` and `repeat_sales.R:187` (`max_repeat_id <- max(repeated_output$repeat_id)`).
- **Problem:** if no property ever repeats (empty input, filtered subsample, schema drift upstream), `max()` of an empty vector returns `-Inf` with only a console warning that the file-based logger does not capture. Every single then gets `repeat_id = -Inf + seq_len(.N)`, the export succeeds, and the downstream `filter(n() > 1)` keeps the entire singles set as one fake property.
- **Evidence:** `max(integer(0))` returning `-Inf` with a warning verified by execution; propagation traced mechanically.
- **Suggested fix:** `max_repeat_id <- if (nrow(repeated_output) > 0L) max(repeated_output$repeat_id) else 0L`, plus `stopifnot(all(is.finite(all_output$repeat_id)))` before the write.
- **Flagged by:** correctness, testing, adversarial (independently); verified by the orchestrator.

### 6. `[ ]` Full-width loads and full-width intermediate copies waste roughly a gigabyte of peak memory per run

- **Where:** `repeat_rentals.R:90` and `repeat_sales.R:88` (`rio::import` reads all 32 and 31 columns when only 7–8 are used); `repeat_rentals.R:186-196` and `repeat_sales.R:181-191` (`repeat_dt`/`single_dt` are full-width copies made only to derive two columns).
- **Evidence (verified by execution):** the sales table is ~1.12 GB in memory; the two intermediate copies transiently add ~1.27 GB. Column pruning was verified against the actual parquet schemas. The one-step alternative (`dt[filter, .(id, repeat_id = .GRP), by = simple_key]`) was verified to produce the identical id-to-group partition.
- **Suggested fix:** `arrow::read_parquet(path, col_select = ...)` plus `setDT()` at load; compute the projection inside a single data.table call in `export_repeat_ids`. Both belong in the rebuild together.
- **Flagged by:** performance; equivalence and magnitudes confirmed by validator execution.

### 7. `[ ]` Dead code and dead columns: the all-`NA` key regex can never fire, and six computed columns are never read

- **Where:** regex cleanup at `repeat_rentals.R:130` and `repeat_sales.R:125`; computed columns (`*_sequence`, `lag_date`, `lag_price`, `holding_period_days`, `price_change`, `pct_change`) at `repeat_rentals.R:146-168` and `repeat_sales.R:141-163`.
- **Problem:** the `fifelse` guard already forces the first key component to be real text, so `^(NA\|)*NA$` can only match if a cleaned field is the literal string `"NA"` — which occurs zero times in either full dataset (verified). The six per-transaction columns are exported nowhere and read by nothing (the downstream consumer selects only the id and `repeat_id`); the Palmquist script recomputes pairs itself.
- **Suggested fix:** delete the regex lines. In the rebuild, either compute only what is exported, or deliberately export the pair metrics if a consumer is planned — decide in the grilling session.
- **Flagged by:** maintainability (both at maximum confidence); data check by validator; consumer check by the orchestrator.

### 8. `[ ]` The distance summary measures different things in the two twins and nothing consumes it

- **Where:** `generate_summary`/`export_summary` in both twins; flags at `repeat_rentals.R:227-232` and `repeat_sales.R:222-227`.
- **Problem:** the rentals lookup extends to 5 km and the sales lookup to 10 km (verified maxima 4,999.7 m and 9,998.1 m), despite both builder scripts being named "10km". So `outside_1000m` means "between 1 km and 5 km" for rentals but "between 1 km and 10 km" for sales, and the two summary parquets are not comparable. Separately, no script reads either summary output — they are human diagnostics only.
- **Suggested fix:** decide in the rebuild whether the summary stage survives. If it does: drive it from the mapping, name the bands honestly, assert the expected lookup radius at load so a regenerated lookup fails loudly. Note the radius mismatch is already flagged as a pending P0 in `todos/012-pending-p0-review-10km-site-match-scripts.md`.
- **Flagged by:** adversarial (empirically), maintainability; radii confirmed by validator.

### 9. `[ ]` Address keys embed the literal string "NA", so inconsistent secondary-address fields split true repeat pairs

- **Where:** `repeat_rentals.R:117-127` and `repeat_sales.R:115-121` (`paste(..., sep = "|")` over fields that may be `NA`).
- **Problem:** `paste()` renders `NA` as the text `"NA"`. A house sold once with `saon` missing and once with `saon` filled gets two different keys and is missed as a repeat pair. This is systematic under-matching (Land Registry `saon` is missing for most non-flat records), and it interacts with a documented history of postcode-completeness regressions upstream.
- **Evidence (verified by execution):** in a ~33,000-row Birmingham sample, 16 of 2,188 groups sharing postcode, primary address, and street had inconsistent `saon` missingness — the mechanism is real, magnitude modest in that sample.
- **Suggested fix:** a methodological decision for the grilling session, not a mechanical patch: choose the key fields deliberately (for example postcode + paon + saon with explicit missing-handling), quantify both under-matching (splits) and over-merging (coarse keys) before committing, and consider cross-checking merged keys against coordinates.
- **Flagged by:** correctness, adversarial; quantified by validator.

### 10. `[ ]` No output-contract checks despite the repo having a contract-test convention

- **Where:** both scripts, `export_repeat_ids` and `main`.
- **Problem:** the exported mapping is a join key for the paper's repeat-transaction regressions, yet nothing asserts id uniqueness, row-count reconciliation against keyed input rows, finiteness of `repeat_id`, or a minimum address-key match rate. The repo already has the convention (`scripts/R/testing/test_merge_outputs_contracts.R`, `test_aggregate_spill_stats_crosswalk_contracts.R`); these two scripts predate it. A small fixture test would have caught findings 3 and 5.
- **Suggested fix:** in the rebuild, add a contract test in `scripts/R/testing/` (uniqueness, reconciliation, finiteness, band partition) and inline `stopifnot` guards at export.
- **Flagged by:** testing; convention existence confirmed against the repo.

---

## Rejected in validation

- **"Replace local helpers with canonical `scripts/R/utils/` versions" (maintainability, filed P1).** Rejected as stated: `postcode_processing_utils.R::normalise_postcode` exists but is **not** behaviorally identical — it maps literal `"NA"` strings to missing, the local copies do not — so a drop-in swap could silently change match rates; `clean_basic` has no canonical counterpart; and the claimed "widespread" duplication is two scripts. The surviving kernel: in the rebuild, converge on one postcode normaliser deliberately, with the `"NA"`-handling difference decided consciously.

## Residual risks and observations

- Transactions with an `NA` address key (9,196 sales; unquantified for rentals) receive no `repeat_id` and are silently absent from the mapping; the downstream inner join drops them with no logged count. Log per-run attrition.
- The two mappings use overlapping `repeat_id` ranges (both start at 1); stacking them would silently merge unrelated properties.
- `house_id` names a transaction row, not a property — the name invites a grain misread in future code.
- R warnings (for example from `max()` on an empty vector) are not routed to the file logger, so silent-warning failures leave no trace in `output/log`.
- `CONFIG` paths are hardcoded with no override, and the colour log layout writes ANSI codes to file; a machine-readable run manifest (row counts, match rate, timestamps) alongside the parquet outputs would make runs auditable (agent-native review).
- Both scripts still use the runtime `install.packages()` bootstrap that `docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md` is migrating the repo away from; staged debt, worth folding into the rebuild.
