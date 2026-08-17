# Code review — `clean_rainfall_data.R`

- **Date:** 2026-08-17
- **Scope:** whole-file audit of `scripts/R/02_data_cleaning/clean_rainfall_data.R` (HadUK-Grid NetCDF extraction for grid cells near spill sites, plus the Site Group grid lookup), reviewed against the repo's current conventions (`clean_lr_house_price_data.R`, AGENTS.md, docs/solutions) and verified against the published parquet artifacts.
- **Method:** two independent reviews, merged. Review 1 — seven parallel Claude review agents (correctness, adversarial, project-standards, maintainability, performance, reliability, learnings) plus a cross-model adversarial pass via Codex; ten validators confirmed every P0/P1 and judgment-call P2 finding. Review 2 — a separate Codex full-source audit (correctness, project-standards, testing, maintainability, performance, reliability, adversarial, learnings) with its own validator batch confirming 7/7 findings. Findings carry `[codex]` where the second review independently reached the same conclusion.
- **Verdict:** the numerical core is correct and the currently published artifacts pass every direct integrity check, but the script silently tolerates partial inputs, validates nothing before canonical publication, and writes its two outputs non-atomically. Any future rerun can publish month-sized holes without failing. The downstream aggregator then hides those holes — see the sibling review `2026-08-17-review-aggregate_rainfall_stats.md`.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

---

## Cross-cutting themes (shared with the aggregator review)

| Theme | Findings here | Aggregator findings | Decision or mechanical |
|-------|---------------|---------------------|------------------------|
| T1. The 2024 horizon | C1 | A-residual | **Decision first.** Either acquire 2024 HadUK files and extend the window, or pin every rainfall consumer to 2021:2023 and document the cutoff. Related: `aggregate_dry_spill_stats.R` already flagged for fabricating 2024 zeros (R2 in the 2026-07-07 `aggregate_spill_stats` review). |
| T2. Fail-closed computation | C2, C3, C13, C14 | A1 | Mechanical. Missing inputs and per-unit failures must stop the run at both stages. |
| T3. Validate before publication | C5, C10, C11 | A2, A3, A8 | Mechanical. Assert calendar coverage, key uniqueness and expected row counts before any canonical write; make missingness explicit rather than zero-filled. |
| T4. Atomic generation publication | C4 | A6 | Mechanical. Stage all outputs of a run and promote together, so a crash can never publish a mixed generation. |
| T5. Shared bootstrap migration | C6, C8, C16 | A5 | Mechanical. Both scripts auto-install packages at runtime and bypass `script_setup.R`. |
| T6. Site-coverage contract | C12 | (validator-rejected candidate) | **Decision.** The two review sessions disagree — see C12. |

---

## Critical

### C1. `[ ]` Rainfall coverage ends 2023 while the study window says 2021–2024; 2024 dry spills are ~99.96% silently lost

- **Where:** `clean_rainfall_data.R:57,69-70` (`site_group_years = 2021:2024` vs `end_date = as.Date("2023-12-31")`); no 2024 files in `data/raw/haduk_rainfall_data/`.
- **Problem:** `identify_dry_spills.R` iterates every year present in the spill data (line 491, unpinned) and inner-joins rainfall with `nomatch = 0` (line 219), so 2024 spill-days simply vanish. `aggregate_daily_spill_rainfall.R:73` pins 2021:2023, so the pipeline is internally inconsistent about the cutoff.
- **Evidence (verified against published files):** `dry_spills.parquet` holds 674 rows for 2024 — all dated 1 or 3 January, consistent with lookback leakage from December 2023 — against 346,549 / 245,552 / 127,426 rows for 2021–2023 and 1,850,747 source spill rows in 2024.
- **Suggested fix:** decide the horizon (theme T1). If extending: download `rainfall_2024_01.nc` … `rainfall_2024_12.nc`, set `end_date = 2024-12-31`, re-run the cleaner and all consumers. If pinning: derive consumer year loops from one configured window and stop loudly on uncovered years.
- **Flagged by:** correctness, adversarial, adversarial-codex (independently); confirmed empirically by a validator.

### C2. `[ ]` Missing expected monthly NetCDF files are warn-and-continue

- **Where:** `clean_rainfall_data.R:248-251` (`generate_rainfall_paths`).
- **Problem:** missing files produce `log_warn` and the run proceeds on whatever exists, publishing month-sized holes as the canonical rainfall table. The exemplar (`validate_expected_lr_files` in `clean_lr_house_price_data.R`) stops before reading anything.
- **Evidence:** a targeted run reproduced the behavior — one failed file was dropped and the function logged success for 1/2 files.
- **Suggested fix:** `stop()` listing the missing expected files; if partial coverage is ever intentional, make it an explicit reviewed allowlist.
- **Flagged by:** reliability, correctness, adversarial `[codex]`; validator-confirmed in both reviews.

### C3. `[ ]` Per-file NetCDF failures are swallowed by `tryCatch` and dropped

- **Where:** `clean_rainfall_data.R:271-284` (`process_files_parallel`).
- **Problem:** any per-file error becomes `NULL` and is filtered out; the run stops only if every file fails. One corrupt month becomes a silent hole; the failure notice goes to `message()` inside a multisession worker and never reaches the log file.
- **Suggested fix:** fail the run on any per-file error (collect error objects, log them from the main process, then `stop()`).
- **Flagged by:** reliability, adversarial, correctness `[codex]`; validator-confirmed.

### C4. `[ ]` Both canonical outputs are overwritten in place with no staging

- **Where:** `clean_rainfall_data.R:392-398`.
- **Problem:** `rainfall_data_cleaned.parquet` and `spill_site_grid_lookup.parquet` form one logical dataset but are written sequentially in place. A crash or full disk mid-sequence leaves a corrupt parquet or new rainfall paired with a stale lookup; no consumer checks generation consistency.
- **Suggested fix:** stage both to tempfiles in the output directory and promote via `file.rename` only after both writes succeed (exemplar: `write_cleaning_candidate`, `clean_lr_house_price_data.R:334-351`). Adopting the full candidate-plus-metadata pattern is a product decision worth taking at the same time.
- **Flagged by:** reliability, adversarial `[codex]`; validator-confirmed.

---

## High

### C5. `[ ]` Nothing validates the combined table before publication

- **Where:** `clean_rainfall_data.R:287,395`.
- **Problem:** no calendar-coverage assertion, no uniqueness check on `(x_idx, y_idx, date)`, no expected-row-count check (`n_dates x n_cells`), and no comparison of each file's internal dates against its filename month. This is the single guard that would turn C1–C3 from silent into loud.
- **Suggested fix:** before `write_parquet`, assert unique keys spanning exactly `seq(start_date, end_date, by = "day")` for every required cell, and per-file assert internal dates fall inside the filename month.
- **Flagged by:** adversarial-codex; validator-confirmed.

### C6. `[ ]` The script never adopted the standardised bootstrap contract (including runtime `install.packages()`)

- **Where:** `clean_rainfall_data.R:18-47` (`install.packages` at `:27`).
- **Problem:** no `here` guard, no `source(script_setup.R)`, no `REQUIRED_PACKAGES` / `LOG_FILE`, no `check_required_packages()`, a local `setup_logging()` duplicating the shared helper, and `install.packages(pkg)` at runtime — which can mutate the rv-managed library and select versions outside `rv.lock`, explicitly forbidden by `docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md`. The standardisation doc lists this script as an un-migrated straggler, and the file was last modified after the convention landed.
- **Suggested fix:** migrate to the exemplar's bootstrap exactly (fail-hard `requireNamespace("here")` guard, `REQUIRED_PACKAGES` + `check_required_packages()` with `rv sync` guidance, shared `setup_logging()`); delete the local `setup_logging()`.
- **Flagged by:** project-standards, maintainability, adversarial, learnings `[codex]`; validator-confirmed in both reviews.

### C7. `[ ]` Grid-index logic duplicated across two functions with an unasserted equality invariant

- **Where:** `clean_rainfall_data.R:130-155` (`get_required_grid_indices`) vs `:300-345` (`create_spill_site_lookup`); independent `radius = 1` defaults at both signatures.
- **Problem:** the extraction cell set and the lookup cell set stay equal only because the same break-vector/`findInterval`/offset code is pasted twice. Nothing asserts the invariant anywhere in the repo; drift would be silently absorbed by downstream `nomatch = 0` joins.
- **Suggested fix:** derive `required_indices` from the lookup table (`unique(lookup[, .(x_idx, y_idx)])`) so the invariant holds by construction, folding both functions into one shared expansion helper (also resolves C15 and C17).
- **Flagged by:** maintainability, adversarial; validator-confirmed.

---

## Moderate

### C8. `[ ]` Header lacks the standardised Purpose/Inputs/Outputs/Source block

- **Where:** `clean_rainfall_data.R:1-11`. Rewrite in the exemplar's format per the header-standardisation doc; fold into the C6 migration.

### C9. `[ ]` Full national rainfall array loaded per worker

- **Where:** `clean_rainfall_data.R:197`.
- **Problem:** each worker loads the full 900 x 1450 x days array (~300MB, verified via ncdump; ~324MB for a 31-day month before R overhead) to extract a few thousand cells; six workers peak near 1.9GB even though only 49,594 cells are retained. Works today; wasteful and unguarded against growth.
- **Suggested fix:** bounding-box `ncvar_get(start=, count=)` read with a byte-for-byte parity check against current output. Best done alongside the T1 re-run; benchmark RSS and runtime.
- **Flagged by:** performance `[codex]`.

### C10. `[ ]` Grid geometry read from the first file only; later files extracted positionally with no check

- **Where:** `clean_rainfall_data.R:371-375` and `:213-216`.
- **Problem:** a same-shaped but shifted-grid file would silently yield plausible rainfall for the wrong locations. All 37 current files share identical grids (verified by fingerprint), but mixed-resolution HadUK files already sit in the raw directory.
- **Suggested fix:** per file, assert dims and coordinate bounds match the reference before extraction; test with a readable mismatched grid.
- **Flagged by:** adversarial `[codex]`.

### C11. `[ ]` Unknown time-unit fallback guesses "days"

- **Where:** `clean_rainfall_data.R:190-193`.
- **Problem:** a legitimate "minutes since" file would warn (inside a worker, so invisibly) and publish dates years off. Unreachable for current HadUK files (verified hours-since), so hardening.
- **Suggested fix:** `stop()` on unsupported units with the file named; optionally assert computed dates fall inside the filename month (same assertion as C5). Verify with tiny NetCDF fixtures.
- **Flagged by:** reliability `[codex]`.

### C12. `[ ]` Site Groups without spatial keys silently vanish from downstream panels — contract undecided (reviews disagree)

- **Where:** `clean_rainfall_data.R:96-115` (`load_spill_sites`).
- **Problem:** 21 of 13,990 Site Groups lacking NGR/easting/northing are dropped (with CSV + warning at the point of loss) and are then silently absent from every downstream rainfall panel built with `nomatch = 0` joins — no NA rows, no missingness flag.
- **Disagreement to resolve:** this review's validator confirmed it as a legitimate contract-clarification item (the drop is documented at the point of loss but not in the artifacts consumers read); the aggregator review's validator rejected the equivalent candidate as an intentional, logged design choice that currently excludes zero matched events. Decide the contract once: fail publication on spatially-unkeyed groups, preserve them with explicit NA rainfall and a missingness flag, or explicitly document the exclusion as intended.

### C13. `[ ]` Empty rainfall-file list dies as cryptic `nc_open(NA)`

- **Where:** `clean_rainfall_data.R:371`. Add `if (length(rainfall_files) == 0) stop(...)` after `generate_rainfall_paths`.

### C14. `[ ]` Worker progress and failure detail never reach the log file

- **Where:** `clean_rainfall_data.R:164,274`. Return error objects from workers and log them from the main process (folds into C3).

### C15. `[ ]` Lookup table built with per-row `lapply` over ~14,000 sites

- **Where:** `clean_rainfall_data.R:316-334`. Replace with a vectorised repeat-and-join inside the shared helper from C7.

### C18. `[ ]` The parent process retains every monthly table plus a second combined copy

- **Where:** `clean_rainfall_data.R:270-287`.
- **Problem:** `future_map()` returns and retains all monthly tables in the parent, then `rbindlist()` materialises the full combined table alongside them — 55,842,844 rows, 399MB compressed on disk, held twice at peak. Distinct from C9, which is per-worker.
- **Suggested fix:** write validated monthly partitions or stream to Parquet. Embedded decision: whether consumers keep one Parquet file or move to an Arrow dataset (the aggregator currently reads the single file whole — see A7).
- **Flagged by:** performance `[codex]`.

---

## Low

### C16. `[ ]` `purrr` and `rnrfa` declared but never used

- **Where:** `clean_rainfall_data.R:21-22` (verified by grep, including the sourced `site_group_utils.R`). Remove during the C6 migration.

### C17. `[ ]` Log message hardcodes division by 9

- **Where:** `clean_rainfall_data.R:342` (plus stale "all 9 grid cell combinations" comment at `:319`). Use `nrow(offsets)`; wrong for any radius other than 1.

---

## Known patterns from docs/solutions

- [`data-cleaning-script-header-bootstrap-standardisation-20260310.md`](../docs/solutions/best-practices/data-cleaning-script-header-bootstrap-standardisation-20260310.md) — the standardised header and bootstrap contract. This script is named in that doc as an un-migrated straggler; it governs C6, C8 and C16.
- [`script-setup-runtime-package-cleanup-ingestion-20260310.md`](../docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md) — environment setup belongs to `rv sync`, never runtime installation. Makes C6 a documented violation rather than a style preference.
- [`edm-api-combine-hardening-20260310.md`](../docs/solutions/best-practices/edm-api-combine-hardening-20260310.md) — validate candidates and propagate fatal errors before canonical publication. The established pattern for C2, C3, C4 and C5.

---

## Residual risks

- Out-of-grid coordinates could generate invalid positional indices; the current lookup is safely interior (x 288-857, y 208-856 on the 900 x 1450 grid), so the scenario is unreachable with real GB data and would error loudly rather than corrupt silently.
- Outputs persist no source checksums, grid fingerprint, or run manifest, so a consumer cannot tell which generation it is reading (folds into C4/C5 if a manifest is adopted).
- Machine-specific comments ("optimized for M1 MacBook Pro", lines 72 and 265) will mislead on other hosts; clean up alongside C9.
- 2,581,918 missing rainfall values in the source are sea/coastal cells by design — legitimate missingness, but it is exactly what the aggregator converts to zero (see A3).

---

## Verified non-issues (checked and cleared)

- **Numerical core:** array dimension order `(x, y, time)`, hours-since-1800 time parsing to UTC dates, and the `rep(each/times)` flattening alignment all verified correct (ncdump plus trace); the trailing-comma 3D slice `rainfall_full[x, y, ]` is a valid full-time-series extraction. A two-cell extraction smoke test matched direct `ncvar_get(start, count)` reads and produced the correct 31 dates.
- **Current inputs are complete:** all 37 expected monthly files present, dates match filenames, time units supported, all grid fingerprints identical.
- **Current outputs are internally consistent:** 55,842,844 rows = 49,594 grid cells x 1,126 dates (2020-12-01 to 2023-12-31), every date carrying the full cell count; the lookup holds 13,969 sites with nine cells and exactly one center each, no duplicate keys.
- **Edge-cell indexing:** the HadUK grid extent (-200..700km easting, -200..1250km northing) keeps all GB coordinates interior, so `findInterval` zero-index and out-of-bounds neighbour scenarios are unreachable with real data.
- **ANSI colour codes in log files:** crayon disables colour in non-interactive Rscript runs, so `layout_glue_colors` writes plain text under normal execution (still dies with the local logging setup in the C6 migration).
- **Extraction/lookup cell-set equality:** currently holds (same code, same default radius) — but see C7 for why it is fragile.

---

## Testing gaps

1. A calendar-coverage plus key-uniqueness assertion on the output — the one check that catches C1, C2, C3 and C5.
2. Fault injection: corrupt or remove one monthly NetCDF and prove no canonical output changes.
3. A synthetic-NetCDF unit test of `process_rainfall_file` vector alignment, and fixture tests for the four time-unit branches (dimension order, fill values, supported and unsupported units, malformed dates).
4. Failure injection between the two paired writes (C4), verifying neither artifact is left mismatched.
5. Any exercise of `radius != 1` (would have caught C17).
6. A memory regression check covering peak RSS and streaming-output equivalence (C9, C18).
7. A cross-script contract test that spill-event years are a subset of rainfall date coverage before dry-spill classification (C1).
8. Existing `test_site_group_consumer_contracts.R` passes but covers interior lookup cardinality and Site Group uniqueness only — none of the producer failure paths above.

---

## Coverage notes

- Review 1 (Claude multi-agent): 10 of 10 validators confirmed; mechanical anchor-100 items verified by direct quote-and-doc checks. Severity calibrations by synthesis: C9 from P1, C11 and C12 from P0, C13 from P1, C17 from P1. One fast-pass item withdrawn (edge-cell indexing) after grid-extent verification. Run artifacts: `/tmp/compound-engineering/ce-code-review/20260817-115316-38d26b4d/`.
- Review 2 (Codex full-source audit): 7 findings, one independent validator batch confirming all 7; 20 raw reviewer candidates reconciled to 7 semantic findings with no malformed returns. Its findings map onto C2/C3 (fail-open inputs), C11, C9, C6, C18, C4, C10 — every one an independent rediscovery except C18, which only that review surfaced. Run artifacts: `/tmp/compound-engineering-501/ce-code-review/20260817-103223-f7ab7373/`.
- Suggested execution order: T1 decision, then T2+T3+T4 as one hardening branch across both scripts (regenerating outputs once at the end), then T5 bootstrap migration, then C7/C15/C17 consolidation, with C9/C18 optional alongside the T1 re-run.
- Stale doc surfaced in passing: `docs/solutions/best-practices/lr-house-price-local-ons-postcode-lookup-20260310.md` still warns against candidate-only outputs, superseded by commit `30f10bd`; separate task raised.
