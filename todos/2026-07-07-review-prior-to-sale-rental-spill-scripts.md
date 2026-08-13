# Code review — `house_spill_prior_to_sale.R` and `rental_spill_prior_to_rental.R`

- **Original review:** 2026-07-07
- **Updated review:** 2026-08-14
- **Scope:** whole-file audit of `scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R` and `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R`, including `site_group_utils.R`, `spill_aggregation_utils.R`, relevant tests and consumers, the current Parquet inputs, and the outputs regenerated on 2026-08-11.
- **Method:** independent correctness, adversarial, testing, maintainability, performance/reliability, and project-standards passes. The load-bearing findings were then checked directly with R 4.6.0, Arrow 24.0.0, DuckDB queries over the production Parquet files, targeted type-dispatch probes, and an independent validation pass.
- **Verdict:** **ready as a guarded production step.** The focused contracts, shared streaming engine, and staged publisher now enforce the agreed eligibility, evidence, schema, key, and publication contracts. All four production outputs were regenerated and reconciled against their accepted eligible baselines on 2026-08-14.

**Status key:** `[ ]` open · `[x]` fixed · `[-]` won't fix (note why)

## Current status summary

| # | Finding | Current status | Priority |
|---:|---|---|---|
| 1 | Date/POSIXct comparison depends on lubridate | Resolved 2026-08-13 | P1 |
| 2 | Zero-day windows produce `NaN` | Resolved 2026-08-14 | P2 decision |
| 3 | Broken empty-chunk schema | Resolved 2026-08-14 | P2 |
| 4 | Global rather than transaction-specific missingness | Resolved 2026-08-13 | P1 decision |
| 5 | Future year can void the sample | Resolved 2026-08-13 | P1 |
| 6 | Stale Arrow partitions | Resolved 2026-08-13 | P1 |
| 7 | Missing output-contract test | Resolved 2026-08-13 | P1 |
| 8 | Hand-maintained twin scripts | Resolved 2026-08-14 | P2 |
| 9 | Debug/dead-code debris | Resolved 2026-08-14 | P3 advisory |
| 10 | Per-pair interpreted loop | Won't fix 2026-08-14 | P3 advisory |
| 11 | Reported-positive years with no matched events are undercounted | Resolved 2026-08-13 | P1 decision |
| 12 | Empty eligible transaction input fails opaquely | Resolved 2026-08-14 | P2 |
| 13 | Chunking does not bound peak memory | Resolved 2026-08-14 | P2 scaling |

## High

### 1. `[x]` Rental time comparisons rely on attached lubridate methods

- **Resolution (2026-08-13):** both rental builders now normalize `rented_est` to UTC `POSIXct` immediately after collection. The focused producer contract passed in a clean isolated environment, and both full rental producers regenerated successfully with plain `Rscript`.

- **Where:** `rental_spill_prior_to_rental.R:20-28`, `:165`, and `:173`.
- **Problem:** `rented_est` is Arrow `date32[day]` and collects as R `Date`, while `start_time` and `end_time` are `POSIXct`. Base R reports incompatible methods, makes `start_time < rented_est` false for valid overlaps, and gives the wrong `pmin()` clamp. The script works only because `initialise_environment()` attaches lubridate, whose methods repair both operations as a namespace side effect.
- **Evidence:** a clean base-R probe reproduced the false comparison and wrong clamp; attaching lubridate changed both to the intended UTC-midnight result. The production rental schema remains `Date` versus `POSIXct`.
- **Impact:** removing or bypassing one package attachment silently changes all rental spill exposure to zero. Calling the processing functions independently is also unsafe.
- **Suggested fix:** convert `rented_est` explicitly after collection with `as.POSIXct(rented_est, tz = "UTC")`, assert compatible UTC timestamp types, and add a clean-namespace regression test. Prefer namespaced package calls and fail-fast dependency checks over runtime installation/attachment.
- **Validation:** correctness, adversarial, and the independent validator confirmed the mechanism.

### 4. `[x]` Global missingness invalidates transaction windows that ended before a later monitoring gap

- **Resolution (2026-08-13):** all four builders now join Site Group prefix completeness at each transaction's exclusive cutoff year. Full-output reconciliation unmasked 70,709 sale and 33,064 rental site-radius rows (46,094 sale and 16,937 rental radius rows); every site-row change had complete coverage through its cutoff and an `absent` year only afterward, with zero newly masked rows and zero independent cutoff-flag mismatches.

- **Where:** both `load_data()` functions at lines `81-92`, calling `derive_site_group_missing_flags()` in `site_group_utils.R:51-116` with `seq(min_transaction_year, max_transaction_year)`.
- **Problem:** one Site Group flag is computed over the full sample range and copied to every transaction-site pair. A 2021 transaction is therefore set to missing when its site is absent only in 2022 or 2023, even though those years are outside that transaction's exposure window.
- **Evidence in the August outputs:** across all radii, 526,878 sale rows and 408,036 rental rows are globally flagged. Of those, 70,708 sale rows and 32,911 rental rows have complete coverage through their own transaction year. At 1,000 m alone, the corresponding false-global counts are 52,304 of 406,188 flagged sale rows and 25,096 of 316,189 flagged rental rows.
- **Impact:** valid early observations are dropped non-randomly according to monitoring gaps that occur after the transaction.
- **Decision required:** confirm whether the intended estimand deliberately uses a balanced full-sample monitoring panel. If not, derive prefix completeness by `(site_id, transaction cutoff year)` from `CONFIG$base_year`. For a transaction exactly at January 1 midnight, the current exposure interval ends in the preceding year.
- **Validation:** correctness, adversarial, and the independent validator confirmed the execution path; the orchestrator quantified the current impact.

### 5. `[x]` An unsupported future transaction year converts every exposure to missing

- **Resolution (2026-08-13):** the shared prefix helper now validates requested years against crosswalk coverage and stops with the unsupported year list before Site Group-year expansion or publication. The focused sale/rental unsupported-year fixtures passed, and all four supported-year production runs completed successfully.

- **Where:** both `load_data()` functions at lines `81-92`; `site_group_utils.R:101-114` expands every Site Group over all requested years and replaces missing group-years with `"absent"`.
- **Problem:** one legitimate future transaction or one mis-dated row widens `sample_years`. If that year is beyond the crosswalk horizon, it is manufactured as `absent` for every Site Group, so every output metric becomes `NA` while the run can still complete successfully.
- **Evidence:** the crosswalk covers 2021-2024. `derive_site_group_missing_flags(crosswalk, 2021:2025)` marks all 13,990 Site Groups missing; 2021-2023 marks 961 (6.9%) and 2021-2024 marks 1,098 (7.8%).
- **Suggested fix:** validate transaction dates and required years before deriving flags. Stop with the unsupported year list when a transaction window exceeds crosswalk coverage, and reject implausible future dates. Add a publication gate that refuses an all-missing result.
- **Validation:** correctness, adversarial, and the independent validator reproduced the all-site failure.

### 6. `[x]` Direct Arrow writes retain stale partitions and can expose mixed run generations

- **Resolution (2026-08-13):** both site-level producers now publish through a shared staged-generation helper. It rejects empty candidates, reopens and validates the literal schema, row count, and configured radii, then promotes through a checked recoverable `.prev` generation. Focused tests cover obsolete-radius removal, promotion failure with successful restoration, promotion plus restoration failure, interrupted state detection, and empty-candidate rejection. Both full outputs published successfully with only `radius=250`, `500`, and `1000`; their `.prev` datasets reopened as the exact preceding generations.

- **Where:** `house_spill_prior_to_sale.R:370-394` and `rental_spill_prior_to_rental.R:369-392`.
- **Problem:** both scripts write directly into the live radius-partitioned dataset. Arrow's default `existing_data_behavior = "overwrite"` replaces colliding files but does not remove a radius absent from the new run. `"delete_matching"` deletes only partitions receiving new data, so it also leaves a radius removed from `CONFIG$radius_thresholds`. A failed write can additionally leave readers with a partial mix of old and new partitions.
- **Evidence:** a two-run Arrow probe wrote radii 250/500 and then only 250; both the default and `delete_matching` left radius 500 readable. The current production outputs are clean today: each contains only `radius=250`, `500`, and `1000`, with one Parquet file per partition.
- **Suggested fix:** write to a unique sibling staging directory, reopen and validate the complete dataset, then promote it over the live directory through a controlled replacement with a recoverable backup or version manifest. Validate the exact radius set after publication. Do not treat `delete_matching` alone as the fix.
- **Validation:** correctness, adversarial, performance/reliability, and the independent validator confirmed the failure mode.

### 7. `[x]` No producer or output-contract test covers either script

- **Resolution (2026-08-13):** `test_prior_exposure_contracts.R` now executes isolated sale and rental producer fixtures and the shared publisher contract. Together with the prerequisite prior-window contract work, the suite covers transaction-specific completeness, unsupported years, rental timestamp clipping, sale/rental final-output evidence parity, public schemas, stale-partition removal, and publication recovery. The full regenerated outputs were also reconciled to their snapshots for schemas, row counts, keys, radii, stable fields, and evidence-attributed masking. Findings 2, 3, and 12 remain separately open; this closure does not claim zero-day, mixed empty-chunk, or empty-cohort coverage.

- **Where:** project-level gap under `scripts/R/testing/`.
- **Problem:** the existing Site Group consumer tests validate the crosswalk helper but do not execute these producers or audit their outputs. The defects in findings 1-6 and 11-12 can therefore ship with a green script exit.
- **Suggested fix:** add `scripts/R/testing/test_prior_exposure_contracts.R` with isolated fixtures for both scripts. Assert:
  - exact schema and integer identifier types;
  - unique `(transaction_id, site_id, radius)` grain and exact radius set;
  - correct event clipping at the window start and transaction timestamp;
  - the chosen zero-day policy and finite-or-`NA` rates, never `NaN`/`Inf`;
  - transaction-specific missingness, unsupported-year failure, and event-incomplete positive years;
  - populated-plus-empty chunk binding and empty-cohort behavior;
  - sale/rental parity on isomorphic fixtures; and
  - two-run export behavior with an obsolete radius and an injected write failure.
- **Current structural baseline:** the August outputs have zero duplicate `(id, site_id, radius)` keys. At radius 1,000, all 4,283,449 house lookup pairs and all 2,299,421 rental lookup pairs appear exactly once, with no stale output-only pairs.
- **Validation:** testing and the independent validator confirmed that no focused contract exists.

### 11. `[x]` Reported-positive Site Group-years with no matched events are treated as observed zero or partial exposure

- **Resolution (2026-08-13):** the shared prefix helper now marks event evidence unknown for `reported_positive` years with zero matched events and for `reported_na`/`absent` years, while preserving `reported_zero` as observed zero. Both site-level producers carry that flag internally and mask counts, hours, and derived rates only after final zero filling; the public 13-column schemas are unchanged. Full-output reconciliation found 968,674 newly masked sale rows and 392,654 newly masked rental rows, all attributable to the new evidence-gap rule, with zero material changes outside those gaps, exact pre-change key sets and row counts, and unchanged `site_missing`.

- **Where:** missingness is reduced to `annual_status == "absent"` at `load_data():89-92`; raw events are loaded at `:114-123`; unmatched transaction-site pairs are zero-filled at `calculate_metrics_by_radius():198-219`.
- **Problem:** the event feed is positives-only and not complete for every reported-positive Site Group-year. When a positive annual return has no matched event rows, these scripts do not mark the year unknown. A transaction-site pair instead receives zero for that missing year, or a cumulative total that omits the year. This conflicts with the repaired `aggregate_spill_stats.R` contract, which keeps EA-only positive counts and subannual metrics `NA` rather than interpreting absent event matches as zero.
- **Evidence:** the current 2021-2024 crosswalk has 832 `reported_positive` Site Group-years with zero matched events across 732 groups. Their annual-return totals include 151,179 spill hours and 22,741 outlet-summed spill counts. In the current non-missing outputs, 165,965 sale rows (122,980 at 1,000 m) and 79,149 rental rows (59,906 at 1,000 m) have a transaction window that reaches at least one such year.
- **Impact:** the central exposure regressors are understated for affected observations even though `site_missing == FALSE`.
- **Decision required:** align these builders with the established data-source contract. At minimum, mark cumulative counts unknown when an intersecting `reported_positive` year has no matched events. Decide whether hours should use the annual EA hours fallback or also remain unknown. Keep `reported_zero` as valid zero, and make the treatment of `reported_na` explicit.
- **Validation:** correctness and the independent validator confirmed the inconsistency; the orchestrator quantified current exposure.

## Moderate

### 2. `[x]` Transactions on 2021-01-01 emit zero-day windows and `NaN` averages

- **Resolution (2026-08-14):** the shared loader computes integer complete 24-hour days from the UTC window start and retains only transactions with at least 30 complete days before deriving cutoffs, prefixes, or chunks. Boundary contracts exclude 29 days 23 hours 59 minutes and retain exactly 30 days. All four regenerated outputs have a minimum window of 30 days and staged validation rejects `NaN` or infinite rates.

- **Where:** input filters at line `75`, denominators at `house_spill_prior_to_sale.R:257-262` and `rental_spill_prior_to_rental.R:256-261`, and divisions at house `:350-356` / rental `:349-355`.
- **Problem:** the inclusive `>= CONFIG$window_start` filter admits transactions whose exposure interval has zero days. Their non-missing metrics are zero, so the four rates evaluate `0 / 0 = NaN`.
- **Evidence:** 63 sales and 373 rentals occur on 2021-01-01. The August sales output has 116 zero-day site-radius rows, all with `NaN` rates. The rental output has 820 zero-day rows: 659 have `NaN` rates and 161 have `NA` through the missing-site mask.
- **Decision required:** either exclude boundary transactions with a strict `>` filter, or retain them with all four averages explicitly set to `NA_real_` when `n_days_in_window == 0`. Apply and document one rule in both scripts.
- **Validation:** correctness and adversarial confirmed the arithmetic; current output counts were measured directly.

### 3. `[x]` Empty-chunk prototypes corrupt identifier types and the rental schema

- **Resolution (2026-08-14):** the shared engine owns literal typed schemas for all four variants. Site-grain empty chunks write no fragment; radius-grain no-site chunks write the complete typed zero-site grid. Every populated chunk and the reopened stage are checked against the exact public schema and keys. Mixed empty/populated fixtures and all four regenerated datasets pass those checks.

- **Where:** house `process_chunk():284-296`; rental `process_chunk():283-295`.
- **Problem:** both empty prototypes declare transaction IDs and `site_id` as `character()` although production inputs are `int32`. The rental prototype also declares `price` instead of `listing_price`. If one chunk has no pair within 1,000 m, `rbindlist(..., fill = TRUE)` promotes all real identifiers to character and adds an all-`NA` rental `price` column.
- **Evidence:** a direct bind of one populated row and the current empty prototypes reproduces both type promotion and the phantom column. The August production runs did not hit an empty chunk, so their schemas remain clean.
- **Suggested fix:** define one typed output prototype per pipeline from the actual contract, or at minimum use `integer()` identifiers and `listing_price` in the rental prototype. Test one empty and one populated chunk together.
- **Validation:** correctness, adversarial, and the independent validator reproduced the corruption.

### 8. `[x]` Four prior-exposure builders maintain the same preparation logic independently

- **Resolution (2026-08-14):** one closed `sale|rental × site|radius` engine in `prior_exposure_utils.R` now owns eligibility, completeness prefixes, event clipping and aggregation, rates, schema normalization, chunk validation, and streaming publication. The four producer files retain only configuration, bootstrap, compatibility delegates, and a short main entrypoint. Parity contracts exercise all four variants through that shared seam.

- **Where:** these two files plus `cross_section_prior_to_sale.R` and `cross_section_prior_to_rental.R`.
- **Problem:** loading, date clipping, missingness, event joining, chunking, rate construction, and export logic are copied across sale/rental and site/radius products. Finding 3 is a demonstrated copy-paste divergence, and every correction in this review otherwise requires parallel edits.
- **Suggested fix:** after the measurement decisions in findings 2, 4, and 11 are settled, extract a shared transaction-to-site exposure preparation helper. Keep thin sale/rental wrappers and separate final reducers for the intentionally different output grains. Add parity fixtures before consolidating.
- **Validation:** the maintainability pass and a mechanical comparison confirmed the repeated structure.

### 12. `[x]` An empty eligible transaction input fails with opaque base-R sequence errors

- **Resolution (2026-08-14):** the shared loader now stops immediately after the 30-day eligibility filter when no transaction remains, with a variant- and window-specific error before cutoff, prefix, or chunk construction. Focused contracts cover the empty retained cohort for every variant.

- **Where:** year derivation at lines `81-84` and chunk starts at house `:314-318` / rental `:313-317`.
- **Problem:** if filtering leaves zero eligible transactions, `min()`/`max()` operate on an empty year vector and `seq()` receives non-finite endpoints; even a bypass of that point makes `seq(1L, 0L, by = chunk_size)` fail with `wrong sign in 'by' argument`.
- **Suggested fix:** fail fast immediately after collection with an input-path and window-specific error. If a zero-row artifact is genuinely a valid product state, return and publish a fully typed empty prototype instead. Cover the chosen contract in both scripts.
- **Validation:** testing, performance/reliability, and the independent validator reproduced the failure.

### 13. `[x]` Chunking bounds the cartesian join but not peak pipeline memory

- **Resolution (2026-08-14):** completed chunk results are now validated, written to a run-specific stage, removed, and garbage-collected before the next sequential chunk; no result list or final in-memory bind remains. All four production runs completed through this path. The largest output contained 11,419,359 rows and published in 39 chunks with 6.23 GB maximum resident memory; per-chunk logs and failure fixtures confirm that partial stages cannot replace the canonical dataset. Eager input collection remains an explicit, measured boundary rather than an unresolved output-accumulation defect.

- **Where:** all transaction, lookup, and event inputs are collected at lines `71-123`; all chunk results are retained inside `rbindlist(lapply(...))` at house `:322-332` / rental `:321-331`.
- **Problem:** the 10,000-ID chunks limit only the per-chunk event expansion. The run still holds every input in memory, retains every completed chunk result until `lapply()` finishes, and allocates the combined result while that list remains live.
- **Current scale:** the sales path collects about 3.18 million transactions, 4.28 million lookup pairs within 1,000 m, and 7.27 million raw events before producing 5.68 million output rows. The rental path produces 3.04 million rows from 1.45 million transactions, 2.30 million lookup pairs, and the same event table.
- **Suggested fix:** profile peak memory first. If it is material, keep Arrow inputs lazy, collect only chunk-relevant lookup/event rows, and stream validated chunks to a run-specific staging dataset rather than retaining the entire result list. Coordinate this with the safe publication design in finding 6.
- **Validation:** the performance/reliability pass confirmed the allocation shape; this remains a scaling concern rather than a demonstrated failed run.

## Low / advisory

### 9. `[x]` Debug markers, dead alternatives, stale comments, and bootstrap debris remain

- **Resolution (2026-08-14):** all four entrypoints now use the standard header and `script_setup.R` fail-fast bootstrap, source the shared utilities locally, and contain no runtime installation, stale test markers, commented alternatives, or list-binding implementation. `spill_aggregation_utils.R` now reports `rv sync` guidance instead of installing packages while leaving `count_spills()` unchanged.

- **Where:** both scripts at `:222` (`# TEST: REMOVE LATER` above live production code), `:235-241` (commented alternative implementation), and house `:315` / rental `:314` (commented test truncation). The `# Shallow copy` comment at `:227` is inaccurate; house `:378` retains an unresolved `# CHANGE`; `fs` is loaded but unused; and the bootstrap installs missing packages at runtime.
- **Problem:** the comments make the authoritative path unclear and invite deletion of live code. Runtime package installation also mutates the execution environment instead of failing against the project's `rv`-managed dependency contract.
- **Suggested fix:** delete the test markers and dead blocks, correct/remove stale comments, remove unused dependencies, and use a shared fail-fast project bootstrap. Rename the house log consistently if desired.

### 10. `[-]` `count_spills()` remains an interpreted loop invoked once per transaction-site group

- **Won't fix (2026-08-14):** this is an advisory scaling concern, not a correctness defect. The refactor deliberately preserves the established `count_spills()` interface and exact boundary behavior rather than introducing a second counting implementation. The streaming engine now records transaction, lookup-pair, joined-event, output-row, and elapsed-time diagnostics for every chunk, providing the agreed instrumentation for any future evidence-driven optimization.

- **Where:** `calculate_metrics_by_radius():198-202` in both scripts, calling the loop in `spill_aggregation_utils.R:199-253`.
- **Problem:** this is still the dominant algorithmic scaling concern as radii, years, or matched-site fan-out grow. It is not a correctness defect, and the current full runs completed.
- **Suggested fix:** retain as an instrumentation item. Record per-chunk elapsed time and group counts before attempting vectorisation or a compiled implementation.

## Verified current invariants and resolved upstream notes

- Both scripts parse under R 4.6.0. Current Arrow output schemas use integer transaction/Site Group IDs and the expected daily/weekly metric columns.
- Both August outputs are unique on `(transaction_id, site_id, radius)`, contain only radii 250/500/1000, and match every lookup pair within 1,000 m at the 1,000 m partition.
- The house and rental lookups are unique on `(transaction_id, site_id)`, contain no Site Group absent from the crosswalk, and now both extend to 10 km. The old rental 5 km asymmetry is fixed.
- `count_spills()` now uses `current_start >= block_end`, so the old exact-boundary overcount note is resolved.
- The 2,979 coincident `(site_id, start_time, end_time)` sets are not exact source-event duplicates. They are simultaneous events from different monitored outlets grouped to one Site Group; summing their durations as outlet-hours is the current intended convention. Do not deduplicate at Site Group/timestamp grain.
- The current raw event table has 7,271,711 rows, no missing timestamps, and no non-positive intervals.

## Known patterns from project learnings

- `CONCEPTS.md` defines Annual Status specifically to distinguish reported zero from missing evidence. Findings 4 and 11 must preserve that distinction.
- `docs/solutions/design-patterns/rescale-regressor-at-source-for-interpretable-units.md` identifies these builders as the canonical construction point for daily and weekly exposure. Any fix must preserve `weekly = daily * 7` whenever the daily value is defined.
- `docs/solutions/best-practices/output-compatible-edm-standardisation-refactor-20260309.md` treats output path, partition layout, names, types, and downstream semantics as one compatibility contract. This supports staging plus post-write validation rather than an in-place overwrite.

## Review coverage notes

- All six specialist review contexts returned. The project-standards pass found no violation of `AGENTS.md` or `CLAUDE.md` in the two scripts.
- The independent validator accepted all eight selected blocker/actionable findings. No selected finding was dropped.
- The cross-model branch-diff pass was not used because this was a static audit of two named current files, not a review of the current branch diff; the in-process adversarial pass covered that lens.
- One anchor-50 cleanup observation was kept as advisory finding 9 rather than treated as a blocker. The old per-pair performance note remains advisory finding 10.
- Validation used read-only production-data queries and small temporary probes. The full multi-hour producers were not rerun during this update; the outputs audited here were generated on 2026-08-11.
- No fixes were applied to the R scripts. This updated review document is the only repository change.
- Review artifacts: `/tmp/compound-engineering-501/ce-code-review/20260812-174310-35505a4e`.

## Recommended resolution order

1. Make the three measurement decisions once: transaction-specific missingness (finding 4), event-incomplete positive years and `reported_na` (finding 11), and zero-day transactions (finding 2).
2. Add the focused contract fixtures from finding 7 so those choices and the current output contract are executable.
3. Apply the mechanical safeguards: explicit rental timestamps (finding 1), year coverage checks (finding 5), typed empty prototypes (finding 3), empty-input handling (finding 12), and staged publication (finding 6).
4. Rebuild and audit both outputs. Compare key membership, missingness shares, finite rates, and exposure distributions against the August baseline.
5. Consolidate the shared preparation path (finding 8), then profile before undertaking the performance work in findings 10 and 13. Finish with the cleanup in finding 9.

## Actionable findings

| # | Priority | Location | Response |
|---:|---|---|---|
| 1 | P1 | `rental_spill_prior_to_rental.R:165` | Convert rental dates explicitly to UTC `POSIXct`; add clean-namespace test. |
| 3 | P2 | `house_spill_prior_to_sale.R:286`; rental `:285` | Replace drifting empty schemas with typed prototypes. |
| 5 | P1 | both `load_data():81-92` | Fail on unsupported years and implausible future dates. |
| 6 | P1 | both `export_data()` functions | Stage, validate, and promote complete datasets. |
| 7 | P1 | `scripts/R/testing/` | Add the producer/output-contract suite before refactoring. |
| 8 | P2 | four prior-exposure builders | Extract shared preparation after measurement decisions are settled. |
| 12 | P2 | both `load_data()` / driver functions | Define and test fail-fast empty-input behavior. |
| 13 | P2 | input collection and result binding | Profile peak memory; stream chunks if material. |

## Decision gates

| # | Priority | Decision |
|---:|---|---|
| 2 | P2 | Exclude 2021-01-01 transactions, or retain them with undefined (`NA`) rates? |
| 4 | P1 | Use transaction-specific monitoring completeness, or deliberately enforce a balanced full-sample window? |
| 11 | P1 | For positive annual returns with no matched events, which cumulative metrics use annual fallback and which remain unknown? How should `reported_na` behave? |

## Verdict

**Ready.** The measurement decisions for findings 2, 4, and 11 are implemented, the producer/output contracts and publication safeguards are installed, and the four regenerated canonical datasets passed full eligible-baseline reconciliation. Finding 10 remains an explicitly accepted instrumentation item rather than a correctness blocker.
