---
status: done
priority: p0
issue_id: "011"
tags: [code-review, data-integrity, matching, r, pipeline, consolidated]
dependencies: []
merged_from:
  - "001: Preserve and identify annual unmatched rows"
  - "002: Preserve annual rows through max matching"
  - "003: Harden fuzzy matching stage"
  - "004: Harden windfall matching stage"
  - "006: Add stage-handoff regression tests for location merge"
  - "008: Stage location merge outputs atomically"
  - "010: Full-script sanity review (2026-06-10)"
---

# Consolidated findings: merge_individ_annual_location.R

> **Superseded (2026-07-04):** the script will be rebuilt from scratch rather than
> fixed in place. All decisions and the findings→resolution map live in
> [`docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md`](../../docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md).
> Do not implement fixes from this file directly; it is retained as the evidence record.
>
> **Resolved (2026-07-06):** the rebuild shipped. All 25 findings are resolved or
> obsoleted per the findings map at the end of the plan above. Implementation landed
> across the chunk branches (merge pipeline `jo/merge-ch6-orchestrator` @ a52ca73,
> reconciliation gate `jo/merge-ch7-reconciliation`, downstream migrations
> `jo/merge-ch8-unique-sites`, `jo/merge-ch9-agg-stats`, `jo/merge-ch10-verification`);
> reconciliation and migration evidence under
> `output/merge_rebuild_reconciliation_2026-07-05/` and
> `output/merge_rebuild_downstream_migration_2026-07-06/`. CH7 exact-tier exceptions
> were reviewed and accepted case-by-case (manual overrides with per-row evidence in
> `data/processed/matched_events_annual_data_manual_overrides.csv`).

Single source of truth for all open issues in
[`scripts/R/05_data_integration/merge_individ_annual_location.R`](../../scripts/R/05_data_integration/merge_individ_annual_location.R),
consolidating todos 001–004, 006, 008, and the 2026-06-10 full-script sanity review (010).
The superseded per-issue files are deleted (recoverable from git history). Line numbers
refer to the current script (1108 lines, identical on `jo/merge_indivi_annual` and
`jo/annual_lookup_check`); stale line references from the older todos have been remapped.

**Review provenance:** initial review 2026-03-10 (Codex; synthetic-data reproductions for
findings 2, 7, 8, 13), scope expansions 2026-04-23 (weak keys, joined-loser drops, candidate
bounds, finalisation contract), full-script multi-agent sanity review 2026-06-10 (static
analysis; environment, max-match internals, fuzzy rebuild, metadata, docs).

## Summary

| # | Sev | Line | Stage | Finding | From |
|---|-----|------|-------|---------|------|
| 1 | P0 | 955 | Environment | `rio::import()` parquet path broken: rio 1.3.0 needs `nanoparquet`, not installed | 010 |
| 2 | P1 | 446 | Windfall | Event-only companies dropped before matching (one-sided loop) | 004 |
| 3 | P1 | 185 | Windfall | Weak-key fallthrough (`water_company\|year`) creates false matches; 2^n−1 key blowup | 004 |
| 4 | P1 | 416 | Windfall handoff | Annual rows with NA spill metrics dropped from **both** outputs | 001 |
| 5 | P1 | 629 | Max-match | Annual-only company-years never enter the loop and vanish | 002 |
| 6 | P1 | 596 | Max-match | Joined-but-not-selected annual candidates dropped (`keep_joined_losses = FALSE`) | 002 |
| 7 | P1 | 545 | Max-match | All-NA column re-prune can crash the tie computation | 010 |
| 8 | P1 | 567 | Max-match | Tie flag wrong under NA metrics; disagrees with the actual `slice(1)` winner | 010 |
| 9 | P1 | 560 | Max-match | `match_quality` never set → NA for every max-method row | 010 |
| 10 | P1 | 878 | Fuzzy | Zero-column `fuzzy_merges` crashes the reconstruction joins | 003 |
| 11 | P1 | 998 | Finalisation | `annual_unmatched.parquet` lacks a canonical `site_id`; hidden late input reads | 001 |
| 12 | P2 | 728 | Fuzzy | Unbounded candidate generation per company-year block | 003 |
| 13 | P2 | 918 | Fuzzy | `_dup`-drop discards event values instead of coalescing (windfall does the opposite) | 010 |
| 14 | P2 | 801 | Fuzzy | Installed reclin2 0.6.0 silently ignores `n`, `m`, `include_ties` | 010 |
| 15 | P2 | 279 | Windfall | Coalesce overwrites the event `unique_id` with the annual one | 010 |
| 16 | P2 | 417 | Windfall | Zero-spill annual rows mislabelled `match_method="windfall"`, quality 1 | 010 |
| 17 | P2 | 304 | Windfall | `find_matchable_keys` defined twice (byte-identical) — edits to the first copy are dead | 010 |
| 18 | P2 | 975 | Finalisation | Per-year enrichment invariant (exactly one non-NA `site_id_<year>`) unasserted | 010 |
| 19 | P2 | 959 | Finalisation | `2021:2024` hard-coded in three places, drifting from `CONFIG$years` | 010 |
| 20 | P2 | 1023 | Export | Non-atomic four-file publish — partial failure leaves a mixed snapshot | 008 |
| 21 | P3 | 1008 | Finalisation | `site_metadata` is event-granular, not site-level | 010 |
| 22 | P3 | 114 | Config | Commented-out `keep_cols` fragment lists `site_id_2024` twice, omits 2023 | 010 |
| 23 | P3 | 524 | Docs | Doc drift: garbled roxygen, stale params and examples | 010 |
| 24 | P3 | 672 | Max-match | `annual_unmatched` vs `annual_unmatched_nonzero` naming asymmetry | 010 |
| 25 | P3 | 1104 | Main | Comment claims `main()` runs "when sourced" — the guard does the opposite | 010 |

Testing/regression work (former todo 006) is tracked in its own section below, not as a
numbered finding.

---

## Environment / run blocker

### 1. `finalise_merged_data` crashes: `rio::import()` needs `nanoparquet` (P0)

[`merge_individ_annual_location.R:955-956`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L955)

```r
lookup <- import(CONFIG$data_path_lookup)
annual_returns <- import(CONFIG$data_path_annual)
```

- The rv library's `rio` is **1.3.0** (re-installed 2026-06-09). Since rio 1.1.0, parquet import
  goes through `nanoparquet` (confirmed in the installed package's NEWS.md and DESCRIPTION),
  not `arrow`.
- `nanoparquet` is **not** in `rv.lock` nor in `rv/library/4.6/arm64/`, and rio only *suggests*
  it, so `check_required_packages()` cannot catch this.
- Existing outputs date from 2026-01-16 (before the library re-sync) — the last successful run
  does not prove the current environment works.
- Failure point is *after* the windfall, max, and fuzzy stages complete: hours of compute lost.

**Fix (recommended):** replace both `import()` calls with `arrow::read_parquet()`, consistent
with `load_data()`. Combine with the preflight fix in finding 11 (validate all three inputs —
including the lookup — *before* matching starts). Alternative: add `nanoparquet` to the rv
project and re-sync.

---

## Windfall stage (exact matching)

### 2. Event-only companies dropped before matching (P1)

[`merge_individ_annual_location.R:446-450`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L446)

`company_year_combinations` is built from `unique(annual_return_df$water_company)` × `CONFIG$years`
only. Events for a company absent from the annual returns never enter the loop and never reach
`events_unmatched` — silent row loss before any matching logic runs. Reproduced 2026-03-10 with
synthetic events for companies `A` and `B` but annual data only for `A`: the `B` event vanished
(`events_unmatched_rows = 0`). Hidden only as long as both inputs share identical company
inventories; wrong for renamed, partial, or future inventories. Same bug class as finding 5
(max-match stage).

**Fix:** build the loop domain from the **union** of event and annual `(water_company, year)`
pairs and pass one-sided empty subsets through as unmatched.

### 3. Weak-key fallthrough and unbounded key space (P1)

[`merge_individ_annual_location.R:185-195`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L185),
[`:350-351`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L350),
[`:221-223`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L221)

`generate_key_combinations()` enumerates **all** non-empty subsets of the shared columns
(2^n − 1; 9 shared columns today → 511 combinations per company-year, doubling per added
column), and `find_matchable_keys()` only requires uniqueness on the annual side — no strong
identifier required. The loop therefore falls through to keys as weak as `water_company|year`:
reproduced with two unrelated synthetic event rows and one annual row in the same company-year →
both matched with `match_key = "water_company|year"`. With company-year event groups in the
hundreds of thousands of rows, this is both a false-positive-match correctness risk and a
scaling risk.

**Fix:** replace unrestricted generation with an **ordered whitelist** of curated key
combinations, each containing at least one strong site identifier (permit reference, site name,
site_id); log and bound the key search space. (Smaller fallback: filter generated keys to require
a strong identifier and cap the count — more indirect, key space can still grow.)

### 15. Coalesce overwrites the event `unique_id` with the annual one (P2)

[`merge_individ_annual_location.R:279-283`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L279)

Both inputs carry `unique_id` (events: `ea_id`/`yw_discharge_urn`/`srn` mapped to `unique_id`;
annual returns: native `unique_id`). When a windfall match is made on a key that does not include
`unique_id`, the coalesce-prefer-annual loop replaces the event's identifier wherever the annual
value is non-NA. Any downstream join back to event-level data on `unique_id` will misfire.

**Fix:** exclude `unique_id` from `overlap_bases` (keep `unique_id_event` / `unique_id_annual`
as separate columns), or document that `unique_id` in the matched output is annual-derived.

### 16. Zero-spill annual rows mislabelled as windfall matches (P2)

[`merge_individ_annual_location.R:415-424`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L415)

Rows appended by `add_zero_spill_stats()` matched *nothing*, yet are tagged
`match_method = "windfall"`, `match_quality = 1`, `match_type = "unique"`. Only
`match_key = ""` / `key_length = 0` distinguish them. Downstream filters on
`match_method == "windfall"` spuriously include site-years with no events.

**Fix:** distinct labels, e.g. `match_method = "zero_spill_annual"`, `match_type = "no_event"`.

### 17. `find_matchable_keys` defined twice (P2)

[`merge_individ_annual_location.R:205-234`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L205) and
[`:304-333`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L304)

Byte-identical copies (same commit, `0e5f6a51`), including the roxygen block. Harmless at
runtime (the second silently shadows the first), but any future edit to the first copy is dead
code.

**Fix:** delete one copy (lines 296–333 including its roxygen block).

---

## Windfall handoff: NA spill-metric handling

### 4. Annual rows with NA spill metrics dropped from both outputs (P1)

[`merge_individ_annual_location.R:416`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L416) and
[`:428`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L428)

`add_zero_spill_stats()` partitions unmatched annual rows with
`spill_hrs_ea == 0 & spill_count_ea == 0` (zero bucket) and
`spill_hrs_ea != 0 | spill_count_ea != 0` (non-zero bucket). Rows where the predicates evaluate
to `NA` fail **both** filters and are silently dropped from the pipeline. Verified with synthetic
`NA/NA`, `0/NA`, and `NA/0` inputs — all returned zero rows in both buckets. The current
annual-return input contains **4,076 rows with both metrics NA**; mixed `0/NA` and `NA/0`
cases are latent. The loss happens before export, so downstream files cannot recover these
site-years; `investigate_partial_availability_missingness.qmd:190` documents the same mechanism
behind partial-availability gaps.

**Fix (recommended):** carry `NA`-metric rows forward in a defined unmatched-annual bucket
(NA-safe predicates, e.g. `coalesce(spill_hrs_ea, 0) == 0 & coalesce(spill_count_ea, 0) == 0`
plus an explicit unknown-metrics bucket if the distinction matters downstream). The cleanest
contract is three explicit buckets (zero / non-zero / unknown-metrics), at the cost of touching
more output contracts.

---

## Max-match stage (many-to-many)

### 5. Annual-only company-years never enter the loop (P1)

[`merge_individ_annual_location.R:629-632`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L629),
[`:669-673`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L669)

`company_years` is built from `unique(events_unmatched$water_company)` ×
`unique(events_unmatched$year)` only, and the final `rbindlist()` binds only loop outputs.
Annual rows in company-years with no remaining unmatched events vanish before fuzzy matching and
export. Reproduced with one unmatched event in `A-2021` and one annual-only row in `B-2022`:
the `B-2022` row was lost. Same one-sided-loop bug class as finding 2.

**Fix:** build `company_years` from the **union** of event and annual `(water_company, year)`
pairs; pass empty-side subsets through as unmatched (the empty-side early-return at lines
647–653 already does this once the loop domain includes those combinations).

### 6. Joined-but-not-selected annual candidates dropped (P1)

[`merge_individ_annual_location.R:596-604`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L596)

With the default `keep_joined_losses = FALSE`, annual rows that joined but lost the
one-best-row selection are removed from **all** outputs — neither matched nor unmatched.
Reproduced with one event and two annual candidates sharing a join key: `max_matches = 1`,
`max_annual_unmatched = 0` (with `keep_joined_losses = TRUE`: 1, as expected). The current
annual-return data has **90 duplicate full shared-key groups covering 195 annual rows**, so the
losing-candidate shape is realistic.

**Fix:** define the remaining annual set as "annual rows not selected by `best_matches`"
(i.e. make the `keep_joined_losses = TRUE` semantics the contract), and add row accounting:
annual input rows = selected + unmatched.

### 7. All-NA column re-prune can crash the tie computation (P1)

[`merge_individ_annual_location.R:544-545`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L544)

The caller deliberately re-includes the spill metrics after pruning all-NA columns
([`:645`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L645):
`select(where(~ any(!is.na(.))), any_of(c("spill_hrs_ea", "spill_count_ea")))`), but
`run_max_match` prunes **again** without the `any_of()` re-add. A company-year subset where every
row has `spill_hrs_ea = NA` (with non-zero counts — such rows pass the non-zero filter because
`NA | TRUE` is `TRUE`) loses the column, and the `summarise(max_hrs = ...)` at line 571 errors
with *object 'spill_hrs_ea' not found*, aborting the script. Finding 4 shows mixed-NA metric
rows are latent in the real data.

**Fix:** mirror the caller's guard inside `run_max_match` (add the `any_of()` re-add to lines
544–545), or make the tie/max logic tolerate missing spill columns.

### 8. Tie flag disagrees with the selection and is wrong under NA metrics (P1)

[`merge_individ_annual_location.R:567-588`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L567)

Three defects:

- When `max_hrs` is NA (all-NA hours), `spill_hrs_ea == max_hrs` is NA for every row, so
  `sum(..., na.rm = TRUE)` is 0 and `tie = FALSE` — even with several genuinely tied candidates.
  The arbitrary `slice(1)` winner is then reported as unambiguous.
- `max(spill_count_ea[spill_hrs_ea == max_hrs], na.rm = TRUE)` returns `-Inf` with a warning
  when the selected counts are all NA, and `-Inf` feeds the tie comparison.
- The tie indicator is computed from `max_hrs`/`max_count` while the actual winner comes from a
  separate `arrange(desc(...)) %>% slice(1)`; under NAs the two code paths can disagree.

**Fix:** derive both winner and tie flag from the same NA-safe ordering, e.g. rank on
`coalesce(spill_hrs_ea, -Inf)`, `coalesce(spill_count_ea, -Inf)` and set
`tie = (rank-1 value == rank-2 value)` per `event_row_id`.

### 9. `match_quality` never set for max matches (P1)

[`merge_individ_annual_location.R:560-565`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L560)

The mutate sets `match_method`, `match_type`, `match_key`, `key_length` — but not
`match_quality`, which windfall sets to `1` and fuzzy to `mpost`. Every max-method row exports
`match_quality = NA` (silently materialised by `rbindlist(fill = TRUE)`), so downstream filters
or weights on `match_quality` silently treat max matches differently. Flagged independently by
three reviewers.

**Fix:** set `match_quality` explicitly in the line-560 mutate (documented `NA_real_` or a
defined constant for exact-on-all-shared-columns matches). While there, consider populating
`tie = NA` on windfall/fuzzy rows for a consistently typed column.

### 24. Stage-boundary naming asymmetry (P3)

[`merge_individ_annual_location.R:672`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L672)

`run_many_to_many_max_match` returns `annual_unmatched` where the windfall stage and the
`run_fuzzy_matching` contract use `annual_unmatched_nonzero`; the caller compensates at
[`:1088`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1088). Rename the
return key to remove the translation step. (Note: once finding 4 is fixed, the `_nonzero` name
itself needs revisiting — the bucket will also carry unknown-metric rows.)

---

## Fuzzy stage

### 10. Zero-column `fuzzy_merges` crashes the reconstruction joins (P1)

[`merge_individ_annual_location.R:878-928`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L878)

If `events_unmatched` is empty, **or** no block yields usable comparator columns / candidate
pairs (every block returns `NULL`), `bind_rows()` produces a zero-column tibble and the
reconstruction joins fail with *Join columns in x must be present in the data. Problem with
`group_id_annual`* (lines 912–928). `main()` calls `run_fuzzy_matching()` unconditionally
([`:1081`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1081)), and
"all events matched before fuzzy" is a valid — desirable — end state. Both trigger paths
reproduced synthetically 2026-03-10. A related entry path: 0-row inputs reaching
`prepare_fuzzy_data` lose all columns to the `where()` prune and hit the misleading
`"events_df must contain 'water_company'"` stop at line 686.

**Fix:** short-circuit `run_fuzzy_matching()` and return `final_res` unchanged when either side
is empty; when blocks produce no selected pairs, construct a typed empty `fuzzy_merges` (with
`group_id_event`/`group_id_annual` columns) so the joins are no-ops.

### 12. Unbounded candidate generation per block (P2)

[`merge_individ_annual_location.R:728-733`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L728)

`pair_blocking()` blocks only on `water_company`/`year`, materialising the full Cartesian
product within each block. Existing logs show batches up to **155,236 candidate pairs**
(including one that selected 0 matches), and current outputs still leave 124,368 unmatched
events and 742 unmatched annual rows. The row-preservation fixes (findings 4–6) will *increase*
fuzzy-stage inputs, so bounds should land before or alongside them. No candidate cap, chunking,
stronger blocking keys, or top-k policy exists.

**Fix:** add a documented per-block candidate ceiling (log + skip with explicit unmatched
carry-forward, or fail closed, when exceeded). Optionally strengthen blocking (permit prefixes,
normalised site-name buckets, NGR prefixes) or add cheap top-k prefilters before EM scoring —
with validation that plausible matches aren't silently discarded.

### 13. `_dup`-drop discards event values instead of coalescing (P2)

[`merge_individ_annual_location.R:912-918`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L912)

The reconstruction joins `events_full` with `suffix = c("", "_dup")` and then
`select(-ends_with("_dup"))`. For every column present on both sides, the event value is
discarded *even when the annual value is NA* — the opposite of the windfall stage's
coalesce-prefer-annual policy (lines 274–284). The same event–annual pair yields different
identifier completeness depending on which stage matched it.

**Fix:** coalesce each colliding column with its `_dup` twin before dropping, mirroring
`extract_join_tag`.

### 14. Installed reclin2 0.6.0 silently ignores `n`, `m`, `include_ties` (P2)

[`merge_individ_annual_location.R:801-815`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L801)

`select_n_to_m()` in the installed version accepts these only via `...` and hard-codes 1:1
matching (verified in the installed package source by two reviewers independently). Today's
behaviour is correct only by coincidence (`main()` passes `n = 1, m = 1`); `include_ties = TRUE`
is never applied — equal-`mpost` candidates get an arbitrary, unflagged winner — and the log
line reports configuration that is not in effect.

**Fix:** drop the dead arguments and misleading log fields, or pin/upgrade to a reclin2 version
that honours them; add a `formals()` assertion so a future package change is caught.

---

## Finalisation

### 11. Missing `site_id` contract on unmatched outputs; hidden late input reads (P1)

[`merge_individ_annual_location.R:955-956`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L955),
[`:998-1004`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L998)

- `load_data()` validates only the event and annual inputs (lines 146–154);
  `annual_return_lookup.parquet` is read for the first time inside finalisation, after the
  expensive matching stages, and the annual returns are re-read instead of reusing the loaded
  data. A missing/malformed lookup fails the run at the very end (compounded by finding 1).
- The current `annual_unmatched.parquet` (742 rows) has **no `site_id` column** even though the
  lookup contains canonical site identifiers — finalisation only renames `outlet_discharge_ngr`
  for unmatched annual rows before trimming to `CONFIG$keep_cols`. Downstream consumers join on
  `site_id` and cannot interpret these rows.

**Fix:** preflight all three inputs (existence + required columns) before matching starts; pass
the loaded lookup/annual data into `finalise_merged_data()` instead of re-reading; enrich
`annual_unmatched` (and, where possible, `events_unmatched`) with the canonical `site_id` from
the lookup — for annual rows a `coalesce(site_id_2024, ..., site_id_2021)`-keyed lookup join
before the keep-cols trim suffices.

### 18. Per-year enrichment invariant unasserted (P2)

[`merge_individ_annual_location.R:975-995`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L975)

The loop keeps a matched row only in year-slices where `site_id_<year>` is non-NA: a row with
all four NA would be **silently dropped**; a row with several non-NA year-ids would be
**duplicated** once per year. Safe *today* because `combine_annual_return_data.R` assigns
`site_id_{year} = row_number()` per year (exactly one non-NA per annual row) and every matched
row carries an annual side — but nothing in this script checks it.

**Fix:** assert row conservation around the loop
(`nrow(enriched) == nrow(matched_combined)`) and exactly-one non-NA `site_id_<year>` per row
before the loop.

### 19. `2021:2024` hard-coded in three places (P2)

[`merge_individ_annual_location.R:959`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L959),
[`:989-994`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L989),
[`:104-105`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L104)

`years_lookup <- 2021:2024`, the `coalesce(outlet_discharge_ngr_2024, ..., _2021)`, and
`CONFIG$id_cols`' literal `site_id_2021..2024` all duplicate `CONFIG$years`. Extending the study
window to 2025 silently produces stale/missing `ngr` values rather than erroring.

**Fix:** derive all three from `CONFIG$years`
(e.g. `coalesce(!!!syms(paste0("outlet_discharge_ngr_", rev(CONFIG$years))))`).

### 21. `site_metadata` is event-granular (P3)

[`merge_individ_annual_location.R:1008-1010`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1008)

`keep_cols` retains `group_id_event`, `match_key`, spill stats, etc., so `distinct()` after
dropping only `start_time`/`end_time` leaves near-event granularity. Verified consumers
(`create_unique_spill_sites.R`, `aggregate_spill_stats.R`) re-aggregate by `site_id` themselves,
so impact is low — but either deduplicate to genuinely site-level columns or rename the output.

---

## Export

### 20. Non-atomic four-file publish (P2)

[`merge_individ_annual_location.R:1023-1051`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1023)

`export_results()` writes the four parquet outputs sequentially (lines 1039, 1042, 1045, 1048)
straight into the live directory, with no staging, manifest, or rollback. A failure between
writes leaves a **mixed snapshot** — some files from the new run, some from the previous one —
that downstream analysis consumes without noticing. The repo's EDM hardening notes
([`docs/solutions/best-practices/edm-api-combine-hardening-20260310.md`](../../docs/solutions/best-practices/edm-api-combine-hardening-20260310.md))
already treat fail-closed publish as the preferred pattern.

**Fix (recommended):** write all four files to a temp/staging directory, validate
existence/schema, then swap into place only after the full set succeeds. (Alternatives: per-file
`*.tmp.parquet` + batch rename; a publish manifest as an additional validation gate.)

---

## Config / docs / minor

### 22. Comment typo in `keep_cols` (P3)

[`merge_individ_annual_location.R:114`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L114):
`# "site_id_2021", "site_id_2022", "site_id_2024", "site_id_2024"` — `2024` twice, `2023`
missing. If ever uncommented as-is, `site_id_2023` silently disappears from the output.

### 23. Doc-drift bundle (P3)

- [`:524-538`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L524): `run_max_match` has two pasted `@param`/`@return` blocks; the first is stale.
- [`:682-683`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L682): `prepare_fuzzy_data` documents `@param vars` (not a parameter) and claims `id_cols` "defaults to `vars`" (no default exists).
- [`:787`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L787): `select_matches` example threshold "e.g., 0.2" vs `CONFIG$threshold = 0.5`.
- [`:851`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L851): `block_var` example `"company_year"` vs actual `"blocking_var"`.

### 25. Wrong comment on the execution guard (P3)

[`merge_individ_annual_location.R:1104`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1104):
"Execute the main function when the script is sourced" — `sys.nframe() == 0` does the opposite:
`main()` runs under `Rscript`, *not* under `source()`.

---

## Testing & regression work (former todo 006)

The current test artifact (`scripts/R/testing/test_merge_individ_annual_location.rmd`)
re-embeds large chunks of production logic instead of asserting invariants — it can drift with
the implementation and still appear to pass. **Recommended approach:** small synthetic-data
checks under `scripts/R/testing/` that `source()` the production script and assert stage-handoff
invariants directly (effort 2–4 h).

Deduplicated invariant checklist (006's criteria ∪ the 2026-06-10 review's testing gaps), keyed
to the findings above:

- [ ] All-NA / `0/NA` / `NA/0` annual-metric rows are preserved in a defined bucket (finding 4).
- [ ] Annual-only company-years survive `run_many_to_many_max_match()` (finding 5).
- [ ] Annual candidates that join but lose selection remain in `annual_unmatched`; row
      accounting holds: annual in = selected + unmatched (finding 6).
- [ ] Event-only company-years survive (or fail explicitly) in windfall matching (finding 2).
- [ ] Windfall never matches on `water_company|year` alone; every key includes a strong site
      identifier; synthetic unrelated records in the same company-year stay unmatched (finding 3).
- [ ] `run_fuzzy_matching()` returns cleanly for empty-event, empty-annual, both-empty,
      zero-comparator, and zero-candidate inputs; `main()` completes when exact/max matching
      exhausts all events (finding 10).
- [ ] Fuzzy candidate generation respects a documented per-block cap; candidate-count logging
      includes the block identifier (finding 12).
- [ ] Every `match_method` populates `match_quality` (and `tie` where applicable) in the
      exported matched parquet (finding 9).
- [ ] Tie flag is correct on groups with NA spill stats: all-NA hours; non-NA hours with all-NA
      counts at the max; float-equal hours (finding 8).
- [ ] Company-year subsets whose `spill_hrs_ea` is entirely NA do not crash `run_max_match`
      (finding 7).
- [ ] Row conservation holds around the per-year enrichment loop (finding 18).
- [ ] `formals()` pin on `reclin2::select_n_to_m` arguments (finding 14).
- [ ] A failed export does not replace any subset of the previous output bundle; a successful
      export publishes a complete, consistent set (finding 20).
- [ ] `annual_unmatched.parquet` retains site identifiers after finalisation (finding 11).
- [ ] Clean-environment smoke test of the finalisation input reads — the rio→nanoparquet
      dependency is invisible to `check_required_packages()` (finding 1).
- [ ] Tests source production code rather than duplicating it.

---

## Candidates examined and refuted (2026-06-10 review — do not rediscover)

- **"Zero-spill annual rows lack `site_id_<year>` and are dropped by the enrichment loop"** —
  refuted: they are annual-return rows and carry their own-year `site_id_{year}`, which survives
  the all-NA pruning.
- **"Lookup join at line 983 collides on shared identifier columns and `keep_cols` silently
  drops them"** — not true for the current lookup schema; see residual risks.
- **Tidy-eval at lines 968-969** (`select({{ year_id }}, ...)`, `rename({{ ngr_col }} := ...)`
  with character scalars) — verified valid rlang usage.
- **`load_data` tryCatch scoping** (lines 157-170) — assignments persist in the function
  environment; correct.
- **`predict(..., binary = TRUE, inplace = TRUE)` in `score_pairs_em`** — real arguments in the
  installed reclin2 0.6.0; the in-place mutation pattern is correct.

## Residual risks (low confidence / future-proofing — no action required now)

- `score_pairs_em`'s error handler converts an EM failure into `mpost = 0` for the whole block —
  indistinguishable from "no plausible matches" (logged only).
- `extract_join_tag` detects join overlap purely by `_event`/`_annual` name suffixes; an
  upstream column whose natural name ends in those suffixes would be mangled (none exist today).
- The lookup join ([`:983`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L983))
  has no `suffix=` guard or uniqueness assertion on `site_id_<year>`; it relies on upstream
  guarantees. A `distinct()` on coords plus `relationship = "one-to-one"` would be self-defending.
- `count(..., .drop = FALSE)` in `find_matchable_keys` would create phantom zero-count key
  combinations if factor columns ever enter the pipeline (all-character today).
- `rbindlist(fill = TRUE)` silently type-promotes across company-year groups where `bind_rows`
  would error, masking cross-group type drift.
- The file is 1108 lines; the fuzzy pipeline (~676–935) is a natural extraction seam.

## Suggested fix order

1. **Finding 1** (P0, env) — two-line `arrow::read_parquet()` change; do before the next run.
2. **Findings 4, 5, 6, 2** (P1, row preservation) — the silent data-loss cluster; these block
   trusting refreshed canonical outputs. Fix the two one-sided loops (2, 5) with the same
   union-domain pattern.
3. **Findings 7, 8, 9** (P1) — all inside `run_max_match`; one pass.
4. **Findings 10, 12** (P1/P2, fuzzy robustness) — empty-stage contract + candidate caps; land
   before or alongside the row-preservation fixes, which will increase fuzzy inputs.
5. **Finding 11** (P1) — preflight + `site_id` contract on unmatched outputs (pairs with 1).
6. **Findings 3, 13–20** (P1/P2) — weak-key whitelist, fuzzy/metadata semantics, hardcoded
   years, atomic export.
7. **Regression tests** — add alongside each fix; the checklist above is the target set.
8. **Findings 21–25** (P3) — mechanical cleanups, any time.

## Related (other scripts — out of scope here)

- Lookup construction issues in `create_annual_return_lookup.R` are tracked in todo 009
  (`009-pending-p1-audit-annual-return-lookup-construction.md`, currently on branch
  `jo/annual_lookup_check`): same-year component conflicts collapsed via `first(site_id)`,
  RF-model load/save mismatches, and unstable canonical `site_id` ordering. Relevant as the
  producer of this script's lookup input.
