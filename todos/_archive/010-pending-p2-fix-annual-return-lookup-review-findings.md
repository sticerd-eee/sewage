---
status: done
priority: p2
issue_id: "010"
tags: [code-review, r, annual-return, record-linkage, data-quality, conflict-resolution, audit-trail]
dependencies: []
---

# Fix remaining annual return lookup review findings

Multi-agent code review (2026-06-10) of
[`scripts/R/03_data_enrichment/create_annual_return_lookup.R`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R)
at HEAD of `jo/annual_lookup_check`. Follow-up to the Codex review captured in
[`todos/_archive/008-pending-p1-fix-annual-return-lookup-rf-and-conflict-bugs.md`](_archive/008-pending-p1-fix-annual-return-lookup-rf-and-conflict-bugs.md)
and [`todos/009-pending-p1-audit-annual-return-lookup-construction.md`](009-pending-p1-audit-annual-return-lookup-construction.md).

## Problem Statement

The core conflict-resolution algorithm (year-constrained spanning forest, bitmask
duplicate-year check, union-find) is sound — no reviewer found an error that corrupts
the current deterministic lookup outputs. However, **two fixes recorded as `done` in
archived todo 008 were never actually applied to the code**, plus several audit-trail
and bookkeeping issues remain. None of the P3 items affect current output.

> Note: archived todo 008 is marked `status: done` but items 6 and 7 below are not in
> the code. That archive frontmatter should be corrected.

---

## P2 fixes (fix before the next full lookup run)

### 1. TBC/"N/A" placeholders still match as real identifiers in windfall matching

[`create_annual_return_lookup.R:121`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L121)
(`prepare_data_list`)

The cleaning that maps `"TBC"`/`"N/A"` -> `NA` exists **only** in the RF helpers
(lines 611, 738). The deterministic windfall joins — the path actually run by default —
see raw strings, and `na_matches = "never"` only guards genuine `NA`, so `"TBC" == "TBC"`
links unrelated sites. Verified on the real parquet: 1,258 `TBC` in `permit_reference_ea`
(plus 682 `permit_reference_wa_sc`, 414 `site_name_ea`, others). At deep cascade levels a
single TBC-keyed pair per water company yields a spurious 1:1 edge.

**Fix:** move the cleaning into `prepare_data_list()` so all matching paths see it, then
remove the now-redundant copies in the RF helpers (lines 611, 738):
```r
mutate(df, across(any_of(CONFIG$evidence_field_priority),
                  ~ if_else(toupper(.) %in% c("TBC", "N/A"), NA_character_, .)))
```
Regenerate the lookup afterwards.

---

### 2. Upstream: 2021 header variant still unmapped -> nulls site_name_wa_sc for ~2,577 rows

[`combine_annual_return_data.R:84`](../scripts/R/02_data_cleaning/combine_annual_return_data.R#L84)

Only `site_name_wa_sc_operational_optional` is mapped; the 2021 variant
`site_name_wa_sc_operational_name_optional` is not. Verified: 2021 `site_name_wa_sc`
coverage is ~62% vs 95%+ in other years, capping the quality of every 2021 edge this
script can build.

**Fix (in `combine_annual_return_data.R`):**
```r
"site_name_wa_sc_operational_name_optional" = "site_name_wa_sc"
```
Regenerate the combined parquet, then rerun the lookup.

---

### 3. Stale conflict-audit files survive a conflict-free rerun

[`create_annual_return_lookup.R:1523`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1523)
(`export_conflict_audit`)

`export_conflict_audit()` returns early when the summary is empty, leaving the previous
run's six conflict files (5 parquet + xlsx) on disk next to a fresh clean lookup. A reader
opening them would wrongly conclude the current lookup is conflicted.

**Fix:** when the summary has zero rows, overwrite the six audit outputs with
empty-schema tables (or `unlink()` them) before returning, so on-disk audit state always
matches the run that produced the lookup.

---

### 4. Post-resolution failure path references audit files it never wrote

[`create_annual_return_lookup.R:1757`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1757)

The post-resolution `stop_if_lookup_conflicts()` (line 1762) is the last-line safety net.
It is provably unreachable today (the constrained forest enforces one site per year per
component), but if a future edit breaks that invariant, its `stop()` message says
"Conflict audit files were written to data/processed/" — yet only the *pre-resolution*
audit is exported (line 1742), and the post-resolution audit is built without the
kept/dropped edge tables (defaults to empty). The diagnostics would be stale exactly when
needed most.

**Fix:** pass `kept_edges_for_audit`/`dropped_edges_for_audit` into the post-resolution
`build_lookup_conflict_audit()` call, and export it (under distinct `_post_resolution_`
paths) before stopping. At minimum, log the post-resolution conflict count so the
zero-conflict invariant is observable.

---

### 5. Edges parquet drops the columns explaining why each edge won

[`create_annual_return_lookup.R:1747`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1747)

The exported `annual_return_lookup_edges.parquet` keeps 10 columns, discarding
`edge_priority`, `evidence_field_count`, `field_priority_score`, and `resolution_order` —
the variables that determine which competing edge survived conflict resolution. Cleanly
resolved edges then have no provenance record (the conflict diagnostics keep these only
for conflicted components).

**Fix:** add those columns to the `select()` (lines 1748-1752) and to
`empty_edge_metadata()` (line 1592).

---

## P3 fixes (tidy-ups; none affect current output)

- **[Line 1387](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1387):** `empty_edges` declares `site_id_from`/`site_id_to` as `integer()`, but every live path produces `character()` (from `vertex_site_id()`/`str_remove`). Change both to `character()` to avoid an Arrow schema mismatch on the empty-edges boundary.
- **[Line 1766](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1766):** `summarise(site_id = first(site_id))` is guarded by the unconditional stop two lines above, so it cannot silently collapse today — but it reads like the original 96-conflict bug. Replace with an assertion (`stopifnot(length(site_id) == 1); site_id[[1]]`) or add an explicit comment naming the upstream invariant. (One reviewer rated P1; downgraded because guarded.)
- **[Line 953](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L953):** `build_annual_identifier_lookup` iterates `CONFIG$years` instead of deriving years from its `data_list` arg; a partial/test `data_list` would index a missing element and error cryptically. Mirror `append_singleton_sites` (line 1799): `years <- sort(as.integer(sub("^df", "", names(data_list))))`.
- **[Line 339](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L339) & ~799:** edge selects put the right-year `site_id` first, so `from`/`year_from`/`site_id_from` carry the *later* year in all exported edge tables. Linkage is undirected and consistent, but the labels mislead anyone reading edges as chronological. Swap the selects or canonicalise so `from` is the earlier year, and document the convention.
- **[Line 1165](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1165):** cycle-closing edges (`root_from == root_to`) are skipped silently, so kept + dropped edges do not reconcile to the candidate total. Record them under a third `drop_reason = "redundant_within_component"` bucket.
- **[Line 497](../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L497):** comment "Build escalating variable combinations (largest -> smallest)" describes the old size-descending sort; the code now sorts by evidence field count then priority score. Update the comment.

---

## Cheap safeguards worth adding

Two one-line assertions at the end of `main()` would have caught both historical bugs in
this file:

1. **Row conservation** — for each year, count of non-`NA` `site_id_<year>` in the final
   lookup equals that year's input row count.
2. **Uniqueness** — no `site_id_<year>` value appears twice in the lookup.

These are the two invariants the solution doc already lists under "Prevention".

---

## Residual risks (no action required; documented for awareness)

- `unique_id` join between 2023 and 2024 is safe today (0 contradictions across 2,187 overlapping IDs) but the guard checks only column presence, not scheme comparability. Revisit before trusting it when 2025 data arrives (pairs 2023_2025 / 2024_2025 will also satisfy the guard).
- Sites that lose all edges in conflict resolution become singletons and are then excluded from RF rescue (RF "unmatched" set is derived from the post-resolution lookup).
- Component IDs are deterministic for identical input but **not stable across data refreshes** — downstream must key on `site_id_<year>`, never `component`.
- `match_level` is not comparable across year pairs (2023-2024 has extra levels) yet acts as a tie-break in edge ordering; pair-invariant evidence scores dominate, so the effect is marginal.

---

## Findings checked and dropped (so they are not re-investigated)

- Claimed `bitwAnd`-on-named-vector indexing bug: **disproved empirically** — R logical subscripting is positional; diagnostic labels are correct.
- Claimed character-vs-integer mismatch in the RF `setdiff`, and claimed `many_to_many` row-loss: both contradicted by tracing the code (problem IDs re-enter the cascade; setdiff coercion already verified in the prior review).
- Numeric coercion in `append_singleton_sites` and lexicographic component numbering: intentional/deterministic — residual risks only, not bugs.

---

## Documentation follow-ups

- [`docs/solutions/logic-errors/annual-return-lookup-same-year-component-conflicts.md`](../docs/solutions/logic-errors/annual-return-lookup-same-year-component-conflicts.md) is accurate but does not mention that the production default of `conflict_resolution` is `"year_constrained_forest"` (not fail-closed), nor that the pre-resolution hard stop only fires in `"fail"` mode.
- Verify the `03_data_enrichment` README lists the six new conflict-audit outputs per the README convention.
- Correct archived todo 008 frontmatter: items 6 and 7 are not done.

---

## Affected files

- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R)
- [`scripts/R/02_data_cleaning/combine_annual_return_data.R`](../scripts/R/02_data_cleaning/combine_annual_return_data.R) (item 2)

## Acceptance Criteria

- [ ] TBC/"N/A" cleaning applies to all matching paths (deterministic + RF); lookup regenerated.
- [ ] 2021 `site_name_wa_sc` header variant mapped upstream; combined parquet + lookup regenerated.
- [ ] Conflict-audit outputs on disk always reflect the run that produced the current lookup.
- [ ] Post-resolution conflict stop writes (or its message correctly references) the matching audit files.
- [ ] Exported edges parquet carries full resolution provenance.
- [ ] P3 type/label/audit-reconciliation tidy-ups applied.
- [ ] Row-conservation and uniqueness assertions added to `main()`.

## Review Context

Reviewers: correctness, adversarial, data-integrity, maintainability, testing,
project-standards, learnings-researcher. Project library: R 4.6.0, dplyr 1.2.1,
reclin2 0.6.0, igraph 2.3.1, ranger 0.18.0. Run artifacts:
`/tmp/compound-engineering/ce-code-review/20260610-153727-9eece6f2/`.
