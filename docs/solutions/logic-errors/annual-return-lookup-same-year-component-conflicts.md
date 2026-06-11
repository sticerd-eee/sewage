---
title: Annual return lookup same-year component conflicts
module: Annual Return Lookup
date: 2026-06-09
category: logic-errors
problem_type: logic_error
component: database
symptoms:
  - "Graph components with multiple annual-return IDs for the same year were collapsed with first(site_id)."
  - "Ambiguous matched components could be under-linked while later same-year IDs were appended as singletons."
  - "The old lookup export had no conflict audit trail for same-year duplicate IDs inside matched components."
root_cause: logic_error
resolution_type: code_fix
severity: high
related_components: [annual-return-lookup, record-linkage-graph, canonical-site-mapping, create-unique-spill-sites]
tags: [annual-return, lookup, record-linkage, graph-matching, data-integrity, spanning-forest, conflict-audit, r]
---

# Annual return lookup same-year component conflicts

## Problem
`scripts/R/03_data_enrichment/create_annual_return_lookup.R` builds canonical cross-year annual-return site identifiers from pairwise match edges. The old graph collapse assumed each connected component contained at most one annual-return ID per reporting year, then reduced `(component, year)` with `first(site_id)`.

That assumption was false. Some plausible match bridges connected multiple records from the same reporting year into one component, so the lookup made an unobserved resolution choice and later singleton appending made raw ID coverage look correct.

## Symptoms
- Review of the deterministic matching graph found same-year component conflicts before lookup pivoting.
- The original review artifact recorded 113 conflicted component-years after deterministic matching and MST construction.
- After adding the final matching-priority and forest-resolution logic, the pre-resolution audit still found 96 conflicted component-years across 43 components.
- The final singleton-appended lookup could cover every raw annual-return ID exactly once while still splitting ambiguous matched IDs incorrectly.
- Example pattern: one 2024 site connected to different pre-2024 records through different high-scoring deterministic edges.

## What Didn't Work
**Checking raw annual-return coverage alone.**
Every raw yearly ID could still appear exactly once after singleton appending. That proved no rows were lost, but not that the canonical graph had resolved ambiguous matches correctly.

**Trusting pairwise one-to-one matches globally.**
Pairwise one-to-one matching does not imply globally valid connected components. Bridge edges can join two otherwise plausible site tracks and create a component with two IDs from the same reporting year.

**Using the unconstrained MST as the canonical graph.**
An MST optimizes edge strength, but it does not encode the lookup invariant: one canonical annual-return site track may contain at most one record per reporting year.

**Stopping at audit-only fail-closed behavior.**
The first fix direction wrote conflict diagnostics and stopped before refreshing canonical outputs. That was useful diagnostically, but the production pipeline needed an algorithmic resolution rather than permanent manual blocking. (session history)

**Building the constrained forest with heavy per-edge data structures.**
An early constrained-forest implementation used per-edge tibble/list bookkeeping and was too slow on the full data. The final version uses integer vertex IDs and bitmasks for year membership. (session history)

## Solution
Commit `b4493f2` keeps the old unconstrained MST only as a pre-resolution audit view, then builds the canonical lookup from a deterministic year-constrained maximum spanning forest.

The key rule is simple: before accepting an edge, check whether merging the two endpoint components would create a duplicate reporting year. Keep the edge only when the merged component still has at most one annual-return ID per year.

```r
duplicate_year_mask <- bitwAnd(
  component_year_mask[root_from],
  component_year_mask[root_to]
)

if (duplicate_year_mask != 0L) next

union_roots(root_from, root_to)
```

The implementation records skipped edges in the dropped-edge audit table before moving on.

The final implementation separates three views:

- `build_unconstrained_mst_components()` preserves the legacy graph shape for conflict audit.
- `build_year_constrained_spanning_forest()` constructs the canonical year-valid components.
- `build_lookup_conflict_audit()` records conflict summaries, annual-return identifiers, source edges, kept resolution edges, and dropped resolution edges.

The audit outputs are:

- `annual_return_lookup_conflict_summary.parquet`
- `annual_return_lookup_conflict_records.parquet`
- `annual_return_lookup_conflict_edges.parquet`
- `annual_return_lookup_resolution_kept_edges.parquet`
- `annual_return_lookup_resolution_dropped_edges.parquet`
- `annual_return_lookup_conflicts.xlsx`

Since the 2026-06-10 decomposition refactor, the post-resolution audit is
failure-gated (user-approved deviation from plan 002 R4): the
`annual_return_lookup_post_resolution_*` diagnostic set is written only at
the moment the final safety net trips - inside the same code path as the
stop, whose message names the files - and a healthy run deletes stale
post-resolution files instead of writing six empty ones. The absence of
post-resolution files is therefore the expected state after a clean run.
The same refactor made `build_lookup_from_matches()` pure on success (all
exports happen in `main()`) and moved the graph and audit machinery to
`scripts/R/utils/annual_return_lookup_{graph_utils,audit_utils}.R`.

Edge ordering is deterministic. Exact `unique_id_2023` and `unique_id` matches dominate, and non-unique evidence is ordered by field count, an explicit field-priority score, then stable endpoint and metadata tie-breakers. The field priority is:

```text
activity_reference > outlet_discharge_ngr > permit_reference_ea >
permit_reference_wa_sc > site_name_ea > site_name_wa_sc
```

`water_company` remains a blocking field rather than evidence strength. (session history)

## Why This Works
The lookup table's structural contract is not just row coverage. It is a graph invariant: each canonical component can contain zero or one annual-return record for each reporting year.

The constrained forest makes that invariant part of graph construction. Each component carries a compact year-membership bitmask. An edge is accepted only if the bitwise intersection of the endpoint components' year masks is empty. If the intersection is non-empty, the edge is not used in the canonical graph and is retained in the dropped-edge audit table.

That makes the later pivot safe:

```r
post_conflict_audit <- build_lookup_conflict_audit(
  membership_tbl = constrained_forest$membership_tbl,
  edge_metadata = edge_metadata,
  data_list = data_list
)
stop_if_lookup_conflicts(post_conflict_audit)

lookup_table <- constrained_forest$membership_tbl %>%
  group_by(component, year) %>%
  summarise(site_id = first(site_id), .groups = "drop")
```

`first(site_id)` remains in the code, but it is now protected by `stop_if_lookup_conflicts()` immediately before pivoting. If the constrained forest ever fails to maintain the invariant, the pipeline stops instead of silently choosing a site ID.

The verification run after the final fix showed:

- all six deterministic year pairs completed;
- pre-resolution audit found 96 conflicted component-years across 43 components;
- resolution dropped 98 duplicate-year edges;
- post-resolution same-year conflicts were zero;
- duplicated yearly IDs after singleton appending were zero;
- raw annual-return coverage was exact for 2021-2024.

(After the committed placeholder trim fix, the pinned baseline became 91
conflicted component-years across 41 components with 153 dropped edges;
`scripts/R/testing/test_annual_return_lookup_contracts.R` asserts these
values on every run.)

## Monitor-Granularity Limitation (2026-06-10 evaluation)
The unit of analysis is the Monitored Discharge Point (see `CONCEPTS.md`), not the wastewater works. One annual-return row is one monitored overflow asset, and a works can carry several rows that share every identifying field.

Evaluation findings on the current 2021-2024 data:

- 89 within-year identifier-duplicate groups (199 rows) exist across the four returns: 38 groups / 81 rows in 2021, 20 / 45 in 2022, 14 / 35 in 2023, 17 / 38 in 2024. These are distinct monitored discharge points of the same works, identical on water company, both site names, both permit references, activity reference, and outlet NGR.
- Worked example: Oldham WWTW carries four monitor rows in both 2023 and 2024. Each carries its own `unique_id` (UUG0690-UUG0693 in 2023; UUP01458-UUP01461 in 2024), and their spill hours range from 0 to 1,021 in the same year - the duplicates are real, behaviorally distinct monitors, not data errors.
- 2021/2022 monitor-multiples are untrackable across years by construction: those returns carry no monitor-level key, so no evidence can distinguish which of several identical rows continues which. They are correctly left unmatched, and the RF matching path flags proposals among identifier-identical groups as ambiguous instead of pairing them arbitrarily (41 of the 50 legacy-threshold RF proposals were exactly such arbitrary pairings).
- Works-level aggregation was considered and rejected: collapsing monitor rows to one works row would break alignment with the per-monitor unique-ID era (2023+) and destroy real variation (e.g. Oldham's 0 vs 1,021 spill hours).

A characterization test in `scripts/R/testing/test_annual_return_lookup_contracts.R` asserts the per-year duplicate-group counts, so a future data refresh that changes this landscape is noticed rather than silently absorbed.

## Prevention
- Test both invariants: exact raw yearly ID coverage and zero duplicate `(component, year)` memberships.
- Keep `stop_if_lookup_conflicts()` directly before lookup pivoting.
- Treat pre-resolution conflicts as audit evidence, not as canonical lookup data.
- Export both kept and dropped resolution edges whenever graph conflicts are resolved automatically.
- Preserve deterministic edge ordering; otherwise a conflict fix can become non-reproducible.
- Remember that pairwise one-to-one record linkage does not guarantee a globally valid canonical entity graph.

## Related Issues
- `todos/_archive/009-done-p1-audit-annual-return-lookup-construction.md` records the original review and acceptance criteria.
- `docs/solutions/best-practices/data-enrichment-readme-standardisation-20260310.md` covers enrichment-layer output inventory discipline; the new conflict-audit outputs are a related documentation surface.
- `docs/solutions/best-practices/annual-return-combiner-simplification-20260310.md` covers the upstream annual-return data preparation layer.
