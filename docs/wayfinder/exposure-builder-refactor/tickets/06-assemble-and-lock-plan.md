---
id: 06
title: "Assemble and lock the plan document"
type: grilling
status: open
assignee: jacopo
blocked-by: [02, 03, 04, 05, 08, 09, 10]
---

## Question

Assemble the implementation-ready plan document in `docs/plans/`
(ce-unified-plan style: goal capsule, requirements, key technical decisions,
acceptance examples, implementation units, verification contract, definition
of done), incorporating every decision on this map:

- the two-stage execution with per-stage verification contracts
  (Stage 1: re-layering with frozen semantics, reconciliation exact on
  keys, integers, NA patterns, and flags, floats within the documented
  tolerance; Stage 2: harmonized NA policy, every difference attributable
  to the rule);
- the `house_site_spills` / `rental_site_spills` design from ticket 02;
- the shared measurement core design from ticket 03;
- the downstream boundary from ticket 05;
- the variant model from ticket 08, including the closed-publication
  principle (extensibility in computation only; every public output keeps a
  hand-written schema and an enumerated entry) stated as an explicit
  amendment of R7/R8 of
  `docs/plans/2026-08-13-1322-refactor-prior-exposure-shared-builders-plan.md`;
- the Stage-2 sample-impact checkpoint memo and sign-off;
- CONCEPTS.md and documentation updates.

Run /ce-doc-review on the draft, then walk Jacopo through it. The map is
done when he signs the plan off as locked.

## Progress (2026-08-17)

The plan is drafted and reviewed; only Jacopo's sign-off remains.

- Draft assembled from all nine closed tickets and both assets:
  [docs/plans/2026-08-17-002-refactor-layered-exposure-builders-plan.md](../../../plans/2026-08-17-002-refactor-layered-exposure-builders-plan.md).
- /ce-doc-review ran with coherence, feasibility, and scope-guardian
  reviewers. Coherence and scope-guardian returned zero findings.
  Feasibility surfaced five: two citation fixes applied automatically
  (the `repeat_sales.R` offender path, the 2026-08-17-001 plan filename),
  and two fixes Jacopo approved in the walk-through routing — the
  `annual_returns_na_then_absent` definition in R3/A5 corrected to the
  code's sequence semantics, and U5 extended so the study engine's
  per-radius re-sum explicitly adopts the stable-sum wrappers. One FYI
  (the "eligible-transaction ledger" is an in-memory table, not a
  persisted artifact) and two deferred questions (memo template
  provenance; the truth table's universe-expansion seam, to settle at U2
  review) are recorded in the review report.
- The ticket closes when Jacopo signs the plan off as locked; at that
  point the sign-off is recorded here and on the map.
