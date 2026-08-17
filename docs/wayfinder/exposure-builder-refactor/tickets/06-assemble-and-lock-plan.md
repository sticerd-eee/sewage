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
