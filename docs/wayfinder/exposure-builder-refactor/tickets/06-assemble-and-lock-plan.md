---
id: 06
title: "Assemble and lock the plan document"
type: grilling
status: open
assignee:
blocked-by: [02, 03, 04, 05]
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
- the Stage-2 sample-impact checkpoint memo and sign-off;
- CONCEPTS.md and documentation updates.

Run /ce-doc-review on the draft, then walk Jacopo through it. The map is
done when he signs the plan off as locked.
