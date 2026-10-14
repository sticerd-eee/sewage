---
id: 09
title: "Does study_period_ea need anything beyond the shared classification?"
type: grilling
status: closed
assignee: jacopo
blocked-by: [03]
---

## Question

The [shared-measurement-core resolution](03-shared-measurement-core.md)
(2026-08-17) settled what the core owns: clipping, the atomic-flag evidence
truth table with its window reducer, the per-site collapse, and the rate
helper. The EA study-period variant (`study_period_ea`, built by
`collapse_study_period_annual_returns()`) measures from Annual Returns
rather than events, so of those pieces it inherits only the evidence
classification (the truth table plus the window reducer) and the rate
helper — it has no events to clip or collapse. To settle:

1. **Stage-1 wiring.** Confirm the EA builder's `missing_evidence`
   expression is replaced by the shared window reducer over the first two
   atomic flags (`annual_returns_absent`, `annual_returns_na`), reproducing
   today's verdict exactly.
2. **Stage-2 policy.** The charter says Stage 2 moves every *event-based*
   derivation onto the three-flag OR. The EA variant's measures come from
   the Annual Returns themselves, so
   `reported_positive_without_matched_events` arguably does not indict its
   evidence. Rule whether `study_period_ea` stays on the two-flag OR
   permanently (an intentional, documented per-source verdict, consistent
   with the charter's "per-source verdicts" language) or joins the
   three-flag OR for cross-dataset consistency.
3. **Contract coverage.** The consumer inventory found the fourteen-artifact
   ID verifier does not cover `study_period_ea`. Rule whether closing that
   gap is in this plan's scope (and if so, in which stage) or is a separate
   background task.

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo. Every decision below was put
to him explicitly and confirmed.

- **Stage-1 wiring: one classification, no EA-special variant.** The EA
  builder calls the same shared truth-table function as every other dataset
  and simply ignores the third flag
  (`reported_positive_without_matched_events`), which its Annual-Returns
  measures never use. Consequence, accepted: its crosswalk read gains the
  `matched_event_count` column that the shared function requires. The
  shared window reducer over the first two flags
  (`annual_returns_absent | annual_returns_na`) replaces the inline
  `missing_evidence` expression in `study_period_annual_evidence_grid()`;
  verified against the code that this reproduces today's verdict exactly,
  including the case where a site-year row is missing from the crosswalk
  (the complete-grid join turns it into an NA status, which the
  `annual_returns_absent` flag covers by definition). A slimmed-down
  EA-only classification was rejected as exactly the duplication this
  refactor exists to eliminate.
- **Stage-1 wiring: the EA sums join the stable-sum discipline.** The EA
  collapse's two plain `base::sum()` calls over `spill_count_ea` and
  `spill_hrs_ea` move onto the stable-summation wrappers, with the row
  order already fixed by the existing `order(site_id, year)`. This applies
  the ticket 03 core-wide invariant with no exceptions; any low-order
  float shifts sit far inside the charter tolerance.
- **Stage-2 policy: the two-flag verdict is locked permanently.** The
  charter's per-source ruling is confirmed, not reopened:
  `study_period_ea` goes unknown on `annual_returns_absent` or
  `annual_returns_na` only, forever. The third flag indicts the event
  matching, not the Annual Returns the EA measures are read from, so
  applying it would discard valid EA observations for no measurement gain;
  cross-source comparability is handled in quantified form by
  `verify_study_period_exposure_sources.R` rather than by forcing
  identical missingness. The plan must state explicitly that ticket 08's
  "no per-variant opt-out" rule governs derivations from the event-based
  measurement tables, while `study_period_ea` is not an opt-out but a
  different data source with its own verdict computed from the same shared
  classification — so the two sentences cannot be read as contradicting
  each other.
- **Contract coverage: in scope, closed before Stage 1 runs.** The two
  `study_period_ea` entries (sales and rentals, mirroring the
  `study_period` specs) are added to
  `verify_id_artifact_match_rates.R` inside this plan, before the Stage-1
  reconciliation, so both the baseline and the rebuilt datasets carry
  verified transaction identifiers on the keys the reconciliation matches
  on. This keeps ticket 05's "passing contract suite" boundary true for
  all six datasets; a separate background task for a two-line change was
  rejected as pure coordination overhead.
