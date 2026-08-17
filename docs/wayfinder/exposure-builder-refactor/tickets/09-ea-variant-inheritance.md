---
id: 09
title: "Does study_period_ea need anything beyond the shared classification?"
type: grilling
status: open
assignee:
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
