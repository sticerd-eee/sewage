---
id: 03
title: "Design the shared measurement core"
type: grilling
status: open
assignee:
blocked-by: [01]
---

## Question

What exactly moves into the shared measurement core that both engines
(`prior_exposure_utils.R` and `cross_section_study_period_utils.R`) consume?
To settle:

1. **Clipping.** One implementation of the window overlap filter and
   clamping (`start_time < window_end & end_time >= window_start`, `pmax`/
   `pmin`, drop nonpositive clipped durations), parameterized by window,
   replacing the two current mirrored implementations.
2. **Evidence classification.** Where the single classification producing
   the two per-source verdicts lives — extending
   `derive_site_group_prefix_missing_flags()` in `site_group_utils.R`, the
   branch's `study_period_annual_evidence_grid()`, or a new shared function
   both call — and how the prefix (per-cutoff) and fixed-window uses share
   it.
3. **Counting.** Confirm `count_spills()` stays the single 12/24
   implementation and whether the per-site collapse (order, then count, then
   sum hours) also becomes one shared function.
4. **Boundaries.** What explicitly does *not* move (chunking, streaming,
   schemas, publication), so the core stays a measurement library rather
   than a third engine.
