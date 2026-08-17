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
2. **Evidence classification.** Where the single classification lives —
   extending `derive_site_group_prefix_missing_flags()` in
   `site_group_utils.R`, the branch's `study_period_annual_evidence_grid()`,
   or a new shared function both call — and how the prefix (per-cutoff) and
   fixed-window uses share it. Reshaped by the
   [ticket 02 resolution](02-house-site-spills-design.md) (2026-08-17): the
   classification's output is the **atomic per-condition flags**
   (`annual_returns_absent`, `annual_returns_na`,
   `reported_positive_without_matched_events`), not composite verdicts —
   the event and EA verdicts are ORs the derivations compute. Decide here
   how the classification exposes those atomics for both the per-cutoff and
   fixed-window cases.
3. **Counting.** Confirm `count_spills()` stays the single 12/24
   implementation and whether the per-site collapse (order, then count, then
   sum hours) also becomes one shared function.
4. **Boundaries.** What explicitly does *not* move (chunking, streaming,
   schemas, publication), so the core stays a measurement library rather
   than a third engine.
5. **Divergence adjudication at the boundary.** The drift map
   ([assets/02-drift-map.md](../assets/02-drift-map.md), item 1) records
   machinery differences between the two engines beyond clipping and
   classification: the study-period engine re-sums from scratch per radius
   with plain `base::sum` while the prior engine uses distance-ordered
   cumulative sums with stable-sum wrappers and explicit ordering; the two
   differ on defensive `min(distance_m)` dedupe versus validated pair
   uniqueness. For each, rule whether it moves into the core, stays
   per-engine as an intentional difference, or is out of scope — and in
   particular whether the stable-sum discipline becomes a core-wide
   invariant.
