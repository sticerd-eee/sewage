---
id: 03
title: "Design the shared measurement core"
type: grilling
status: closed
assignee: jacopo
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

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo. Every decision below was put
to him explicitly and confirmed.

- **The core has no new file.** The shared measurement core is a named set of
  functions across the two existing utility files, not a third file: the
  spill arithmetic (the shared clip, the shared per-site collapse, and the
  existing `count_spills()`) lives in `spill_aggregation_utils.R`, and the
  evidence classification lives in `site_group_utils.R`. A third utility file
  with its own partial view of measurement is exactly the third-engine
  failure mode this ticket exists to prevent; the plan names the core as a
  concept — a function list with stated invariants.
- **One shared clip function.** A single event-clipping function takes the
  event table plus `window_start` and `window_end` arguments accepting
  scalars or per-row vectors; the prior engine passes per-row transaction
  endpoints, the study engine passes its two constants. The
  `event_hours > 0` filter lives inside it. The arithmetic is already
  identical in both engines today (overlap filter, `pmax`/`pmin` clamping,
  hours via `difftime`), so the prior engine is unchanged bit for bit and
  Stage-1 reconciliation proves the no-op.
- **One evidence truth table, two reducers.** A new shared function takes
  crosswalk rows and returns, per site-year, the three atomic condition
  flags from the ticket 02 resolution: `annual_returns_absent` (status
  `absent` or the site-year row missing entirely), `annual_returns_na`
  (status `reported_na`), and `reported_positive_without_matched_events`
  (status `reported_positive` with `matched_event_count == 0`). On top sit
  two thin reducers: a prefix reducer for the prior family (cumulative `any`
  up to each cutoff year; `derive_site_group_prefix_missing_flags()` becomes
  a wrapper preserving its interface) and a window reducer for the study
  family (`any` over the fixed window years, replacing the
  `missing_evidence` expression inside
  `study_period_annual_evidence_grid()`). `annual_returns_na_then_absent`
  stays computed in the prefix reducer only — it is inherently sequential
  and the fixed-window family does not use it. Consequence: the study
  builder starts reading `matched_event_count` from the crosswalk; in
  Stage 1 its verdict still ORs only the first two flags (output unchanged),
  and Stage 2 flips it to the three-flag OR as the charter specifies.
- **One shared per-site collapse; `count_spills()` confirmed single.** The
  collapse (order the clipped events, then per group compute
  `count_spills(clipped_start, clipped_end)` and summed hours) becomes one
  shared function parameterized by grouping keys — (transaction, site) for
  the prior engine, site alone for the study engine — with the explicit
  `setorderv` and the stable-sum wrappers inside. The study engine's site
  totals may shift in the lowest-order floating bits because its summation
  order changes; the charter's float tolerance absorbs this.
  `count_spills()` remains the single 12/24 implementation; no competitor
  was found.
- **Radius reductions stay per-engine; the stable-sum discipline goes
  core-wide.** The prior engine keeps its distance-ordered cumulative
  reduction (already the efficient one-pass approach on the heavy path) and
  the study engine keeps its per-radius re-summing; merging them would be
  the largest rewrite in the refactor for zero numerical gain, so the
  difference is documented in the plan as intentional, with the stated cost
  that any future change to radius semantics must be made twice. What does
  become a core-wide invariant is the discipline: a fixed row order before
  every grouped reduction and every sum or cumulative sum routed through
  the stable wrappers, adopted by the study engine as a two-line change so
  both engines' published floats are reproducible bit for bit between runs.
- **Validated pair uniqueness replaces the silent dedupe.** The prior
  engine's defensive `min(distance_m)` collapse of duplicate
  transaction-site pairs is retired in favor of the study engine's stance:
  uniqueness is asserted loudly. Empirical check (2026-08-17, DuckDB full
  group-by): zero duplicate pairs in both lookups — 272,728,062 rows in
  `spill_house_lookup.parquet` and 111,923,121 rows in
  `spill_rental_lookup.parquet`, both overall and within the 1000 m engine
  radius — so the dedupe never fires today and the assertion is a free
  strengthening. Per the validation-placement decision below, the check
  lives in the publication gate (which already checks duplicate public
  keys), not in the measurement code.
- **The exclusion list.** The core deliberately excludes, and each engine
  keeps its own: chunking, streaming, and publication machinery;
  hand-written public schemas and projections; input validation of
  transaction ledgers and lookups; radius-reduction machinery; and
  eligibility rules (the 30-complete-days rule on the prior side,
  coordinate eligibility on the study side). The core is exactly: clipping,
  evidence classification, per-site collapse — plus one tiny shared rate
  helper, since the daily and weekly average formulas
  (`total / n_days_in_window`, times seven for weekly) are duplicated
  verbatim in both engines today; moving them changes no floats and closes
  the last place the identically-named published columns could drift in
  definition.
- **Validation lives in three layers.** (1) The measurement-core functions
  carry no validation at all — clean pure arithmetic, with correctness
  proven by unit tests and the one-off Stage-1 reconciliation, which stays
  the full exact comparison the charter locked. (2) The existing
  publication gate (schema match, row counts, duplicate keys before atomic
  promotion) is the single runtime check, and is where the uniqueness
  assertion lives. (3) The existing defensive checks inside the two engines
  are left alone in this refactor; the no-validation principle applies to
  the core and to new code, and any cleanup of old checks rides a later
  effort. This implements Jacopo's standing preference: test that the code
  works, then keep the code clean.
