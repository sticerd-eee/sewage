---
id: 00
title: "Charter grilling: destination and strategic frame"
type: grilling
status: closed
assignee: jacopo
blocked-by: []
---

## Question

What is this effort finding its way to, and what strategic decisions frame
every downstream design ticket for the layered exposure-builder refactor?

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo. Every decision below was put
to him explicitly and confirmed.

- **Destination.** An implementation-ready plan document in `docs/plans/`
  (ce-unified-plan style, like the finding-11 plan), signed off by Jacopo.
  The refactor itself executes later, in its own sessions, when these
  builders next need real surgery.
- **Bundling.** The plan covers the re-layering refactor *and* the
  harmonization of NA treatment, executed as two gated stages inside one
  plan: Stage 1 re-layers everything with semantics frozen (any output
  difference is a refactor bug); Stage 2 flips the harmonized NA policy
  (every difference must be attributable to the rule). Each stage has its
  own verification contract.
- **Scope.** The prior-exposure family (`prior_to_sale`, `prior_to_rental`,
  `prior_to_sale_house_site`, `prior_to_rental_rental_site`) gets the full
  layered treatment. The study-period family gets a shared measurement core
  (one implementation of event clipping, the 12/24 count, and the evidence
  classification, used by both engines) — its builders are already
  internally layered. The EA variant (`study_period_ea`) is touched only
  where it shares the study-period skeleton.
- **Architecture.** A new *internal* unmasked artifact at
  transaction–Site Group grain — `house_site_spills` (sales) and
  `rental_site_spills` (rentals) — carries the raw clipped-event measures
  and the evidence verdicts. Both published prior-family datasets become
  thin, validated derivations from it (the site-grain dataset applies its
  mask; the radius dataset aggregates and applies its mask). Public schemas
  stay byte-compatible; no consumer changes in Stage 1. Chosen over
  publishing evidence flags on the public site-grain datasets (breaking for
  consumers) and over widening public schemas with parallel raw columns.
- **Harmonized NA rule.** One shared classification computes two verdicts
  per Site Group-year: *event evidence unknown* when Annual Status is
  `reported_na` or `absent`, or `reported_positive` with zero matched
  events; *EA evidence unknown* when Annual Status is `reported_na` or
  `absent`. Every event-based dataset (all four prior datasets and
  `study_period`) masks on the event verdict; `study_period_ea` masks on
  the EA verdict. Consequence, accepted explicitly: `study_period` and
  `study_period_ea` diverge in missingness exactly at unverifiable
  positives; the source-comparison script quantifies where. Rejected
  alternatives: masking on `reported_na`/`absent` only everywhere (reopens
  the finding-11 understatement), and applying the strictest rule to the EA
  variant too (throws away valid EA observations for row-level
  comparability).
- **`annual_returns_na_then_absent`.** The column stays in the published
  radius schemas with unchanged meaning; its manual-exclusion role in the
  hedonic scripts is subsumed by the harmonized mask. It may be deprecated
  in documentation and removed in a later schema revision.
- **Sequencing.** Branch `jo/cross-section-individual-edm` (event-based
  study-period split) lands before the plan locks; the plan is written
  against the post-merge state.
- **Reconciliation bar.** Exact match on keys, integer columns, NA
  patterns, and flags; float measures within a small documented tolerance
  (order 1e-9 relative), since re-layering may change summation order and
  low-order float bits. Bit-identity was rejected as it would freeze
  incidental accumulation order into the new code.
- **Sample-impact checkpoint.** After Stage 2's dry run, a quantified
  per-dataset memo (newly missing rows and affected transactions) lands in
  `docs/reports/`; Jacopo signs it off before the new generations are
  accepted as canonical.
- **Naming.** `house_site_spills` / `rental_site_spills`, chosen over
  "Exposure Ledger", "Site Exposure Measurements", and
  `prior_to_sale_house_site_unmasked` as the plainest name. CONCEPTS.md
  gets one line: the pre-masking transaction–Site Group record of clipped
  event measures and evidence verdicts from which the published
  prior-exposure datasets are derived.
- **Tracker.** Local markdown under `docs/wayfinder/exposure-builder-refactor/`,
  following the ideation-funnel-v2 conventions; nothing on GitHub issues
  because the repo is public.
