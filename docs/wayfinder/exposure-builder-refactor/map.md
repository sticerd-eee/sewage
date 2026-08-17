---
label: wayfinder:map
title: "Exposure Builder Refactor — locked plan"
created: 2026-08-17
tickets: tickets/
---

# Exposure Builder Refactor — locked plan

## Destination

An implementation-ready, Jacopo-signed plan document in `docs/plans/` for the
layered exposure refactor plus NA-rule harmonization: `house_site_spills` /
`rental_site_spills` become the single unmasked measurement layer for the
prior-exposure family, a shared measurement core serves both the prior and
study-period engines, and every published exposure dataset is derived from
those layers. The layered architecture must also express the named future
variants — Directional Spill Exposure, Nearest-Site Exposure, and window
choice — as cheap derivations (ticket 08), without reopening the closed
literal publication contracts. Execution is specified as two gated stages
(pure re-layering with frozen semantics, then the harmonized NA policy)
with a sign-off checkpoint on sample impact. The map is done when Jacopo
locks the plan.

## Notes

- Domain: sewage-spills paper (Balboni & Dhingra coauthors). The datasets in
  scope: `prior_to_sale`, `prior_to_rental`, `prior_to_sale_house_site`,
  `prior_to_rental_rental_site` (engine: `scripts/R/utils/prior_exposure_utils.R`)
  and `study_period`, `study_period_ea`
  (engine: `scripts/R/utils/cross_section_study_period_utils.R`, as revised on
  branch `jo/cross-section-individual-edm`).
- Key references: `docs/plans/2026-08-12-002-fix-prior-exposure-evidence-publication-plan.md`
  (the finding-11 fix this effort partially revisits),
  `todos/2026-07-07-review-prior-to-sale-rental-spill-scripts.md` (defect
  evidence), `CONCEPTS.md` (Annual Status vocabulary).
- Tracker: local markdown. Tickets live in `tickets/NN-slug.md` with
  frontmatter (`id`, `title`, `type`, `status`, `assignee`, `blocked-by`). A
  ticket is claimed when `assignee` is set; the frontier is any open ticket
  with empty `assignee` whose `blocked-by` ids are all closed. Assets created
  while resolving a ticket go in `assets/`, linked from the ticket.
- HITL tickets run through /grilling and /domain-modeling, one round at a
  time, recommended answer attached.
- Standing writing requirement: complete plain sentences in anything Jacopo
  or coauthors read; no shorthand, no arrow chains.
- Repo is public: planning stays in this local tracker, never on GitHub
  issues.
- Plan, don't do: this map produces decisions and the locked plan document.
  The refactor itself executes later, outside this map.
- Confirmed with Jacopo in a second charting session (2026-08-17 afternoon):
  extensibility lives in the computation layer only, and every public output
  keeps a hand-written Arrow schema plus an explicit entry in an enumerated
  list — an amendment, not a repeal, of R7/R8 of
  `docs/plans/2026-08-13-1322-refactor-prior-exposure-shared-builders-plan.md`.
  Variant design covers the named axes only (directional, nearest-site,
  window), not hypothetical markets beyond sale and rental. He also accepted
  a looser 1e-6 float tolerance for outputs whose methodology deliberately
  changes; the Stage-1 re-layering bar stays at the charter's order-1e-9
  relative tolerance, which satisfies the looser figure.

## Decisions so far

- [Charter grilling: destination and strategic frame](tickets/00-charter-grilling.md) —
  destination is a locked plan document; the refactor bundles NA-rule
  harmonization, executed as two gated stages; scope is the prior family
  (full layered treatment) plus the study-period family (shared measurement
  core), with the EA variant touched only through the shared skeleton; the
  measurement layer is an internal unmasked artifact named
  `house_site_spills` / `rental_site_spills`; the harmonized rule is one
  shared evidence classification with per-source verdicts; the
  `annual_returns_na_then_absent` column stays; the event-based study-period
  branch lands before the plan locks; reconciliation is exact on keys,
  integers, NA patterns, and flags, tolerance-based on floats.
- [Consumer and contract inventory for the six exposure datasets](tickets/01-consumer-and-contract-inventory.md) —
  seventeen consumers read `prior_to_sale`, sixteen read `prior_to_rental`,
  nineteen live plus nine archived read each site-grain dataset, and
  `study_period` has one analysis consumer; no live consumer reads the masking
  flags directly (the NA policy propagates through dropped NA averages, and
  the only manual `annual_returns_na_then_absent` exclusion is the branch
  version of `hedonic_continuous_prior.R`), so the compatibility surface is
  the literal schemas, the key grains, and the NA patterns pinned by the two
  contract tests and the fourteen-artifact ID verifier, which does not yet
  cover `study_period_ea`.
- [Design house_site_spills / rental_site_spills](tickets/02-house-site-spills-design.md) —
  one row per eligible transaction × nearby Site Group within the maximum
  radius, no radius column and no transaction metadata; evidence travels as
  four atomic flags (`annual_returns_absent`, `annual_returns_na`,
  `reported_positive_without_matched_events`,
  `annual_returns_na_then_absent`) with no stored verdict — the event and EA
  masks are ORs computed in the derivations, and the site-grain derivation
  renames `annual_returns_absent` back to `site_missing` for the frozen
  public schema; real pairs only with the radius derivation rejoining the
  transaction ledger; published through the full staged machinery to
  `data/processed/cross_section/{sales,rentals}/` as unpartitioned chunked
  parquet, off the public list but schema-pinned in the contract tests;
  prior-family windows only.
- [Drift map: independent exposure aggregations across the repo](tickets/07-drift-map.md) —
  beyond the two engines, four scripts re-implement study-period summation
  over `agg_spill_yr.parquet` with inconsistent NA conventions, the
  twelve-script `upstream_downstream_*` family re-reduces site-grain output
  with `na.rm = TRUE` (inverting the engine's NA-poisoning), and
  `repeat_sales.R` carries a fifth windowing scheme; the panel and grid
  builders and the `*_prior*` analysis families are pure consumers. Full
  findings in the asset linked from the ticket.

## Not yet specified

- Whether the EA study-period variant needs any change beyond inheriting the
  shared skeleton — sharpens once the shared-measurement-core ticket settles
  what the skeleton owns.
- How downstream analyses absorb the harmonized rule's extra NAs (which
  outputs must be regenerated, in what order, and whether any regression
  script needs a code change beyond re-running) — sharpens after the
  consumer inventory and the harmonization-boundary ticket.
- Whether identically-named average columns (`spill_count_weekly_avg` and
  friends) across the prior-family and study-period datasets need renaming
  or only documentation, given they differ in window and denominator —
  sharpens once the event-based study-period branch lands (ticket 04),
  which removes the measurement-basis half of the collision.

## Out of scope

- The panel and grid exposure datasets (`sale_panel_exp`, `rental_panel_exp`,
  `house_panel_within_radius`, `rental_panel_within_radius`, grid long
  differences, site panels): they measure exposure with separate code, but
  the destination covers only the six cross-section datasets. Extending the
  measurement layer to them would be a fresh effort with its own map.
- Executing the refactor: the destination is the locked plan; execution rides
  the next real surgery on these builders.
- Upstream semantics: Annual Status definitions, the crosswalk build, and
  event matching stay as they are; only the exposure builders' use of them is
  in scope.
- Fixing the analysis-layer re-aggregations the drift map found
  (`grid_long_difference_*`, `hedonic_*_full`, `did_trends_full`,
  `repeat_sales.R`, and the twelve `upstream_downstream_*` scripts): ticket
  05 rules on the canonical convention and on which of them the plan's
  boundary names, but the code fixes are a separate effort. The
  `hedonic_continuous_full.R` hard-coded 1095-day denominator is flagged as
  an independent background task outside this map.
