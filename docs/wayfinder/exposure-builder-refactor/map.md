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
those layers. Execution is specified as two gated stages (pure re-layering
with frozen semantics, then the harmonized NA policy) with a sign-off
checkpoint on sample impact. The map is done when Jacopo locks the plan.

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

## Not yet specified

- Whether the EA study-period variant needs any change beyond inheriting the
  shared skeleton — sharpens once the shared-measurement-core ticket settles
  what the skeleton owns.
- How downstream analyses absorb the harmonized rule's extra NAs (which
  outputs must be regenerated, in what order, and whether any regression
  script needs a code change beyond re-running) — sharpens after the
  consumer inventory and the harmonization-boundary ticket.

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
