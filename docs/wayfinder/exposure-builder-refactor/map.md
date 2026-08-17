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
  (engine: `scripts/R/utils/cross_section_study_period_utils.R`, now on main
  since pull request #34 merged on 2026-08-17).
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
- [Design the shared measurement core](tickets/03-shared-measurement-core.md) —
  the core is a named set of functions in the two existing utility files
  (spill arithmetic in `spill_aggregation_utils.R`, evidence classification
  in `site_group_utils.R`), never a third file: one shared clip
  parameterized by scalar-or-vector window bounds, one atomic-flag evidence
  truth table with a prefix reducer (prior family) and a window reducer
  (study family), one shared per-site collapse parameterized by grouping
  keys, and one shared rate helper; radius reductions stay per-engine as an
  intentional documented difference while the stable-sum discipline goes
  core-wide; validated pair uniqueness replaces the silent `min(distance_m)`
  dedupe (empirically zero duplicates in both lookups); core functions carry
  no validation — correctness lives in tests and Stage-1 reconciliation,
  and the publication gate stays the single runtime check.
- [Land the event-based study-period branch](tickets/04-land-event-based-study-period-branch.md) —
  merged to main as pull request #34 (merge commit `b9f8203`, 2026-08-17);
  one review commit (`11c1687`) tightened the event collapse so unexpected
  NA totals fail loudly instead of becoming zeros and dropped the unused
  `window` parameter from `study_period_read_events()`; all four
  study-period datasets regenerated from the merged code the same
  afternoon.
- [Where does the plan's responsibility for downstream analyses end?](tickets/05-harmonization-downstream-boundary.md) —
  the boundary is narrow: the plan ends at the rebuilt six datasets and a
  passing contract suite; the only in-scope analysis code change is
  removing the redundant manual `annual_returns_na_then_absent` exclusion
  from `hedonic_continuous_prior.R` under a contract assertion; the
  Stage-2 sign-off memo is a self-contained `.qmd` in `docs/reports/` with
  sample accounting plus before-and-after estimates of the preferred
  250-metre hedonic; the NA-propagation convention goes to `CONCEPTS.md`
  with the named offender list in the plan and a `todos/` follow-up; the
  deferred work splits into two named follow-ups (battery refresh, then
  NA-convention cleanup).

- [Variant model: directional, nearest-site, and window variants on the layered architecture](tickets/08-variant-model.md) —
  two kinds of variant: selection, direction, and weighting are named
  derivation functions over `house_site_spills` (the directional one joins
  the river-network pair table at derivation time), while window choice is
  a re-run of the measurement layer through the core's window arguments;
  the missing-evidence NA rule is baked into the shared reduction with no
  per-variant opt-out; the directional derivation is the plan's fully
  specified but unpublished worked example, inherited as the spec by the
  follow-up that fixes the twelve `upstream_downstream_*` scripts; the
  one-site restriction is a consuming-analysis sample filter; the
  acceptance test is a slot table over all current and named future
  variants with a published-versus-expressible status column; no Arrow
  schema text for unpublished variants.
- [Does study_period_ea need anything beyond the shared classification?](tickets/09-ea-variant-inheritance.md) —
  the EA builder calls the one shared truth table (ignoring the
  event-matching flag) with the window reducer over the first two atomic
  flags, and its sums adopt the stable-sum wrappers; the two-flag verdict
  is locked permanently as a per-source verdict, with the plan stating
  explicitly that ticket 08's no-opt-out rule governs event-based
  derivations so the two rulings cannot be misread as contradicting; the
  `study_period_ea` ID-verifier gap is closed inside the plan, before
  Stage 1 runs.
- [Do the identically-named average columns need renaming or only documentation?](tickets/10-average-column-naming.md) —
  no rename in either family: the four average columns are conceptually one
  variable over the dataset's own stated window, with `n_days_in_window`
  self-describing every row; the collision is documented in `CONCEPTS.md`
  (locked wording in the ticket, including the rename-on-join instruction)
  and in a named subsection of the plan document; Arrow field metadata was
  rejected as a schema reopening.

## Not yet specified

Nothing remains in the fog: every decision ticket is closed, and the only
open ticket is the plan assembly (06), now unblocked.

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
