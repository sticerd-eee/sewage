---
id: 08
title: "Variant model: directional, nearest-site, and window variants on the layered architecture"
type: grilling
status: open
assignee:
blocked-by: [02, 03]
---

## Question

How does the layered architecture express future exposure variants —
Directional Spill Exposure (upstream/downstream split, with and without
inverse-river-distance weighting), Nearest-Site Exposure (with its one-site
sample restriction), and window choice — so that adding one is a small
variant definition plus one literal schema, rather than edits across a
closed switch matrix?

Standing constraints, confirmed with Jacopo on 2026-08-17: extensibility
lives in the computation layer only. Every public output keeps a
hand-written Arrow schema and an explicit entry in an enumerated list, so
publication stays a closed, reviewable contract; this amends, rather than
repeals, requirements R7/R8 of
`docs/plans/2026-08-13-1322-refactor-prior-exposure-shared-builders-plan.md`
(the no-framework rule). Design for these named axes only; do not design
for hypothetical markets beyond sale and rental.

To settle:

1. **Where variants attach.** Whether a variant is a derivation from
   `house_site_spills` / `rental_site_spills` (ticket 02) — which already
   carry per-site rows with distance, so nearest-site restriction and
   directional joins are downstream selections — or a parameterization of
   the shared measurement core (ticket 03), or both, and what a variant
   definition concretely contains.
2. **The acceptance test.** The model must express, without special-casing,
   the four current prior-family outputs, the directional variants, the
   nearest-site variant, and the study-period window, while making clear
   which are published outputs versus merely expressible.
3. **What the upstream/downstream family adopts.** The drift map
   ([assets/02-drift-map.md](../assets/02-drift-map.md), item 5) shows
   twelve scripts re-reducing site-grain output with `na.rm = TRUE` because
   the directional split cannot be done post-reduction. Decide whether the
   variant model gives them a first-class directional derivation to consume
   (removing the twelve-fold duplication and the NA inversion at the
   source), or only rules on the convention they must follow.
4. **Plan placement.** Whether the locked plan includes concrete schema
   definitions for any new variant outputs, or only demonstrates the model
   and defers each variant to its own future effort.
