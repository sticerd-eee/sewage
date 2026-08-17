---
id: 08
title: "Variant model: directional, nearest-site, and window variants on the layered architecture"
type: grilling
status: closed
assignee: jacopo
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

## Resolution (2026-08-17)

Settled in a live grilling session with Jacopo. Every decision below was put
to him explicitly and confirmed.

- **Two kinds of variant, attaching at different layers.** Direction,
  nearest-site restriction, and any per-pair weighting are **derivations**
  from `house_site_spills` / `rental_site_spills`: they select, split, or
  weight pair rows that already exist in the measurement table, then apply
  the shared reduction. Window choice is a **measurement-layer parameter**:
  the clipped hours stored on each pair row were computed for one specific
  window and cannot be re-windowed downstream, so a new window is a new run
  of the table-building step through the core's window arguments (ticket
  03), never a derivation. Each new variant is one short, named function in
  the derivation layer — no registry, no configuration framework, no
  central switch matrix, consistent with the amended R7/R8. The plan states
  the two-kind split explicitly so nobody later bolts a window change on as
  a derivation or rebuilds the measurement table just to get a directional
  split.
- **Directional attributes stay in their own artifact.** The measurement
  table keeps the ticket 02 schema unchanged; direction and river distance
  remain in the river-network pair table
  (`upstream_downstream/output/03-02/river_filter/spill_house_signed_with_lateral`
  and its rental sibling). The directional variant's definition *includes
  the join*: join the measurement table to the signed-pair table on
  (transaction, site), apply the lateral- and river-distance eligibility
  filters, split by direction, optionally weight by inverse river distance,
  then the shared collapse. The plan names the river artifact as the
  variant's declared input. This avoids making every prior-family rebuild
  depend on the river-network pipeline and avoids majority-NA directional
  columns on the pair table.
- **The missing-evidence rule is baked into the shared reduction.** The
  evidence mask and NA-poisoning are part of the fixed reduction slot every
  variant uses, not per-variant choices: a directional or nearest-site
  total is NA whenever a contributing site has unknown evidence, exactly
  like the radius sums. This makes the drift map's twelve-fold
  `na.rm = TRUE` inversion structurally impossible to reproduce by
  accident. An analysis that genuinely wants "treat unknown as zero" must
  write that choice as an explicit, named step in its own script — never
  silently via `na.rm = TRUE` on masked values.
- **The upstream/downstream family gets a demonstrated derivation, not a
  published one.** The directional derivation is the variant model's worked
  example in the plan: fully specified — inputs, slots, output grain (one
  row per property with upstream and downstream totals for spill hours and
  spill counts, unweighted and inverse-river-distance-weighted, plus the
  evidence-mask columns), and NA semantics — but not built, published, or
  added to the enumerated public list inside this plan. The deferred
  follow-up that fixes the twelve `upstream_downstream_*` scripts inherits
  the worked example as its spec and becomes a consumer swap plus one
  publication.
- **The one-site restriction is a sample filter, not an exposure variant.**
  Nearest-Site Exposure is an exposure definition (pair selection: keep the
  minimum-distance row per transaction) and the model expresses it
  directly. The *one-site* sample (properties with exactly one site within
  the radius) is a filter on transactions, owned by the consuming analysis;
  the model's contribution is that the per-transaction site count within a
  radius is a one-line derivation from the measurement table, noted as
  expressible with no new dataset defined for it.
- **Acceptance test: a slot table in the plan.** One row per exposure
  definition, one column per slot — measurement window, pair selection,
  per-pair weight, reduction grouping — plus a status column marking each
  row **published** (entry on the enumerated list with a hand-written
  schema) or **expressible** (demonstrated only). Rows: the four current
  prior-family outputs and the study-period family as degenerate cases of
  the same model, then directional unweighted, directional weighted, and
  nearest-site. The model passes only if every row fills its slots with no
  footnotes; any needed footnote is a design failure to fix before the
  plan locks.
- **No schema text for unpublished variants.** The plan describes the
  worked example's rows, columns, inputs, and NA behavior in plain words;
  the hand-written Arrow schema is written by the future effort at the
  moment it actually publishes the dataset — when a schema starts doing
  its job of catching drift — keeping the plan's schema inventory
  identical to its enumerated public list.
