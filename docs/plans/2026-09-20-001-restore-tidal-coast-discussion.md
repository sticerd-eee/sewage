---
title: Restore tidal-coast salience — interview record
date: 2026-09-20
status: complete
---

# Objective

Restore the analysis and report to their pre-open-coast-plan state, retaining the
new original-classification map with London Coastal CSOs shown separately.
The user requested a grilling session followed by a written plan. This record
does not authorize implementing the rollback during the planning session.

## User's rationale

The intended mechanism is recreational salience: people near substantial water
may use and value it recreationally and therefore care more about sewage.
The official open-coast versus transitional-water distinction does not directly
capture that mechanism. The user prefers the original tidal-coast proxy and
does not want open-coast versus estuarine regressions as a substitute question.
Neither shoreline proximity nor bathing association directly measures individual
recreational behaviour.

## Verified starting point

- Pre-implementation checkpoint: `c4eba6e5cf952b7d4c0accf76556f359eae32615`.
- Implementation commit: `a62a02a`; subsequent map/report edits are uncommitted.
- The preservation manifest names the same pre-implementation revision.
- All inventoried copies are present: 11 historical model bundles, 89 exports,
  and eight pre-refinement data files. Hash verification remains an execution gate.
- `data/` and `output/` link to shared Dropbox storage. Git rollback alone cannot
  restore those products; restoration must precede deletion of recovery copies.
- The original report supported saved-result rendering but defaulted to
  `reestimate: true`.
- The retained map uses the original inclusive 2 km rule: 2,917 mapped Coastal
  Site Groups outside Greater London and 83 inside it. Its London category uses
  CSO location; the original regression exclusion uses property region.
- The current map builder depends on the new open-coast helpers/reference even
  in legacy mode. Retaining the map requires removing those dependencies.

## Settled decisions

The user confirmed the recommendations in the first round, with one correction:
no separate short decision note or new ADR is wanted.

1. Restore the original analysis and outputs. Remove the experimental open-coast
   code, downloaded inputs, revised datasets/results and diagnostic maps after
   restoration is verified. Keep earlier planning documents as superseded
   history. Recovery copies must survive until all acceptance checks pass.
2. Load restored saved results by default through the old report's existing
   `reestimate` parameter. Explicit re-estimation remains supported; the new
   storage/generation framework is not retained.
3. Restore original analyses and figures while keeping accurate explanations of
   tidal-shoreline proximity as a recreational-salience proxy and of bathing
   evidence as reported association. Remove the open-coast analyses and comparisons.
4. Retain the latest original-classification map, including the London category
   and the distinction between site geography and property-level London exclusion.

There are no outstanding design decisions. The user confirmed the proposed scope;
implementation remains a later task. The settled meanings of Site Coast Distance
and Local Salience have been recorded in the existing `CONCEPTS.md`, preserving
the user's other glossary edits. No new ADR or duplicate glossary was created.

The executable plan is [Restore original tidal-coast salience](2026-09-20-002-restore-tidal-coast-salience-plan.md).
No analysis code, datasets, model outputs, or Git history have been rolled back
during this planning session.
