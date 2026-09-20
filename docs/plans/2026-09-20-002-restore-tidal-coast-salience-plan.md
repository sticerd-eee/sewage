---
title: Restore original tidal-coast salience and retain the London-category map
type: fix
date: 2026-09-20
status: completed
completed: 2026-09-20
origin: 2026-09-20-001-restore-tidal-coast-discussion.md
baseline: c4eba6e5cf952b7d4c0accf76556f359eae32615
reverses: a62a02a
---

# Objective and agreed scope

Restore the original coastal salience analysis, datasets, saved results and report
to checkpoint `c4eba6e`, with three deliberate exceptions: retain the latest
original-classification map with London Coastal CSOs highlighted; make the old
report's existing saved-result mode the default; retain accurate salience and
bathing-association explanations. Implementation completed on 20 September 2026;
the verification record below documents the restoration.

The scientific target is local salience, potentially through recreational use
and the value of nearby water. Tidal-shoreline proximity is a broad proxy for that
mechanism. The official coastal/transitional distinction excludes settings the
user wants represented. The decision is to restore the original proxy, without
adding an estuary group, water-openness measure, or further regression experiments.

The user approved cleanup and the report choices in the linked interview record.
Retain earlier plans as superseded history. Do not create a separate decision note
or new ADR. Preserve the independent bathing-evidence audit and unrelated work.

## Final scientific contract

- Original `distance_to_coast_m` measures straight-line distance from the existing
  representative Site Group location to the tidal Mean High Water coastline,
  including estuarine and tidal-river banks. Coastal uses the inclusive 2,000 m rule.
- Restore the original nearest-site assignment for extensive-margin and baseline
  hedonic models, and original any-site-within-250-m assignment for intensive models.
  Restore historical missing-evidence behaviour exactly; retain no new coast policy.
- All five paper specifications exclude properties with `region == "London"`.
  Preserve the original equations, controls, exposure windows, distance bands,
  inference, markets, Bathing definition and group overlap.
- Restore the original report's full exploration, including its pre-existing
  four-stratum, intensity, robustness and exclusive three-way sections. An original
  London-retained robustness result is not a new-plan artifact and must survive.
- Remove the new open-coast estimates, revised London-included headline, new
  2021-only Bathing analysis and old/new coast comparisons.
- Describe Bathing as positive reported association during 2021–2024; neither
  that association nor shoreline proximity establishes individual swimming,
  direct discharge, continuous designation or designation at transaction date.

## Verified recovery material

The report asset directory is
`docs/reports/2026-09-03-003-heterogeneity-by-salience-report/` (REPORT below).
Its `historical/manifest.json` identifies baseline `c4eba6e` and inventories:

| Material | Recovery location | Required restoration |
|---|---|---|
| 11 saved regression bundles | `REPORT/historical/regs/` | Original paths under `output/regs/` |
| 89 preserved table/CSV exports | Manifest-relative paths under `REPORT/historical/` | Corresponding paths under `output/` |
| Site characteristics | Recovery inventory | Original site-characteristics Parquet |
| Six radius companions | Recovery inventory | Sales/rentals × 250/500/1000 m partitions |
| Intensity cutoffs | Recovery inventory | Original shared cutoff Parquet |

All copies were present during planning; their hashes must be verified before
execution. The eight data copies are rooted at the manifest's recorded
`data/processed/recovery/open-coast-pre-refinement` location.

`data/` and `output/` are links to shared Dropbox storage, not Git-managed data.
Keep those links operational. Reverting source code alone would leave refined
products in place. Original historical bundles predate full upstream input hashes;
do not claim to reconstruct provenance that was never recorded.

## Implementation order

### U1 — Inventory and protect the restore set

1. Confirm current Git state and record every path affected by `a62a02a`, subsequent
   map/report changes, and experiment-created files outside Git. Inspect current
   storage for staging files, manifests and generation directories.
2. Verify hashes for every inventoried historical bundle, export and data recovery
   file. Record original hashes in a temporary execution receipt outside the
   directories scheduled for deletion. Inventory retained map/plan/audit files too.
3. Distinguish this experiment's files from unrelated work and pre-existing raw
   inputs. Do not use broad `git clean`, `reset --hard`, or a blanket storage delete.
4. If a recovery file is absent/corrupt or a shared destination has unexpected
   concurrent changes, stop restoration and resolve the discrepancy. Never infer
   the old schema merely by dropping the new columns or silently refit substitutes.

### U2 — Restore canonical data and outputs before removing recovery support

1. Restore the eight exact data files using the existing staged publication helper
   and recovery inventory. Read the full manifest and verify all source hashes
   before replacing any destination. Keep recovery material available throughout.
2. Restore the 11 model bundles and the inventoried exports to their original
   `output/` paths. Leave already-identical destinations alone. Verify every
   restored destination against its original recorded hash.
3. Remove the new `_open_coast_manifest.json` from each market's restored
   `prior_characteristics/` directory. These must not describe the restored data.
4. Restore production builders, all five paper scripts, shared classification
   helpers and pre-existing tests to baseline `c4eba6e`. Use an explicit reviewed
   path list; preserve user changes outside the reversal. No history rewrite.
5. Ensure the restored source reads the restored exact schemas. Remove new
   generation/publication hooks and avoid running revised consumers during the
   transition. A temporary migration helper may be used and removed after success.

### U3 — Retain a minimal original-classification map

Keep `REPORT/map_coastal_eligibility.R` and `REPORT/original-coastal-eligibility/`,
but simplify the script to generate only the original map. Remove its dependency
on `report_storage.R`, open-coast validators, the physical open-coast reference,
new site-generation columns and refined-map assets. Use restored site distances,
the existing crosswalk's representative coordinates and pre-existing ONS sources.

- Left panel: original boundary of dissolved England/Wales/Scotland geometry,
  removing country seams before extracting the boundary, as in the original builder.
- Right panel: 2,917 Coastal Site Groups outside Greater London (blue), 83 inside
  Greater London (purple), and 10,968 mapped Inland sites (grey).
- Greater London: union of the 33 ONS May 2025 local authorities with E090 codes;
  preserve the current outline. No hand-drawn or postcode-prefix substitute.
- Keep the same map extent, representative points, labels, PNG and zoomable PDF.
  Explain the 21 missing-coordinate sites and the one located Scottish site outside
  the map. The two Coastal categories sum to the original 3,000.
- Preserve the distinction between CSO location and the regression exclusion of
  London properties, especially for properties and sites on opposite sides of
  the boundary. The map is not an exact plot of fitted property observations.
- Regenerate the compact counts and map manifest using only surviving inputs
  and assets. Remove all open-coast hashes/generations and references from it.

### U4 — Restore and render the original report

1. Restore the QMD from `c4eba6e`, then make only the agreed presentation changes.
2. Set its existing `params$reestimate` default to `false`. Saved models come from
   restored `output/regs/`. Keep explicit `reestimate:true` supported. Missing saved
   results must fail clearly, never silently cause a fit.
3. Insert the original map after Data and definitions, before regression results,
   with the London-category explanation and a PDF link. Rendering uses saved map
   assets; map regeneration is a separate explicit operation.
4. Keep the accurate descriptions of tidal-coast proximity, recreational salience
   as a hypothesis/proxy, and reported bathing association. Do not relabel old
   coefficients as new analyses or alter group definitions through prose edits.
5. Remove open-coast result sections, geography audits, cross-border diagnostics,
   transition comparisons, and their imports. Restore the original result plots,
   tables and interpretation. Keep the original report title/path; regenerate HTML.
6. Restore pipeline documentation to baseline plus the saved-render default and
   retained map command. Preserve the accepted glossary clarifications. Remove
   abandoned open-coast and 2021-only terms from the active glossary when no longer
   needed, without discarding unrelated user edits.

### U5 — Validate, then remove experimental artifacts

First run the acceptance checks below with recovery files still available. After
they pass, delete the reviewed experiment-owned paths; then render and check again
to prove that the report and map have no hidden dependency on deleted material.

| Action | Paths / scope |
|---|---|
| Remove new executable code/tests | Files introduced by `a62a02a` under `scripts/R/`, and experiment-specific report helpers; retain only the simplified map builder |
| Remove experimental report assets | `REPORT/geometry/`, `geometry-alignment-25/`, `geometry-alignment-50-sepa/`, `refinement/`, `coastal-eligibility/`, experiment caches/logs and `execution-record.md` |
| Remove downloaded experiment sources | `data/raw/geography/open_coast/` only; preserve all pre-existing ONS boundaries and original inputs |
| Remove refined geography | `data/processed/geography/open_coast/` |
| Remove new paper-publication artifacts | Experiment-owned `output/salience-generations/`, `output/salience-current.json`, and any inventoried staging files |
| Remove redundant recovery copies last | `REPORT/historical/`, any experiment-created historical-reproduction directory, and `data/processed/recovery/open-coast-pre-refinement/`, only after verified canonical restoration |
| Keep | Original map/script, restored original report/data/results, original dated memos, the independent bathing-evidence audit, pre-existing ONS inputs, unrelated outputs and user work |
| Retain as superseded history | Earlier open-coast planning/interview/oracle documents and existing ADR 0003; mark the controlling plan and ADR superseded by this plan, without creating a new ADR or separate decision note |

Do not delete shared parent directories such as `data/raw/geography/`,
`data/processed/`, `output/regs/` or `output/logs/`. Existing builder logs may have
pre-existing content; delete only dedicated experiment logs with established
ownership. Audit the actual filesystem before finalising the deletion list.
Earlier plans can retain historical references to deleted experimental artifacts;
active report/code dependencies cannot.

## Acceptance checks

1. **Exact restoration:** all eight data files and 11 bundles match their recorded
   pre-plan hashes. All 89 inventoried exports are restored/unchanged as appropriate.
   Verify unrelated source-data and property–site lookup files remain untouched.
2. **Original contracts:** run the restored site-characteristics, both-market
   prior-characteristics and affected salience group/model/report contract tests
   under R 4.6.0 and the project's `rv` environment. Original schemas and London
   exclusion must pass without open-coast columns, packages or artifacts.
3. **Results:** reconcile the five paper specifications × two markets × three
   groups against the preserved original bundles: focal coefficients, confidence
   intervals, fitted N and saved counts. Check the original exploration bundles
   as well. Restoring saved results does not require rerunning full regressions.
4. **Map:** regenerate from restored legacy inputs; confirm the 2,917/83/10,968
   split, 3,000 total Coastal, unchanged representative locations and 2 km rule.
   Inspect PNG/PDF for readable legend, London outline and unobstructed labels.
5. **Render behaviour:** default rendering loads saved results. Forbid fitting and
   live dataset reads during the check, and compare production-output hashes before
   and after. Make chunk errors fatal during verification so errors cannot merely
   appear as rendered text. Reuse a temporary check rather than retain the removed
   open-coast test framework.
6. **Rendered HTML:** verify the original results and the London-category map are
   present, with no refined-result sections, missing images or stale links. Check
   the embedded PNG against the saved asset's hash. No new browser-service
   disclosure is required to complete local artifact verification.
7. **Dependency closure:** after cleanup, map regeneration, baseline contract tests
   and default HTML rendering still pass. Active code and map manifests contain no
   dependency on deleted open-coast/reference/generation/storage helpers.
8. **Final diff:** only the agreed map, rendering-default, wording/documentation and
   supersession changes remain beyond the pre-plan state. Save the rollback as a
   local commit using explicit paths, preserving unrelated changes and Git history.
   Do not push or open a PR unless separately requested.

## Completion report

Report the restored checkpoint, successful data/result comparisons, retained map
location, completed cleanup and any concrete limitations. Link the refreshed old
HTML report. Do not claim restoration solely because the source diff was reverted.

## Completed implementation and verification

All U1–U5 work completed on `jo/restore-tidal-coast-salience`. Production builders,
five paper scripts, classification helpers and pre-existing tests match
`c4eba6e` exactly. Every original R analysis chunk in the report is unchanged;
the additional chunk checks that saved bundles exist before rendering.

- Verified the complete recovery inventory before publication. All eight data
  files, 11 regression bundles and 89 exports match their recorded original
  SHA-256 hashes. Staged publication replaced 27 files and left 81 identical
  files alone. Both obsolete companion manifests were removed.
- Reconciled coefficients, stored 95% confidence intervals, fitted N and counts
  for 30 overlapping paper cells, 176 exploration cells and 30 exclusive
  three-way cells. The original London-retained robustness results survive.
- All eight restored contract suites passed under R 4.6.0 with the `rv` library,
  including canonical site read-back and both markets' three radius partitions.
  They passed again after cleanup.
- Regenerated the map before and after cleanup. Counts are 2,917 Coastal outside
  London, 83 inside, and 10,968 Inland; 21 sites lack coordinates and one located
  site is outside the map. The PNG and counts CSV match the retained map exactly.
  PNG and rendered PDF inspection found readable legends, labels and London outline.
- Default rendering passed with estimators and Arrow live-data readers forbidden,
  chunk errors fatal, and all 108 production hashes unchanged. A simulated missing
  bundle raised the explicit saved-results error without fitting. This check also
  passed after cleanup.
- Quarto regenerated the [original HTML report](../reports/2026-09-03-003-heterogeneity-by-salience-report.html).
  Local HTML inspection verified 15 tables, four images, the exact embedded map
  PNG and all local links. Quarto required access to its local Sass cache outside
  the workspace; the completed render succeeded.
- Removed the inventoried experiment code, diagnostics, downloaded sources,
  geography reference, publication generations and redundant recovery copies.
  Active scripts, report, documentation and map manifest contain no dependencies
  on the removed framework. Shared `data/` and `output/` links remain operational.
- Verified 188 protected source/lookup/boundary files by hash and 6,522 unrelated
  shared files by unchanged size and modification time. No new shared files were
  introduced. Earlier planning history, dated memos and the independent bathing
  audit remain; the old controlling plan and ADR 0003 are marked superseded.

Temporary inventories, original hashes and verification logs are in
`/tmp/tidal-restore-20260920/`. Full regressions were not rerun: this restores the
saved original results. The historical bundles predate complete upstream-input
hashes, so this does not reconstruct provenance that was never recorded.
