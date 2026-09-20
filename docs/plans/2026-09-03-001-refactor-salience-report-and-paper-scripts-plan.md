---
title: Salience research report and paper specifications
type: refactor
date: 2026-09-03
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
---

# Goal

Keep the salience additions to `scripts/R/09_analysis/` limited to the five
specifications selected for the paper. Preserve the full exploration as an
executable Quarto report in `docs/reports/`.

## Decisions confirmed in the interview

- The report contains executable analyses, results and interpretation. It
  preserves the original four coast/bathing strata, intensity splits, three
  robustness variants and the earlier exclusive three-way comparison.
- Paper results use overlapping **Salience Groups**: All Bathing, All Coastal
  and All Inland. Coastal and Inland include bathing locations. Bathing
  includes both coastal and inland locations. Membership is not a partition.
- Retain five paper specifications, each for sales and rentals: extensive
  Post, extensive articles, intensive Post, intensive articles, and baseline
  hedonic. No formal tests of coefficient differences across groups.
- Retain the existing exposure windows, distance bands, controls, fixed
  effects, standard errors, 2 km coast rule and London exclusion. Bathing
  follows positive designation evidence; unknown evidence does not negate a
  positive. Each group uses its own evidence: missing coast cannot establish
  Coastal or Inland membership, but does not negate observed bathing status.
- Extensive and hedonic classifications use the nearest Site Group;
  intensive classification uses the 250 m property-radius companion.
- Follow the existing analysis style: descriptive headers and input/output
  contracts, configuration, preparation, estimation, export and execution
  sections. Keep estimation visible in each script. Shared low-level helpers
  belong in `scripts/R/utils/`.

## Implementation

1. Document Salience Groups separately from the existing exclusive Salience
   Strata. Add focused tests of overlapping membership and missing evidence.
2. Refactor the five existing salience entry points into readable paper
   scripts. Publish distinct `_groups` outputs so the historical model bundles
   and dated memos retain their meaning.
3. Move the exploratory salience implementation into the QMD, including the
   original exclusive three-way analysis. Remove its obsolete main-folder
   helpers and omnibus entry point. Keep relevant tests executable against
   the report's definition chunks.
4. Render the report, rerun all paper specifications, and update pipeline
   documentation and links. Retain dated result memos as historical snapshots.

## Verification

- Classification fixtures: coastal bathing and inland bathing enter two
  groups; thresholds, unknown flags, missing evidence and London are handled
  explicitly; duplicate joins cannot inflate observations.
- Existing sample, model and reproduction contract tests continue to pass
  after moving exploratory code into the report.
- Reproduce historical coefficients, SEs and Ns against saved bundles;
  recompute overlapping Coastal and Inland models rather than averaging old
  coefficients. Bathing reproduces the earlier pooled bathing models.
- Run all five paper scripts and render the full QMD. Match report results,
  paper bundles, sample audits and tables. Inspect rendered HTML and LaTeX.
- The analysis runner retains only the five paper entry points for salience;
  no obsolete salience source paths remain in executable consumers.
