# Open-coast implementation execution record

Implementation resumed on 20 September 2026 after the initial source-alignment
audit. Geography, site characteristics, both companion markets and all five
paper model runs are complete. All 70 refined/robustness and 30 compatible legacy
report fits are complete. The saved results reconcile with the paper exports,
and the HTML has been refreshed and checked locally.

## Geography and continuity

The initial exact overlay supported only 639 of 13,990 Site Groups. That was an
alignment diagnostic, not an accepted coast reference. The final review combines
April 2026 OS OpenMap Local physical high-water lines, EA/NRW Cycle 3 coastal and
transitional classifications, the complete SEPA coastal/estuary inventories, and
EA river evidence. Supplementary OS foreshore and EA statutory-river evidence
resolve the Saltfleet coastal creek. Decisions are recorded in the committed
segment and mouth-separator CSVs; their source hashes are bound into the review.

All 13,969 located Site Groups now have covered nearest-coast distances. The 21
without usable representative coordinates remain unknown. No unresolved physical
segment could be nearer for a located study site. No reviewed distance falls
within the 1 m mouth-endpoint resolution of the unchanged inclusive 2,000 m
threshold. Only portions of physical OS shoreline enter the distance reference:
selectors and extended mouth separators never become distance targets, and
physical coordinates are not snapped or displaced. These generalised sources
do not provide survey-level accuracy.

The six final maps cover Thames, Severn, Morecambe, Poole, Dee and Solway. Additional
review panels and the final threshold table record mouth/coastal-bay decisions.
The unrestricted country audit identifies 735 English sites whose nearest eligible
shore is in Wales, plus one Scottish site whose nearest eligible shore is in
England. Country labels use nearest UK Nations polygons only for this diagnostic;
country does not constrain the distance search. The compact cross-border manifest
records the exact physical reference and boundary-source hash.

Geography was resolved before revised regression coefficients were inspected.
The acceptance criteria concern physical shoreline and complete evidence, not
coefficient signs, magnitudes or significance.

## Data and model implementation

- U1: eleven historical result bundles, matching exports, source snapshots and
  eight targeted recovery files were preserved before replacement. Historical
  source-input hashes were not recorded originally and cannot be reconstructed
  retroactively; the preserved bundles remain identified as historical evidence.
- U2: the reviewed physical reference is published with source, geometry and
  representative-location provenance. Source/decision replay commands are in
  `docs/pipeline_documentation.md`.
- U3: site publication adds four open-coast fields. Exact Site Group key and
  legacy-field equality passed before promotion; representative locations retain
  the established selection rule and match the reviewed location hash.
- U4: both reducers publish the same five additive radius fields. Both markets
  and all 250/500/1000 m partitions were rebuilt. All legacy companion fields and
  intensity cutoff values remained exact. Each radius contains 4,063,875 sales
  or 1,404,446 rental rows. Canonical read-back checks actually ran and passed.
- U5: all five actual paper scripts ran for both markets and three overlapping
  groups. Headline membership uses open coast and includes London. Model
  equations, controls, fixed effects, exposure windows and covariance estimators
  are unchanged. Final table wording and the equivalent vectorized duplicate-key
  check are recorded in refreshed export provenance; original fit provenance is
  retained with the unchanged fitted models.
- U6: the report owns the 70 refined/robustness cells and a separate compatible
  30-cell legacy-definition comparison on identical current inputs. Exact Bathing
  membership and estimates are checked for invariance in the coast-only comparison.
  Site distances, membership, nearest/any assignment, bathing evidence and selected/
  fitted turnover are saved alongside models. The extensive diagnostic derives
  radius evidence from the full lookup on the same prepared IDs, because exposure
  companions do not cover its entire property universe.
- U7: ordered acquisition/rebuild, recovery, generation and report-mode
  documentation is updated. Final report/render checks passed; the implementation
  is ready for its local commit.

## Verification

Tests use R 4.6.0 with the rv startup profile and plain Rscript. Geometry, site,
prior-characteristics, group, actual five-paper, refinement, recovery and affected
historical suites pass. Fixture tests cover threshold precision, mixed evidence,
missing/duplicate keys, unchanged equations/inference, London/unknown region,
missing outcomes, explicit unavailable fits, incomplete fifth-specification
publication, immutable retries and exact restored recovery bytes. Canonical site
and both-market read-back passed after the real rebuild.

The report validator rejects missing focal terms, changed coefficient values,
mismatched provenance, missing variants and missing/altered legacy comparisons.
The common-denominator test includes a property with no exposure-companion row.
A production run exposed a scalar/column shadowing bug in a diagnostic; it was
fixed and added to fixtures. CPU sampling identified expensive row-wise pair
checks; equivalent dplyr distinct-key checks now avoid that bottleneck.

R parsing, Python acquisition-script compilation and git whitespace checks pass.
Some sandboxed R runs print Arrow CPU-probe permission messages; fixture Parquet
round trips also emit Arrow metadata warnings. These did not skip or fail checks.
Estimator removal notes are reconciled to saved selected/fitted identities.

Final production reconciliation passed for all 30 paper cells and 20 exports,
all 70 refined/robustness report cells and all 30 compatible legacy cells.
Headline coefficients and exact selected/fitted identity sets match the paper
bundles; all saved CSV values, artifact hashes and effective code/input provenance
reconcile. Identity checks compare sets because scan order can differ.

- Paper generation: `d04ccf5c8b927e516ade667df4bc56f0ebedcaf75a1bdf8cac4f75e4ac8c1f42`.
- Report generation: `a3d3f8beee797360affa9c4b7109755a317ce33c38b493851f8caa9d7228bc98`.

The final render-only contract ran with fitting and Arrow data-reading functions
traced to fail, knitr errors made fatal, and production artifact hashes checked
before and after. It passed without fitting, dataset reads or production writes.
Quarto rendered the final HTML successfully. Local HTML inspection found 32 tables,
the refined section and no execution-error trace; all six embedded final maps
match the reviewed PNG source bytes. Table overflow styling is present. This
checks generated content and assets, not browser pixel layout. The HTML SHA-256 is
`bd57b285c42f77e4b66547248957aa97c0f8a5fe98b300976eb63c84011e4b1f`.

## Review and shipping scope

The ce-simplify-code and ce-code-review workflows were applied with sequential
main-context passes, as required by AGENTS.md. Coverage includes correctness,
project standards, testing, maintainability, performance, data migration,
reliability, adversarial cases and repository learnings. These are not independent
agent reviews. Automatic approval rejected the attempted external Claude review
because the diff could contain non-public code; no diff was sent and no peer job
started. The adversarial pass therefore ran locally.

Automatic approval also rejected opening the local report through the browser
connector because it considered that a possible disclosure to an unverified
external service. No browser preview was opened; local HTML/content and embedded
image checks were used instead. Browser layout remains uninspected.

The user's pre-existing CONCEPTS, plan, ADR and bathing-audit work is preserved and
excluded from this task's commit. No push or PR is authorized by the publication
scope of the pre-existing branch; shipping will be a local commit.
