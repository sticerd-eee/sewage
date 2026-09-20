---
title: Open-coast salience plan — Oracle review
date: 2026-09-19
type: review
subject: 2026-09-19-2209-fix-open-coast-salience-plan.md
model: gpt-6-pro
engine: browser
status: findings-incorporated
---

# Open-coast salience plan — Oracle review

The [implementation plan](2026-09-19-2209-fix-open-coast-salience-plan.md) was reviewed through Oracle using **ChatGPT GPT-6 Astra Pro**. The verdict on the submitted version was **ready after specific corrections**. All three prioritised findings were checked locally and incorporated into the plan, together with the concrete acceptance clarifications below. No new research-design decision was required.

This was a plan review. No pipeline implementation, R contract tests, data rebuild, regression estimation or report rendering was performed. The revised plan received a local coherence check; it was not submitted for a second Oracle pass.

## Review evidence

- The raw Oracle response, the submitted plan copy, the review prompt and the receipt were archived in a working directory that was not retained after the plan was superseded; the submitted plan text is the tracked [implementation plan](2026-09-19-2209-fix-open-coast-salience-plan.md) as of this review's date. Receipt facts: Oracle 0.21.1, browser engine, 42 attached files, approximately 171,062 input tokens.
- [ChatGPT review conversation](https://chatgpt.com/c/6aaf0345-7268-83eb-be7c-cdfda034472a), completed at 22:06 UTC on 19 September 2026.

Oracle verified the `Latest` model selection and `Pro` effort before submission; the composer displayed `6 Pro`. This is UI-selection evidence, not independent server-side model attestation. The earlier Sol run was superseded after the user's correction; its response was not used. The other 41 supplied files still matched their submitted hashes when this review was archived.

## Findings and disposition

| Finding | Local verification | Plan amendment |
|---|---|---|
| **F1 — High: existing model tests exercise historical report functions, not the five paper scripts** | The extensive, intensive and hedonic suites load `salience_report_test_setup.R`, which purls the QMD. Its paper-execution chunk is excluded. `test_salience_groups_contracts.R` covers shared helpers only. Separate hard-coded London masks exist in the current paper callers. | U5 now names a direct five-paper suite, isolated script environments, actual preparation/audit/selection/fitting paths, and both-market checks. It records the equations, eligibility restrictions and inference to preserve. Historical tests retain their original role. |
| **F2 — High: complete model/export publication is unspecified** | The five exporters write a table, RDS and two CSVs sequentially. Data publication checks alone cannot stop a new table being paired with an old model or a partially refreshed specification collection. | KTD4–KTD5 now require staging, exact artifact hashes, generation identity, explicit expected keys and a completion manifest written last. U5–U7 add interrupted-export, stale-cell and incomplete-collection checks. Prior validated outputs remain recoverable. |
| **F3 — Medium: restoring old data alone cannot restore revised consumers** | Revised callers must reject absent open-coast fields. Restoring old datasets while leaving those callers selected therefore cannot satisfy the previous operational-recovery promise. | U1 records compatible consumer code/configuration, recoverable uncommitted source, data and result identities. U7 tests recovery after the consumer switch. Refined consumers stay disabled until recovery or completion; no automatic tidal fallback is introduced. |

F2 does not require a new pipeline runner or distributed transaction system. Standalone paper scripts and standard output paths remain. A completed collection accounts for each requested cell, including documented unavailable fits; corrupted inputs remain fatal errors. Missing historical bundles retain their separate AE6 unavailable state without mandatory re-estimation.

## Acceptance clarifications incorporated

| Oracle checklist | Added precision in the plan |
|---|---|
| A–B: geography and distances | U2 tests a physical line spanning coast and estuary, segment-interior distances, omitted closer shores, and source-driven ambiguity. Alignment tolerances cannot expand the threshold. U3 tests 1,999.999 / 2,000 / 2,000.001 m through serialization, evidence consistency and preservation of representative locations. |
| C: radius aggregation | U4 explicitly exercises production Arrow and in-memory paths for both markets and all radii. Counts reconcile, all-unknown summaries cannot become infinities, duplicate pairs fail, and the rewritten intensity-cutoff audit is reconciled. |
| D: actual paper selection | U5 includes London-only support, missing-region observations, far controls, count/hours restrictions, zero versus missing exposure and exact-key joins. Qualifying-site membership does not narrow the existing all-site exposure measure. |
| E: 2021 bathing evidence | U6 includes first-positive years 2022–2024, positive evidence alongside unknowns, and no-positive versus absent evidence. Nearest assignment precedes bathing-status evaluation. |
| F: interpretation and sample turnover | U6 fixes exact common transaction support, preserves All Bathing under coast-only changes with otherwise identical inputs, and tests shuffled rows and estimator removals. The London-excluded comparison is labelled as coast measurement plus evidence-completeness refinement, not solely shoreline removal. |
| G: history and report modes | U1 distinguishes historical four-way/intensity, exclusive three-way and overlapping paper families. Purl checks executed side effects; U6 keeps historical numerical prose with its original bundles. |
| H: publication and recovery | Headline 5 × 2 × 3, London-excluded 5 × 2 × 3 and 2021-only Bathing 5 × 2 cells are enumerated. Manifest validation checks the saved inference and matching exports. Fixture-only success is separated from canonical read-back and production validation. |

The local coherence pass found and resolved one ambiguity: collection completeness applies to new headline/refinement runs, while an unavailable historical bundle may still render as unavailable without fitting. The original 11 requirements, six technical decisions and seven implementation units remain intact.

## Qualifications to the Oracle response

The runner continues after script failures only when `--keep-going` is requested; its default is fail-fast, and either mode ultimately signals failure. F2 remains valid because outputs written before a failure can already have changed. No runner redesign follows from that finding.

`prior_intensity_cutoffs.parquet` already uses staged file publication. The missing guarantee concerns its reconciliation with the separately published market companions, not absence of staging.

The omitted startup and radius-table dependencies were inspected locally. `.Rprofile` activates `rv` and contains no additional analytical settings; execution must still verify the actual R version/library because missing `rv` can produce a warning. `utils_radius_robustness_table.R` formats fitted parent models and exports only when called; the five selected paper scripts do not import it directly. Its omission did not conceal another sample or model-contract change.

## Remaining execution gates

The official-source checks support the source strategy, not any unbuilt reference geometry. In particular, [NRW metadata](https://datamap.gov.wales/layers/geonode:nrw_wfd_transitional_c3_baseline_classification/metadata_detail) confirms a different cartographic source, reinforcing the alignment gate; [Poole Harbour's record](https://environment.data.gov.uk/catchment-planning/WaterBody/GB520804415800) confirms its transitional classification. No spatial payload was downloaded or validated during this review.

Actual shoreline validity and coverage, stable input generations, production Arrow parity, direct paper behavior, publication recovery and side-effect-free report rendering remain mandatory implementation checks. Statistical robustness remains unknown until the agreed report analysis runs. All empirical diagnostics and results stay under `docs/reports/`; these files document the planning review only.
