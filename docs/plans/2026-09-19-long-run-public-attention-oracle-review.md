# Long-run public attention: Oracle review

Reviewed 19 September 2026. The review supports the agreed exploratory design, with corrections to validation and interpretation before implementation. No supplied evidence demonstrates an underlying mapping or estimator defect, and no settled user choice needs reopening.

The [amended plan](2026-09-19-001-long-run-public-attention-design.md) incorporates all eight findings below after local source verification. Its scope remains one future QMD, with 40 scalar model cells and four annual profiles. No QMD, analysis script, regression, test run or data rebuild was produced during this review.

## Review provenance

- Oracle browser session: `long-run-repeat-plan-review-2`, completed successfully.
- Requested model: GPT Astra Pro (`gpt-6-pro`). Oracle recorded verified `Latest` model selection and verified `Pro` thinking effort using strict selection checks. This records the browser selection, not independent server-side model attestation.
- Input: 28 selected plan, source and documentation files, including issue #14 and the relevant slide extract. No transaction datasets or credentials were attached.
- [Original Oracle conversation](https://chatgpt.com/c/6aaeaaf9-8720-83eb-8500-1f0c33721de7). The local session also retains the full transcript.
- Oracle assessed the supplied sources and earlier audit measurements. It did not validate live transaction inputs, model estimates or runtime. Local verification checked its claims against implementation code and project documentation.

## Findings and disposition

| Priority | Finding | Adopted correction |
|:--|:--|:--|
| High | A successful transaction-ID join can conceal an old address mapping after an address correction. | KTD10 and U1 require read-only current-address key/hash checks, including mapped singles, and explicit generation reconciliation. No mapping rebuild is authorized. |
| High | Large samples and non-collinearity do not describe the variation left after all fixed effects. | KTD11 and U3 require residual variation/concentration diagnostics, full annual-restriction checks and consistent clustered inference, without new eligibility thresholds. |
| Medium | Archive coverage was not operationally defined. | KTD4 and U2 require search/export evidence and valid unique monthly counts. Unknown months cannot silently become zeros. January 2014's audited count is not a hard-coded invariant. |
| Medium | Geographic stability does not establish dwelling-level precision. | KTD2 and the spatial diagnostics identify postcode-derived coordinates, 2011 geography labels and Site Group representatives, and report quality/vintage and unusable sites. |
| Medium | Observation weighting and attention timing needed clearer interpretation. | The plan states the unweighted transaction/listing projection, contemporaneous month-inclusive article stock, conditional near/far ratio interpretation and rental-unit validation. Frequency, gap and fallback-date diagnostics add no exclusions. |
| Medium | Sequential fitting can still accumulate full model objects. | KTD9 and U3 specify one working fit, extraction before compaction, shared sample indices and release of matrices after each fit. Nearest matching cannot allocate an unbounded all-pairs matrix. |
| Medium | Cache validity lacked precise input/dependency boundaries. | KTD8 and U4 require content identity, computational dependency fingerprints, exact expected model keys and atomic publication. Presentation-only edits retain reuse. |
| Low | Explicit England selection was incorrectly described as established baseline equivalence. | The population paragraph now distinguishes the baseline specification from its preparation filters. The new headline explicitly selects England, retains London and does not inherit complete-control selection. |

## Additional local finding

The [July LexisNexis refinement plan](2026-07-01-001-feat-lexisnexis-llm-relevance-geotag-ra-issue.md) already documents off-topic matches and merged article boundaries that can misassociate metadata. This document was discovered during local verification and was not in Oracle's input bundle.

The current PDF parser and monthly aggregator do not establish complete export coverage or reliable article boundaries. Export cover pages show consistent query/source filters, but do not certify complete pagination or extraction. The plan now distinguishes readable inherited counts from verified monthly coverage, requires available provenance checks and discloses measurement limitations. Repairing the parser or redefining the corpus remains separate work. Coverage failure blocks affected article cells while eligible Post and annual-profile cells can proceed.

## Remaining evidence requirements

Implementation or additional source evidence must establish current address-to-mapping consistency, usable article coverage, consistent rental units, final sample support, annual-contrast availability, memory use and successful fresh/cached rendering. Their absence from this review is not evidence that the data fail those checks.

Even a successful implementation cannot establish historical asset existence, immutable dwelling identity, independent rental episodes, stable media composition or the absence of competing near-specific shocks. These remain limits on interpretation.

Final local reviews checked the amended mapping/sample and identification/inference contracts. Their remaining presentation inconsistency was resolved: the render flow permits explicit blocked article cells, a complete accounting bundle retains all expected keys, and unavailable article evidence receives an explanation instead of an unvalidated plot. The user-selected periods, fixed geography, property effects, two FE structures, two attention measures, characteristic sensitivities and January 2014 article origin remain unchanged.
