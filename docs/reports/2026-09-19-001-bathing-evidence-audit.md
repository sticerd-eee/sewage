# Bathing evidence audit — 19 September 2026

The current bathing flag is useful as a fixed characteristic based on reported associations during 2021–2024. It does **not** establish that an overflow was associated with a designated bathing water at the transaction date. Later evidence determines membership for a material share of the current fitted Bathing samples.

This is the approved diagnostic audit, not an implemented reclassification. No builders or regressions were run and no estimates changed. All fitted-sample counts below describe the saved specifications **excluding London**: sales cover 2021–2024; rentals cover 2021–2023. The agreed restoration of London and revised coastline are not reflected here.

## Meaning and parsing

The source field records a bathing water associated with an overflow's monitoring requirement. Describe the flag as **“reported association with a designated bathing water”**. Direct discharge is not required: EA guidance also covers overflows that affect bathing waters. [EA permitting guidance](https://www.gov.uk/government/publications/water-companies-environmental-permits-for-storm-overflows-and-emergency-overflows/water-companies-environmental-permits-for-storm-overflows-and-emergency-overflows#bathing-waters-water-quality-standards).

The audit reproduces the current parser: blanks, missing values, `0`, `no` and `not applicable` become `not_designated`; `tbc`, `to be confirmed` and `unknown` become `unknown`; other strings become positive evidence. No positive entry triggered the audit's obvious-placeholder heuristic. This is not a complete validation of every positive string against an official bathing-water register.

| Current saved Site Group evidence | Count |
|---|---:|
| All Site Groups | 13,990 |
| Positive in any year, 2021–2024 | 1,118 |
| First observed positive in 2024 | 97 |
| Ever-positive groups with unknown evidence in any year | 17 |
| Ever-positive groups with mixed member evidence in any year | 48 |

Mixed or unknown member evidence can coexist with a positive group status; these rows are not mutually exclusive categories.

## Timing of the 97 first-positive groups

Official designation checks divide the 97 groups into **68 referencing waters newly designated in 2024** and **29 referencing waters already designated by 2021**. There are 98 distinct Site Group/name/receiving-water records because one group appears twice; counts use 97 unique groups, with no group spanning the new/existing categories.

The 68 comprise Steamer Quay (15), Shrewsbury (8), Nidd at the Lido (8), Littlehaven (7), Wharfe at Wilderness (7), Friars Meadow (5), Dittisham (4), Warfleet Creek (4), Ironbridge (3), Ludlow (2), Stoke Gabriel (2), Manningtree (1), Goring (1), and Wallingford (1). Official sources: [2024 designation decision](https://www.gov.uk/government/consultations/bathing-waters-proposed-designation-of-27-new-bathing-waters-in-england/outcome/summary-of-responses-and-government-response), [2021 classifications](https://www.gov.uk/government/publications/bathing-waters-in-england-compliance-reports/bathing-water-classifications-2021).

The 29 existing-water groups do **not** establish a reporting error: their overflow association or permit requirement may have changed. Do not backdate their associations from the water's designation date alone. Likewise, first-positive 2024 is an observation in these returns, not an effective designation date.

Original-workbook checks explain the earlier missing values. Among the 116 member rows per year, 2021 has **75 empty cells, 40 `N/a` markers and one literal `0`**; 2022 and 2023 each have **75 empty cells and 41 `N/a`/`N/A` markers**. The cleaner turns the markers into missing values, then the Site Group parser maps missing values to `not_designated`. These statuses therefore mostly reflect blanks or missing markers, not affirmative evidence of no association. All 461 traced member rows across 2021–2024 matched the processed values and company/permit/site-name anchors; XML checks found no Excel errors or unresolved missing-cell types. This establishes cell provenance, not historical designation or association dates.

## How much fitted-sample membership depends on 2024-only evidence?

An observation is counted below if it would lose Bathing membership when positive evidence first observed in 2024 is withheld. Nearest-site specifications use the nearest Site Group; intensive specifications count an observation only when **every** bathing-positive group within 250 m first appears positive in 2024. This is a sensitivity diagnostic, not a recommendation to remove those observations.

| Market and specification | Current Bathing observations | Dependent on 2024-only evidence | Share |
|---|---:|---:|---:|
| Sales, extensive | 193,843 | 14,435 | 7.45% |
| Sales, intensive | 15,630 | 1,513 | 9.68% |
| Sales, hedonic | 15,384 | 1,513 | 9.83% |
| Rentals, extensive | 41,966 | 3,124 | 7.44% |
| Rentals, intensive | 2,715 | 228 | 8.40% |
| Rentals, hedonic | 2,674 | 228 | 8.53% |

Article and Trends specifications share sample membership within the intensive and extensive families. Rows are separate model samples and must not be added together.

For sales transactions **before 2024**, the corresponding counts are 11,240/151,012 (extensive), 1,190/12,209 (intensive), and 1,190/12,029 (hedonic). All rental transactions precede 2024. These counts combine the 68 new-water and 29 existing-water groups; their separate fitted-sample contributions have not been calculated.

Final membership was reconstructed using the exact fixed-effect levels in each saved model's `fixef_removed` field. Retained counts match saved `nobs`; exclusions match the full saved `obsRemoved` count. This avoids assuming that saved observation positions correspond to a newly ordered data frame. Pre-estimation counts also match the saved sample audits.

## Agreed definitions and remaining checks

- **Main bathing definition:** retain the fixed, ever-observed 2021–2024 reported association. Add a robustness specification using positive association evidence from **2021 only**. Neither measure establishes an association at each transaction date; the 2021-only version fixes the evidence window rather than reconstructing historical effective dates.
- **Property assignment:** retain the nearest Site Group for extensive and hedonic specifications, and any qualifying Site Group within 250 m for intensive specifications. Add a diagnostic showing where these assignment rules disagree.
- **Location of result testing:** extend the [existing salience Quarto report](2026-09-03-003-heterogeneity-by-salience-report.qmd), keeping diagnostic and regression-robustness code and outputs under `docs/reports/`. Do not add testing variants to the main analysis scripts. The approved diagnostics and robustness specifications have not yet been run.
- Preserve annual evidence and its provenance. Blanks and missing markers establish no reported positive evidence; they do not confirm absence of association. Historical association dates and any change to the representation of missing evidence remain unresolved. Use “association” in table wording and assess coefficient changes only after rerunning the agreed specifications.

## Reproduction and archived evidence

An additional coastline-source coverage check finds **115 Welsh sales and two Welsh rentals** in each of the prepared baseline and intensive samples, retained after London exclusion. These are before subgroup and estimator exclusions. Country comes from the transaction country field; their region is missing, and LSOA values are names rather than ONS codes. This establishes Welsh property coverage, not the associated overflows' country, so the coastline refinement must validate compatible Welsh and cross-border shoreline coverage.

The audit's working directory — count tables, the diagnostic scripts, source metadata with MD5 hashes and the R session record — was not retained after the memo was written; this memo is the audit's record. Every count above was checked against the saved models and pipeline data when the audit ran (R 4.6.0, recorded project library, reading existing data and saved models without evaluating analysis-script setup or main execution). The historical diagnostic sequence was a raw-site audit, a first-positive trace and property-lookup membership check, an original-workbook cell audit, market-sample reconstructions, and a later country-coverage check.

For **any future execution**, prepare fresh diagnostic or robustness code under `docs/reports/` and direct its results and intermediate outputs there. The earlier "official checks outstanding" status is superseded only by the limited 97-group chronology check documented above; full positive-string validation and historical overflow-association checks remain open.
