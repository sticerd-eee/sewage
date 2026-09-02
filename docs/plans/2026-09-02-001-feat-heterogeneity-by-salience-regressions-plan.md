---
title: Heterogeneity by Salience Regressions - Plan
type: feat
date: 2026-09-02
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
---

# Heterogeneity by Salience Regressions - Plan

Status: locked — signed off by Jacopo on 2026-09-02 after a
`grill-with-docs` design session.

## Goal Capsule

- **Objective:** Test whether the Public Attention effect on property prices
  is larger in places of high Local Salience, by re-estimating the existing
  attention specifications within mutually exclusive Salience Strata.
- **Primary outputs:** regression tables for the extensive-margin attention
  specification (headline), the intensive-margin attention specification
  (secondary), and the baseline hedonic (appendix), each estimated within
  every stratum, plus three robustness variants.
- **Execution profile:** R 4.6.0 through the project `rv` environment,
  `fixest`, `arrow`, `dplyr`, existing table helpers.
- **Tail ownership:** this plan ends when every table publishes and the
  cell counts are logged. Deck edits, slide text, and the choice of which
  tables enter the deck are decided only after results are reviewed.

## Product Contract

### Summary

Nothing new is estimated. The three existing families are rerun on
subsamples defined from the published Site Group and property-radius
characteristics. Strata are built once, in a shared utility, so every
family classifies a transaction identically.

```text
site_group_characteristics + spill_house_lookup / spill_rental_lookup
                    |  nearest Site Group per property (<= 2 km)
                    v
      Salience Strata (coast / bathing family; intensity family)
                    |
    +---------------+---------------+
    v               v               v
extensive-margin  intensive-margin  baseline hedonic
attention tables  attention tables  tables (appendix)
```

### Vocabulary

- **Public Attention** varies over time and is common to all places
  (Post-August-2022 indicator; log cumulative UK Media Article Count).
- **Local Salience** varies across places and is fixed for a property:
  near the coast, at a designated bathing water, or near a heavy-spilling
  overflow. See `CONCEPTS.md`.
- **Salience Stratum**: a mutually exclusive subsample within which one
  specification is re-estimated. Heterogeneity is read across strata, not
  from added interaction terms.

### Requirements

#### Specifications (unchanged from the deck)

- **R1. Extensive margin (headline).** For each market, the saturated model
  from `did_trends_prior_extensive.R` and `did_articles_prior_extensive.R`:
  log price on Near, Near × Attention, property controls, LSOA FE, month FE,
  LSOA-clustered SEs. Near is nearest overflow within 0–500 m; far is
  1,000–2,000 m. Headline band only; no near-band sweep.
- **R2. Intensive margin (secondary).** The saturated model from
  `did_trends_prior.R` and `did_articles_prior.R` at 250 m: log price on
  weekly spill count, spill count × Attention, property controls, LSOA FE,
  month FE, LSOA-clustered SEs.
- **R3. Baseline hedonic (appendix).** The saturated LSOA column of
  `hedonic_continuous_prior.R` at 250 m with heteroskedasticity-robust SEs.
- **R4.** Attention measures: Post-August-2022 indicator (primary) and log
  cumulative articles (secondary, expected appendix). Windowed article
  counts are out of scope.
- **R5.** Spill-count exposure only; spill hours are out of scope.
- **R6.** Sales 2021–2024 and rentals 2021–2023, as in the existing tables.

#### Salience Strata

- **R7. Coast and bathing family (four strata, one table family).**
  A property's class comes from its nearest Site Group (extensive margin and
  baseline hedonic) or from its radius companion (intensive margin):
  - *bathing*: nearest Site Group has `bath_ever_2124 = TRUE`
    (intensive margin: `any_bath_2124` at 250 m);
  - *coastal not bathing*: coastal (below) and not bathing;
  - *coastal (all)*: nearest Site Group `distance_to_coast_m <= 2000`
    (intensive margin: `min_coast_dist_m <= 2000`);
  - *inland*: not coastal.
  Bathing is expected to be almost entirely a subset of coastal; the
  coastal-not-bathing column isolates designation from coastal location.
- **R8. Unknown bathing evidence** (`bath_unknown_2124`, never observed
  designated) counts as not designated in the headline.
- **R9. Intensity family (near group only).** Extensive margin: near
  properties split by their 500 m `spill_count_band` into `spill_le_p50`
  and `spill_gt_p50`, each estimated against the full far group, which has
  no band by construction. Intensive margin: the 250 m sample split by its
  own 250 m `spill_count_band`. Rows with band `unknown` or `zero` are
  excluded from both intensity strata. The baseline hedonic is not split
  by intensity because exposure is its regressor.
- **R10. Greater London** is dropped from every regression in this plan,
  flagged by `region == "London"` in `house_price.parquet` and
  `zoopla_rentals.parquet`. Motivation: `distance_to_coast_m` is measured
  to the tidal Mean High Water line, so the Thames through London reads as
  coast. The measure is to be cleaned up in a later feature.
- **R11.** Nearest Site Group is the pair with minimum `distance_m` in the
  existing lookups, restricted to pairs within 2,000 m; ties broken
  deterministically. No new spatial match.

#### Robustness (appendix tables, identical structure)

- **R12.** Coast rule at 10,000 m instead of 2,000 m, rerunning the whole
  four-way family.
- **R13.** With Greater London retained, coast/bathing family only.
- **R14.** Bathing family with unknown-evidence Site Groups excluded.

#### Tables and logs

- **R15.** Tables use the same structure and formatting as the existing
  attention and hedonic tables, with one column per stratum and market and
  only the saturated specification. Each table reports N per column.
- **R16.** Logs record, per market and family, the number of transactions
  in each stratum before and after the London drop, and the share of
  nearest Site Groups with missing coast distance.
- **R17.** No deck, slide, or manuscript change. Which tables enter the
  deck is decided after review.

### Scope Boundaries

Out of scope: pooled triple-interaction tests; near-band sweeps; spill-hours
exposure; windowed article measures; shellfish designation; an open-sea
coast distance; nearest-site intensity for far properties (the site-grain
prior-exposure dataset stops at 1 km); any new data build.

## Data Contracts

| Input | Use |
|---|---|
| `data/processed/site_characteristics/site_group_characteristics.parquet` | `distance_to_coast_m`, `bath_ever_2124`, `bath_unknown_2124` |
| `data/processed/spill_house_lookup.parquet`, `data/processed/zoopla/spill_rental_lookup.parquet` | nearest Site Group per property within 2 km |
| `data/processed/cross_section/{sales,rentals}/prior_characteristics/` | `min_coast_dist_m`, `any_bath_2124`, `spill_count_band` at 250 and 500 m |
| `data/processed/house_price.parquet`, `data/processed/zoopla/zoopla_rentals.parquet` | transactions, controls, `region` |
| existing prior-exposure datasets and attention series | as consumed by the current scripts |

Stratum assignment output (in-memory, not published): one row per
transaction with `salience_class` in
`{bathing, coastal_not_bathing, inland}` (coastal = union of the first
two), `coast_rule_m` in `{2000, 10000}`, `bath_unknown`, `london`, and
for the intensity family `spill_count_band` at the relevant radius.

## Implementation Units

### U1. Shared stratum utility

- **Create:** `scripts/R/09_analysis/utils_salience_strata.R`.
- Functions: nearest-Site-Group lookup with characteristics; coast/bathing
  classification for a given coast rule and unknown policy; radius-companion
  classification for the intensive margin; London flag; a stratum
  enumerator returning the ordered list of subsample filters for a family;
  a cell-count logger.
- Contract tests in `scripts/R/testing/test_salience_strata_contracts.R`
  with fixtures for ties, missing coast distance, unknown evidence, the
  2 km / 10 km rules, and the far group's absent band.

### U2. Extensive-margin attention by stratum

- **Create:** `scripts/R/09_analysis/05_news/did_trends_prior_extensive_salience.R`
  and `did_articles_prior_extensive_salience.R`.
- Reuse the existing data preparation from `extensive_margin_news_utils.R`,
  join strata, loop over families and strata, fit the saturated model, and
  write one table per family and attention measure, plus the R12–R14
  robustness tables.
- **Outputs:** `output/tables/did_{trends,articles}_prior_extensive_salience_{coast_bathing,intensity}.tex`
  and `_robust_{coast10km,london,dropunknown}.tex`.

### U3. Intensive-margin attention by stratum

- **Create:** `scripts/R/09_analysis/05_news/did_trends_prior_salience.R`
  and `did_articles_prior_salience.R`, at 250 m, radius-companion
  classification.
- **Outputs:** `output/tables/did_{trends,articles}_prior_salience_{coast_bathing,intensity}.tex`
  plus robustness.

### U4. Baseline hedonic by stratum

- **Create:** `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior_salience.R`
  at 250 m, coast/bathing family only.
- **Outputs:** `output/tables/hedonic_count_continuous_prior_salience_coast_bathing.tex`
  plus robustness.

### U5. Runner and documentation

- Add the new scripts to `scripts/R/09_analysis/run_all_analysis.sh` after
  their parent scripts.
- Document the scripts and tables in `docs/pipeline_documentation.md`.

## Verification Matrix

| Gate | Evidence |
|---|---|
| Parse | all new R files parse under the project environment |
| Stratum contract tests | fixtures pass; strata are mutually exclusive and exhaustive within each family |
| Cell counts | every stratum × market has N logged; no stratum silently empty |
| Reproduction | with the stratum filter removed and London retained, the headline scripts reproduce the existing deck coefficients exactly |
| Publication | all tables compile in the deck's table harness |

## Failure and Edge-Case Policy

- Nearest Site Group with missing coast distance: excluded from the
  coast/bathing family, counted in the log.
- Property with no Site Group within 2 km: not in the extensive-margin
  sample by construction; excluded from the baseline hedonic strata.
- Empty stratum for a market: hard failure with the stratum named.
- Far-group property with a non-`no_site` band at 500 m: hard failure, it
  contradicts the far definition.

## Final Acceptance Criteria

1. Every table in U2–U4 publishes with the agreed structure and N.
2. Strata are built by one utility and are identical across families.
3. The reproduction gate passes.
4. Logs make cell counts and exclusions auditable.
5. No deck, slide, or manuscript file is changed.
