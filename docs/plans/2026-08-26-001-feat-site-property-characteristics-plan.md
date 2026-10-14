---
title: Site and Property Spill Characteristics - Plan
type: feat
date: 2026-08-26
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
---

# Site and Property Spill Characteristics - Plan

Status: locked — signed off by Jacopo on 2026-08-26 after a
`grill-with-docs` design session.

## Goal Capsule

- **Objective:** Publish reusable Site Group and property-radius companion
  datasets for coast distance, designated bathing/shellfish waters, and
  prior-to-transaction spill-intensity bands without changing or rebuilding
  the existing exposure datasets.
- **Primary outputs:** one general Site Group characteristics file; one
  radius-partitioned companion dataset for sales; one for rentals; and one
  small intensity-cutoff audit file.
- **Execution profile:** R 4.6.0 through the project `rv` environment, using
  `here::here()`, `arrow`, `dplyr`, and `sf` in the existing pipeline style.
- **Tail ownership:** this plan ends when the four datasets publish, their
  contracts pass, their diagnostics are reviewed, and pipeline documentation
  is current. Regressions, tables, slides, and manuscript changes are outside
  scope.

## Product Contract

### Summary

The feature is a post-processing layer over existing canonical artifacts.
Intrinsic Site Group characteristics live in a general processed-data folder.
Transaction-, market-, window-, and radius-dependent characteristics live in
separate cross-sectional companion datasets. Downstream analysis may join
these companions by their explicit keys; the canonical prior-exposure schemas
remain closed and unchanged.

```text
Annual Return EDM + Annual Return Lookup + Site Group membership
ONS Great Britain coastline + Site Group projection
                            |
                            v
          site_group_characteristics.parquet
                            |
Property-Site Group lookups + existing prior-exposure radius datasets
                            |
                 +----------+----------+
                 v                     v
       sales/prior_characteristics  rentals/prior_characteristics
                 \                     /
                  +---- intensity cutoff audit
```

### Problem Frame

The headline hedonic data currently contain spill exposure but no reusable
environmental classification of the nearby Site Groups. The required source
fields already exist, but they sit at different grains:

- Site Group location is available through the canonical Site Group projection.
- Bathing- and shellfish-water fields are annual-return metadata whose encoding
  changes across years and whose values must first be mapped through Canonical
  Spill Sites to Site Groups.
- Spill intensity is already measured at transaction-radius grain in the
  prior-to-transaction datasets and should not be recomputed from events.

Appending the new columns to the existing exposure datasets would couple those
closed products to coastline and designation policy and would require expensive
republication. Joinable companion datasets preserve the existing architecture
and make later sample filters cheap.

### Requirements

#### Boundary and ownership

- **R1.** Do not change the schema, values, paths, or builders of any existing
  prior-exposure, measurement-layer, property-Site Group lookup, Site Group
  crosswalk, or Canonical Spill Site artifact.
- **R2.** Characteristics that do not depend on a transaction, market, exposure
  window, or radius belong under `data/processed/site_characteristics/`.
- **R3.** Transaction-dependent summaries belong under the corresponding
  `data/processed/cross_section/{sales,rentals}/` folder and must be joinable
  without analysis-specific property controls.
- **R4.** This feature publishes data only. No regression, table, figure, slide,
  or manuscript code changes are permitted.

#### Site coast distance

- **R5.** Publish `distance_to_coast_m` as a continuous Site Group
  characteristic. Do not publish a canonical coastal/inland indicator or any
  hard-coded 1/5/10 km threshold.
- **R6.** Use the same one-row-per-Site-Group projection as the property matching
  pipeline: `read_site_group_projection(..., years = 2021:2024)`, whose location
  rule selects the most recent valid Site Group coordinates in the window.
- **R7.** Use the existing local ONS boundary at
  `data/raw/shapefiles/UK_Nations/CTRY_DEC_2024_UK_BGC.shp`. It is the December
  2024 BGC product: generalised to 20 metres, clipped to the Mean High Water
  coastline, and stored in EPSG:27700.
- **R8.** Filter to England, Wales, and Scotland, dissolve them into one Great
  Britain landmass, and only then extract the boundary. Dissolving removes
  England-Wales and England-Scotland land borders so they cannot be mistaken
  for coast. Validate that the dissolved source has no interior rings (the
  current file has none), or retain only exterior rings before calculating
  distances.
- **R9.** Calculate planar metre distances in EPSG:27700 with `sf::st_distance()`.
  Preserve missing distance for a Site Group without a valid projected point;
  never drop it from the general Site Group universe.
- **R10.** Name and describe this construct as **Site Coast Distance** or
  near-coast location. It does not classify receiving water and must not be
  labelled a coastal discharge. WFD inland/transitional/coastal classification
  is a future feature. The distance is measured to the nearest Mean High Water
  tidal line, which extends up tidal rivers (through central London on the
  Thames and up the Severn to Gloucester); it is not distance to open sea.
  An open-sea distance would require an estuary-clipped geometry and is
  deferred alongside the WFD receiving-water feature.

#### Annual designated-water status

- **R11.** Classify bathing-water and shellfish-water evidence independently at
  Annual-Return Site-year, Canonical Spill Site-year, and Site Group-year grain.
- **R12.** At an active Annual-Return Site-year, normalise whitespace and case,
  then classify:
  - a named non-placeholder water as `designated`;
  - blank in the 2021-2023 “populate only if applicable” fields, `0`, `No`, or
    `Not Applicable` as `not_designated`;
  - `TBC`, `To be confirmed`, or an unresolvable conflicting value as `unknown`.
- **R13.** Distinguish a blank field on a present annual-return row from an
  absent annual-return row. The former follows R12; a Site Group-year with no
  annual-return evidence is `unknown`, not `not_designated`.
- **R14.** Map year-specific annual-return identifiers through
  `annual_return_lookup.parquet` to `site_id_canonical`, then through the
  canonical membership in `unique_spill_sites.parquet` to Site Group `site_id`.
  The physical join keys: `annual_return_lookup.site_id` holds the Canonical
  Spill Site identifier and joins to `unique_spill_sites.site_id_canonical`;
  `unique_spill_sites.site_id` is the Site Group key. Rename the lookup's
  `site_id` on read (for example to `site_id_canonical`) so it is never
  confused with the Site Group `site_id` used by the crosswalk and all
  outputs. Assert complete and unique mappings; do not infer identity from
  names or coordinates in this feature.
- **R15.** Within a Canonical Spill Site-year and then a Site Group-year:
  - any designated member makes the status `designated`;
  - otherwise any unknown member makes the status `unknown`;
  - otherwise the status is `not_designated`.
  Separately retain whether designated and non-designated member evidence was
  mixed and whether any unknown member evidence occurred.
- **R16.** Preserve the 2021, 2022, 2023, and 2024 Site Group statuses as
  separate columns. For each water-use type derive:
  - `*_ever_2124`: at least one designated year;
  - `*_changed_2124`: at least one designated and one known non-designated year;
  - `*_unknown_2124`: at least one unknown year or unknown-member flag;
  - `*_24`: the 2024 logical status, with unknown represented as `NA`.
  “Ever” means positive evidence exists during 2021-2024; it does not mean
  continuous designation.

#### Property-radius environmental summaries

- **R17.** Aggregate Site Group characteristics through the existing
  `spill_house_lookup.parquet` and
  `zoopla/spill_rental_lookup.parquet`. Expand real property-Site Group pairs to
  the existing 250, 500, and 1,000 m thresholds using `distance_m`; do not
  perform a new spatial match.
- **R18.** Re-enumerate the authoritative transaction-radius universe from the
  corresponding existing prior-exposure dataset. Property outputs must have
  exactly one row for every source transaction-radius row, including rows with
  no nearby Site Group.
- **R19.** Publish `min_coast_dist_m` and `max_coast_dist_m` across all nearby
  Site Groups. These two continuous values allow any later threshold to derive
  near-coast-only, inland-only, and mixed-radius samples without changing this
  dataset. Preserve `NA` when no site exists; fail if a real pair unexpectedly
  lacks a joinable Site Group characteristic.
- **R20.** For bathing and shellfish, using `*_ever_2124` as the primary site
  evidence, publish at property-radius grain:
  - `any_*_2124`: at least one nearby designated Site Group;
  - `all_*_2124`: every nearby Site Group is designated and none is unknown;
  - `mixed_*_2124`: at least one designated and one known non-designated Site
    Group;
  - `*_unknown_2124`: at least one nearby Site Group has uncertain designation
    evidence;
  - `n_*_2124`: number of nearby designated Site Groups.
- **R21.** For a transaction-radius row with no nearby Site Group, publish
  `any_* = FALSE`, `n_* = 0`, `all_* = NA`, `mixed_* = FALSE`, and
  `*_unknown = FALSE`. Retain `n_spill_sites` in the companion dataset and
  assert equality with the source exposure row so `no_site` is auditable.

#### Property spill-intensity bands

- **R22.** Derive intensity only from the existing radius-level
  prior-to-transaction columns `spill_count_weekly_avg` and
  `spill_hrs_weekly_avg`. Do not reconstruct events and do not publish a fixed
  Site Group-level intensity classification in this feature.
- **R23.** For each market, radius, and measure, compute p50 among rows with
  `n_spill_sites > 0` and known, strictly positive exposure. Compute it before
  applying any later regression-specific property-control filters.
- **R24.** Publish `spill_count_band` and `spill_hrs_band` with the mutually
  exclusive values:
  - `no_site` when `n_spill_sites == 0`;
  - `unknown` when sites exist but exposure is missing;
  - `zero` when sites exist and known exposure is zero;
  - `spill_le_p50` when known positive exposure is at or below p50;
  - `spill_gt_p50` when known positive exposure is above p50.
  The comparison abbreviations are fixed as `le` = `<=` and `gt` = `>`.
- **R25.** Publish a cutoff audit keyed by market, radius, and measure, recording
  the p50 and counts of total, no-site, unknown, zero, positive, at/below-p50,
  and above-p50 rows. The two band counts must reconcile exactly to the positive
  count.

#### Publication and diagnostics

- **R26.** All outputs have hand-written Arrow schemas, exact key uniqueness,
  allowed-value checks, row-count checks, and staged validation before
  promotion. Large property outputs use the existing sibling-directory atomic
  publication machinery and Hive partitioning by `radius`.
- **R27.** Logs report source coverage, mapping coverage, annual designation
  distributions, mixed/unknown counts, cross-year changes, coast-distance
  missingness and quantiles, property join coverage, band distributions, and
  cutoff reconciliation.
- **R28.** Updating these companion datasets must not change modification times
  or contents of the existing canonical exposure datasets.

### Scope Boundaries

In scope:

- Site Group coast distance.
- Bathing- and shellfish-designation annual histories and 2021-2024 summaries.
- Sales and rental property-radius environmental summaries.
- Property-radius spill-count and spill-hours intensity bands.
- Cutoff audit, tests, logs, source documentation, and pipeline ordering.

Out of scope:

- WFD water-body lookup or inland/transitional/coastal receiving-water type.
- A fixed coastal-distance threshold or coastal/inland boolean.
- Site Group-level fixed-window spill-intensity classes.
- Designated-site-only spill exposure or any alternative exposure estimand.
- Hedonic regressions, heterogeneous coefficients, tables, figures, slides, or
  manuscript text.
- Changes to Annual Status, Site Group identity, event matching, missingness
  policy, or existing publication contracts.

## Data Contracts

### Inputs

| Input | Grain | Fields used | Authority |
|---|---|---|---|
| `annual_return_edm.parquet` | Annual-Return Site-year | year IDs, bathing and shellfish raw values | Source designation evidence |
| `annual_return_lookup.parquet` | Canonical Spill Site | `site_id` (the Canonical Spill Site key; joins to `site_id_canonical`), `site_id_2021:site_id_2024` | Year-site to canonical identity |
| `unique_spill_sites.parquet` | Canonical Spill Site | `site_id_canonical`, containing `site_id` | Canonical-to-Site Group membership |
| `site_group_crosswalk.parquet` | Site Group-year | location and annual-return presence | Site Group projection and absent-year guard |
| `CTRY_DEC_2024_UK_BGC.shp` | Country geometry | country name/code and geometry | Mean High Water coastline geometry |
| `spill_house_lookup.parquet` | sale-Site Group | `house_id`, `site_id`, `distance_m` | Nearby Site Group membership |
| `zoopla/spill_rental_lookup.parquet` | rental-Site Group | `rental_id`, `site_id`, `distance_m` | Nearby Site Group membership |
| `cross_section/sales/prior_to_sale/` | sale-radius | keys, `n_spill_sites`, weekly measures | Sales row universe and intensity |
| `cross_section/rentals/prior_to_rental/` | rental-radius | keys, `n_spill_sites`, weekly measures | Rental row universe and intensity |

### Output 1: Site Group characteristics

Path:
`data/processed/site_characteristics/site_group_characteristics.parquet`

Grain and key: one row per `site_id`.

Schema groups. Notation: a field written `x_21:24` denotes four separate
annual columns `x_21`, `x_22`, `x_23`, and `x_24` (two-digit year suffixes,
matching `bath_24`/`shell_24` and the `*_2124` summary names):

| Fields | Type | Rule |
|---|---|---|
| `site_id` | int32 | Unique, non-missing Site Group key |
| `distance_to_coast_m` | double | Non-negative metres; nullable only when projection coordinates are unavailable |
| `bath_status_21:24` | utf8 | `designated`, `not_designated`, or `unknown` |
| `shell_status_21:24` | utf8 | Same allowed values |
| `bath_mixed_21:24`, `shell_mixed_21:24` | bool | Both designated and non-designated member evidence occurs in that year |
| `bath_unknown_21:24`, `shell_unknown_21:24` | bool | Unknown member/year evidence occurs |
| `bath_ever_2124`, `shell_ever_2124` | bool | Any designated annual status |
| `bath_changed_2124`, `shell_changed_2124` | bool | Both designated and known non-designated annual statuses |
| `bath_unknown_2124`, `shell_unknown_2124` | bool | Any annual unknown evidence |
| `bath_24`, `shell_24` | bool | Logical 2024 status; nullable when unknown |

The output row set must equal the Site Group projection row set exactly.

### Outputs 2 and 3: Property-radius characteristics

Paths:

- `data/processed/cross_section/sales/prior_characteristics/`
- `data/processed/cross_section/rentals/prior_characteristics/`

Grain and key: `house_id`/`rental_id` plus Hive `radius`, with partitions for
250, 500, and 1,000 m.

| Fields | Type | Rule |
|---|---|---|
| `house_id` or `rental_id` | utf8 | Source transaction key |
| `radius` | int32 | Hive partition field in `{250,500,1000}` |
| `n_spill_sites` | int32 | Must equal the source prior-exposure value |
| `min_coast_dist_m`, `max_coast_dist_m` | double | Nearby Site Group range; `NA` for no-site rows |
| `any_bath_2124`, `any_shell_2124` | bool | Any nearby Site Group ever designated |
| `all_bath_2124`, `all_shell_2124` | bool | All nearby Site Groups ever designated; `NA` for no-site rows |
| `mixed_bath_2124`, `mixed_shell_2124` | bool | Both designated and known non-designated Site Groups nearby |
| `bath_unknown_2124`, `shell_unknown_2124` | bool | Any nearby Site Group carries uncertain evidence |
| `n_bath_2124`, `n_shell_2124` | int32 | Count of nearby ever-designated Site Groups |
| `spill_count_band`, `spill_hrs_band` | utf8 | One of the five R24 levels |

The output keys and row counts must equal the corresponding prior-exposure
dataset exactly, both globally and within every radius partition.

### Output 4: Intensity cutoff audit

Path:
`data/processed/cross_section/prior_intensity_cutoffs.parquet`

Grain and key: `market`, `radius`, `measure`.

| Field | Type | Rule |
|---|---|---|
| `market` | utf8 | `sales` or `rentals` |
| `radius` | int32 | `{250,500,1000}` |
| `measure` | utf8 | `spill_count_weekly_avg` or `spill_hrs_weekly_avg` |
| `p50` | double | Median among known strictly positive rows |
| `n_total` | int64 | Source transaction-radius rows |
| `n_no_site` | int64 | `n_spill_sites == 0` |
| `n_unknown` | int64 | Sites exist and measure is missing |
| `n_zero` | int64 | Sites exist and measure equals zero |
| `n_positive` | int64 | Sites exist and measure is strictly positive |
| `n_le_p50` | int64 | Positive rows at or below p50 |
| `n_gt_p50` | int64 | Positive rows above p50 |

Required reconciliations:

```text
n_total = n_no_site + n_unknown + n_zero + n_positive
n_positive = n_le_p50 + n_gt_p50
```

## Implementation Units

### U1. Lock pure classification and aggregation contracts in tests

- **Create:**
  - `scripts/R/testing/test_site_group_characteristics_contracts.R`
  - `scripts/R/testing/test_prior_characteristics_contracts.R`
- **Fixtures:**
  - Designation placeholders, named waters, `TBC`, absent annual returns,
    duplicate annual rows, mixed canonical members, and cross-year changes.
  - Simple synthetic land polygons and Site Group points with known distances.
  - Zero-site, one-site, multi-site, mixed designation, missing evidence,
    median ties, and measure-specific p50 property rows.
- **Assertions:** exact schemas, key uniqueness, allowed enums, empty-set
  semantics, coast min/max, p50 split, audit reconciliations, and row-universe
  conservation.
- **Stop condition:** do not write producers until the fixtures express every
  requirement above and fail for the expected missing functions.

### U2. Build and publish general Site Group characteristics

- **Create:** `scripts/R/03_data_enrichment/build_site_group_characteristics.R`.
- **Bootstrap:** source `script_setup.R`, `site_group_utils.R`, and use explicit
  package checks; do not install packages at runtime.
- **Approach:**
  1. Load and validate the five general inputs.
  2. Build the authoritative 2021-2024 Site Group projection.
  3. Build the active Annual-Return Site-year to Canonical Spill Site to Site
     Group mapping.
  4. Apply the designation truth table and the canonical/group reducers.
  5. Complete the Site Group-year universe against the crosswalk so absent
     annual returns become unknown designation evidence.
  6. Dissolve the ONS Great Britain land geometry and calculate coast distance.
  7. Join annual and summary fields onto the projection universe.
  8. Write a sibling candidate Parquet file, validate it, and promote it onto
     the canonical path without leaving a half-written file.
- **Log:** `output/log/build_site_group_characteristics.log`.
- **Diagnostics:** status counts by year/type, mixed and unknown counts,
  ever/changed cross-tabulations, mapping misses (required zero), coordinate
  misses, and coast-distance quantiles.

### U3. Build and publish market-specific property companions

- **Create:** `scripts/R/06_analysis_datasets/build_prior_characteristics.R`.
- **Approach:** one explicit market-spec list handles sales and rentals without
  duplicating classification logic. For each market:
  1. Read only the required columns from the property-Site Group lookup and the
     existing radius-level prior-exposure dataset.
  2. Filter real pairs to 1,000 m, join Site Group characteristics with a
     row-conservation and complete-match gate, and expand each pair into every
     qualifying radius.
  3. Reduce coast and designation characteristics per transaction-radius.
  4. Rejoin the complete source transaction-radius universe and apply the
     agreed no-site semantics.
  5. Compute measure-specific p50 values and bands within each radius.
  6. Stage, validate, and atomically publish the radius-partitioned companion.
  7. Collect the two measures' cutoff audit rows.
  After both markets pass, validate and promote the combined cutoff file.
- **Publication:** use `publish_validated_dataset()` for the two directory
  outputs. Use sibling-candidate validation and promotion for the cutoff file.
- **Log:** `output/log/build_prior_characteristics.log`.
- **Performance constraint:** never collect full event data or rebuild prior
  exposure. Arrow projection/filtering should restrict all reads to keys and
  required columns; only reduced transaction-radius tables are materialised.

### U4. Production-level contract and provenance verification

- Extend both test files with optional checks against canonical outputs when
  present: schema, keys, allowed levels, partition set, row parity, and cutoff
  reconciliation.
- Verify that every real property-Site Group pair joins exactly one Site Group
  characteristic and that no join changes pair counts.
- Verify no negative or infinite coast distances and inspect the smallest and
  largest distances for obvious border/coastline errors.
- Spot-check known inland, coastal, Welsh, northern-English, and island Site
  Groups. The northern-English check specifically guards against accidentally
  retaining the England-Scotland land border. Also spot-check two estuarine
  Site Groups — one on the tidal Thames in London and one on the Severn near
  Gloucester — both expected near-zero, confirming the documented tidal
  Mean High Water semantics of `distance_to_coast_m`.
- Compare designation counts with the planning audit baseline:
  - bathing designated Site Groups by year: approximately 914, 963, 975, 1,053;
  - shellfish designated Site Groups by year: approximately 950, 967, 749, 722.
  Differences require explanation, not automatic failure, because the final
  absent-year and active-member rules are stricter than the exploratory audit.
- Record ONS source/licence attribution in the producer header and pipeline
  documentation: Office for National Statistics under OGL v3.0; contains OS
  data © Crown copyright and database right 2024.

### U5. Integrate the new builders into pipeline documentation

- **Modify:** `docs/pipeline_documentation.md`.
- Add `build_site_group_characteristics.R` to Layer 03 immediately after
  `create_unique_spill_sites.R` because it consumes canonical membership and
  the Site Group projection.
- Add `build_prior_characteristics.R` to Layer 06 immediately after
  `cross_section_prior_to_sale.R` and `cross_section_prior_to_rental.R`, because
  those outputs define its row universe and intensity values.
- Document the four output paths, keys, source coastline, and the distinction
  between Site Coast Distance and receiving-water type, including the tidal
  Mean High Water semantics of `distance_to_coast_m`.
- Do not add an ADR. The companion-dataset boundary is documented here and is
  straightforward to reverse; it does not meet the project threshold for a
  permanent architectural decision record.

## Verification Matrix

All commands run with plain `Rscript` from the repository root so that
`.Rprofile` activates the rv-managed project library (`rv` has no `run`
subcommand, and `--vanilla` would bypass the project library).

| Gate | Command | Expected evidence |
|---|---|---|
| Parse | `Rscript -e "files <- c('scripts/R/03_data_enrichment/build_site_group_characteristics.R','scripts/R/06_analysis_datasets/build_prior_characteristics.R','scripts/R/testing/test_site_group_characteristics_contracts.R','scripts/R/testing/test_prior_characteristics_contracts.R'); invisible(lapply(files, parse))"` | All new R files parse under the project environment |
| Site unit/contract tests | `Rscript scripts/R/testing/test_site_group_characteristics_contracts.R` | Designation, coastline, schema, and key fixtures pass |
| Property unit/contract tests | `Rscript scripts/R/testing/test_prior_characteristics_contracts.R` | Multi-site aggregation, empty-set, bands, and audit fixtures pass |
| Site production build | `Rscript scripts/R/03_data_enrichment/build_site_group_characteristics.R` | Validated Site Group companion and coverage log publish |
| Property production build | `Rscript scripts/R/06_analysis_datasets/build_prior_characteristics.R` | Both market companions and cutoff audit publish |
| Site canonical read-back | Re-run site contract test | Canonical schema/key/value checks pass against the published file |
| Property canonical read-back | Re-run property contract test | Exact source-key parity and cutoff reconciliations pass for all six market-radius partitions |
| Existing-output guard | Compare checksums or mtimes captured before the builds | No existing exposure or lookup artifact changed |
| Documentation | Inspect `docs/pipeline_documentation.md` ordering and paths | Both builders appear after their actual dependencies |
| Diagnostics review | Inspect both build logs against U4's baseline counts and spot-check list | Reviewed designation counts and coast spot checks, with written explanations for any divergence from the audit baseline |

## Failure and Edge-Case Policy

- **Missing annual-return row:** designation is unknown; never silently negative.
- **Blank designation field on a present older-format row:** negative under the
  source field's “populate only if applicable” contract.
- **Named value plus negative member value:** designated with `mixed = TRUE`.
- **Named value plus unknown member value:** designated with the annual unknown
  diagnostic retained.
- **Multiple distinct named waters:** still designated; names are not published
  by this feature and do not create a boolean conflict.
- **Unmapped Annual-Return Site or Canonical Spill Site:** hard failure with
  sampled keys.
- **Site Group without valid coordinates:** retain site row with missing coast
  distance; log and count it.
- **Real property pair without Site Group characteristic:** hard failure.
- **No nearby Site Group:** preserve row and apply R21/R24 semantics.
- **Known negative exposure:** hard failure; exposure must be non-negative.
- **No positive rows for a market-radius-measure:** hard failure; a p50 band
  cannot be defined honestly.
- **Median ties:** all exact ties enter `spill_le_p50`; audit counts disclose the
  resulting unequal band sizes.
- **Interrupted property publication:** existing canonical generation remains
  readable or is restored through `publish_validated_dataset()`.

## Final Acceptance Criteria

The feature is complete only when:

1. The general Site Group characteristics file is unique on `site_id`, covers
   the complete Site Group projection, and passes coast/designation contracts.
2. Sales and rental companion datasets have exact key parity with their source
   prior-exposure datasets at 250, 500, and 1,000 m.
3. Every band is one of the five agreed values, and all cutoff audit identities
   reconcile exactly.
4. No existing exposure, lookup, or analysis artifact is modified.
5. Logs make mapping, unknown evidence, designation changes, coast coverage,
   and intensity cutoffs auditable.
6. Pipeline documentation contains the correct dependency order and output
   paths.
7. No regression, table, figure, slide, or paper output has been regenerated.
8. The U4 spot-checks (inland, coastal, estuarine, Welsh, northern-English,
   and island Site Groups) and the designation baseline comparison have been
   performed, and any divergence from the audit counts is explained in the
   build log or plan record.

