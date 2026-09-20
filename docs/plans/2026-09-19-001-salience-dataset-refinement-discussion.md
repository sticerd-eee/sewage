---
title: Salience dataset refinement discussion
date: 2026-09-19
---

# Agreed scope

The [implementation plan](2026-09-19-2209-fix-open-coast-salience-plan.md)
turns these decisions into the work to implement. This document preserves the
interview evidence and rationale.

The primary focus is improving coastal geography: the coastline definition,
distance measurement, assignment to properties, and resulting London policy.
Jacopo chose coastal only, excluding estuaries, and declined the proposed
separate estuary-proximity measure. The revised coastal measure will reference
the open coast rather than estuarine or tidal-river shorelines. Its geographic
source geometry and precise mouth boundaries must be validated during
implementation. Jacopo approved retaining bays classified as coastal by the
Environment Agency, using actual shoreline rather than offshore polygon edges
or artificial lines across estuary mouths. Distance remains overflow-to-coast,
not property-to-coast.
Jacopo confirmed that the exclusion applies to the reference shorelines, not
to every overflow with an estuarine receiving water. An overflow 500 m from
the retained open coastline may qualify as coastal even when it discharges
into an estuary. The existing 2 km threshold remains the starting point.

Jacopo approved restoring London eligibility after validating the corrected
geography. London observations receive the same geographic and bathing rules
as other observations; London exclusion remains a robustness check.

Jacopo approved retaining the current multiple-site rules: nearest Site Group
for extensive-margin and baseline hedonic models; any qualifying Site Group
within 250 m for intensive-margin models. Quantify how often these rules give
different classifications on the same eligible properties.

Jacopo approved retaining the fixed 2021–2024 ever-observed bathing-water
association for the headline groups. Label it as an association, without
claiming direct discharge, continuous designation or designation at transaction
date. Use positive association evidence recorded in 2021 as a fixed robustness
measure; do not interpret absent 2021 evidence as proof of no historical
association. Positive evidence continues to prevail over other unknown evidence.

All classification comparisons, exploratory analyses and regression robustness
work belong in `docs/reports/`, including their code and results. Extend the
existing [salience Quarto report](../reports/2026-09-03-003-heterogeneity-by-salience-report.qmd)
for this work; no separate results report is planned. In particular,
the nearest-versus-any diagnostic, 2021-only bathing check and London-excluded
robustness must not become extra paper entry points in `scripts/R/09_analysis/`.
The five existing paper specifications keep their roles and equations.

The starting paper specification uses overlapping All Coastal, All Inland and
All Bathing groups. Earlier agreements distinguish general visibility and
perceived local harm from recreation alone. The open-coast direction is agreed;
no data or estimation changes have been implemented.

# Evidence and retained cautions

- The existing site builder measures proximity to the tidal Mean High Water
  boundary. It explicitly does not measure distance to open sea or identify
  receiving-water type. The original salience plan records the London exclusion
  as a temporary response to this geography, with refinement deferred.
- Estuary boundaries depend on the definition. The JNCC habitat description
  uses the brackish-water limit; its estuary inventory uses the normal tidal
  limit and includes tidal freshwater reaches. The EA Thames Estuary 2100
  area extends from Teddington through central London to the North Sea.
  Consequently, including all official estuary polygons would not necessarily
  remove urban tidal-river locations from a coastal group.
- Bathing evidence comes from annual-return columns described as populated
  for overflows with a bathing-water EDM requirement. The current builder
  treats a present-row blank as negative and most other nonempty text as
  positive. EA guidance covers direct discharges and discharges that impact
  bathing waters from elsewhere. A reported monitoring association therefore
  does not establish direct discharge or location within a bathing area.
  Blank-as-negative remains an analytical assumption: the inspected source
  guidance does not establish that every blank excludes a relevant association.
- Ever-observed bathing evidence uses all of 2021–2024, including information
  later than some transactions. Annual statuses survive in the site dataset,
  allowing an earlier-information sensitivity measure without discarding the
  original definition. EA reports that 27 new bathing-water designations added
  102 associated overflows to the 2024 bathing-season return; later evidence
  can therefore reflect genuine designation changes.
- Extensive-margin and baseline hedonic classifications use the nearest Site
  Group. Intensive-margin classifications use any qualifying Site Group within
  250 m. This can give one property different memberships across specifications.

The approved bathing diagnostic is complete; its scope and remaining limits
are recorded in the [audit report](../reports/2026-09-19-001-bathing-evidence-audit.md).
No data builders or regressions have been run.

# Implementation requirements

1. Build a real high-water shoreline using the coastal/transitional distinction
   and a compatible shoreline source. Remove estuarine and tidal-river shores;
   never use offshore polygon limits or artificial estuary-mouth separators
   as coastline. Record source, vintage, country coverage and any reviewed
   ambiguous mouth segments. A classification year is not a shoreline survey date.
2. Preserve the existing geographic study universe. A country-field check
   finds 115 Welsh sales and two Welsh rentals in the prepared baseline and
   intensive samples, before subgroup and estimator removals. Property-country
   coverage does not establish associated overflow locations; validate those
   and their nearest shoreline coverage. Include compatible NRW Cycle 3
   coastal/transitional evidence where needed,
   including relevant cross-border nearest shores. Never use company identity
   as a country proxy or silently remove sites outside the EA layer's coverage.
   Check EA/NRW geometry alignment at the Dee and Severn against a consistent
   physical shoreline; their source cartography is not the same vintage.
3. Preserve the existing Site Group identity, representative locations and
   property–Site Group lookups. Add explicitly named open-coast distance and
   radius summaries, retaining the old tidal-coast measurement for provenance
   and comparison. Publish clear source/version metadata so saved historical
   results retain their meaning. Keep missing geographic evidence explicit.
4. Use the corrected distance in the five existing paper specifications and
   restore London eligibility. Retain exposure windows, distance bands,
   controls, fixed effects, inference, group overlap, and the chosen nearest/
   radius assignment rules. Bathing membership remains positive ever-observed
   association; a missing coast distance must not erase positive bathing evidence.
5. Extend `docs/reports/2026-09-03-003-heterogeneity-by-salience-report.qmd`
   for new result testing and refresh its rendered output. Compare old/new
   classifications and samples before interpreting coefficients;
   include the London-excluded, 2021-only bathing and assignment-disagreement
   diagnostics there. Keep those code paths and outputs out of the main analysis
   folder. Label historical and refined definitions explicitly within the
   existing report; preserve the dated Markdown memos and supporting bathing
   audit as evidence snapshots.
6. Verify join keys, row counts, coast coverage, representative locations near
   thresholds, estuary mouths, tidal Thames/Severn examples and retained coastal
   bays. Inspect both missing evidence and reclassification counts. For model
   comparisons, report N, coefficient and 95% CI, and identify changes in sample
   composition. Reuse existing data-contract checks; statistical result testing
   remains in `docs/reports/`.

Source geometry and country coverage are implementation facts to resolve, not
reasons to change the agreed study sample. No new research-design choices are
currently pending; the implementation plan carries the agreed scope forward.

# Completed bathing audit

Read-only inspection finds 1,118 ever-bathing Site Groups out of 13,990; 97
first have positive recorded evidence in 2024. Their prior negative statuses
are almost entirely created from missing source entries: 115 of 116 member
rows are NA in 2021, and all 116 are NA in 2022 and 2023. First recorded
positive evidence therefore must not be treated as a designation date.

In the current London-excluded fitted rental Bathing samples, membership
depends entirely on these 2024-first-positive sites for 3,124 of 41,966
extensive-margin observations (7.44%), 228 of 2,715 intensive-margin
observations (8.40%), and 228 of 2,674 baseline hedonic observations (8.53%).
Saved model fixed-effect removals reproduce final sample counts without
re-estimation. These diagnostics concern membership, not changes in estimates.
The corresponding fitted sales counts are 14,435 of 193,843 extensive-margin
observations, 1,513 of 15,630 intensive-margin observations, and 1,513 of
15,384 baseline hedonic observations. Restricting the diagnostic to sales
before 2024 gives 11,240 of 151,012 extensive-margin observations, 1,190 of
12,209 intensive-margin observations, and 1,190 of 12,029 hedonic observations.

Official records distinguish 68 of the 97 Site Groups whose named waters were
newly designated for the 2024 season from 29 whose named waters already appear
in the 2021 classifications. The latter could reflect changes in reporting,
overflow association or monitoring requirements; they are not established
reporting errors and must not automatically be backdated. Original-workbook
checks found 75 genuinely empty cells in each earlier year, 40 missing markers
in 2021 and 41 in 2022/2023, plus one literal zero in 2021. All 461 traced
member records across 2021–2024 match the processed data after normalization.
See the audit report and its compact evidence archive for counts, method and
source limitations.

# Agreed bathing audit

1. Inspect the original annual-return values behind the bathing flag. Verify
   that positive strings identify named bathing waters, identify placeholders
   and uncertain blanks, and cross-check questionable associations with EA
   bathing-season records where available. Preserve reporting uncertainty.
2. Identify first observed positive years and positive/negative transitions.
   Distinguish documented designation changes from possible reporting changes;
   do not assume first observation equals the legal designation date.
3. Report the number of affected Site Groups and sales/rental observations,
   including dependence on evidence recorded after the transaction. Compare
   membership under clearly labelled alternatives if needed to quantify the
   issue, without changing the published definition or rerunning regressions.

This diagnostic informed the agreed fixed association measure and the
2021-only robustness. Full positive-string register matching and exact
historical overflow-association dates were not established and are not
required to assert an ever-observed association.

# Sources

- [Original salience plan](2026-09-02-001-feat-heterogeneity-by-salience-regressions-plan.md), R10.
- [Annual-return field mapping](../../scripts/R/02_data_cleaning/combine_annual_return_data.R).
- [Site characteristics builder](../../scripts/R/03_data_enrichment/build_site_group_characteristics.R).
- [Group classification](../../scripts/R/utils/salience_group_utils.R).
- [JNCC estuary habitat definition](https://sac.jncc.gov.uk/habitat/H1130/).
- [JNCC estuary inventory methodology](https://data.jncc.gov.uk/data/69ebf0da-dc7e-47b2-afcc-440624768395/jncc-inventory-uk-estuaries-1-1997.pdf), section 4.3.1.
- [EA Thames Estuary 2100 extent](https://www.gov.uk/guidance/thames-estuary-2100-why-we-need-it).
- [EA transitional and coastal water-body polygons](https://www.data.gov.uk/dataset/57fbe7c3-f4be-446b-96db-e23e9dc9f09f/water-framework-directive-wfd-transitional-and-coastal-water-bodies-cycle-3-classification-2022).
- [EA Morecambe Bay coastal classification](https://environment.data.gov.uk/catchment-planning/WaterBody/GB641211171000).
- [EA Poole Harbour transitional classification](https://environment.data.gov.uk/catchment-planning/WaterBody/GB520804415800).
- [NRW Cycle 3 coastal waterbodies](https://datamap.gov.wales/layers/geonode:nrw_wfd_coastal_c3_baseline_classification).
- [NRW Cycle 3 transitional waterbodies](https://datamap.gov.wales/layers/geonode:nrw_wfd_transitional_c3_baseline_classification/metadata_detail).
- [Prepared sample country coverage](../reports/2026-09-19-001-bathing-evidence-audit/country_coverage_summary.csv).
- [EA overflow permit guidance](https://www.gov.uk/government/publications/water-companies-environmental-permits-for-storm-overflows-and-emergency-overflows/water-companies-environmental-permits-for-storm-overflows-and-emergency-overflows#bathing-waters-water-quality-standards).
- [EA 2024 bathing-season assessment](https://environmentagency.blog.gov.uk/2024/11/29/regulatory-edm-2024-bathing-season-storm-overflow-data-analysed/).
- [Defra's final 2024 bathing-water designations](https://www.gov.uk/government/consultations/bathing-waters-proposed-designation-of-27-new-bathing-waters-in-england/outcome/summary-of-responses-and-government-response).
- [Official 2021 bathing-water classifications](https://www.gov.uk/government/publications/bathing-waters-in-england-compliance-reports/bathing-water-classifications-2021).
