---
title: "Annual return rows are monitor-level, not works-level: within-works duplicate identifiers are real"
date: 2026-06-10
category: conventions
module: Annual Return Lookup
problem_type: convention
component: database
severity: high
applies_when:
  - "Aggregating annual-return rows to works level (e.g. summing spill hours per site per year)."
  - "Diagnosing unmatched rows after deterministic linkage for 2021 or 2022 data."
  - "Interpreting within-year groups of rows with identical matcher identifiers."
  - "Evaluating RF or any matcher that forces 1:1 pairing within identifier-duplicate groups."
  - "Constructing per-works spill trajectories across reporting years."
tags:
  - annual-return
  - unit-of-analysis
  - monitor-level
  - data-integrity
  - record-linkage
  - duplicate-rows
  - convention
  - r
related_components:
  - annual-return-lookup
  - record-linkage-graph
  - create-unique-spill-sites
  - canonical-site-mapping
---

# Annual return rows are monitor-level, not works-level: within-works duplicate identifiers are real

## Context

While evaluating the RF matcher in `scripts/R/03_data_enrichment/create_annual_return_lookup.R`
(2026-06-10, branch `jo/annual_lookup_check`), 41 of its 50 proposed links turned out to pair
rows that are *literally identical* on every identifier the matcher uses. Investigating those
rows settled a domain fact about the Annual Return EDM's unit of analysis. An earlier
investigation (2026-06-09) had already characterized the same-year conflict components as
"multi-discharge facilities, not matching noise" and decided to treat distinct outlet/activity
records as distinct sites (session history) — this doc grounds that decision in per-monitor
`unique_id` evidence.

## Guidance

**One annual-return row is one monitored discharge point** (storm tank, inlet SO/CSO, network
outfall) — not one works. A single works can carry several monitors that are
**indistinguishable on every identifier field** (site names, permits, activity reference, NGR)
while carrying entirely different spill data.

Rules that follow:

1. **Never aggregate annual-return rows to works level.** From 2023 onward every monitor has
   its own `unique_id` (re-keyed yearly; bridged by `unique_id_2023` in the 2024 return), so
   aggregation breaks unique-ID alignment — and it destroys real variation (see Oldham below:
   0 vs 1,021 spill hours at one works in one year).
2. **The deterministic matcher's 1:1 ambiguity rule is correct for monitor-multiples.** When
   several rows share all identifiers, there is no principled basis for pairing them with a
   specific monitor in another year. Pre-2023 rows have no monitor-level key, so 2021/2022
   monitor-multiples are **genuinely untrackable across years** — leaving them unmatched is
   the honest output. They are real panel attrition, not linkage failure.
3. **Any matcher that pairs them anyway is assigning monitors at random.** RF scored such
   pairs at probability ≈ 1.0 (every feature agrees), but a perfect feature match on a
   shared-identifier group signals shared *works* identity, not the same *monitor*. Random
   assignment scrambles within-works spill trajectories across years.

Diagnostic for finding these groups:

```r
# within-year monitor groups identical on all matcher identifiers
edm |>
  group_by(year, water_company, site_name_ea, site_name_wa_sc,
           permit_reference_ea, permit_reference_wa_sc,
           activity_reference, outlet_discharge_ngr) |>
  filter(n() > 1) |>
  summarise(n_monitors = n(), .groups = "drop")
```

Scale on current data (2021–2024): **89 groups, 199 rows** (excess rows by year: 43/25/21/21);
worst case CHIGWELL ROAD, WOODFORD GREEN (Thames Water), 8 rows in 2023. In 2023/2024, **all
31 such groups carry distinct per-monitor unique_ids** — confirming the unit of analysis.

## Why This Matters

The finding changes the interpretation of every unmatched singleton in the lookup. Of the 774
singletons left by deterministic matching (515 in 2021 / 178 in 2022 / 20 in 2023 / 61 in
2024), the 2021/2022 concentration is *expected*: works with several monitors cannot be
tracked monitor-by-monitor into the unique-ID era. These are not missing links to chase. It
also explains why fuzzy/probabilistic matching shows inflated yields on this data — 41 of
RF's 50 "rescues" were monitor-multiples paired at random — and why the refactor plan adds an
ambiguity guard that flags identifier-identical groups instead of linking them.

## When to Apply

- Building works-level analyses: aggregate **after** linking, deliberately, and document the
  choice — never collapse rows before matching.
- Extending the lookup to a new year: run the diagnostic above first; expect monitor-multiples
  and verify their unique_ids are distinct.
- Reading singleton counts as a linkage-quality metric: discount the monitor-multiples first.
- Reviewing any matcher proposal whose evidence fields are all exactly equal: treat it as an
  ambiguous monitor-multiple, not a high-confidence match.

## Examples

OLDHAM WWTW (United Utilities), 2023 — four monitor rows, identical on names, permit
(`016950038`) and NGR (`SD8937004430`):

| year | unique_id | activity | asset_type | spill_hrs_ea |
|---|---|---|---|---|
| 2023 | UUG0690 | ST | Storm tank at WwTW | 0.00 |
| 2023 | UUG0691 | SO | Inlet SO at WwTW | 0.00 |
| 2023 | UUG0692 | ST | Storm tank at WwTW | 11.53 |
| 2023 | UUG0693 | ST | Storm tank at WwTW | 1,021.38 |

In 2024 the same four monitors reappear with fresh unique_ids (`UUP01458`–`UUP01461`), mapped
back via `unique_id_2023`. Works-level aggregation would collapse a 1,000+ hour spread into
one number; arbitrary cross-year pairing of the three `ST` rows (as the unguarded RF did for
their 2021/2022 counterparts) would attach one monitor's 2021 spills to a different monitor's
2022 record.

## Related

- `docs/solutions/tooling-decisions/annual-return-lookup-rf-matcher-light-trainer-threshold.md`
  — companion decision doc; the 41 artifact links and the ambiguity guard.
- `docs/solutions/logic-errors/annual-return-lookup-same-year-component-conflicts.md` — the
  same-year invariant; monitor-multiples are the dominant real-world source of those conflicts.
- `docs/plans/2026-06-10-004-refactor-annual-return-lookup-decomposition-plan.html` — unit U8
  documents this limitation in the solution note and adds a characterization test for the
  duplicate-group count.
- `docs/solutions/best-practices/annual-return-combiner-simplification-20260310.md` — upstream
  combiner that produces the annual-return parquet whose rows this convention interprets.
- `CONCEPTS.md` — "Monitored Discharge Point" entry.
