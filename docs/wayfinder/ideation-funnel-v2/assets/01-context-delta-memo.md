# Context delta memo — what changed since the first run's context pack (2026-07-04 → 2026-07-09)

This memo records the current facts that the run-2 context-pack agents' instructions
must be written against. It was compiled on 2026-07-09 by three parallel research
agents (paper draft, analysis data surface, repo history), without opening anything
under `docs/ideas/run1/`. Where the first run's context pack or this planning effort's
own assumptions turn out to be wrong, the correction is stated explicitly.

## Headline corrections to assumptions this planning effort was carrying

1. **The repeat-transactions rebuild has NOT happened.** It exists only as a plan
   document plus a review note on the unmerged branch `jo/repeat-transactions-rebuild`
   (single commit `73d9e96`, 2026-07-07, adding
   `docs/plans/2026-07-07-001-refactor-repeat-transactions-rebuild-plan.md` and
   `todos/2026-07-07-review-repeat-rentals-sales.md`). On `main`, `rental_id` and
   `house_id` are still positional row numbers assigned by `.GRP`
   (`scripts/R/06_analysis_datasets/repeat_sales.R:182`), the distance-summary stage
   still exists and still writes `repeated_sales_summary.parquet` /
   `repeated_rentals_summary.parquet`, and the rental spill lookup still uses the
   temporary 5 km radius. A context pack must describe the hashed-ID rebuild as a
   pending proposal, not a shipped state.

2. **The news and daily spill-rainfall scripts are not new code.**
   `scripts/R/02_data_cleaning/clean_lexis_nexis_search1.R` (restored 2026-07-01) and
   `scripts/R/03_data_enrichment/aggregate_daily_spill_rainfall.R` (from 2026-03-08)
   pre-date the window. What changed on 2026-07-07 (commit `c7214cd`) is
   documentation only: both were folded into the execution-order list in
   `docs/pipeline_documentation.md` and the pipeline was renumbered to 40 steps.

3. **The one genuinely merged change is the location-merge rebuild** (merge commit
   `979cdab`, 2026-07-06), fully implemented with its five output artifacts
   regenerated on disk on 2026-07-06.

4. **The `09_analysis` loader is dormant.** No analysis script sources
   `scripts/R/09_analysis/00_data_load/load_data_sewage.R`; the orchestrator
   `run_all_analysis.sh` has the loader line commented out (lines 51–52), and every
   active analysis script re-loads its own parquet inputs directly. The loader
   remains the canonical statement of the general-panel and within-radius-panel data
   surface, but "what the loader reads" and "what the analyses read" are different,
   wider sets — both are inventoried below.

5. **There is no `docs/pipeline/` directory.** The execution-order documentation is
   the single file `docs/pipeline_documentation.md`.

## (a) Current state of the paper draft

### Where it lives

The formal manuscript is a LaTeX draft OUTSIDE the git repo, on Dropbox/Overleaf at
`/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters`
(path documented in `AGENTS.md:14-15`; title page lists Clare Balboni and Swati
Dhingra). Context-pack agents that need the draft must read it from that path; it
has no git history, so recency comes from file modification times. A separate
in-repo Quarto book at `book/` ("Sewage in Our Waters", single-authored by Jacopo)
is the empirical companion and contains no theory or welfare section.

### Section structure (compiled order from `_main.tex`)

Introduction; Context and data (storm overflows, UK CSO infrastructure, monitoring,
data subsections including data integration and the analysis sample); Motivating
evidence (spatial dispersion, population near overflows, persistence, price
correlations, and a "changes on changes" subsection covering long differences and
repeat sales à la Palmquist); Causal impacts of spills on house prices (upstream
versus downstream, publicity, policy reforms including EDM timing and Thames
Tideway); Overview of Hydraulics Instrument; Research questions and policy
counterfactuals; the model section (titled "A Quantitative Spatial Equilibrium with
Inherited Sewer Infrastructure", labeled "Model Sketch" in the master file); then
appendices (glossary, descriptives, results, dry spills, data, infrastructure
costs, and a 144-byte identification-strategies-comparison stub).

### Recent effort is concentrated on the model

`07_model.tex` was edited 2026-07-07 (the most recent file in the manuscript); the
alternative full model draft `106_model_logit.tex` was edited 2026-06-30 but is NOT
`\input` in `_main.tex`, so it is not compiled. The empirical body was last touched
2026-06-26.

### What the model section now contains

This is a substantial, largely complete quantitative spatial equilibrium, not a
placeholder. It builds on Monte, Redding and Rossi-Hansberg (2018), with
heterogeneous workers à la Diamond (2016) and historical state dependence à la
Allen and Donaldson (2023). The stated novel object is an "infrastructure
transmission technology" mapping modern urban activity and rainfall into
environmental disamenities through the inherited Victorian sewer stock. Two-stage
timing: a period-0 Victorian planner chooses sewer topology to minimize expected
lifetime sanitation cost, producing the inherited infrastructure; a period-1 modern
equilibrium takes it as fixed. Sewage exposure enters residential amenities
linearly; a structural house-price equation is derived from within-neighbourhood
indifference and links directly to the reduced-form hedonic estimates. An
infrastructure-transmission block builds hydraulic load, predetermined CSO
vulnerability, hydraulic head, spills, and river-weighted transmission to houses.
An identification bridge motivates the rainfall-times-vulnerability instrument that
`05_hydraulics_instrument.tex` develops empirically.

**The welfare block exists but is disabled.** Expected utility (the welfare object)
is stated live, but the entire compensating-variation derivation — per-worker CV,
general-equilibrium aggregation, and the house-price capitalization object — is
wrapped in `\begin{comment}...\end{comment}` (`07_model.tex:507-594`) and does not
compile. The four policy counterfactuals in `06_research_question.tex` (eliminate
all spills; eliminate dry spills; spatially reallocate spills; local versus
national regulation) are prose framing with bracketed research gaps.

### Headline empirical estimates as currently stated in the draft

All are semi-elasticities of log price on weekly spill count within a radius.
Sample: over 1 million spill events, 3.15 million sales, and 1.45 million rental
transactions, 2021–2023.

- Baseline hedonic correlation at 250 m (`03_motivating_evidence.tex:141`): one
  additional spill per week (about 1.2 standard deviations of exposure) is
  associated with 0.8% lower house prices and 0.6% lower rents in the most
  conservative specification.
- Quartile bins (`03_motivating_evidence.tex:179`): raw correlations of 1.3% (sales)
  and 4.2% (rentals) lower prices in the lowest non-zero spill quartile, and 13.0%
  (sales) and 7.5% (rentals) in the top quartile, attenuating with location fixed
  effects.
- Upstream versus downstream (`04_causal_impacts.tex:81`): one additional weekly
  spill upstream is associated with 1.7% lower sale prices and 2.1% lower rents;
  downstream, 2.6% lower sale prices and 1.3% lower rents. Distance weighting
  attenuates these substantially, with the downstream distance-weighted coefficient
  negligible.
- Nearest-site interaction (`04_causal_impacts.tex:133`): 2.8% lower sale prices and
  1.5% lower rents for an upstream relative to a downstream nearest site.
- A rendered RA report (`docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html`)
  gives daily-spill-count coefficients (log price, property controls plus MSOA fixed
  effects): sales −0.06 at 250 m, −0.02 at 500 m, −0.09 at 1000 m; rentals −0.02,
  −0.02, −0.03 (all significant, 250 m rentals at the 10% level).

### Known gaps and disabled material in the draft

The instrumental-variable subsection of the causal-impacts section
(`04_causal_impacts.tex:286-401`) is fully commented out. The estimation-strategy
input in `_main.tex:90` is commented out. `05_hydraulics_instrument.tex` contains
three `\section` headers and two parallel sets of identification subsections — an
unconsolidated working file. The research-question section carries bracketed author
TODOs (missing estimates, citations, engineering-literature and regulatory-structure
questions). The identification-strategies-comparison appendix is an empty stub.

## (b) The analysis data surface

### What the canonical loader reads

`scripts/R/09_analysis/00_data_load/load_data_sewage.R` (dormant but canonical; see
headline correction 4) assembles four in-memory frames — `dat_sales`, `dat_rent`,
`dat_sales_within`, `dat_rent_within` — from:

1. `data/processed/house_price.parquet` — one row per house transaction; 3,175,951
   rows; keys `house_id` + `transaction_id`; coverage 2021–2023 (`qtr_id` 1–12),
   England and Wales Land Registry; outcome `price`; geography (easting/northing,
   lat/long, postcode, LSOA/MSOA, region) and property attributes.
2. `data/processed/zoopla/zoopla_rentals.parquet` — one row per rental listing;
   1,450,255 rows; key `rental_id`; coverage `qtr_id` 1–12; outcome `listing_price`;
   bedrooms, bathrooms, property type, EPC fields.
3. `data/processed/agg_spill_stats/agg_spill_dry_qtr.parquet` — one row per spill
   site × quarter; 223,840 rows; 13,990 distinct `site_id` (now the works-grain
   key); `qtr_id` 1–16 (spills run to 2024, a year past the transactions); 10 water
   companies. Carries `spill_count_qt`, `spill_hrs_qt`, the new crosswalk-sourced
   `spill_count_ea_crosswalk` and `spill_hrs_ea_crosswalk`, `annual_status`, and a
   family of dry-spill variants
   (`dry_spill_{count,hrs}_qt_r{0,1}_d{01,0123}_{weak,strict}`).
4. `data/processed/general_panel/{sales,rentals}/` — Arrow datasets, one row per
   property × site × quarter within a radius, hive-partitioned by `radius`
   (250/500/1000/2000); sales partition 251,623,860 rows. The loader filters to
   `radius == 250`.
5. `data/processed/within_radius_panel/{sales,rentals}/` — the site-anchored
   counterpart, partitioned by `radius` and `period_type` (monthly/quarterly);
   sales partition 41,940,706 rows. The loader filters to 250 m quarterly.

Three loader paths are declared but never consumed (the monthly aggregation
`agg_spill_dry_mo.parquet` and the two `agg_spill_stats_{qtr,mo}.parquet` files),
and the 2000 m and monthly partitions on disk are never selected — intended-but-
inactive inputs.

### What the active analysis scripts read directly (beyond the loader)

- `02_hedonic` reads `house_price.parquet` and `data/processed/cross_section/{sales,rentals}/`.
- `03_repeat_sales` reads `data/processed/repeated_transactions/` (current grain
  `house_id` + `repeat_id`, 3,166,755 rows, positional IDs) plus
  `spill_house_lookup.parquet` and `zoopla/spill_rental_lookup.parquet`.
- `04_long_difference` reads `data/processed/long_difference/long_diff_grid_*.parquet`.
- `07_dry_spills` reads `agg_spill_stats/agg_spill_daily.parquet` (the balanced
  site-day panel, roughly 599 MB), `unique_spill_sites.parquet`, and
  `data/processed/rainfall/` artifacts.

Notable dormant assets no analysis currently reads include the aggregation variants
(`agg_spill_dry_yr`, `agg_spill_mo`, `agg_spill_qtr`, `agg_spill_yr`, the two
`agg_spill_stats_*` files), `never_spilled_sites/`, `pop_stats/`, `lexis_nexis/`
outputs, `housing_graph_data/`, `UpstreamDownstream/`, and the consent-discharges
database.

## (c) Pipeline changes since 2026-07-04 that a context pack must reflect

### The location-merge rebuild (merged 2026-07-06, commit `979cdab`)

The old merge matched 7.38 million events row by row through windfall key
enumeration, a max-to-one merge that selected candidates on the treatment variable
(largest `spill_hrs_ea`), and EM fuzzy matching, and had 25 open review findings
including silent row loss. It was rebuilt around a **works register**: a Works is
the year-invariant collapse of all monitored outlets filing annual returns under
the same company plus normalised site name with a corroborator (identical EA permit
reference or National Grid Reference distance of at most 250 m); connected
components are works; the representative id is the smallest member `site_id`
(`CONCEPTS.md:24-32`). Matching now happens once per distinct identifier tuple at
works grain through a Tier-1 ordered exact-key ladder and a narrow Tier-2
aggregate-agreement stage; there is no fuzzy stage and no magnitude tiebreak; a
manual-overrides CSV replaces them.

`data/processed/matched_events_annual_data/` now holds five files (all regenerated
2026-07-06): `site_works_crosswalk.parquet` (the canonical artifact — one row per
works-year over a full works × years grid, 55,960 rows, 13,990 works, 2021–2024,
with summed EA totals, `annual_status`, membership columns);
`matched_events_annual_data.parquet` (pure event grain, 7,271,711 rows — the old
NA-timestamp pseudo-rows are gone); `events_unmatched.parquet` (reason-coded);
`annual_unmatched.parquet`; and `near_miss_report.parquet`. The previously-unread
`site_metadata.parquet` is retired. Every works-year now carries an explicit
`annual_status` in {reported_zero, reported_positive, reported_na, absent} — the
roughly 4,100 reported-NA rows the old script silently dropped are retained.

Downstream, `create_unique_spill_sites.R` and `aggregate_spill_stats.R` were
migrated to the crosswalk, and the site-keyed consumers (rainfall cleaning and
aggregation, dry-spill identification, and both 10 km property-site match scripts)
were verified or regenerated on works grain (commit `aa039a0`). The effect on the
analysis surface is described in section (b): `agg_spill_dry_qtr.parquet` and both
exposure panels now carry the works-grain `site_id`, and the quarterly aggregation
gained the two `*_ea_crosswalk` columns. All 25 old merge findings are retired
(`todos/_archive/011-done-p0-location-merge-findings.md`).

Authoritative sources: decision record
`docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md`
(decisions D1–D12), execution plan
`docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md`,
and the glossary entries in `CONCEPTS.md`.

### The repeat-transactions rebuild (planned only — unmerged)

See headline correction 1. If executed, the plan would replace positional IDs with
content-stable xxhash64 strings, remove 3,453 exact-duplicate Zoopla rows before ID
assignment, collapse the two repeat scripts into a shared utilities module, delete
the distance-summary stage, reduce the mapping output to three columns at
transaction grain with singles included, and rebuild the rental spill lookup at
10 km. None of this is on `main` or on disk.

### Documentation and execution order

Commit `2abb6c0` (2026-07-06) fixed a dependency-order bug in
`docs/pipeline_documentation.md` (the merge was listed before the lookup it depends
on) and rewrote the merge bullet around the five crosswalk outputs. Commit
`c7214cd` (2026-07-07) then added the news script (Layer 02 step 11, producing
`search1_monthly.parquet` for the `05_news` analysis), the daily spill-rainfall
script (Layer 03 step 21, producing the balanced site-day panel feeding
`07_dry_spills` and `01_descriptive`), and four previously-omitted Layer 06 scripts
(the two prior-exposure scripts and the two repeat scripts), renumbering the
pipeline to 40 steps.

### Other plan documents since 2026-07-04 (excluding ideation)

`docs/plans/2026-07-05-002-merge-rebuild-chunk-prompts.md` holds the ten-chunk
execution prompts for the merge rebuild.
`docs/plans/2026-07-01-001-feat-lexisnexis-llm-relevance-geotag-ra-issue.md`
(committed 2026-07-04) is an RA-issue proposal for LLM relevance filtering and
geo-tagging of LexisNexis articles — a proposal, not a locked build.
