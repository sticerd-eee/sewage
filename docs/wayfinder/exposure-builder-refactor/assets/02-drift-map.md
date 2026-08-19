# Drift map: independent spill-exposure aggregations across the repo

Produced 2026-08-17 by a repo-wide search agent for ticket 07. It answers:
which scripts implement spill-exposure aggregation independently of the
prior-exposure engine, and where do their methodologies diverge? Caveat: the
study-period observations describe `main`'s EA-based
`cross_section_study_period_utils.R`; branch `jo/cross-section-individual-edm`
(ticket 04) moves `study_period` to an event basis, which resolves the
measurement-basis divergence for that dataset but not the machinery
duplication.

## The engine's distinguishing methodology

`scripts/R/utils/prior_exposure_utils.R` computes, per transaction and
radius, summed spill hours and 12/24-block spill counts over sites within
the radius, over the window `[2021-01-01 UTC, transaction endpoint)`.
Distinguishing features: event-level clamping to the window with an
`event_hours > 0` filter; `count_spills()` recomputed on the clamped event
set; dedupe to one row per transaction-site pair taking `min(distance_m)`;
radius reduction by distance-ordered cumulative sums; missing-site poisoning
(`site_missing` / `has_missing_site` set `spill_hrs` / `spill_count` to NA);
the 30-complete-days eligibility rule; per-transaction denominators; and
deliberate float-stability wrappers (`prior_exposure_stable_sum` /
`_cumsum`) with explicit ordering before every reduction. Radii 250/500/1000.

`spill_aggregation_utils.R` is a leaf utility (the shared `count_spills()`
12/24 counter, `calculate_spill_hours()`, calendar-year clamping), not a
competing engine. `site_group_utils.R` holds no spill arithmetic.

## Independent implementations and their divergences

1. **Study-period engine** (`scripts/R/utils/cross_section_study_period_utils.R`,
   collapse at L130-198, reduction at L514-608, as on `main`). Publishes a
   schema field-for-field near-identical to the engine's radius outputs,
   including identically-named `spill_count_weekly_avg` / `spill_hrs_daily_avg`
   columns, but shares no code. Divergences: (a) measurement basis is EA
   annual returns, never `count_spills()` (changes with ticket 04's branch);
   (b) fixed 2021-2024 window with constant denominator 1461, versus
   per-transaction days with the 30-day minimum; (c) radius reduction
   re-sums from scratch per radius with plain `base::sum` / `base::mean`,
   no stable-sum wrappers — mathematically equivalent to the engine's
   cumulative reduction, bit-wise not; (d) missingness predicate is
   `evidence_unknown` from `annual_status ∈ {reported_na, absent}` versus
   the engine's Site Group prefix-missingness flags (the harmonized rule
   from the charter supersedes both); (e) it materialises
   `spatially_eligible = FALSE` all-NA rows for coordinate-less
   transactions, a concept the prior engine lacks; (f) `mean_distance`
   relies on validated pair uniqueness rather than defensive
   `min(distance_m)` dedupe.

2. **Study-period re-implementations over `agg_spill_yr.parquet`**:
   `grid_long_difference_sales.R` (L240-302, grid means L320-345),
   `grid_long_difference_rentals.R` (L219-280, L299-325),
   `hedonic_continuous_full.R` (L138-155, L220-236), `hedonic_bins_full.R`
   (L152-155, L235-238). Four copies of "sum yearly site totals within
   250 m", each without `na.rm`, so one missing site-year silently NAs a
   property (and in the grid scripts one NA property nulls a whole
   cell-year); no missingness flag is published; `hedonic_continuous_full.R`
   divides by a hard-coded 1095 days that is wrong if the panel reaches
   2024 (flagged as a background task outside this map); and summing
   calendar-year `count_spills()` totals is not the engine's statistic
   because 12/24 blocks reset at each 1 January.

3. **`did_trends_full.R`** (L102-105, L128-133, L182-194): same yearly basis
   but with `na.rm = TRUE` and `replace_na(..., 0)` — missing evidence is
   coerced to zero exposure, the exact opposite of the engine's NA-poisoning
   rule. Sales and rentals also take different routes to the same radius
   semantics.

4. **`repeat_sales.R` (09_analysis)** (L164-179, L195-205, L223-233): a
   lagged 4-quarter rolling window per site, `na.rm = TRUE` cross-site sums,
   warm-up-quarter drops instead of the 30-day rule, and quarter-boundary
   resets of the 12/24 counter. A fifth windowing scheme.

5. **`upstream_downstream_*.R` family** (twelve live scripts plus an
   archive; for example `upstream_downstream_prior.R` L172-185, L293-305):
   consume the engine's site-grain output but re-implement the radius
   reduction with `na.rm = TRUE`, so sites the engine deliberately set to
   NA contribute zero; the radius is re-applied via
   `spill_house_euclid_m <= RAD` rather than by the partition alone. The
   directional split cannot be done post-reduction, so re-reduction is
   intentional, but the NA convention silently inverts the engine's central
   missingness rule and is repeated across twelve files.

6. **`aggregate_spill_stats.R` (03_data_enrichment)**: not a duplicate but
   the shared upstream for all `agg_spill_*` consumers; its zero-fill rule
   applies only to monthly/quarterly outputs while `spill_count_yr` falls
   back to NA outside `reported_zero` — this is the NA that the no-`na.rm`
   sums propagate and the `na.rm = TRUE` sums swallow.

## Pure consumers (no drift risk)

The six panel builders in `06_analysis_datasets` (`house_panel_within_radius`,
`rental_panel_within_radius`, `sale_panel_exp`, `rental_panel_exp`,
`site_panel_sales`, `site_panel_rental`) hold no spill arithmetic; their
radius grid is 250/500/1000/2000 versus the engine's 250/500/1000, applied
with the same inclusive `<=`. `cross_section_sales.R` /
`cross_section_rental.R` are thin entry points delegating to the
study-period util. In `09_analysis`, the `*_prior*` hedonic, DiD, and
event-study families, the `longdiff_*` scripts, and the descriptive scripts
consume published columns as-is.

## Ranked drift risk

First, the two full engines publishing identically-named but
differently-defined columns into sibling directories (item 1) — partially
addressed by ticket 04's branch and the shared measurement core (ticket 03).
Second, the four `agg_spill_yr`-based copies of study-period summation with
inconsistent NA conventions feeding headline results (items 2 and 3).
Third, the `na.rm = TRUE` re-reduction in the upstream/downstream family
(item 5), which will not track any future change to the engine's missingness
flags and must be fixed twelve times.
