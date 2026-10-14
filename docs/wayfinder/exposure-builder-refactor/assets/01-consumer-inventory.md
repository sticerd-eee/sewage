# Consumer and contract inventory for the six exposure datasets

Date: 2026-08-17. Prepared for ticket 01 of the exposure-builder-refactor map.

This memo inventories every in-repo consumer of the six cross-section exposure
datasets, records their published schemas, key grains, and the contract scripts
that pin them, summarizes what branch `jo/cross-section-individual-edm` changes
relative to `main`, and lists the analysis outputs that sit downstream of these
datasets. Every script path named here was verified to exist, either on the
current worktree (branch `claude/scripts-review-9fbbcf`, which matches `main`
for all files discussed) or, where explicitly noted, on branch
`jo/cross-section-individual-edm`.

## 1. The six datasets and where they live on disk

The published locations nest the dataset name under a market directory, so
searches for `cross_section/<name>` alone find nothing; the paths are built
with `here::here("data", "processed", "cross_section", "<market>", "<name>")`.

| Dataset | Published path | Builder script | Engine |
|---|---|---|---|
| `prior_to_sale` | `data/processed/cross_section/sales/prior_to_sale/` | `scripts/R/06_analysis_datasets/cross_section_prior_to_sale.R` | `scripts/R/utils/prior_exposure_utils.R` |
| `prior_to_rental` | `data/processed/cross_section/rentals/prior_to_rental/` | `scripts/R/06_analysis_datasets/cross_section_prior_to_rental.R` | `scripts/R/utils/prior_exposure_utils.R` |
| `prior_to_sale_house_site` | `data/processed/cross_section/sales/prior_to_sale_house_site/` | `scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R` | `scripts/R/utils/prior_exposure_utils.R` |
| `prior_to_rental_rental_site` | `data/processed/cross_section/rentals/prior_to_rental_rental_site/` | `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R` | `scripts/R/utils/prior_exposure_utils.R` |
| `study_period` | `data/processed/cross_section/sales/study_period/` and `data/processed/cross_section/rentals/study_period/` | `scripts/R/06_analysis_datasets/cross_section_sales.R` and `scripts/R/06_analysis_datasets/cross_section_rental.R` | `scripts/R/utils/cross_section_study_period_utils.R` |
| `study_period_ea` | `data/processed/cross_section/sales/study_period_ea/` and `data/processed/cross_section/rentals/study_period_ea/` (branch only) | `scripts/R/06_analysis_datasets/cross_section_sales_ea.R` and `scripts/R/06_analysis_datasets/cross_section_rental_ea.R` (branch only) | `scripts/R/utils/cross_section_study_period_utils.R` (branch revision) |

`study_period_ea` and its two builders exist only on branch
`jo/cross-section-individual-edm`. On `main` there are no references to
`study_period_ea` anywhere in the repository.

## 2. Published schemas, key grains, and the contracts that pin them

### 2.1 Prior-exposure family (four datasets)

`prior_exposure_public_schema()` in `scripts/R/utils/prior_exposure_utils.R`
(lines 33 to 111) holds the four literal Arrow schemas. The comment above it
states the intent directly: the public matrix is intentionally closed, and the
schemas are kept literal so that field drift requires an explicit contract
change. All four datasets are Hive-partitioned by `radius` with radii 250, 500,
and 1000 metres, and the exposure window runs from 2021-01-01 to each
transaction's endpoint.

Radius grain (`prior_to_sale` uses `house_id` and int32 `price`;
`prior_to_rental` uses `rental_id` and float64 `listing_price`; all other
fields are shared):

| Column | Type | Notes |
|---|---|---|
| `house_id` or `rental_id` | utf8 | transaction identifier, nonmissing and nonempty |
| `price` or `listing_price` | int32 or float64 | transaction value |
| `n_days_in_window` | int32 | must be at least 30 for a published row |
| `spill_hrs` | float64 | NA when evidence is incomplete |
| `n_spill_sites` | int32 | |
| `spill_count` | float64 | NA when evidence is incomplete |
| `mean_distance` | float64 | |
| `min_distance` | float64 | |
| `has_missing_site` | bool | |
| `annual_returns_na_then_absent` | bool | flag for a nearby site that reported NA and then vanished from the register |
| `spill_count_daily_avg`, `spill_hrs_daily_avg` | float64 | finite or NA |
| `spill_count_weekly_avg`, `spill_hrs_weekly_avg` | float64 | weekly must equal daily times seven |
| `radius` | int32 | restored Hive field |

The radius-grain key is the pair of transaction identifier and `radius`
(`prior_exposure_public_key_columns()` at line 787 and the duplicate-key check
at lines 731 to 738 of `prior_exposure_utils.R`).

Site grain (`prior_to_sale_house_site` and `prior_to_rental_rental_site`) has
one row per transaction, site, and radius. Its columns are the transaction
identifier, the transaction value, `n_days_in_window`, `site_id` (int32),
`distance_m` (float64), `spill_hrs`, `spill_count`, `site_missing` (bool, the
per-site missing-evidence flag), the four daily and weekly averages, and
`radius`. The site grain does not carry `annual_returns_na_then_absent`; that
flag exists only at the radius grain, while the site grain exposes the
per-site `site_missing` flag instead. The site-grain key is the triple of
transaction identifier, `site_id`, and `radius`.

### 2.2 Study-period family

`study_period_public_schema()` in
`scripts/R/utils/cross_section_study_period_utils.R` (lines 39 to 83) holds
the two literal schemas. The sales schema has sixteen columns: `house_id`
(utf8), `price` (int32), `ppd_category` (utf8), `n_days_in_window` (int32),
`spill_hrs`, `n_spill_sites` (int32), `spill_count`, `mean_distance`,
`min_distance`, `spatially_eligible` (bool), `has_missing_site` (bool), the
four daily and weekly averages, and `radius` (int32). The rental schema is the
same without `ppd_category` and with `rental_id` and float64 `listing_price`.
The key is the pair of transaction identifier and `radius`, checked at line
434 of the utility file, and the output is Hive-partitioned by `radius`
(line 752) with the same three radii. On `main` the window is 2021-01-01 to
2024-12-31 for both markets; on the branch the rental window shortens to
2021-01-01 to 2023-12-31. On the branch, `study_period_ea` shares this schema,
grain, and window exactly; the two variants differ only in exposure source.

The study-period missingness rule lives in
`collapse_study_period_annual_returns()` (line 130 on `main`): a Site Group
whose window contains any `reported_na` or `absent` year, or any year missing
from the crosswalk grid, contributes unknown evidence, and a property with
such a site inside the radius gets NA `spill_count` and `spill_hrs`.

### 2.3 Contract and verification scripts

| Script | What it pins |
|---|---|
| `scripts/R/testing/test_prior_exposure_contracts.R` (2,008 lines) | The four literal schema signatures (`variant_schema_signatures`, line 1130), the builders' exact output paths, public key uniqueness, the 30-day minimum window, finite-or-NA rates with weekly equal to daily times seven, the exposure-evidence NA rules including the `annual_returns_na_then_absent` sequence semantics, staged publication and promotion, and streaming chunk-local invariants. It also sources and exercises the ID verifier. |
| `scripts/R/testing/test_cross_section_study_period_contracts.R` (1,105 lines on `main`) | Section U1 pins whole-year window authority, the two literal schemas, and the annual-return truth table; U2 pins row-group ownership and spatial status semantics; U3 pins the staged publication lifecycle; U4 pins the thin production adapters; U5 pins the direct consumer `cross_sectional_plots.R` (its preparation seams, its use of `study_period`, and the requirement that eligible zeros and unknown exposure stay distinct) plus the supported documentation. The branch adds roughly 480 lines covering the event-based collapse and the exposure-source dispatch and comparison invariants. |
| `scripts/R/testing/verify_id_artifact_match_rates.R` (234 lines) | Referential integrity: every nonmissing transaction ID in fourteen declared artifacts must match its cleaned source at exactly 100 percent. Five of the six datasets are in the inventory (`prior_to_sale`, `prior_to_rental`, `prior_to_sale_house_site`, `prior_to_rental_rental_site`, and both `study_period` markets). `study_period_ea` is not in the inventory, on `main` or on the branch. Writes `output/log/id_artifact_match_rates.csv`. |
| `scripts/R/testing/test_verify_id_artifact_match_rates_contracts.R` (73 lines) | Pins the verifier's specification inventory to exactly the fourteen declared artifacts and their source paths. |
| `scripts/R/testing/verify_study_period_exposure_sources.R` (283 lines, branch only) | Asserts that the event-based `study_period` and the Annual-Returns `study_period_ea` agree exactly on the public key set and on which rows carry NA exposure, and reports (without asserting) the distributional differences. Writes `output/log/verify_study_period_exposure_sources.log`. |

`scripts/R/testing/site_grain_consumer_manifest.R` is adjacent rather than a
contract on the published datasets: it is a manifest of scripts that consume
site-grain artifacts upstream of publication, and it lists the two radius-grain
prior builders as consumers of Site Group annual status.

## 3. Consumers per dataset

Builders are excluded from the consumer lists (they are the producers).
Everything below reads the published dataset with `arrow::open_dataset()`. A
recurring pattern matters for the NA-harmonization plan: no live consumer of
the radius-grain prior datasets reads `has_missing_site`, and no live consumer
of the site-grain datasets reads `site_missing`; consumers instead inherit the
NA policy implicitly by dropping rows where the exposure averages are NA. On
`main`, no analysis script reads `annual_returns_na_then_absent` at all; the
single manual exclusion exists only on the branch, in
`hedonic_continuous_prior.R`.

### 3.1 `prior_to_sale` and `prior_to_rental` (radius grain)

Every consumer below reads both datasets unless noted. All apply the same core
masking themselves: they filter to one radius, keep `n_spill_sites > 0`, and
drop rows whose exposure averages are NA.

| Consumer | Columns read from the dataset | Own masking and exclusion logic |
|---|---|---|
| `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R` | `house_id`/`rental_id`, `n_spill_sites`, `spill_count_weekly_avg`, `spill_hrs_weekly_avg`, `radius` (the dataset's own price column is dropped and rejoined from the cleaned source) | Loops radii 250, 500, 1000; keeps `n_spill_sites > 0`; drops NA weekly averages and NA controls. On the branch it additionally excludes rows where `annual_returns_na_then_absent` is TRUE. |
| `scripts/R/09_analysis/02_hedonic/hedonic_bins_prior.R` | identifier, `n_spill_sites`, `spill_count_daily_avg`, `spill_hrs_daily_avg`, `radius` | Single radius 250; keeps `n_spill_sites > 0`; bins the daily averages and drops rows whose bin is NA (which removes NA exposure). |
| `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior_qtr_fe.R` | identifier, `n_spill_sites`, `spill_count_weekly_avg`, `spill_hrs_weekly_avg`, `radius` | Loops radii 250, 500, 1000; keeps `n_spill_sites > 0`; drops NA weekly averages; adds quarter fixed effects from the source join. |
| `scripts/R/09_analysis/05_news/did_articles_prior.R` | identifier, price column, `n_spill_sites`, `spill_count_weekly_avg`, `radius` | Loops radii; keeps `n_spill_sites > 0`; drops NA `spill_count_weekly_avg`. |
| `scripts/R/09_analysis/05_news/did_articles_lag4_prior.R` | same as `did_articles_prior.R` | Same pattern at a single radius. |
| `scripts/R/09_analysis/05_news/did_articles_windowed_prior.R` | same as `did_articles_prior.R` | Same pattern; also requires finite article log columns. |
| `scripts/R/09_analysis/05_news/did_articles_lagged_sales.R` | selects exactly `house_id`/`rental_id`, `price`/`listing_price`, `spill_count_weekly_avg` | Filters `radius == <RAD>` and `n_spill_sites > 0` inside the Arrow query; drops NA `spill_count_weekly_avg`. |
| `scripts/R/09_analysis/05_news/did_trends_lagged_sales.R` | same as `did_articles_lagged_sales.R` | Same pattern. |
| `scripts/R/09_analysis/05_news/did_trends_prior.R` | identifier, price column, `n_spill_sites`, `spill_count_weekly_avg`, `radius` | Loops radii; keeps `n_spill_sites > 0`; drops NA `spill_count_weekly_avg`. |
| `scripts/R/09_analysis/05_news/es_trends_prior.R` | same as `did_trends_prior.R` | Single radius; same pattern. |
| `book/scaled_effects.qmd` | identifier, `n_spill_sites`, `spill_count_daily_avg`, `spill_hrs_daily_avg`, `radius` | Radius 250; keeps `n_spill_sites > 0`; drops NA daily averages and non-finite log prices. |
| `scripts/R/testing/test_lsoa_variation_updown_prior.R` | identifier, `n_spill_sites`, `spill_count_daily_avg`, `radius` | Keeps `n_spill_sites > 0`; drops NA `spill_count_daily_avg`; decomposes exposure variation within and across LSOAs. |
| `scripts/R/testing/spill_count_variation_share_prior.qmd` | identifier, `n_spill_sites`, `spill_count_daily_avg`, `radius` | Keeps `n_spill_sites > 0`; drops NA `spill_count_daily_avg`. |
| `scripts/R/testing/london_total_shares_houses_spills.qmd` (`prior_to_sale` only) | `house_id`, `n_spill_sites`, `spill_count_daily_avg`, `spill_hrs_daily_avg`, `radius` | Keeps `n_spill_sites > 0`; drops NA daily averages; computes London shares. |

The contract trio (`test_prior_exposure_contracts.R`,
`verify_id_artifact_match_rates.R`,
`test_verify_id_artifact_match_rates_contracts.R`) also touches both datasets
as described in section 2.3. Counting analysis, book, exploratory testing, and
contract scripts together, `prior_to_sale` has seventeen consumers and
`prior_to_rental` has sixteen (the London-shares notebook reads sales only).

### 3.2 `prior_to_sale_house_site` and `prior_to_rental_rental_site` (site grain)

Every consumer below reads both datasets. The fourteen live
upstream-downstream scripts share one template: open the site-grain dataset,
filter to a single radius (250 or a configured maximum distance), inner-join
property characteristics from the cleaned source and an upstream-downstream
signed-distance CSV keyed on the transaction identifier and `site_id`, then
drop rows with NA `spill_count_weekly_avg`, NA `spill_hrs_weekly_avg`, NA
controls, or NA direction. Variants differ in how they collapse the pair rows:
nearest site (sort on `distance_m` and keep the first row per property), only
site (keep properties whose group size is one), full sums across pairs,
distance rings cut on `distance_m`, or inverse-distance weighting.

Live upstream-downstream consumers, all under
`scripts/R/09_analysis/06_upstream_downstream/`:
`upstream_downstream_prior.R`, `upstream_downstream_prior_full.R`,
`upstream_downstream_prior_nearest_site.R`,
`upstream_downstream_prior_only_site.R`,
`upstream_downstream_full_all_radii.R`,
`upstream_downstream_nearest_all_radii.R`,
`upstream_downstream_only_site_all_radii.R`,
`upstream_downstream_nearest_by_bin.R`,
`upstream_downstream_nearest_vary_lateral.R`,
`upstream_downstream_nearest_vary_river.R`,
`upstream_downstream_decay_binary_did.R`, and
`upstream_downstream_decay_ring_triple.R` (twelve scripts; together with the
descriptive script and the book chapter below the live count per site-grain
dataset is fourteen analysis and book consumers). Columns read from the
datasets are
the transaction identifier, `site_id`, `distance_m`,
`spill_count_weekly_avg`, `spill_hrs_weekly_avg`, and `radius`; none reads
`site_missing`.

The other live consumers:

| Consumer | Columns read | Own masking and exclusion logic |
|---|---|---|
| `scripts/R/09_analysis/01_descriptive/property_spill_site_pair_count.R` | identifier, `site_id`, `distance_m`, `radius` | None. It collects the full dataset with no filters, so pair counts and mean distances include rows whose exposure is NA. |
| `book/scaled_effects.qmd` | identifier, `site_id`, `distance_m`, `spill_count_daily_avg`, `spill_hrs_daily_avg`, `radius` | Radius 250; joins the upstream-downstream CSV; drops NA daily averages, controls, and direction; keeps the nearest site per property. |
| `scripts/R/testing/test_lsoa_variation_updown_prior.R` | identifier, `site_id`, `spill_count_daily_avg`, `radius` and the join columns | Filters to one radius; joins direction data; drops NA `spill_count_daily_avg`. |
| `scripts/R/testing/explore_extensive_margin_news.qmd` | identifier, `site_id`, `distance_m`, `spill_count_daily_avg`, `spill_hrs_daily_avg` | Filters `radius == 1000`; keeps the nearest site per property; tolerates dataset absence with a `tryCatch` fallback. |

The same contract trio from section 2.3 covers both site-grain datasets.
Counting everything, each site-grain dataset has nineteen live consumers
(fourteen analysis and book, two exploratory testing, and three contract or
verification scripts), plus the nine archived consumers listed in
section 3.4.

### 3.3 `study_period` (and `study_period_ea` on the branch)

| Consumer | Columns read | Own masking and exclusion logic |
|---|---|---|
| `scripts/R/09_analysis/01_descriptive/cross_sectional_plots.R` | sales: `house_id`, `price`, `ppd_category`, `spill_count`, `spill_hrs`, `min_distance`, `spatially_eligible`, `has_missing_site`, `radius`; rentals: the same without `ppd_category`, with `rental_id` and `listing_price` | Requires `spatially_eligible` and `has_missing_site` to be present but does not filter on them. It trims to the 5th-to-95th price percentiles, optionally samples, and keeps configured radii. The U5 contract pins that eligible zeros and unknown (NA) exposure stay distinct through preparation. |
| `scripts/R/testing/test_cross_section_study_period_contracts.R` | full schema via fixtures and reopen checks | Contract script; see section 2.3. |
| `scripts/R/testing/verify_id_artifact_match_rates.R` | `house_id` and `rental_id` only | Referential-integrity tripwire; see section 2.3. |
| `scripts/R/09_analysis/02_hedonic/hedonic_bins_full.R` (branch version only) | identifier, `spill_count`, `spill_hrs`, `n_spill_sites`, `spatially_eligible`, `radius` | Filters `spatially_eligible` and `n_spill_sites > 0`, then bins the whole-period totals. On `main` this script reads `data/processed/general_panel/` instead and is not a consumer of these datasets. |
| `scripts/R/testing/verify_study_period_exposure_sources.R` (branch only) | keys plus `spill_count` and `spill_hrs` from both `study_period` and `study_period_ea` | Comparison verifier; see section 2.3. |

On `main`, `study_period` therefore has exactly one analysis consumer
(`cross_sectional_plots.R`) plus three contract or verification scripts. On
the branch it gains `hedonic_bins_full.R` and the exposure-source verifier.
`study_period_ea` has no analysis consumers at all; its only reader is the
branch-only comparison verifier, and it is absent from the ID-verifier
inventory.

For completeness: `book/house_data_exploration.qmd` and
`book/zoopla_data_exploration.qmd` read the retired `all_years` and
`prior_12mo` cross-sections, not any of the six datasets, and the U5 contract
pins that both notebooks stay out of the supported book render.
`scripts/R/testing/test_cross_section.Rmd` likewise reads only the retired
paths. The `07_dry_spills` scripts read `site_missing` from
`agg_spill_stats/agg_spill_daily.parquet`, which is not one of the six
datasets.

### 3.4 Archived consumers

All nine live under
`scripts/R/09_analysis/06_upstream_downstream/Archive/` and read both
site-grain datasets; none is invoked by `run_all_analysis.sh`.

- `260309_upstream_downstream_prior.R` reads both site-grain datasets for an older dated variant of the direction analysis.
- `upstream_downstream_prior copy.R` is a superseded copy of the live direction script.
- `upstream_downstream_prior.R` is the archived predecessor of the live script of the same name.
- `upstream_downstream_prior_full.R` is the archived full-sum variant.
- `upstream_downstream_prior_full_250m.R` is the archived full-sum variant fixed at 250 metres.
- `upstream_downstream_prior_nearest_site.R` is the archived nearest-site variant.
- `upstream_downstream_prior_nearest_site_250m.R` is the archived nearest-site variant fixed at 250 metres.
- `upstream_downstream_prior_only_site.R` is the archived single-site variant.
- `upstream_downstream_prior_only_site_250m.R` is the archived single-site variant fixed at 250 metres.

### 3.5 Which consumers the standard run executes

`scripts/R/09_analysis/run_all_analysis.sh` currently executes these consumers
of the six datasets: `hedonic_continuous_prior.R`, `hedonic_bins_prior.R`,
`did_trends_prior.R`, `es_trends_prior.R`, `did_articles_prior.R`,
`did_articles_windowed_prior.R`, and `did_articles_lag4_prior.R`. It also runs
`hedonic_bins_full.R`, which becomes a `study_period` consumer on the branch.
The `cross_sectional_plots.R` entry and the whole `06_upstream_downstream`
block are commented out, and the lagged-sales pair
(`did_articles_lagged_sales.R`, `did_trends_lagged_sales.R`) is not listed.

## 4. What branch `jo/cross-section-individual-edm` changes

The diff `git diff main...jo/cross-section-individual-edm --stat` touches 17
files with 1,607 insertions and 141 deletions.

Builders. `cross_section_sales.R` and `cross_section_rental.R` switch the
unsuffixed `study_period` outputs from Annual-Returns totals to matched
individual EDM events: events are clipped to the study window, spill hours are
summed clipped durations, and spill counts are recomputed under the EA 12/24
rule, which is the same measurement the prior-exposure family uses, so the two
families then differ only in window. The rental window shortens to 2021 to
2023 because no 2024 Zoopla data exist. Both builders gain an
`exposure_source` configuration field. Two new builders,
`cross_section_sales_ea.R` and `cross_section_rental_ea.R`, publish the
Annual-Returns measurement to the sibling `study_period_ea` paths.

Utilities. `cross_section_study_period_utils.R` gains roughly 171 net lines:
`study_period_clip_events()`, `collapse_study_period_events()`, an
`exposure_source` dispatch validated to `annual_returns` or `events`, and an
`study_period_annual_evidence_grid()` seam factored out of the existing
collapse. The Annual Returns remain the evidence oracle under both sources,
because the event feed carries positives only; the two outputs therefore share
a Site Group set and a missingness pattern by construction. The public schemas
and the key grain are unchanged.

Tests. `test_cross_section_study_period_contracts.R` gains roughly 480 lines:
new U1 sections proving the Annual-Returns collapse is unchanged by the source
dispatch and pinning the event-based collapse and its evidence flow, and new
U4 sections pinning the exposure-source configuration and the comparison
invariants. The new `verify_study_period_exposure_sources.R` (283 lines)
asserts exact key-set and NA-pattern agreement between `study_period` and
`study_period_ea` and reports distributional differences.

Analysis scripts. `hedonic_continuous_prior.R` adds the manual exclusion
`!annual_returns_na_then_absent` to both the sales and rental samples, with a
comment explaining that exposure is understated for those properties, and
rewrites the table notes for the asymmetric window (sales 2021 to 2024,
rentals 2021 to 2023). `hedonic_bins_full.R` migrates from the stale
`general_panel` inputs to the `study_period` cross-sections and now filters on
`spatially_eligible` with `n_spill_sites > 0`. `cross_sectional_plots.R`
changes only a slide-height constant. `google_trends_article_counts_combined.R`
fixes an axis-limit bug and is not a consumer of the six datasets.

Documentation. `CONCEPTS.md` introduces the two named variants (Event-Based
and Annual-Returns Study-Period Spill Exposure) and records that which variant
is canonical is an open question. `docs/pipeline_documentation.md` and
`book/data_clean_documentation/01_pipeline.qmd` are updated to match. Two plan
documents are added: `docs/plans/2026-08-17-001-event-based-study-period-cross-sections-plan.md`
and `docs/plans/2026-08-16-001-deck-output-refresh-checklist.md`.

## 5. Outputs downstream of the six datasets

A semantic change to exposure NAs (the harmonized rule producing extra NAs)
changes the estimation samples of every script in section 3, so the following
artifacts would need regeneration. The deck refresh checklist on the branch
(`docs/plans/2026-08-16-001-deck-output-refresh-checklist.md`) already
documents the propagation route for deck assets: each regenerated table in
`output/tables/` is converted by `scripts/python/convert_paper_tables_to_beamer.py`
into `slides/tables/` and the deck is recompiled, and figure PDFs are copied
into the Overleaf figures directory.

From `prior_to_sale` and `prior_to_rental`:

- `output/tables/hedonic_count_continuous_prior_<RAD>m.tex` and `output/tables/hedonic_hrs_continuous_prior_<RAD>m.tex` for radii 250, 500, 1000, plus `output/tables/hedonic_count_continuous_prior_radius_robustness.tex`. The 250-metre count table and the robustness table are on deck slides and in the paper's motivating-evidence section.
- `output/tables/hedonic_count_bins_prior.tex` and `output/tables/hedonic_hrs_bins_prior.tex`.
- `output/tables/hedonic_count_continuous_prior_qtr_fe_<RAD>m.tex` and `output/tables/hedonic_hrs_continuous_prior_qtr_fe_<RAD>m.tex`.
- `output/tables/did_articles_prior_<RAD>m.tex` (the 250-metre file is converted to the deck's `slides/tables/media_attention.tex`), `output/tables/did_articles_lag4_prior.tex`, `output/tables/did_articles_windowed_prior_<WIN>m.tex` with its comparison table and effect-size CSV, `output/tables/did_trends_prior_<RAD>m.tex`, `output/tables/es_trends_prior.tex`, and the figures `output/figures/es_trends_prior_sales.pdf` and `output/figures/es_trends_prior_rentals.pdf`.
- `output/tables/did_articles_lagged_sales_grid.tex` and `output/tables/did_trends_lagged_sales_grid.tex` with their effect-size CSVs, and downstream of those CSVs the combined outputs of `scripts/R/09_analysis/05_news/summarise_lagged_attention_sales.R`: `output/tables/did_news_lagged_sales_effect_sizes.csv` and the coefficient-path and intensive-extension figures in `output/figures/`.
- The rendered book chapter from `book/scaled_effects.qmd`.

From `prior_to_sale_house_site` and `prior_to_rental_rental_site`:

- `output/tables/property_spill_site_pair_count.tex`.
- The upstream-downstream table family in `output/tables/`: the `hedonic_count_continuous_prior_direction*` and `hedonic_hrs_continuous_prior_direction*` tables (including weighted and MSOA variants), the `hedonic_*_continuous_prior_nearest_site*` tables (including distance, lateral, river, and bin variants), the `hedonic_*_continuous_prior_one_site*` tables, `ud_did_count.tex`, `ud_did_hrs.tex`, `ud_decay_ring_triple_count.tex`, and `ud_decay_ring_triple_hrs.tex`, each with their `_distance` variants. The deck checklist notes that eight of these appendix tables are currently blocked on missing signed-pair CSVs owned by a colleague.
- The nearest-site sections of the rendered `book/scaled_effects.qmd` chapter.

From `study_period` (and, on the branch, `study_period_ea`):

- The cross-section binscatter figures from `cross_sectional_plots.R` in `output/figures/`: `sales_<variable>_<method>.pdf` and `rental_<variable>_<method>.pdf` for the distance, spill-count, spill-hours, and inverse-distance variables, plus the `_slides`, `_nolegend`, and `_slides_nolegend` variants and the shared `cross_section_radius_legend.pdf` and `cross_section_radius_legend_slides.pdf`. These feed both the deck and the paper's motivating-evidence figures.
- On the branch, `output/tables/hedonic_count_bins_full.tex` from the migrated `hedonic_bins_full.R`, and `output/log/verify_study_period_exposure_sources.log`.
- `output/log/id_artifact_match_rates.csv` from the ID verifier, which re-certifies five of the six datasets after any rebuild.

Two facts from the checklist are worth carrying into the harmonization
decision. First, the current all-or-nothing study-period NA rule already sets
36.4 percent of properties at the 1,000-metre radius to NA and removes the
densest-exposure properties disproportionately, which roughly doubled
per-spill slopes; the checklist flags this sample restriction as needing a
decision before further reruns. Second, the branch's
`annual_returns_na_then_absent` exclusion removes 1.66 percent of the
250-metre hedonic sample and moves the preferred sales estimate from
insignificant to significant at 5 percent, so any change to that flag's
semantics directly moves headline results.
