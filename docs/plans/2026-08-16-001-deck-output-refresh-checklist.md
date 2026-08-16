# Deck output refresh checklist — `short_pres.tex`

**Date:** 2026-08-16
**Scope:** outputs displayed in `slides/short_pres.tex` (Overleaf), plus hardcoded in-text numbers. Deck only.
**Policy:** every output extends to 2024 where data allows. Rentals stay 2021–2023 (no 2024 Zoopla data), so pooled tables have asymmetric windows — note this in table notes.
**Table pipeline reminder:** R script → `output/tables/*.tex` → `scripts/python/convert_paper_tables_to_beamer.py` → `slides/tables/*.tex` → recompile deck. Every table item below implies the converter + recompile steps.

## Sequencing prerequisites (before any Category 1/2 run that uses the prior-exposure cross-sections)

- [X] Wait for the in-flight `cross_section_prior_to_sale.R` run (started 2026-08-16 17:07) to finish.
- [X] Rerun `cross_section_prior_to_rental.R` so the rentals cross-section carries the new `annual_returns_na_then_absent` column and matches the sales schema.

## Category 1 — run today, no code edits

Inputs on disk are current (2024 sales already in cleaned data, panels, and cross-sections; spill aggregates carry 2024).

- [X] **Cross-section binscatter figures** (slide "Spill exposure is negatively correlated with property values": 4 panels + shared legend) — run `01_descriptive/cross_sectional_plots.R` → `sales/rental_{distance,spill_count}_lm_slides_nolegend.pdf`, `cross_section_radius_legend_slides.pdf`. Sales gain 2024 automatically via the study-period cross-section.
  Done 2026-08-16. The paper variants (`..._lm_nolegend.pdf` + `cross_section_radius_legend.pdf`, used by `03_motivating_evidence.tex`) were refreshed and copied too. Figure notes in both the paper and the deck now state the asymmetric window. Signs and radius ordering are unchanged, so all existing text claims still hold.

  **Exposure-measure change affecting every cross-section output.** The spill-count support fell from ~14,800 to ~4,400. Two distinct causes, neither of them the 2024 extension (site-record completeness is 80.4% over 2021--2023 vs 80.1% over 2021--2024):

  1. *Input rebuild (the larger effect).* `spill_house_lookup.parquet` was rebuilt 2026-08-14 22:37, after the previous figures were written at 13:12, as part of the stable-ID/Site Group work (`0972ed9`, `093f063`, `16f8723`). Reconstructing the **old** partial-credit rule on the **current** inputs gives a maximum of 5,004 spills within 1km across all 1.85M properties, untrimmed. The old ~14,800 is therefore not reproducible from current data under any aggregation rule, and looks like a join fan-out in the previous lookup. The `dat_mo` join is now exactly 1:1 (671,520 site-months = 13,990 sites x 48 months).
  2. *All-or-nothing NA rule (secondary).* `collapse_study_period_annual_returns()` in `scripts/R/utils/cross_section_study_period_utils.R:189` marks a Site Group unknown if **any** year in 2021--2024 is `reported_na` or `absent`, and a property with one such site inside the radius gets NA. The old DuckDB builder summed available site-months with `na.rm = TRUE` and dropped a property only when every site-month was missing. Effect at 1km: 36.4% of properties set to NA, p99 1,717 -> 1,241, max 5,004 -> 4,753. P(NA) rises with site density (21% at 1 nearby site, 87% at 11--20, 99.8% above 20), so the densest-exposure properties are removed: max sites among survivors is 22 vs 54 overall.

  Consequence for downstream tables: per-spill slopes roughly doubled (sales: -159/-138/-114 GBP per spill at 250m/500m/1km) and attenuation across radii weakened from ~2.1x to ~1.4x. The 36% NA share is a real sample restriction worth a decision before the remaining reruns.
- [ ] **Public attention over time figure** (`google_trends_article_counts_combined_slides.pdf`) — run `01_descriptive/google_trends_article_counts_combined.R` (axis already ends 2024.5).

## Category 2 — edit script in `09_analysis`, then run

### Small edits (data extends automatically; fix year text / constants)

- [ ] **Main hedonic table + radius robustness** (`hedonic_count_continuous_prior_250m.tex`, `hedonic_count_continuous_prior_radius_robustness.tex`) — `02_hedonic/hedonic_continuous_prior.R`: update "2021--2023" note strings, rerun.
- [ ] **Full-period hedonic bins table** (`hedonic_count_bins_full.tex`) — `02_hedonic/hedonic_bins_full.R`: fix hardcoded `year = (qtr_id - 1) %/% 4 + 2021` literal (L214, use `BASE_YEAR`), update note strings, rerun.
- [ ] **Population exposure table** (`population_exposure.tex`) — `01_descriptive/population_exposure.R`: `TARGET_YEARS <- 2021:2024`, update caption; 2021 population raster stays.
- [ ] **Spill maps** (`{spill,dry_spill}_avg_annual_count_2021_2023_london_inset_slides.pdf`) — `01_descriptive/spill_maps_inset.R`: `TARGET_YEARS <- 2021:2024`. Filenames change to `..._2021_2024_...` → update the two `\includegraphics` in the deck.
- [ ] **Spill persistence figure** (`spill_count_persistence_slides.pdf`) — `01_descriptive/spill_phase_diagrams.R`: years are hardcoded in the transition-pair logic in the body (no named constant); extend to 2024.

### News family (asymmetric window: sales month_id 1–48, rentals stay 1–36)

Shared edit: `05_news/extensive_margin_news_utils.R` L245 `Year <= base_year + 2L` → sales-side horizon to 2024.

- [ ] **Articles DiD (media_attention) + radius robustness** — `05_news/did_articles_prior.R` (`month_id <= 36` at L84). Note: deck's `slides/tables/media_attention.tex` is converted from `output/tables/did_articles_prior_250m.tex` — keep that mapping intact.
- [ ] **Trends DiD + radius robustness** (`did_trends_prior_250m.tex`, `did_trends_prior_radius_robustness.tex`) — `05_news/did_trends_prior.R` (`Year <= 2023` filter L87, peak-month formula).
- [ ] **Extensive-margin articles DiD + robustness** (`did_articles_prior_extensive.tex`, `did_articles_prior_extensive_radius_robustness.tex`) — `05_news/did_articles_prior_extensive.R` (`analysis_end_month_id = 36L`).
- [ ] **Extensive-margin trends DiD + robustness** (`did_trends_prior_extensive.tex`, `did_trends_prior_extensive_radius_robustness.tex`) — `05_news/did_trends_prior_extensive.R` (`analysis_end_month_id = 36L`, `base_year = 2021L`).
- [ ] **Extensive-margin coefficient figure** (`extensive_margin_news_coefficients_lsoa.pdf`) — `05_news/extensive_margin_coefficient_plots.R` (`END_DATE 2023-12-31`, `month_id <= 36` at L223/L281–282/L307–308, `Year <= 2023` L207).

### Structural edit

- [ ] **Repeat-sales table** (`repeat_sales.tex`) — `03_repeat_sales/repeat_sales.R`: migrate the exposure join from per-`site_id` grain to Site Group grain (decision 2026-08-16) so it matches every other table; then rerun (repeat mappings and lookups on disk are current, sales include 2024).

## Category 3 — upstream code not yet fixed

- [ ] **Long-difference table** (`longdiff_unweighted_exposed.tex`) — blocked: `06_analysis_datasets/grid_long_difference_{sales,rentals}.R` hardcode `years = c(2021L, 2022L, 2023L)`. Fix + rerun grids, then set `YEAR_END <- 2024L` in `04_long_difference/longdiff_unweighted_exposed.R` and rerun.
- [ ] **Upstream/downstream appendix tables** (8 tables: `..._nearest_site_distance_{500,1000}m`, `..._one_site_distance_{250,500,1000}m`, `..._direction_binned_{river,euclidean,site_lateral}`) — blocked: signed pair CSVs missing at referenced paths and pre-date the hashed-ID rebuild. Owned by colleague; leave for now.

## In-text hardcoded numbers (update after the reruns above)

- [ ] Sample-period prose: "from 2021--2023" claims, incl. "universe of ≈14,000 storm overflows in England from 2021--2023" (L208).
- [ ] "More than 1.1 Mn spill events totalling ≈8 Mn spill-hours" (L162, L215) — recompute for 2021–2024.
- [ ] "≈3.18 Mn sales, ≈1.4 Mn within 1 km" (L229) — study window now 2021–2024 (~4.15 Mn total sales).
- [ ] "≈1.45 Mn rental listings, ≈0.62 Mn within 1 km" (L237) — verify unchanged (rentals window unchanged, but cleaning fixes may have shifted counts).
- [ ] "5% (16%) of England's population within 250m (500m)" (L371–372) — from new `population_exposure.tex`.
- [ ] "+35% in cumulative articles ... 0.07% larger rental discount" (L736) — from new `did_articles_prior_extensive.tex`.

## No update needed (static graphics)

Headline collages, SAS alerts screenshot, OS rivers map, combined-sewer diagram, upstream/downstream schematic.

## Deck hygiene (optional, while editing)

- [ ] Six appendix Back-buttons point at the commented-out `ud-results` frame (dead `\hyperlink` target).
