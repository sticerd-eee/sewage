---
title: "feat: Report spill exposure effects per one additional weekly unit"
type: feat
date: 2026-06-24
branch: jo/rescale-spill-effect
status: planned
depth: standard
---

# feat: Report spill exposure effects per one additional weekly unit

## Summary

Today the project reports the effect of sewage spills using daily-average
regressors: `spill_count_daily_avg` (average spill events **per day**) and
`spill_hrs_daily_avg` (average spill hours **per day**) at the overflows near a
property. Every continuous count or hours coefficient is therefore per one
additional daily-average unit, which is awkward and often large relative to the
support of the data.

This plan reports those coefficients in weekly units: **one additional spill per
week** for counts and **one additional spill hour per week** for hours. The
weekly regressors `spill_count_weekly_avg` and `spill_hrs_weekly_avg` are
constructed once, as `daily_avg * 7`, at the points where the daily averages are
first computed, and stored alongside the daily columns. The reporting models then
regress on the weekly columns, so `fixest`/`modelsummary` produce the scaled
coefficient, standard error, confidence interval, stars, N, and R² directly. The
daily columns stay in every dataset and remain the canonical exposure for
`book/scaled_effects.qmd` and any other use.

Scope: Data Construction (add the weekly columns), Hedonic Analysis, News /
Public Attention, Upstream / Downstream, Overleaf-facing table artifacts under
`docs/overleaf/slides/tables/`, and a second-pass text update. `book/scaled_effects.qmd`,
testing scripts (`scripts/R/testing/`), and archived scripts (`_archive/`,
`Archive/`, `_old/`) keep their current behaviour.

---

## Problem Frame

`spill_count_daily_avg` and `spill_hrs_daily_avg` are constructed as
`spill_count / n_days_in_window` and `spill_hrs / n_days_in_window` in the
analysis-dataset builders, giving average daily spill-count and spill-hours rates
over the exposure window. In log-price hedonic specifications the fitted
coefficient `β` is the semi-elasticity of price with respect to a **+1
daily-average unit** change — one spill/day for counts or one spill hour/day for
hours — which is often far outside the support of the data, so the printed
numbers are hard to read.

The preferred table unit is **+1 spill/week**, which equals a change of
`Δ = 1/7 ≈ 0.142857` in `spill_count_daily_avg`. The user has verified that on
the preferred 250 m hedonic samples one spill/week corresponds to roughly
**1.14 SD** of the exposure in sales and **1.23 SD** in rentals — i.e. a
one-spill/week move is a large but on-support change, which is exactly why it is
the right table unit. Spill hours are rescaled the same way: **+1 spill hour/week**
equals `1/7` in `spill_hrs_daily_avg`, with its own SD-equivalent computed from
the relevant estimation sample when prose needs to describe the size of the
contrast.

The work is mechanically uniform across ~3 analysis areas plus the generated
table-copy path, with the spill term appearing as a level term, in several
interactions, and in derived upstream/downstream aggregates. Constructing the
weekly column once at the source and pointing every reporting model at it keeps
all areas consistent.

---

## Requirements

- **R1.** Regression tables that report continuous spill-count or spill-hours
  coefficients use weekly units: **one additional spill/week** for counts and
  **one additional spill hour/week** for hours.
- **R2.** Tables present the weekly coefficients (level and interactions) with
  coefficient labels and table notes in per-week wording. Coefficient cells show
  weekly-unit labels and scaled coefficients; SD-equivalence lives in prose.
- **R3.** The construction scripts compute `spill_count_weekly_avg` and
  `spill_hrs_weekly_avg` as `daily_avg * 7` immediately after the daily averages,
  and store both. The daily columns stay as the canonical exposure in every
  dataset.
- **R4.** Reporting scripts regress on the stored weekly columns. Downstream edits
  are variable swaps in the model formulas plus matching coefficient-map keys,
  labels, and notes.
- **R5.** A temporary validation script confirms the scaled outputs equal the
  originals transformed by the known factor (β and SE scale by `1/7`; t-stats,
  p-values, stars, N, and R² invariant; signs preserved). Once the checks pass it
  is removed from the tree.
- **R6.** The spill-**hours** measure is rescaled the same way as spill count.
  Categorical bin specifications keep their daily exposure because per-week scaling
  applies to continuous terms.
- **R7.** Overleaf-facing table copies are regenerated after `output/tables/` is
  refreshed, so the slides/manuscript-facing LaTeX shows the same weekly units and
  scaled coefficients as the analysis outputs.
- **R8.** `book/scaled_effects.qmd` keeps reporting effects for a 1-SD change in
  the independent variable, unchanged.
- **R9.** Work is split into two chunks: Chunk 1 (Phase 1) edits construction +
  reporting scripts, regenerates datasets and `output/tables/`, and validates
  numeric invariants; Chunk 2 (Phase 2) refreshes the generated Overleaf table
  copies and updates the prose that describes the results.

---

## Key Technical Decisions

### KTD1. Construct the weekly regressor at the source, regress on it

**Decision:** Add `spill_count_weekly_avg = spill_count_daily_avg * 7` and
`spill_hrs_weekly_avg = spill_hrs_daily_avg * 7` wherever the daily averages are
first computed, store both, and use the weekly columns in the reporting model
formulas.

**Why:** If `w = 7·d`, then `y = β_d·d = (β_d/7)·w`, so the coefficient on `w` is
exactly `β_d/7` — the per-spill/week effect — and `fixest`/`modelsummary` compute
the scaled **standard error, confidence interval, stars, p-values, N, and R²
automatically**. This propagates through **interactions** (`w:post`,
`w:log_cumulative_articles`, `w:direction`) and through derived count or hours
aggregates, because the underlying column is what carries the scale.

### KTD2. Store both daily and weekly columns

**Decision:** Each dataset carries the daily column (canonical, unchanged) and the
weekly column (derived `×7`). The daily construction lines
(`spill_count / n_days_in_window`, `spill_hrs / n_days_in_window`,
`spill_count / N_DAYS_FULL_PERIOD`) keep their current form.

**Why:** The daily column stays available for `book/scaled_effects.qmd` SD/quantile
summaries and any non-reporting use, while the weekly column is ready for every
reporting model with a single source of truth.

### KTD3. Compute percent effects from the weekly coefficient

**Decision:** In log-price models the percent effect of +1 weekly unit is
`100·(exp(β_w) − 1)`, where `β_w` is the weekly coefficient read straight from the
model. This applies to both spill counts and spill hours.

**Why:** Because the reporting models are fit on the weekly regressor, `fixest`
returns `β_w = β_d/7` directly, so the percent effect is one exponentiation of the
reported coefficient with no further rescaling. (The `β_d/7` form only appears in
the U5 validation, where comparing `β_w` against the legacy daily fit is the point.)

---

## High-Level Technical Design

### Where the weekly column is built and how it flows

```
construction (×7 added here)                         consumed by
─────────────────────────────────────────────────   ───────────────────────────
cross_section_prior_to_sale.R   → cross_section/sales/prior_to_sale          ┐
cross_section_prior_to_rental.R → cross_section/rentals/prior_to_rental       ├─ Hedonic (prior, qtr_fe), News
                                                                              ┘
house_spill_prior_to_sale.R     → cross_section/sales/prior_to_sale_house_site   ┐
rental_spill_prior_to_rental.R  → cross_section/rentals/prior_to_rental_rental_site ├─ Upstream/Downstream
                                  (per-site daily/weekly → summed into aggregates) ┘

hedonic_continuous_full.R       → builds daily/weekly in-script (N_DAYS_FULL_PERIOD) ─ Hedonic (full)
```

Once the four builders are re-run, the house/rental-level and house-site-level
datasets carry `spill_count_weekly_avg` and `spill_hrs_weekly_avg`. Reporting
scripts read those columns directly; `hedonic_continuous_full.R` adds them in its
own data-prep.

### Scaling math (reference for the implementer)

For a log-price model `y = α + β_d·d + …`, where `d` is a daily-average count or
hours measure and the weekly-unit contrast is `Δ = 1/7`:

| Quantity | Daily units | Per-week units | Relationship |
|---|---|---|---|
| Coefficient | `β_d` | `β_w = β_d / 7` | exact linear |
| Std. error | `SE_d` | `SE_w = SE_d / 7` | exact linear |
| 95% CI bound | `[lo_d, hi_d]` | `[lo_d/7, hi_d/7]` | exact linear |
| t-statistic | `β_d/SE_d` | `β_w/SE_w` | **invariant** |
| p-value / stars | — | — | **invariant** |
| N, R², adj. R² | — | — | **invariant** |
| % price effect | `100·(exp(β_d)−1)` | `100·(exp(β_w)−1)` | exp of the reported weekly coef |
| SD-equivalent of the change | — | `(1/7) / sd(d)` | per count and hours |

Interaction terms (`d:post`, `d:log_cumulative_articles`, `d:direction`) scale by
the same `1/7` for their per-week marginal interpretation; regressing on the weekly
column carries them. For the preferred 250 m hedonic count samples, the
SD-equivalent is ≈1.14 in sales and ≈1.23 in rentals; the hours equivalents are
computed the same way from each retained estimation sample.

### Implementation phases

**Chunk 1 (Phase 1): construction, reporting, validation.** Add the weekly columns
at the five construction sites, re-run the four builders, edit the active reporting
scripts in Hedonic, News / Public Attention, and Upstream / Downstream to use the
weekly columns, run the edited scripts to regenerate `output/tables/`, run the
temporary validation, and remove the validation script once it passes. This chunk
produces the validated weekly-unit analysis outputs (U1–U5).

**Chunk 2 (Phase 2): Overleaf sync and prose.** After Chunk 1's numbers validate,
refresh the repo-local Overleaf table copies from the regenerated `output/tables/`
(U6), then update prose that describes daily spill effects to the weekly-unit
interpretation, using SD-equivalence where it helps the writing (U7).
`book/scaled_effects.qmd` keeps the existing 1-SD interpretation.

---

## Implementation Units

### U1. Construct weekly exposure columns at the source

**Goal:** Emit `spill_count_weekly_avg` and `spill_hrs_weekly_avg` (`= daily * 7`)
in every dataset that carries the daily averages.

**Requirements:** R1, R3, R6.

**Dependencies:** none.

**Files:**
- `scripts/R/06_analysis_datasets/cross_section_prior_to_sale.R` (after `:385`–`:386`)
- `scripts/R/06_analysis_datasets/cross_section_prior_to_rental.R` (after `:385`–`:386`)
- `scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R` (after `:374`–`:375`)
- `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R` (after `:373`–`:374`)
- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_full.R` (after `:148`–`:149` and `:228`–`:229`)

**Approach:** In each `mutate()` that creates the daily averages, add two lines
directly below them:

```r
spill_count_daily_avg  = spill_count / n_days_in_window,
spill_hrs_daily_avg    = spill_hrs   / n_days_in_window,
spill_count_weekly_avg = spill_count_daily_avg * 7,
spill_hrs_weekly_avg   = spill_hrs_daily_avg   * 7,
```

For `hedonic_continuous_full.R` the daily lines divide by `N_DAYS_FULL_PERIOD`; add
the two `* 7` lines below each of the two construction blocks. Re-run the four
`06_analysis_datasets` builders so the stored parquet datasets carry the weekly
columns (`hedonic_continuous_full.R` builds in-script, so it needs no separate
build).

**Test scenarios:**
- After re-running the builders, `cross_section/sales/prior_to_sale` and
  `cross_section/sales/prior_to_sale_house_site` contain `spill_count_weekly_avg`
  and `spill_hrs_weekly_avg`, each equal to its daily column `× 7` row-by-row.

**Verification:** open each regenerated dataset and confirm the weekly columns are
present and equal `daily × 7`.

---

### U2. Hedonic analysis — per-week reporting

**Goal:** Report the hedonic count and hours coefficients in per-week units, and
update the count cross-radius robustness summaries to match.

**Requirements:** R2, R4, R6.

**Dependencies:** U1.

**Files:**
- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R`
- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior_qtr_fe.R`
- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_full.R` (weekly columns added in U1)
- Confirm-only: `hedonic_bins_prior.R`, `hedonic_bins_full.R` (bins keep their
  daily categorical exposure; update note wording only where it states a daily unit)

**Approach:** In the count model formulas replace `spill_count_daily_avg` with
`spill_count_weekly_avg`, and in the hours model formulas replace
`spill_hrs_daily_avg` with `spill_hrs_weekly_avg`, using whole-word matching
(`\bspill_count_daily_avg\b`) so the categorical `spill_count_daily_avg_bin` and
`spill_hrs_daily_avg_bin` columns keep their daily values. Set the
`coef_labels_count` / `coef_labels_hrs` **keys** to the weekly term names and map
them to `"Spills per week (avg.)"` and `"Spill hours per week (avg.)"`. Update the
count and hours `custom_notes` exposure sentences and the `coef_map` passed to
`write_radius_robustness_table` (e.g. `hedonic_continuous_prior.R:581`) to the
weekly key and label. Keep the existing count radius-robustness output family as-is.

**Patterns to follow:** existing `coef_labels_count` / `custom_notes` /
`write_radius_robustness_table` blocks in `hedonic_continuous_prior.R`.

**Test scenarios:**
- `hedonic_count_continuous_prior_250m.tex` and
  `hedonic_hrs_continuous_prior_250m.tex` show spill coefficients equal to the
  prior values `/7` (and SEs `/7`); stars and Observations rows match the
  pre-change tables.
- `hedonic_count_continuous_prior_radius_robustness.tex` shows spill cells equal
  to prior `/7`.

**Verification:** scripts run end-to-end with `rv`; count and hours `.tex` outputs
carry weekly labels and `/7` magnitudes.

---

### U3. News / Public Attention — per-week reporting

**Goal:** Report the spill-count level and interaction coefficients in the
public-attention DiD / event-study tables in per-spill/week units.

**Requirements:** R2, R4.

**Dependencies:** U1.

**Files (edit — reporting):**
- `scripts/R/09_analysis/05_news/did_articles_prior.R`
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior.R`
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior_extensive.R`
- `scripts/R/09_analysis/05_news/did_articles_windowed_prior.R`
- `scripts/R/09_analysis/05_news/windowed_article_effect_size_utils.R`
- `scripts/R/09_analysis/05_news/did_trends_prior.R`
- `scripts/R/09_analysis/05_news/did_trends_full.R`
- `scripts/R/09_analysis/05_news/did_trends_full_extensive.R`
- `scripts/R/09_analysis/05_news/es_trends_prior.R`
- `scripts/R/09_analysis/05_news/es_trends_prior_extensive.R`
- `scripts/R/09_analysis/05_news/extensive_margin_coefficient_plots.R` (relabel any
  axis/label that uses the spill-count unit)

**Approach:** Replace `spill_count_daily_avg` with `spill_count_weekly_avg` in the
formulas, which moves the level term and every interaction
(`spill_count_weekly_avg:post`, `spill_count_weekly_avg:log_cumulative_articles`)
to weekly units. Set the `coef_labels` / `coef_map` **keys** to the weekly level
and interaction term names and update their labels (e.g.
`"spill_count_weekly_avg:log_cumulative_articles" = "{Spills per week \\\\ $\\times$ $\\log(\\text{Articles})$}"`).
Keep `log_cumulative_articles`, `post`, and trends terms as they are. Update the
per-radius and robustness-summary `custom_notes` exposure sentence.

In `windowed_article_effect_size_utils.R`, build the interaction term from the
weekly spill column so windowed callers use `spill_count_weekly_avg` and its weekly
interaction term. Keep the effect-size CSV scale explicit: coefficient-level
effects are per spill/week, and any SD-scaled column uses
`sd(spill_count_weekly_avg)` and is labelled as an SD effect.

**Patterns to follow:** `coef_labels` / `custom_notes` / robustness blocks in
`did_articles_prior.R` and `did_trends_prior.R`.

**Test scenarios:**
- `did_trends_prior_250m.tex`: level coef and `:post` interaction coef each equal
  prior `/7`; SEs `/7`; stars and N unchanged.
- `did_articles_prior_250m.tex`: the `:log_cumulative_articles` interaction coef
  equals prior `/7`; the standalone `log(Articles)` coefficient is unchanged.
- Robustness summaries report `/7` interaction cells.
- Windowed effect-size CSVs run after the term rename, with SD-scaled columns
  labelled by the scale they use.

**Verification:** all listed scripts run under `rv`; the spill-bearing coefficients
and their labels/notes are in weekly units; non-spill terms are unchanged.

---

### U4. Upstream / Downstream — per-week reporting

**Goal:** Report the directional and nearest-site spill-count and spill-hours
coefficients in per-week units, including the derived upstream/downstream aggregates.

**Requirements:** R2, R4.

**Dependencies:** U1.

**Files (edit — reporting):**
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_prior.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_prior_full.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_prior_nearest_site.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_prior_only_site.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_full_all_radii.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_nearest_all_radii.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_only_site_all_radii.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_decay_binary_did.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_decay_ring_triple.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_nearest_by_bin.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_nearest_vary_lateral.R`
- `scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_nearest_vary_river.R`

**Approach:** Two forms appear.

1. **Nearest-site interaction form**
   (`feols(log_price ~ spill_count_daily_avg*direction …)` and the hours analogue):
   replace `spill_count_daily_avg` with `spill_count_weekly_avg` and
   `spill_hrs_daily_avg` with `spill_hrs_weekly_avg` in those formulas. The level,
   `direction`, and `spill:direction` interaction move to weekly units together.
   Set the `coef_labels_count` / `coef_labels_hrs` **keys** to the weekly terms and
   keep the `direction` label.

2. **Derived-aggregate form** (`upstream_count`, `downstream_count`, `upstream_hrs`,
   `downstream_hrs`, and the `*_exposure` variants, e.g.
   `upstream_downstream_prior.R:175`–`182`): build each aggregate by summing the
   weekly per-site column, e.g.
   `upstream_count = sum(spill_count_weekly_avg * (direction == 1), na.rm = TRUE)`
   and `upstream_hrs = sum(spill_hrs_weekly_avg * (direction == 1), na.rm = TRUE)`,
   with the `_exposure` versions multiplying by `inv_dist_weight_raw` as now. The
   regressor names (`upstream_count`, …) stay the same, so the `coef_map` keys are
   unchanged; update their displayed labels to weekly wording.

Update each script's `custom_notes` exposure sentence and any `add_rows` header to
weekly wording. A per-file grep for `spill_count_daily_avg`, `spill_hrs_daily_avg`,
and the aggregate names (see Commands) enumerates the exact sites.

**Patterns to follow:** `coef_labels_count`, the aggregate `sum(...)` block, and the
`add_rows`/`custom_notes` blocks in `upstream_downstream_prior.R`.

**Test scenarios:**
- `upstream_downstream_prior` nearest-site count and hours tables:
  `spill:direction` coefs equal prior `/7`; SEs `/7`; stars/N unchanged.
- Derived-aggregate models (directional `upstream_count`/`downstream_count` and
  hours counterparts): each spill coefficient equals prior `/7`.
- `..._all_radii` robustness outputs report `/7` cells across 250/500/1000 m.

**Verification:** all listed scripts run under `rv`; count and hours spill-bearing
terms and labels are in weekly units; non-spill controls (`dist_river_m`, FE) are
unchanged.

---

### U5. Temporary validation of the rescaling

**Goal:** Confirm the scaled outputs equal the originals transformed by the known
factor, then remove the check.

**Requirements:** R5.

**Dependencies:** U1–U4 (can be drafted alongside U1).

**Files:**
- Create `scripts/R/09_analysis/validate_spill_rescaling.R` (temporary; removed once
  it passes)

**Approach:** A standalone script that, for representative models in each area
(hedonic count and hours 250 m preferred specs; news count `:post` and
`:log_cumulative_articles`; upstream count and hours `:direction`), refits **both**
the daily-units model and the weekly-units model on the same prepared sample and
asserts:
- `coef_weekly / coef_daily == 1/7` (rel. tol `1e-6`).
- `se_weekly / se_daily == 1/7`.
- `t`, `p`, `nobs`, `r2`/`adj.r2` identical.
- sign preserved.
- the weekly percent effect `100*(exp(beta_weekly)-1)` equals the daily-model
  cross-check `100*(exp(beta_daily/7)-1)`.
- Count SD-equivalents at 250 m ≈ 1.14 (sales) / 1.23 (rentals), computed as
  `(1/7) / sd(spill_count_daily_avg)` on the retained samples; hours SD-equivalents
  computed the same way for any prose that quotes them.

The script prints a pass/fail summary and `stop()`s on any failure. After it prints
all-pass on the post-implementation tree, remove the script before the final commit.

**Test scenarios:** running the script on the post-implementation tree prints all
checks `pass`.

**Verification:** `rv run Rscript scripts/R/09_analysis/validate_spill_rescaling.R`
exits 0 with an all-pass summary; the script is then deleted.

---

### U6. Overleaf-facing table sync

**Goal:** Make the Overleaf-facing LaTeX table copies reflect the regenerated
weekly-unit analysis outputs.

**Requirements:** R7.

**Dependencies:** U2, U3, U4, U5.

**Files:**
- `scripts/python/convert_paper_tables_to_beamer.py` (run)
- `docs/overleaf/slides/tables/*.tex` (regenerated)

**Approach:** After regenerating `output/tables/*.tex` and passing the validation
check, run the Beamer-table converter with `--from-target-manifest` so the curated
repo-local slide tables inherit the weekly labels and scaled coefficients. The
converter records a `% Source: output/tables/<name>.tex` marker; keep that
generated-file convention.

If the external Dropbox/Overleaf manuscript quotes coefficient values in prose
(rather than via `\input{...}` table files), list those locations for the Phase 2
text pass.

**Test scenarios:**
- `docs/overleaf/slides/tables/hedonic_count_continuous_prior_250m.tex` and
  `docs/overleaf/slides/tables/hedonic_count_continuous_prior_radius_robustness.tex`
  reflect the same `/7` scaling and weekly labels as their `output/tables/` sources.
- `docs/overleaf/slides/tables/media_attention.tex` and the DiD robustness tables
  reflect the updated count interaction labels and `/7` coefficients.
- Generated markers still point to the correct `output/tables/*.tex` source.

**Verification:** run the converter, inspect `git diff docs/overleaf/slides/tables`,
and compare each changed Overleaf table against its source for matching labels,
magnitudes, and source markers.

---

### U7. Text and prose update pass

**Goal:** Update narrative text after the validated weekly-unit outputs exist.

**Requirements:** R1, R2, R8, R9.

**Dependencies:** U5, U6.

**Files:**
- Repo prose under `README.md`, `docs/**/*.md`, and `book/**/*.qmd`, keeping
  `book/scaled_effects.qmd`, `docs/plans/**`, generated table files, and archived
  paths as they are.
- External Dropbox/Overleaf manuscript prose with hard-coded daily-unit
  interpretations or old coefficient values, updated in this chunk with explicit
  filesystem scope/approval.

**Approach:** Search prose for phrases such as "daily average spill", "spill per
day", "one additional spill", "spill hour", "per day", and quoted
coefficient/percent-effect values tied to the old daily unit. Rewrite those
passages so regression-table effects are described as one additional spill/week or
one additional spill hour/week. Where prose mentions the size of the contrast, use
the SD-equivalent values from U5. `book/scaled_effects.qmd` keeps the existing 1-SD
interpretation.

**Test scenarios:**
- Prose describing a regression-table coefficient uses the +1 spill/week or +1
  spill hour/week unit.
- Any quoted percent effect for a weekly-unit log-price coefficient is computed as
  `100*(exp(beta_weekly)-1)` from the weekly model coefficient.
- `book/scaled_effects.qmd` matches its branch baseline.

**Verification:** `rg` over the prose search terms returns only intentional
daily-data-construction language or text in kept/generated paths; the final diff
separates Phase 1 script/output changes from Phase 2 prose-only edits.

---

## Files: in scope vs out of scope

**Construction — EDIT (add weekly alongside daily, R3):**
- `scripts/R/06_analysis_datasets/cross_section_prior_to_sale.R` (`:385`–`:386`)
- `scripts/R/06_analysis_datasets/cross_section_prior_to_rental.R` (`:385`–`:386`)
- `scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R` (`:374`–`:375`)
- `scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R` (`:373`–`:374`)
- `scripts/R/09_analysis/02_hedonic/hedonic_continuous_full.R` in-script construction
  (`:148`–`:149`, `:228`–`:229`)

**Reporting scripts — EDIT (use weekly columns + relabel):** the active reporting
scripts in U2–U4.

**Validation — TEMPORARY:** the U5 script, removed once it passes.

**Generated Overleaf-facing tables — UPDATE VIA CONVERTER:** affected files under
`docs/overleaf/slides/tables/`, regenerated from `output/tables/*.tex` by
`scripts/python/convert_paper_tables_to_beamer.py --from-target-manifest`.

**Phase 2 prose — EDIT AFTER VALIDATION:** prose files found by U7's search,
keeping generated outputs and `book/scaled_effects.qmd` as they are.

**Out of scope — keep as-is:**
- `hedonic_bins_*` continuous-bin specs keep daily categorical exposure (continuous
  rescaling applies to the continuous terms). Bin columns
  (`spill_count_daily_avg_bin`, `spill_hrs_daily_avg_bin`) stay in daily units.
- `scripts/R/testing/**` (e.g. `spill_count_variation_share_prior.qmd`,
  `test_lsoa_variation_updown_prior.R`, `explore_extensive_margin_news.qmd`).
- any `_archive/`, `Archive/`, `_old/` path.
- `book/scaled_effects.qmd` (keeps the existing 1-SD interpretation).
- `docs/plans/2026-06-22-001-feat-windowed-article-salience-measures-plan.md`.

---

## Validation & Expected Invariants

Run after each area and once at the end (U5 automates this; remove U5 once it passes):

| Invariant | Expectation |
|---|---|
| `β_week / β_daily` | `1/7` (exact, ≤1e-6 rel. error) |
| `SE_week / SE_daily` | `1/7` |
| 95% CI bounds | both `×1/7` |
| t-stat, p-value, significance stars | unchanged |
| Observations (N) | unchanged |
| R² / adj. R² | unchanged |
| Coefficient sign | unchanged |
| % effect | `100·(exp(β_w)−1)`, exp of the reported weekly coef |
| Count SD-equivalent @250 m | ≈1.14 SD (sales), ≈1.23 SD (rentals) |
| Hours SD-equivalent | `(1/7) / sd(spill_hrs_daily_avg)` in the retained sample |
| Spill-**hours** coefficients | `β_week / β_daily == 1/7`; SEs `/7`; t/p/stars/N/R² unchanged |
| Non-spill coefficients | unchanged (only count and hours spill factors move) |
| Stored weekly columns | equal `daily × 7` row-by-row |
| Overleaf table copies | regenerated from updated `output/tables/*.tex`; labels and `/7` magnitudes match sources |

---

## Commands to Run After Implementation

Run with the project's `rv` environment (R 4.6, paths via `here::here`). From repo
root:

```bash
# Chunk 1 (Phase 1): construction, reporting, validation

# 0. Enumerate the continuous spill-term sites per area (whole-word, before/after)
rg -nw 'spill_count_daily_avg|spill_hrs_daily_avg' scripts/R/09_analysis/02_hedonic \
   scripts/R/09_analysis/05_news scripts/R/09_analysis/06_upstream_downstream \
   --glob '!**/_archive/**' --glob '!**/Archive/**' --glob '!**/_old/**'

# 0a. Locate script-generated table text to move to weekly wording
rg -n 'Daily spill|daily spill|per day|spills/day|spill hours/day' \
   scripts/R/09_analysis/02_hedonic scripts/R/09_analysis/05_news \
   scripts/R/09_analysis/06_upstream_downstream \
   --glob '!**/_archive/**' --glob '!**/Archive/**' --glob '!**/_old/**'

# 1. Re-run the four builders so datasets carry the weekly columns
rv run Rscript scripts/R/06_analysis_datasets/cross_section_prior_to_sale.R
rv run Rscript scripts/R/06_analysis_datasets/cross_section_prior_to_rental.R
rv run Rscript scripts/R/06_analysis_datasets/house_spill_prior_to_sale.R
rv run Rscript scripts/R/06_analysis_datasets/rental_spill_prior_to_rental.R

# 2. Re-run the edited analysis scripts (examples; run each edited file)
rv run Rscript scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R
rv run Rscript scripts/R/09_analysis/05_news/did_trends_prior.R
rv run Rscript scripts/R/09_analysis/05_news/did_articles_prior.R
rv run Rscript scripts/R/09_analysis/06_upstream_downstream/upstream_downstream_prior.R
# … plus the remaining files listed in U2–U4

# 3. Run the temporary validation suite (must exit 0, all-pass), then remove it
rv run Rscript scripts/R/09_analysis/validate_spill_rescaling.R
rm scripts/R/09_analysis/validate_spill_rescaling.R

# 4. Confirm the intended cells moved: diff regenerated tables
git diff --stat output/tables/
git diff output/tables/hedonic_count_continuous_prior_250m.tex   # coef & SE /7, stars/N same
git diff output/tables/hedonic_hrs_continuous_prior_250m.tex     # coef & SE /7, stars/N same

# Chunk 2 (Phase 2): Overleaf sync and prose, after Chunk 1 validates

# 5. Refresh only manifest-listed repo-local Overleaf/Beamer table copies
python3 scripts/python/convert_paper_tables_to_beamer.py --from-target-manifest
git diff --stat docs/overleaf/slides/tables/

# 6. Prose pass
rg -n 'daily average spill|spill per day|spills per day|spill hour|spill hours|per day|one additional spill|daily-unit|daily units' \
   README.md docs book \
   --glob '!docs/plans/**' \
   --glob '!book/scaled_effects.qmd' \
   --glob '!**/_archive/**' --glob '!**/Archive/**' --glob '!**/_old/**'
```

(If `rv run` is not the exact wrapper, substitute the project's documented `rv`
invocation.)

---

## Confirmed Decisions

- **Weekly built at the source.** `spill_count_weekly_avg` / `spill_hrs_weekly_avg`
  are constructed as `daily_avg * 7` at the five construction sites and stored
  alongside the daily columns; reporting models read the weekly columns.
- **Both columns kept.** Daily stays canonical for `scaled_effects.qmd` and
  non-reporting use; weekly is the reporting column.
- **Tables show weekly units only.** Regression tables, robustness tables, and
  repo-local Overleaf copies show weekly-unit labels and scaled coefficients;
  SD-equivalence appears in prose, using the U5 values.
- **Validation is temporary.** The U5 script proves the invariants, then is removed.
- **`scaled_effects.qmd` unchanged.** It keeps the 1-SD independent-variable
  interpretation.
- **Two chunks.** Chunk 1 (Phase 1) covers construction, reporting, and validation
  to produce the regenerated `output/tables/` (U1–U5); Chunk 2 (Phase 2) refreshes
  the Overleaf table copies and updates the prose after the numbers validate
  (U6–U7).
- **Bins.** `hedonic_bins_*` keep categorical daily exposure; update bin notes only
  where they state a daily unit.
- **Non-daily strategies.** Long-difference (`annual_delta`) and repeat-sales
  (`four_quarter_delta`) rows keep their existing scale.
- **Windowed effect-size CSVs.** Weekly coefficient effects are per spill/week; any
  SD-scaled column uses `sd(spill_count_weekly_avg)` and is labelled as SD-scaled.

---

## Verification Checkpoints

- **Coefficient-map keys.** Each edited `coef_map` / `coef_labels` uses the weekly
  term name as its key (for the nearest-site/level/interaction forms), so the spill
  rows render. Aggregate models keep their existing regressor names and update
  labels only.
- **Whole-word substitution.** Continuous spill terms are edited with `\b…\b`
  matching, so `spill_count_daily_avg_bin` and `spill_hrs_daily_avg_bin` keep their
  daily values.
- **Display strings.** Coefficient labels, `add_rows` headers, and `custom_notes`
  carry weekly wording (Commands step 0a locates them).
- **Aggregates and full period.** Upstream/downstream aggregates sum the weekly
  per-site column; `hedonic_continuous_full.R` builds the weekly column in-script.
- **Cross-area ratios.** U5 confirms `β_week/β_daily == 1/7` and the invariants
  across hedonic, news, and upstream/downstream before the validation script is
  removed.
- **Phase ordering.** Chunk 1 regenerates datasets and `output/tables/` and
  validates; Chunk 2 refreshes the Overleaf copies and updates prose against the
  validated weekly outputs.
