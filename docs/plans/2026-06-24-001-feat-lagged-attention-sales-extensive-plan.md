# feat: Lagged public-attention sales re-estimation — extensive + intensive margins (Issue #21)

Created: 2026-06-24
Revised: 2026-06-25 — folded in the **intensive margin** (continuous spill-intensity treatment) per directive; intensive runs a **lag × radius grid**. Extensive scope unchanged.
Type: feat
Depth: Standard
Origin: GitHub issue #21 — "Re-estimate extensive-margin sales regressions with lagged public-attention measures" (parent: sticerd-eee/sewage#2). The intensive-margin extension was added by directive on 2026-06-25 (issue #21 was extensive-only; this plan now covers both margins).

---

## Summary

The public-attention interaction is null for **house sales** but negative and significant (≈0.8 pp on the extensive margin) for **rentals**. The leading explanation is *mistimed attention*: rental listing prices are set near the listing date, but sale prices reflect a transaction price agreed weeks-to-months before the recorded completion/registration date. Because the attention measure is currently aligned to the recorded transaction month, for sales it is dated *after* the price was actually agreed.

This plan adds a **systematic lag sweep** for sales over the two public-attention measures, across **both margins**, lagging the attention measure relative to the recorded transaction date to approximate the agreement-to-completion gap. Lags of **3, 6, and 12 months** are estimated against the **contemporaneous** (lag 0) baseline, for both attention measures, with rentals carried only as a contextual benchmark.

Two margins, distinguished by the **treatment variable** interacted with the attention measure:
- **Extensive margin** — binary `near_bin` (near band **0–500m** vs far band **1000–2000m**). Run at the single headline band comparison; radius (band) sweep deferred.
- **Intensive margin** — continuous `spill_count_daily_avg` (daily-average spill count within radius `RAD`, among properties with `n_spill_sites > 0`). Run as a **lag × radius grid** over `RADII = c(250, 500, 1000)`.

Scope is **plan + new analysis scripts only**: no changes to the published contemporaneous scripts, no data-pipeline changes, no new *band*-radius sweep for the extensive margin.

---

## Problem Frame

Two **attention measures**, each interacted with the margin's treatment variable:

- **Measure 1 — Post-peak indicator**: `post = 1[month_id >= peak_month_id]`, where the Google Trends peak is August 2022. Coefficient of interest is the `treatment:post` interaction.
- **Measure 2 — Log cumulative articles**: `log_cumulative_articles` = log of cumulative LexisNexis UK sewage coverage from Jan 2021 to the transaction month. Coefficient of interest is the `treatment:log_cumulative_articles` interaction.

Crossed with the two margins, the four coefficients of interest are:

| | Measure 1 (post) | Measure 2 (articles) |
|---|---|---|
| **Extensive** (`near_bin`) | `near_bin:post` | `near_bin:log_cumulative_articles` |
| **Intensive** (`spill_count_daily_avg`) | `spill_count_daily_avg:post` | `spill_count_daily_avg:log_cumulative_articles` |

**Existing lag precedents (single fixed 4-month lag, Measure 2 only, to be generalized):**
- Extensive: `did_articles_lag4_prior_extensive.R` — `lagged_month_id = month_id - lag` join, reduced spec set, no MSOA FE, no robustness.
- Intensive: `did_articles_lag4_prior.R` — same lag join at fixed `RAD = 250`, `LAG_MONTHS = 4`. **Note**: this intensive precedent uses `lsoa + qtr_id` FE; this plan deliberately uses **month FE** for the intensive preferred spec (KTD3) to match the non-lag intensive baseline (`model_sale_5`) and the extensive plan.

Neither precedent is systematic (one hard-coded lag), applied to Measure 1, or run as a grid. This plan generalizes both into a configurable lag sweep across both measures and both margins.

**Calendar reference** (base_year = 2021, so `month_id = (year - 2021)*12 + month`):
- `month_id = 1` → Jan 2021; `month_id = 36` → Dec 2023 (analysis window is fixed at 1–36).
- Google Trends peak Aug 2022 → `peak_month_id = 20`.

---

## Requirements (traceability to Issue #21 + intensive directive)

- **R1** — Re-estimate **sales** regressions with public-attention measures lagged relative to the recorded transaction date.
- **R2** — Sweep a range of lags. Issue text suggests "1, 3, 6"; the implementation directive fixes the initial set at **3, 6, 12 months** vs contemporaneous (lag 0). `CONFIG$lags` must be a configurable vector so 1-month or other lags can be added without code edits.
- **R3** — Apply to **both** attention measures: Measure 1 (shift the post threshold by the lag) and Measure 2 (cumulative count as of the lagged month).
- **R4** — Compare lagged sales against the **contemporaneous** sales spec and against the **rentals** result, and summarise whether mistiming explains the null.
- **R5** — Outputs are comparison tables and/or summary artifacts, mirroring existing `05_news` table/CSV conventions.
- **R6 (intensive directive)** — Cover **both margins**: extensive (`near_bin`, headline band only) and intensive (`spill_count_daily_avg`). The intensive margin is estimated as a **lag × radius grid** over `RADII = c(250, 500, 1000)`; the extensive margin stays at the single headline band comparison (KTD5).

---

## Key Technical Decisions

### KTD1 — Lag is implemented per measure, but the two framings are algebraically identical (both margins)

For both measures, "lag the attention by `L` months" means: *use the attention value that prevailed `L` months before the recorded transaction month.* This is margin-agnostic — only the treatment variable interacted with the (now lagged) attention measure differs.

- **Measure 2 (cumulative articles)** — shift the **measure backward**: join `log_cumulative_articles` keyed on `month_id - L` instead of `month_id`. This is exactly what the lag4 precedents do (`lagged_month_id = month_id - lag`, then `inner_join(articles, by = c("lagged_month_id" = "month_id"))`).
- **Measure 1 (post indicator)** — shift the **threshold forward**: `post_L = 1[month_id >= peak_month_id + L]`. This is identical to `1[month_id - L >= peak_month_id]` (shifting the measure backward), but the forward-threshold form is preferred because **it drops no observations** (every transaction retains a defined `post` status).

**Why the distinction matters (a real asymmetry, surfaced in table notes):**
- The Measure 2 join *mechanically drops* the first `L` transaction months, because `lagged_month_id = month_id - L < 1` has no matching article row. Lag 12 drops all of 2021 (12 months).
- The Measure 1 threshold shift drops *nothing* but shrinks the post-period: lag 12 moves the cut to `month_id = 32` (Aug 2023), leaving only 5 post-period months and a pre-period that still spans months 1–31.

### KTD2 — Hold the estimation sample fixed across lags for an honest comparison (configurable; per margin, and per radius for intensive)

If each lag uses its own maximal sample, the lag-0 column and the lag-12 column are estimated on *different samples* (lag 12 silently drops 2021 for Measure 2), so coefficient movement conflates *timing* with *sample composition*. To isolate the timing channel:

- **Default (`hold_sample_fixed = TRUE`)**: every lagged column within a comparison is estimated on the **common sample** `month_id >= analysis_start_month_id + max_lag` (= `month_id >= 13` for max lag 12). The lag-0 column in this comparison is therefore the *contemporaneous spec on the restricted sample*, directly comparable to the lagged columns.
  - **Intensive caveat**: the common-sample restriction is applied **within each (measure, radius) cell** — every lag column at a given radius shares one sample; samples differ across radii by construction (the `radius == RAD` + `n_spill_sites > 0` filters select different properties).
- **Reference column**: additionally report the **full-sample contemporaneous** estimate (`month_id >= 1`, no restriction) — this reproduces the published result and shows what the restriction itself costs. For the intensive margin, the reference is per radius.
- **Trade-off to document**: for Measure 1, restricting to `month_id >= 13` removes months 1–12 of the clean pre-peak period (peak is month 20), weakening the pre/post contrast. This is the price of cross-lag comparability; the full-sample reference column lets the reader see the published estimate alongside.

`hold_sample_fixed` is a CONFIG flag so the secondary "each-lag-maximal-sample" variant can be produced by flipping it.

### KTD3 — One preferred specification per measure per margin, lag as the comparison dimension

The published scripts report 12 columns (pooled / MSOA+month / LSOA+month × with/without controls × sales/rentals). For a lag-comparison table that is unreadable. Fix the spec to the **fully-saturated preferred specification** (the `model_sale_5` analog) and let **lag** (and, for intensive, **radius**) vary:

- **Extensive preferred spec** = `near_bin` + interaction + property controls, **LSOA + month FE**, LSOA-clustered SEs:
  - Measure 1: `log_price ~ near_bin + near_bin:post + property_type + old_new + duration | lsoa + month_id`
  - Measure 2: `log_price ~ near_bin + near_bin:log_cumulative_articles + property_type + old_new + duration | lsoa + month_id`
- **Intensive preferred spec** = `spill_count_daily_avg` + interaction + property controls, **LSOA + month FE**, LSOA-clustered SEs (the `model_sale_5` analog in `did_trends_prior.R` / `did_articles_prior.R`):
  - Measure 1: `log_price ~ spill_count_daily_avg + spill_count_daily_avg:post + property_type + old_new + duration | lsoa + month_id`
  - Measure 2: `log_price ~ spill_count_daily_avg + spill_count_daily_avg:log_cumulative_articles + property_type + old_new + duration | lsoa + month_id`
  - **FE choice**: use **month FE** (not the `qtr_id` seen in `did_articles_lag4_prior.R`) — consistent with the non-lag intensive baseline and the extensive plan. Record this deviation from the lag4 precedent in a code comment and the Risks section.
- **Identification note (carry into code comments)**: with `month_id` FE, the `post` / `log_cumulative_articles` *main* effect is monthly-constant and absorbed; only the interaction is identified. This already holds in the baselines and is unchanged by lagging (the lagged measure is still a deterministic function of `month_id`).
- **Rentals**: include the contemporaneous (lag 0, full sample) rentals preferred-spec estimate as a **benchmark column/panel** (per radius on the intensive side) so the reader can read the sales lag sweep against the significant rental effect. Do **not** sweep lags for rentals (rentals have no agreement-to-completion gap).

### KTD4 — Output artifacts: per-measure tables + one shared effect-size CSV

Follow the pattern set by `did_articles_windowed_prior_extensive.R` (per-parameter `.tex` + an `effect_sizes.csv`) and the post-analysis report convention in `docs/reports/`:

- **Extensive** — one **comparison `.tex` per measure** under `output/tables/`, columns = {full-sample contemporaneous reference, lag 0 (common sample), lag 3, lag 6, lag 12} for the sales preferred spec, plus a rentals contemporaneous benchmark column.
- **Intensive** — one **lag × radius grid `.tex` per measure** under `output/tables/`: **rows = radius (250/500/1000m)**, **columns = {full-sample contemporaneous reference, lag 0 (common sample), lag 3, lag 6, lag 12}**, cells = interaction estimate with clustered SE and stars, plus a rentals contemporaneous benchmark row per radius. This single grid is the primary intensive artifact (far more readable than six per-radius files). Per-radius detailed tables are optional appendix output via the existing `run_for_radius()` pattern; the consolidated grid leverages `utils_radius_robustness_table.R` conventions.
- **One shared effect-size CSV** `output/tables/did_news_lagged_sales_effect_sizes.csv` capturing **every** cell across both margins: columns `margin` (extensive|intensive), `measure` (post|articles), `radius` (band label `0-500_vs_1000-2000` for extensive; numeric metres for intensive), `lag`, `sample` (full|common), `estimate`, `std_error`, `p_value`, `n`. Built with the existing `extract_fixest_term()` helper in `windowed_article_effect_size_utils.R`. All four scripts append to it.
- An **optional results report** under `docs/reports/` (`YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{md,html}`) summarising whether the lag sweep moves the sales interaction toward the rental sign/magnitude — i.e., whether mistiming explains the null (R4) — across both margins.

### KTD5 — Radius scope differs by margin

- **Extensive**: runs at the headline band comparison **0–500m vs 1000–2000m** only. The band-radius robustness machinery (`run_radius_robustness()` / `utils_radius_robustness_table.R`) is **not** wired in here — issue #21 scopes the extensive work to lags, not band radius. A lag × band-radius grid stays under Deferred Work.
- **Intensive**: runs the full **lag × radius grid** over `RADII = c(250, 500, 1000)` (R6 directive). This is the one place the plan crosses lag with radius.

### KTD6 — Shared, margin-agnostic lag helpers live in a new `news_lag_utils.R`

The lag mechanics (`shifted_post_indicator`, `join_lagged_cumulative_articles`, `restrict_to_common_sample`) are identical across margins and have no dependency on the extensive-specific data loaders. To avoid duplication and the naming mismatch of sourcing `extensive_margin_news_utils.R` from intensive scripts, put these three helpers in a **new dedicated file** `scripts/R/09_analysis/05_news/news_lag_utils.R`, sourced by all four new scripts. The extensive scripts continue to source `extensive_margin_news_utils.R` for data loading/sample building; the intensive scripts continue to follow their own inline loading (mirroring `did_articles_lag4_prior.R`). Only the lag helpers are shared.

---

## Files

**Inspect (read-only, baselines to mirror — do not modify):**

*Extensive:*
- `scripts/R/09_analysis/05_news/did_trends_prior_extensive.R` — Measure 1 contemporaneous baseline (post indicator, `load_google_trends_peak`, `post` construction ~line 136).
- `scripts/R/09_analysis/05_news/did_articles_prior_extensive.R` — Measure 2 contemporaneous baseline (cumulative-article join ~lines 131–135).
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior_extensive.R` — single-lag precedent (lag join + start-month filter ~lines 131–137, 194–200; sample-loss note ~lines 157–160).
- `scripts/R/09_analysis/05_news/did_articles_windowed_prior_extensive.R` — precedent for a parameter sweep + `.tex`-per-parameter + effect-size CSV.

*Intensive:*
- `scripts/R/09_analysis/05_news/did_trends_prior.R` — Measure 1 intensive baseline (`spill_count_daily_avg`, `PEAK_MONTH_ID` ~line 93, `model_sale_5` ~line 292, per-radius `run_for_radius` + radius-robustness table).
- `scripts/R/09_analysis/05_news/did_articles_prior.R` — Measure 2 intensive baseline (`RADII` loop, `model_sale_5` ~line 310, `run_for_radius` ~line 496, radius-robustness consolidation).
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior.R` — intensive single-lag precedent (RAD=250, LAG=4; lag join ~lines 122–127; `lsoa + qtr_id` FE; "Excluded first N months" note ~line 150).
- `scripts/R/09_analysis/utils_radius_robustness_table.R` — radius-grid consolidation helper used by the intensive baselines.

**Reuse (shared helpers, source as-is):**
- `scripts/R/09_analysis/05_news/extensive_margin_news_utils.R` — `validate_comparison_config`, `load_google_trends_peak`, `load_articles_data`, `load_nearest_distance_lookup`, `load_sales_transactions`, `load_rental_transactions`, `build_extensive_margin_sample`, `standardise_*_estimation_data`, `print_extensive_margin_summary`, `comparison_note_text`, `patch_modelsummary_latex` (extensive scripts only).
- `scripts/R/09_analysis/05_news/windowed_article_effect_size_utils.R` — `extract_fixest_term()` for the effect-size CSV (all scripts).
- `scripts/R/09_analysis/utils_radius_robustness_table.R` — radius-grid table conventions (intensive scripts).

**Create:**
- `scripts/R/09_analysis/05_news/news_lag_utils.R` — shared lag helpers (KTD6, see U1).
- `scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R` — extensive Measure 1 lag sweep (U2).
- `scripts/R/09_analysis/05_news/did_articles_lagged_sales_extensive.R` — extensive Measure 2 lag sweep (U3).
- `scripts/R/09_analysis/05_news/did_trends_lagged_sales.R` — intensive Measure 1 lag × radius grid (U4).
- `scripts/R/09_analysis/05_news/did_articles_lagged_sales.R` — intensive Measure 2 lag × radius grid (U5).
- Generated tables:
  - `output/tables/did_trends_lagged_sales_extensive.tex`, `output/tables/did_articles_lagged_sales_extensive.tex` (extensive).
  - `output/tables/did_trends_lagged_sales_grid.tex`, `output/tables/did_articles_lagged_sales_grid.tex` (intensive lag × radius grids); optional per-radius `did_*_lagged_sales_<RAD>m.tex`.
  - `output/tables/did_news_lagged_sales_effect_sizes.csv` (shared; all four scripts append).
- `docs/reports/YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{md,html}` (optional, U6).

> **Testing note**: this repo has **no R test harness** (only Python deps carry `tests/`). "Test scenarios" below are concrete **sanity assertions** the implementer should encode as inline `stopifnot()` / `cat()` diagnostics inside each script (matching the existing `print_extensive_margin_summary` diagnostic style), plus manual checks on the produced tables. They are not a separate test framework.

---

## Implementation Units

### U1. Shared lag helpers — `news_lag_utils.R`

**Goal**: centralize the lag mechanics so all four new scripts share one tested implementation, generalizing the inline logic in the two lag4 precedents (KTD6).

**Requirements**: R1, R2, R3 (mechanism), R5/R6 (consistency).

**Dependencies**: none.

**Files**: `scripts/R/09_analysis/05_news/news_lag_utils.R` (new).

**Approach** — three small, documented helpers (roxygen-style headers matching repo conventions):
- `shifted_post_indicator(month_id, peak_month_id, lag)` → integer `1[month_id >= peak_month_id + lag]`. Pure; no row drops. Used by Measure 1 (both margins).
- `join_lagged_cumulative_articles(sample, articles, lag, start_month_id)` → mutate `lagged_month_id = month_id - lag`, filter `lagged_month_id >= start_month_id`, `inner_join` `articles` (`cumulative_articles`, `log_cumulative_articles`) on `lagged_month_id = month_id`. `lag = 0` must reproduce the contemporaneous join. Used by Measure 2 (both margins).
- `restrict_to_common_sample(sample, start_month_id, max_lag)` → filter `month_id >= start_month_id + max_lag`. Applied per (margin, radius) cell when `hold_sample_fixed = TRUE` (KTD2).

**Patterns to follow**: `.data$` pronoun usage and helper-signature style in `extensive_margin_news_utils.R`.

**Test scenarios** (inline assertions):
- `shifted_post_indicator(20, 20, 0)` → 1; `shifted_post_indicator(31, 20, 12)` → 0; `shifted_post_indicator(32, 20, 12)` → 1.
- `join_lagged_cumulative_articles(sample, articles, lag = 0, start = 1)` returns the same rows and values as a direct `month_id` join (lag-0 identity).
- `join_lagged_cumulative_articles(..., lag = L, ...)` drops exactly rows with `month_id < start + L` (assert min `month_id == start + L`).
- `restrict_to_common_sample(sample, 1, 12)` yields `min(month_id) == 13` and is a subset.

**Verification**: sourcing the file errors-free; three helpers exist with documented args.

---

### U2. Extensive Measure 1 — `did_trends_lagged_sales_extensive.R`

**Goal**: post-peak-indicator sales lag comparison (lag 0/3/6/12) + full-sample contemporaneous reference + rentals benchmark, at the headline band.

**Requirements**: R1, R2, R3 (M1), R4. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_trends_lagged_sales_extensive.tex`; appends to the shared effect-size CSV.

**Approach**:
- Copy the `CONFIG` / package-check / `initialise_environment` scaffold from `did_trends_prior_extensive.R`. Add `CONFIG$lags = c(0L, 3L, 6L, 12L)`, `CONFIG$hold_sample_fixed = TRUE`, `CONFIG$max_lag = max(CONFIG$lags)`. Keep `comparison = 0–500m vs 1000–2000m`. Source `news_lag_utils.R`.
- Load sales + lookup + `peak_info` (`load_google_trends_peak`); build the base extensive sample once (`build_extensive_margin_sample`).
- For each lag `L`: `post = shifted_post_indicator(month_id, peak_month_id, L)`; if `hold_sample_fixed`, `restrict_to_common_sample(..., max_lag)`; standardise; estimate preferred spec (`near_bin + near_bin:post + controls | lsoa + month_id`, `vcov = ~lsoa`).
- References: (a) full-sample contemporaneous sales (L=0, no restriction → reproduces published spec); (b) rentals contemporaneous preferred spec (full sample).
- One `modelsummary` `cbind` table: columns = [full-sample contemp | lag 0 | lag 3 | lag 6 | lag 12 | rentals contemp]; coef row `near_bin:post`. `patch_modelsummary_latex` + `add_rows` (lag, sample, FE, controls). Notes: peak month, threshold shifted forward by lag, common-sample restriction, pre-period shrinkage caveat (KTD2).
- Effect-size rows via `extract_fixest_term(model, "near_bin:post")` → append `(margin="extensive", measure="post", radius="0-500_vs_1000-2000", lag, sample, …)`.

**Test scenarios**: lag-0 common-OFF reproduces `model_sale_5` of `did_trends_prior_extensive.R`; per-lag post cut `== peak_month_id + L`; identical N across lag columns when fixed; CSV rows finite; `.tex` has `near_bin:post` row and correct column count.

**Verification**: runs to "completed successfully"; `.tex` + CSV exist; lag-0 reference matches published estimate.

---

### U3. Extensive Measure 2 — `did_articles_lagged_sales_extensive.R`

**Goal**: generalize `did_articles_lag4_prior_extensive.R` to the {0,3,6,12} sweep with preferred spec, common-sample handling, full-sample reference, rentals benchmark.

**Requirements**: R1, R2, R3 (M2), R4. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_articles_lagged_sales_extensive.tex`; appends to the shared CSV.

**Approach**:
- Scaffold from `did_articles_lag4_prior_extensive.R` + `did_articles_prior_extensive.R`. Same CONFIG (lags, fixed sample, max_lag). Load articles via `load_articles_data`, sales/lookup, build base sample once. Source `news_lag_utils.R`.
- For each lag `L`: `join_lagged_cumulative_articles(base_sample, articles, L, start_month_id)` (lag 0 = contemporaneous). If `hold_sample_fixed`, `restrict_to_common_sample(..., max_lag)` **after** the join. Standardise; estimate preferred spec (`near_bin + near_bin:log_cumulative_articles + controls | lsoa + month_id`).
- References: full-sample contemporaneous sales (lag 0, no restriction → reproduces published null); rentals contemporaneous benchmark.
- Same `cbind` table + CSV append (`extract_fixest_term(model, "near_bin:log_cumulative_articles")`, `measure="articles"`). Notes: count taken as of the lagged month; lag `L` mechanically drops the first `L` transaction months (KTD1); common-sample restriction.

**Test scenarios**: lag-0 common-OFF reproduces `model_sale_5` of `did_articles_prior_extensive.R`; lag-`L` common-OFF drops first `L` months (`min(month_id) == start + L`); identical N across lags when fixed; `log_cumulative_articles` finite + monotone non-decreasing in `lagged_month_id`; `.tex` has the interaction row.

**Verification**: runs clean; lag-0 reference matches published null; per-lag months-dropped diagnostic prints.

---

### U4. Intensive Measure 1 — `did_trends_lagged_sales.R` (lag × radius grid)

**Goal**: post-peak-indicator sales lag sweep crossed with radius (250/500/1000m), continuous `spill_count_daily_avg` treatment, plus per-radius full-sample reference and rentals benchmark.

**Requirements**: R1, R2, R3 (M1), R4, **R6**. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_trends_lagged_sales_grid.tex` (and optional per-radius `did_trends_lagged_sales_<RAD>m.tex`); appends to the shared CSV.

**Approach**:
- Scaffold from `did_trends_prior.R` (intensive baseline) + `did_articles_lag4_prior.R` (lag mechanics). `CONFIG$RADII = c(250L, 500L, 1000L)`, `CONFIG$lags = c(0L,3L,6L,12L)`, `hold_sample_fixed = TRUE`, `max_lag = 12`. Compute `PEAK_MONTH_ID` as in the baseline (peak Aug 2022 → 20). Source `news_lag_utils.R`.
- Generalize the baseline `run_for_radius(RAD)` into `run_for_radius_lag(RAD, L)`:
  - Load cross-section sales filtered `radius == RAD`, `n_spill_sites > 0`; join transactions (mirror baseline loading).
  - `post = shifted_post_indicator(month_id, PEAK_MONTH_ID, L)`; if `hold_sample_fixed`, `restrict_to_common_sample(..., max_lag)` (within this RAD).
  - Estimate preferred spec: `log_price ~ spill_count_daily_avg + spill_count_daily_avg:post + controls | lsoa + month_id`, `vcov = ~lsoa`.
- Per radius, also estimate: full-sample contemporaneous sales (L=0, no restriction → reproduces `model_sale_5` of `did_trends_prior.R` at that radius); rentals contemporaneous benchmark.
- **Grid table** (KTD4): rows = radius, columns = [full-sample contemp | lag 0 | lag 3 | lag 6 | lag 12], cells = `spill_count_daily_avg:post` estimate (clustered SE, stars), + a rentals contemp row per radius. Follow `utils_radius_robustness_table.R` conventions for the row-per-radius layout. Notes: peak month, threshold-forward shift, common-sample restriction per radius, month-FE choice.
- CSV rows: `(margin="intensive", measure="post", radius=RAD, lag, sample, …)` for every cell + references.

**Test scenarios**: per radius, lag-0 common-OFF reproduces `model_sale_5` of `did_trends_prior.R` (coef, SE, N); post cut `== PEAK_MONTH_ID + L`; identical N across lags within each radius when fixed; grid `.tex` has one row per radius and the right column count; CSV has every (radius × {full-contemp,0,3,6,12}) cell + rentals benchmark, all finite.

**Verification**: runs to completion; grid `.tex` + CSV exist; per-radius lag-0 references match the published intensive estimates.

---

### U5. Intensive Measure 2 — `did_articles_lagged_sales.R` (lag × radius grid)

**Goal**: generalize `did_articles_lag4_prior.R` from a single 4-month lag at RAD=250 to the {0,3,6,12} × {250,500,1000} grid with the preferred (month-FE) spec, common-sample handling, per-radius full-sample reference, and rentals benchmark.

**Requirements**: R1, R2, R3 (M2), R4, **R6**. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_articles_lagged_sales_grid.tex` (and optional per-radius files); appends to the shared CSV.

**Approach**:
- Scaffold from `did_articles_lag4_prior.R` + `did_articles_prior.R`. Same CONFIG (RADII, lags, fixed sample, max_lag). Load articles (cumulative + log cumulative). Source `news_lag_utils.R`.
- `run_for_radius_lag(RAD, L)`: load cross-section sales `radius == RAD`, `n_spill_sites > 0`; join transactions; `join_lagged_cumulative_articles(sample, articles, L, start_month_id)` (lag 0 = contemporaneous). If `hold_sample_fixed`, `restrict_to_common_sample(..., max_lag)` after the join. Estimate preferred spec: `log_price ~ spill_count_daily_avg + spill_count_daily_avg:log_cumulative_articles + controls | lsoa + month_id` (**month FE**, deliberately departing from the lag4 precedent's `qtr_id`), `vcov = ~lsoa`.
- Per radius: full-sample contemporaneous sales (lag 0, no restriction → reproduces `model_sale_5` of `did_articles_prior.R` at that radius); rentals contemporaneous benchmark.
- Same grid table + CSV append (`extract_fixest_term(model, "spill_count_daily_avg:log_cumulative_articles")`, `measure="articles"`). Notes: count as of the lagged month; lag `L` drops the first `L` transaction months (KTD1); common-sample restriction per radius; month-FE choice.

**Test scenarios**: per radius, lag-0 common-OFF reproduces `model_sale_5` of `did_articles_prior.R`; lag-`L` common-OFF drops first `L` months; identical N across lags within radius when fixed; `log_cumulative_articles` finite + monotone; grid `.tex` row-per-radius; CSV complete.

**Verification**: runs clean; per-radius lag-0 references match published intensive estimates; per-(radius,lag) months-dropped diagnostic prints.

---

### U6. Cross-margin comparison artifact + results narrative (R4)

**Goal**: turn the four scripts' output / shared effect-size CSV into a single answer to "does mistiming explain the null?" — across both margins.

**Requirements**: R4, R5, R6. **Dependencies**: U2, U3, U4, U5.

**Files**: `output/tables/did_news_lagged_sales_effect_sizes.csv` (consumed); `docs/reports/YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{md,html}` (optional; follows the `docs/reports/` pattern, e.g. `2026-06-23-001-windowed-article-salience-results-report.html`).

**Approach**:
- Read the combined CSV; tabulate sales `estimate (p)` by (margin × measure × lag), with the full-sample contemporaneous and rentals benchmark rows alongside; for the intensive margin, break out by radius (or report the headline 250m row with the grid in an appendix).
- Narrative judgement: does the sales interaction move toward the rental sign/magnitude as the lag increases, and does it cross significance at any lag, on either margin? State plainly whether mistiming plausibly explains the null, or whether the sales effect remains null even under the agreement-to-completion shift. Note where intensive and extensive agree or diverge.
- Keep this lightweight — a CSV summary plus a short report. The per-measure `.tex` tables / grids remain the primary artifacts.

**Test scenarios**: CSV parses; every (margin, measure, radius, lag) cell present; report renders without broken references.

**Verification**: report exists and cites freshly produced numbers; no placeholders.

---

## Risks & Design Choices

- **Sample loss at the start of 2021 (Measure 2, both margins)** — the cumulative-article lag join drops the first `L` months; lag 12 removes all of 2021. Mitigated by KTD2's common fixed sample (per radius on the intensive side) plus a full-sample contemporaneous reference. Each script must print months/observations dropped per lag (per radius for intensive), mirroring the lag4 precedents' "Excluded first N months" note.
- **Shift direction (KTD1)** — threshold-forward (post) vs measure-backward (articles) are algebraically identical; mixing them inconsistently is the main correctness trap. The U1 helpers and lag-0 identity tests guard against it.
- **Measure 1 pre-period shrinkage under the common sample** — restricting to `month_id >= 13` removes a year of clean pre-peak data (peak at month 20). The full-sample reference column preserves the published estimate so the cost is visible. If Measure 1 power collapses at lag 12, report it.
- **Power at long lags (Measure 1)** — lag 12 leaves only 5 post-peak months (32–36); a null there may be low power, not absence of effect. Note in the report.
- **Intensive FE choice (month vs quarter)** — the intensive lag4 precedent uses `lsoa + qtr_id`; this plan uses `lsoa + month_id` for the preferred spec to match the non-lag intensive baseline and the extensive plan. Document the deviation in code; if it materially changes results, report both.
- **Intensive radius sample composition** — the `radius == RAD` + `n_spill_sites > 0` filters select different properties per radius, so radii are not nested samples. The common-sample restriction is applied within each (measure, radius) cell, not across radii; the grid compares lags within a radius, not levels across radii.
- **Spec drift from the published baselines** — the lag-0, full-sample reference columns must reproduce `model_sale_5` of the contemporaneous scripts exactly (per radius for intensive); treat any mismatch as a bug in the new script, not a finding.
- **Lag set** — issue text suggests "1, 3, 6"; this plan uses **3, 6, 12** per directive, with `CONFIG$lags` configurable so a 1-month lag can be added trivially.
- **Table proliferation (intensive)** — six per-radius files would be unreadable; the plan's primary intensive artifact is one lag × radius **grid** per measure (KTD4), with per-radius detail optional.

---

## Deferred to Follow-Up Work

- **Lag × band-radius grid (extensive)** — re-running the extensive sweep across alternative near/far band definitions via `run_radius_robustness()` / `utils_radius_robustness_table.R`. (Intensive lag × radius is now in scope; this item is the extensive band analog.)
- **Rentals placebo lag** — lagging rentals (which should *not* improve, since listing prices are contemporaneous) as a falsification check.
- **Continuous agreement-to-completion model** — distributed-lag or a calibrated single lag from registration-gap data, rather than a discrete sweep.

---

## Verification & Commands

Run from repo root with project R 4.6.0 (rv-managed environment). Scripts self-execute via `if (sys.nframe() == 0) main()`.

```bash
# Extensive margin (headline band)
Rscript scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R     # Measure 1
Rscript scripts/R/09_analysis/05_news/did_articles_lagged_sales_extensive.R   # Measure 2

# Intensive margin (lag × radius grid over 250/500/1000m)
Rscript scripts/R/09_analysis/05_news/did_trends_lagged_sales.R               # Measure 1
Rscript scripts/R/09_analysis/05_news/did_articles_lagged_sales.R             # Measure 2

# If rv must wrap execution, prefix with: rv run
```

Post-run checks:
- Extensive: `output/tables/did_trends_lagged_sales_extensive.tex` and `did_articles_lagged_sales_extensive.tex` exist with the expected interaction row and column count.
- Intensive: `output/tables/did_trends_lagged_sales_grid.tex` and `did_articles_lagged_sales_grid.tex` exist with one row per radius and the expected columns.
- `output/tables/did_news_lagged_sales_effect_sizes.csv` has rows for every (margin × measure × radius × {full-sample contemp, lag 0, 3, 6, 12}) plus rentals benchmarks, all finite.
- Lag-0 full-sample reference cells match the published `model_sale_5` estimates from the contemporaneous scripts (extensive: the two `*_prior_extensive.R`; intensive: `did_trends_prior.R` / `did_articles_prior.R` per radius).
- Console diagnostics print per-lag (and per-radius) sample sizes and months dropped.
- (If U6 done) the results report renders and cites freshly produced numbers.
