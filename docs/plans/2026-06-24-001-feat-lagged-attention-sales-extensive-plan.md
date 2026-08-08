# feat: Lagged public-attention sales re-estimation — extensive + intensive margins (Issue #21)

Created: 2026-06-24
Revised: 2026-08-06 — incorporated the confirmed grilling decisions: extensive margin is the issue-closing core, intensive margin is a labelled extension; post models retain the full sample; article models use a common Jan 2022--Dec 2023 sample; the headline extensive rental lag sweep is a secondary placebo; the results report and coefficient-path figure are required; result consolidation is deterministic.
Type: feat
Depth: Standard
Origin: GitHub issue #21 — "Re-estimate extensive-margin sales regressions with lagged public-attention measures" (parent: sticerd-eee/sewage#2). The intensive-margin extension was added by directive on 2026-06-25 (issue #21 was extensive-only; this plan now covers both margins).

---

## Summary

The public-attention interaction is null for **house sales** but negative and significant (≈0.8 pp on the extensive margin) for **rentals**. The leading explanation is *mistimed attention*: rental listing prices are set near the listing date, but sale prices reflect a transaction price agreed weeks-to-months before the recorded completion/registration date. Because the attention measure is currently aligned to the recorded transaction month, for sales it is dated *after* the price was actually agreed.

This plan adds a **systematic lag sweep** for sales over the two public-attention measures, lagging attention relative to the recorded transaction date to approximate the agreement-to-completion gap. Lags of **3, 6, and 12 months** are estimated against the **contemporaneous** (lag 0) baseline. The **extensive margin is the issue-closing core analysis**; the intensive margin is a clearly labelled extension. Rentals enter both as a contemporaneous benchmark and, for the headline extensive-margin comparison only, as a secondary placebo lag sweep.

Two margins, distinguished by the **treatment variable** interacted with the attention measure:
- **Extensive margin** — binary `near_bin` (near band **0–500m** vs far band **1000–2000m**). Run at the single headline band comparison; radius (band) sweep deferred.
- **Intensive margin extension** — continuous `spill_count_weekly_avg` (weekly-average spill count within radius `RAD`, among properties with `n_spill_sites > 0`). Run as a **lag × radius grid** over `RADII = c(250, 500, 1000)`.

Scope is **plan + new analysis scripts + required results synthesis**: no changes to the published contemporaneous scripts, no data-pipeline changes, no new *band*-radius sweep for the extensive margin.

---

## Problem Frame

Two **attention measures**, each interacted with the margin's treatment variable:

- **Measure 1 — Post-peak indicator**: `post = 1[month_id >= peak_month_id]`, where the Google Trends peak is August 2022. Coefficient of interest is the `treatment:post` interaction.
- **Measure 2 — Log cumulative articles**: `log_cumulative_articles` = log of cumulative LexisNexis UK sewage coverage from Jan 2021 to the transaction month. Coefficient of interest is the `treatment:log_cumulative_articles` interaction.

Crossed with the two margins, the four coefficients of interest are:

| | Measure 1 (post) | Measure 2 (articles) |
|---|---|---|
| **Extensive** (`near_bin`) | `near_bin:post` | `near_bin:log_cumulative_articles` |
| **Intensive extension** (`spill_count_weekly_avg`) | `spill_count_weekly_avg:post` | `spill_count_weekly_avg:log_cumulative_articles` |

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
- **R6 (intensive extension)** — After completing the extensive core, estimate the intensive margin (`spill_count_weekly_avg`) as a labelled **lag × radius extension** over `RADII = c(250, 500, 1000)`; the extensive margin stays at the single headline band comparison (KTD5).
- **R7 (rental placebo)** — Run the same lag sweep for rentals only at the headline extensive comparison. Treat it as a secondary falsification exercise, not part of the primary sales specification grid.

---

## Key Technical Decisions

### KTD1 — Lag is implemented per measure, but the two framings are algebraically identical (both margins)

For both measures, "lag the attention by `L` months" means: *use the attention value that prevailed `L` months before the recorded transaction month.* This is margin-agnostic — only the treatment variable interacted with the (now lagged) attention measure differs.

- **Measure 2 (cumulative articles)** — shift the **measure backward**: join `log_cumulative_articles` keyed on `month_id - L` instead of `month_id`. This is exactly what the lag4 precedents do (`lagged_month_id = month_id - lag`, then `inner_join(articles, by = c("lagged_month_id" = "month_id"))`).
- **Measure 1 (post indicator)** — shift the **threshold forward**: `post_L = 1[month_id >= peak_month_id + L]`. This is identical to `1[month_id - L >= peak_month_id]` (shifting the measure backward), but the forward-threshold form is preferred because **it drops no observations** (every transaction retains a defined `post` status).

**Why the distinction matters (a real asymmetry, surfaced in table notes):**
- The Measure 2 join *mechanically drops* the first `L` transaction months, because `lagged_month_id = month_id - L < 1` has no matching article row. Lag 12 drops all of 2021 (12 months).
- The Measure 1 threshold shift drops *nothing* but shrinks the post-period: lag 12 moves the cut to `month_id = 32` (Aug 2023), leaving only 5 post-period months and a pre-period that still spans months 1–31.

### KTD2 — Sample policy differs by measure

The two attention measures have different data-support consequences, so imposing one sample rule on both would discard useful information.

- **Measure 1 (post indicator): full Jan 2021--Dec 2023 sample at every lag.** Shifting the threshold forward creates no missing values. All lag-0/3/6/12 post models therefore retain `month_id = 1:36`. This preserves the full pre-period and maximises power.
- **Measure 2 (cumulative articles): common Jan 2022--Dec 2023 sample for the main lag comparison.** The cumulative measure remains defined from January 2021 (KTD1/Q4). A 12-month lag is therefore undefined for all 2021 transactions. To prevent coefficient movement from reflecting sample composition, every main lag-0/3/6/12 article model uses `month_id >= 13`.
  - Additionally report a **full-sample contemporaneous reference** (`lag = 0`, `month_id = 1:36`) to reproduce the published result and reveal the effect of the common-sample restriction itself.
  - For the intensive extension, apply the common article sample within each radius. Radius-specific property samples still differ by construction.
- **Rental placebo:** follow the same measure-specific rules: full sample for post-indicator lags and the Jan 2022--Dec 2023 common sample for article lags, with a full-sample contemporaneous article benchmark.

Do not expose a `hold_sample_fixed` switch in the primary workflow. The confirmed policy is part of the design, and per-lag maximal samples are out of scope because they confound lag timing with observation changes.

### KTD3 — One preferred specification per measure per margin, lag as the comparison dimension

The published scripts report 12 columns (pooled / MSOA+month / LSOA+month × with/without controls × sales/rentals). For a lag-comparison table that is unreadable. Fix the spec to the **fully-saturated preferred specification** (the `model_sale_5` analog) and let **lag** (and, for intensive, **radius**) vary:

- **Extensive preferred spec** = `near_bin` + interaction + property controls, **LSOA + month FE**, LSOA-clustered SEs:
  - Measure 1: `log_price ~ near_bin + near_bin:post + property_type + old_new + duration | lsoa + month_id`
  - Measure 2: `log_price ~ near_bin + near_bin:log_cumulative_articles + property_type + old_new + duration | lsoa + month_id`
- **Intensive preferred spec** = `spill_count_weekly_avg` + interaction + property controls, **LSOA + month FE**, LSOA-clustered SEs (the current `model_sale_5` analog in `did_trends_prior.R` / `did_articles_prior.R`):
  - Measure 1: `log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post + property_type + old_new + duration | lsoa + month_id`
  - Measure 2: `log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:log_cumulative_articles + property_type + old_new + duration | lsoa + month_id`
  - **FE choice**: use **month FE** (not the `qtr_id` seen in `did_articles_lag4_prior.R`) — consistent with the non-lag intensive baseline and the extensive plan. Record this deviation from the lag4 precedent in a code comment and the Risks section.
- **Identification note (carry into code comments)**: with `month_id` FE, the `post` / `log_cumulative_articles` *main* effect is monthly-constant and absorbed; only the interaction is identified. This already holds in the baselines and is unchanged by lagging (the lagged measure is still a deterministic function of `month_id`).
- **Rentals**: include the contemporaneous rentals preferred-spec estimate as a benchmark in the primary sales tables. In addition, sweep lags 0/3/6/12 for rentals at the headline extensive comparison only as a **secondary placebo**. Do not add rental lag sweeps to the intensive radius extension.

### KTD4 — Output artifacts: per-measure tables, deterministic result components, figure, and required report

Follow the pattern set by `did_articles_windowed_prior_extensive.R` (per-parameter `.tex` + an `effect_sizes.csv`) and the post-analysis report convention in `docs/reports/`:

- **Extensive core** — one comparison `.tex` per measure. Post columns are {lag 0, lag 3, lag 6, lag 12} on the full sample, plus a contemporaneous rentals benchmark. Article columns are {full-sample contemporaneous reference, common-sample lag 0, lag 3, lag 6, lag 12}, plus a contemporaneous rentals benchmark. Rental placebo paths are kept in component CSVs and the results report rather than expanding the primary tables.
- **Intensive extension** — one lag × radius grid `.tex` per measure. Post columns are {lag 0, lag 3, lag 6, lag 12}, all full-sample. Article columns are {full-sample contemporaneous reference, common-sample lag 0, lag 3, lag 6, lag 12}. Include contemporaneous rentals benchmarks only; do not run intensive rental lag paths.
- **Idempotent component CSVs** — each of the four analysis scripts writes (replaces) its own uniquely named result CSV rather than appending to a shared file. Required columns are `margin`, `market`, `measure`, `radius`, `lag`, `sample`, `estimate`, `std_error`, `conf_low`, `conf_high`, `p_value`, and `n`.
- **Deterministic consolidation** — a dedicated summary script reads the four component CSVs, validates unique result keys and expected coverage, binds them in a fixed order, and atomically replaces `output/tables/did_news_lagged_sales_effect_sizes.csv`. Re-running any script or the full workflow must not duplicate rows.
- **Required coefficient-path figure** — show estimates and ordinary pointwise 95% confidence intervals by lag. Extensive sales paths are the core panels; the headline extensive rental placebo is a clearly marked secondary panel; intensive radius paths are labelled as extensions.
- **Required results report** — create a source document and rendered HTML under `docs/reports/` (`YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{qmd,html}`). It must state whether timing plausibly explains the sales null using the pre-specified pattern criterion in KTD7, not an isolated p-value.

### KTD5 — Radius scope and core/extension status differ by margin

- **Extensive**: runs at the headline band comparison **0–500m vs 1000–2000m** only. The band-radius robustness machinery (`run_radius_robustness()` / `utils_radius_robustness_table.R`) is **not** wired in here — issue #21 scopes the extensive work to lags, not band radius. A lag × band-radius grid stays under Deferred Work.
- **Intensive extension**: after the extensive core is complete, run the full **lag × radius grid** over `RADII = c(250, 500, 1000)`. Label these results as an extension in tables, figures, and prose.

### KTD6 — Shared, margin-agnostic lag helpers live in a new `news_lag_utils.R`

The lag mechanics (`shifted_post_indicator`, `join_lagged_cumulative_articles`, `restrict_to_common_sample`) are identical across margins and have no dependency on the extensive-specific data loaders. To avoid duplication and the naming mismatch of sourcing `extensive_margin_news_utils.R` from intensive scripts, put these three helpers in a **new dedicated file** `scripts/R/09_analysis/05_news/news_lag_utils.R`, sourced by all four new scripts. The extensive scripts continue to source `extensive_margin_news_utils.R` for data loading/sample building; the intensive scripts continue to follow their own inline loading (mirroring `did_articles_lag4_prior.R`). Only the lag helpers are shared.

### KTD7 — Interpretation is pattern-based; inference remains pointwise

The report may conclude that mistiming is plausible only if sales estimates move coherently toward the rental sign and an economically relevant magnitude over plausible lags, preferably across both attention measures. A lone coefficient crossing a conventional significance threshold is insufficient. Report ordinary pointwise confidence intervals and unadjusted p-values; do not add multiplicity corrections or simultaneous bands. Discuss the 12-month post estimate cautiously because it has only five post-threshold months.

---

## Files

**Inspect (read-only, baselines to mirror — do not modify):**

*Extensive:*
- `scripts/R/09_analysis/05_news/did_trends_prior_extensive.R` — Measure 1 contemporaneous baseline (post indicator, `load_google_trends_peak`, `post` construction ~line 136).
- `scripts/R/09_analysis/05_news/did_articles_prior_extensive.R` — Measure 2 contemporaneous baseline (cumulative-article join ~lines 131–135).
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior_extensive.R` — single-lag precedent (lag join + start-month filter ~lines 131–137, 194–200; sample-loss note ~lines 157–160).
- `scripts/R/09_analysis/05_news/did_articles_windowed_prior_extensive.R` — precedent for a parameter sweep + `.tex`-per-parameter + effect-size CSV.

*Intensive:*
- `scripts/R/09_analysis/05_news/did_trends_prior.R` — Measure 1 intensive baseline (`spill_count_weekly_avg`, `PEAK_MONTH_ID`, `model_sale_5`, per-radius `run_for_radius` + radius-robustness table).
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
- `scripts/R/09_analysis/05_news/summarise_lagged_attention_sales.R` — validate and deterministically consolidate component results; generate the coefficient-path figure (U6).
- Generated tables:
  - `output/tables/did_trends_lagged_sales_extensive.tex`, `output/tables/did_articles_lagged_sales_extensive.tex` (extensive).
  - `output/tables/did_trends_lagged_sales_grid.tex`, `output/tables/did_articles_lagged_sales_grid.tex` (intensive lag × radius grids); optional per-radius `did_*_lagged_sales_<RAD>m.tex`.
  - `output/tables/did_trends_lagged_sales_extensive_effect_sizes.csv`.
  - `output/tables/did_articles_lagged_sales_extensive_effect_sizes.csv`.
  - `output/tables/did_trends_lagged_sales_effect_sizes.csv`.
  - `output/tables/did_articles_lagged_sales_effect_sizes.csv`.
    Each analysis script owns and replaces exactly one of these component files.
  - `output/tables/did_news_lagged_sales_effect_sizes.csv` (deterministically consolidated by U6).
- `output/figures/did_news_lagged_sales_coefficient_paths.{pdf,png}` (required, U6).
- `docs/reports/YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{qmd,html}` (required, U6).

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
- `restrict_to_common_sample(sample, start_month_id, max_lag)` → filter `month_id >= start_month_id + max_lag`. Apply only to cumulative-article lag comparisons (sales and the extensive rental placebo), per radius for the intensive extension. Post-indicator models never call it (KTD2).

**Patterns to follow**: `.data$` pronoun usage and helper-signature style in `extensive_margin_news_utils.R`.

**Test scenarios** (inline assertions):
- `shifted_post_indicator(20, 20, 0)` → 1; `shifted_post_indicator(31, 20, 12)` → 0; `shifted_post_indicator(32, 20, 12)` → 1.
- `join_lagged_cumulative_articles(sample, articles, lag = 0, start = 1)` returns the same rows and values as a direct `month_id` join (lag-0 identity).
- `join_lagged_cumulative_articles(..., lag = L, ...)` drops exactly rows with `month_id < start + L` (assert min `month_id == start + L`).
- `restrict_to_common_sample(sample, 1, 12)` yields `min(month_id) == 13` and is a subset.

**Verification**: sourcing the file errors-free; three helpers exist with documented args.

---

### U2. Extensive Measure 1 — `did_trends_lagged_sales_extensive.R`

**Goal**: full-sample post-peak-indicator sales lag comparison (lag 0/3/6/12), contemporaneous rentals benchmark, and secondary rental placebo path at the headline band.

**Requirements**: R1, R2, R3 (M1), R4. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_trends_lagged_sales_extensive.tex`; replaces its own extensive-post component CSV.

**Approach**:
- Copy the `CONFIG` / package-check / `initialise_environment` scaffold from `did_trends_prior_extensive.R`. Add `CONFIG$lags = c(0L, 3L, 6L, 12L)`. Keep `comparison = 0–500m vs 1000–2000m`. Source `news_lag_utils.R`.
- Load sales + lookup + `peak_info` (`load_google_trends_peak`); build the base extensive sample once (`build_extensive_margin_sample`).
- For each sales lag `L`: `post = shifted_post_indicator(month_id, peak_month_id, L)`; retain the full `month_id = 1:36` sample; standardise; estimate the preferred spec (`near_bin + near_bin:post + controls | lsoa + month_id`, `vcov = ~lsoa`). Lag 0 must reproduce the published specification.
- Estimate the same full-sample lag path for rentals as a secondary placebo. Keep only lag-0 rentals in the primary `.tex`; retain the full rental path in the component CSV and results report.
- One `modelsummary` `cbind` table: columns = [sales lag 0 | lag 3 | lag 6 | lag 12 | rentals contemporaneous]; coefficient row `near_bin:post`. Notes: peak month, threshold shifted forward, full sample at every lag, and low post support at lag 12.
- Write effect-size rows via `extract_fixest_term(model, "near_bin:post")` to the script-specific component CSV with `market = sales|rentals` and `sample = full`; replace the file on every run.

**Test scenarios**: sales lag 0 reproduces `model_sale_5` of `did_trends_prior_extensive.R`; per-lag post cut `== peak_month_id + L`; identical N across sales lags and across rental placebo lags; component keys are unique; `.tex` has the interaction row and correct column count.

**Verification**: runs to "completed successfully"; `.tex` + CSV exist; lag-0 reference matches published estimate.

---

### U3. Extensive Measure 2 — `did_articles_lagged_sales_extensive.R`

**Goal**: generalize `did_articles_lag4_prior_extensive.R` to the {0,3,6,12} sweep on the common Jan 2022--Dec 2023 sample, with a full-sample contemporaneous reference, rentals benchmark, and secondary rental placebo path.

**Requirements**: R1, R2, R3 (M2), R4. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_articles_lagged_sales_extensive.tex`; replaces its own extensive-articles component CSV.

**Approach**:
- Scaffold from `did_articles_lag4_prior_extensive.R` + `did_articles_prior_extensive.R`. Set lags and `max_lag = 12`; load articles via `load_articles_data`, sales/lookup, and build each market's base sample once. Source `news_lag_utils.R`.
- For each lag `L`: `join_lagged_cumulative_articles(base_sample, articles, L, start_month_id)` (lag 0 = contemporaneous), then unconditionally call `restrict_to_common_sample(..., max_lag = 12)` for the main comparison. Standardise and estimate the preferred spec (`near_bin + near_bin:log_cumulative_articles + controls | lsoa + month_id`).
- Apply `restrict_to_common_sample(..., max_lag = 12)` to every main sales and rental-placebo lag, yielding the fixed Jan 2022--Dec 2023 sample. Separately estimate full-sample lag-0 sales and rentals references.
- Primary table columns are [sales full-sample contemporaneous | sales common lag 0 | lag 3 | lag 6 | lag 12 | rentals full-sample contemporaneous]. Keep common-sample rental lags in the component CSV/report only.
- Replace the script-specific component CSV with all sales and rental rows. Notes must distinguish the Jan 2021 origin of the measure, the common-sample comparison, and the full-sample references.

**Test scenarios**: full-sample lag 0 reproduces `model_sale_5` of `did_articles_prior_extensive.R`; every common-sample lag has `min(month_id) == 13` and identical N within market; lagged article values are finite and monotone in `lagged_month_id`; component keys are unique; `.tex` has the interaction row.

**Verification**: runs clean; lag-0 reference matches published null; per-lag months-dropped diagnostic prints.

---

### U4. Intensive Measure 1 extension — `did_trends_lagged_sales.R` (lag × radius grid)

**Goal**: full-sample post-peak-indicator sales lag sweep crossed with radius (250/500/1000m), continuous `spill_count_weekly_avg` treatment, plus contemporaneous rentals benchmarks, clearly labelled as an extension.

**Requirements**: R1, R2, R3 (M1), R4, **R6**. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_trends_lagged_sales_grid.tex` (and optional per-radius `did_trends_lagged_sales_<RAD>m.tex`); replaces its own intensive-post component CSV.

**Approach**:
- Scaffold from `did_trends_prior.R` (intensive baseline). Set `CONFIG$RADII = c(250L, 500L, 1000L)` and `CONFIG$lags = c(0L,3L,6L,12L)`. Compute `PEAK_MONTH_ID` as in the baseline (peak Aug 2022 → 20). Source `news_lag_utils.R`.
- Generalize the baseline `run_for_radius(RAD)` into `run_for_radius_lag(RAD, L)`:
  - Load cross-section sales filtered `radius == RAD`, `n_spill_sites > 0`; join transactions (mirror baseline loading).
  - `post = shifted_post_indicator(month_id, PEAK_MONTH_ID, L)`; retain the full sample at every lag.
  - Estimate preferred spec: `log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:post + controls | lsoa + month_id`, `vcov = ~lsoa`.
- Per radius, estimate a full-sample contemporaneous rentals benchmark only; do not sweep intensive rental lags.
- **Grid table** (KTD4): rows = radius, columns = [lag 0 | lag 3 | lag 6 | lag 12], cells = `spill_count_weekly_avg:post` estimate (clustered SE, stars), plus a rentals contemporaneous benchmark row per radius. Notes: extension status, peak month, threshold-forward shift, full sample, and low post support at lag 12.
- CSV rows: `(margin="intensive", measure="post", radius=RAD, lag, sample, …)` for every cell + references.

**Test scenarios**: per radius, lag 0 reproduces `model_sale_5` of `did_trends_prior.R` (coefficient, SE, N); post cut `== PEAK_MONTH_ID + L`; identical N across lags within radius; grid has the correct rows/columns; component result keys are unique and complete.

**Verification**: runs to completion; grid `.tex` + CSV exist; per-radius lag-0 references match the published intensive estimates.

---

### U5. Intensive Measure 2 extension — `did_articles_lagged_sales.R` (lag × radius grid)

**Goal**: generalize `did_articles_lag4_prior.R` from a single 4-month lag at RAD=250 to the {0,3,6,12} × {250,500,1000} grid with the preferred month-FE specification, a common Jan 2022--Dec 2023 sample, per-radius full-sample references, and contemporaneous rentals benchmarks.

**Requirements**: R1, R2, R3 (M2), R4, **R6**. **Dependencies**: U1.

**Files**: creates the script; generates `output/tables/did_articles_lagged_sales_grid.tex` (and optional per-radius files); replaces its own intensive-articles component CSV.

**Approach**:
- Scaffold from `did_articles_lag4_prior.R` + `did_articles_prior.R`. Set RADII, lags, and `max_lag = 12`; load articles (cumulative + log cumulative). Source `news_lag_utils.R`.
- `run_for_radius_lag(RAD, L)`: load cross-section sales `radius == RAD`, `n_spill_sites > 0`; join transactions; join lagged articles; restrict every main lag to `month_id >= 13`; estimate `log_price ~ spill_count_weekly_avg + spill_count_weekly_avg:log_cumulative_articles + controls | lsoa + month_id` (**month FE**, deliberately departing from the lag4 precedent's `qtr_id`), `vcov = ~lsoa`.
- Per radius: full-sample contemporaneous sales (lag 0, no restriction → reproduces `model_sale_5` of `did_articles_prior.R` at that radius); rentals contemporaneous benchmark.
- Grid and component rows use `spill_count_weekly_avg:log_cumulative_articles`. Notes distinguish the full-sample reference from the common Jan 2022--Dec 2023 comparison and label the analysis as an extension.

**Test scenarios**: per radius, the full-sample lag-0 reference reproduces `model_sale_5` of `did_articles_prior.R`; every main lag has `min(month_id) == 13` and identical N within radius; `log_cumulative_articles` is finite and monotone; grid and component results are complete with unique keys.

**Verification**: runs clean; per-radius lag-0 references match published intensive estimates; per-(radius,lag) months-dropped diagnostic prints.

---

### U6. Deterministic consolidation, coefficient paths, and required results report (R4)

**Goal**: deterministically consolidate the four component result files and answer "does mistiming explain the null?", presenting the extensive margin as the core and the intensive margin as an extension.

**Requirements**: R4, R5, R6. **Dependencies**: U2, U3, U4, U5.

**Files**: creates `scripts/R/09_analysis/05_news/summarise_lagged_attention_sales.R`; consumes the four component CSVs; writes `output/tables/did_news_lagged_sales_effect_sizes.csv`, `output/figures/did_news_lagged_sales_coefficient_paths.{pdf,png}`, and `docs/reports/YYYY-MM-DD-NNN-lagged-attention-sales-results-report.{qmd,html}`.

**Approach**:
- Read the four component CSVs in a fixed order; validate required columns, unique keys, allowed lags, expected market/margin/radius coverage, finite estimates, and the KTD2 sample labels. Abort on duplicates or missing cells. Atomically replace the consolidated CSV; never append.
- Generate coefficient-by-lag paths with ordinary pointwise 95% confidence intervals. Lead with extensive sales; show the headline extensive rental lag sweep as a secondary placebo; place intensive radius paths in an explicitly labelled extension panel or appendix.
- Tabulate sales estimates by margin × measure × lag, with full-sample contemporaneous article references and contemporaneous rental benchmarks alongside.
- Apply KTD7's pre-specified judgement: look for coherent movement toward the rental sign and economically relevant magnitude over lags, preferably across both measures. Do not treat a single conventional p-value as confirmation. Note the limited five-month post support for lag 12.
- Render a short required report that states plainly whether mistiming plausibly explains the null and whether the rental placebo supports or weakens that interpretation. Note where intensive and extensive results agree or diverge.

**Test scenarios**: consolidation is idempotent; rerunning it leaves row count and keys unchanged; deliberate duplicate/missing fixtures fail validation; every expected cell is present; figure files exist and are non-empty; report renders without broken references or placeholders.

**Verification**: report exists and cites freshly produced numbers; no placeholders.

---

## Risks & Design Choices

- **Sample loss at the start of 2021 (Measure 2, both margins)** — because the published cumulative measure begins in January 2021, a 12-month lag is undefined throughout 2021. KTD2 fixes the main article comparison to Jan 2022--Dec 2023 and retains a full-sample contemporaneous reference. Each article script must print common/full sample sizes and verify identical N across main lags within market/radius.
- **Shift direction (KTD1)** — threshold-forward (post) vs measure-backward (articles) are algebraically identical; mixing them inconsistently is the main correctness trap. The U1 helpers and lag-0 identity tests guard against it.
- **Power at long lags (Measure 1)** — all post models retain the full sample, but lag 12 moves the threshold to month 32 and leaves only five post-threshold months. Treat that estimate as weakly supported and report pre/post counts.
- **Intensive FE choice (month vs quarter)** — the intensive lag4 precedent uses `lsoa + qtr_id`; this plan uses `lsoa + month_id` for the preferred spec to match the non-lag intensive baseline and the extensive plan. Document the deviation in code; if it materially changes results, report both.
- **Intensive radius sample composition** — the `radius == RAD` + `n_spill_sites > 0` filters select different properties per radius, so radii are not nested samples. For article models, the common time restriction is applied within each radius, not across radius-specific property samples; every grid comparison is interpreted within radius rather than across radius levels.
- **Spec drift from the published baselines** — the lag-0, full-sample reference columns must reproduce `model_sale_5` of the contemporaneous scripts exactly (per radius for intensive); treat any mismatch as a bug in the new script, not a finding.
- **Lag set** — issue #21 gives 1/3/6 months as an example; the confirmed implementation uses **0/3/6/12**, preserving the later directive and keeping the vector configurable. State the chosen grid in the report rather than implying that the example grid was used verbatim.
- **Multiple comparisons** — the lag sweep creates opportunities for chance significance. Per the confirmed decision, report ordinary pointwise intervals and unadjusted p-values only, but enforce KTD7's pattern-based interpretation and never elevate an isolated significant lag.
- **Output idempotence** — independent scripts must never append to a shared result file. Each owns one component CSV; U6 validates and replaces the consolidated artifact deterministically.
- **Table proliferation (intensive)** — six per-radius files would be unreadable; the plan's primary intensive artifact is one lag × radius **grid** per measure (KTD4), with per-radius detail optional.

---

## Deferred to Follow-Up Work

- **Lag × band-radius grid (extensive)** — re-running the extensive sweep across alternative near/far band definitions via `run_radius_robustness()` / `utils_radius_robustness_table.R`. (Intensive lag × radius is now in scope; this item is the extensive band analog.)
- **Intensive rental placebo lag** — the confirmed placebo is restricted to the headline extensive comparison; lagging rentals across intensive radii remains deferred.
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

# Deterministic consolidation + coefficient paths
Rscript scripts/R/09_analysis/05_news/summarise_lagged_attention_sales.R

# Required results report
quarto render docs/reports/YYYY-MM-DD-NNN-lagged-attention-sales-results-report.qmd

# If rv must wrap execution, prefix with: rv run
```

Post-run checks:
- Extensive: `output/tables/did_trends_lagged_sales_extensive.tex` and `did_articles_lagged_sales_extensive.tex` exist with the expected interaction row and column count.
- Intensive: `output/tables/did_trends_lagged_sales_grid.tex` and `did_articles_lagged_sales_grid.tex` exist with one row per radius and the expected columns.
- Each analysis script replaces its own component CSV; rerunning it creates no duplicate keys.
- Post models use the full Jan 2021--Dec 2023 sample at every lag. Main article models use the common Jan 2022--Dec 2023 sample at every lag, plus full-sample contemporaneous references.
- The consolidated CSV has every expected extensive-core, rental-placebo, and intensive-extension cell exactly once, with all estimates and pointwise confidence intervals finite.
- Lag-0 full-sample reference cells match the published `model_sale_5` estimates from the contemporaneous scripts (extensive: the two `*_prior_extensive.R`; intensive: `did_trends_prior.R` / `did_articles_prior.R` per radius).
- Console diagnostics print per-lag (and per-radius) sample sizes and months dropped.
- Coefficient-path PDF/PNG files exist and show extensive sales core paths, the extensive rental placebo, and labelled intensive extensions.
- The required report renders, cites freshly produced numbers, applies the pattern-based decision rule, and contains no placeholders.
