# feat: Lagged public-attention extensive-margin sales re-estimation (Issue #21)

Created: 2026-06-24
Type: feat
Depth: Standard
Origin: GitHub issue #21 — "Re-estimate extensive-margin sales regressions with lagged public-attention measures" (parent: sticerd-eee/sewage#2)

---

## Summary

The extensive-margin (near/far band) public-attention interaction is null for **house sales** but negative and significant (≈0.8 pp) for **rentals**. The leading explanation is *mistimed attention*: rental listing prices are set near the listing date, but sale prices reflect a transaction price agreed weeks-to-months before the recorded completion/registration date. Because the attention measure is currently aligned to the recorded transaction month, for sales it is dated *after* the price was actually agreed.

This plan adds a **systematic lag sweep** for sales over the two extensive-margin public-attention specifications, lagging the attention measure relative to the recorded transaction date to approximate the agreement-to-completion gap. Lags of **3, 6, and 12 months** are estimated against the **contemporaneous** (lag 0) baseline, for both attention measures, with rentals carried only as a contextual benchmark.

Scope is **plan + new analysis scripts only**: no changes to the published contemporaneous scripts, no data-pipeline changes, no new radius sweep.

---

## Problem Frame

- **Measure 1 — Post-peak indicator** (`did_trends_prior_extensive.R`): `post = 1[month_id >= peak_month_id]`, where the Google Trends peak is August 2022. Coefficient of interest is `near_bin:post`.
- **Measure 2 — Log cumulative articles** (`did_articles_prior_extensive.R`): `log_cumulative_articles` = log of cumulative LexisNexis UK sewage coverage from Jan 2021 to the transaction month. Coefficient of interest is `near_bin:log_cumulative_articles`.
- **Existing lag precedent**: `did_articles_lag4_prior_extensive.R` already lags Measure 2 by a single fixed 4 months via a `lagged_month_id = month_id - lag` join. It is *not* systematic (one hard-coded lag), *not* applied to Measure 1, and uses a reduced spec set (3 sales + 3 rental columns, no MSOA FE, no robustness). This plan generalizes that idea to a configurable lag sweep across both measures.

**Calendar reference** (base_year = 2021, so `month_id = (year - 2021)*12 + month`):
- `month_id = 1` → Jan 2021; `month_id = 36` → Dec 2023 (analysis window is fixed at 1–36).
- Google Trends peak Aug 2022 → `peak_month_id = 20`.

---

## Requirements (traceability to Issue #21)

- **R1** — Re-estimate extensive-margin **sales** regressions with public-attention measures lagged relative to the recorded transaction date.
- **R2** — Sweep a range of lags. Issue text suggests "1, 3, 6"; the implementation directive for this plan fixes the initial set at **3, 6, 12 months** (lag must be a configurable vector so 1-month or other lags can be added without code edits).
- **R3** — Apply to **both** attention measures: Measure 1 (shift the post threshold by the lag) and Measure 2 (cumulative count as of the lagged month).
- **R4** — Compare lagged sales against the **contemporaneous** sales spec and against the **rentals** result, and summarise whether mistiming explains the null.
- **R5** — Outputs are comparison tables and/or summary artifacts, mirroring existing `05_news` table/CSV conventions.

---

## Key Technical Decisions

### KTD1 — Lag is implemented per measure, but the two framings are algebraically identical

For both measures, "lag the attention by `L` months" means: *use the attention value that prevailed `L` months before the recorded transaction month.* Issue #21 phrases this as "shift the threshold by the lag" for Measure 1 and "use the count as of the lagged month" for Measure 2. These are two faces of the same operation:

- **Measure 2 (cumulative articles)** — shift the **measure backward**: join `log_cumulative_articles` keyed on `month_id - L` instead of `month_id`. This is exactly what `did_articles_lag4_prior_extensive.R` does (`lagged_month_id = month_id - CONFIG$lag_months`, then `inner_join(articles, by = c("lagged_month_id" = "month_id"))`).
- **Measure 1 (post indicator)** — shift the **threshold forward**: `post_L = 1[month_id >= peak_month_id + L]`. This is identical to `1[month_id - L >= peak_month_id]` (shifting the measure backward), but the forward-threshold form is preferred because **it drops no observations** (every transaction retains a defined `post` status).

**Why the distinction matters (a real asymmetry, must be surfaced in the plan and the table notes):**
- The Measure 2 join *mechanically drops* the first `L` transaction months, because `lagged_month_id = month_id - L < 1` has no matching article row. Lag 12 drops all of 2021 (12 months).
- The Measure 1 threshold shift drops *nothing* but shrinks the post-period: lag 12 moves the cut to `month_id = 32` (Aug 2023), leaving only 5 post-period months and a pre-period that still spans months 1–31.

### KTD2 — Hold the estimation sample fixed across lags for an honest comparison (configurable)

If each lag uses its own maximal sample, the lag-0 column and the lag-12 column are estimated on *different samples* (lag 12 silently drops 2021 for Measure 2), so coefficient movement conflates *timing* with *sample composition*. To isolate the timing channel:

- **Default (`hold_sample_fixed = TRUE`)**: every lagged column within a measure's comparison table is estimated on the **common sample** `month_id >= analysis_start_month_id + max_lag` (= `month_id >= 13` for max lag 12). The lag-0 column in this table is therefore the *contemporaneous spec on the restricted sample*, directly comparable to the lagged columns.
- **Reference column**: additionally report the **full-sample contemporaneous** estimate (`month_id >= 1`, no restriction) — this reproduces the published null and shows what the restriction itself costs.
- **Trade-off to document**: for Measure 1, restricting to `month_id >= 13` removes months 1–12 of the clean pre-peak period (peak is month 20), weakening the pre/post contrast. This is the price of cross-lag comparability; the full-sample reference column lets the reader see the published estimate alongside.

`hold_sample_fixed` is a CONFIG flag so the secondary "each-lag-maximal-sample" variant can be produced by flipping it.

### KTD3 — One preferred specification per measure, lag as the comparison dimension

The published `*_prior_extensive.R` scripts report 12 columns (pooled / MSOA+month / LSOA+month × with/without controls × sales/rentals). For a lag-comparison table that is unreadable. Fix the spec to the **fully-saturated preferred specification** and let **lag** vary across columns:

- **Preferred spec** = `near_bin` + interaction + property controls, with **LSOA + month FE**, LSOA-clustered SEs — i.e., the `model_sale_5` analog in both baseline scripts.
  - Measure 1: `log_price ~ near_bin + near_bin:post + property_type + old_new + duration | lsoa + month_id`
  - Measure 2: `log_price ~ near_bin + near_bin:log_cumulative_articles + property_type + old_new + duration | lsoa + month_id`
- **Identification note (carry into code comments)**: with `month_id` FE, the `post` / `log_cumulative_articles` *main* effect is monthly-constant and absorbed; only the `near_bin:` interaction is identified. This already holds in the baseline scripts and is unchanged by lagging (the lagged measure is still a deterministic function of `month_id`).
- **Secondary spec (optional, same table or appendix)**: the MSOA+month analog (`model_sale_3`) for robustness. Include only if it does not crowd the table.
- **Rentals**: include the contemporaneous (lag 0, full sample) rentals preferred-spec estimate as a single **benchmark column/panel** so the reader can read the sales lag sweep against the significant rental effect. Do **not** sweep lags for rentals (issue scopes lags to sales; rentals have no agreement-to-completion gap).

### KTD4 — Output artifacts mirror the windowed-salience precedent

Follow the pattern set by `did_articles_windowed_prior_extensive.R` (per-parameter `.tex` + an `effect_sizes.csv`) and the post-analysis report convention in `docs/reports/`:

- One **comparison `.tex` table per measure** under `output/tables/`, columns = {full-sample contemporaneous reference, lag 0 (common sample), lag 3, lag 6, lag 12} for sales preferred spec, plus a rentals contemporaneous benchmark column.
- One **effect-size CSV** capturing every (measure, lag, sample, spec) cell: `estimate`, `std_error`, `p_value`, `n`, built with the existing `extract_fixest_term()` helper in `windowed_article_effect_size_utils.R`.
- An **optional results report** under `docs/reports/` (`YYYY-MM-DD-NNN-lagged-attention-sales-extensive-results-report.{md,html}`) summarising whether the lag sweep moves the sales interaction toward the rental sign/magnitude — i.e., whether mistiming explains the null (R4).

### KTD5 — Radius held fixed at the headline comparison; radius robustness deferred

The lag analysis runs at the headline extensive-margin comparison **0–500m vs 1000–2000m** only. The existing cross-radius machinery (`run_radius_robustness()` / `utils_radius_robustness_table.R`) is *not* wired in here — issue #21 scopes the work to lags, not radius. A lag × radius grid is recorded under Deferred Work.

---

## Files

**Inspect (read-only, baselines to mirror — do not modify):**
- `scripts/R/09_analysis/05_news/did_trends_prior_extensive.R` — Measure 1 contemporaneous baseline (post indicator, `load_google_trends_peak`, `post` construction at line ~136).
- `scripts/R/09_analysis/05_news/did_articles_prior_extensive.R` — Measure 2 contemporaneous baseline (cumulative-article join at lines ~131–135).
- `scripts/R/09_analysis/05_news/did_articles_lag4_prior_extensive.R` — single-lag precedent to generalize (lag join + start-month filter at lines ~131–137, 194–200; sample-loss note at lines ~157–160).
- `scripts/R/09_analysis/05_news/did_articles_windowed_prior_extensive.R` — precedent for a parameter sweep + `.tex`-per-parameter + effect-size CSV.

**Reuse (shared helpers, source as-is):**
- `scripts/R/09_analysis/05_news/extensive_margin_news_utils.R` — `validate_comparison_config`, `load_google_trends_peak`, `load_articles_data`, `load_nearest_distance_lookup`, `load_sales_transactions`, `load_rental_transactions`, `build_extensive_margin_sample`, `standardise_*_estimation_data`, `print_extensive_margin_summary`, `comparison_note_text`, `patch_modelsummary_latex`.
- `scripts/R/09_analysis/05_news/windowed_article_effect_size_utils.R` — `extract_fixest_term()` for the effect-size CSV.

**Create:**
- `scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R` — Measure 1 lag sweep.
- `scripts/R/09_analysis/05_news/did_articles_lagged_sales_extensive.R` — Measure 2 lag sweep.
- New shared helpers appended to `extensive_margin_news_utils.R` (see U1).
- `output/tables/did_trends_lagged_sales_extensive.tex`, `output/tables/did_articles_lagged_sales_extensive.tex` (generated).
- `output/tables/did_news_lagged_sales_extensive_effect_sizes.csv` (generated; one or both scripts append).
- `docs/reports/2026-06-24-NNN-lagged-attention-sales-extensive-results-report.{md,html}` (optional, U4).

> **Testing note**: this repo has **no R test harness** (only Python deps carry `tests/`). "Test scenarios" below are concrete **sanity assertions** the implementer should encode as inline `stopifnot()` / `cat()` diagnostics inside each script (matching the existing `print_extensive_margin_summary` diagnostic style), plus manual checks on the produced tables. They are not a separate test framework.

---

## Implementation Units

### U1. Lag and common-sample helpers in `extensive_margin_news_utils.R`

**Goal**: centralize the lag mechanics so both new scripts share one tested implementation, generalizing the inline logic in `did_articles_lag4_prior_extensive.R`.

**Requirements**: R1, R2, R3 (mechanism), R5 (consistency with existing utils).

**Dependencies**: none.

**Files**: `scripts/R/09_analysis/05_news/extensive_margin_news_utils.R`.

**Approach** — add three small, documented helpers (roxygen-style headers matching the file's conventions):
- `shifted_post_indicator(month_id, peak_month_id, lag)` → integer `1[month_id >= peak_month_id + lag]`. Pure function; no row drops. Used by Measure 1.
- `join_lagged_cumulative_articles(sample, articles, lag, start_month_id)` → mutate `lagged_month_id = month_id - lag`, filter `lagged_month_id >= start_month_id`, `inner_join` `articles` (`cumulative_articles`, `log_cumulative_articles`) on `lagged_month_id = month_id`. `lag = 0` must reproduce the contemporaneous join (key on `month_id`). Used by Measure 2.
- `restrict_to_common_sample(sample, start_month_id, max_lag)` → filter `month_id >= start_month_id + max_lag`. Applied to every column when `hold_sample_fixed = TRUE` (KTD2), including the lag-0 common-sample column, for both measures.

**Patterns to follow**: existing helper signatures and `.data$` pronoun usage in `extensive_margin_news_utils.R`.

**Test scenarios** (inline assertions):
- `shifted_post_indicator(20, 20, 0)` → 1; `shifted_post_indicator(31, 20, 12)` → 0; `shifted_post_indicator(32, 20, 12)` → 1 (threshold lands at month 32).
- `join_lagged_cumulative_articles(sample, articles, lag = 0, start = 1)` returns the same rows and `log_cumulative_articles` values as a direct `month_id` join (lag-0 identity).
- `join_lagged_cumulative_articles(..., lag = L, ...)` drops exactly the rows with `month_id < start + L` (assert min `month_id` == `start + L`).
- `restrict_to_common_sample(sample, 1, 12)` yields `min(month_id) == 13` and is a subset of the input.

**Verification**: sourcing the file errors-free; the three helpers exist with documented args.

---

### U2. Measure 1 lag-sweep script — `did_trends_lagged_sales_extensive.R`

**Goal**: produce the post-peak-indicator sales lag comparison (lag 0/3/6/12) plus full-sample contemporaneous reference and rentals benchmark.

**Requirements**: R1, R2, R3 (Measure 1), R4.

**Dependencies**: U1.

**Files**: `scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R`; generates `output/tables/did_trends_lagged_sales_extensive.tex` and appends to `output/tables/did_news_lagged_sales_extensive_effect_sizes.csv`.

**Approach**:
- Copy the `CONFIG` / package-check / `initialise_environment` scaffold from `did_trends_prior_extensive.R`. Add `CONFIG$lags = c(0L, 3L, 6L, 12L)`, `CONFIG$hold_sample_fixed = TRUE`, `CONFIG$max_lag = max(CONFIG$lags)`. Keep `comparison = 0–500m vs 1000–2000m`.
- Load sales + lookup + `peak_info` once (`load_google_trends_peak`). Build the base extensive-margin sample once (`build_extensive_margin_sample`).
- For each lag `L`: set `post = shifted_post_indicator(month_id, peak_month_id, L)`; if `hold_sample_fixed`, apply `restrict_to_common_sample(..., max_lag)`; standardise; estimate the **preferred spec** (`near_bin + near_bin:post + controls | lsoa + month_id`, `vcov = ~lsoa`).
- Estimate two reference fits: (a) **full-sample contemporaneous** sales (`L = 0`, no common-sample restriction) — reproduces the published spec; (b) **rentals contemporaneous** preferred spec (full sample) as the benchmark.
- Build one `modelsummary` `cbind` table: columns = [full-sample contemp. | lag 0 | lag 3 | lag 6 | lag 12 | rentals contemp.]. Coefficient row = `near_bin:post`. Use `patch_modelsummary_latex` and an `add_rows` block recording lag, sample (full vs common), FE, controls. Table notes must state: peak month, that the post threshold is shifted forward by the lag, the common-sample restriction, and the pre-period shrinkage caveat (KTD2).
- Emit effect-size rows via `extract_fixest_term(model, "near_bin:post")` → append `(measure = "post", lag, sample, estimate, std_error, p_value, n)` to the shared CSV.

**Patterns to follow**: `prepare_sales_analysis_data` / `estimate_models` / `export_table` structure in `did_trends_prior_extensive.R`; LSOA-clustered SEs; `print_extensive_margin_summary` diagnostics.

**Test scenarios** (inline assertions + manual):
- Lag-0 common-sample-OFF reference column reproduces `model_sale_5` from `did_trends_prior_extensive.R` (same coefficient, SE, N) — guards against accidental spec drift.
- For each lag, the post cut equals `peak_month_id + L` (assert `min(month_id[post==1]) == peak_month_id + L`, when that month is in range).
- When `hold_sample_fixed = TRUE`, every lagged column reports the **same N** (common sample) — assert equality across lag columns.
- Effect-size CSV has one row per (lag + the two references) with finite `estimate`/`std_error`/`p_value`.
- Table compiles: `.tex` written, contains the `near_bin:post` row and the correct column count.

**Verification**: script runs to "completed successfully"; `.tex` and CSV exist; lag-0 reference matches published estimate.

---

### U3. Measure 2 lag-sweep script — `did_articles_lagged_sales_extensive.R`

**Goal**: generalize `did_articles_lag4_prior_extensive.R` from a single 4-month lag to the configurable {0,3,6,12} sweep with the preferred spec, common-sample handling, full-sample reference, and rentals benchmark.

**Requirements**: R1, R2, R3 (Measure 2), R4.

**Dependencies**: U1.

**Files**: `scripts/R/09_analysis/05_news/did_articles_lagged_sales_extensive.R`; generates `output/tables/did_articles_lagged_sales_extensive.tex` and appends to `output/tables/did_news_lagged_sales_extensive_effect_sizes.csv`.

**Approach**:
- Scaffold from `did_articles_lag4_prior_extensive.R` + `did_articles_prior_extensive.R`. `CONFIG$lags = c(0L,3L,6L,12L)`, `hold_sample_fixed = TRUE`, `max_lag = 12`. Load articles via `load_articles_data` (cumulative + log cumulative), sales/lookup, build base extensive-margin sample once.
- For each lag `L`: `join_lagged_cumulative_articles(base_sample, articles, L, start_month_id)` (lag 0 = contemporaneous join). If `hold_sample_fixed`, apply `restrict_to_common_sample(..., max_lag)` **after** the lag join so all columns share `month_id >= 13`. Standardise; estimate preferred spec (`near_bin + near_bin:log_cumulative_articles + controls | lsoa + month_id`).
- References: full-sample contemporaneous sales (lag 0, no restriction → reproduces published null); rentals contemporaneous benchmark.
- Same `cbind` table + effect-size CSV append (`extract_fixest_term(model, "near_bin:log_cumulative_articles")`, `measure = "articles"`). Table notes must state that the cumulative count is taken as of the lagged month and that lag `L` mechanically drops the first `L` transaction months (KTD1 asymmetry), plus the common-sample restriction.

**Patterns to follow**: lag join in `did_articles_lag4_prior_extensive.R` lines ~131–137; spec + export in `did_articles_prior_extensive.R`.

**Test scenarios** (inline assertions + manual):
- Lag-0 common-sample-OFF reference reproduces `model_sale_5` from `did_articles_prior_extensive.R` (coefficient, SE, N).
- Lag-`L` (common-sample OFF) drops exactly the first `L` months: assert `min(month_id) == start_month_id + L`.
- With `hold_sample_fixed = TRUE`, all lag columns share identical N.
- `log_cumulative_articles` is finite and monotone non-decreasing in `lagged_month_id` (cumulative count) — spot-check.
- `.tex` + CSV produced with the `near_bin:log_cumulative_articles` row.

**Verification**: runs clean; lag-0 reference matches published null; sample-loss diagnostic prints months dropped per lag.

---

### U4. Cross-measure comparison artifact + results narrative (R4)

**Goal**: turn the two tables / effect-size CSV into a single answer to "does mistiming explain the null?"

**Requirements**: R4, R5.

**Dependencies**: U2, U3.

**Files**: `output/tables/did_news_lagged_sales_extensive_effect_sizes.csv` (consumed); `docs/reports/2026-06-24-NNN-lagged-attention-sales-extensive-results-report.{md,html}` (optional, follows the `docs/reports/` post-analysis pattern, e.g. `2026-06-23-001-windowed-article-salience-results-report.html`).

**Approach**:
- Read the combined effect-size CSV; tabulate sales `estimate (p)` by (measure × lag) with the full-sample contemporaneous and rentals benchmark rows alongside.
- Narrative judgement: does the sales interaction move toward the rental sign/magnitude (negative ≈0.8 pp) as the lag increases, and does it cross significance at any lag? State plainly whether mistiming plausibly explains the null, or whether the sales effect remains null even under the agreement-to-completion shift.
- Keep this lightweight — a CSV summary table plus a short report. The per-measure `.tex` tables remain the primary artifacts.

**Test scenarios**: CSV parses; every (measure, lag) cell present; report renders without broken references.

**Verification**: report exists and references real, freshly produced numbers; no placeholder values.

---

## Risks & Design Choices

- **Sample loss at the start of 2021 (Measure 2)** — the cumulative-article lag join drops the first `L` months; lag 12 removes all of 2021. Mitigated by KTD2's common fixed sample plus a full-sample contemporaneous reference column. Each script must print the count of months/observations dropped per lag (mirroring `did_articles_lag4_prior_extensive.R`'s "Excluded first N months" note).
- **Shift direction (KTD1)** — shifting the measure backward vs. the threshold forward are algebraically identical; the plan deliberately uses *threshold-forward* for the post indicator (no row drops) and *measure-backward* for cumulative articles (matches the existing lag4 precedent and the "count as of the lagged month" framing). Mixing these inconsistently is the main correctness trap — the U1 helpers and lag-0 identity tests guard against it.
- **Measure 1 pre-period shrinkage under the common sample** — restricting to `month_id >= 13` removes a year of clean pre-peak data (peak at month 20). The full-sample contemporaneous reference column preserves the published estimate so the restriction's cost is visible. If Measure 1 power collapses at lag 12, report it rather than hiding it.
- **Power at long lags (Measure 1)** — lag 12 leaves only 5 post-peak months (32–36); a null there may be low power, not absence of effect. Note in the report.
- **Spec drift from the published baselines** — the lag-0, full-sample reference columns must reproduce `model_sale_5` of the contemporaneous scripts exactly; treat any mismatch as a bug in the new script, not a finding.
- **Lag set** — issue text suggests "1, 3, 6"; this plan uses **3, 6, 12** per the implementation directive, with `CONFIG$lags` configurable so a 1-month lag can be added trivially.

---

## Deferred to Follow-Up Work

- **Lag × radius grid** — re-running the sweep across 250/500/1000m exposure radii via the existing `run_radius_robustness()` / `utils_radius_robustness_table.R` machinery.
- **Intensive-margin lagging** — issue #21 is extensive-margin only; the same timing logic could later apply to intensive-margin / windowed measures.
- **Rentals placebo lag** — lagging rentals (which should *not* improve, since listing prices are contemporaneous) as a falsification check.
- **Continuous agreement-to-completion model** — distributed-lag or a calibrated single lag from registration-gap data, rather than a discrete sweep.

---

## Verification & Commands

Run from repo root with project R 4.6.0 (rv-managed environment). Scripts self-execute via `if (sys.nframe() == 0) main()`.

```bash
# Measure 1 (post-peak indicator) lag sweep
Rscript scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R

# Measure 2 (log cumulative articles) lag sweep
Rscript scripts/R/09_analysis/05_news/did_articles_lagged_sales_extensive.R

# If rv must wrap execution:
# rv run Rscript scripts/R/09_analysis/05_news/did_trends_lagged_sales_extensive.R
```

Post-run checks:
- `output/tables/did_trends_lagged_sales_extensive.tex` and `output/tables/did_articles_lagged_sales_extensive.tex` exist and contain the expected interaction row and column count.
- `output/tables/did_news_lagged_sales_extensive_effect_sizes.csv` has rows for every (measure × {full-sample contemp, lag 0, 3, 6, 12}) plus rentals benchmark, all finite.
- Lag-0 full-sample reference columns match the published `model_sale_5` estimates from the contemporaneous scripts (run those baselines and compare if needed).
- Console diagnostics print per-lag sample sizes and months dropped.
- (If U4 done) the results report renders and cites the freshly produced numbers.
