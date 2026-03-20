---
module: Extensive-Margin News Analysis
date: 2026-03-19
problem_type: developer_experience
component: tooling
symptoms:
  - "Rendering `scripts/R/testing/explore_extensive_margin_news.qmd` failed because `fixest::feols()` in the local package environment did not accept the assumed `formula =` interface."
  - "Model post-processing failed when the notebook called `fixest::nobs()` and treated `fixest::fitstat(model, \"ar2\")` as a plain numeric scalar."
  - "The first notebook design exhausted available memory because it retained oversized transaction columns, full comparison samples, and model-heavy deep-dive objects across a large model grid."
  - "Early output tables were unreliable because `sample_summary()` did not force a clean two-row near/far layout and deep-dive helpers mixed pretty market labels (`Sales`, `Rentals`) with lowercase internal keys."
root_cause: wrong_api
resolution_type: tooling_addition
severity: medium
tags: [r, quarto, fixest, exploratory-analysis, extensive-margin, housing, sewage-news, memory]
---

# Troubleshooting: Stabilising the exploratory extensive-margin news notebook render

## Problem
The goal was to add a broad exploratory Quarto notebook for extensive-margin sewage-news analysis at `scripts/R/testing/explore_extensive_margin_news.qmd`. The notebook needed to build nearest-overflow distance samples from raw transactions plus the 10km lookup universe, estimate a large grid of `fixest` models for sales and rentals, run structured follow-ups, and render cleanly to HTML with tidy CSV outputs.

This was not just a new notebook-writing task. The broad `0-500m` vs `500-10km` comparison could not be built from the existing `prior_to_*` cross-sections because those stop at `1000m`, so the notebook also needed a new raw-data assembly path. The first pass did not survive the local package/runtime environment: several `fixest` API assumptions were wrong for the installed version, helper outputs were unstable, and the initial design kept too much data in memory for a heavy multi-market, multi-spec render.

## Environment
- Module: Extensive-margin news analysis / exploratory notebook workflow
- Affected component: Repository tooling
- Key files:
  - `scripts/R/testing/explore_extensive_margin_news.qmd`
  - `scripts/R/testing/explore_extensive_margin_news.html`
  - `output/tables/exploratory_extensive_margin_news/main_results.csv`
  - `output/tables/exploratory_extensive_margin_news/preferred_result_ranking.csv`
  - `output/tables/exploratory_extensive_margin_news/flat_split_results.csv`
  - `output/tables/exploratory_extensive_margin_news/intensity_overlay_results.csv`
- Key inputs:
  - `data/processed/house_price.parquet`
  - `data/processed/zoopla/zoopla_rentals.parquet`
  - `data/processed/spill_house_lookup.parquet`
  - `data/processed/zoopla/spill_rental_lookup.parquet`
  - `data/processed/lexis_nexis/search1_monthly.parquet`
- Main verification command:

```bash
quarto render scripts/R/testing/explore_extensive_margin_news.qmd --to html
```

- Date solved: 2026-03-19

## Symptoms
- `quarto render` failed at model-estimation steps because `fixest::feols()` rejected the assumed `formula = ...` argument style in this environment.
- Coefficient-extraction helpers failed because `fixest::nobs()` is not exported and `fixest::fitstat(model, "ar2")` returned a `fixest_fitstat` object rather than a scalar.
- A broader first version of the notebook hit the local memory ceiling while carrying large transaction frames, cached comparison samples, and deep-dive model objects through the render.
- Intermediate descriptive output was malformed because the sample-summary helper did not explicitly build one near row and one far row.
- Deep-dive logic was brittle because some helpers received `"Sales"` / `"Rentals"` labels while other branches expected lowercase `"sales"` / `"rentals"` keys.
- The broad-control comparison was easy to specify conceptually but required a different data path from the existing `prior_to_*` notebooks, which capped the lookup universe at `1000m`.

## What Didn't Work
**Attempted approach 1:** Treat the notebook like other recent `fixest` code and call `fixest::feols(formula = ..., data = ...)`.  
- **Why it failed:** the installed `fixest` interface in this environment required `fml = ...`, so model estimation stopped before the notebook could produce any results.

**Attempted approach 2:** Pull observation counts and adjusted R-squared with `fixest::nobs()` and a direct `fitstat()` assignment.  
- **Why it failed:** `nobs()` is provided by `stats`, not exported from `fixest`, and `fitstat()` returned a classed object that needed explicit numeric extraction.

**Attempted approach 3:** Keep full transaction data, full comparison samples, and deep-dive model objects in memory for convenience.  
- **Why it failed:** the notebook estimates a wide model grid across two markets and several comparison samples; retaining bulky objects pushed the render into avoidable memory pressure.

**Attempted approach 4:** Reuse generic summary and helper logic without normalising market keys or forcing a strict near/far table shape.  
- **Why it failed:** the summary output became unreliable, and some deep-dive branches misfired because pretty labels and internal keys were mixed.

**Attempted approach 5:** Build the broad comparison from the existing `prior_to_sale` / `prior_to_rental` style inputs.  
- **Why it failed:** those exploratory cross-sections are capped at `1000m`, so they cannot support the intended `0-500m` vs `500-10km` design cleanly.

## Solution
The working implementation did two things at once: it added the exploratory notebook itself and then tightened the notebook so it matched the local `fixest` API and the machine's memory limits.

### Core notebook design
- Built the main extensive-margin samples from raw sales/rental transactions plus the 10km nearest-overflow lookup files rather than from the pre-capped `prior_to_*` cross-sections.
- Defined extensive-margin treatment as a proximity indicator, `near_bin`, for each distance-band comparison.
- Ran the main model families for sales and rentals:
  - baseline: `log_price ~ near_bin`
  - post: `near_bin:post`
  - articles: `near_bin:log_cumulative_articles`
- Added structured follow-ups:
  - finer ring dose-response
  - flat vs non-flat split
  - nearest-site count/hours overlays where the data supported them

### Render/debugging fixes
- Switched all `fixest` estimation calls to `fixest::feols(fml = ..., data = ..., vcov = ...)`.
- Replaced `fixest::nobs()` with `stats::nobs()`.
- Coerced the adjusted R-squared extraction to a scalar with:

```r
adj_r2 = suppressWarnings(tryCatch(
  as.numeric(fixest::fitstat(model, "ar2"))[[1]],
  error = function(e) NA_real_
))
```

- Reduced memory use by:
  - selecting only needed transaction columns on import
  - dropping cached full-sample/model-object storage from the main notebook flow
  - rebuilding deep-dive samples on demand instead of retaining everything
- Rewrote `sample_summary()` to construct exactly two rows per comparison: one near row and one far row.
- Normalised market routing with lowercase keys inside helper functions such as `controls_for_market()` and `run_intensity_overlay()`.
- Kept output writing simple and explicit, with the main notebook exporting:
  - `main_results.csv`
  - `preferred_result_ranking.csv`
  - `sample_summaries.csv`
  - `ring_deep_dive_results.csv`
  - `flat_split_results.csv`
  - `intensity_overlay_results.csv`
  - `intensity_descriptives.csv`

**Representative code changes**:

```r
# Model estimation: use the interface available in this environment
model <- tryCatch(
  fixest::feols(
    fml = build_formula(family, spec_row, market),
    data = data_est,
    vcov = resolve_vcov(data_est)
  ),
  error = function(e) NULL
)
```

```r
# Robust model metadata extraction
dplyr::mutate(
  nobs = stats::nobs(model),
  adj_r2 = suppressWarnings(tryCatch(
    as.numeric(fixest::fitstat(model, "ar2"))[[1]],
    error = function(e) NA_real_
  ))
)
```

```r
# Sample summaries: force a clean near/far output shape
dplyr::bind_rows(
  summarise_group(dplyr::filter(sample, .data$near_bin == 1L), near_label),
  summarise_group(dplyr::filter(sample, .data$near_bin == 0L), far_label)
)
```

## Why This Works
The notebook was failing for two different reasons, and the final version addresses both.

1. The render now matches the actual `fixest` API available in the local environment, so estimation and post-processing complete instead of dying on interface mismatches.
2. Model metadata extraction no longer assumes that every helper returns a plain numeric vector; `stats::nobs()` and explicit scalar coercion make the tidy results stable.
3. The notebook now treats memory as a first-class constraint. Pulling only required columns and recomputing deep-dive slices on demand avoids carrying millions of unneeded values through the full render.
4. The descriptive and deep-dive helpers now use consistent internal keys and output shapes, so downstream tables and branching logic are reproducible.
5. Because the notebook is built from raw transactions plus the 10km distance lookup, it can support the broad `0-500m` vs `500-10km` comparison that the existing `prior_to_*` data cannot represent cleanly.

## Prevention
- When adding a new `fixest`-heavy notebook in this repo, smoke-test the smallest render path early and confirm the local interface rather than assuming argument names from another environment.
- Prefer `stats::nobs()` and explicit coercion for any classed statistic objects used inside `tibble` pipelines.
- Treat Quarto notebooks with large model grids as memory-constrained workflows:
  - select only needed columns at import
  - avoid caching full model objects unless the notebook truly needs them later
  - rebuild slices on demand for follow-up sections
- Keep “pretty” labels out of control-flow helpers. Use lowercase machine keys internally and add human-readable labels only at presentation boundaries.
- For exploratory comparison notebooks, force deterministic sample-summary shapes and skip empty cells explicitly rather than relying on generic summaries.
- Keep the estimand explicit in both prose and code: in this notebook the extensive-margin treatment is `near_bin` based on nearest-overflow distance, while spill counts/hours belong in mechanism overlays rather than the main treatment definition.
- Use a single nearest-distance source for all ring comparisons. Do not mix nearest-distance annuli with “within radius” samples inside the same table.
- If a comparison extends beyond `1000m`, build it from raw transactions plus the full lookup, not from capped exploratory cross-sections.

**Recommended smoke checks**:

```bash
quarto render scripts/R/testing/explore_extensive_margin_news.qmd --to html
```

```bash
Rscript --vanilla -e "x <- read.csv('output/tables/exploratory_extensive_margin_news/main_results.csv'); cat('rows=', nrow(x), '\n', sep=''); stopifnot(nrow(x) == 96L)"
```

```bash
Rscript --vanilla -e "x <- read.csv('output/tables/exploratory_extensive_margin_news/sample_summaries.csv'); bad <- x |> dplyr::count(market, comparison_id); print(bad); stopifnot(all(bad$n == 2L))"
```

```bash
Rscript --vanilla -e "needed <- c('main_results.csv','preferred_result_ranking.csv','sample_summaries.csv','ring_deep_dive_results.csv','flat_split_results.csv','intensity_overlay_results.csv','intensity_descriptives.csv'); ok <- file.exists(file.path('output/tables/exploratory_extensive_margin_news', needed)); print(ok); stopifnot(all(ok))"
```

- If a future notebook needs the broad-control comparisons, build from raw transactions plus the full-distance lookup rather than inheriting capped cross-sections.

## Related Issues
No close analogue is documented yet in `docs/solutions/`, but these adjacent notes are useful for workflow patterns around exploratory assets and data-source boundaries:
- See also: [postcodes-io-reference-audit-after-local-ons-migration-20260310.md](../best-practices/postcodes-io-reference-audit-after-local-ons-migration-20260310.md)
- See also: [england-only-edm-api-contract-alignment-20260310.md](../best-practices/england-only-edm-api-contract-alignment-20260310.md)

Relevant repository references:
- `scripts/R/testing/explore_extensive_margin_news.qmd`
- `scripts/R/09_analysis/05_news/did_articles_prior.R`
- `scripts/R/09_analysis/05_news/did_trends_prior.R`
- `output/tables/exploratory_extensive_margin_news/preferred_result_ranking.csv`
- `output/tables/exploratory_extensive_margin_news/flat_split_results.csv`
- `output/tables/exploratory_extensive_margin_news/intensity_overlay_results.csv`

## Verification Note
The notebook rendered successfully to HTML and wrote the expected exploratory outputs under `output/tables/exploratory_extensive_margin_news/`.

Verification evidence:
- `scripts/R/testing/explore_extensive_margin_news.html` was generated successfully.
- `main_results.csv` has `96` rows, which matches `2 markets × 4 comparisons × 3 families × 4 specs`.
- `sample_summaries.csv` returns exactly two rows for each market-comparison pair.
- `preferred_result_ranking.csv` contains the expected preferred-spec ranking table.
- `flat_split_results.csv` and `intensity_overlay_results.csv` confirm that the structured deep dives executed.

The first render also surfaced substantive patterns worth preserving alongside the technical fix:
- Rentals show the strongest extensive-margin salience results. In the preferred `LSOA + month FE + controls` spec, `0-500m` vs `500-10km` gives `near_bin:post = -0.0084` and `near_bin:log_cumulative_articles = -0.0028`.
- Sales baseline effects are control-group sensitive: positive in local rings but negative against the broad `500-10km` comparison (`near_bin = -0.0126`).
- The flat split is unusually strong in sales. For `0-500m` vs `500-10km`, the baseline preferred estimate is about `-0.0208` for non-flats and `+0.0274` for flats.
- The rental `0-500m` vs `500-1000m` news effects persist after nearest-site intensity overlays, with `near_bin:log_cumulative_articles ≈ -0.00143` and `near_bin:post ≈ -0.00282`.

This makes the note useful for both future notebook debugging and future follow-up on the empirical patterns the exploratory workflow uncovered.
