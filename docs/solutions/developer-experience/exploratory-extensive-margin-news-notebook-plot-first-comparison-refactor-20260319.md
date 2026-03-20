---
module: Extensive-Margin News Analysis
date: 2026-03-19
problem_type: developer_experience
component: tooling
symptoms:
  - "The notebook rendered, but the main results still read like a pile of large coefficient tables, making the exploratory findings hard to interpret."
  - "Sales and rentals were not visually integrated in the main coefficient views, so cross-market comparisons required scanning separate outputs."
  - "The comparison grid lacked intermediate control bins between the tight local contrasts and the broad 10km benchmark."
  - "The preferred-spec overview only showed LSOA + month FE + controls, leaving concern that the fixed-effect geography might be too fine."
root_cause: missing_tooling
resolution_type: workflow_improvement
severity: medium
tags: [r, quarto, exploratory-analysis, extensive-margin, housing, sewage-news, coefficient-plots, notebook-readability, fixed-effects, comparison-design]
---

# Troubleshooting: Refactoring the extensive-margin news notebook into a plot-first comparison note

## Problem

The problem was no longer notebook stability. [explore_extensive_margin_news.qmd](../../scripts/R/testing/explore_extensive_margin_news.qmd) already rendered, but the main results were still hard to read because the notebook presented most of the useful signal in large coefficient tables with limited narrative guidance.

The goal was to make the exploratory exercise faster to interpret without changing the underlying estimation grid or losing the CSV audit trail. That meant three concrete improvements:

1. move the main readout from table-first to plot-first;
2. put `Sales` and `Rentals` on the same coefficient plots;
3. extend the comparison grid with useful mid-range controls and show both `LSOA + month FE + controls` and `MSOA + month FE + controls` in the preferred-spec overview.

## Environment

- Module: Extensive-Margin News Analysis
- Affected component: exploratory notebook tooling and presentation
- Key source file:
  - `scripts/R/testing/explore_extensive_margin_news.qmd`
- Key rendered/output files:
  - `scripts/R/testing/explore_extensive_margin_news.html`
  - `output/tables/exploratory_extensive_margin_news/main_results.csv`
  - `output/tables/exploratory_extensive_margin_news/sample_summaries.csv`
  - `output/tables/exploratory_extensive_margin_news/preferred_result_ranking.csv`
  - `output/tables/exploratory_extensive_margin_news/ring_deep_dive_results.csv`
  - `output/tables/exploratory_extensive_margin_news/flat_split_results.csv`
  - `output/tables/exploratory_extensive_margin_news/intensity_overlay_results.csv`
  - `output/tables/exploratory_extensive_margin_news/intensity_descriptives.csv`
- Related prior note:
  - [exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md](./exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md)
- Main verification command:

```bash
quarto render scripts/R/testing/explore_extensive_margin_news.qmd --to html
```

- Date solved: 2026-03-19

## Symptoms

- The main results section read like a pile of wide coefficient tables, so the reader had to scan rows manually to recover sign, magnitude, and specification sensitivity.
- `Sales` and `Rentals` were not visually integrated in the main readout, which made cross-market comparison slower than it needed to be.
- The comparison grid jumped from tight local controls to the broad `0-500m vs 500-10km` benchmark without intermediate `1000-2000m` or `500-2000m` bins.
- The preferred-spec overview relied only on `LSOA + month FE + controls`, leaving an obvious concern that the finest geography might be too restrictive.
- The new mid-range bins should not automatically flow into deep-dive overlays, because the nearest-site intensity path still depends on `radius == 1000` cross-sections.

## What Didn't Work

**Attempted approach 1:** Keep the main notebook logic but add more prose around the existing tables.  
- **Why it failed:** the core problem was visual. The useful comparison was across markets, specs, and bins, and wide tables remained cumbersome even with better text.

**Attempted approach 2:** Keep separate market-specific readouts.  
- **Why it failed:** the reader still had to mentally align `Sales` and `Rentals` across two separate outputs instead of seeing the contrast directly.

**Attempted approach 3:** Add the new mid-range bins everywhere.  
- **Why it failed:** the deep-dive overlay machinery is still backed by the existing `radius == 1000` input path, so pushing `1000-2000m` and `500-2000m` bins through those checks would have overstated data support.

**Attempted approach 4:** Treat `LSOA + month FE + controls` as the only preferred-spec view.  
- **Why it failed:** that left a live interpretation concern that the finest fixed-effect geography might be too small, especially in a notebook meant to guide exploratory judgment.

## Solution

The working solution changed the notebook interface rather than the underlying estimand.

### Plot-first notebook structure

- Replaced the large main coefficient tables with:
  - a preferred-spec overview plot;
  - one combined coefficient figure per comparison.
- Added a reusable helper so the notebook uses the same plotting grammar throughout:
  - x-axis: coefficient estimate;
  - y-axis: comparison labels or spec labels;
  - colour: `Sales` versus `Rentals`;
  - horizontal 95% confidence intervals;
  - facets: `baseline`, `post`, `articles`.

Representative code:

```r
make_coefficient_plot <- function(data, y_var, title = NULL, subtitle = NULL, legend_position = "bottom") {
  y_sym <- rlang::sym(y_var)
  dodge <- ggplot2::position_dodge(width = 0.45)

  ggplot2::ggplot(
    data,
    ggplot2::aes(
      x = .data$estimate,
      y = !!y_sym,
      xmin = .data$estimate - 1.96 * .data$std_error,
      xmax = .data$estimate + 1.96 * .data$std_error,
      colour = .data$market,
      group = .data$market
    )
  ) +
    ggplot2::geom_errorbarh(height = 0.16, alpha = 0.8, position = dodge) +
    ggplot2::geom_point(size = 2.3, position = dodge) +
    ggplot2::facet_wrap(~ family, scales = "free_x") +
    ggplot2::scale_colour_manual(values = MARKET_COLOURS, drop = FALSE)
}
```

### Expanded comparison grid

- Kept the original comparisons.
- Added two new mid-range bins:
  - `0-500m vs 1000-2000m`
  - `0-500m vs 500-2000m`
- Kept the broad `0-500m vs 500-10km` benchmark.

Representative code:

```r
comparison_specs <- tibble::tribble(
  ~comparison_id, ~comparison_label,        ~near_min, ~near_max, ~far_min, ~far_max,
  "500_vs_500_1000",  "0-500m vs 500-1000m",   0, 500,  500, 1000,
  "500_vs_1000_2000", "0-500m vs 1000-2000m",  0, 500, 1000, 2000,
  "500_vs_500_2000",  "0-500m vs 500-2000m",   0, 500,  500, 2000,
  "500_vs_500_10km",  "0-500m vs 500-10km",    0, 500,  500, 10000
)
```

### Main-grid-only support boundary for new bins

- Added an explicit constant for comparisons that belong in the main coefficient grid but should not enter the deep-dive overlay path yet.
- Filtered those ids out of `pick_deep_dive_targets()`.

Representative code:

```r
MAIN_GRID_ONLY_COMPARISON_IDS <- c("500_vs_1000_2000", "500_vs_500_2000")

pick_deep_dive_targets <- function(term_table) {
  preferred <- term_table |>
    dplyr::filter(
      .data$spec_id == PREFERRED_SPEC_ID,
      !.data$comparison_id %in% MAIN_GRID_ONLY_COMPARISON_IDS
    )
  ...
}
```

### MSOA companion view in the preferred-spec overview

- Kept the `LSOA + month FE + controls` preferred-spec overview.
- Added a second overview figure for `MSOA + month FE + controls`.
- Updated the surrounding prose so the reader is explicitly invited to compare the fine and coarse spatial FE versions rather than treating `LSOA` as unquestioned.

Representative code:

```r
msoa_overview_results <- main_results |>
  dplyr::filter(.data$spec_id == "msoa_month_controls", .data$status == "ok")

msoa_plot_data <- msoa_overview_results |>
  dplyr::mutate(
    comparison_label = factor(.data$comparison_label, levels = rev(comparison_specs$comparison_label)),
    family = pretty_family_short(as.character(.data$family)),
    market = factor(.data$market, levels = c("Sales", "Rentals"))
  )
```

## Why This Works

1. The notebook now shows the estimand in the format the reader actually needs. Coefficient plots make sign, magnitude, uncertainty, and control-group sensitivity visible at a glance.
2. Putting `Sales` and `Rentals` on the same figure turns a textual comparison into a visual one.
3. The new `1000-2000m` and `500-2000m` bins fill the gap between the tight local controls and the broad 10km benchmark, so control-group sensitivity is easier to diagnose.
4. The explicit `MAIN_GRID_ONLY_COMPARISON_IDS` boundary keeps the notebook honest about which comparisons are supported by the existing deep-dive overlay inputs.
5. The `MSOA` companion overview makes it easy to judge whether the `LSOA` results are robust or just a by-product of very fine geography.
6. The CSV audit trail remains intact even though the notebook became plot-first.

## Prevention

- Use combined coefficient plots when the main task is comparing the same estimand across markets, specs, or bins.
- Keep tables for things plots do not show well: sample sizes, near/far composition, deep-dive selection metadata, model status failures, and descriptive intensity summaries.
- Keep the exact audit trail in CSV outputs even when the notebook becomes plot-first.
- Prefer one comparison per figure when there are several bins; otherwise the main result section becomes dense again.
- Add new bins to the main grid only when the underlying transaction sample and nearest-distance lookup support them cleanly.
- Do not automatically push new bins into deep dives; deep-dive eligibility is a separate support question from main-grid eligibility.
- If a follow-up input is capped by construction, keep that boundary explicit in code. In this notebook, nearest-site intensity overlays remain limited by the `radius == 1000` source.
- When readers may worry that `LSOA` fixed effects are too fine, show both `LSOA` and `MSOA` overview plots directly rather than burying that robustness check in prose.

## Related Issues

The most important related note is the earlier technical background for the same notebook:

- See also: [exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md](./exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md)

Adjacent notes already linked from that earlier notebook note remain relevant for source-boundary context:

- See also: [postcodes-io-reference-audit-after-local-ons-migration-20260310.md](../best-practices/postcodes-io-reference-audit-after-local-ons-migration-20260310.md)
- See also: [england-only-edm-api-contract-alignment-20260310.md](../best-practices/england-only-edm-api-contract-alignment-20260310.md)

Useful repository references:

- `scripts/R/testing/explore_extensive_margin_news.qmd`
- `scripts/R/testing/explore_extensive_margin_news.html`
- `output/tables/exploratory_extensive_margin_news/main_results.csv`
- `output/tables/exploratory_extensive_margin_news/sample_summaries.csv`
- `output/tables/exploratory_extensive_margin_news/ring_deep_dive_results.csv`
- `output/tables/exploratory_extensive_margin_news/flat_split_results.csv`
- `output/tables/exploratory_extensive_margin_news/intensity_overlay_results.csv`
- `scripts/R/09_analysis/05_news/did_trends_prior.R`
- `scripts/R/09_analysis/05_news/did_articles_prior.R`

## Verification Note

The notebook rendered successfully after the refactor and again after the `MSOA` overview addition.

Verification evidence:

- `quarto render scripts/R/testing/explore_extensive_margin_news.qmd --to html` completed successfully.
- `main_results.csv` has `144` rows, matching `2 markets × 6 comparisons × 3 families × 4 specs`.
- The new comparison ids are present in the main result grid:
  - `250_vs_250_500`
  - `250_vs_250_1000`
  - `500_vs_500_1000`
  - `500_vs_1000_2000`
  - `500_vs_500_2000`
  - `500_vs_500_10km`
- The `Preferred-Spec Overview` section in the rendered HTML now contains `2` figures: one `LSOA` view and one `MSOA` view.
- `intensity_overlay_results.csv` still only contains the original overlay-supported comparisons:
  - `250_vs_250_500`
  - `250_vs_250_1000`
  - `500_vs_500_1000`

This confirms that the notebook now delivers a plot-first interface, a richer comparison grid, and an explicit `LSOA` versus `MSOA` robustness view without breaking the existing deep-dive support boundary.
