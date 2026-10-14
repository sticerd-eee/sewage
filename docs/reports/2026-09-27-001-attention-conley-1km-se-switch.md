# Public-attention family: switch to Conley 1 km standard errors — 27 September 2026

All 21 public-attention regression scripts in `scripts/R/09_analysis/05_news/` now report Conley spatial standard errors with a 1 km cutoff, set through the shared helper `spatial_vcov()` in `scripts/R/09_analysis/utils_table_formatting.R`, in place of standard errors clustered by LSOA. Every table note now carries the helper's sentence (`spatial_se_note()`), and the two lagged-attention plot captions written by `summarise_lagged_attention_sales.R` say the intervals use Conley standard errors with a 1 km cutoff. Point estimates and observation counts are unchanged in every table and every effect-size file: every estimate in the old and new files matches once the comparison uses a baseline built from the current data (see "Baseline" below).

The reason is that the attention regressions are estimated on the same transaction observations as the hedonic cross-sections, so the spatial dependence between houses that share overflows and local shocks is the same, and the estimator should not change because an interaction with a time-varying attention measure is added. Month fixed effects absorb shocks common to all properties. This reverses the earlier decision that the difference-in-differences tables keep LSOA clustering; LSOA clustering is now a comparison estimator only (see "Property-Level Inference" in `CONCEPTS.md`).

fixest's Conley estimator uses a uniform kernel: every pair of observations within the 1 km cutoff gets full weight, and pairs farther apart get none. The `distance = "triangular"` argument of `fixest::conley()` sets the distance approximation, not the kernel. The earlier wording "triangular (Bartlett) kernel" in the helper comment, `CONCEPTS.md` and the hedonic report has been corrected.

In `extensive_margin_coefficient_plots.R` the previous estimator clustered on each specification's own fixed-effect unit, so the MSOA-FE figures used MSOA clustering and the LSOA-FE figures used LSOA clustering. Both now use Conley 1 km. The `_lsoa` and `_msoa` suffixes in the figure names refer to the fixed-effect unit, so the names are unchanged.

Scripts changed (22): `did_trends_prior.R`, `did_trends_prior_extensive.R`, `did_articles_prior.R`, `did_articles_prior_extensive.R`, `did_trends_full.R`, `did_trends_full_extensive.R`, `did_articles_windowed_prior.R`, `did_articles_windowed_prior_extensive.R`, `did_articles_lag4_prior.R`, `did_articles_lag4_prior_extensive.R`, `did_trends_prior_salience.R`, `did_trends_prior_extensive_salience.R`, `did_articles_prior_salience.R`, `did_articles_prior_extensive_salience.R`, `es_trends_prior.R`, `es_trends_prior_extensive.R`, `did_trends_lagged_sales.R`, `did_trends_lagged_sales_extensive.R`, `did_articles_lagged_sales.R`, `did_articles_lagged_sales_extensive.R`, `extensive_margin_coefficient_plots.R` (estimator), and `summarise_lagged_attention_sales.R` (plot captions only; it reads the effect-size files and estimates nothing). Seven of them did not source `utils_table_formatting.R` directly and now do; three of those also list `modelsummary` among their required packages, because the helper file calls `modelsummary::fmt_decimal()` when it is sourced.

## Baseline

The old side comes from the pre-change snapshot of `output/tables/`, with one exception. The snapshot copies of six tables were stale: they were built on an older sample, so their point estimates and observation counts do not match the current data. For those six tables, the old side is a fresh run of the unchanged pre-change version of each script (LSOA-clustered standard errors) on current data, with its output paths redirected to the scratchpad. That run matches the new tables exactly in every estimate and observation count, so only the standard errors differ. The stale files and how far they were off:

| Table | Estimates that differ in the stale snapshot | First sales N, snapshot → current | First rentals N, snapshot → current |
|---|---|---|---|
| `did_articles_lag4_prior` | 17 of 18 | 107,984 → 124,505 | 41,614 → 42,688 |
| `did_articles_lag4_prior_extensive` | 11 of 18 | 1,268,727 → 1,308,877 | 482,768 → 492,576 |
| `did_articles_prior_extensive_radius_robustness` | 3 of 12 | 1,087,054 → 1,421,929 | 404,901 → 403,281 |
| `did_trends_full_extensive` | 10 of 14 | 1,473,360 → 1,517,653 | 538,441 → 549,318 |
| `es_trends_prior` | 50 of 54 | 97,818 → 103,088 | 39,362 → 40,085 |
| `es_trends_prior_extensive` | 51 of 54 | 1,142,885 → 1,179,994 | 454,804 → 464,003 |

These differences come from the sample having changed since the snapshot files were written, not from this switch.

Two scripts write to files that another script also writes. `did_articles_windowed_prior.R` writes `did_articles_prior_{250,500,1000}m.tex` (its cumulative-measure tables, with a different label and note), and `did_articles_windowed_prior_extensive.R` writes `did_articles_prior_extensive.tex` (its `500_vs_1000_2000` cumulative table). The snapshot copies of these four files came from the windowed scripts. In this run each windowed script ran before its non-windowed counterpart, so the files now on disk come from `did_articles_prior.R` and `did_articles_prior_extensive.R` (labels `tbl:did-articles-prior-<R>m` and `tbl:did-articles-prior-extensive`). The models are the same: the old windowed-written files and the new files agree in every estimate and observation count, and a fresh pre-change run of `did_articles_prior_extensive.R` reproduces the snapshot's `did_articles_prior_extensive.tex` in every estimate, standard error and star. `run_all_analysis.sh` runs each windowed script after its counterpart, so a full pipeline run leaves the windowed versions on disk.

Tables are parsed from the `.tex` files by `parse_tables.py` and `compare_tables.py` in the session scratchpad. Values are at the tables' 3-decimal precision, so the confidence intervals for small coefficients (printed as 0.000 or 0.001) are only indicative. Stars use the new Conley p-values that fixest reports; the confidence interval is the estimate plus or minus 1.96 times the new standard error. Stars are carried by the estimate in the tables and are shown next to each standard error here for comparison. Column labels follow the table headers; in the radius-robustness, windowed-summary and lagged-grid tables the row panel is prefixed to the column.

## Headline: extensive margin, Google Trends peak (0-500 m vs 1000-2000 m)

`did_trends_prior_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.198 | 0.008 *** | 0.016 *** | [-0.229, -0.167] |  |
| Sales (2) | Near bin | -0.149 | 0.008 *** | 0.016 *** | [-0.180, -0.118] |  |
| Sales (3) | Near bin | -0.079 | 0.005 *** | 0.005 *** | [-0.089, -0.069] |  |
| Sales (4) | Near bin | -0.017 | 0.004 *** | 0.004 *** | [-0.025, -0.009] |  |
| Sales (5) | Near bin | -0.101 | 0.008 *** | 0.007 *** | [-0.115, -0.087] |  |
| Sales (6) | Near bin | -0.062 | 0.006 *** | 0.006 *** | [-0.074, -0.050] |  |
| Rentals (7) | Near bin | -0.206 | 0.012 *** | 0.033 *** | [-0.271, -0.141] |  |
| Rentals (8) | Near bin | -0.177 | 0.011 *** | 0.029 *** | [-0.234, -0.120] |  |
| Rentals (9) | Near bin | -0.029 | 0.005 *** | 0.006 *** | [-0.041, -0.017] |  |
| Rentals (10) | Near bin | 0.006 | 0.003 * | 0.004  | [-0.002, 0.014] | * → none |
| Rentals (11) | Near bin | -0.057 | 0.009 *** | 0.009 *** | [-0.075, -0.039] |  |
| Rentals (12) | Near bin | -0.012 | 0.005 ** | 0.006 ** | [-0.024, 0.000] |  |
| Sales (1) | Post | 0.043 | 0.003 *** | 0.003 *** | [0.037, 0.049] |  |
| Sales (2) | Post | 0.047 | 0.003 *** | 0.003 *** | [0.041, 0.053] |  |
| Rentals (7) | Post | 0.134 | 0.003 *** | 0.003 *** | [0.128, 0.140] |  |
| Rentals (8) | Post | 0.115 | 0.002 *** | 0.003 *** | [0.109, 0.121] |  |
| Sales (1) | Near bin x Post | 0.002 | 0.005  | 0.005  | [-0.008, 0.012] |  |
| Sales (2) | Near bin x Post | 0.004 | 0.005  | 0.005  | [-0.006, 0.014] |  |
| Sales (3) | Near bin x Post | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Sales (4) | Near bin x Post | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Sales (5) | Near bin x Post | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Sales (6) | Near bin x Post | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Rentals (7) | Near bin x Post | 0.010 | 0.005 * | 0.007  | [-0.004, 0.024] | * → none |
| Rentals (8) | Near bin x Post | 0.013 | 0.005 *** | 0.006 ** | [0.001, 0.025] | *** → ** |
| Rentals (9) | Near bin x Post | -0.011 | 0.002 *** | 0.004 *** | [-0.019, -0.003] |  |
| Rentals (10) | Near bin x Post | -0.006 | 0.002 *** | 0.003  | [-0.012, 0.000] | *** → none |
| Rentals (11) | Near bin x Post | -0.010 | 0.002 *** | 0.004 *** | [-0.018, -0.002] |  |
| Rentals (12) | Near bin x Post | -0.007 | 0.002 *** | 0.003 * | [-0.013, -0.001] | *** → * |

## Headline: extensive margin, cumulative articles (0-500 m vs 1000-2000 m)

`did_articles_prior_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.207 | 0.010 *** | 0.016 *** | [-0.238, -0.176] |  |
| Sales (2) | Near bin | -0.160 | 0.010 *** | 0.017 *** | [-0.193, -0.127] |  |
| Sales (3) | Near bin | -0.079 | 0.006 *** | 0.006 *** | [-0.091, -0.067] |  |
| Sales (4) | Near bin | -0.020 | 0.005 *** | 0.005 *** | [-0.030, -0.010] |  |
| Sales (5) | Near bin | -0.101 | 0.008 *** | 0.008 *** | [-0.117, -0.085] |  |
| Sales (6) | Near bin | -0.064 | 0.007 *** | 0.007 *** | [-0.078, -0.050] |  |
| Rentals (7) | Near bin | -0.228 | 0.016 *** | 0.039 *** | [-0.304, -0.152] |  |
| Rentals (8) | Near bin | -0.202 | 0.014 *** | 0.033 *** | [-0.267, -0.137] |  |
| Rentals (9) | Near bin | -0.021 | 0.006 *** | 0.009 ** | [-0.039, -0.003] | *** → ** |
| Rentals (10) | Near bin | 0.007 | 0.005  | 0.008  | [-0.009, 0.023] |  |
| Rentals (11) | Near bin | -0.050 | 0.010 *** | 0.011 *** | [-0.072, -0.028] |  |
| Rentals (12) | Near bin | -0.011 | 0.006 * | 0.009  | [-0.029, 0.007] | * → none |
| Sales (1) | log(Articles) | 0.007 | 0.001 *** | 0.001 *** | [0.005, 0.009] |  |
| Sales (2) | log(Articles) | 0.011 | 0.001 *** | 0.001 *** | [0.009, 0.013] |  |
| Rentals (7) | log(Articles) | 0.046 | 0.001 *** | 0.002 *** | [0.042, 0.050] |  |
| Rentals (8) | log(Articles) | 0.040 | 0.001 *** | 0.001 *** | [0.038, 0.042] |  |
| Sales (1) | Near bin x log(Articles) | 0.002 | 0.001  | 0.002  | [-0.002, 0.006] |  |
| Sales (2) | Near bin x log(Articles) | 0.003 | 0.001 * | 0.002 * | [-0.001, 0.007] |  |
| Sales (3) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (5) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Near bin x log(Articles) | 0.006 | 0.002 ** | 0.003 * | [0.000, 0.012] | ** → * |
| Rentals (8) | Near bin x log(Articles) | 0.007 | 0.002 *** | 0.002 *** | [0.003, 0.011] |  |
| Rentals (9) | Near bin x log(Articles) | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (10) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (11) | Near bin x log(Articles) | -0.003 | 0.001 *** | 0.001 * | [-0.005, -0.001] | *** → * |
| Rentals (12) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

## Intensive margin, Google Trends peak, 250 m

`did_trends_prior_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.025 | 0.009 *** | 0.011 ** | [-0.047, -0.003] | *** → ** |
| Sales (2) | Spills per week (avg.) | -0.013 | 0.009  | 0.011  | [-0.035, 0.009] |  |
| Sales (3) | Spills per week (avg.) | -0.005 | 0.005  | 0.005  | [-0.015, 0.005] |  |
| Sales (4) | Spills per week (avg.) | 0.000 | 0.004  | 0.005  | [-0.010, 0.010] |  |
| Sales (5) | Spills per week (avg.) | -0.003 | 0.005  | 0.005  | [-0.013, 0.007] |  |
| Sales (6) | Spills per week (avg.) | 0.002 | 0.005  | 0.004  | [-0.006, 0.010] |  |
| Rentals (7) | Spills per week (avg.) | -0.035 | 0.010 *** | 0.014 ** | [-0.062, -0.008] | *** → ** |
| Rentals (8) | Spills per week (avg.) | -0.032 | 0.008 *** | 0.011 *** | [-0.054, -0.010] |  |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.004  | 0.004  | [-0.014, 0.002] |  |
| Rentals (10) | Spills per week (avg.) | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Rentals (11) | Spills per week (avg.) | -0.006 | 0.004  | 0.005  | [-0.016, 0.004] |  |
| Rentals (12) | Spills per week (avg.) | -0.004 | 0.003  | 0.004  | [-0.012, 0.004] |  |
| Sales (1) | Post | 0.047 | 0.010 *** | 0.011 *** | [0.025, 0.069] |  |
| Sales (2) | Post | 0.051 | 0.010 *** | 0.011 *** | [0.029, 0.073] |  |
| Rentals (7) | Post | 0.151 | 0.009 *** | 0.011 *** | [0.129, 0.173] |  |
| Rentals (8) | Post | 0.130 | 0.008 *** | 0.010 *** | [0.110, 0.150] |  |
| Sales (1) | Spills per week (avg.) x Post | -0.034 | 0.011 *** | 0.013 *** | [-0.059, -0.009] |  |
| Sales (2) | Spills per week (avg.) x Post | -0.023 | 0.010 ** | 0.012 ** | [-0.047, 0.001] |  |
| Sales (3) | Spills per week (avg.) x Post | -0.010 | 0.006 * | 0.006 * | [-0.022, 0.002] |  |
| Sales (4) | Spills per week (avg.) x Post | -0.004 | 0.005  | 0.005  | [-0.014, 0.006] |  |
| Sales (5) | Spills per week (avg.) x Post | -0.004 | 0.006  | 0.005  | [-0.014, 0.006] |  |
| Sales (6) | Spills per week (avg.) x Post | 0.003 | 0.005  | 0.005  | [-0.007, 0.013] |  |
| Rentals (7) | Spills per week (avg.) x Post | -0.024 | 0.013 * | 0.020  | [-0.063, 0.015] | * → none |
| Rentals (8) | Spills per week (avg.) x Post | -0.019 | 0.012  | 0.018  | [-0.054, 0.016] |  |
| Rentals (9) | Spills per week (avg.) x Post | 0.000 | 0.004  | 0.004  | [-0.008, 0.008] |  |
| Rentals (10) | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| Rentals (11) | Spills per week (avg.) x Post | 0.003 | 0.004  | 0.005  | [-0.007, 0.013] |  |
| Rentals (12) | Spills per week (avg.) x Post | 0.006 | 0.003 * | 0.004  | [-0.002, 0.014] | * → none |

## Intensive margin, cumulative articles, 250 m

`did_articles_prior_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.009 | 0.011  | 0.011  | [-0.013, 0.031] |  |
| Sales (2) | Spills per week (avg.) | 0.009 | 0.011  | 0.012  | [-0.015, 0.033] |  |
| Sales (3) | Spills per week (avg.) | 0.008 | 0.007  | 0.006  | [-0.004, 0.020] |  |
| Sales (4) | Spills per week (avg.) | 0.009 | 0.006  | 0.006 * | [-0.003, 0.021] | none → * |
| Sales (5) | Spills per week (avg.) | 0.005 | 0.006  | 0.006  | [-0.007, 0.017] |  |
| Sales (6) | Spills per week (avg.) | 0.006 | 0.006  | 0.005  | [-0.004, 0.016] |  |
| Rentals (7) | Spills per week (avg.) | 0.006 | 0.015  | 0.023  | [-0.039, 0.051] |  |
| Rentals (8) | Spills per week (avg.) | 0.003 | 0.013  | 0.020  | [-0.036, 0.042] |  |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.005  | 0.005  | [-0.016, 0.004] |  |
| Rentals (10) | Spills per week (avg.) | -0.003 | 0.004  | 0.004  | [-0.011, 0.005] |  |
| Rentals (11) | Spills per week (avg.) | -0.008 | 0.005  | 0.006  | [-0.020, 0.004] |  |
| Rentals (12) | Spills per week (avg.) | -0.008 | 0.004 ** | 0.004 * | [-0.016, 0.000] | ** → * |
| Sales (1) | log(Articles) | 0.012 | 0.004 *** | 0.004 *** | [0.004, 0.020] |  |
| Sales (2) | log(Articles) | 0.016 | 0.003 *** | 0.004 *** | [0.008, 0.024] |  |
| Rentals (7) | log(Articles) | 0.062 | 0.004 *** | 0.005 *** | [0.052, 0.072] |  |
| Rentals (8) | log(Articles) | 0.055 | 0.004 *** | 0.004 *** | [0.047, 0.063] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.011 | 0.003 *** | 0.004 *** | [-0.019, -0.003] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.003 *** | 0.003 ** | [-0.013, -0.001] | *** → ** |
| Sales (3) | Spills per week (avg.) x log(Articles) | -0.004 | 0.002 *** | 0.002 *** | [-0.008, 0.000] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | -0.003 | 0.001 ** | 0.001 ** | [-0.005, -0.001] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | -0.003 | 0.002 * | 0.002 * | [-0.007, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.011 | 0.004 ** | 0.007  | [-0.025, 0.003] | ** → none |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.009 | 0.004 ** | 0.006  | [-0.021, 0.003] | ** → none |
| Rentals (9) | Spills per week (avg.) x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (11) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

## Intensive margin, Google Trends peak, 500 m and 1000 m

`did_trends_prior_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.045 | 0.006 *** | 0.009 *** | [-0.063, -0.027] |  |
| Sales (2) | Spills per week (avg.) | -0.031 | 0.005 *** | 0.008 *** | [-0.047, -0.015] |  |
| Sales (3) | Spills per week (avg.) | -0.008 | 0.003 *** | 0.003 ** | [-0.014, -0.002] | *** → ** |
| Sales (4) | Spills per week (avg.) | 0.000 | 0.003  | 0.003  | [-0.006, 0.006] |  |
| Sales (5) | Spills per week (avg.) | -0.009 | 0.003 *** | 0.003 ** | [-0.015, -0.003] | *** → ** |
| Sales (6) | Spills per week (avg.) | 0.000 | 0.003  | 0.003  | [-0.006, 0.006] |  |
| Rentals (7) | Spills per week (avg.) | -0.046 | 0.006 *** | 0.011 *** | [-0.068, -0.024] |  |
| Rentals (8) | Spills per week (avg.) | -0.039 | 0.006 *** | 0.009 *** | [-0.057, -0.021] |  |
| Rentals (9) | Spills per week (avg.) | -0.007 | 0.002 *** | 0.002 *** | [-0.011, -0.003] |  |
| Rentals (10) | Spills per week (avg.) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Spills per week (avg.) | -0.008 | 0.003 *** | 0.002 *** | [-0.012, -0.004] |  |
| Rentals (12) | Spills per week (avg.) | -0.004 | 0.002 ** | 0.002 * | [-0.008, 0.000] | ** → * |
| Sales (1) | Post | 0.050 | 0.005 *** | 0.007 *** | [0.036, 0.064] |  |
| Sales (2) | Post | 0.049 | 0.005 *** | 0.007 *** | [0.035, 0.063] |  |
| Rentals (7) | Post | 0.151 | 0.006 *** | 0.008 *** | [0.135, 0.167] |  |
| Rentals (8) | Post | 0.128 | 0.005 *** | 0.007 *** | [0.114, 0.142] |  |
| Sales (1) | Spills per week (avg.) x Post | -0.028 | 0.007 *** | 0.008 *** | [-0.044, -0.012] |  |
| Sales (2) | Spills per week (avg.) x Post | -0.016 | 0.006 ** | 0.008 ** | [-0.032, 0.000] |  |
| Sales (3) | Spills per week (avg.) x Post | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Sales (4) | Spills per week (avg.) x Post | 0.002 | 0.003  | 0.003  | [-0.004, 0.008] |  |
| Sales (5) | Spills per week (avg.) x Post | 0.000 | 0.003  | 0.003  | [-0.006, 0.006] |  |
| Sales (6) | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.002  | [-0.001, 0.007] |  |
| Rentals (7) | Spills per week (avg.) x Post | -0.030 | 0.007 *** | 0.009 *** | [-0.048, -0.012] |  |
| Rentals (8) | Spills per week (avg.) x Post | -0.021 | 0.006 *** | 0.008 *** | [-0.037, -0.005] |  |
| Rentals (9) | Spills per week (avg.) x Post | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (10) | Spills per week (avg.) x Post | 0.003 | 0.002 * | 0.002  | [-0.001, 0.007] | * → none |
| Rentals (11) | Spills per week (avg.) x Post | -0.002 | 0.002  | 0.003  | [-0.008, 0.004] |  |
| Rentals (12) | Spills per week (avg.) x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |

`did_trends_prior_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.056 | 0.003 *** | 0.005 *** | [-0.066, -0.046] |  |
| Sales (2) | Spills per week (avg.) | -0.044 | 0.003 *** | 0.005 *** | [-0.054, -0.034] |  |
| Sales (3) | Spills per week (avg.) | -0.015 | 0.002 *** | 0.002 *** | [-0.019, -0.011] |  |
| Sales (4) | Spills per week (avg.) | -0.005 | 0.001 *** | 0.002 *** | [-0.009, -0.001] |  |
| Sales (5) | Spills per week (avg.) | -0.011 | 0.002 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (6) | Spills per week (avg.) | -0.004 | 0.002 ** | 0.002 ** | [-0.008, 0.000] |  |
| Rentals (7) | Spills per week (avg.) | -0.049 | 0.004 *** | 0.008 *** | [-0.065, -0.033] |  |
| Rentals (8) | Spills per week (avg.) | -0.041 | 0.003 *** | 0.007 *** | [-0.055, -0.027] |  |
| Rentals (9) | Spills per week (avg.) | -0.007 | 0.001 *** | 0.002 *** | [-0.011, -0.003] |  |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.001 ** | 0.001  | [-0.004, 0.000] | ** → none |
| Rentals (11) | Spills per week (avg.) | -0.007 | 0.002 *** | 0.002 *** | [-0.011, -0.003] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Sales (1) | Post | 0.051 | 0.004 *** | 0.005 *** | [0.041, 0.061] |  |
| Sales (2) | Post | 0.052 | 0.004 *** | 0.005 *** | [0.042, 0.062] |  |
| Rentals (7) | Post | 0.153 | 0.004 *** | 0.007 *** | [0.139, 0.167] |  |
| Rentals (8) | Post | 0.128 | 0.004 *** | 0.006 *** | [0.116, 0.140] |  |
| Sales (1) | Spills per week (avg.) x Post | -0.026 | 0.003 *** | 0.004 *** | [-0.034, -0.018] |  |
| Sales (2) | Spills per week (avg.) x Post | -0.018 | 0.002 *** | 0.004 *** | [-0.026, -0.010] |  |
| Sales (3) | Spills per week (avg.) x Post | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Sales (4) | Spills per week (avg.) x Post | 0.003 | 0.001 ** | 0.002 * | [-0.001, 0.007] | ** → * |
| Sales (5) | Spills per week (avg.) x Post | 0.004 | 0.001 *** | 0.002 ** | [0.000, 0.008] | *** → ** |
| Sales (6) | Spills per week (avg.) x Post | 0.005 | 0.001 *** | 0.001 *** | [0.003, 0.007] |  |
| Rentals (7) | Spills per week (avg.) x Post | -0.026 | 0.003 *** | 0.005 *** | [-0.036, -0.016] |  |
| Rentals (8) | Spills per week (avg.) x Post | -0.019 | 0.002 *** | 0.004 *** | [-0.027, -0.011] |  |
| Rentals (9) | Spills per week (avg.) x Post | -0.004 | 0.001 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (10) | Spills per week (avg.) x Post | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Spills per week (avg.) x Post | -0.004 | 0.001 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (12) | Spills per week (avg.) x Post | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |

## Intensive margin, cumulative articles, 500 m and 1000 m

`did_articles_prior_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.010 | 0.007  | 0.009  | [-0.028, 0.008] |  |
| Sales (2) | Spills per week (avg.) | -0.009 | 0.007  | 0.008  | [-0.025, 0.007] |  |
| Sales (3) | Spills per week (avg.) | -0.005 | 0.004  | 0.004  | [-0.013, 0.003] |  |
| Sales (4) | Spills per week (avg.) | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Sales (5) | Spills per week (avg.) | -0.007 | 0.003 * | 0.004 * | [-0.015, 0.001] |  |
| Sales (6) | Spills per week (avg.) | -0.002 | 0.003  | 0.003  | [-0.008, 0.004] |  |
| Rentals (7) | Spills per week (avg.) | 0.005 | 0.008  | 0.015  | [-0.024, 0.034] |  |
| Rentals (8) | Spills per week (avg.) | -0.001 | 0.007  | 0.012  | [-0.025, 0.023] |  |
| Rentals (9) | Spills per week (avg.) | -0.005 | 0.003  | 0.003  | [-0.011, 0.001] |  |
| Rentals (10) | Spills per week (avg.) | -0.004 | 0.002 * | 0.003  | [-0.010, 0.002] | * → none |
| Rentals (11) | Spills per week (avg.) | -0.005 | 0.003 * | 0.003  | [-0.011, 0.001] | * → none |
| Rentals (12) | Spills per week (avg.) | -0.005 | 0.002 ** | 0.003 * | [-0.011, 0.001] | ** → * |
| Sales (1) | log(Articles) | 0.013 | 0.002 *** | 0.002 *** | [0.009, 0.017] |  |
| Sales (2) | log(Articles) | 0.015 | 0.002 *** | 0.002 *** | [0.011, 0.019] |  |
| Rentals (7) | log(Articles) | 0.062 | 0.003 *** | 0.004 *** | [0.054, 0.070] |  |
| Rentals (8) | log(Articles) | 0.054 | 0.002 *** | 0.003 *** | [0.048, 0.060] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.011 | 0.002 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.002 *** | 0.002 *** | [-0.011, -0.003] |  |
| Sales (3) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.014 | 0.002 *** | 0.003 *** | [-0.020, -0.008] |  |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.010 | 0.002 *** | 0.003 *** | [-0.016, -0.004] |  |
| Rentals (9) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Rentals (11) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

`did_articles_prior_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.024 | 0.003 *** | 0.005 *** | [-0.034, -0.014] |  |
| Sales (2) | Spills per week (avg.) | -0.021 | 0.003 *** | 0.005 *** | [-0.031, -0.011] |  |
| Sales (3) | Spills per week (avg.) | -0.013 | 0.002 *** | 0.002 *** | [-0.017, -0.009] |  |
| Sales (4) | Spills per week (avg.) | -0.005 | 0.002 *** | 0.002 *** | [-0.009, -0.001] |  |
| Sales (5) | Spills per week (avg.) | -0.012 | 0.002 *** | 0.002 *** | [-0.016, -0.008] |  |
| Sales (6) | Spills per week (avg.) | -0.005 | 0.002 *** | 0.002 *** | [-0.009, -0.001] |  |
| Rentals (7) | Spills per week (avg.) | -0.004 | 0.004  | 0.010  | [-0.024, 0.016] |  |
| Rentals (8) | Spills per week (avg.) | -0.007 | 0.004 ** | 0.008  | [-0.023, 0.009] | ** → none |
| Rentals (9) | Spills per week (avg.) | -0.003 | 0.002 * | 0.002  | [-0.007, 0.001] | * → none |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |
| Rentals (11) | Spills per week (avg.) | -0.003 | 0.002  | 0.003  | [-0.009, 0.003] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Sales (1) | log(Articles) | 0.012 | 0.001 *** | 0.002 *** | [0.008, 0.016] |  |
| Sales (2) | log(Articles) | 0.014 | 0.001 *** | 0.002 *** | [0.010, 0.018] |  |
| Rentals (7) | log(Articles) | 0.064 | 0.002 *** | 0.003 *** | [0.058, 0.070] |  |
| Rentals (8) | log(Articles) | 0.053 | 0.002 *** | 0.003 *** | [0.047, 0.059] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.010 | 0.001 *** | 0.001 *** | [-0.012, -0.008] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.001 *** | 0.001 *** | [-0.009, -0.005] |  |
| Sales (3) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | 0.001 | 0.000 ** | 0.000 * | [0.001, 0.001] | ** → * |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.012 | 0.001 *** | 0.002 *** | [-0.016, -0.008] |  |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.009 | 0.001 *** | 0.001 *** | [-0.011, -0.007] |  |
| Rentals (9) | Spills per week (avg.) x log(Articles) | -0.001 | 0.000 *** | 0.001 ** | [-0.003, 0.001] | *** → ** |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spills per week (avg.) x log(Articles) | -0.001 | 0.000 *** | 0.001 ** | [-0.003, 0.001] | *** → ** |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |

## Radius robustness summaries

`did_trends_prior_extensive_radius_robustness.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales 250m | Near bin x Post | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Property controls + MSOA FE, Sales 500m | Near bin x Post | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Property controls + MSOA FE, Sales 1000m | Near bin x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Property controls + MSOA FE, Rentals 250m | Near bin x Post | -0.007 | 0.003 *** | 0.004 * | [-0.015, 0.001] | *** → * |
| Property controls + MSOA FE, Rentals 500m | Near bin x Post | -0.006 | 0.002 *** | 0.003  | [-0.012, 0.000] | *** → none |
| Property controls + MSOA FE, Rentals 1000m | Near bin x Post | -0.003 | 0.002 * | 0.003  | [-0.009, 0.003] | * → none |
| Property controls + LSOA FE, Sales 250m | Near bin x Post | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Property controls + LSOA FE, Sales 500m | Near bin x Post | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Property controls + LSOA FE, Sales 1000m | Near bin x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Property controls + LSOA FE, Rentals 250m | Near bin x Post | -0.008 | 0.003 *** | 0.004 ** | [-0.016, 0.000] | *** → ** |
| Property controls + LSOA FE, Rentals 500m | Near bin x Post | -0.007 | 0.002 *** | 0.003 * | [-0.013, -0.001] | *** → * |
| Property controls + LSOA FE, Rentals 1000m | Near bin x Post | -0.004 | 0.002 ** | 0.003  | [-0.010, 0.002] | ** → none |

`did_articles_prior_extensive_radius_robustness.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales 250m | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Property controls + MSOA FE, Sales 500m | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Sales 1000m | Near bin x log(Articles) | 0.001 | 0.001 * | 0.001 * | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Rentals 250m | Near bin x log(Articles) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Property controls + MSOA FE, Rentals 500m | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + MSOA FE, Rentals 1000m | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + LSOA FE, Sales 250m | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Property controls + LSOA FE, Sales 500m | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Sales 1000m | Near bin x log(Articles) | 0.001 | 0.001 ** | 0.001 * | [-0.001, 0.003] | ** → * |
| Property controls + LSOA FE, Rentals 250m | Near bin x log(Articles) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Property controls + LSOA FE, Rentals 500m | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + LSOA FE, Rentals 1000m | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

`did_trends_prior_radius_robustness.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales 250m | Spills per week (avg.) x Post | -0.004 | 0.005  | 0.005  | [-0.014, 0.006] |  |
| Property controls + MSOA FE, Sales 500m | Spills per week (avg.) x Post | 0.002 | 0.003  | 0.003  | [-0.004, 0.008] |  |
| Property controls + MSOA FE, Sales 1000m | Spills per week (avg.) x Post | 0.003 | 0.001 ** | 0.002 * | [-0.001, 0.007] | ** → * |
| Property controls + MSOA FE, Rentals 250m | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| Property controls + MSOA FE, Rentals 500m | Spills per week (avg.) x Post | 0.003 | 0.002 * | 0.002  | [-0.001, 0.007] | * → none |
| Property controls + MSOA FE, Rentals 1000m | Spills per week (avg.) x Post | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Property controls + LSOA FE, Sales 250m | Spills per week (avg.) x Post | 0.003 | 0.005  | 0.005  | [-0.007, 0.013] |  |
| Property controls + LSOA FE, Sales 500m | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.002  | [-0.001, 0.007] |  |
| Property controls + LSOA FE, Sales 1000m | Spills per week (avg.) x Post | 0.005 | 0.001 *** | 0.001 *** | [0.003, 0.007] |  |
| Property controls + LSOA FE, Rentals 250m | Spills per week (avg.) x Post | 0.006 | 0.003 * | 0.004  | [-0.002, 0.014] | * → none |
| Property controls + LSOA FE, Rentals 500m | Spills per week (avg.) x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Property controls + LSOA FE, Rentals 1000m | Spills per week (avg.) x Post | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |

`did_articles_prior_radius_robustness.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales 250m | Spills per week (avg.) x log(Articles) | -0.003 | 0.001 ** | 0.001 ** | [-0.005, -0.001] |  |
| Property controls + MSOA FE, Sales 500m | Spills per week (avg.) x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Property controls + MSOA FE, Sales 1000m | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Property controls + MSOA FE, Rentals 250m | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Rentals 500m | Spills per week (avg.) x log(Articles) | 0.001 | 0.001 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Property controls + MSOA FE, Rentals 1000m | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Property controls + LSOA FE, Sales 250m | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + LSOA FE, Sales 500m | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Sales 1000m | Spills per week (avg.) x log(Articles) | 0.001 | 0.000 ** | 0.000 * | [0.001, 0.001] | ** → * |
| Property controls + LSOA FE, Rentals 250m | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Rentals 500m | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Rentals 1000m | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |

## Windowed articles: summary tables

`did_articles_windowed_prior_extensive_slide.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales Cumulative | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Sales 3m | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Sales 6m | Near bin x log (Articles measure) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Property controls + MSOA FE, Sales 12m | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Rentals Cumulative | Near bin x log (Articles measure) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + MSOA FE, Rentals 3m | Near bin x log (Articles measure) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Property controls + MSOA FE, Rentals 6m | Near bin x log (Articles measure) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Property controls + MSOA FE, Rentals 12m | Near bin x log (Articles measure) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Property controls + LSOA FE, Sales Cumulative | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Sales 3m | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Sales 6m | Near bin x log (Articles measure) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Property controls + LSOA FE, Sales 12m | Near bin x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Rentals Cumulative | Near bin x log (Articles measure) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + LSOA FE, Rentals 3m | Near bin x log (Articles measure) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |
| Property controls + LSOA FE, Rentals 6m | Near bin x log (Articles measure) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Property controls + LSOA FE, Rentals 12m | Near bin x log (Articles measure) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |

`did_articles_windowed_prior_comparison.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Property controls + MSOA FE, Sales Cumulative | Spills per week (avg.) x log (Articles measure) | -0.003 | 0.001 ** | 0.001 ** | [-0.005, -0.001] |  |
| Property controls + MSOA FE, Sales 3m | Spills per week (avg.) x log (Articles measure) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Property controls + MSOA FE, Sales 6m | Spills per week (avg.) x log (Articles measure) | -0.005 | 0.003 * | 0.003 * | [-0.011, 0.001] |  |
| Property controls + MSOA FE, Sales 12m | Spills per week (avg.) x log (Articles measure) | -0.005 | 0.002 ** | 0.002 ** | [-0.009, -0.001] |  |
| Property controls + MSOA FE, Rentals Cumulative | Spills per week (avg.) x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Rentals 3m | Spills per week (avg.) x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + MSOA FE, Rentals 6m | Spills per week (avg.) x log (Articles measure) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Property controls + MSOA FE, Rentals 12m | Spills per week (avg.) x log (Articles measure) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Property controls + LSOA FE, Sales Cumulative | Spills per week (avg.) x log (Articles measure) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Property controls + LSOA FE, Sales 3m | Spills per week (avg.) x log (Articles measure) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Property controls + LSOA FE, Sales 6m | Spills per week (avg.) x log (Articles measure) | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Property controls + LSOA FE, Sales 12m | Spills per week (avg.) x log (Articles measure) | -0.002 | 0.003  | 0.002  | [-0.006, 0.002] |  |
| Property controls + LSOA FE, Rentals Cumulative | Spills per week (avg.) x log (Articles measure) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Property controls + LSOA FE, Rentals 3m | Spills per week (avg.) x log (Articles measure) | 0.002 | 0.001  | 0.002  | [-0.002, 0.006] |  |
| Property controls + LSOA FE, Rentals 6m | Spills per week (avg.) x log (Articles measure) | 0.003 | 0.002 * | 0.002  | [-0.001, 0.007] | * → none |
| Property controls + LSOA FE, Rentals 12m | Spills per week (avg.) x log (Articles measure) | 0.003 | 0.002  | 0.002  | [-0.001, 0.007] |  |

## Windowed articles, extensive margin, by distance-band comparison and window

The legacy copies `did_articles_windowed_prior_extensive_{3m,6m,12m}.tex` are byte-identical to the `500_vs_1000_2000` files below and are not repeated. The `500_vs_1000_2000` cumulative table is the same model as the headline extensive articles table.

`did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.207 | 0.010 *** | 0.016 *** | [-0.238, -0.176] |  |
| Sales (2) | Near bin | -0.160 | 0.010 *** | 0.017 *** | [-0.193, -0.127] |  |
| Sales (3) | Near bin | -0.079 | 0.006 *** | 0.006 *** | [-0.091, -0.067] |  |
| Sales (4) | Near bin | -0.020 | 0.005 *** | 0.005 *** | [-0.030, -0.010] |  |
| Sales (5) | Near bin | -0.101 | 0.008 *** | 0.008 *** | [-0.117, -0.085] |  |
| Sales (6) | Near bin | -0.064 | 0.007 *** | 0.007 *** | [-0.078, -0.050] |  |
| Rentals (7) | Near bin | -0.228 | 0.016 *** | 0.039 *** | [-0.304, -0.152] |  |
| Rentals (8) | Near bin | -0.202 | 0.014 *** | 0.033 *** | [-0.267, -0.137] |  |
| Rentals (9) | Near bin | -0.021 | 0.006 *** | 0.009 ** | [-0.039, -0.003] | *** → ** |
| Rentals (10) | Near bin | 0.007 | 0.005  | 0.008  | [-0.009, 0.023] |  |
| Rentals (11) | Near bin | -0.050 | 0.010 *** | 0.011 *** | [-0.072, -0.028] |  |
| Rentals (12) | Near bin | -0.011 | 0.006 * | 0.009  | [-0.029, 0.007] | * → none |
| Sales (1) | log(Articles) | 0.007 | 0.001 *** | 0.001 *** | [0.005, 0.009] |  |
| Sales (2) | log(Articles) | 0.011 | 0.001 *** | 0.001 *** | [0.009, 0.013] |  |
| Rentals (7) | log(Articles) | 0.046 | 0.001 *** | 0.002 *** | [0.042, 0.050] |  |
| Rentals (8) | log(Articles) | 0.040 | 0.001 *** | 0.001 *** | [0.038, 0.042] |  |
| Sales (1) | Near bin x log(Articles) | 0.002 | 0.001  | 0.002  | [-0.002, 0.006] |  |
| Sales (2) | Near bin x log(Articles) | 0.003 | 0.001 * | 0.002 * | [-0.001, 0.007] |  |
| Sales (3) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (5) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Near bin x log(Articles) | 0.006 | 0.002 ** | 0.003 * | [0.000, 0.012] | ** → * |
| Rentals (8) | Near bin x log(Articles) | 0.007 | 0.002 *** | 0.002 *** | [0.003, 0.011] |  |
| Rentals (9) | Near bin x log(Articles) | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (10) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (11) | Near bin x log(Articles) | -0.003 | 0.001 *** | 0.001 * | [-0.005, -0.001] | *** → * |
| Rentals (12) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

`did_articles_windowed_prior_extensive_500_vs_1000_2000_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.205 | 0.011 *** | 0.016 *** | [-0.236, -0.174] |  |
| Sales (2) | Near bin | -0.159 | 0.011 *** | 0.017 *** | [-0.192, -0.126] |  |
| Sales (3) | Near bin | -0.076 | 0.006 *** | 0.007 *** | [-0.090, -0.062] |  |
| Sales (4) | Near bin | -0.018 | 0.006 *** | 0.006 *** | [-0.030, -0.006] |  |
| Sales (5) | Near bin | -0.099 | 0.009 *** | 0.008 *** | [-0.115, -0.083] |  |
| Sales (6) | Near bin | -0.063 | 0.007 *** | 0.007 *** | [-0.077, -0.049] |  |
| Rentals (7) | Near bin | -0.227 | 0.016 *** | 0.038 *** | [-0.301, -0.153] |  |
| Rentals (8) | Near bin | -0.200 | 0.013 *** | 0.033 *** | [-0.265, -0.135] |  |
| Rentals (9) | Near bin | -0.021 | 0.007 *** | 0.009 ** | [-0.039, -0.003] | *** → ** |
| Rentals (10) | Near bin | 0.007 | 0.005  | 0.008  | [-0.009, 0.023] |  |
| Rentals (11) | Near bin | -0.050 | 0.010 *** | 0.012 *** | [-0.074, -0.026] |  |
| Rentals (12) | Near bin | -0.010 | 0.006  | 0.009  | [-0.028, 0.008] |  |
| Sales (1) | log (Articles_3m) | 0.005 | 0.002 *** | 0.002 *** | [0.001, 0.009] |  |
| Sales (2) | log (Articles_3m) | 0.012 | 0.002 *** | 0.002 *** | [0.008, 0.016] |  |
| Rentals (7) | log (Articles_3m) | 0.064 | 0.001 *** | 0.002 *** | [0.060, 0.068] |  |
| Rentals (8) | log (Articles_3m) | 0.055 | 0.001 *** | 0.002 *** | [0.051, 0.059] |  |
| Sales (1) | Near bin x log (Articles_3m) | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Sales (2) | Near bin x log (Articles_3m) | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| Sales (3) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Near bin x log (Articles_3m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (5) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Near bin x log (Articles_3m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Near bin x log (Articles_3m) | 0.007 | 0.003 ** | 0.004 * | [-0.001, 0.015] | ** → * |
| Rentals (8) | Near bin x log (Articles_3m) | 0.008 | 0.002 *** | 0.003 *** | [0.002, 0.014] |  |
| Rentals (9) | Near bin x log (Articles_3m) | -0.004 | 0.001 *** | 0.002 * | [-0.008, 0.000] | *** → * |
| Rentals (10) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_3m) | -0.003 | 0.001 ** | 0.002  | [-0.007, 0.001] | ** → none |
| Rentals (12) | Near bin x log (Articles_3m) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |

`did_articles_windowed_prior_extensive_500_vs_1000_2000_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.206 | 0.013 *** | 0.018 *** | [-0.241, -0.171] |  |
| Sales (2) | Near bin | -0.162 | 0.014 *** | 0.019 *** | [-0.199, -0.125] |  |
| Sales (3) | Near bin | -0.072 | 0.008 *** | 0.008 *** | [-0.088, -0.056] |  |
| Sales (4) | Near bin | -0.018 | 0.007 *** | 0.007 ** | [-0.032, -0.004] | *** → ** |
| Sales (5) | Near bin | -0.096 | 0.009 *** | 0.009 *** | [-0.114, -0.078] |  |
| Sales (6) | Near bin | -0.062 | 0.008 *** | 0.008 *** | [-0.078, -0.046] |  |
| Rentals (7) | Near bin | -0.247 | 0.020 *** | 0.045 *** | [-0.335, -0.159] |  |
| Rentals (8) | Near bin | -0.222 | 0.017 *** | 0.037 *** | [-0.295, -0.149] |  |
| Rentals (9) | Near bin | -0.018 | 0.008 ** | 0.012  | [-0.042, 0.006] | ** → none |
| Rentals (10) | Near bin | 0.007 | 0.006  | 0.011  | [-0.015, 0.029] |  |
| Rentals (11) | Near bin | -0.048 | 0.011 *** | 0.014 *** | [-0.075, -0.021] |  |
| Rentals (12) | Near bin | -0.010 | 0.007  | 0.012  | [-0.034, 0.014] |  |
| Sales (1) | log (Articles_6m) | 0.010 | 0.002 *** | 0.002 *** | [0.006, 0.014] |  |
| Sales (2) | log (Articles_6m) | 0.017 | 0.002 *** | 0.002 *** | [0.013, 0.021] |  |
| Rentals (7) | log (Articles_6m) | 0.083 | 0.002 *** | 0.003 *** | [0.077, 0.089] |  |
| Rentals (8) | log (Articles_6m) | 0.071 | 0.002 *** | 0.002 *** | [0.067, 0.075] |  |
| Sales (1) | Near bin x log (Articles_6m) | 0.002 | 0.003  | 0.003  | [-0.004, 0.008] |  |
| Sales (2) | Near bin x log (Articles_6m) | 0.004 | 0.003  | 0.003  | [-0.002, 0.010] |  |
| Sales (3) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (4) | Near bin x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (6) | Near bin x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_6m) | 0.011 | 0.004 *** | 0.005 ** | [0.001, 0.021] | *** → ** |
| Rentals (8) | Near bin x log (Articles_6m) | 0.012 | 0.003 *** | 0.004 *** | [0.004, 0.020] |  |
| Rentals (9) | Near bin x log (Articles_6m) | -0.004 | 0.002 ** | 0.003  | [-0.010, 0.002] | ** → none |
| Rentals (10) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_6m) | -0.003 | 0.002 ** | 0.003  | [-0.009, 0.003] | ** → none |
| Rentals (12) | Near bin x log (Articles_6m) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |

`did_articles_windowed_prior_extensive_500_vs_1000_2000_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.211 | 0.014 *** | 0.018 *** | [-0.246, -0.176] |  |
| Sales (2) | Near bin | -0.167 | 0.014 *** | 0.019 *** | [-0.204, -0.130] |  |
| Sales (3) | Near bin | -0.076 | 0.008 *** | 0.009 *** | [-0.094, -0.058] |  |
| Sales (4) | Near bin | -0.021 | 0.007 *** | 0.008 *** | [-0.037, -0.005] |  |
| Sales (5) | Near bin | -0.099 | 0.010 *** | 0.009 *** | [-0.117, -0.081] |  |
| Sales (6) | Near bin | -0.065 | 0.008 *** | 0.008 *** | [-0.081, -0.049] |  |
| Rentals (7) | Near bin | -0.248 | 0.021 *** | 0.044 *** | [-0.334, -0.162] |  |
| Rentals (8) | Near bin | -0.225 | 0.018 *** | 0.037 *** | [-0.298, -0.152] |  |
| Rentals (9) | Near bin | -0.010 | 0.009  | 0.013  | [-0.035, 0.015] |  |
| Rentals (10) | Near bin | 0.011 | 0.007  | 0.012  | [-0.013, 0.035] |  |
| Rentals (11) | Near bin | -0.040 | 0.012 *** | 0.015 *** | [-0.069, -0.011] |  |
| Rentals (12) | Near bin | -0.006 | 0.008  | 0.013  | [-0.031, 0.019] |  |
| Sales (1) | log (Articles_12m) | 0.013 | 0.002 *** | 0.002 *** | [0.009, 0.017] |  |
| Sales (2) | log (Articles_12m) | 0.019 | 0.002 *** | 0.002 *** | [0.015, 0.023] |  |
| Rentals (7) | log (Articles_12m) | 0.085 | 0.002 *** | 0.003 *** | [0.079, 0.091] |  |
| Rentals (8) | log (Articles_12m) | 0.074 | 0.002 *** | 0.002 *** | [0.070, 0.078] |  |
| Sales (1) | Near bin x log (Articles_12m) | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| Sales (2) | Near bin x log (Articles_12m) | 0.004 | 0.003  | 0.003  | [-0.002, 0.010] |  |
| Sales (3) | Near bin x log (Articles_12m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (4) | Near bin x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (5) | Near bin x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Near bin x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Near bin x log (Articles_12m) | 0.010 | 0.004 ** | 0.005 ** | [0.000, 0.020] |  |
| Rentals (8) | Near bin x log (Articles_12m) | 0.011 | 0.003 *** | 0.004 *** | [0.003, 0.019] |  |
| Rentals (9) | Near bin x log (Articles_12m) | -0.005 | 0.002 *** | 0.003 ** | [-0.011, 0.001] | *** → ** |
| Rentals (10) | Near bin x log (Articles_12m) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Rentals (11) | Near bin x log (Articles_12m) | -0.005 | 0.002 *** | 0.003 * | [-0.011, 0.001] | *** → * |
| Rentals (12) | Near bin x log (Articles_12m) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |

`did_articles_windowed_prior_extensive_250_vs_250_1000_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.067 | 0.015 *** | 0.016 *** | [-0.098, -0.036] |  |
| Sales (2) | Near bin | -0.033 | 0.014 ** | 0.016 ** | [-0.064, -0.002] |  |
| Sales (3) | Near bin | -0.023 | 0.007 *** | 0.008 *** | [-0.039, -0.007] |  |
| Sales (4) | Near bin | 0.021 | 0.006 *** | 0.007 *** | [0.007, 0.035] |  |
| Sales (5) | Near bin | -0.014 | 0.007 ** | 0.007 ** | [-0.028, 0.000] |  |
| Sales (6) | Near bin | 0.018 | 0.006 *** | 0.006 *** | [0.006, 0.030] |  |
| Rentals (7) | Near bin | -0.087 | 0.019 *** | 0.023 *** | [-0.132, -0.042] |  |
| Rentals (8) | Near bin | -0.080 | 0.017 *** | 0.021 *** | [-0.121, -0.039] |  |
| Rentals (9) | Near bin | 0.004 | 0.006  | 0.007  | [-0.010, 0.018] |  |
| Rentals (10) | Near bin | 0.017 | 0.005 *** | 0.006 *** | [0.005, 0.029] |  |
| Rentals (11) | Near bin | 0.001 | 0.006  | 0.007  | [-0.013, 0.015] |  |
| Rentals (12) | Near bin | 0.012 | 0.005 ** | 0.005 ** | [0.002, 0.022] |  |
| Sales (1) | log(Articles) | 0.008 | 0.001 *** | 0.001 *** | [0.006, 0.010] |  |
| Sales (2) | log(Articles) | 0.012 | 0.001 *** | 0.001 *** | [0.010, 0.014] |  |
| Rentals (7) | log(Articles) | 0.049 | 0.001 *** | 0.002 *** | [0.045, 0.053] |  |
| Rentals (8) | log(Articles) | 0.043 | 0.001 *** | 0.001 *** | [0.041, 0.045] |  |
| Sales (1) | Near bin x log(Articles) | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Sales (2) | Near bin x log(Articles) | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Sales (3) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (5) | Near bin x log(Articles) | -0.002 | 0.001  | 0.001  | [-0.004, 0.000] |  |
| Sales (6) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (7) | Near bin x log(Articles) | 0.004 | 0.002  | 0.003  | [-0.002, 0.010] |  |
| Rentals (8) | Near bin x log(Articles) | 0.005 | 0.002 ** | 0.003 * | [-0.001, 0.011] | ** → * |
| Rentals (9) | Near bin x log(Articles) | -0.002 | 0.001 ** | 0.001 ** | [-0.004, 0.000] |  |
| Rentals (10) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (11) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (12) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_extensive_250_vs_250_1000_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.060 | 0.015 *** | 0.016 *** | [-0.091, -0.029] |  |
| Sales (2) | Near bin | -0.026 | 0.015 * | 0.017  | [-0.059, 0.007] | * → none |
| Sales (3) | Near bin | -0.020 | 0.008 ** | 0.009 ** | [-0.038, -0.002] |  |
| Sales (4) | Near bin | 0.023 | 0.007 *** | 0.008 *** | [0.007, 0.039] |  |
| Sales (5) | Near bin | -0.011 | 0.008  | 0.008  | [-0.027, 0.005] |  |
| Sales (6) | Near bin | 0.019 | 0.007 *** | 0.007 *** | [0.005, 0.033] |  |
| Rentals (7) | Near bin | -0.089 | 0.019 *** | 0.024 *** | [-0.136, -0.042] |  |
| Rentals (8) | Near bin | -0.079 | 0.017 *** | 0.021 *** | [-0.120, -0.038] |  |
| Rentals (9) | Near bin | 0.004 | 0.007  | 0.008  | [-0.012, 0.020] |  |
| Rentals (10) | Near bin | 0.021 | 0.006 *** | 0.006 *** | [0.009, 0.033] |  |
| Rentals (11) | Near bin | 0.002 | 0.007  | 0.007  | [-0.012, 0.016] |  |
| Rentals (12) | Near bin | 0.015 | 0.005 *** | 0.006 *** | [0.003, 0.027] |  |
| Sales (1) | log (Articles_3m) | 0.006 | 0.001 *** | 0.002 *** | [0.002, 0.010] |  |
| Sales (2) | log (Articles_3m) | 0.015 | 0.001 *** | 0.001 *** | [0.013, 0.017] |  |
| Rentals (7) | log (Articles_3m) | 0.069 | 0.002 *** | 0.002 *** | [0.065, 0.073] |  |
| Rentals (8) | log (Articles_3m) | 0.059 | 0.001 *** | 0.002 *** | [0.055, 0.063] |  |
| Sales (1) | Near bin x log (Articles_3m) | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Sales (2) | Near bin x log (Articles_3m) | 0.000 | 0.003  | 0.003  | [-0.006, 0.006] |  |
| Sales (3) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (4) | Near bin x log (Articles_3m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (5) | Near bin x log (Articles_3m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (6) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_3m) | 0.006 | 0.003  | 0.004  | [-0.002, 0.014] |  |
| Rentals (8) | Near bin x log (Articles_3m) | 0.006 | 0.003 ** | 0.004 * | [-0.002, 0.014] | ** → * |
| Rentals (9) | Near bin x log (Articles_3m) | -0.003 | 0.002 * | 0.002 * | [-0.007, 0.001] |  |
| Rentals (10) | Near bin x log (Articles_3m) | -0.002 | 0.001  | 0.001  | [-0.004, 0.000] |  |
| Rentals (11) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (12) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

`did_articles_windowed_prior_extensive_250_vs_250_1000_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.066 | 0.020 *** | 0.021 *** | [-0.107, -0.025] |  |
| Sales (2) | Near bin | -0.034 | 0.020 * | 0.021  | [-0.075, 0.007] | * → none |
| Sales (3) | Near bin | -0.017 | 0.011  | 0.011  | [-0.039, 0.005] |  |
| Sales (4) | Near bin | 0.024 | 0.009 ** | 0.010 ** | [0.004, 0.044] |  |
| Sales (5) | Near bin | -0.008 | 0.010  | 0.010  | [-0.028, 0.012] |  |
| Sales (6) | Near bin | 0.021 | 0.009 ** | 0.009 ** | [0.003, 0.039] |  |
| Rentals (7) | Near bin | -0.087 | 0.023 *** | 0.029 *** | [-0.144, -0.030] |  |
| Rentals (8) | Near bin | -0.089 | 0.021 *** | 0.025 *** | [-0.138, -0.040] |  |
| Rentals (9) | Near bin | 0.012 | 0.010  | 0.010  | [-0.008, 0.032] |  |
| Rentals (10) | Near bin | 0.019 | 0.007 *** | 0.008 ** | [0.003, 0.035] | *** → ** |
| Rentals (11) | Near bin | 0.005 | 0.010  | 0.010  | [-0.015, 0.025] |  |
| Rentals (12) | Near bin | 0.012 | 0.007  | 0.008  | [-0.004, 0.028] |  |
| Sales (1) | log (Articles_6m) | 0.010 | 0.002 *** | 0.002 *** | [0.006, 0.014] |  |
| Sales (2) | log (Articles_6m) | 0.019 | 0.002 *** | 0.002 *** | [0.015, 0.023] |  |
| Rentals (7) | log (Articles_6m) | 0.091 | 0.002 *** | 0.003 *** | [0.085, 0.097] |  |
| Rentals (8) | log (Articles_6m) | 0.077 | 0.002 *** | 0.002 *** | [0.073, 0.081] |  |
| Sales (1) | Near bin x log (Articles_6m) | 0.001 | 0.004  | 0.004  | [-0.007, 0.009] |  |
| Sales (2) | Near bin x log (Articles_6m) | 0.002 | 0.004  | 0.004  | [-0.006, 0.010] |  |
| Sales (3) | Near bin x log (Articles_6m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (4) | Near bin x log (Articles_6m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (5) | Near bin x log (Articles_6m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (6) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_6m) | 0.004 | 0.004  | 0.005  | [-0.006, 0.014] |  |
| Rentals (8) | Near bin x log (Articles_6m) | 0.008 | 0.004 ** | 0.004 * | [0.000, 0.016] | ** → * |
| Rentals (9) | Near bin x log (Articles_6m) | -0.005 | 0.002 ** | 0.002 ** | [-0.009, -0.001] |  |
| Rentals (10) | Near bin x log (Articles_6m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (12) | Near bin x log (Articles_6m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |

`did_articles_windowed_prior_extensive_250_vs_250_1000_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.069 | 0.022 *** | 0.023 *** | [-0.114, -0.024] |  |
| Sales (2) | Near bin | -0.038 | 0.022 * | 0.023  | [-0.083, 0.007] | * → none |
| Sales (3) | Near bin | -0.019 | 0.011 * | 0.012  | [-0.043, 0.005] | * → none |
| Sales (4) | Near bin | 0.023 | 0.010 ** | 0.011 ** | [0.001, 0.045] |  |
| Sales (5) | Near bin | -0.008 | 0.011  | 0.011  | [-0.030, 0.014] |  |
| Sales (6) | Near bin | 0.021 | 0.009 ** | 0.010 ** | [0.001, 0.041] |  |
| Rentals (7) | Near bin | -0.096 | 0.025 *** | 0.031 *** | [-0.157, -0.035] |  |
| Rentals (8) | Near bin | -0.092 | 0.022 *** | 0.027 *** | [-0.145, -0.039] |  |
| Rentals (9) | Near bin | 0.012 | 0.010  | 0.011  | [-0.010, 0.034] |  |
| Rentals (10) | Near bin | 0.019 | 0.008 ** | 0.009 ** | [0.001, 0.037] |  |
| Rentals (11) | Near bin | 0.005 | 0.010  | 0.010  | [-0.015, 0.025] |  |
| Rentals (12) | Near bin | 0.011 | 0.008  | 0.009  | [-0.007, 0.029] |  |
| Sales (1) | log (Articles_12m) | 0.013 | 0.002 *** | 0.002 *** | [0.009, 0.017] |  |
| Sales (2) | log (Articles_12m) | 0.021 | 0.001 *** | 0.002 *** | [0.017, 0.025] |  |
| Rentals (7) | log (Articles_12m) | 0.091 | 0.002 *** | 0.003 *** | [0.085, 0.097] |  |
| Rentals (8) | log (Articles_12m) | 0.080 | 0.002 *** | 0.002 *** | [0.076, 0.084] |  |
| Sales (1) | Near bin x log (Articles_12m) | 0.002 | 0.004  | 0.004  | [-0.006, 0.010] |  |
| Sales (2) | Near bin x log (Articles_12m) | 0.002 | 0.004  | 0.004  | [-0.006, 0.010] |  |
| Sales (3) | Near bin x log (Articles_12m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (4) | Near bin x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (5) | Near bin x log (Articles_12m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (6) | Near bin x log (Articles_12m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_12m) | 0.006 | 0.004  | 0.005  | [-0.004, 0.016] |  |
| Rentals (8) | Near bin x log (Articles_12m) | 0.007 | 0.004 ** | 0.004 * | [-0.001, 0.015] | ** → * |
| Rentals (9) | Near bin x log (Articles_12m) | -0.004 | 0.002 ** | 0.002 ** | [-0.008, 0.000] |  |
| Rentals (10) | Near bin x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_12m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (12) | Near bin x log (Articles_12m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |

`did_articles_windowed_prior_extensive_250_vs_500_1000_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.089 | 0.015 *** | 0.017 *** | [-0.122, -0.056] |  |
| Sales (2) | Near bin | -0.049 | 0.015 *** | 0.018 *** | [-0.084, -0.014] |  |
| Sales (3) | Near bin | -0.038 | 0.007 *** | 0.008 *** | [-0.054, -0.022] |  |
| Sales (4) | Near bin | 0.021 | 0.006 *** | 0.007 *** | [0.007, 0.035] |  |
| Sales (5) | Near bin | -0.032 | 0.008 *** | 0.008 *** | [-0.048, -0.016] |  |
| Sales (6) | Near bin | 0.012 | 0.007 * | 0.007 * | [-0.002, 0.026] |  |
| Rentals (7) | Near bin | -0.112 | 0.020 *** | 0.027 *** | [-0.165, -0.059] |  |
| Rentals (8) | Near bin | -0.103 | 0.018 *** | 0.024 *** | [-0.150, -0.056] |  |
| Rentals (9) | Near bin | -0.007 | 0.007  | 0.008  | [-0.023, 0.009] |  |
| Rentals (10) | Near bin | 0.016 | 0.006 *** | 0.007 ** | [0.002, 0.030] | *** → ** |
| Rentals (11) | Near bin | -0.012 | 0.008  | 0.009  | [-0.030, 0.006] |  |
| Rentals (12) | Near bin | 0.012 | 0.007 * | 0.007  | [-0.002, 0.026] | * → none |
| Sales (1) | log(Articles) | 0.007 | 0.001 *** | 0.001 *** | [0.005, 0.009] |  |
| Sales (2) | log(Articles) | 0.012 | 0.001 *** | 0.001 *** | [0.010, 0.014] |  |
| Rentals (7) | log(Articles) | 0.048 | 0.001 *** | 0.002 *** | [0.044, 0.052] |  |
| Rentals (8) | log(Articles) | 0.042 | 0.001 *** | 0.001 *** | [0.040, 0.044] |  |
| Sales (1) | Near bin x log(Articles) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Sales (2) | Near bin x log(Articles) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Sales (3) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (5) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (7) | Near bin x log(Articles) | 0.004 | 0.003  | 0.003  | [-0.002, 0.010] |  |
| Rentals (8) | Near bin x log(Articles) | 0.006 | 0.002 *** | 0.003 ** | [0.000, 0.012] | *** → ** |
| Rentals (9) | Near bin x log(Articles) | -0.002 | 0.001 ** | 0.001 * | [-0.004, 0.000] | ** → * |
| Rentals (10) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (12) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_extensive_250_vs_500_1000_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.082 | 0.016 *** | 0.018 *** | [-0.117, -0.047] |  |
| Sales (2) | Near bin | -0.041 | 0.016 *** | 0.018 ** | [-0.076, -0.006] | *** → ** |
| Sales (3) | Near bin | -0.033 | 0.009 *** | 0.009 *** | [-0.051, -0.015] |  |
| Sales (4) | Near bin | 0.023 | 0.008 *** | 0.008 *** | [0.007, 0.039] |  |
| Sales (5) | Near bin | -0.028 | 0.009 *** | 0.009 *** | [-0.046, -0.010] |  |
| Sales (6) | Near bin | 0.015 | 0.008 * | 0.008 * | [-0.001, 0.031] |  |
| Rentals (7) | Near bin | -0.112 | 0.021 *** | 0.028 *** | [-0.167, -0.057] |  |
| Rentals (8) | Near bin | -0.101 | 0.018 *** | 0.024 *** | [-0.148, -0.054] |  |
| Rentals (9) | Near bin | -0.006 | 0.008  | 0.009  | [-0.024, 0.012] |  |
| Rentals (10) | Near bin | 0.020 | 0.006 *** | 0.007 *** | [0.006, 0.034] |  |
| Rentals (11) | Near bin | -0.010 | 0.009  | 0.010  | [-0.030, 0.010] |  |
| Rentals (12) | Near bin | 0.015 | 0.007 ** | 0.008 ** | [-0.001, 0.031] |  |
| Sales (1) | log (Articles_3m) | 0.005 | 0.002 *** | 0.002 *** | [0.001, 0.009] |  |
| Sales (2) | log (Articles_3m) | 0.014 | 0.002 *** | 0.002 *** | [0.010, 0.018] |  |
| Rentals (7) | log (Articles_3m) | 0.068 | 0.002 *** | 0.003 *** | [0.062, 0.074] |  |
| Rentals (8) | log (Articles_3m) | 0.058 | 0.001 *** | 0.002 *** | [0.054, 0.062] |  |
| Sales (1) | Near bin x log (Articles_3m) | 0.000 | 0.004  | 0.004  | [-0.008, 0.008] |  |
| Sales (2) | Near bin x log (Articles_3m) | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Sales (3) | Near bin x log (Articles_3m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (4) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (5) | Near bin x log (Articles_3m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (6) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_3m) | 0.006 | 0.004  | 0.005  | [-0.004, 0.016] |  |
| Rentals (8) | Near bin x log (Articles_3m) | 0.008 | 0.003 ** | 0.004 ** | [0.000, 0.016] |  |
| Rentals (9) | Near bin x log (Articles_3m) | -0.004 | 0.002 * | 0.002 * | [-0.008, 0.000] |  |
| Rentals (10) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (11) | Near bin x log (Articles_3m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (12) | Near bin x log (Articles_3m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |

`did_articles_windowed_prior_extensive_250_vs_500_1000_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.089 | 0.021 *** | 0.023 *** | [-0.134, -0.044] |  |
| Sales (2) | Near bin | -0.050 | 0.021 ** | 0.023 ** | [-0.095, -0.005] |  |
| Sales (3) | Near bin | -0.030 | 0.011 *** | 0.012 *** | [-0.054, -0.006] |  |
| Sales (4) | Near bin | 0.024 | 0.010 ** | 0.010 ** | [0.004, 0.044] |  |
| Sales (5) | Near bin | -0.026 | 0.011 ** | 0.011 ** | [-0.048, -0.004] |  |
| Sales (6) | Near bin | 0.016 | 0.009 * | 0.009 * | [-0.002, 0.034] |  |
| Rentals (7) | Near bin | -0.113 | 0.026 *** | 0.033 *** | [-0.178, -0.048] |  |
| Rentals (8) | Near bin | -0.116 | 0.023 *** | 0.029 *** | [-0.173, -0.059] |  |
| Rentals (9) | Near bin | 0.001 | 0.010  | 0.012  | [-0.023, 0.025] |  |
| Rentals (10) | Near bin | 0.017 | 0.008 ** | 0.009 * | [-0.001, 0.035] | ** → * |
| Rentals (11) | Near bin | -0.008 | 0.011  | 0.012  | [-0.032, 0.016] |  |
| Rentals (12) | Near bin | 0.011 | 0.009  | 0.010  | [-0.009, 0.031] |  |
| Sales (1) | log (Articles_6m) | 0.008 | 0.002 *** | 0.002 *** | [0.004, 0.012] |  |
| Sales (2) | log (Articles_6m) | 0.018 | 0.002 *** | 0.002 *** | [0.014, 0.022] |  |
| Rentals (7) | log (Articles_6m) | 0.090 | 0.002 *** | 0.004 *** | [0.082, 0.098] |  |
| Rentals (8) | log (Articles_6m) | 0.075 | 0.002 *** | 0.003 *** | [0.069, 0.081] |  |
| Sales (1) | Near bin x log (Articles_6m) | 0.002 | 0.004  | 0.004  | [-0.006, 0.010] |  |
| Sales (2) | Near bin x log (Articles_6m) | 0.002 | 0.004  | 0.004  | [-0.006, 0.010] |  |
| Sales (3) | Near bin x log (Articles_6m) | -0.003 | 0.002  | 0.003  | [-0.009, 0.003] |  |
| Sales (4) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (5) | Near bin x log (Articles_6m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (6) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_6m) | 0.005 | 0.005  | 0.006  | [-0.007, 0.017] |  |
| Rentals (8) | Near bin x log (Articles_6m) | 0.010 | 0.004 ** | 0.005 ** | [0.000, 0.020] |  |
| Rentals (9) | Near bin x log (Articles_6m) | -0.005 | 0.002 ** | 0.003 * | [-0.011, 0.001] | ** → * |
| Rentals (10) | Near bin x log (Articles_6m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_6m) | -0.002 | 0.002  | 0.003  | [-0.008, 0.004] |  |
| Rentals (12) | Near bin x log (Articles_6m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |

`did_articles_windowed_prior_extensive_250_vs_500_1000_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.094 | 0.023 *** | 0.025 *** | [-0.143, -0.045] |  |
| Sales (2) | Near bin | -0.055 | 0.022 ** | 0.025 ** | [-0.104, -0.006] |  |
| Sales (3) | Near bin | -0.033 | 0.012 *** | 0.013 *** | [-0.058, -0.008] |  |
| Sales (4) | Near bin | 0.023 | 0.010 ** | 0.011 ** | [0.001, 0.045] |  |
| Sales (5) | Near bin | -0.027 | 0.011 ** | 0.011 ** | [-0.049, -0.005] |  |
| Sales (6) | Near bin | 0.015 | 0.010  | 0.010  | [-0.005, 0.035] |  |
| Rentals (7) | Near bin | -0.123 | 0.027 *** | 0.034 *** | [-0.190, -0.056] |  |
| Rentals (8) | Near bin | -0.118 | 0.024 *** | 0.030 *** | [-0.177, -0.059] |  |
| Rentals (9) | Near bin | 0.002 | 0.011  | 0.013  | [-0.023, 0.027] |  |
| Rentals (10) | Near bin | 0.018 | 0.009 * | 0.010 * | [-0.002, 0.038] |  |
| Rentals (11) | Near bin | -0.007 | 0.012  | 0.013  | [-0.032, 0.018] |  |
| Rentals (12) | Near bin | 0.012 | 0.010  | 0.011  | [-0.010, 0.034] |  |
| Sales (1) | log (Articles_12m) | 0.012 | 0.002 *** | 0.002 *** | [0.008, 0.016] |  |
| Sales (2) | log (Articles_12m) | 0.020 | 0.002 *** | 0.002 *** | [0.016, 0.024] |  |
| Rentals (7) | log (Articles_12m) | 0.090 | 0.002 *** | 0.003 *** | [0.084, 0.096] |  |
| Rentals (8) | log (Articles_12m) | 0.078 | 0.002 *** | 0.002 *** | [0.074, 0.082] |  |
| Sales (1) | Near bin x log (Articles_12m) | 0.003 | 0.004  | 0.004  | [-0.005, 0.011] |  |
| Sales (2) | Near bin x log (Articles_12m) | 0.003 | 0.004  | 0.004  | [-0.005, 0.011] |  |
| Sales (3) | Near bin x log (Articles_12m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (4) | Near bin x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (5) | Near bin x log (Articles_12m) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Sales (6) | Near bin x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (7) | Near bin x log (Articles_12m) | 0.007 | 0.005  | 0.005  | [-0.003, 0.017] |  |
| Rentals (8) | Near bin x log (Articles_12m) | 0.009 | 0.004 ** | 0.005 * | [-0.001, 0.019] | ** → * |
| Rentals (9) | Near bin x log (Articles_12m) | -0.004 | 0.002 * | 0.002 * | [-0.008, 0.000] |  |
| Rentals (10) | Near bin x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_12m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Rentals (12) | Near bin x log (Articles_12m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |

`did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.144 | 0.009 *** | 0.013 *** | [-0.169, -0.119] |  |
| Sales (2) | Near bin | -0.106 | 0.008 *** | 0.013 *** | [-0.131, -0.081] |  |
| Sales (3) | Near bin | -0.042 | 0.004 *** | 0.005 *** | [-0.052, -0.032] |  |
| Sales (4) | Near bin | -0.002 | 0.004  | 0.004  | [-0.010, 0.006] |  |
| Sales (5) | Near bin | -0.031 | 0.004 *** | 0.005 *** | [-0.041, -0.021] |  |
| Sales (6) | Near bin | -0.008 | 0.004 ** | 0.004 ** | [-0.016, 0.000] |  |
| Rentals (7) | Near bin | -0.164 | 0.014 *** | 0.029 *** | [-0.221, -0.107] |  |
| Rentals (8) | Near bin | -0.146 | 0.012 *** | 0.025 *** | [-0.195, -0.097] |  |
| Rentals (9) | Near bin | -0.004 | 0.005  | 0.006  | [-0.016, 0.008] |  |
| Rentals (10) | Near bin | 0.010 | 0.004 *** | 0.005 * | [0.000, 0.020] | *** → * |
| Rentals (11) | Near bin | -0.004 | 0.005  | 0.006  | [-0.016, 0.008] |  |
| Rentals (12) | Near bin | 0.006 | 0.004 * | 0.005  | [-0.004, 0.016] | * → none |
| Sales (1) | log(Articles) | 0.007 | 0.001 *** | 0.001 *** | [0.005, 0.009] |  |
| Sales (2) | log(Articles) | 0.011 | 0.001 *** | 0.001 *** | [0.009, 0.013] |  |
| Rentals (7) | log(Articles) | 0.047 | 0.001 *** | 0.001 *** | [0.045, 0.049] |  |
| Rentals (8) | log(Articles) | 0.041 | 0.001 *** | 0.001 *** | [0.039, 0.043] |  |
| Sales (1) | Near bin x log(Articles) | 0.002 | 0.001 * | 0.001  | [0.000, 0.004] | * → none |
| Sales (2) | Near bin x log(Articles) | 0.002 | 0.001 * | 0.001 * | [0.000, 0.004] |  |
| Sales (3) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (7) | Near bin x log(Articles) | 0.005 | 0.002 ** | 0.003 * | [-0.001, 0.011] | ** → * |
| Rentals (8) | Near bin x log(Articles) | 0.006 | 0.002 *** | 0.002 *** | [0.002, 0.010] |  |
| Rentals (9) | Near bin x log(Articles) | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (10) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (11) | Near bin x log(Articles) | -0.002 | 0.001 *** | 0.001 * | [-0.004, 0.000] | *** → * |
| Rentals (12) | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

`did_articles_windowed_prior_extensive_500_vs_500_2000_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.143 | 0.009 *** | 0.013 *** | [-0.168, -0.118] |  |
| Sales (2) | Near bin | -0.104 | 0.009 *** | 0.014 *** | [-0.131, -0.077] |  |
| Sales (3) | Near bin | -0.040 | 0.005 *** | 0.005 *** | [-0.050, -0.030] |  |
| Sales (4) | Near bin | -0.001 | 0.005  | 0.005  | [-0.011, 0.009] |  |
| Sales (5) | Near bin | -0.029 | 0.005 *** | 0.005 *** | [-0.039, -0.019] |  |
| Sales (6) | Near bin | -0.007 | 0.004  | 0.005  | [-0.017, 0.003] |  |
| Rentals (7) | Near bin | -0.162 | 0.014 *** | 0.029 *** | [-0.219, -0.105] |  |
| Rentals (8) | Near bin | -0.144 | 0.012 *** | 0.025 *** | [-0.193, -0.095] |  |
| Rentals (9) | Near bin | -0.004 | 0.005  | 0.007  | [-0.018, 0.010] |  |
| Rentals (10) | Near bin | 0.010 | 0.004 *** | 0.005 * | [0.000, 0.020] | *** → * |
| Rentals (11) | Near bin | -0.003 | 0.005  | 0.007  | [-0.017, 0.011] |  |
| Rentals (12) | Near bin | 0.007 | 0.004 * | 0.005  | [-0.003, 0.017] | * → none |
| Sales (1) | log (Articles_3m) | 0.005 | 0.001 *** | 0.001 *** | [0.003, 0.007] |  |
| Sales (2) | log (Articles_3m) | 0.013 | 0.001 *** | 0.002 *** | [0.009, 0.017] |  |
| Rentals (7) | log (Articles_3m) | 0.065 | 0.001 *** | 0.002 *** | [0.061, 0.069] |  |
| Rentals (8) | log (Articles_3m) | 0.056 | 0.001 *** | 0.002 *** | [0.052, 0.060] |  |
| Sales (1) | Near bin x log (Articles_3m) | 0.003 | 0.002  | 0.003  | [-0.003, 0.009] |  |
| Sales (2) | Near bin x log (Articles_3m) | 0.003 | 0.002  | 0.003  | [-0.003, 0.009] |  |
| Sales (3) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Near bin x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Near bin x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_3m) | 0.006 | 0.003 ** | 0.004  | [-0.002, 0.014] | ** → none |
| Rentals (8) | Near bin x log (Articles_3m) | 0.007 | 0.002 *** | 0.003 *** | [0.001, 0.013] |  |
| Rentals (9) | Near bin x log (Articles_3m) | -0.004 | 0.001 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (10) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_3m) | -0.003 | 0.001 ** | 0.002 * | [-0.007, 0.001] | ** → * |
| Rentals (12) | Near bin x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

`did_articles_windowed_prior_extensive_500_vs_500_2000_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.146 | 0.011 *** | 0.015 *** | [-0.175, -0.117] |  |
| Sales (2) | Near bin | -0.109 | 0.011 *** | 0.016 *** | [-0.140, -0.078] |  |
| Sales (3) | Near bin | -0.038 | 0.007 *** | 0.007 *** | [-0.052, -0.024] |  |
| Sales (4) | Near bin | -0.001 | 0.006  | 0.006  | [-0.013, 0.011] |  |
| Sales (5) | Near bin | -0.028 | 0.006 *** | 0.006 *** | [-0.040, -0.016] |  |
| Sales (6) | Near bin | -0.007 | 0.006  | 0.006  | [-0.019, 0.005] |  |
| Rentals (7) | Near bin | -0.177 | 0.017 *** | 0.035 *** | [-0.246, -0.108] |  |
| Rentals (8) | Near bin | -0.163 | 0.015 *** | 0.029 *** | [-0.220, -0.106] |  |
| Rentals (9) | Near bin | 0.000 | 0.007  | 0.009  | [-0.018, 0.018] |  |
| Rentals (10) | Near bin | 0.010 | 0.005 * | 0.008  | [-0.006, 0.026] | * → none |
| Rentals (11) | Near bin | -0.001 | 0.007  | 0.009  | [-0.019, 0.017] |  |
| Rentals (12) | Near bin | 0.006 | 0.005  | 0.008  | [-0.010, 0.022] |  |
| Sales (1) | log (Articles_6m) | 0.009 | 0.001 *** | 0.002 *** | [0.005, 0.013] |  |
| Sales (2) | log (Articles_6m) | 0.017 | 0.001 *** | 0.002 *** | [0.013, 0.021] |  |
| Rentals (7) | log (Articles_6m) | 0.086 | 0.001 *** | 0.003 *** | [0.080, 0.092] |  |
| Rentals (8) | log (Articles_6m) | 0.072 | 0.001 *** | 0.002 *** | [0.068, 0.076] |  |
| Sales (1) | Near bin x log (Articles_6m) | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| Sales (2) | Near bin x log (Articles_6m) | 0.004 | 0.003  | 0.003  | [-0.002, 0.010] |  |
| Sales (3) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (4) | Near bin x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Near bin x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (7) | Near bin x log (Articles_6m) | 0.008 | 0.004 ** | 0.005 * | [-0.002, 0.018] | ** → * |
| Rentals (8) | Near bin x log (Articles_6m) | 0.011 | 0.003 *** | 0.004 *** | [0.003, 0.019] |  |
| Rentals (9) | Near bin x log (Articles_6m) | -0.004 | 0.002 *** | 0.002 * | [-0.008, 0.000] | *** → * |
| Rentals (10) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_6m) | -0.003 | 0.001 ** | 0.002  | [-0.007, 0.001] | ** → none |
| Rentals (12) | Near bin x log (Articles_6m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |

`did_articles_windowed_prior_extensive_500_vs_500_2000_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.150 | 0.012 *** | 0.016 *** | [-0.181, -0.119] |  |
| Sales (2) | Near bin | -0.114 | 0.012 *** | 0.016 *** | [-0.145, -0.083] |  |
| Sales (3) | Near bin | -0.041 | 0.007 *** | 0.007 *** | [-0.055, -0.027] |  |
| Sales (4) | Near bin | -0.003 | 0.006  | 0.007  | [-0.017, 0.011] |  |
| Sales (5) | Near bin | -0.030 | 0.006 *** | 0.007 *** | [-0.044, -0.016] |  |
| Sales (6) | Near bin | -0.009 | 0.006  | 0.006  | [-0.021, 0.003] |  |
| Rentals (7) | Near bin | -0.180 | 0.019 *** | 0.035 *** | [-0.249, -0.111] |  |
| Rentals (8) | Near bin | -0.165 | 0.016 *** | 0.029 *** | [-0.222, -0.108] |  |
| Rentals (9) | Near bin | 0.006 | 0.007  | 0.010  | [-0.014, 0.026] |  |
| Rentals (10) | Near bin | 0.013 | 0.006 ** | 0.009  | [-0.005, 0.031] | ** → none |
| Rentals (11) | Near bin | 0.005 | 0.007  | 0.010  | [-0.015, 0.025] |  |
| Rentals (12) | Near bin | 0.009 | 0.006 * | 0.009  | [-0.009, 0.027] | * → none |
| Sales (1) | log (Articles_12m) | 0.012 | 0.001 *** | 0.002 *** | [0.008, 0.016] |  |
| Sales (2) | log (Articles_12m) | 0.019 | 0.001 *** | 0.002 *** | [0.015, 0.023] |  |
| Rentals (7) | log (Articles_12m) | 0.087 | 0.001 *** | 0.002 *** | [0.083, 0.091] |  |
| Rentals (8) | log (Articles_12m) | 0.076 | 0.001 *** | 0.002 *** | [0.072, 0.080] |  |
| Sales (1) | Near bin x log (Articles_12m) | 0.003 | 0.002  | 0.003  | [-0.003, 0.009] |  |
| Sales (2) | Near bin x log (Articles_12m) | 0.004 | 0.002 * | 0.003  | [-0.002, 0.010] | * → none |
| Sales (3) | Near bin x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Near bin x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (5) | Near bin x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Near bin x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Near bin x log (Articles_12m) | 0.008 | 0.004 ** | 0.005 * | [-0.002, 0.018] | ** → * |
| Rentals (8) | Near bin x log (Articles_12m) | 0.010 | 0.003 *** | 0.004 *** | [0.002, 0.018] |  |
| Rentals (9) | Near bin x log (Articles_12m) | -0.005 | 0.001 *** | 0.002 ** | [-0.009, -0.001] | *** → ** |
| Rentals (10) | Near bin x log (Articles_12m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Rentals (11) | Near bin x log (Articles_12m) | -0.004 | 0.001 *** | 0.002 * | [-0.008, 0.000] | *** → * |
| Rentals (12) | Near bin x log (Articles_12m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |

## Windowed articles, intensive margin, by radius and window

The legacy copies `did_articles_windowed_prior_{3m,6m,12m}.tex` are byte-identical to the 250 m files below and are not repeated. The cumulative tables are the same models as `did_articles_prior_<R>m.tex`.

`did_articles_windowed_prior_250m_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.009 | 0.011  | 0.011  | [-0.013, 0.031] |  |
| Sales (2) | Spills per week (avg.) | 0.009 | 0.011  | 0.012  | [-0.015, 0.033] |  |
| Sales (3) | Spills per week (avg.) | 0.008 | 0.007  | 0.006  | [-0.004, 0.020] |  |
| Sales (4) | Spills per week (avg.) | 0.009 | 0.006  | 0.006 * | [-0.003, 0.021] | none → * |
| Sales (5) | Spills per week (avg.) | 0.005 | 0.006  | 0.006  | [-0.007, 0.017] |  |
| Sales (6) | Spills per week (avg.) | 0.006 | 0.006  | 0.005  | [-0.004, 0.016] |  |
| Rentals (7) | Spills per week (avg.) | 0.006 | 0.015  | 0.023  | [-0.039, 0.051] |  |
| Rentals (8) | Spills per week (avg.) | 0.003 | 0.013  | 0.020  | [-0.036, 0.042] |  |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.005  | 0.005  | [-0.016, 0.004] |  |
| Rentals (10) | Spills per week (avg.) | -0.003 | 0.004  | 0.004  | [-0.011, 0.005] |  |
| Rentals (11) | Spills per week (avg.) | -0.008 | 0.005  | 0.006  | [-0.020, 0.004] |  |
| Rentals (12) | Spills per week (avg.) | -0.008 | 0.004 ** | 0.004 * | [-0.016, 0.000] | ** → * |
| Sales (1) | log(Articles) | 0.012 | 0.004 *** | 0.004 *** | [0.004, 0.020] |  |
| Sales (2) | log(Articles) | 0.016 | 0.003 *** | 0.004 *** | [0.008, 0.024] |  |
| Rentals (7) | log(Articles) | 0.062 | 0.004 *** | 0.005 *** | [0.052, 0.072] |  |
| Rentals (8) | log(Articles) | 0.055 | 0.004 *** | 0.004 *** | [0.047, 0.063] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.011 | 0.003 *** | 0.004 *** | [-0.019, -0.003] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.003 *** | 0.003 ** | [-0.013, -0.001] | *** → ** |
| Sales (3) | Spills per week (avg.) x log(Articles) | -0.004 | 0.002 *** | 0.002 *** | [-0.008, 0.000] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | -0.003 | 0.001 ** | 0.001 ** | [-0.005, -0.001] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | -0.003 | 0.002 * | 0.002 * | [-0.007, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.011 | 0.004 ** | 0.007  | [-0.025, 0.003] | ** → none |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.009 | 0.004 ** | 0.006  | [-0.021, 0.003] | ** → none |
| Rentals (9) | Spills per week (avg.) x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (11) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

`did_articles_windowed_prior_250m_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.011 | 0.012  | 0.013  | [-0.014, 0.036] |  |
| Sales (2) | Spills per week (avg.) | 0.009 | 0.012  | 0.013  | [-0.016, 0.034] |  |
| Sales (3) | Spills per week (avg.) | 0.007 | 0.008  | 0.008  | [-0.009, 0.023] |  |
| Sales (4) | Spills per week (avg.) | 0.008 | 0.007  | 0.007  | [-0.006, 0.022] |  |
| Sales (5) | Spills per week (avg.) | 0.005 | 0.008  | 0.007  | [-0.009, 0.019] |  |
| Sales (6) | Spills per week (avg.) | 0.005 | 0.007  | 0.007  | [-0.009, 0.019] |  |
| Rentals (7) | Spills per week (avg.) | -0.002 | 0.015  | 0.023  | [-0.047, 0.043] |  |
| Rentals (8) | Spills per week (avg.) | -0.006 | 0.013  | 0.020  | [-0.045, 0.033] |  |
| Rentals (9) | Spills per week (avg.) | -0.005 | 0.006  | 0.006  | [-0.017, 0.007] |  |
| Rentals (10) | Spills per week (avg.) | -0.003 | 0.004  | 0.005  | [-0.013, 0.007] |  |
| Rentals (11) | Spills per week (avg.) | -0.006 | 0.005  | 0.006  | [-0.018, 0.006] |  |
| Rentals (12) | Spills per week (avg.) | -0.007 | 0.004 * | 0.005  | [-0.017, 0.003] | * → none |
| Sales (1) | log (Articles_3m) | 0.010 | 0.005 * | 0.006 * | [-0.002, 0.022] |  |
| Sales (2) | log (Articles_3m) | 0.017 | 0.005 *** | 0.005 *** | [0.007, 0.027] |  |
| Rentals (7) | log (Articles_3m) | 0.079 | 0.006 *** | 0.006 *** | [0.067, 0.091] |  |
| Rentals (8) | log (Articles_3m) | 0.068 | 0.005 *** | 0.006 *** | [0.056, 0.080] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_3m) | -0.016 | 0.005 *** | 0.006 *** | [-0.028, -0.004] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_3m) | -0.010 | 0.004 ** | 0.005 * | [-0.020, 0.000] | ** → * |
| Sales (3) | Spills per week (avg.) x log (Articles_3m) | -0.005 | 0.003 ** | 0.003 * | [-0.011, 0.001] | ** → * |
| Sales (4) | Spills per week (avg.) x log (Articles_3m) | -0.003 | 0.002  | 0.002  | [-0.007, 0.001] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_3m) | -0.004 | 0.003  | 0.003  | [-0.010, 0.002] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_3m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_3m) | -0.013 | 0.006 ** | 0.010  | [-0.033, 0.007] | ** → none |
| Rentals (8) | Spills per week (avg.) x log (Articles_3m) | -0.011 | 0.005 ** | 0.009  | [-0.029, 0.007] | ** → none |
| Rentals (9) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_3m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_3m) | 0.002 | 0.001  | 0.002  | [-0.002, 0.006] |  |

`did_articles_windowed_prior_250m_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.036 | 0.019 * | 0.021 * | [-0.005, 0.077] |  |
| Sales (2) | Spills per week (avg.) | 0.026 | 0.019  | 0.021  | [-0.015, 0.067] |  |
| Sales (3) | Spills per week (avg.) | 0.018 | 0.011  | 0.011 * | [-0.004, 0.040] | none → * |
| Sales (4) | Spills per week (avg.) | 0.017 | 0.010 * | 0.009 * | [-0.001, 0.035] |  |
| Sales (5) | Spills per week (avg.) | 0.010 | 0.011  | 0.011  | [-0.012, 0.032] |  |
| Sales (6) | Spills per week (avg.) | 0.007 | 0.010  | 0.009  | [-0.011, 0.025] |  |
| Rentals (7) | Spills per week (avg.) | 0.018 | 0.026  | 0.042  | [-0.064, 0.100] |  |
| Rentals (8) | Spills per week (avg.) | 0.008 | 0.023  | 0.038  | [-0.066, 0.082] |  |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.009  | 0.009  | [-0.024, 0.012] |  |
| Rentals (10) | Spills per week (avg.) | -0.007 | 0.006  | 0.007  | [-0.021, 0.007] |  |
| Rentals (11) | Spills per week (avg.) | -0.010 | 0.009  | 0.010  | [-0.030, 0.010] |  |
| Rentals (12) | Spills per week (avg.) | -0.014 | 0.006 ** | 0.007 * | [-0.028, 0.000] | ** → * |
| Sales (1) | log (Articles_6m) | 0.016 | 0.007 ** | 0.007 ** | [0.002, 0.030] |  |
| Sales (2) | log (Articles_6m) | 0.024 | 0.006 *** | 0.007 *** | [0.010, 0.038] |  |
| Rentals (7) | log (Articles_6m) | 0.101 | 0.007 *** | 0.008 *** | [0.085, 0.117] |  |
| Rentals (8) | log (Articles_6m) | 0.086 | 0.006 *** | 0.007 *** | [0.072, 0.100] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_6m) | -0.019 | 0.006 *** | 0.007 *** | [-0.033, -0.005] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_6m) | -0.012 | 0.005 ** | 0.007 * | [-0.026, 0.002] | ** → * |
| Sales (3) | Spills per week (avg.) x log (Articles_6m) | -0.007 | 0.003 ** | 0.003 ** | [-0.013, -0.001] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_6m) | -0.005 | 0.003 * | 0.003 * | [-0.011, 0.001] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_6m) | -0.004 | 0.003  | 0.003  | [-0.010, 0.002] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_6m) | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_6m) | -0.016 | 0.008 * | 0.013  | [-0.041, 0.009] | * → none |
| Rentals (8) | Spills per week (avg.) x log (Articles_6m) | -0.012 | 0.007 * | 0.012  | [-0.036, 0.012] | * → none |
| Rentals (9) | Spills per week (avg.) x log (Articles_6m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_6m) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_6m) | 0.001 | 0.003  | 0.003  | [-0.005, 0.007] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_6m) | 0.003 | 0.002 * | 0.002  | [-0.001, 0.007] | * → none |

`did_articles_windowed_prior_250m_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.048 | 0.020 ** | 0.022 ** | [0.005, 0.091] |  |
| Sales (2) | Spills per week (avg.) | 0.036 | 0.019 * | 0.022 * | [-0.007, 0.079] |  |
| Sales (3) | Spills per week (avg.) | 0.023 | 0.011 ** | 0.011 ** | [0.001, 0.045] |  |
| Sales (4) | Spills per week (avg.) | 0.020 | 0.010 ** | 0.009 ** | [0.002, 0.038] |  |
| Sales (5) | Spills per week (avg.) | 0.015 | 0.011  | 0.011  | [-0.007, 0.037] |  |
| Sales (6) | Spills per week (avg.) | 0.009 | 0.010  | 0.009  | [-0.009, 0.027] |  |
| Rentals (7) | Spills per week (avg.) | 0.038 | 0.029  | 0.047  | [-0.054, 0.130] |  |
| Rentals (8) | Spills per week (avg.) | 0.028 | 0.025  | 0.042  | [-0.054, 0.110] |  |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.009  | 0.009  | [-0.024, 0.012] |  |
| Rentals (10) | Spills per week (avg.) | -0.006 | 0.007  | 0.007  | [-0.020, 0.008] |  |
| Rentals (11) | Spills per week (avg.) | -0.011 | 0.009  | 0.010  | [-0.031, 0.009] |  |
| Rentals (12) | Spills per week (avg.) | -0.014 | 0.006 ** | 0.008 * | [-0.030, 0.002] | ** → * |
| Sales (1) | log (Articles_12m) | 0.021 | 0.007 *** | 0.007 *** | [0.007, 0.035] |  |
| Sales (2) | log (Articles_12m) | 0.027 | 0.006 *** | 0.007 *** | [0.013, 0.041] |  |
| Rentals (7) | log (Articles_12m) | 0.105 | 0.007 *** | 0.008 *** | [0.089, 0.121] |  |
| Rentals (8) | log (Articles_12m) | 0.092 | 0.006 *** | 0.008 *** | [0.076, 0.108] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_12m) | -0.020 | 0.005 *** | 0.006 *** | [-0.032, -0.008] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_12m) | -0.013 | 0.005 *** | 0.006 ** | [-0.025, -0.001] | *** → ** |
| Sales (3) | Spills per week (avg.) x log (Articles_12m) | -0.008 | 0.003 *** | 0.003 *** | [-0.014, -0.002] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_12m) | -0.005 | 0.002 ** | 0.002 ** | [-0.009, -0.001] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_12m) | -0.005 | 0.003  | 0.003  | [-0.011, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_12m) | -0.002 | 0.003  | 0.002  | [-0.006, 0.002] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_12m) | -0.018 | 0.008 ** | 0.013  | [-0.043, 0.007] | ** → none |
| Rentals (8) | Spills per week (avg.) x log (Articles_12m) | -0.015 | 0.007 ** | 0.011  | [-0.037, 0.007] | ** → none |
| Rentals (9) | Spills per week (avg.) x log (Articles_12m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_12m) | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_12m) | 0.001 | 0.002  | 0.003  | [-0.005, 0.007] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_12m) | 0.003 | 0.002  | 0.002  | [-0.001, 0.007] |  |

`did_articles_windowed_prior_500m_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.010 | 0.007  | 0.009  | [-0.028, 0.008] |  |
| Sales (2) | Spills per week (avg.) | -0.009 | 0.007  | 0.008  | [-0.025, 0.007] |  |
| Sales (3) | Spills per week (avg.) | -0.005 | 0.004  | 0.004  | [-0.013, 0.003] |  |
| Sales (4) | Spills per week (avg.) | 0.000 | 0.003  | 0.004  | [-0.008, 0.008] |  |
| Sales (5) | Spills per week (avg.) | -0.007 | 0.003 * | 0.004 * | [-0.015, 0.001] |  |
| Sales (6) | Spills per week (avg.) | -0.002 | 0.003  | 0.003  | [-0.008, 0.004] |  |
| Rentals (7) | Spills per week (avg.) | 0.005 | 0.008  | 0.015  | [-0.024, 0.034] |  |
| Rentals (8) | Spills per week (avg.) | -0.001 | 0.007  | 0.012  | [-0.025, 0.023] |  |
| Rentals (9) | Spills per week (avg.) | -0.005 | 0.003  | 0.003  | [-0.011, 0.001] |  |
| Rentals (10) | Spills per week (avg.) | -0.004 | 0.002 * | 0.003  | [-0.010, 0.002] | * → none |
| Rentals (11) | Spills per week (avg.) | -0.005 | 0.003 * | 0.003  | [-0.011, 0.001] | * → none |
| Rentals (12) | Spills per week (avg.) | -0.005 | 0.002 ** | 0.003 * | [-0.011, 0.001] | ** → * |
| Sales (1) | log(Articles) | 0.013 | 0.002 *** | 0.002 *** | [0.009, 0.017] |  |
| Sales (2) | log(Articles) | 0.015 | 0.002 *** | 0.002 *** | [0.011, 0.019] |  |
| Rentals (7) | log(Articles) | 0.062 | 0.003 *** | 0.004 *** | [0.054, 0.070] |  |
| Rentals (8) | log(Articles) | 0.054 | 0.002 *** | 0.003 *** | [0.048, 0.060] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.011 | 0.002 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.002 *** | 0.002 *** | [-0.011, -0.003] |  |
| Sales (3) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.014 | 0.002 *** | 0.003 *** | [-0.020, -0.008] |  |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.010 | 0.002 *** | 0.003 *** | [-0.016, -0.004] |  |
| Rentals (9) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Rentals (11) | Spills per week (avg.) x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

`did_articles_windowed_prior_500m_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.006 | 0.008  | 0.009  | [-0.024, 0.012] |  |
| Sales (2) | Spills per week (avg.) | -0.008 | 0.007  | 0.009  | [-0.026, 0.010] |  |
| Sales (3) | Spills per week (avg.) | -0.004 | 0.004  | 0.004  | [-0.012, 0.004] |  |
| Sales (4) | Spills per week (avg.) | 0.001 | 0.004  | 0.004  | [-0.007, 0.009] |  |
| Sales (5) | Spills per week (avg.) | -0.006 | 0.004  | 0.004  | [-0.014, 0.002] |  |
| Sales (6) | Spills per week (avg.) | -0.001 | 0.004  | 0.003  | [-0.007, 0.005] |  |
| Rentals (7) | Spills per week (avg.) | -0.001 | 0.008  | 0.014  | [-0.028, 0.026] |  |
| Rentals (8) | Spills per week (avg.) | -0.007 | 0.007  | 0.012  | [-0.031, 0.017] |  |
| Rentals (9) | Spills per week (avg.) | -0.004 | 0.003  | 0.003  | [-0.010, 0.002] |  |
| Rentals (10) | Spills per week (avg.) | -0.004 | 0.002  | 0.003  | [-0.010, 0.002] |  |
| Rentals (11) | Spills per week (avg.) | -0.003 | 0.003  | 0.003  | [-0.009, 0.003] |  |
| Rentals (12) | Spills per week (avg.) | -0.004 | 0.002 * | 0.003  | [-0.010, 0.002] | * → none |
| Sales (1) | log (Articles_3m) | 0.012 | 0.003 *** | 0.003 *** | [0.006, 0.018] |  |
| Sales (2) | log (Articles_3m) | 0.017 | 0.003 *** | 0.003 *** | [0.011, 0.023] |  |
| Rentals (7) | log (Articles_3m) | 0.077 | 0.004 *** | 0.004 *** | [0.069, 0.085] |  |
| Rentals (8) | log (Articles_3m) | 0.065 | 0.003 *** | 0.004 *** | [0.057, 0.073] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_3m) | -0.016 | 0.003 *** | 0.004 *** | [-0.024, -0.008] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_3m) | -0.010 | 0.003 *** | 0.004 ** | [-0.018, -0.002] | *** → ** |
| Sales (3) | Spills per week (avg.) x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_3m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_3m) | -0.018 | 0.003 *** | 0.005 *** | [-0.028, -0.008] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_3m) | -0.013 | 0.002 *** | 0.004 *** | [-0.021, -0.005] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_3m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_3m) | 0.001 | 0.001 * | 0.001  | [-0.001, 0.003] | * → none |
| Rentals (11) | Spills per week (avg.) x log (Articles_3m) | -0.002 | 0.001 * | 0.001  | [-0.004, 0.000] | * → none |
| Rentals (12) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_500m_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.019 | 0.013  | 0.016  | [-0.012, 0.050] |  |
| Sales (2) | Spills per week (avg.) | 0.006 | 0.012  | 0.015  | [-0.023, 0.035] |  |
| Sales (3) | Spills per week (avg.) | -0.005 | 0.006  | 0.007  | [-0.019, 0.009] |  |
| Sales (4) | Spills per week (avg.) | -0.001 | 0.006  | 0.006  | [-0.013, 0.011] |  |
| Sales (5) | Spills per week (avg.) | -0.007 | 0.006  | 0.006  | [-0.019, 0.005] |  |
| Sales (6) | Spills per week (avg.) | -0.005 | 0.005  | 0.005  | [-0.015, 0.005] |  |
| Rentals (7) | Spills per week (avg.) | 0.033 | 0.013 ** | 0.023  | [-0.012, 0.078] | ** → none |
| Rentals (8) | Spills per week (avg.) | 0.015 | 0.012  | 0.020  | [-0.024, 0.054] |  |
| Rentals (9) | Spills per week (avg.) | -0.001 | 0.005  | 0.006  | [-0.013, 0.011] |  |
| Rentals (10) | Spills per week (avg.) | -0.007 | 0.004 * | 0.005  | [-0.017, 0.003] | * → none |
| Rentals (11) | Spills per week (avg.) | 0.000 | 0.005  | 0.006  | [-0.012, 0.012] |  |
| Rentals (12) | Spills per week (avg.) | -0.007 | 0.004 * | 0.005  | [-0.017, 0.003] | * → none |
| Sales (1) | log (Articles_6m) | 0.017 | 0.003 *** | 0.004 *** | [0.009, 0.025] |  |
| Sales (2) | log (Articles_6m) | 0.022 | 0.003 *** | 0.004 *** | [0.014, 0.030] |  |
| Rentals (7) | log (Articles_6m) | 0.102 | 0.004 *** | 0.006 *** | [0.090, 0.114] |  |
| Rentals (8) | log (Articles_6m) | 0.085 | 0.004 *** | 0.005 *** | [0.075, 0.095] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_6m) | -0.019 | 0.004 *** | 0.005 *** | [-0.029, -0.009] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_6m) | -0.011 | 0.003 *** | 0.005 ** | [-0.021, -0.001] | *** → ** |
| Sales (3) | Spills per week (avg.) x log (Articles_6m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_6m) | 0.001 | 0.001  | 0.002  | [-0.003, 0.005] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_6m) | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_6m) | 0.001 | 0.001  | 0.002  | [-0.003, 0.005] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_6m) | -0.023 | 0.004 *** | 0.006 *** | [-0.035, -0.011] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_6m) | -0.016 | 0.003 *** | 0.005 *** | [-0.026, -0.006] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_6m) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_6m) | 0.002 | 0.001 ** | 0.001  | [0.000, 0.004] | ** → none |
| Rentals (11) | Spills per week (avg.) x log (Articles_6m) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |
| Rentals (12) | Spills per week (avg.) x log (Articles_6m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

`did_articles_windowed_prior_500m_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.028 | 0.013 ** | 0.016 * | [-0.003, 0.059] | ** → * |
| Sales (2) | Spills per week (avg.) | 0.014 | 0.012  | 0.015  | [-0.015, 0.043] |  |
| Sales (3) | Spills per week (avg.) | -0.003 | 0.006  | 0.007  | [-0.017, 0.011] |  |
| Sales (4) | Spills per week (avg.) | -0.001 | 0.006  | 0.006  | [-0.013, 0.011] |  |
| Sales (5) | Spills per week (avg.) | -0.005 | 0.006  | 0.006  | [-0.017, 0.007] |  |
| Sales (6) | Spills per week (avg.) | -0.004 | 0.005  | 0.005  | [-0.014, 0.006] |  |
| Rentals (7) | Spills per week (avg.) | 0.050 | 0.014 *** | 0.025 ** | [0.001, 0.099] | *** → ** |
| Rentals (8) | Spills per week (avg.) | 0.030 | 0.013 ** | 0.021  | [-0.011, 0.071] | ** → none |
| Rentals (9) | Spills per week (avg.) | -0.001 | 0.005  | 0.006  | [-0.013, 0.011] |  |
| Rentals (10) | Spills per week (avg.) | -0.008 | 0.004 ** | 0.005  | [-0.018, 0.002] | ** → none |
| Rentals (11) | Spills per week (avg.) | 0.000 | 0.005  | 0.006  | [-0.012, 0.012] |  |
| Rentals (12) | Spills per week (avg.) | -0.008 | 0.004 ** | 0.005  | [-0.018, 0.002] | ** → none |
| Sales (1) | log (Articles_12m) | 0.021 | 0.003 *** | 0.004 *** | [0.013, 0.029] |  |
| Sales (2) | log (Articles_12m) | 0.025 | 0.003 *** | 0.004 *** | [0.017, 0.033] |  |
| Rentals (7) | log (Articles_12m) | 0.106 | 0.005 *** | 0.006 *** | [0.094, 0.118] |  |
| Rentals (8) | log (Articles_12m) | 0.091 | 0.004 *** | 0.006 *** | [0.079, 0.103] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_12m) | -0.019 | 0.003 *** | 0.005 *** | [-0.029, -0.009] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_12m) | -0.012 | 0.003 *** | 0.004 *** | [-0.020, -0.004] |  |
| Sales (3) | Spills per week (avg.) x log (Articles_12m) | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_12m) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_12m) | -0.024 | 0.004 *** | 0.006 *** | [-0.036, -0.012] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_12m) | -0.017 | 0.003 *** | 0.005 *** | [-0.027, -0.007] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_12m) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_12m) | 0.002 | 0.001 ** | 0.001  | [0.000, 0.004] | ** → none |
| Rentals (11) | Spills per week (avg.) x log (Articles_12m) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |

`did_articles_windowed_prior_1000m_cumulative.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.024 | 0.003 *** | 0.005 *** | [-0.034, -0.014] |  |
| Sales (2) | Spills per week (avg.) | -0.021 | 0.003 *** | 0.005 *** | [-0.031, -0.011] |  |
| Sales (3) | Spills per week (avg.) | -0.013 | 0.002 *** | 0.002 *** | [-0.017, -0.009] |  |
| Sales (4) | Spills per week (avg.) | -0.005 | 0.002 *** | 0.002 *** | [-0.009, -0.001] |  |
| Sales (5) | Spills per week (avg.) | -0.012 | 0.002 *** | 0.002 *** | [-0.016, -0.008] |  |
| Sales (6) | Spills per week (avg.) | -0.005 | 0.002 *** | 0.002 *** | [-0.009, -0.001] |  |
| Rentals (7) | Spills per week (avg.) | -0.004 | 0.004  | 0.010  | [-0.024, 0.016] |  |
| Rentals (8) | Spills per week (avg.) | -0.007 | 0.004 ** | 0.008  | [-0.023, 0.009] | ** → none |
| Rentals (9) | Spills per week (avg.) | -0.003 | 0.002 * | 0.002  | [-0.007, 0.001] | * → none |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.001 * | 0.002  | [-0.006, 0.002] | * → none |
| Rentals (11) | Spills per week (avg.) | -0.003 | 0.002  | 0.003  | [-0.009, 0.003] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Sales (1) | log(Articles) | 0.012 | 0.001 *** | 0.002 *** | [0.008, 0.016] |  |
| Sales (2) | log(Articles) | 0.014 | 0.001 *** | 0.002 *** | [0.010, 0.018] |  |
| Rentals (7) | log(Articles) | 0.064 | 0.002 *** | 0.003 *** | [0.058, 0.070] |  |
| Rentals (8) | log(Articles) | 0.053 | 0.002 *** | 0.003 *** | [0.047, 0.059] |  |
| Sales (1) | Spills per week (avg.) x log(Articles) | -0.010 | 0.001 *** | 0.001 *** | [-0.012, -0.008] |  |
| Sales (2) | Spills per week (avg.) x log(Articles) | -0.007 | 0.001 *** | 0.001 *** | [-0.009, -0.005] |  |
| Sales (3) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (5) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (6) | Spills per week (avg.) x log(Articles) | 0.001 | 0.000 ** | 0.000 * | [0.001, 0.001] | ** → * |
| Rentals (7) | Spills per week (avg.) x log(Articles) | -0.012 | 0.001 *** | 0.002 *** | [-0.016, -0.008] |  |
| Rentals (8) | Spills per week (avg.) x log(Articles) | -0.009 | 0.001 *** | 0.001 *** | [-0.011, -0.007] |  |
| Rentals (9) | Spills per week (avg.) x log(Articles) | -0.001 | 0.000 *** | 0.001 ** | [-0.003, 0.001] | *** → ** |
| Rentals (10) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spills per week (avg.) x log(Articles) | -0.001 | 0.000 *** | 0.001 ** | [-0.003, 0.001] | *** → ** |
| Rentals (12) | Spills per week (avg.) x log(Articles) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_1000m_3m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.016 | 0.003 *** | 0.004 *** | [-0.024, -0.008] |  |
| Sales (2) | Spills per week (avg.) | -0.015 | 0.003 *** | 0.004 *** | [-0.023, -0.007] |  |
| Sales (3) | Spills per week (avg.) | -0.011 | 0.002 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (4) | Spills per week (avg.) | -0.004 | 0.002 * | 0.002 * | [-0.008, 0.000] |  |
| Sales (5) | Spills per week (avg.) | -0.011 | 0.002 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (6) | Spills per week (avg.) | -0.004 | 0.002 ** | 0.002 ** | [-0.008, 0.000] |  |
| Rentals (7) | Spills per week (avg.) | -0.006 | 0.004  | 0.009  | [-0.024, 0.012] |  |
| Rentals (8) | Spills per week (avg.) | -0.010 | 0.003 *** | 0.008  | [-0.026, 0.006] | *** → none |
| Rentals (9) | Spills per week (avg.) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.001  | 0.002  | [-0.006, 0.002] |  |
| Rentals (11) | Spills per week (avg.) | -0.001 | 0.002  | 0.003  | [-0.007, 0.005] |  |
| Rentals (12) | Spills per week (avg.) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (1) | log (Articles_3m) | 0.011 | 0.002 *** | 0.002 *** | [0.007, 0.015] |  |
| Sales (2) | log (Articles_3m) | 0.018 | 0.002 *** | 0.002 *** | [0.014, 0.022] |  |
| Rentals (7) | log (Articles_3m) | 0.081 | 0.003 *** | 0.004 *** | [0.073, 0.089] |  |
| Rentals (8) | log (Articles_3m) | 0.065 | 0.002 *** | 0.003 *** | [0.059, 0.071] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_3m) | -0.016 | 0.001 *** | 0.002 *** | [-0.020, -0.012] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_3m) | -0.011 | 0.001 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (3) | Spills per week (avg.) x log (Articles_3m) | -0.001 | 0.001 * | 0.001  | [-0.003, 0.001] | * → none |
| Sales (4) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_3m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_3m) | -0.017 | 0.001 *** | 0.002 *** | [-0.021, -0.013] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_3m) | -0.012 | 0.001 *** | 0.002 *** | [-0.016, -0.008] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_3m) | -0.002 | 0.001 *** | 0.001 *** | [-0.004, 0.000] |  |
| Rentals (10) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_3m) | -0.003 | 0.001 *** | 0.001 *** | [-0.005, -0.001] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_3m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_1000m_6m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.008 | 0.005  | 0.007  | [-0.006, 0.022] |  |
| Sales (2) | Spills per week (avg.) | 0.001 | 0.005  | 0.007  | [-0.013, 0.015] |  |
| Sales (3) | Spills per week (avg.) | -0.012 | 0.003 *** | 0.004 *** | [-0.020, -0.004] |  |
| Sales (4) | Spills per week (avg.) | -0.006 | 0.003 ** | 0.003 * | [-0.012, 0.000] | ** → * |
| Sales (5) | Spills per week (avg.) | -0.014 | 0.003 *** | 0.003 *** | [-0.020, -0.008] |  |
| Sales (6) | Spills per week (avg.) | -0.009 | 0.003 *** | 0.003 *** | [-0.015, -0.003] |  |
| Rentals (7) | Spills per week (avg.) | 0.020 | 0.006 *** | 0.013  | [-0.005, 0.045] | *** → none |
| Rentals (8) | Spills per week (avg.) | 0.006 | 0.005  | 0.011  | [-0.016, 0.028] |  |
| Rentals (9) | Spills per week (avg.) | 0.002 | 0.003  | 0.004  | [-0.006, 0.010] |  |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.002  | 0.004  | [-0.010, 0.006] |  |
| Rentals (11) | Spills per week (avg.) | 0.003 | 0.003  | 0.004  | [-0.005, 0.011] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.002  | 0.004  | [-0.010, 0.006] |  |
| Sales (1) | log (Articles_6m) | 0.017 | 0.002 *** | 0.003 *** | [0.011, 0.023] |  |
| Sales (2) | log (Articles_6m) | 0.023 | 0.002 *** | 0.003 *** | [0.017, 0.029] |  |
| Rentals (7) | log (Articles_6m) | 0.107 | 0.003 *** | 0.006 *** | [0.095, 0.119] |  |
| Rentals (8) | log (Articles_6m) | 0.084 | 0.003 *** | 0.005 *** | [0.074, 0.094] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_6m) | -0.019 | 0.001 *** | 0.002 *** | [-0.023, -0.015] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_6m) | -0.013 | 0.001 *** | 0.002 *** | [-0.017, -0.009] |  |
| Sales (3) | Spills per week (avg.) x log (Articles_6m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_6m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_6m) | 0.002 | 0.001 ** | 0.001 ** | [0.000, 0.004] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_6m) | -0.020 | 0.002 *** | 0.003 *** | [-0.026, -0.014] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_6m) | -0.014 | 0.001 *** | 0.002 *** | [-0.018, -0.010] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_6m) | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (10) | Spills per week (avg.) x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_6m) | -0.003 | 0.001 *** | 0.001 *** | [-0.005, -0.001] |  |
| Rentals (12) | Spills per week (avg.) x log (Articles_6m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

`did_articles_windowed_prior_1000m_12m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | 0.012 | 0.005 ** | 0.007  | [-0.002, 0.026] | ** → none |
| Sales (2) | Spills per week (avg.) | 0.004 | 0.005  | 0.007  | [-0.010, 0.018] |  |
| Sales (3) | Spills per week (avg.) | -0.012 | 0.003 *** | 0.004 *** | [-0.020, -0.004] |  |
| Sales (4) | Spills per week (avg.) | -0.006 | 0.003 ** | 0.003 * | [-0.012, 0.000] | ** → * |
| Sales (5) | Spills per week (avg.) | -0.014 | 0.003 *** | 0.003 *** | [-0.020, -0.008] |  |
| Sales (6) | Spills per week (avg.) | -0.009 | 0.003 *** | 0.003 *** | [-0.015, -0.003] |  |
| Rentals (7) | Spills per week (avg.) | 0.034 | 0.006 *** | 0.013 ** | [0.009, 0.059] | *** → ** |
| Rentals (8) | Spills per week (avg.) | 0.022 | 0.005 *** | 0.011 * | [0.000, 0.044] | *** → * |
| Rentals (9) | Spills per week (avg.) | 0.002 | 0.003  | 0.004  | [-0.006, 0.010] |  |
| Rentals (10) | Spills per week (avg.) | -0.002 | 0.002  | 0.004  | [-0.010, 0.006] |  |
| Rentals (11) | Spills per week (avg.) | 0.003 | 0.003  | 0.004  | [-0.005, 0.011] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.002  | 0.004  | [-0.010, 0.006] |  |
| Sales (1) | log (Articles_12m) | 0.019 | 0.002 *** | 0.003 *** | [0.013, 0.025] |  |
| Sales (2) | log (Articles_12m) | 0.025 | 0.002 *** | 0.003 *** | [0.019, 0.031] |  |
| Rentals (7) | log (Articles_12m) | 0.109 | 0.003 *** | 0.006 *** | [0.097, 0.121] |  |
| Rentals (8) | log (Articles_12m) | 0.091 | 0.003 *** | 0.005 *** | [0.081, 0.101] |  |
| Sales (1) | Spills per week (avg.) x log (Articles_12m) | -0.018 | 0.001 *** | 0.002 *** | [-0.022, -0.014] |  |
| Sales (2) | Spills per week (avg.) x log (Articles_12m) | -0.013 | 0.001 *** | 0.002 *** | [-0.017, -0.009] |  |
| Sales (3) | Spills per week (avg.) x log (Articles_12m) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Sales (4) | Spills per week (avg.) x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales (5) | Spills per week (avg.) x log (Articles_12m) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales (6) | Spills per week (avg.) x log (Articles_12m) | 0.002 | 0.001 ** | 0.001 ** | [0.000, 0.004] |  |
| Rentals (7) | Spills per week (avg.) x log (Articles_12m) | -0.021 | 0.001 *** | 0.003 *** | [-0.027, -0.015] |  |
| Rentals (8) | Spills per week (avg.) x log (Articles_12m) | -0.016 | 0.001 *** | 0.002 *** | [-0.020, -0.012] |  |
| Rentals (9) | Spills per week (avg.) x log (Articles_12m) | -0.002 | 0.001 *** | 0.001 ** | [-0.004, 0.000] | *** → ** |
| Rentals (10) | Spills per week (avg.) x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spills per week (avg.) x log (Articles_12m) | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (12) | Spills per week (avg.) x log (Articles_12m) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |

## Four-month article lag

`did_articles_lag4_prior_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.190 | 0.010 *** | 0.017 *** | [-0.223, -0.157] |  |
| Sales (2) | Near bin | -0.089 | 0.009 *** | 0.008 *** | [-0.105, -0.073] |  |
| Sales (3) | Near bin | -0.055 | 0.008 *** | 0.007 *** | [-0.069, -0.041] |  |
| Rentals (4) | Near bin | -0.217 | 0.014 *** | 0.035 *** | [-0.286, -0.148] |  |
| Rentals (5) | Near bin | -0.049 | 0.010 *** | 0.011 *** | [-0.071, -0.027] |  |
| Rentals (6) | Near bin | -0.005 | 0.006  | 0.008  | [-0.021, 0.011] |  |
| Sales (1) | Log cumulative articles (4-mo lag) | 0.009 | 0.001 *** | 0.001 *** | [0.007, 0.011] |  |
| Sales (2) | Log cumulative articles (4-mo lag) | 0.085 | 0.003 *** | 0.003 *** | [0.079, 0.091] |  |
| Sales (3) | Log cumulative articles (4-mo lag) | 0.060 | 0.003 *** | 0.003 *** | [0.054, 0.066] |  |
| Rentals (4) | Log cumulative articles (4-mo lag) | 0.045 | 0.001 *** | 0.001 *** | [0.043, 0.047] |  |
| Rentals (5) | Log cumulative articles (4-mo lag) | 0.020 | 0.003 *** | 0.004 *** | [0.012, 0.028] |  |
| Rentals (6) | Log cumulative articles (4-mo lag) | 0.027 | 0.002 *** | 0.003 *** | [0.021, 0.033] |  |
| Sales (1) | Near bin x Log cumulative articles | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (2) | Near bin x Log cumulative articles | -0.002 | 0.001 ** | 0.001 ** | [-0.004, 0.000] |  |
| Sales (3) | Near bin x Log cumulative articles | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| Rentals (4) | Near bin x Log cumulative articles | 0.004 | 0.002 * | 0.003  | [-0.002, 0.010] | * → none |
| Rentals (5) | Near bin x Log cumulative articles | -0.003 | 0.001 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (6) | Near bin x Log cumulative articles | -0.002 | 0.001 *** | 0.001 * | [-0.004, 0.000] | *** → * |

`did_articles_lag4_prior.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.018 | 0.013  | 0.016  | [-0.049, 0.013] |  |
| Sales (2) | Spills per week (avg.) | -0.003 | 0.009  | 0.009  | [-0.021, 0.015] |  |
| Sales (3) | Spills per week (avg.) | 0.003 | 0.008  | 0.008  | [-0.013, 0.019] |  |
| Rentals (4) | Spills per week (avg.) | -0.019 | 0.014  | 0.018  | [-0.054, 0.016] |  |
| Rentals (5) | Spills per week (avg.) | -0.015 | 0.007 ** | 0.007 ** | [-0.029, -0.001] |  |
| Rentals (6) | Spills per week (avg.) | -0.013 | 0.006 ** | 0.006 ** | [-0.025, -0.001] |  |
| Sales (1) | Log cumulative articles (4-mo lag) | 0.005 | 0.003 * | 0.003  | [-0.001, 0.011] | * → none |
| Sales (2) | Log cumulative articles (4-mo lag) | 0.088 | 0.010 *** | 0.010 *** | [0.068, 0.108] |  |
| Sales (3) | Log cumulative articles (4-mo lag) | 0.066 | 0.009 *** | 0.009 *** | [0.048, 0.084] |  |
| Rentals (4) | Log cumulative articles (4-mo lag) | 0.049 | 0.004 *** | 0.004 *** | [0.041, 0.057] |  |
| Rentals (5) | Log cumulative articles (4-mo lag) | 0.008 | 0.010  | 0.011  | [-0.014, 0.030] |  |
| Rentals (6) | Log cumulative articles (4-mo lag) | 0.020 | 0.006 *** | 0.008 *** | [0.004, 0.036] |  |
| Sales (1) | Spills per week (avg.) x Log cumulative articles | -0.007 | 0.003 ** | 0.003 ** | [-0.013, -0.001] |  |
| Sales (2) | Spills per week (avg.) x Log cumulative articles | -0.001 | 0.002  | 0.002  | [-0.005, 0.003] |  |
| Sales (3) | Spills per week (avg.) x Log cumulative articles | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| Rentals (4) | Spills per week (avg.) x Log cumulative articles | -0.007 | 0.004 * | 0.006  | [-0.019, 0.005] | * → none |
| Rentals (5) | Spills per week (avg.) x Log cumulative articles | 0.002 | 0.001  | 0.001  | [0.000, 0.004] |  |
| Rentals (6) | Spills per week (avg.) x Log cumulative articles | 0.002 | 0.001 ** | 0.001 ** | [0.000, 0.004] |  |

## Salience groups

`did_trends_prior_extensive_salience_groups.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales Inland | Near bin | -0.075 | 0.007 *** | 0.006 *** | [-0.087, -0.063] |  |
| Sales Coastal | Near bin | 0.017 | 0.018  | 0.016  | [-0.014, 0.048] |  |
| Sales Bathing water | Near bin | 0.053 | 0.029 * | 0.027 ** | [0.000, 0.106] | * → ** |
| Rentals Inland | Near bin | -0.029 | 0.006 *** | 0.005 *** | [-0.039, -0.019] |  |
| Rentals Coastal | Near bin | 0.013 | 0.013  | 0.012  | [-0.011, 0.037] |  |
| Rentals Bathing water | Near bin | -0.012 | 0.017  | 0.017  | [-0.045, 0.021] |  |
| Sales Inland | Near bin x Post | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Sales Coastal | Near bin x Post | -0.013 | 0.005 ** | 0.006 ** | [-0.025, -0.001] |  |
| Sales Bathing water | Near bin x Post | -0.009 | 0.009  | 0.009  | [-0.027, 0.009] |  |
| Rentals Inland | Near bin x Post | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Rentals Coastal | Near bin x Post | -0.003 | 0.004  | 0.005  | [-0.013, 0.007] |  |
| Rentals Bathing water | Near bin x Post | -0.001 | 0.005  | 0.006  | [-0.013, 0.011] |  |

`did_articles_prior_extensive_salience_groups.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales Inland | Near bin | -0.076 | 0.007 *** | 0.007 *** | [-0.090, -0.062] |  |
| Sales Coastal | Near bin | 0.025 | 0.019  | 0.018  | [-0.010, 0.060] |  |
| Sales Bathing water | Near bin | 0.057 | 0.030 * | 0.029 ** | [0.000, 0.114] | * → ** |
| Rentals Inland | Near bin | -0.037 | 0.007 *** | 0.008 *** | [-0.053, -0.021] |  |
| Rentals Coastal | Near bin | 0.011 | 0.014  | 0.015  | [-0.018, 0.040] |  |
| Rentals Bathing water | Near bin | -0.026 | 0.019  | 0.019  | [-0.063, 0.011] |  |
| Sales Inland | Near bin x log(Articles) | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| Sales Coastal | Near bin x log(Articles) | -0.003 | 0.001 ** | 0.002 ** | [-0.007, 0.001] |  |
| Sales Bathing water | Near bin x log(Articles) | -0.002 | 0.002  | 0.002  | [-0.006, 0.002] |  |
| Rentals Inland | Near bin x log(Articles) | 0.002 | 0.001 ** | 0.001 * | [0.000, 0.004] | ** → * |
| Rentals Coastal | Near bin x log(Articles) | 0.000 | 0.001  | 0.002  | [-0.004, 0.004] |  |
| Rentals Bathing water | Near bin x log(Articles) | 0.003 | 0.002  | 0.002  | [-0.001, 0.007] |  |

`did_trends_prior_salience_groups.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales Inland | Spills per week (avg.) | 0.005 | 0.005  | 0.005  | [-0.005, 0.015] |  |
| Sales Coastal | Spills per week (avg.) | 0.002 | 0.010  | 0.009  | [-0.016, 0.020] |  |
| Sales Bathing water | Spills per week (avg.) | -0.007 | 0.015  | 0.015  | [-0.036, 0.022] |  |
| Rentals Inland | Spills per week (avg.) | -0.004 | 0.003  | 0.003  | [-0.010, 0.002] |  |
| Rentals Coastal | Spills per week (avg.) | 0.004 | 0.007  | 0.008  | [-0.012, 0.020] |  |
| Rentals Bathing water | Spills per week (avg.) | -0.005 | 0.020  | 0.022  | [-0.048, 0.038] |  |
| Sales Inland | Spills per week (avg.) x Post | 0.003 | 0.005  | 0.005  | [-0.007, 0.013] |  |
| Sales Coastal | Spills per week (avg.) x Post | 0.003 | 0.011  | 0.012  | [-0.021, 0.027] |  |
| Sales Bathing water | Spills per week (avg.) x Post | -0.001 | 0.015  | 0.016  | [-0.032, 0.030] |  |
| Rentals Inland | Spills per week (avg.) x Post | 0.008 | 0.003 ** | 0.004 ** | [0.000, 0.016] |  |
| Rentals Coastal | Spills per week (avg.) x Post | 0.001 | 0.007  | 0.007  | [-0.013, 0.015] |  |
| Rentals Bathing water | Spills per week (avg.) x Post | 0.006 | 0.015  | 0.017  | [-0.027, 0.039] |  |

`did_articles_prior_salience_groups.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales Inland | Spills per week (avg.) | 0.009 | 0.006  | 0.005 * | [-0.001, 0.019] | none → * |
| Sales Coastal | Spills per week (avg.) | 0.006 | 0.014  | 0.013  | [-0.019, 0.031] |  |
| Sales Bathing water | Spills per week (avg.) | -0.002 | 0.019  | 0.019  | [-0.039, 0.035] |  |
| Rentals Inland | Spills per week (avg.) | -0.013 | 0.004 *** | 0.004 *** | [-0.021, -0.005] |  |
| Rentals Coastal | Spills per week (avg.) | 0.009 | 0.007  | 0.007  | [-0.005, 0.023] |  |
| Rentals Bathing water | Spills per week (avg.) | -0.009 | 0.026  | 0.030  | [-0.068, 0.050] |  |
| Sales Inland | Spills per week (avg.) x log(Articles) | -0.001 | 0.002  | 0.001  | [-0.003, 0.001] |  |
| Sales Coastal | Spills per week (avg.) x log(Articles) | -0.001 | 0.003  | 0.003  | [-0.007, 0.005] |  |
| Sales Bathing water | Spills per week (avg.) x log(Articles) | -0.002 | 0.003  | 0.003  | [-0.008, 0.004] |  |
| Rentals Inland | Spills per week (avg.) x log(Articles) | 0.003 | 0.001 *** | 0.001 *** | [0.001, 0.005] |  |
| Rentals Coastal | Spills per week (avg.) x log(Articles) | -0.002 | 0.002  | 0.003  | [-0.008, 0.004] |  |
| Rentals Bathing water | Spills per week (avg.) x log(Articles) | 0.002 | 0.005  | 0.005  | [-0.008, 0.012] |  |

## Full study window, Google Trends peak

`did_trends_full_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Near bin | -0.198 | 0.008 *** | 0.016 *** | [-0.229, -0.167] |  |
| Sales (2) | Near bin | -0.100 | 0.008 *** | 0.007 *** | [-0.114, -0.086] |  |
| Sales (3) | Near bin | -0.061 | 0.007 *** | 0.006 *** | [-0.073, -0.049] |  |
| Rentals (4) | Near bin | -0.206 | 0.012 *** | 0.033 *** | [-0.271, -0.141] |  |
| Rentals (5) | Near bin | -0.058 | 0.009 *** | 0.009 *** | [-0.076, -0.040] |  |
| Rentals (6) | Near bin | -0.014 | 0.005 ** | 0.005 ** | [-0.024, -0.004] |  |
| Sales (1) | Post | 0.044 | 0.003 *** | 0.003 *** | [0.038, 0.050] |  |
| Rentals (4) | Post | 0.134 | 0.003 *** | 0.003 *** | [0.128, 0.140] |  |
| Sales (1) | Near bin x Post | 0.000 | 0.006  | 0.006  | [-0.012, 0.012] |  |
| Sales (2) | Near bin x Post | 0.001 | 0.003  | 0.003  | [-0.005, 0.007] |  |
| Sales (3) | Near bin x Post | 0.002 | 0.002  | 0.003  | [-0.004, 0.008] |  |
| Rentals (4) | Near bin x Post | 0.010 | 0.005 * | 0.007  | [-0.004, 0.024] | * → none |
| Rentals (5) | Near bin x Post | -0.008 | 0.002 *** | 0.004 ** | [-0.016, 0.000] | *** → ** |
| Rentals (6) | Near bin x Post | -0.004 | 0.002 ** | 0.003  | [-0.010, 0.002] | ** → none |

`did_trends_full.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Weekly spill count | -0.196 | 0.013 *** | 0.019 *** | [-0.233, -0.159] |  |
| Sales (2) | Weekly spill count | -0.023 | 0.006 *** | 0.007 *** | [-0.037, -0.009] |  |
| Sales (3) | Weekly spill count | 0.004 | 0.005  | 0.005  | [-0.006, 0.014] |  |
| Rentals (4) | Weekly spill count | -0.208 | 0.018 *** | 0.027 *** | [-0.261, -0.155] |  |
| Rentals (5) | Weekly spill count | -0.005 | 0.005  | 0.005  | [-0.015, 0.005] |  |
| Rentals (6) | Weekly spill count | 0.010 | 0.004 *** | 0.004 *** | [0.002, 0.018] |  |
| Sales (1) | Post | 0.040 | 0.002 *** | 0.002 *** | [0.036, 0.044] |  |
| Rentals (4) | Post | 0.135 | 0.002 *** | 0.003 *** | [0.129, 0.141] |  |
| Sales (1) | Weekly spill count x Post | 0.001 | 0.009  | 0.010  | [-0.019, 0.021] |  |
| Sales (2) | Weekly spill count x Post | 0.002 | 0.005  | 0.006  | [-0.010, 0.014] |  |
| Sales (3) | Weekly spill count x Post | 0.004 | 0.005  | 0.005  | [-0.006, 0.014] |  |
| Rentals (4) | Weekly spill count x Post | 0.017 | 0.008 ** | 0.008 ** | [0.001, 0.033] |  |
| Rentals (5) | Weekly spill count x Post | -0.007 | 0.004 ** | 0.004 ** | [-0.015, 0.001] |  |
| Rentals (6) | Weekly spill count x Post | -0.004 | 0.003  | 0.003  | [-0.010, 0.002] |  |

## Event studies around the Google Trends peak

`es_trends_prior_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Event time -4 | -0.259 | 0.008 *** | 0.014 *** | [-0.286, -0.232] |  |
| Sales (2) | Event time -4 | -0.055 | 0.006 *** | 0.006 *** | [-0.067, -0.043] |  |
| Sales (3) | Event time -4 | -0.033 | 0.005 *** | 0.005 *** | [-0.043, -0.023] |  |
| Rentals (4) | Event time -4 | -0.307 | 0.012 *** | 0.031 *** | [-0.368, -0.246] |  |
| Rentals (5) | Event time -4 | -0.013 | 0.005 ** | 0.006 ** | [-0.025, -0.001] |  |
| Rentals (6) | Event time -4 | 0.001 | 0.003  | 0.004  | [-0.007, 0.009] |  |
| Sales (1) | Event time -3 | -0.261 | 0.011 *** | 0.017 *** | [-0.294, -0.228] |  |
| Sales (2) | Event time -3 | -0.052 | 0.007 *** | 0.007 *** | [-0.066, -0.038] |  |
| Sales (3) | Event time -3 | -0.030 | 0.006 *** | 0.006 *** | [-0.042, -0.018] |  |
| Rentals (4) | Event time -3 | -0.272 | 0.012 *** | 0.030 *** | [-0.331, -0.213] |  |
| Rentals (5) | Event time -3 | -0.011 | 0.007  | 0.009  | [-0.029, 0.007] |  |
| Rentals (6) | Event time -3 | -0.002 | 0.004  | 0.005  | [-0.012, 0.008] |  |
| Sales (1) | Event time -2 | -0.214 | 0.010 *** | 0.016 *** | [-0.245, -0.183] |  |
| Sales (2) | Event time -2 | -0.061 | 0.007 *** | 0.006 *** | [-0.073, -0.049] |  |
| Sales (3) | Event time -2 | -0.037 | 0.006 *** | 0.006 *** | [-0.049, -0.025] |  |
| Rentals (4) | Event time -2 | -0.252 | 0.012 *** | 0.030 *** | [-0.311, -0.193] |  |
| Rentals (5) | Event time -2 | -0.006 | 0.006  | 0.007  | [-0.020, 0.008] |  |
| Rentals (6) | Event time -2 | 0.003 | 0.003  | 0.004  | [-0.005, 0.011] |  |
| Sales (1) | Event time 0 | -0.112 | 0.010 *** | 0.017 *** | [-0.145, -0.079] |  |
| Sales (2) | Event time 0 | -0.053 | 0.006 *** | 0.006 *** | [-0.065, -0.041] |  |
| Sales (3) | Event time 0 | -0.028 | 0.005 *** | 0.005 *** | [-0.038, -0.018] |  |
| Rentals (4) | Event time 0 | -0.196 | 0.013 *** | 0.032 *** | [-0.259, -0.133] |  |
| Rentals (5) | Event time 0 | -0.025 | 0.005 *** | 0.005 *** | [-0.035, -0.015] |  |
| Rentals (6) | Event time 0 | -0.008 | 0.003 *** | 0.003 ** | [-0.014, -0.002] | *** → ** |
| Sales (1) | Event time 1 | -0.127 | 0.012 *** | 0.018 *** | [-0.162, -0.092] |  |
| Sales (2) | Event time 1 | -0.057 | 0.006 *** | 0.006 *** | [-0.069, -0.045] |  |
| Sales (3) | Event time 1 | -0.033 | 0.006 *** | 0.005 *** | [-0.043, -0.023] |  |
| Rentals (4) | Event time 1 | -0.175 | 0.012 *** | 0.030 *** | [-0.234, -0.116] |  |
| Rentals (5) | Event time 1 | -0.026 | 0.006 *** | 0.008 *** | [-0.042, -0.010] |  |
| Rentals (6) | Event time 1 | -0.010 | 0.003 *** | 0.004 ** | [-0.018, -0.002] | *** → ** |
| Sales (1) | Event time 2 | -0.191 | 0.011 *** | 0.018 *** | [-0.226, -0.156] |  |
| Sales (2) | Event time 2 | -0.059 | 0.007 *** | 0.007 *** | [-0.073, -0.045] |  |
| Sales (3) | Event time 2 | -0.038 | 0.006 *** | 0.006 *** | [-0.050, -0.026] |  |
| Rentals (4) | Event time 2 | -0.175 | 0.013 *** | 0.031 *** | [-0.236, -0.114] |  |
| Rentals (5) | Event time 2 | -0.026 | 0.005 *** | 0.006 *** | [-0.038, -0.014] |  |
| Rentals (6) | Event time 2 | -0.010 | 0.003 *** | 0.004 ** | [-0.018, -0.002] | *** → ** |
| Sales (1) | Event time 3 | -0.191 | 0.011 *** | 0.017 *** | [-0.224, -0.158] |  |
| Sales (2) | Event time 3 | -0.068 | 0.007 *** | 0.007 *** | [-0.082, -0.054] |  |
| Sales (3) | Event time 3 | -0.039 | 0.007 *** | 0.006 *** | [-0.051, -0.027] |  |
| Rentals (4) | Event time 3 | -0.133 | 0.015 *** | 0.034 *** | [-0.200, -0.066] |  |
| Rentals (5) | Event time 3 | -0.036 | 0.005 *** | 0.005 *** | [-0.046, -0.026] |  |
| Rentals (6) | Event time 3 | -0.018 | 0.003 *** | 0.004 *** | [-0.026, -0.010] |  |
| Sales (1) | Event time 4 | -0.141 | 0.010 *** | 0.017 *** | [-0.174, -0.108] |  |
| Sales (2) | Event time 4 | -0.058 | 0.007 *** | 0.006 *** | [-0.070, -0.046] |  |
| Sales (3) | Event time 4 | -0.033 | 0.006 *** | 0.006 *** | [-0.045, -0.021] |  |
| Rentals (4) | Event time 4 | -0.075 | 0.014 *** | 0.034 ** | [-0.142, -0.008] | *** → ** |
| Rentals (5) | Event time 4 | -0.026 | 0.005 *** | 0.005 *** | [-0.036, -0.016] |  |
| Rentals (6) | Event time 4 | -0.007 | 0.003 ** | 0.004 ** | [-0.015, 0.001] |  |
| Sales (1) | Event time 5 | -0.168 | 0.010 *** | 0.016 *** | [-0.199, -0.137] |  |
| Sales (2) | Event time 5 | -0.052 | 0.007 *** | 0.006 *** | [-0.064, -0.040] |  |
| Sales (3) | Event time 5 | -0.026 | 0.006 *** | 0.006 *** | [-0.038, -0.014] |  |
| Rentals (4) | Event time 5 | -0.071 | 0.013 *** | 0.031 ** | [-0.132, -0.010] | *** → ** |
| Rentals (5) | Event time 5 | -0.022 | 0.006 *** | 0.007 *** | [-0.036, -0.008] |  |
| Rentals (6) | Event time 5 | -0.009 | 0.004 ** | 0.004 ** | [-0.017, -0.001] |  |

`es_trends_prior.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Event time -4 | -0.060 | 0.017 *** | 0.020 *** | [-0.099, -0.021] |  |
| Sales (2) | Event time -4 | -0.001 | 0.008  | 0.008  | [-0.017, 0.015] |  |
| Sales (3) | Event time -4 | 0.000 | 0.007  | 0.007  | [-0.014, 0.014] |  |
| Rentals (4) | Event time -4 | -0.077 | 0.013 *** | 0.018 *** | [-0.112, -0.042] |  |
| Rentals (5) | Event time -4 | -0.009 | 0.007  | 0.008  | [-0.025, 0.007] |  |
| Rentals (6) | Event time -4 | -0.006 | 0.005  | 0.006  | [-0.018, 0.006] |  |
| Sales (1) | Event time -3 | -0.056 | 0.019 *** | 0.024 ** | [-0.103, -0.009] | *** → ** |
| Sales (2) | Event time -3 | -0.008 | 0.011  | 0.011  | [-0.030, 0.014] |  |
| Sales (3) | Event time -3 | -0.006 | 0.010  | 0.011  | [-0.028, 0.016] |  |
| Rentals (4) | Event time -3 | -0.077 | 0.016 *** | 0.020 *** | [-0.116, -0.038] |  |
| Rentals (5) | Event time -3 | -0.013 | 0.009  | 0.010  | [-0.033, 0.007] |  |
| Rentals (6) | Event time -3 | -0.007 | 0.006  | 0.007  | [-0.021, 0.007] |  |
| Sales (1) | Event time -2 | -0.043 | 0.018 ** | 0.022 * | [-0.086, 0.000] | ** → * |
| Sales (2) | Event time -2 | 0.006 | 0.012  | 0.013  | [-0.019, 0.031] |  |
| Sales (3) | Event time -2 | 0.013 | 0.011  | 0.011  | [-0.009, 0.035] |  |
| Rentals (4) | Event time -2 | -0.096 | 0.015 *** | 0.019 *** | [-0.133, -0.059] |  |
| Rentals (5) | Event time -2 | -0.004 | 0.007  | 0.008  | [-0.020, 0.012] |  |
| Rentals (6) | Event time -2 | -0.006 | 0.005  | 0.005  | [-0.016, 0.004] |  |
| Sales (1) | Event time 0 | -0.013 | 0.015  | 0.017  | [-0.046, 0.020] |  |
| Sales (2) | Event time 0 | 0.006 | 0.011  | 0.011  | [-0.016, 0.028] |  |
| Sales (3) | Event time 0 | 0.014 | 0.010  | 0.009  | [-0.004, 0.032] |  |
| Rentals (4) | Event time 0 | -0.050 | 0.016 *** | 0.023 ** | [-0.095, -0.005] | *** → ** |
| Rentals (5) | Event time 0 | -0.001 | 0.008  | 0.008  | [-0.017, 0.015] |  |
| Rentals (6) | Event time 0 | 0.003 | 0.005  | 0.005  | [-0.007, 0.013] |  |
| Sales (1) | Event time 1 | -0.036 | 0.017 ** | 0.019 * | [-0.073, 0.001] | ** → * |
| Sales (2) | Event time 1 | 0.007 | 0.012  | 0.012  | [-0.017, 0.031] |  |
| Sales (3) | Event time 1 | 0.021 | 0.010 ** | 0.011 ** | [-0.001, 0.043] |  |
| Rentals (4) | Event time 1 | -0.053 | 0.018 *** | 0.024 ** | [-0.100, -0.006] | *** → ** |
| Rentals (5) | Event time 1 | -0.011 | 0.009  | 0.009  | [-0.029, 0.007] |  |
| Rentals (6) | Event time 1 | 0.002 | 0.006  | 0.005  | [-0.008, 0.012] |  |
| Sales (1) | Event time 2 | -0.047 | 0.018 *** | 0.020 ** | [-0.086, -0.008] | *** → ** |
| Sales (2) | Event time 2 | -0.007 | 0.013  | 0.013  | [-0.032, 0.018] |  |
| Sales (3) | Event time 2 | 0.002 | 0.011  | 0.012  | [-0.022, 0.026] |  |
| Rentals (4) | Event time 2 | -0.038 | 0.018 ** | 0.025  | [-0.087, 0.011] | ** → none |
| Rentals (5) | Event time 2 | -0.001 | 0.008  | 0.008  | [-0.017, 0.015] |  |
| Rentals (6) | Event time 2 | 0.002 | 0.006  | 0.007  | [-0.012, 0.016] |  |
| Sales (1) | Event time 3 | -0.071 | 0.019 *** | 0.023 *** | [-0.116, -0.026] |  |
| Sales (2) | Event time 3 | -0.010 | 0.016  | 0.017  | [-0.043, 0.023] |  |
| Sales (3) | Event time 3 | 0.005 | 0.014  | 0.015  | [-0.024, 0.034] |  |
| Rentals (4) | Event time 3 | -0.005 | 0.022  | 0.030  | [-0.064, 0.054] |  |
| Rentals (5) | Event time 3 | 0.003 | 0.009  | 0.010  | [-0.017, 0.023] |  |
| Rentals (6) | Event time 3 | -0.003 | 0.007  | 0.007  | [-0.017, 0.011] |  |
| Sales (1) | Event time 4 | -0.024 | 0.017  | 0.020  | [-0.063, 0.015] |  |
| Sales (2) | Event time 4 | -0.008 | 0.014  | 0.014  | [-0.035, 0.019] |  |
| Sales (3) | Event time 4 | 0.009 | 0.012  | 0.012  | [-0.015, 0.033] |  |
| Rentals (4) | Event time 4 | 0.026 | 0.021  | 0.029  | [-0.031, 0.083] |  |
| Rentals (5) | Event time 4 | -0.001 | 0.009  | 0.009  | [-0.019, 0.017] |  |
| Rentals (6) | Event time 4 | -0.004 | 0.006  | 0.006  | [-0.016, 0.008] |  |
| Sales (1) | Event time 5 | -0.039 | 0.019 ** | 0.021 * | [-0.080, 0.002] | ** → * |
| Sales (2) | Event time 5 | 0.011 | 0.015  | 0.014  | [-0.016, 0.038] |  |
| Sales (3) | Event time 5 | 0.018 | 0.013  | 0.013  | [-0.007, 0.043] |  |
| Rentals (4) | Event time 5 | 0.022 | 0.022  | 0.029  | [-0.035, 0.079] |  |
| Rentals (5) | Event time 5 | -0.011 | 0.010  | 0.010  | [-0.031, 0.009] |  |
| Rentals (6) | Event time 5 | -0.001 | 0.007  | 0.006  | [-0.013, 0.011] |  |

## Lagged attention: extensive margin

`did_trends_lagged_sales_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales: lag 0 | Near bin x Post | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Sales: lag 3 | Near bin x Post | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| Sales: lag 6 | Near bin x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| Sales: lag 12 | Near bin x Post | 0.005 | 0.002 ** | 0.002 ** | [0.001, 0.009] |  |
| Rentals: lag 0 | Near bin x Post | -0.007 | 0.002 *** | 0.003 * | [-0.013, -0.001] | *** → * |

`did_articles_lagged_sales_extensive.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales: full lag 0 | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Sales: common lag 0 | Near bin x log(Articles) | 0.004 | 0.002 * | 0.002 * | [0.000, 0.008] |  |
| Sales: common lag 3 | Near bin x log(Articles) | 0.003 | 0.002 * | 0.002 * | [-0.001, 0.007] |  |
| Sales: common lag 6 | Near bin x log(Articles) | 0.003 | 0.002 ** | 0.002 ** | [-0.001, 0.007] |  |
| Sales: common lag 12 | Near bin x log(Articles) | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| Rentals: full lag 0 | Near bin x log(Articles) | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |

## Lagged attention: intensive-margin grids

`did_trends_lagged_sales_grid.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| House sales 250m, Lag 0 | Spills per week (avg.) x Post | 0.003 | 0.005  | 0.005  | [-0.007, 0.013] |  |
| House sales 250m, Lag 3 | Spills per week (avg.) x Post | 0.001 | 0.005  | 0.005  | [-0.009, 0.011] |  |
| House sales 250m, Lag 6 | Spills per week (avg.) x Post | -0.002 | 0.005  | 0.005  | [-0.012, 0.008] |  |
| House sales 250m, Lag 12 | Spills per week (avg.) x Post | -0.001 | 0.006  | 0.005  | [-0.011, 0.009] |  |
| House sales 500m, Lag 0 | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.002  | [-0.001, 0.007] |  |
| House sales 500m, Lag 3 | Spills per week (avg.) x Post | 0.003 | 0.003  | 0.003  | [-0.003, 0.009] |  |
| House sales 500m, Lag 6 | Spills per week (avg.) x Post | 0.004 | 0.003  | 0.003  | [-0.002, 0.010] |  |
| House sales 500m, Lag 12 | Spills per week (avg.) x Post | 0.004 | 0.003 * | 0.003  | [-0.002, 0.010] | * → none |
| House sales 1000m, Lag 0 | Spills per week (avg.) x Post | 0.005 | 0.001 *** | 0.001 *** | [0.003, 0.007] |  |
| House sales 1000m, Lag 3 | Spills per week (avg.) x Post | 0.006 | 0.001 *** | 0.001 *** | [0.004, 0.008] |  |
| House sales 1000m, Lag 6 | Spills per week (avg.) x Post | 0.006 | 0.001 *** | 0.001 *** | [0.004, 0.008] |  |
| House sales 1000m, Lag 12 | Spills per week (avg.) x Post | 0.007 | 0.001 *** | 0.001 *** | [0.005, 0.009] |  |
| House rentals 250m, Lag 0 | Spills per week (avg.) x Post | 0.006 | 0.003 * | 0.004  | [-0.002, 0.014] | * → none |
| House rentals 500m, Lag 0 | Spills per week (avg.) x Post | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| House rentals 1000m, Lag 0 | Spills per week (avg.) x Post | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |

`did_articles_lagged_sales_grid.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| House sales 250m, Full lag 0 | Spills per week (avg.) x log cumulative articles | -0.001 | 0.001  | 0.001  | [-0.003, 0.001] |  |
| House sales 250m, Common lag 0 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.005  | 0.005  | [-0.009, 0.011] |  |
| House sales 250m, Common lag 3 | Spills per week (avg.) x log cumulative articles | -0.000 | 0.005  | 0.004  | [-0.008, 0.008] |  |
| House sales 250m, Common lag 6 | Spills per week (avg.) x log cumulative articles | -0.001 | 0.004  | 0.003  | [-0.007, 0.005] |  |
| House sales 250m, Common lag 12 | Spills per week (avg.) x log cumulative articles | 0.000 | 0.002  | 0.002  | [-0.004, 0.004] |  |
| House sales 500m, Full lag 0 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| House sales 500m, Common lag 0 | Spills per week (avg.) x log cumulative articles | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| House sales 500m, Common lag 3 | Spills per week (avg.) x log cumulative articles | 0.002 | 0.002  | 0.002  | [-0.002, 0.006] |  |
| House sales 500m, Common lag 6 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.002  | 0.002  | [-0.003, 0.005] |  |
| House sales 500m, Common lag 12 | Spills per week (avg.) x log cumulative articles | 0.000 | 0.001  | 0.001  | [-0.002, 0.002] |  |
| House sales 1000m, Full lag 0 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.000 ** | 0.000 * | [0.001, 0.001] | ** → * |
| House sales 1000m, Common lag 0 | Spills per week (avg.) x log cumulative articles | 0.006 | 0.001 *** | 0.001 *** | [0.004, 0.008] |  |
| House sales 1000m, Common lag 3 | Spills per week (avg.) x log cumulative articles | 0.005 | 0.001 *** | 0.001 *** | [0.003, 0.007] |  |
| House sales 1000m, Common lag 6 | Spills per week (avg.) x log cumulative articles | 0.004 | 0.001 *** | 0.001 *** | [0.002, 0.006] |  |
| House sales 1000m, Common lag 12 | Spills per week (avg.) x log cumulative articles | 0.002 | 0.000 *** | 0.000 *** | [0.002, 0.002] |  |
| House rentals 250m, Full lag 0 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| House rentals 500m, Full lag 0 | Spills per week (avg.) x log cumulative articles | 0.001 | 0.001  | 0.001  | [-0.001, 0.003] |  |
| House rentals 1000m, Full lag 0 | Spills per week (avg.) x log cumulative articles | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |


## Figures

The figures redraw the same point estimates with Conley 1 km bands. Band changes are read from the data behind each figure: the event-study columns of the event-study tables, the consolidated `did_news_lagged_sales_effect_sizes.csv` (full precision, old from the snapshot), and, for the coefficient plots, which save no data, a scratchpad copy of `extensive_margin_coefficient_plots.R` that re-estimates all 48 models and stores both the new Conley and the old fixed-effect-unit clustered standard errors (`coef_plot_harness.R`).

**`extensive_margin_news_coefficients_{lsoa,msoa}.pdf` and their `_slides` versions.** All 48 models estimate, as before. Sales bands barely move: the ratio of new to old standard error runs from 0.95 to 1.09 across the 24 sales models. Rental bands mostly widen, and most for the 0-500 m near band against the 500-2000 m and 1000-2000 m far bands: the ratio runs from 0.86 to 1.88 (median 1.23 with LSOA fixed effects, 1.11 with MSOA fixed effects). Rental bands for the two 0-250 m vs 250-500 m article models narrow by about 13%. Three rental Near × Post intervals that excluded zero now include it: 0-500 m vs 1000-2000 m with MSOA fixed effects (-0.0056, old [-0.0106, -0.0005], new [-0.0122, 0.0011]), the same comparison with LSOA fixed effects (-0.0066, old [-0.0103, -0.0029], new [-0.0132, 0.0000]), and 0-500 m vs 500-2000 m with MSOA fixed effects (-0.0051, old [-0.0094, -0.0007], new [-0.0105, 0.0004]). The 0-500 m vs 500-2000 m rental Post interval with LSOA fixed effects still excludes zero. No interval that included zero now excludes it, and no sales interval changes side.

**`es_trends_prior_sales.pdf` and `es_trends_prior_rentals.pdf`** (column 3 and column 6 of `es_trends_prior.tex`). Sales bands are within about 10% of their old width; rental bands change by -17% to +20%. The one sales event-time coefficient whose interval excluded zero (event time 1, 0.021) keeps two stars, so its interval still excludes zero; at the printed 3 decimals it looks as if it now touches zero, but that is rounding. No rental interval changes side.

**`es_trends_prior_extensive_sales.pdf` and `es_trends_prior_extensive_rentals.pdf`** (column 3 and column 6 of `es_trends_prior_extensive.tex`). Sales bands are unchanged or slightly narrower. Rental bands widen by about a quarter to a third at most event times. Every interval that excluded zero still does (the rental event-time 4 coefficient, -0.007, keeps two stars), and none that included zero now excludes it.

**`did_news_lagged_sales_coefficient_paths.{pdf,png}` and `lagged_attention_extensive_response_slides.pdf`** (both draw the extensive-margin, 500 m paths: Post on the full sample and cumulative articles on the common sample). Sales bands widen by 0 to 6%; every sales interval stays on the same side of zero, including the two that only just exclude it (Post at lag 12, lower bound 0.0002, and articles at lag 6, lower bound 0.00004). Rental bands widen by 36% to 80%. The rental Post interval at lag 0 now includes zero (-0.0066, old [-0.0103, -0.0029], new [-0.0132, 0.00005], p = 0.052); at lag 3 it only just excludes zero (upper bound -0.00003, p = 0.049). The rental article intervals still exclude zero at every lag.

**`did_news_lagged_sales_intensive_extension.{pdf,png}`** (intensive margin, sales, 250/500/1000 m paths). Bands narrow by 6% to 10% at 250 m, stay within about 7% at 500 m, and widen by 5% to 9% at 1000 m. No interval changes side; the 1000 m paths still exclude zero at every lag.

## Star changes summary

Across the 56 tables above there are 1,352 printed coefficient cells. 189 lost at least one star level, 6 gained one, and 1,157 are unchanged. By transition: 51 went from * to none, 49 from *** to **, 39 from ** to *, 31 from ** to none, 15 from *** to *, 4 from *** to none, 4 from none to *, and 2 from * to **. The counts are over printed cells, not distinct models: some models appear in more than one table (for example the windowed cumulative tables repeat `did_articles_prior_<R>m` and the headline extensive articles table, the radius-robustness summaries repeat columns of the per-radius tables, and the windowed summary tables repeat columns of the per-window tables).

The pattern follows the market. Sales standard errors barely move on average (median ratio of new to old 1.00 over the 520 sales cells with an old standard error of at least 0.002). Rental standard errors rise (median ratio 1.28 over 490 such rental cells), so 150 of the 189 lost star levels are in rental columns. The six gains are the sales Spills per week coefficient in `did_articles_prior_250m` column 4 and its copy in the 250 m windowed cumulative table, the sales Spills per week coefficient in the 250 m six-month window table (column 3), the sales bathing-water Near bin coefficient in both extensive salience tables, and the sales inland Spills per week coefficient in the intensive articles salience table.

Every coefficient whose star level changed, from the most important table to the least. "none" means not significant at 10%.

- `did_trends_prior_extensive`, Rentals (10), Near bin: 0.006, * → none (SE 0.003 → 0.004)
- `did_trends_prior_extensive`, Rentals (7), Near bin x Post: 0.010, * → none (SE 0.005 → 0.007)
- `did_trends_prior_extensive`, Rentals (8), Near bin x Post: 0.013, *** → ** (SE 0.005 → 0.006)
- `did_trends_prior_extensive`, Rentals (10), Near bin x Post: -0.006, *** → none (SE 0.002 → 0.003)
- `did_trends_prior_extensive`, Rentals (12), Near bin x Post: -0.007, *** → * (SE 0.002 → 0.003)
- `did_articles_prior_extensive`, Rentals (9), Near bin: -0.021, *** → ** (SE 0.006 → 0.009)
- `did_articles_prior_extensive`, Rentals (12), Near bin: -0.011, * → none (SE 0.006 → 0.009)
- `did_articles_prior_extensive`, Rentals (7), Near bin x log(Articles): 0.006, ** → * (SE 0.002 → 0.003)
- `did_articles_prior_extensive`, Rentals (9), Near bin x log(Articles): -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_prior_extensive`, Rentals (11), Near bin x log(Articles): -0.003, *** → * (SE 0.001 → 0.001)
- `did_trends_prior_250m`, Sales (1), Spills per week (avg.): -0.025, *** → ** (SE 0.009 → 0.011)
- `did_trends_prior_250m`, Rentals (7), Spills per week (avg.): -0.035, *** → ** (SE 0.010 → 0.014)
- `did_trends_prior_250m`, Rentals (7), Spills per week (avg.) x Post: -0.024, * → none (SE 0.013 → 0.020)
- `did_trends_prior_250m`, Rentals (12), Spills per week (avg.) x Post: 0.006, * → none (SE 0.003 → 0.004)
- `did_articles_prior_250m`, Sales (4), Spills per week (avg.): 0.009, none → * (SE 0.006 → 0.006)
- `did_articles_prior_250m`, Rentals (12), Spills per week (avg.): -0.008, ** → * (SE 0.004 → 0.004)
- `did_articles_prior_250m`, Sales (2), Spills per week (avg.) x log(Articles): -0.007, *** → ** (SE 0.003 → 0.003)
- `did_articles_prior_250m`, Rentals (7), Spills per week (avg.) x log(Articles): -0.011, ** → none (SE 0.004 → 0.007)
- `did_articles_prior_250m`, Rentals (8), Spills per week (avg.) x log(Articles): -0.009, ** → none (SE 0.004 → 0.006)
- `did_trends_prior_500m`, Sales (3), Spills per week (avg.): -0.008, *** → ** (SE 0.003 → 0.003)
- `did_trends_prior_500m`, Sales (5), Spills per week (avg.): -0.009, *** → ** (SE 0.003 → 0.003)
- `did_trends_prior_500m`, Rentals (12), Spills per week (avg.): -0.004, ** → * (SE 0.002 → 0.002)
- `did_trends_prior_500m`, Rentals (10), Spills per week (avg.) x Post: 0.003, * → none (SE 0.002 → 0.002)
- `did_trends_prior_1000m`, Rentals (10), Spills per week (avg.): -0.002, ** → none (SE 0.001 → 0.001)
- `did_trends_prior_1000m`, Sales (4), Spills per week (avg.) x Post: 0.003, ** → * (SE 0.001 → 0.002)
- `did_trends_prior_1000m`, Sales (5), Spills per week (avg.) x Post: 0.004, *** → ** (SE 0.001 → 0.002)
- `did_trends_prior_1000m`, Rentals (9), Spills per week (avg.) x Post: -0.004, *** → ** (SE 0.001 → 0.002)
- `did_trends_prior_1000m`, Rentals (11), Spills per week (avg.) x Post: -0.004, *** → ** (SE 0.001 → 0.002)
- `did_articles_prior_500m`, Rentals (10), Spills per week (avg.): -0.004, * → none (SE 0.002 → 0.003)
- `did_articles_prior_500m`, Rentals (11), Spills per week (avg.): -0.005, * → none (SE 0.003 → 0.003)
- `did_articles_prior_500m`, Rentals (12), Spills per week (avg.): -0.005, ** → * (SE 0.002 → 0.003)
- `did_articles_prior_500m`, Rentals (10), Spills per week (avg.) x log(Articles): 0.001, ** → none (SE 0.001 → 0.001)
- `did_articles_prior_1000m`, Rentals (8), Spills per week (avg.): -0.007, ** → none (SE 0.004 → 0.008)
- `did_articles_prior_1000m`, Rentals (9), Spills per week (avg.): -0.003, * → none (SE 0.002 → 0.002)
- `did_articles_prior_1000m`, Rentals (10), Spills per week (avg.): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_prior_1000m`, Sales (6), Spills per week (avg.) x log(Articles): 0.001, ** → * (SE 0.000 → 0.000)
- `did_articles_prior_1000m`, Rentals (9), Spills per week (avg.) x log(Articles): -0.001, *** → ** (SE 0.000 → 0.001)
- `did_articles_prior_1000m`, Rentals (11), Spills per week (avg.) x log(Articles): -0.001, *** → ** (SE 0.000 → 0.001)
- `did_trends_prior_extensive_radius_robustness`, Property controls + MSOA FE, Rentals 250m, Near bin x Post: -0.007, *** → * (SE 0.003 → 0.004)
- `did_trends_prior_extensive_radius_robustness`, Property controls + MSOA FE, Rentals 500m, Near bin x Post: -0.006, *** → none (SE 0.002 → 0.003)
- `did_trends_prior_extensive_radius_robustness`, Property controls + MSOA FE, Rentals 1000m, Near bin x Post: -0.003, * → none (SE 0.002 → 0.003)
- `did_trends_prior_extensive_radius_robustness`, Property controls + LSOA FE, Rentals 250m, Near bin x Post: -0.008, *** → ** (SE 0.003 → 0.004)
- `did_trends_prior_extensive_radius_robustness`, Property controls + LSOA FE, Rentals 500m, Near bin x Post: -0.007, *** → * (SE 0.002 → 0.003)
- `did_trends_prior_extensive_radius_robustness`, Property controls + LSOA FE, Rentals 1000m, Near bin x Post: -0.004, ** → none (SE 0.002 → 0.003)
- `did_articles_prior_extensive_radius_robustness`, Property controls + LSOA FE, Sales 1000m, Near bin x log(Articles): 0.001, ** → * (SE 0.001 → 0.001)
- `did_trends_prior_radius_robustness`, Property controls + MSOA FE, Sales 1000m, Spills per week (avg.) x Post: 0.003, ** → * (SE 0.001 → 0.002)
- `did_trends_prior_radius_robustness`, Property controls + MSOA FE, Rentals 500m, Spills per week (avg.) x Post: 0.003, * → none (SE 0.002 → 0.002)
- `did_trends_prior_radius_robustness`, Property controls + LSOA FE, Rentals 250m, Spills per week (avg.) x Post: 0.006, * → none (SE 0.003 → 0.004)
- `did_articles_prior_radius_robustness`, Property controls + MSOA FE, Rentals 500m, Spills per week (avg.) x log(Articles): 0.001, ** → none (SE 0.001 → 0.001)
- `did_articles_prior_radius_robustness`, Property controls + LSOA FE, Sales 1000m, Spills per week (avg.) x log(Articles): 0.001, ** → * (SE 0.000 → 0.000)
- `did_articles_windowed_prior_extensive_slide`, Property controls + LSOA FE, Rentals 3m, Near bin x log (Articles measure): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_slide`, Property controls + LSOA FE, Rentals 12m, Near bin x log (Articles measure): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_comparison`, Property controls + LSOA FE, Rentals 6m, Spills per week (avg.) x log (Articles measure): 0.003, * → none (SE 0.002 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative`, Rentals (9), Near bin: -0.021, *** → ** (SE 0.006 → 0.009)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative`, Rentals (12), Near bin: -0.011, * → none (SE 0.006 → 0.009)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative`, Rentals (7), Near bin x log(Articles): 0.006, ** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative`, Rentals (9), Near bin x log(Articles): -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_cumulative`, Rentals (11), Near bin x log(Articles): -0.003, *** → * (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_3m`, Rentals (9), Near bin: -0.021, *** → ** (SE 0.007 → 0.009)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_3m`, Rentals (7), Near bin x log (Articles_3m): 0.007, ** → * (SE 0.003 → 0.004)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_3m`, Rentals (9), Near bin x log (Articles_3m): -0.004, *** → * (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_3m`, Rentals (11), Near bin x log (Articles_3m): -0.003, ** → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_3m`, Rentals (12), Near bin x log (Articles_3m): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_6m`, Sales (4), Near bin: -0.018, *** → ** (SE 0.007 → 0.007)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_6m`, Rentals (9), Near bin: -0.018, ** → none (SE 0.008 → 0.012)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_6m`, Rentals (7), Near bin x log (Articles_6m): 0.011, *** → ** (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_6m`, Rentals (9), Near bin x log (Articles_6m): -0.004, ** → none (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_6m`, Rentals (11), Near bin x log (Articles_6m): -0.003, ** → none (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_12m`, Rentals (9), Near bin x log (Articles_12m): -0.005, *** → ** (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_12m`, Rentals (11), Near bin x log (Articles_12m): -0.005, *** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_1000_2000_12m`, Rentals (12), Near bin x log (Articles_12m): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_cumulative`, Rentals (8), Near bin x log(Articles): 0.005, ** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_3m`, Sales (2), Near bin: -0.026, * → none (SE 0.015 → 0.017)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_3m`, Rentals (8), Near bin x log (Articles_3m): 0.006, ** → * (SE 0.003 → 0.004)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_6m`, Sales (2), Near bin: -0.034, * → none (SE 0.020 → 0.021)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_6m`, Rentals (10), Near bin: 0.019, *** → ** (SE 0.007 → 0.008)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_6m`, Rentals (8), Near bin x log (Articles_6m): 0.008, ** → * (SE 0.004 → 0.004)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_12m`, Sales (2), Near bin: -0.038, * → none (SE 0.022 → 0.023)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_12m`, Sales (3), Near bin: -0.019, * → none (SE 0.011 → 0.012)
- `did_articles_windowed_prior_extensive_250_vs_250_1000_12m`, Rentals (8), Near bin x log (Articles_12m): 0.007, ** → * (SE 0.004 → 0.004)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_cumulative`, Rentals (10), Near bin: 0.016, *** → ** (SE 0.006 → 0.007)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_cumulative`, Rentals (12), Near bin: 0.012, * → none (SE 0.007 → 0.007)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_cumulative`, Rentals (8), Near bin x log(Articles): 0.006, *** → ** (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_cumulative`, Rentals (9), Near bin x log(Articles): -0.002, ** → * (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_3m`, Sales (2), Near bin: -0.041, *** → ** (SE 0.016 → 0.018)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_6m`, Rentals (10), Near bin: 0.017, ** → * (SE 0.008 → 0.009)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_6m`, Rentals (9), Near bin x log (Articles_6m): -0.005, ** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_250_vs_500_1000_12m`, Rentals (8), Near bin x log (Articles_12m): 0.009, ** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Rentals (10), Near bin: 0.010, *** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Rentals (12), Near bin: 0.006, * → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Sales (1), Near bin x log(Articles): 0.002, * → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Rentals (7), Near bin x log(Articles): 0.005, ** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Rentals (9), Near bin x log(Articles): -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_cumulative`, Rentals (11), Near bin x log(Articles): -0.002, *** → * (SE 0.001 → 0.001)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_3m`, Rentals (10), Near bin: 0.010, *** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_3m`, Rentals (12), Near bin: 0.007, * → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_3m`, Rentals (7), Near bin x log (Articles_3m): 0.006, ** → none (SE 0.003 → 0.004)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_3m`, Rentals (9), Near bin x log (Articles_3m): -0.004, *** → ** (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_3m`, Rentals (11), Near bin x log (Articles_3m): -0.003, ** → * (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_6m`, Rentals (10), Near bin: 0.010, * → none (SE 0.005 → 0.008)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_6m`, Rentals (7), Near bin x log (Articles_6m): 0.008, ** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_6m`, Rentals (9), Near bin x log (Articles_6m): -0.004, *** → * (SE 0.002 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_6m`, Rentals (11), Near bin x log (Articles_6m): -0.003, ** → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Rentals (10), Near bin: 0.013, ** → none (SE 0.006 → 0.009)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Rentals (12), Near bin: 0.009, * → none (SE 0.006 → 0.009)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Sales (2), Near bin x log (Articles_12m): 0.004, * → none (SE 0.002 → 0.003)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Rentals (7), Near bin x log (Articles_12m): 0.008, ** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Rentals (9), Near bin x log (Articles_12m): -0.005, *** → ** (SE 0.001 → 0.002)
- `did_articles_windowed_prior_extensive_500_vs_500_2000_12m`, Rentals (11), Near bin x log (Articles_12m): -0.004, *** → * (SE 0.001 → 0.002)
- `did_articles_windowed_prior_250m_cumulative`, Sales (4), Spills per week (avg.): 0.009, none → * (SE 0.006 → 0.006)
- `did_articles_windowed_prior_250m_cumulative`, Rentals (12), Spills per week (avg.): -0.008, ** → * (SE 0.004 → 0.004)
- `did_articles_windowed_prior_250m_cumulative`, Sales (2), Spills per week (avg.) x log(Articles): -0.007, *** → ** (SE 0.003 → 0.003)
- `did_articles_windowed_prior_250m_cumulative`, Rentals (7), Spills per week (avg.) x log(Articles): -0.011, ** → none (SE 0.004 → 0.007)
- `did_articles_windowed_prior_250m_cumulative`, Rentals (8), Spills per week (avg.) x log(Articles): -0.009, ** → none (SE 0.004 → 0.006)
- `did_articles_windowed_prior_250m_3m`, Rentals (12), Spills per week (avg.): -0.007, * → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_250m_3m`, Sales (2), Spills per week (avg.) x log (Articles_3m): -0.010, ** → * (SE 0.004 → 0.005)
- `did_articles_windowed_prior_250m_3m`, Sales (3), Spills per week (avg.) x log (Articles_3m): -0.005, ** → * (SE 0.003 → 0.003)
- `did_articles_windowed_prior_250m_3m`, Rentals (7), Spills per week (avg.) x log (Articles_3m): -0.013, ** → none (SE 0.006 → 0.010)
- `did_articles_windowed_prior_250m_3m`, Rentals (8), Spills per week (avg.) x log (Articles_3m): -0.011, ** → none (SE 0.005 → 0.009)
- `did_articles_windowed_prior_250m_6m`, Sales (3), Spills per week (avg.): 0.018, none → * (SE 0.011 → 0.011)
- `did_articles_windowed_prior_250m_6m`, Rentals (12), Spills per week (avg.): -0.014, ** → * (SE 0.006 → 0.007)
- `did_articles_windowed_prior_250m_6m`, Sales (2), Spills per week (avg.) x log (Articles_6m): -0.012, ** → * (SE 0.005 → 0.007)
- `did_articles_windowed_prior_250m_6m`, Rentals (7), Spills per week (avg.) x log (Articles_6m): -0.016, * → none (SE 0.008 → 0.013)
- `did_articles_windowed_prior_250m_6m`, Rentals (8), Spills per week (avg.) x log (Articles_6m): -0.012, * → none (SE 0.007 → 0.012)
- `did_articles_windowed_prior_250m_6m`, Rentals (12), Spills per week (avg.) x log (Articles_6m): 0.003, * → none (SE 0.002 → 0.002)
- `did_articles_windowed_prior_250m_12m`, Rentals (12), Spills per week (avg.): -0.014, ** → * (SE 0.006 → 0.008)
- `did_articles_windowed_prior_250m_12m`, Sales (2), Spills per week (avg.) x log (Articles_12m): -0.013, *** → ** (SE 0.005 → 0.006)
- `did_articles_windowed_prior_250m_12m`, Rentals (7), Spills per week (avg.) x log (Articles_12m): -0.018, ** → none (SE 0.008 → 0.013)
- `did_articles_windowed_prior_250m_12m`, Rentals (8), Spills per week (avg.) x log (Articles_12m): -0.015, ** → none (SE 0.007 → 0.011)
- `did_articles_windowed_prior_500m_cumulative`, Rentals (10), Spills per week (avg.): -0.004, * → none (SE 0.002 → 0.003)
- `did_articles_windowed_prior_500m_cumulative`, Rentals (11), Spills per week (avg.): -0.005, * → none (SE 0.003 → 0.003)
- `did_articles_windowed_prior_500m_cumulative`, Rentals (12), Spills per week (avg.): -0.005, ** → * (SE 0.002 → 0.003)
- `did_articles_windowed_prior_500m_cumulative`, Rentals (10), Spills per week (avg.) x log(Articles): 0.001, ** → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_500m_3m`, Rentals (12), Spills per week (avg.): -0.004, * → none (SE 0.002 → 0.003)
- `did_articles_windowed_prior_500m_3m`, Sales (2), Spills per week (avg.) x log (Articles_3m): -0.010, *** → ** (SE 0.003 → 0.004)
- `did_articles_windowed_prior_500m_3m`, Rentals (10), Spills per week (avg.) x log (Articles_3m): 0.001, * → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_500m_3m`, Rentals (11), Spills per week (avg.) x log (Articles_3m): -0.002, * → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_500m_6m`, Rentals (7), Spills per week (avg.): 0.033, ** → none (SE 0.013 → 0.023)
- `did_articles_windowed_prior_500m_6m`, Rentals (10), Spills per week (avg.): -0.007, * → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_500m_6m`, Rentals (12), Spills per week (avg.): -0.007, * → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_500m_6m`, Sales (2), Spills per week (avg.) x log (Articles_6m): -0.011, *** → ** (SE 0.003 → 0.005)
- `did_articles_windowed_prior_500m_6m`, Rentals (10), Spills per week (avg.) x log (Articles_6m): 0.002, ** → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_500m_6m`, Rentals (11), Spills per week (avg.) x log (Articles_6m): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_500m_12m`, Sales (1), Spills per week (avg.): 0.028, ** → * (SE 0.013 → 0.016)
- `did_articles_windowed_prior_500m_12m`, Rentals (7), Spills per week (avg.): 0.050, *** → ** (SE 0.014 → 0.025)
- `did_articles_windowed_prior_500m_12m`, Rentals (8), Spills per week (avg.): 0.030, ** → none (SE 0.013 → 0.021)
- `did_articles_windowed_prior_500m_12m`, Rentals (10), Spills per week (avg.): -0.008, ** → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_500m_12m`, Rentals (12), Spills per week (avg.): -0.008, ** → none (SE 0.004 → 0.005)
- `did_articles_windowed_prior_500m_12m`, Rentals (10), Spills per week (avg.) x log (Articles_12m): 0.002, ** → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_1000m_cumulative`, Rentals (8), Spills per week (avg.): -0.007, ** → none (SE 0.004 → 0.008)
- `did_articles_windowed_prior_1000m_cumulative`, Rentals (9), Spills per week (avg.): -0.003, * → none (SE 0.002 → 0.002)
- `did_articles_windowed_prior_1000m_cumulative`, Rentals (10), Spills per week (avg.): -0.002, * → none (SE 0.001 → 0.002)
- `did_articles_windowed_prior_1000m_cumulative`, Sales (6), Spills per week (avg.) x log(Articles): 0.001, ** → * (SE 0.000 → 0.000)
- `did_articles_windowed_prior_1000m_cumulative`, Rentals (9), Spills per week (avg.) x log(Articles): -0.001, *** → ** (SE 0.000 → 0.001)
- `did_articles_windowed_prior_1000m_cumulative`, Rentals (11), Spills per week (avg.) x log(Articles): -0.001, *** → ** (SE 0.000 → 0.001)
- `did_articles_windowed_prior_1000m_3m`, Rentals (8), Spills per week (avg.): -0.010, *** → none (SE 0.003 → 0.008)
- `did_articles_windowed_prior_1000m_3m`, Sales (3), Spills per week (avg.) x log (Articles_3m): -0.001, * → none (SE 0.001 → 0.001)
- `did_articles_windowed_prior_1000m_6m`, Sales (4), Spills per week (avg.): -0.006, ** → * (SE 0.003 → 0.003)
- `did_articles_windowed_prior_1000m_6m`, Rentals (7), Spills per week (avg.): 0.020, *** → none (SE 0.006 → 0.013)
- `did_articles_windowed_prior_1000m_6m`, Rentals (9), Spills per week (avg.) x log (Articles_6m): -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_windowed_prior_1000m_12m`, Sales (1), Spills per week (avg.): 0.012, ** → none (SE 0.005 → 0.007)
- `did_articles_windowed_prior_1000m_12m`, Sales (4), Spills per week (avg.): -0.006, ** → * (SE 0.003 → 0.003)
- `did_articles_windowed_prior_1000m_12m`, Rentals (7), Spills per week (avg.): 0.034, *** → ** (SE 0.006 → 0.013)
- `did_articles_windowed_prior_1000m_12m`, Rentals (8), Spills per week (avg.): 0.022, *** → * (SE 0.005 → 0.011)
- `did_articles_windowed_prior_1000m_12m`, Rentals (9), Spills per week (avg.) x log (Articles_12m): -0.002, *** → ** (SE 0.001 → 0.001)
- `did_articles_windowed_prior_1000m_12m`, Rentals (11), Spills per week (avg.) x log (Articles_12m): -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_lag4_prior_extensive`, Rentals (4), Near bin x Log cumulative articles: 0.004, * → none (SE 0.002 → 0.003)
- `did_articles_lag4_prior_extensive`, Rentals (5), Near bin x Log cumulative articles: -0.003, *** → ** (SE 0.001 → 0.001)
- `did_articles_lag4_prior_extensive`, Rentals (6), Near bin x Log cumulative articles: -0.002, *** → * (SE 0.001 → 0.001)
- `did_articles_lag4_prior`, Sales (1), Log cumulative articles (4-mo lag): 0.005, * → none (SE 0.003 → 0.003)
- `did_articles_lag4_prior`, Rentals (4), Spills per week (avg.) x Log cumulative articles: -0.007, * → none (SE 0.004 → 0.006)
- `did_trends_prior_extensive_salience_groups`, Sales Bathing water, Near bin: 0.053, * → ** (SE 0.029 → 0.027)
- `did_articles_prior_extensive_salience_groups`, Sales Bathing water, Near bin: 0.057, * → ** (SE 0.030 → 0.029)
- `did_articles_prior_extensive_salience_groups`, Rentals Inland, Near bin x log(Articles): 0.002, ** → * (SE 0.001 → 0.001)
- `did_articles_prior_salience_groups`, Sales Inland, Spills per week (avg.): 0.009, none → * (SE 0.006 → 0.005)
- `did_trends_full_extensive`, Rentals (4), Near bin x Post: 0.010, * → none (SE 0.005 → 0.007)
- `did_trends_full_extensive`, Rentals (5), Near bin x Post: -0.008, *** → ** (SE 0.002 → 0.004)
- `did_trends_full_extensive`, Rentals (6), Near bin x Post: -0.004, ** → none (SE 0.002 → 0.003)
- `es_trends_prior_extensive`, Rentals (6), Event time 0: -0.008, *** → ** (SE 0.003 → 0.003)
- `es_trends_prior_extensive`, Rentals (6), Event time 1: -0.010, *** → ** (SE 0.003 → 0.004)
- `es_trends_prior_extensive`, Rentals (6), Event time 2: -0.010, *** → ** (SE 0.003 → 0.004)
- `es_trends_prior_extensive`, Rentals (4), Event time 4: -0.075, *** → ** (SE 0.014 → 0.034)
- `es_trends_prior_extensive`, Rentals (4), Event time 5: -0.071, *** → ** (SE 0.013 → 0.031)
- `es_trends_prior`, Sales (1), Event time -3: -0.056, *** → ** (SE 0.019 → 0.024)
- `es_trends_prior`, Sales (1), Event time -2: -0.043, ** → * (SE 0.018 → 0.022)
- `es_trends_prior`, Rentals (4), Event time 0: -0.050, *** → ** (SE 0.016 → 0.023)
- `es_trends_prior`, Sales (1), Event time 1: -0.036, ** → * (SE 0.017 → 0.019)
- `es_trends_prior`, Rentals (4), Event time 1: -0.053, *** → ** (SE 0.018 → 0.024)
- `es_trends_prior`, Sales (1), Event time 2: -0.047, *** → ** (SE 0.018 → 0.020)
- `es_trends_prior`, Rentals (4), Event time 2: -0.038, ** → none (SE 0.018 → 0.025)
- `es_trends_prior`, Sales (1), Event time 5: -0.039, ** → * (SE 0.019 → 0.021)
- `did_trends_lagged_sales_extensive`, Rentals: lag 0, Near bin x Post: -0.007, *** → * (SE 0.002 → 0.003)
- `did_trends_lagged_sales_grid`, House sales 500m, Lag 12, Spills per week (avg.) x Post: 0.004, * → none (SE 0.003 → 0.003)
- `did_trends_lagged_sales_grid`, House rentals 250m, Lag 0, Spills per week (avg.) x Post: 0.006, * → none (SE 0.003 → 0.004)
- `did_articles_lagged_sales_grid`, House sales 1000m, Full lag 0, Spills per week (avg.) x log cumulative articles: 0.001, ** → * (SE 0.000 → 0.000)
## Verification

**Point estimates and samples did not change.** For all 62 regenerated `.tex` tables, every printed estimate and every Observations cell is identical between the old side (snapshot, or the fresh pre-change baseline for the six stale tables) and the new file. The 15 regenerated effect-size files (`did_articles_windowed_prior_*_effect_sizes.csv`, `did_articles_windowed_prior_extensive_*effect*.csv`, the four lagged-attention component files and the consolidated `did_news_lagged_sales_effect_sizes.csv`) match the snapshot at full precision in every estimate and sample size. This also covers the two tables without an Observations row (`did_articles_windowed_prior_comparison` and the two lagged grids), whose cells come from models whose sample sizes are in those files. All 48 models behind the coefficient plots estimate under both estimators.

**Coordinates reach the estimator.** Every script already keeps `latitude` and `longitude` in the estimation data, and every estimation sample either filters out rows with a missing LSOA or uses LSOA or MSOA fixed effects, which drop those rows. In the source files, 12,145 sales rows lack coordinates and every one of them also lacks LSOA and MSOA; no rental row lacks coordinates. So the Conley estimator drops no observation.

**Conley note in each written file.** Of the 62 regenerated `.tex` tables, 61 contain the sentence "Conley standard errors with a 1 km cutoff are reported in parentheses." exactly once. `did_articles_windowed_prior_extensive_slide.tex` has no notes block, as before. None of the 62 contains "LSOA level", "LSOA-clustered" or "clustered by LSOA". That wording remains in 22 other files in `output/tables` that no current script writes: the unsuffixed `did_trends_prior.tex` and `did_articles_prior.tex`, and the salience files `*_salience_coast_bathing.tex`, `*_salience_intensity.tex` and `*_salience_robust_{coast10km,dropunknown,london}.tex` for the four salience families (the current salience scripts write only `*_salience_groups.tex`).

**No other output changed.** The changed files in `output/tables` are the 62 `.tex` and 15 `.csv` files above, all written by `05_news` scripts. The changed files in `output/figures` are the 13 listed under "Figures". The four `*_salience_groups.rds` files in `output/regs` and the salience `*_cell_counts.csv` and `*_results.csv` files in `output/logs` were rewritten by the salience scripts. The helper rename is inert: `hedonic_continuous_prior.R` reproduces its seven tables byte-identically.

**Warnings.** Both lagged-attention extensive scripts print ten "Could not get model data" warnings from `modelsummary`. The pre-change version of `did_trends_lagged_sales_extensive.R` prints the same ten, so they are not caused by this switch.

## Open items

- `run_all_analysis.sh` runs `did_articles_windowed_prior.R` after `did_articles_prior.R`, and `did_articles_windowed_prior_extensive.R` after `did_articles_prior_extensive.R`. A full pipeline run therefore leaves the windowed scripts' versions of `did_articles_prior_{250,500,1000}m.tex` and `did_articles_prior_extensive.tex` on disk, with different labels and notes. The models are the same, but which file the paper includes depends on run order. Decide whether to stop the windowed scripts writing these legacy paths or to reorder the runner.
- 22 leftover tables in `output/tables` (listed under "Verification") still say LSOA-clustered and are not written by any current script. Decide whether to delete them or to confirm nothing in the paper or slides includes them.
- The rental results lose the most. In the headline Google Trends table the rental LSOA-FE interaction (column 12) goes from *** to *, and in the lagged-attention figure the rental Post interval at lag 0 now includes zero (p = 0.052). The rental placebo reading in the paper and slides may need rewording.
- Paper and slide text that describes the difference-in-differences standard errors as LSOA-clustered has not been checked or changed, and Overleaf has not been synced.
- `did_attention_repeat_extensive.R` on `jo/long-run-attention-scripts` should call `spatial_vcov()` and `spatial_se_note()`. If its working copy uses the old helper names, it will fail once this branch is merged.
- The plot captions in `summarise_lagged_attention_sales.R` state "1 km" in words, because that script does not estimate and does not source the helper. They will need a manual edit if the cutoff changes.
- The glossary exception says the dry-spill logit clusters by overflow site (`site_id`), which is what `dry_spill_logit_ame_table.R` does, rather than by Site Group.
