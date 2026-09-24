# Hedonic cross-sections: switch to Conley 1 km standard errors — 24 September 2026

All seven hedonic cross-sectional scripts in `scripts/R/09_analysis/02_hedonic/` now report Conley spatial standard errors with a 1 km cutoff and fixest's default triangular kernel, set through one shared helper (`cross_section_vcov()` in `scripts/R/09_analysis/utils_table_formatting.R`), in place of heteroskedasticity-robust standard errors. Point estimates, observation counts and adjusted R-squared values are unchanged in every table: every estimate in the old and new files matches once the comparison uses a baseline built from the current data (see "Baseline" below).

## Baseline

The old side comes from the pre-change snapshot of `output/tables/hedonic_*.tex`, with one exception. The snapshot copies of the quarter fixed effects tables (`hedonic_{count,hrs}_continuous_prior_qtr_fe_{250,500,1000}m`) and the prior-window bins tables (`hedonic_{count,hrs}_bins_prior`) were stale. They were built on an older sample, with 209,032 sales at 250 m against 166,192 now, so their point estimates and observation counts do not match the current data. For those eight tables, the old side is a fresh run of the unchanged `HEAD` version of each script (heteroskedasticity-robust standard errors) on current data, written to the scratchpad. That run matches the new tables exactly in every estimate, observation count and R-squared value, so only the standard errors differ. Against the stale snapshot, 126 printed estimates in the two bins tables and 58 in the six quarter fixed effects tables differ at 3 decimals, all because the sample changed and not because of this switch.

Tables are parsed from the `.tex` files by `compare_tables.py` in the session scratchpad. Values are at the tables' 3-decimal precision, so the confidence intervals for the spill-hours coefficients (printed as 0.000 or 0.001) are only indicative. Stars use the new Conley p-values that fixest reports; the confidence interval is the estimate plus or minus 1.96 times the new standard error. Stars are carried by the estimate in the tables and are shown next to each standard error here for comparison.

## Headline: count, prior window, 250 m

`hedonic_count_continuous_prior_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.039 | 0.002 *** | 0.013 *** | [-0.064, -0.014] |  |
| Sales (2) | Spills per week (avg.) | -0.023 | 0.002 *** | 0.012 * | [-0.047, 0.001] | *** → * |
| Sales (3) | Spills per week (avg.) | -0.015 | 0.003 *** | 0.005 *** | [-0.025, -0.005] |  |
| Sales (4) | Spills per week (avg.) | -0.009 | 0.002 *** | 0.005 ** | [-0.019, 0.001] | *** → ** |
| Sales (5) | Spills per week (avg.) | -0.014 | 0.003 *** | 0.005 *** | [-0.024, -0.004] |  |
| Sales (6) | Spills per week (avg.) | -0.011 | 0.003 *** | 0.004 ** | [-0.019, -0.003] | *** → ** |
| Rentals (7) | Spills per week (avg.) | -0.050 | 0.002 *** | 0.017 *** | [-0.083, -0.017] |  |
| Rentals (8) | Spills per week (avg.) | -0.044 | 0.002 *** | 0.014 *** | [-0.071, -0.017] |  |
| Rentals (9) | Spills per week (avg.) | -0.025 | 0.003 *** | 0.005 *** | [-0.035, -0.015] |  |
| Rentals (10) | Spills per week (avg.) | -0.019 | 0.002 *** | 0.005 *** | [-0.029, -0.009] |  |
| Rentals (11) | Spills per week (avg.) | -0.033 | 0.003 *** | 0.006 *** | [-0.045, -0.021] |  |
| Rentals (12) | Spills per week (avg.) | -0.030 | 0.002 *** | 0.005 *** | [-0.040, -0.020] |  |

## Radius robustness summary

`hedonic_count_continuous_prior_radius_robustness.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales 250m, MSOA FE | Spills per week (avg.) | -0.009 | 0.002 *** | 0.005 ** | [-0.019, 0.001] | *** → ** |
| Sales 500m, MSOA FE | Spills per week (avg.) | -0.006 | 0.001 *** | 0.003 ** | [-0.012, 0.000] | *** → ** |
| Sales 1000m, MSOA FE | Spills per week (avg.) | -0.011 | 0.001 *** | 0.002 *** | [-0.015, -0.007] |  |
| Rentals 250m, MSOA FE | Spills per week (avg.) | -0.019 | 0.002 *** | 0.005 *** | [-0.029, -0.009] |  |
| Rentals 500m, MSOA FE | Spills per week (avg.) | -0.015 | 0.001 *** | 0.002 *** | [-0.019, -0.011] |  |
| Rentals 1000m, MSOA FE | Spills per week (avg.) | -0.018 | 0.001 *** | 0.002 *** | [-0.022, -0.014] |  |
| Sales 250m, LSOA FE | Spills per week (avg.) | -0.011 | 0.003 *** | 0.004 ** | [-0.019, -0.003] | *** → ** |
| Sales 500m, LSOA FE | Spills per week (avg.) | -0.011 | 0.002 *** | 0.003 *** | [-0.017, -0.005] |  |
| Sales 1000m, LSOA FE | Spills per week (avg.) | -0.014 | 0.001 *** | 0.002 *** | [-0.018, -0.010] |  |
| Rentals 250m, LSOA FE | Spills per week (avg.) | -0.030 | 0.002 *** | 0.005 *** | [-0.040, -0.020] |  |
| Rentals 500m, LSOA FE | Spills per week (avg.) | -0.027 | 0.001 *** | 0.003 *** | [-0.033, -0.021] |  |
| Rentals 1000m, LSOA FE | Spills per week (avg.) | -0.026 | 0.001 *** | 0.003 *** | [-0.032, -0.020] |  |

## Inverse-distance-weighted count, 250 m

`hedonic_count_continuous_prior_idw_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg., dist.-weighted) | -0.037 | 0.003 *** | 0.013 *** | [-0.062, -0.012] |  |
| Sales (2) | Spills per week (avg., dist.-weighted) | -0.018 | 0.003 *** | 0.012  | [-0.042, 0.006] | *** → none |
| Sales (3) | Spills per week (avg., dist.-weighted) | -0.016 | 0.003 *** | 0.005 *** | [-0.026, -0.006] |  |
| Sales (4) | Spills per week (avg., dist.-weighted) | -0.001 | 0.002  | 0.004  | [-0.009, 0.007] |  |
| Sales (5) | Spills per week (avg., dist.-weighted) | -0.016 | 0.003 *** | 0.005 *** | [-0.026, -0.006] |  |
| Sales (6) | Spills per week (avg., dist.-weighted) | -0.002 | 0.003  | 0.004  | [-0.010, 0.006] |  |
| Rentals (7) | Spills per week (avg., dist.-weighted) | -0.043 | 0.003 *** | 0.019 ** | [-0.080, -0.006] | *** → ** |
| Rentals (8) | Spills per week (avg., dist.-weighted) | -0.040 | 0.003 *** | 0.015 *** | [-0.069, -0.011] |  |
| Rentals (9) | Spills per week (avg., dist.-weighted) | -0.017 | 0.003 *** | 0.004 *** | [-0.025, -0.009] |  |
| Rentals (10) | Spills per week (avg., dist.-weighted) | -0.010 | 0.002 *** | 0.003 *** | [-0.016, -0.004] |  |
| Rentals (11) | Spills per week (avg., dist.-weighted) | -0.021 | 0.003 *** | 0.005 *** | [-0.031, -0.011] |  |
| Rentals (12) | Spills per week (avg., dist.-weighted) | -0.015 | 0.002 *** | 0.004 *** | [-0.023, -0.007] |  |

## Salience groups

`hedonic_count_continuous_prior_salience_groups.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales Inland | Spills per week (avg.) | -0.008 | 0.003 ** | 0.005 * | [-0.018, 0.002] | ** → * |
| Sales Coastal | Spills per week (avg.) | -0.007 | 0.007  | 0.009  | [-0.025, 0.011] |  |
| Sales Bathing water | Spills per week (avg.) | -0.024 | 0.012 * | 0.014 * | [-0.051, 0.003] |  |
| Rentals Inland | Spills per week (avg.) | -0.024 | 0.002 *** | 0.004 *** | [-0.032, -0.016] |  |
| Rentals Coastal | Spills per week (avg.) | -0.017 | 0.006 *** | 0.009 * | [-0.035, 0.001] | *** → * |
| Rentals Bathing water | Spills per week (avg.) | -0.021 | 0.014  | 0.014  | [-0.048, 0.006] |  |

## Count bins, full study window

`hedonic_count_bins_full.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill count Q1 | -0.010 | 0.009  | 0.050  | [-0.108, 0.088] |  |
| Sales (2) | Spill count Q1 | -0.038 | 0.009 *** | 0.046  | [-0.128, 0.052] | *** → none |
| Sales (3) | Spill count Q1 | 0.010 | 0.014  | 0.033  | [-0.055, 0.075] |  |
| Sales (4) | Spill count Q1 | 0.005 | 0.013  | 0.028  | [-0.050, 0.060] |  |
| Sales (5) | Spill count Q1 | -0.012 | 0.020  | 0.042  | [-0.094, 0.070] |  |
| Sales (6) | Spill count Q1 | -0.013 | 0.019  | 0.035  | [-0.082, 0.056] |  |
| Rentals (7) | Spill count Q1 | -0.081 | 0.009 *** | 0.064  | [-0.206, 0.044] | *** → none |
| Rentals (8) | Spill count Q1 | -0.061 | 0.008 *** | 0.055  | [-0.169, 0.047] | *** → none |
| Rentals (9) | Spill count Q1 | 0.028 | 0.012 ** | 0.028  | [-0.027, 0.083] | ** → none |
| Rentals (10) | Spill count Q1 | 0.031 | 0.008 *** | 0.018 * | [-0.004, 0.066] | *** → * |
| Rentals (11) | Spill count Q1 | -0.026 | 0.019  | 0.027  | [-0.079, 0.027] |  |
| Rentals (12) | Spill count Q1 | 0.021 | 0.012 * | 0.017  | [-0.012, 0.054] | * → none |
| Sales (1) | Spill count Q2 | -0.043 | 0.009 *** | 0.051  | [-0.143, 0.057] | *** → none |
| Sales (2) | Spill count Q2 | -0.074 | 0.009 *** | 0.048  | [-0.168, 0.020] | *** → none |
| Sales (3) | Spill count Q2 | -0.009 | 0.014  | 0.035  | [-0.078, 0.060] |  |
| Sales (4) | Spill count Q2 | 0.006 | 0.013  | 0.029  | [-0.051, 0.063] |  |
| Sales (5) | Spill count Q2 | -0.038 | 0.020 * | 0.041  | [-0.118, 0.042] | * → none |
| Sales (6) | Spill count Q2 | -0.030 | 0.018 * | 0.033  | [-0.095, 0.035] | * → none |
| Rentals (7) | Spill count Q2 | 0.010 | 0.009  | 0.076  | [-0.139, 0.159] |  |
| Rentals (8) | Spill count Q2 | 0.009 | 0.008  | 0.069  | [-0.126, 0.144] |  |
| Rentals (9) | Spill count Q2 | 0.031 | 0.012 ** | 0.030  | [-0.028, 0.090] | ** → none |
| Rentals (10) | Spill count Q2 | 0.027 | 0.008 *** | 0.019  | [-0.010, 0.064] | *** → none |
| Rentals (11) | Spill count Q2 | -0.059 | 0.020 *** | 0.034 * | [-0.126, 0.008] | *** → * |
| Rentals (12) | Spill count Q2 | -0.009 | 0.013  | 0.018  | [-0.044, 0.026] |  |
| Sales (1) | Spill count Q3 | -0.079 | 0.009 *** | 0.049  | [-0.175, 0.017] | *** → none |
| Sales (2) | Spill count Q3 | -0.103 | 0.009 *** | 0.046 ** | [-0.193, -0.013] | *** → ** |
| Sales (3) | Spill count Q3 | -0.023 | 0.014  | 0.034  | [-0.090, 0.044] |  |
| Sales (4) | Spill count Q3 | -0.004 | 0.013  | 0.029  | [-0.061, 0.053] |  |
| Sales (5) | Spill count Q3 | -0.062 | 0.020 *** | 0.042  | [-0.144, 0.020] | *** → none |
| Sales (6) | Spill count Q3 | -0.034 | 0.018 * | 0.034  | [-0.101, 0.033] | * → none |
| Rentals (7) | Spill count Q3 | -0.072 | 0.010 *** | 0.072  | [-0.213, 0.069] | *** → none |
| Rentals (8) | Spill count Q3 | -0.055 | 0.008 *** | 0.065  | [-0.182, 0.072] | *** → none |
| Rentals (9) | Spill count Q3 | 0.033 | 0.013 *** | 0.033  | [-0.032, 0.098] | *** → none |
| Rentals (10) | Spill count Q3 | 0.043 | 0.008 *** | 0.020 ** | [0.004, 0.082] | *** → ** |
| Rentals (11) | Spill count Q3 | -0.034 | 0.020 * | 0.034  | [-0.101, 0.033] | * → none |
| Rentals (12) | Spill count Q3 | 0.022 | 0.013 * | 0.019  | [-0.015, 0.059] | * → none |
| Sales (1) | Spill count Q4 | -0.104 | 0.009 *** | 0.051 ** | [-0.204, -0.004] | *** → ** |
| Sales (2) | Spill count Q4 | -0.102 | 0.009 *** | 0.048 ** | [-0.196, -0.008] | *** → ** |
| Sales (3) | Spill count Q4 | -0.015 | 0.014  | 0.035  | [-0.084, 0.054] |  |
| Sales (4) | Spill count Q4 | 0.004 | 0.013  | 0.030  | [-0.055, 0.063] |  |
| Sales (5) | Spill count Q4 | -0.041 | 0.020 ** | 0.042  | [-0.123, 0.041] | ** → none |
| Sales (6) | Spill count Q4 | -0.010 | 0.018  | 0.034  | [-0.077, 0.057] |  |
| Rentals (7) | Spill count Q4 | -0.153 | 0.009 *** | 0.071 ** | [-0.292, -0.014] | *** → ** |
| Rentals (8) | Spill count Q4 | -0.129 | 0.008 *** | 0.062 ** | [-0.251, -0.007] | *** → ** |
| Rentals (9) | Spill count Q4 | 0.012 | 0.012  | 0.031  | [-0.049, 0.073] |  |
| Rentals (10) | Spill count Q4 | 0.026 | 0.008 *** | 0.020  | [-0.013, 0.065] | *** → none |
| Rentals (11) | Spill count Q4 | -0.055 | 0.019 *** | 0.032 * | [-0.118, 0.008] | *** → * |
| Rentals (12) | Spill count Q4 | 0.001 | 0.013  | 0.018  | [-0.034, 0.036] |  |

## Count, full study window

`hedonic_count_continuous_full.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.048 | 0.003 *** | 0.018 *** | [-0.083, -0.013] |  |
| Sales (2) | Spills per week (avg.) | -0.027 | 0.003 *** | 0.017  | [-0.060, 0.006] | *** → none |
| Sales (3) | Spills per week (avg.) | -0.014 | 0.004 *** | 0.008 * | [-0.030, 0.002] | *** → * |
| Sales (4) | Spills per week (avg.) | -0.001 | 0.003  | 0.006  | [-0.013, 0.011] |  |
| Sales (5) | Spills per week (avg.) | -0.014 | 0.005 *** | 0.010  | [-0.034, 0.006] | *** → none |
| Sales (6) | Spills per week (avg.) | 0.003 | 0.005  | 0.008  | [-0.013, 0.019] |  |
| Rentals (7) | Spills per week (avg.) | -0.063 | 0.003 *** | 0.027 ** | [-0.116, -0.010] | *** → ** |
| Rentals (8) | Spills per week (avg.) | -0.054 | 0.003 *** | 0.023 ** | [-0.099, -0.009] | *** → ** |
| Rentals (9) | Spills per week (avg.) | -0.010 | 0.003 *** | 0.007  | [-0.024, 0.004] | *** → none |
| Rentals (10) | Spills per week (avg.) | -0.001 | 0.002  | 0.005  | [-0.011, 0.009] |  |
| Rentals (11) | Spills per week (avg.) | -0.013 | 0.004 *** | 0.008  | [-0.029, 0.003] | *** → none |
| Rentals (12) | Spills per week (avg.) | -0.005 | 0.003  | 0.007  | [-0.019, 0.009] |  |

## Count with quarter fixed effects, 250 m

`hedonic_count_continuous_prior_qtr_fe_250m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.039 | 0.002 *** | 0.013 *** | [-0.064, -0.014] |  |
| Sales (2) | Spills per week (avg.) | -0.021 | 0.002 *** | 0.013 * | [-0.046, 0.004] | *** → * |
| Sales (3) | Spills per week (avg.) | -0.010 | 0.003 *** | 0.005 * | [-0.020, 0.000] | *** → * |
| Sales (4) | Spills per week (avg.) | -0.002 | 0.003  | 0.005  | [-0.012, 0.008] |  |
| Sales (5) | Spills per week (avg.) | -0.006 | 0.003 * | 0.006  | [-0.018, 0.006] | * → none |
| Sales (6) | Spills per week (avg.) | 0.001 | 0.003  | 0.004  | [-0.007, 0.009] |  |
| Rentals (7) | Spills per week (avg.) | -0.050 | 0.002 *** | 0.017 *** | [-0.083, -0.017] |  |
| Rentals (8) | Spills per week (avg.) | -0.037 | 0.002 *** | 0.014 ** | [-0.064, -0.010] | *** → ** |
| Rentals (9) | Spills per week (avg.) | -0.006 | 0.002 ** | 0.005  | [-0.016, 0.004] | ** → none |
| Rentals (10) | Spills per week (avg.) | 0.000 | 0.002  | 0.004  | [-0.008, 0.008] |  |
| Rentals (11) | Spills per week (avg.) | -0.006 | 0.003 ** | 0.004  | [-0.014, 0.002] | ** → none |
| Rentals (12) | Spills per week (avg.) | -0.004 | 0.002 * | 0.003  | [-0.010, 0.002] | * → none |

## Count, prior window, 500 m and 1000 m

`hedonic_count_continuous_prior_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.057 | 0.001 *** | 0.010 *** | [-0.077, -0.037] |  |
| Sales (2) | Spills per week (avg.) | -0.039 | 0.001 *** | 0.009 *** | [-0.057, -0.021] |  |
| Sales (3) | Spills per week (avg.) | -0.013 | 0.001 *** | 0.003 *** | [-0.019, -0.007] |  |
| Sales (4) | Spills per week (avg.) | -0.006 | 0.001 *** | 0.003 ** | [-0.012, 0.000] | *** → ** |
| Sales (5) | Spills per week (avg.) | -0.018 | 0.002 *** | 0.003 *** | [-0.024, -0.012] |  |
| Sales (6) | Spills per week (avg.) | -0.011 | 0.002 *** | 0.003 *** | [-0.017, -0.005] |  |
| Rentals (7) | Spills per week (avg.) | -0.061 | 0.001 *** | 0.012 *** | [-0.085, -0.037] |  |
| Rentals (8) | Spills per week (avg.) | -0.050 | 0.001 *** | 0.010 *** | [-0.070, -0.030] |  |
| Rentals (9) | Spills per week (avg.) | -0.023 | 0.001 *** | 0.003 *** | [-0.029, -0.017] |  |
| Rentals (10) | Spills per week (avg.) | -0.015 | 0.001 *** | 0.002 *** | [-0.019, -0.011] |  |
| Rentals (11) | Spills per week (avg.) | -0.033 | 0.002 *** | 0.003 *** | [-0.039, -0.027] |  |
| Rentals (12) | Spills per week (avg.) | -0.027 | 0.001 *** | 0.003 *** | [-0.033, -0.021] |  |

`hedonic_count_continuous_prior_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.067 | 0.001 *** | 0.006 *** | [-0.079, -0.055] |  |
| Sales (2) | Spills per week (avg.) | -0.052 | 0.000 *** | 0.005 *** | [-0.062, -0.042] |  |
| Sales (3) | Spills per week (avg.) | -0.019 | 0.001 *** | 0.002 *** | [-0.023, -0.015] |  |
| Sales (4) | Spills per week (avg.) | -0.011 | 0.001 *** | 0.002 *** | [-0.015, -0.007] |  |
| Sales (5) | Spills per week (avg.) | -0.019 | 0.001 *** | 0.002 *** | [-0.023, -0.015] |  |
| Sales (6) | Spills per week (avg.) | -0.014 | 0.001 *** | 0.002 *** | [-0.018, -0.010] |  |
| Rentals (7) | Spills per week (avg.) | -0.062 | 0.001 *** | 0.008 *** | [-0.078, -0.046] |  |
| Rentals (8) | Spills per week (avg.) | -0.052 | 0.001 *** | 0.008 *** | [-0.068, -0.036] |  |
| Rentals (9) | Spills per week (avg.) | -0.024 | 0.001 *** | 0.002 *** | [-0.028, -0.020] |  |
| Rentals (10) | Spills per week (avg.) | -0.018 | 0.001 *** | 0.002 *** | [-0.022, -0.014] |  |
| Rentals (11) | Spills per week (avg.) | -0.033 | 0.001 *** | 0.003 *** | [-0.039, -0.027] |  |
| Rentals (12) | Spills per week (avg.) | -0.026 | 0.001 *** | 0.003 *** | [-0.032, -0.020] |  |

## Inverse-distance-weighted count, 500 m and 1000 m

`hedonic_count_continuous_prior_idw_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg., dist.-weighted) | -0.091 | 0.003 *** | 0.017 *** | [-0.124, -0.058] |  |
| Sales (2) | Spills per week (avg., dist.-weighted) | -0.057 | 0.003 *** | 0.016 *** | [-0.088, -0.026] |  |
| Sales (3) | Spills per week (avg., dist.-weighted) | -0.023 | 0.003 *** | 0.007 *** | [-0.037, -0.009] |  |
| Sales (4) | Spills per week (avg., dist.-weighted) | 0.002 | 0.002  | 0.005  | [-0.008, 0.012] |  |
| Sales (5) | Spills per week (avg., dist.-weighted) | -0.027 | 0.003 *** | 0.006 *** | [-0.039, -0.015] |  |
| Sales (6) | Spills per week (avg., dist.-weighted) | -0.002 | 0.002  | 0.005  | [-0.012, 0.008] |  |
| Rentals (7) | Spills per week (avg., dist.-weighted) | -0.103 | 0.003 *** | 0.022 *** | [-0.146, -0.060] |  |
| Rentals (8) | Spills per week (avg., dist.-weighted) | -0.085 | 0.003 *** | 0.018 *** | [-0.120, -0.050] |  |
| Rentals (9) | Spills per week (avg., dist.-weighted) | -0.031 | 0.002 *** | 0.004 *** | [-0.039, -0.023] |  |
| Rentals (10) | Spills per week (avg., dist.-weighted) | -0.013 | 0.002 *** | 0.004 *** | [-0.021, -0.005] |  |
| Rentals (11) | Spills per week (avg., dist.-weighted) | -0.037 | 0.003 *** | 0.005 *** | [-0.047, -0.027] |  |
| Rentals (12) | Spills per week (avg., dist.-weighted) | -0.020 | 0.002 *** | 0.004 *** | [-0.028, -0.012] |  |

`hedonic_count_continuous_prior_idw_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg., dist.-weighted) | -0.204 | 0.004 *** | 0.021 *** | [-0.245, -0.163] |  |
| Sales (2) | Spills per week (avg., dist.-weighted) | -0.150 | 0.003 *** | 0.019 *** | [-0.187, -0.113] |  |
| Sales (3) | Spills per week (avg., dist.-weighted) | -0.053 | 0.003 *** | 0.007 *** | [-0.067, -0.039] |  |
| Sales (4) | Spills per week (avg., dist.-weighted) | -0.013 | 0.002 *** | 0.005 ** | [-0.023, -0.003] | *** → ** |
| Sales (5) | Spills per week (avg., dist.-weighted) | -0.047 | 0.004 *** | 0.007 *** | [-0.061, -0.033] |  |
| Sales (6) | Spills per week (avg., dist.-weighted) | -0.016 | 0.003 *** | 0.005 *** | [-0.026, -0.006] |  |
| Rentals (7) | Spills per week (avg., dist.-weighted) | -0.207 | 0.005 *** | 0.038 *** | [-0.281, -0.133] |  |
| Rentals (8) | Spills per week (avg., dist.-weighted) | -0.167 | 0.004 *** | 0.034 *** | [-0.234, -0.100] |  |
| Rentals (9) | Spills per week (avg., dist.-weighted) | -0.057 | 0.003 *** | 0.006 *** | [-0.069, -0.045] |  |
| Rentals (10) | Spills per week (avg., dist.-weighted) | -0.030 | 0.002 *** | 0.005 *** | [-0.040, -0.020] |  |
| Rentals (11) | Spills per week (avg., dist.-weighted) | -0.066 | 0.004 *** | 0.008 *** | [-0.082, -0.050] |  |
| Rentals (12) | Spills per week (avg., dist.-weighted) | -0.042 | 0.003 *** | 0.007 *** | [-0.056, -0.028] |  |

## Count with quarter fixed effects, 500 m and 1000 m

`hedonic_count_continuous_prior_qtr_fe_500m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.057 | 0.001 *** | 0.010 *** | [-0.077, -0.037] |  |
| Sales (2) | Spills per week (avg.) | -0.038 | 0.001 *** | 0.009 *** | [-0.056, -0.020] |  |
| Sales (3) | Spills per week (avg.) | -0.009 | 0.001 *** | 0.003 ** | [-0.015, -0.003] | *** → ** |
| Sales (4) | Spills per week (avg.) | 0.000 | 0.001  | 0.003  | [-0.006, 0.006] |  |
| Sales (5) | Spills per week (avg.) | -0.010 | 0.002 *** | 0.004 *** | [-0.018, -0.002] |  |
| Sales (6) | Spills per week (avg.) | 0.000 | 0.002  | 0.003  | [-0.006, 0.006] |  |
| Rentals (7) | Spills per week (avg.) | -0.061 | 0.001 *** | 0.012 *** | [-0.085, -0.037] |  |
| Rentals (8) | Spills per week (avg.) | -0.045 | 0.001 *** | 0.010 *** | [-0.065, -0.025] |  |
| Rentals (9) | Spills per week (avg.) | -0.008 | 0.001 *** | 0.002 *** | [-0.012, -0.004] |  |
| Rentals (10) | Spills per week (avg.) | 0.000 | 0.001  | 0.002  | [-0.004, 0.004] |  |
| Rentals (11) | Spills per week (avg.) | -0.009 | 0.002 *** | 0.002 *** | [-0.013, -0.005] |  |
| Rentals (12) | Spills per week (avg.) | -0.004 | 0.001 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |

`hedonic_count_continuous_prior_qtr_fe_1000m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spills per week (avg.) | -0.067 | 0.001 *** | 0.006 *** | [-0.079, -0.055] |  |
| Sales (2) | Spills per week (avg.) | -0.052 | 0.000 *** | 0.005 *** | [-0.062, -0.042] |  |
| Sales (3) | Spills per week (avg.) | -0.016 | 0.001 *** | 0.002 *** | [-0.020, -0.012] |  |
| Sales (4) | Spills per week (avg.) | -0.005 | 0.001 *** | 0.002 *** | [-0.009, -0.001] |  |
| Sales (5) | Spills per week (avg.) | -0.012 | 0.001 *** | 0.002 *** | [-0.016, -0.008] |  |
| Sales (6) | Spills per week (avg.) | -0.004 | 0.001 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (7) | Spills per week (avg.) | -0.062 | 0.001 *** | 0.008 *** | [-0.078, -0.046] |  |
| Rentals (8) | Spills per week (avg.) | -0.048 | 0.001 *** | 0.008 *** | [-0.064, -0.032] |  |
| Rentals (9) | Spills per week (avg.) | -0.008 | 0.001 *** | 0.002 *** | [-0.012, -0.004] |  |
| Rentals (10) | Spills per week (avg.) | -0.003 | 0.001 *** | 0.001 * | [-0.005, -0.001] | *** → * |
| Rentals (11) | Spills per week (avg.) | -0.007 | 0.001 *** | 0.002 *** | [-0.011, -0.003] |  |
| Rentals (12) | Spills per week (avg.) | -0.002 | 0.001 *** | 0.002  | [-0.006, 0.002] | *** → none |

## Hours, prior window, all radii

`hedonic_hrs_continuous_prior_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | 0.001 | 0.000 *** | 0.001  | [-0.001, 0.003] | *** → none |
| Sales (2) | Spill hours per week (avg.) | 0.000 | 0.000 * | 0.001  | [-0.002, 0.002] | * → none |
| Sales (3) | Spill hours per week (avg.) | 0.000 | 0.000 ** | 0.000  | [0.000, 0.000] | ** → none |
| Sales (4) | Spill hours per week (avg.) | 0.000 | 0.000 * | 0.000  | [0.000, 0.000] | * → none |
| Sales (5) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 * | [-0.001, -0.001] | *** → * |
| Sales (6) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 ** | [-0.001, -0.001] | *** → ** |
| Rentals (7) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (8) | Spill hours per week (avg.) | -0.003 | 0.000 *** | 0.001 ** | [-0.005, -0.001] | *** → ** |
| Rentals (9) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (10) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (11) | Spill hours per week (avg.) | -0.003 | 0.000 *** | 0.000 *** | [-0.003, -0.003] |  |
| Rentals (12) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |

`hedonic_hrs_continuous_prior_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.001  | [-0.003, 0.001] | *** → none |
| Sales (2) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.001 * | [-0.003, 0.001] | *** → * |
| Sales (3) | Spill hours per week (avg.) | 0.000 | 0.000 ** | 0.000  | [0.000, 0.000] | ** → none |
| Sales (4) | Spill hours per week (avg.) | 0.000 | 0.000 *** | 0.000  | [0.000, 0.000] | *** → none |
| Sales (5) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Sales (6) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Rentals (7) | Spill hours per week (avg.) | -0.005 | 0.000 *** | 0.001 *** | [-0.007, -0.003] |  |
| Rentals (8) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Rentals (9) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (10) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (11) | Spill hours per week (avg.) | -0.003 | 0.000 *** | 0.000 *** | [-0.003, -0.003] |  |
| Rentals (12) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |

`hedonic_hrs_continuous_prior_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Sales (2) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Sales (3) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Sales (4) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Sales (5) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Sales (6) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Rentals (7) | Spill hours per week (avg.) | -0.005 | 0.000 *** | 0.001 *** | [-0.007, -0.003] |  |
| Rentals (8) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Rentals (9) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (10) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (11) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Rentals (12) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |

## Inverse-distance-weighted hours, all radii

`hedonic_hrs_continuous_prior_idw_250m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg., dist.-weighted) | 0.001 | 0.000 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Sales (2) | Spill hours per week (avg., dist.-weighted) | 0.001 | 0.000 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Sales (3) | Spill hours per week (avg., dist.-weighted) | -0.001 | 0.000 * | 0.000  | [-0.001, -0.001] | * → none |
| Sales (4) | Spill hours per week (avg., dist.-weighted) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (5) | Spill hours per week (avg., dist.-weighted) | -0.001 | 0.000 *** | 0.001 ** | [-0.003, 0.001] | *** → ** |
| Sales (6) | Spill hours per week (avg., dist.-weighted) | -0.001 | 0.000 ** | 0.000  | [-0.001, -0.001] | ** → none |
| Rentals (7) | Spill hours per week (avg., dist.-weighted) | -0.005 | 0.000 *** | 0.002 ** | [-0.009, -0.001] | *** → ** |
| Rentals (8) | Spill hours per week (avg., dist.-weighted) | -0.004 | 0.000 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (9) | Spill hours per week (avg., dist.-weighted) | -0.003 | 0.000 *** | 0.001 *** | [-0.005, -0.001] |  |
| Rentals (10) | Spill hours per week (avg., dist.-weighted) | -0.002 | 0.000 *** | 0.001 *** | [-0.004, 0.000] |  |
| Rentals (11) | Spill hours per week (avg., dist.-weighted) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Rentals (12) | Spill hours per week (avg., dist.-weighted) | -0.003 | 0.000 *** | 0.000 *** | [-0.003, -0.003] |  |

`hedonic_hrs_continuous_prior_idw_500m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg., dist.-weighted) | -0.002 | 0.000 *** | 0.001  | [-0.004, 0.000] | *** → none |
| Sales (2) | Spill hours per week (avg., dist.-weighted) | -0.002 | 0.000 *** | 0.001  | [-0.004, 0.000] | *** → none |
| Sales (3) | Spill hours per week (avg., dist.-weighted) | -0.001 | 0.000 *** | 0.001  | [-0.003, 0.001] | *** → none |
| Sales (4) | Spill hours per week (avg., dist.-weighted) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (5) | Spill hours per week (avg., dist.-weighted) | -0.002 | 0.000 *** | 0.001 *** | [-0.004, 0.000] |  |
| Sales (6) | Spill hours per week (avg., dist.-weighted) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Rentals (7) | Spill hours per week (avg., dist.-weighted) | -0.011 | 0.001 *** | 0.003 *** | [-0.017, -0.005] |  |
| Rentals (8) | Spill hours per week (avg., dist.-weighted) | -0.010 | 0.000 *** | 0.002 *** | [-0.014, -0.006] |  |
| Rentals (9) | Spill hours per week (avg., dist.-weighted) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Rentals (10) | Spill hours per week (avg., dist.-weighted) | -0.003 | 0.000 *** | 0.001 *** | [-0.005, -0.001] |  |
| Rentals (11) | Spill hours per week (avg., dist.-weighted) | -0.006 | 0.000 *** | 0.001 *** | [-0.008, -0.004] |  |
| Rentals (12) | Spill hours per week (avg., dist.-weighted) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |

`hedonic_hrs_continuous_prior_idw_1000m.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg., dist.-weighted) | -0.008 | 0.000 *** | 0.002 *** | [-0.012, -0.004] |  |
| Sales (2) | Spill hours per week (avg., dist.-weighted) | -0.008 | 0.000 *** | 0.002 *** | [-0.012, -0.004] |  |
| Sales (3) | Spill hours per week (avg., dist.-weighted) | -0.003 | 0.000 *** | 0.001 *** | [-0.005, -0.001] |  |
| Sales (4) | Spill hours per week (avg., dist.-weighted) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Sales (5) | Spill hours per week (avg., dist.-weighted) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Sales (6) | Spill hours per week (avg., dist.-weighted) | -0.003 | 0.000 *** | 0.000 *** | [-0.003, -0.003] |  |
| Rentals (7) | Spill hours per week (avg., dist.-weighted) | -0.022 | 0.001 *** | 0.003 *** | [-0.028, -0.016] |  |
| Rentals (8) | Spill hours per week (avg., dist.-weighted) | -0.018 | 0.001 *** | 0.003 *** | [-0.024, -0.012] |  |
| Rentals (9) | Spill hours per week (avg., dist.-weighted) | -0.007 | 0.000 *** | 0.001 *** | [-0.009, -0.005] |  |
| Rentals (10) | Spill hours per week (avg., dist.-weighted) | -0.005 | 0.000 *** | 0.001 *** | [-0.007, -0.003] |  |
| Rentals (11) | Spill hours per week (avg., dist.-weighted) | -0.009 | 0.000 *** | 0.001 *** | [-0.011, -0.007] |  |
| Rentals (12) | Spill hours per week (avg., dist.-weighted) | -0.007 | 0.000 *** | 0.001 *** | [-0.009, -0.005] |  |

## Hours with quarter fixed effects, all radii

`hedonic_hrs_continuous_prior_qtr_fe_250m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | 0.001 | 0.000 *** | 0.001  | [-0.001, 0.003] | *** → none |
| Sales (2) | Spill hours per week (avg.) | 0.001 | 0.000 *** | 0.001  | [-0.001, 0.003] | *** → none |
| Sales (3) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (4) | Spill hours per week (avg.) | 0.000 | 0.000 * | 0.000  | [0.000, 0.000] | * → none |
| Sales (5) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (6) | Spill hours per week (avg.) | 0.000 | 0.000 * | 0.000  | [0.000, 0.000] | * → none |
| Rentals (7) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.002 ** | [-0.008, 0.000] | *** → ** |
| Rentals (8) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.001 * | [-0.004, 0.000] | *** → * |
| Rentals (9) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (10) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (11) | Spill hours per week (avg.) | -0.001 | 0.000 ** | 0.000  | [-0.001, -0.001] | ** → none |
| Rentals (12) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |

`hedonic_hrs_continuous_prior_qtr_fe_500m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.001  | [-0.003, 0.001] | *** → none |
| Sales (2) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.001  | [-0.003, 0.001] | *** → none |
| Sales (3) | Spill hours per week (avg.) | 0.000 | 0.000 * | 0.000  | [0.000, 0.000] | * → none |
| Sales (4) | Spill hours per week (avg.) | 0.000 | 0.000 *** | 0.000  | [0.000, 0.000] | *** → none |
| Sales (5) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Sales (6) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (7) | Spill hours per week (avg.) | -0.005 | 0.000 *** | 0.001 *** | [-0.007, -0.003] |  |
| Rentals (8) | Spill hours per week (avg.) | -0.003 | 0.000 *** | 0.001 *** | [-0.005, -0.001] |  |
| Rentals (9) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (10) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (11) | Spill hours per week (avg.) | 0.000 | 0.000 ** | 0.000  | [0.000, 0.000] | ** → none |
| Rentals (12) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |

`hedonic_hrs_continuous_prior_qtr_fe_1000m.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Sales (2) | Spill hours per week (avg.) | -0.002 | 0.000 *** | 0.000 *** | [-0.002, -0.002] |  |
| Sales (3) | Spill hours per week (avg.) | -0.001 | 0.000 *** | 0.000 *** | [-0.001, -0.001] |  |
| Sales (4) | Spill hours per week (avg.) | 0.000 | 0.000 ** | 0.000  | [0.000, 0.000] | ** → none |
| Sales (5) | Spill hours per week (avg.) | 0.000 | 0.000 *** | 0.000 *** | [0.000, 0.000] |  |
| Sales (6) | Spill hours per week (avg.) | 0.000 | 0.000 *** | 0.000 ** | [0.000, 0.000] | *** → ** |
| Rentals (7) | Spill hours per week (avg.) | -0.005 | 0.000 *** | 0.001 *** | [-0.007, -0.003] |  |
| Rentals (8) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.001 *** | [-0.006, -0.002] |  |
| Rentals (9) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (10) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (11) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |
| Rentals (12) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.000  | [0.000, 0.000] |  |

## Hours, full study window (continuous and bins)

`hedonic_hrs_continuous_full.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours per week (avg.) | 0.003 | 0.000 *** | 0.002 * | [-0.001, 0.007] | *** → * |
| Sales (2) | Spill hours per week (avg.) | 0.002 | 0.000 *** | 0.002  | [-0.002, 0.006] | *** → none |
| Sales (3) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Sales (4) | Spill hours per week (avg.) | 0.001 | 0.000 ** | 0.001  | [-0.001, 0.003] | ** → none |
| Sales (5) | Spill hours per week (avg.) | -0.001 | 0.001  | 0.002  | [-0.005, 0.003] |  |
| Sales (6) | Spill hours per week (avg.) | 0.001 | 0.001 * | 0.001  | [-0.001, 0.003] | * → none |
| Rentals (7) | Spill hours per week (avg.) | -0.004 | 0.000 *** | 0.003  | [-0.010, 0.002] | *** → none |
| Rentals (8) | Spill hours per week (avg.) | -0.003 | 0.000 *** | 0.003  | [-0.009, 0.003] | *** → none |
| Rentals (9) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Rentals (10) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |
| Rentals (11) | Spill hours per week (avg.) | -0.001 | 0.001 ** | 0.001  | [-0.003, 0.001] | ** → none |
| Rentals (12) | Spill hours per week (avg.) | 0.000 | 0.000  | 0.001  | [-0.002, 0.002] |  |

`hedonic_hrs_bins_full.tex`

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill duration Q1 | -0.082 | 0.009 *** | 0.051  | [-0.182, 0.018] | *** → none |
| Sales (2) | Spill duration Q1 | -0.094 | 0.009 *** | 0.048 ** | [-0.188, 0.000] | *** → ** |
| Sales (3) | Spill duration Q1 | 0.001 | 0.014  | 0.033  | [-0.064, 0.066] |  |
| Sales (4) | Spill duration Q1 | 0.004 | 0.013  | 0.028  | [-0.051, 0.059] |  |
| Sales (5) | Spill duration Q1 | -0.018 | 0.020  | 0.042  | [-0.100, 0.064] |  |
| Sales (6) | Spill duration Q1 | -0.021 | 0.018  | 0.035  | [-0.090, 0.048] |  |
| Rentals (7) | Spill duration Q1 | -0.103 | 0.009 *** | 0.064  | [-0.228, 0.022] | *** → none |
| Rentals (8) | Spill duration Q1 | -0.085 | 0.008 *** | 0.055  | [-0.193, 0.023] | *** → none |
| Rentals (9) | Spill duration Q1 | 0.035 | 0.012 *** | 0.029  | [-0.022, 0.092] | *** → none |
| Rentals (10) | Spill duration Q1 | 0.029 | 0.008 *** | 0.018  | [-0.006, 0.064] | *** → none |
| Rentals (11) | Spill duration Q1 | -0.029 | 0.019  | 0.026  | [-0.080, 0.022] |  |
| Rentals (12) | Spill duration Q1 | 0.010 | 0.012  | 0.016  | [-0.021, 0.041] |  |
| Sales (1) | Spill duration Q2 | -0.060 | 0.009 *** | 0.048  | [-0.154, 0.034] | *** → none |
| Sales (2) | Spill duration Q2 | -0.081 | 0.009 *** | 0.044 * | [-0.167, 0.005] | *** → * |
| Sales (3) | Spill duration Q2 | -0.012 | 0.014  | 0.034  | [-0.079, 0.055] |  |
| Sales (4) | Spill duration Q2 | 0.005 | 0.013  | 0.029  | [-0.052, 0.062] |  |
| Sales (5) | Spill duration Q2 | -0.042 | 0.020 ** | 0.041  | [-0.122, 0.038] | ** → none |
| Sales (6) | Spill duration Q2 | -0.025 | 0.018  | 0.034  | [-0.092, 0.042] |  |
| Rentals (7) | Spill duration Q2 | -0.091 | 0.009 *** | 0.052 * | [-0.193, 0.011] | *** → * |
| Rentals (8) | Spill duration Q2 | -0.072 | 0.008 *** | 0.047  | [-0.164, 0.020] | *** → none |
| Rentals (9) | Spill duration Q2 | 0.022 | 0.012 * | 0.029  | [-0.035, 0.079] | * → none |
| Rentals (10) | Spill duration Q2 | 0.027 | 0.008 *** | 0.018  | [-0.008, 0.062] | *** → none |
| Rentals (11) | Spill duration Q2 | -0.044 | 0.020 ** | 0.032  | [-0.107, 0.019] | ** → none |
| Rentals (12) | Spill duration Q2 | 0.011 | 0.013  | 0.017  | [-0.022, 0.044] |  |
| Sales (1) | Spill duration Q3 | -0.063 | 0.009 *** | 0.052  | [-0.165, 0.039] | *** → none |
| Sales (2) | Spill duration Q3 | -0.082 | 0.009 *** | 0.049 * | [-0.178, 0.014] | *** → * |
| Sales (3) | Spill duration Q3 | -0.022 | 0.014  | 0.034  | [-0.089, 0.045] |  |
| Sales (4) | Spill duration Q3 | -0.009 | 0.013  | 0.029  | [-0.066, 0.048] |  |
| Sales (5) | Spill duration Q3 | -0.045 | 0.020 ** | 0.042  | [-0.127, 0.037] | ** → none |
| Sales (6) | Spill duration Q3 | -0.028 | 0.018  | 0.033  | [-0.093, 0.037] |  |
| Rentals (7) | Spill duration Q3 | 0.020 | 0.010 ** | 0.078  | [-0.133, 0.173] | ** → none |
| Rentals (8) | Spill duration Q3 | 0.012 | 0.008  | 0.070  | [-0.125, 0.149] |  |
| Rentals (9) | Spill duration Q3 | 0.038 | 0.012 *** | 0.033  | [-0.027, 0.103] | *** → none |
| Rentals (10) | Spill duration Q3 | 0.041 | 0.008 *** | 0.021 ** | [0.000, 0.082] | *** → ** |
| Rentals (11) | Spill duration Q3 | -0.037 | 0.020 * | 0.031  | [-0.098, 0.024] | * → none |
| Rentals (12) | Spill duration Q3 | 0.011 | 0.013  | 0.018  | [-0.024, 0.046] |  |
| Sales (1) | Spill duration Q4 | -0.032 | 0.009 *** | 0.050  | [-0.130, 0.066] | *** → none |
| Sales (2) | Spill duration Q4 | -0.060 | 0.009 *** | 0.047  | [-0.152, 0.032] | *** → none |
| Sales (3) | Spill duration Q4 | 0.002 | 0.014  | 0.035  | [-0.067, 0.071] |  |
| Sales (4) | Spill duration Q4 | 0.015 | 0.013  | 0.030  | [-0.044, 0.074] |  |
| Sales (5) | Spill duration Q4 | -0.047 | 0.020 ** | 0.042  | [-0.129, 0.035] | ** → none |
| Sales (6) | Spill duration Q4 | -0.012 | 0.018  | 0.034  | [-0.079, 0.055] |  |
| Rentals (7) | Spill duration Q4 | -0.127 | 0.009 *** | 0.070 * | [-0.264, 0.010] | *** → * |
| Rentals (8) | Spill duration Q4 | -0.096 | 0.008 *** | 0.062  | [-0.218, 0.026] | *** → none |
| Rentals (9) | Spill duration Q4 | 0.014 | 0.012  | 0.031  | [-0.047, 0.075] |  |
| Rentals (10) | Spill duration Q4 | 0.035 | 0.008 *** | 0.019 * | [-0.002, 0.072] | *** → * |
| Rentals (11) | Spill duration Q4 | -0.054 | 0.020 *** | 0.035  | [-0.123, 0.015] | *** → none |
| Rentals (12) | Spill duration Q4 | 0.015 | 0.013  | 0.017  | [-0.018, 0.048] |  |

## Bins, prior window (count and hours)

`hedonic_count_bins_prior.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill count Q1 | 0.003 | 0.006  | 0.027  | [-0.050, 0.056] |  |
| Sales (2) | Spill count Q1 | 0.043 | 0.006 *** | 0.010 *** | [0.023, 0.063] |  |
| Sales (3) | Spill count Q1 | 0.058 | 0.006 *** | 0.009 *** | [0.040, 0.076] |  |
| Sales (4) | Spill count Q1 | 0.047 | 0.007 *** | 0.009 *** | [0.029, 0.065] |  |
| Sales (5) | Spill count Q1 | 0.061 | 0.006 *** | 0.008 *** | [0.045, 0.077] |  |
| Rentals (6) | Spill count Q1 | 0.005 | 0.007  | 0.041  | [-0.075, 0.085] |  |
| Rentals (7) | Spill count Q1 | 0.109 | 0.006 *** | 0.011 *** | [0.087, 0.131] |  |
| Rentals (8) | Spill count Q1 | 0.100 | 0.004 *** | 0.009 *** | [0.082, 0.118] |  |
| Rentals (9) | Spill count Q1 | 0.108 | 0.007 *** | 0.010 *** | [0.088, 0.128] |  |
| Rentals (10) | Spill count Q1 | 0.109 | 0.005 *** | 0.009 *** | [0.091, 0.127] |  |
| Sales (1) | Spill count Q2 | 0.000 | 0.006  | 0.029  | [-0.057, 0.057] |  |
| Sales (2) | Spill count Q2 | 0.018 | 0.007 *** | 0.014  | [-0.009, 0.045] | *** → none |
| Sales (3) | Spill count Q2 | 0.045 | 0.006 *** | 0.011 *** | [0.023, 0.067] |  |
| Sales (4) | Spill count Q2 | 0.022 | 0.008 *** | 0.012 * | [-0.002, 0.046] | *** → * |
| Sales (5) | Spill count Q2 | 0.047 | 0.007 *** | 0.010 *** | [0.027, 0.067] |  |
| Rentals (6) | Spill count Q2 | 0.094 | 0.008 *** | 0.055 * | [-0.014, 0.202] | *** → * |
| Rentals (7) | Spill count Q2 | 0.113 | 0.007 *** | 0.016 *** | [0.082, 0.144] |  |
| Rentals (8) | Spill count Q2 | 0.109 | 0.005 *** | 0.011 *** | [0.087, 0.131] |  |
| Rentals (9) | Spill count Q2 | 0.126 | 0.009 *** | 0.017 *** | [0.093, 0.159] |  |
| Rentals (10) | Spill count Q2 | 0.126 | 0.006 *** | 0.012 *** | [0.102, 0.150] |  |
| Sales (1) | Spill count Q3 | -0.051 | 0.006 *** | 0.027 * | [-0.104, 0.002] | *** → * |
| Sales (2) | Spill count Q3 | 0.012 | 0.007 * | 0.014  | [-0.015, 0.039] | * → none |
| Sales (3) | Spill count Q3 | 0.034 | 0.006 *** | 0.012 *** | [0.010, 0.058] |  |
| Sales (4) | Spill count Q3 | 0.006 | 0.008  | 0.015  | [-0.023, 0.035] |  |
| Sales (5) | Spill count Q3 | 0.032 | 0.008 *** | 0.012 *** | [0.008, 0.056] |  |
| Rentals (6) | Spill count Q3 | 0.036 | 0.008 *** | 0.049  | [-0.060, 0.132] | *** → none |
| Rentals (7) | Spill count Q3 | 0.091 | 0.008 *** | 0.016 *** | [0.060, 0.122] |  |
| Rentals (8) | Spill count Q3 | 0.087 | 0.005 *** | 0.013 *** | [0.062, 0.112] |  |
| Rentals (9) | Spill count Q3 | 0.093 | 0.009 *** | 0.017 *** | [0.060, 0.126] |  |
| Rentals (10) | Spill count Q3 | 0.096 | 0.006 *** | 0.012 *** | [0.072, 0.120] |  |
| Sales (1) | Spill count Q4 | -0.086 | 0.006 *** | 0.034 *** | [-0.153, -0.019] |  |
| Sales (2) | Spill count Q4 | -0.003 | 0.008  | 0.016  | [-0.034, 0.028] |  |
| Sales (3) | Spill count Q4 | 0.022 | 0.007 *** | 0.013 * | [-0.003, 0.047] | *** → * |
| Sales (4) | Spill count Q4 | -0.004 | 0.009  | 0.016  | [-0.035, 0.027] |  |
| Sales (5) | Spill count Q4 | 0.024 | 0.008 *** | 0.013 * | [-0.001, 0.049] | *** → * |
| Rentals (6) | Spill count Q4 | -0.051 | 0.007 *** | 0.056  | [-0.161, 0.059] | *** → none |
| Rentals (7) | Spill count Q4 | 0.049 | 0.008 *** | 0.016 *** | [0.018, 0.080] |  |
| Rentals (8) | Spill count Q4 | 0.054 | 0.005 *** | 0.014 *** | [0.027, 0.081] |  |
| Rentals (9) | Spill count Q4 | 0.050 | 0.010 *** | 0.020 ** | [0.011, 0.089] | *** → ** |
| Rentals (10) | Spill count Q4 | 0.054 | 0.007 *** | 0.016 *** | [0.023, 0.085] |  |

`hedonic_hrs_bins_prior.tex` (old side: heteroskedasticity-robust re-run on current data; snapshot was stale)

| Column | Coefficient | Estimate | Old SE (stars) | New SE (stars) | New 95% CI | Star change |
|---|---|---|---|---|---|---|
| Sales (1) | Spill hours Q1 | -0.065 | 0.006 *** | 0.028 ** | [-0.120, -0.010] | *** → ** |
| Sales (2) | Spill hours Q1 | 0.032 | 0.006 *** | 0.011 *** | [0.010, 0.054] |  |
| Sales (3) | Spill hours Q1 | 0.049 | 0.006 *** | 0.009 *** | [0.031, 0.067] |  |
| Sales (4) | Spill hours Q1 | 0.041 | 0.007 *** | 0.009 *** | [0.023, 0.059] |  |
| Sales (5) | Spill hours Q1 | 0.054 | 0.006 *** | 0.008 *** | [0.038, 0.070] |  |
| Rentals (6) | Spill hours Q1 | -0.027 | 0.007 *** | 0.039  | [-0.103, 0.049] | *** → none |
| Rentals (7) | Spill hours Q1 | 0.109 | 0.006 *** | 0.013 *** | [0.084, 0.134] |  |
| Rentals (8) | Spill hours Q1 | 0.097 | 0.004 *** | 0.010 *** | [0.077, 0.117] |  |
| Rentals (9) | Spill hours Q1 | 0.110 | 0.007 *** | 0.012 *** | [0.086, 0.134] |  |
| Rentals (10) | Spill hours Q1 | 0.107 | 0.005 *** | 0.009 *** | [0.089, 0.125] |  |
| Sales (1) | Spill hours Q2 | -0.032 | 0.006 *** | 0.024  | [-0.079, 0.015] | *** → none |
| Sales (2) | Spill hours Q2 | 0.023 | 0.007 *** | 0.012 * | [-0.001, 0.047] | *** → * |
| Sales (3) | Spill hours Q2 | 0.051 | 0.006 *** | 0.010 *** | [0.031, 0.071] |  |
| Sales (4) | Spill hours Q2 | 0.029 | 0.008 *** | 0.012 ** | [0.005, 0.053] | *** → ** |
| Sales (5) | Spill hours Q2 | 0.054 | 0.007 *** | 0.010 *** | [0.034, 0.074] |  |
| Rentals (6) | Spill hours Q2 | -0.003 | 0.008  | 0.032  | [-0.066, 0.060] |  |
| Rentals (7) | Spill hours Q2 | 0.097 | 0.007 *** | 0.013 *** | [0.072, 0.122] |  |
| Rentals (8) | Spill hours Q2 | 0.099 | 0.005 *** | 0.010 *** | [0.079, 0.119] |  |
| Rentals (9) | Spill hours Q2 | 0.111 | 0.008 *** | 0.013 *** | [0.086, 0.136] |  |
| Rentals (10) | Spill hours Q2 | 0.119 | 0.006 *** | 0.009 *** | [0.101, 0.137] |  |
| Sales (1) | Spill hours Q3 | -0.013 | 0.006 ** | 0.030  | [-0.072, 0.046] | ** → none |
| Sales (2) | Spill hours Q3 | 0.020 | 0.007 *** | 0.014  | [-0.007, 0.047] | *** → none |
| Sales (3) | Spill hours Q3 | 0.041 | 0.006 *** | 0.011 *** | [0.019, 0.063] |  |
| Sales (4) | Spill hours Q3 | 0.023 | 0.008 *** | 0.014 * | [-0.004, 0.050] | *** → * |
| Sales (5) | Spill hours Q3 | 0.050 | 0.008 *** | 0.011 *** | [0.028, 0.072] |  |
| Rentals (6) | Spill hours Q3 | 0.119 | 0.008 *** | 0.060 ** | [0.001, 0.237] | *** → ** |
| Rentals (7) | Spill hours Q3 | 0.108 | 0.008 *** | 0.016 *** | [0.077, 0.139] |  |
| Rentals (8) | Spill hours Q3 | 0.104 | 0.005 *** | 0.011 *** | [0.082, 0.126] |  |
| Rentals (9) | Spill hours Q3 | 0.105 | 0.009 *** | 0.015 *** | [0.076, 0.134] |  |
| Rentals (10) | Spill hours Q3 | 0.111 | 0.007 *** | 0.011 *** | [0.089, 0.133] |  |
| Sales (1) | Spill hours Q4 | -0.024 | 0.006 *** | 0.032  | [-0.087, 0.039] | *** → none |
| Sales (2) | Spill hours Q4 | 0.017 | 0.007 ** | 0.014  | [-0.010, 0.044] | ** → none |
| Sales (3) | Spill hours Q4 | 0.034 | 0.007 *** | 0.012 *** | [0.010, 0.058] |  |
| Sales (4) | Spill hours Q4 | 0.004 | 0.009  | 0.015  | [-0.025, 0.033] |  |
| Sales (5) | Spill hours Q4 | 0.029 | 0.008 *** | 0.012 ** | [0.005, 0.053] | *** → ** |
| Rentals (6) | Spill hours Q4 | -0.014 | 0.008 * | 0.058  | [-0.128, 0.100] | * → none |
| Rentals (7) | Spill hours Q4 | 0.061 | 0.008 *** | 0.015 *** | [0.032, 0.090] |  |
| Rentals (8) | Spill hours Q4 | 0.066 | 0.006 *** | 0.012 *** | [0.042, 0.090] |  |
| Rentals (9) | Spill hours Q4 | 0.057 | 0.010 *** | 0.018 *** | [0.022, 0.092] |  |
| Rentals (10) | Spill hours Q4 | 0.066 | 0.007 *** | 0.013 *** | [0.041, 0.091] |  |

## Star changes summary

Every spill-exposure coefficient whose star level changed, from the most important table to the least. "none" means not significant at 10%. No coefficient gained stars.

- `hedonic_count_continuous_prior_250m`, Sales (2), Spills per week (avg.): -0.023, *** → * (SE 0.002 → 0.012)
- `hedonic_count_continuous_prior_250m`, Sales (4), Spills per week (avg.): -0.009, *** → ** (SE 0.002 → 0.005)
- `hedonic_count_continuous_prior_250m`, Sales (6), Spills per week (avg.): -0.011, *** → ** (SE 0.003 → 0.004)
- `hedonic_count_continuous_prior_radius_robustness`, Sales 250m, MSOA FE, Spills per week (avg.): -0.009, *** → ** (SE 0.002 → 0.005)
- `hedonic_count_continuous_prior_radius_robustness`, Sales 500m, MSOA FE, Spills per week (avg.): -0.006, *** → ** (SE 0.001 → 0.003)
- `hedonic_count_continuous_prior_radius_robustness`, Sales 250m, LSOA FE, Spills per week (avg.): -0.011, *** → ** (SE 0.003 → 0.004)
- `hedonic_count_continuous_prior_idw_250m`, Sales (2), Spills per week (avg., dist.-weighted): -0.018, *** → none (SE 0.003 → 0.012)
- `hedonic_count_continuous_prior_idw_250m`, Rentals (7), Spills per week (avg., dist.-weighted): -0.043, *** → ** (SE 0.003 → 0.019)
- `hedonic_count_continuous_prior_salience_groups`, Sales Inland, Spills per week (avg.): -0.008, ** → * (SE 0.003 → 0.005)
- `hedonic_count_continuous_prior_salience_groups`, Rentals Coastal, Spills per week (avg.): -0.017, *** → * (SE 0.006 → 0.009)
- `hedonic_count_bins_full`, Sales (2), Spill count Q1: -0.038, *** → none (SE 0.009 → 0.046)
- `hedonic_count_bins_full`, Rentals (7), Spill count Q1: -0.081, *** → none (SE 0.009 → 0.064)
- `hedonic_count_bins_full`, Rentals (8), Spill count Q1: -0.061, *** → none (SE 0.008 → 0.055)
- `hedonic_count_bins_full`, Rentals (9), Spill count Q1: 0.028, ** → none (SE 0.012 → 0.028)
- `hedonic_count_bins_full`, Rentals (10), Spill count Q1: 0.031, *** → * (SE 0.008 → 0.018)
- `hedonic_count_bins_full`, Rentals (12), Spill count Q1: 0.021, * → none (SE 0.012 → 0.017)
- `hedonic_count_bins_full`, Sales (1), Spill count Q2: -0.043, *** → none (SE 0.009 → 0.051)
- `hedonic_count_bins_full`, Sales (2), Spill count Q2: -0.074, *** → none (SE 0.009 → 0.048)
- `hedonic_count_bins_full`, Sales (5), Spill count Q2: -0.038, * → none (SE 0.020 → 0.041)
- `hedonic_count_bins_full`, Sales (6), Spill count Q2: -0.030, * → none (SE 0.018 → 0.033)
- `hedonic_count_bins_full`, Rentals (9), Spill count Q2: 0.031, ** → none (SE 0.012 → 0.030)
- `hedonic_count_bins_full`, Rentals (10), Spill count Q2: 0.027, *** → none (SE 0.008 → 0.019)
- `hedonic_count_bins_full`, Rentals (11), Spill count Q2: -0.059, *** → * (SE 0.020 → 0.034)
- `hedonic_count_bins_full`, Sales (1), Spill count Q3: -0.079, *** → none (SE 0.009 → 0.049)
- `hedonic_count_bins_full`, Sales (2), Spill count Q3: -0.103, *** → ** (SE 0.009 → 0.046)
- `hedonic_count_bins_full`, Sales (5), Spill count Q3: -0.062, *** → none (SE 0.020 → 0.042)
- `hedonic_count_bins_full`, Sales (6), Spill count Q3: -0.034, * → none (SE 0.018 → 0.034)
- `hedonic_count_bins_full`, Rentals (7), Spill count Q3: -0.072, *** → none (SE 0.010 → 0.072)
- `hedonic_count_bins_full`, Rentals (8), Spill count Q3: -0.055, *** → none (SE 0.008 → 0.065)
- `hedonic_count_bins_full`, Rentals (9), Spill count Q3: 0.033, *** → none (SE 0.013 → 0.033)
- `hedonic_count_bins_full`, Rentals (10), Spill count Q3: 0.043, *** → ** (SE 0.008 → 0.020)
- `hedonic_count_bins_full`, Rentals (11), Spill count Q3: -0.034, * → none (SE 0.020 → 0.034)
- `hedonic_count_bins_full`, Rentals (12), Spill count Q3: 0.022, * → none (SE 0.013 → 0.019)
- `hedonic_count_bins_full`, Sales (1), Spill count Q4: -0.104, *** → ** (SE 0.009 → 0.051)
- `hedonic_count_bins_full`, Sales (2), Spill count Q4: -0.102, *** → ** (SE 0.009 → 0.048)
- `hedonic_count_bins_full`, Sales (5), Spill count Q4: -0.041, ** → none (SE 0.020 → 0.042)
- `hedonic_count_bins_full`, Rentals (7), Spill count Q4: -0.153, *** → ** (SE 0.009 → 0.071)
- `hedonic_count_bins_full`, Rentals (8), Spill count Q4: -0.129, *** → ** (SE 0.008 → 0.062)
- `hedonic_count_bins_full`, Rentals (10), Spill count Q4: 0.026, *** → none (SE 0.008 → 0.020)
- `hedonic_count_bins_full`, Rentals (11), Spill count Q4: -0.055, *** → * (SE 0.019 → 0.032)
- `hedonic_count_continuous_full`, Sales (2), Spills per week (avg.): -0.027, *** → none (SE 0.003 → 0.017)
- `hedonic_count_continuous_full`, Sales (3), Spills per week (avg.): -0.014, *** → * (SE 0.004 → 0.008)
- `hedonic_count_continuous_full`, Sales (5), Spills per week (avg.): -0.014, *** → none (SE 0.005 → 0.010)
- `hedonic_count_continuous_full`, Rentals (7), Spills per week (avg.): -0.063, *** → ** (SE 0.003 → 0.027)
- `hedonic_count_continuous_full`, Rentals (8), Spills per week (avg.): -0.054, *** → ** (SE 0.003 → 0.023)
- `hedonic_count_continuous_full`, Rentals (9), Spills per week (avg.): -0.010, *** → none (SE 0.003 → 0.007)
- `hedonic_count_continuous_full`, Rentals (11), Spills per week (avg.): -0.013, *** → none (SE 0.004 → 0.008)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Sales (2), Spills per week (avg.): -0.021, *** → * (SE 0.002 → 0.013)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Sales (3), Spills per week (avg.): -0.010, *** → * (SE 0.003 → 0.005)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Sales (5), Spills per week (avg.): -0.006, * → none (SE 0.003 → 0.006)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Rentals (8), Spills per week (avg.): -0.037, *** → ** (SE 0.002 → 0.014)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Rentals (9), Spills per week (avg.): -0.006, ** → none (SE 0.002 → 0.005)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Rentals (11), Spills per week (avg.): -0.006, ** → none (SE 0.003 → 0.004)
- `hedonic_count_continuous_prior_qtr_fe_250m`, Rentals (12), Spills per week (avg.): -0.004, * → none (SE 0.002 → 0.003)
- `hedonic_count_continuous_prior_500m`, Sales (4), Spills per week (avg.): -0.006, *** → ** (SE 0.001 → 0.003)
- `hedonic_count_continuous_prior_idw_1000m`, Sales (4), Spills per week (avg., dist.-weighted): -0.013, *** → ** (SE 0.002 → 0.005)
- `hedonic_count_continuous_prior_qtr_fe_500m`, Sales (3), Spills per week (avg.): -0.009, *** → ** (SE 0.001 → 0.003)
- `hedonic_count_continuous_prior_qtr_fe_500m`, Rentals (12), Spills per week (avg.): -0.004, *** → ** (SE 0.001 → 0.002)
- `hedonic_count_continuous_prior_qtr_fe_1000m`, Sales (6), Spills per week (avg.): -0.004, *** → ** (SE 0.001 → 0.002)
- `hedonic_count_continuous_prior_qtr_fe_1000m`, Rentals (10), Spills per week (avg.): -0.003, *** → * (SE 0.001 → 0.001)
- `hedonic_count_continuous_prior_qtr_fe_1000m`, Rentals (12), Spills per week (avg.): -0.002, *** → none (SE 0.001 → 0.002)
- `hedonic_hrs_continuous_prior_250m`, Sales (1), Spill hours per week (avg.): 0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_250m`, Sales (2), Spill hours per week (avg.): 0.000, * → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_250m`, Sales (3), Spill hours per week (avg.): 0.000, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_250m`, Sales (4), Spill hours per week (avg.): 0.000, * → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_250m`, Sales (5), Spill hours per week (avg.): -0.001, *** → * (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_250m`, Sales (6), Spill hours per week (avg.): -0.001, *** → ** (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_250m`, Rentals (7), Spill hours per week (avg.): -0.004, *** → ** (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_prior_250m`, Rentals (8), Spill hours per week (avg.): -0.003, *** → ** (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_500m`, Sales (1), Spill hours per week (avg.): -0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_500m`, Sales (2), Spill hours per week (avg.): -0.001, *** → * (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_500m`, Sales (3), Spill hours per week (avg.): 0.000, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_500m`, Sales (4), Spill hours per week (avg.): 0.000, *** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_idw_250m`, Sales (1), Spill hours per week (avg., dist.-weighted): 0.001, ** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_idw_250m`, Sales (2), Spill hours per week (avg., dist.-weighted): 0.001, ** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_idw_250m`, Sales (3), Spill hours per week (avg., dist.-weighted): -0.001, * → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_idw_250m`, Sales (5), Spill hours per week (avg., dist.-weighted): -0.001, *** → ** (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_idw_250m`, Sales (6), Spill hours per week (avg., dist.-weighted): -0.001, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_idw_250m`, Rentals (7), Spill hours per week (avg., dist.-weighted): -0.005, *** → ** (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_prior_idw_250m`, Rentals (8), Spill hours per week (avg., dist.-weighted): -0.004, *** → ** (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_prior_idw_500m`, Sales (1), Spill hours per week (avg., dist.-weighted): -0.002, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_idw_500m`, Sales (2), Spill hours per week (avg., dist.-weighted): -0.002, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_idw_500m`, Sales (3), Spill hours per week (avg., dist.-weighted): -0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Sales (1), Spill hours per week (avg.): 0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Sales (2), Spill hours per week (avg.): 0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Sales (4), Spill hours per week (avg.): 0.000, * → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Sales (6), Spill hours per week (avg.): 0.000, * → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Rentals (7), Spill hours per week (avg.): -0.004, *** → ** (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Rentals (8), Spill hours per week (avg.): -0.002, *** → * (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_250m`, Rentals (11), Spill hours per week (avg.): -0.001, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_500m`, Sales (1), Spill hours per week (avg.): -0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_500m`, Sales (2), Spill hours per week (avg.): -0.001, *** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_prior_qtr_fe_500m`, Sales (3), Spill hours per week (avg.): 0.000, * → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_500m`, Sales (4), Spill hours per week (avg.): 0.000, *** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_500m`, Rentals (11), Spill hours per week (avg.): 0.000, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_1000m`, Sales (4), Spill hours per week (avg.): 0.000, ** → none (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_prior_qtr_fe_1000m`, Sales (6), Spill hours per week (avg.): 0.000, *** → ** (SE 0.000 → 0.000)
- `hedonic_hrs_continuous_full`, Sales (1), Spill hours per week (avg.): 0.003, *** → * (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_full`, Sales (2), Spill hours per week (avg.): 0.002, *** → none (SE 0.000 → 0.002)
- `hedonic_hrs_continuous_full`, Sales (4), Spill hours per week (avg.): 0.001, ** → none (SE 0.000 → 0.001)
- `hedonic_hrs_continuous_full`, Sales (6), Spill hours per week (avg.): 0.001, * → none (SE 0.001 → 0.001)
- `hedonic_hrs_continuous_full`, Rentals (7), Spill hours per week (avg.): -0.004, *** → none (SE 0.000 → 0.003)
- `hedonic_hrs_continuous_full`, Rentals (8), Spill hours per week (avg.): -0.003, *** → none (SE 0.000 → 0.003)
- `hedonic_hrs_continuous_full`, Rentals (11), Spill hours per week (avg.): -0.001, ** → none (SE 0.001 → 0.001)
- `hedonic_hrs_bins_full`, Sales (1), Spill duration Q1: -0.082, *** → none (SE 0.009 → 0.051)
- `hedonic_hrs_bins_full`, Sales (2), Spill duration Q1: -0.094, *** → ** (SE 0.009 → 0.048)
- `hedonic_hrs_bins_full`, Rentals (7), Spill duration Q1: -0.103, *** → none (SE 0.009 → 0.064)
- `hedonic_hrs_bins_full`, Rentals (8), Spill duration Q1: -0.085, *** → none (SE 0.008 → 0.055)
- `hedonic_hrs_bins_full`, Rentals (9), Spill duration Q1: 0.035, *** → none (SE 0.012 → 0.029)
- `hedonic_hrs_bins_full`, Rentals (10), Spill duration Q1: 0.029, *** → none (SE 0.008 → 0.018)
- `hedonic_hrs_bins_full`, Sales (1), Spill duration Q2: -0.060, *** → none (SE 0.009 → 0.048)
- `hedonic_hrs_bins_full`, Sales (2), Spill duration Q2: -0.081, *** → * (SE 0.009 → 0.044)
- `hedonic_hrs_bins_full`, Sales (5), Spill duration Q2: -0.042, ** → none (SE 0.020 → 0.041)
- `hedonic_hrs_bins_full`, Rentals (7), Spill duration Q2: -0.091, *** → * (SE 0.009 → 0.052)
- `hedonic_hrs_bins_full`, Rentals (8), Spill duration Q2: -0.072, *** → none (SE 0.008 → 0.047)
- `hedonic_hrs_bins_full`, Rentals (9), Spill duration Q2: 0.022, * → none (SE 0.012 → 0.029)
- `hedonic_hrs_bins_full`, Rentals (10), Spill duration Q2: 0.027, *** → none (SE 0.008 → 0.018)
- `hedonic_hrs_bins_full`, Rentals (11), Spill duration Q2: -0.044, ** → none (SE 0.020 → 0.032)
- `hedonic_hrs_bins_full`, Sales (1), Spill duration Q3: -0.063, *** → none (SE 0.009 → 0.052)
- `hedonic_hrs_bins_full`, Sales (2), Spill duration Q3: -0.082, *** → * (SE 0.009 → 0.049)
- `hedonic_hrs_bins_full`, Sales (5), Spill duration Q3: -0.045, ** → none (SE 0.020 → 0.042)
- `hedonic_hrs_bins_full`, Rentals (7), Spill duration Q3: 0.020, ** → none (SE 0.010 → 0.078)
- `hedonic_hrs_bins_full`, Rentals (9), Spill duration Q3: 0.038, *** → none (SE 0.012 → 0.033)
- `hedonic_hrs_bins_full`, Rentals (10), Spill duration Q3: 0.041, *** → ** (SE 0.008 → 0.021)
- `hedonic_hrs_bins_full`, Rentals (11), Spill duration Q3: -0.037, * → none (SE 0.020 → 0.031)
- `hedonic_hrs_bins_full`, Sales (1), Spill duration Q4: -0.032, *** → none (SE 0.009 → 0.050)
- `hedonic_hrs_bins_full`, Sales (2), Spill duration Q4: -0.060, *** → none (SE 0.009 → 0.047)
- `hedonic_hrs_bins_full`, Sales (5), Spill duration Q4: -0.047, ** → none (SE 0.020 → 0.042)
- `hedonic_hrs_bins_full`, Rentals (7), Spill duration Q4: -0.127, *** → * (SE 0.009 → 0.070)
- `hedonic_hrs_bins_full`, Rentals (8), Spill duration Q4: -0.096, *** → none (SE 0.008 → 0.062)
- `hedonic_hrs_bins_full`, Rentals (10), Spill duration Q4: 0.035, *** → * (SE 0.008 → 0.019)
- `hedonic_hrs_bins_full`, Rentals (11), Spill duration Q4: -0.054, *** → none (SE 0.020 → 0.035)
- `hedonic_count_bins_prior`, Sales (2), Spill count Q2: 0.018, *** → none (SE 0.007 → 0.014)
- `hedonic_count_bins_prior`, Sales (4), Spill count Q2: 0.022, *** → * (SE 0.008 → 0.012)
- `hedonic_count_bins_prior`, Rentals (6), Spill count Q2: 0.094, *** → * (SE 0.008 → 0.055)
- `hedonic_count_bins_prior`, Sales (1), Spill count Q3: -0.051, *** → * (SE 0.006 → 0.027)
- `hedonic_count_bins_prior`, Sales (2), Spill count Q3: 0.012, * → none (SE 0.007 → 0.014)
- `hedonic_count_bins_prior`, Rentals (6), Spill count Q3: 0.036, *** → none (SE 0.008 → 0.049)
- `hedonic_count_bins_prior`, Sales (3), Spill count Q4: 0.022, *** → * (SE 0.007 → 0.013)
- `hedonic_count_bins_prior`, Sales (5), Spill count Q4: 0.024, *** → * (SE 0.008 → 0.013)
- `hedonic_count_bins_prior`, Rentals (6), Spill count Q4: -0.051, *** → none (SE 0.007 → 0.056)
- `hedonic_count_bins_prior`, Rentals (9), Spill count Q4: 0.050, *** → ** (SE 0.010 → 0.020)
- `hedonic_hrs_bins_prior`, Sales (1), Spill hours Q1: -0.065, *** → ** (SE 0.006 → 0.028)
- `hedonic_hrs_bins_prior`, Rentals (6), Spill hours Q1: -0.027, *** → none (SE 0.007 → 0.039)
- `hedonic_hrs_bins_prior`, Sales (1), Spill hours Q2: -0.032, *** → none (SE 0.006 → 0.024)
- `hedonic_hrs_bins_prior`, Sales (2), Spill hours Q2: 0.023, *** → * (SE 0.007 → 0.012)
- `hedonic_hrs_bins_prior`, Sales (4), Spill hours Q2: 0.029, *** → ** (SE 0.008 → 0.012)
- `hedonic_hrs_bins_prior`, Sales (1), Spill hours Q3: -0.013, ** → none (SE 0.006 → 0.030)
- `hedonic_hrs_bins_prior`, Sales (2), Spill hours Q3: 0.020, *** → none (SE 0.007 → 0.014)
- `hedonic_hrs_bins_prior`, Sales (4), Spill hours Q3: 0.023, *** → * (SE 0.008 → 0.014)
- `hedonic_hrs_bins_prior`, Rentals (6), Spill hours Q3: 0.119, *** → ** (SE 0.008 → 0.060)
- `hedonic_hrs_bins_prior`, Sales (1), Spill hours Q4: -0.024, *** → none (SE 0.006 → 0.032)
- `hedonic_hrs_bins_prior`, Sales (2), Spill hours Q4: 0.017, ** → none (SE 0.007 → 0.014)
- `hedonic_hrs_bins_prior`, Sales (5), Spill hours Q4: 0.029, *** → ** (SE 0.008 → 0.012)
- `hedonic_hrs_bins_prior`, Rentals (6), Spill hours Q4: -0.014, * → none (SE 0.008 → 0.058)

## Verification

**Conley note in each written file.** `grep -c "Conley standard errors" output/tables/<file>` for the 26 files the seven scripts wrote in this run:

| File | Count |
|---|---|
| `hedonic_count_bins_full.tex` | 1 |
| `hedonic_count_bins_prior.tex` | 0 |
| `hedonic_count_continuous_full.tex` | 1 |
| `hedonic_count_continuous_prior_1000m.tex` | 1 |
| `hedonic_count_continuous_prior_250m.tex` | 1 |
| `hedonic_count_continuous_prior_500m.tex` | 1 |
| `hedonic_count_continuous_prior_idw_1000m.tex` | 1 |
| `hedonic_count_continuous_prior_idw_250m.tex` | 1 |
| `hedonic_count_continuous_prior_idw_500m.tex` | 1 |
| `hedonic_count_continuous_prior_qtr_fe_1000m.tex` | 1 |
| `hedonic_count_continuous_prior_qtr_fe_250m.tex` | 1 |
| `hedonic_count_continuous_prior_qtr_fe_500m.tex` | 1 |
| `hedonic_count_continuous_prior_radius_robustness.tex` | 1 |
| `hedonic_count_continuous_prior_salience_groups.tex` | 1 |
| `hedonic_hrs_bins_full.tex` | 1 |
| `hedonic_hrs_bins_prior.tex` | 0 |
| `hedonic_hrs_continuous_full.tex` | 1 |
| `hedonic_hrs_continuous_prior_1000m.tex` | 1 |
| `hedonic_hrs_continuous_prior_250m.tex` | 1 |
| `hedonic_hrs_continuous_prior_500m.tex` | 1 |
| `hedonic_hrs_continuous_prior_idw_1000m.tex` | 1 |
| `hedonic_hrs_continuous_prior_idw_250m.tex` | 1 |
| `hedonic_hrs_continuous_prior_idw_500m.tex` | 1 |
| `hedonic_hrs_continuous_prior_qtr_fe_1000m.tex` | 1 |
| `hedonic_hrs_continuous_prior_qtr_fe_250m.tex` | 1 |
| `hedonic_hrs_continuous_prior_qtr_fe_500m.tex` | 1 |

The two `bins_prior` files return 0 because that script writes no notes block, which is expected; only its estimator changed.

**No heteroskedasticity-robust note in the rewritten files.** None of the 26 files written in this run contains the string `Heteroskedasticity-robust`. The string does remain in 29 other `output/tables/hedonic_*.tex` files that none of the seven scripts writes. They were not touched and are left over from other or earlier scripts:

- `hedonic_count_continuous_prior.tex`, `hedonic_hrs_continuous_prior.tex`, `hedonic_count_continuous_prior_qtr_fe.tex` and `hedonic_hrs_continuous_prior_qtr_fe.tex` (unsuffixed files from before the scripts started writing per-radius files; the quarter fixed effects script no longer writes an unsuffixed file);
- `hedonic_count_continuous_prior_one_site.tex` and `hedonic_count_continuous_updown_full.tex`;
- `hedonic_count_continuous_prior_salience_coast_bathing.tex` and `hedonic_count_continuous_prior_salience_robust_{coast10km,dropunknown,london}.tex` (the current salience script writes only `hedonic_count_continuous_prior_salience_groups.tex`);
- 19 `hedonic_hrs_continuous_prior_direction*.tex` files.

**Samples did not change.** The row counts printed by each script, which match the observation rows in the old and new tables:

| Script | Sample | Sales | Rentals |
|---|---|---|---|
| `hedonic_continuous_prior.R` | 250 m | 166,192 | 46,424 |
| | 500 m | 523,626 | 135,327 |
| | 1000 m | 1,109,285 | 270,123 |
| `hedonic_continuous_prior_idw.R` | 250 m | 166,192 | 46,424 |
| | 500 m | 523,626 | 135,327 |
| | 1000 m | 1,109,285 | 270,123 |
| `hedonic_continuous_prior_qtr_fe.R` | 250 m | 166,192 | 46,424 |
| | 500 m | 523,626 | 135,327 |
| | 1000 m | 1,109,285 | 270,123 |
| `hedonic_bins_prior.R` | 250 m | 166,192 | 46,424 |
| `hedonic_continuous_full.R` | 250 m, full window | 163,561 | 46,014 |
| `hedonic_bins_full.R` | 250 m, full window | 163,561 | 46,014 |
| `hedonic_continuous_prior_salience.R` | Bathing, before estimator removals | 15,433 | 2,783 |
| | Coastal | 34,497 | 8,821 |
| | Inland | 125,621 | 31,765 |

The observation counts after fixed-effect singleton removal (for example 165,725 sales and 45,400 rentals in the 250 m LSOA columns, and 15,384 / 34,399 / 125,247 sales and 2,674 / 8,617 / 30,958 rentals in the salience groups) are identical in the old and new tables. No observations were dropped for missing coordinates: property latitude and longitude are missing only where LSOA is also missing, and every script already drops rows with a missing LSOA.

**Coordinates reach the estimator.** Six scripts keep every column of the property file except a list of dropped address and date fields, so `latitude` and `longitude` pass through the join and `conley()` detects them. The salience script selected an explicit column list without coordinates, so `latitude` and `longitude` were added to that list. With `lean = TRUE`, the Conley covariance is computed at estimation time and stored on the model, and the script's check that the spill coefficient and its standard error are finite passed for all six groups.
