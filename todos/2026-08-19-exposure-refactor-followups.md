# Exposure-builder refactor follow-ups (post-Stage-2)

Stage 2 of `docs/plans/2026-08-17-002-refactor-layered-exposure-builders-plan.md`
became canonical on 2026-08-19. Two follow-ups remain, in this order:

1. **Post-Stage-2 standard analysis battery refresh.** Re-run the consumers in
   the inventory that are plain re-runs of published exposure datasets. The
   offender list below is excluded: its outputs must not be refreshed in this
   follow-up.
2. **Analysis-layer NA-convention cleanup.** Bring the offenders below onto the
   NA-Propagation Convention (see `CONCEPTS.md`), explicitly ordered
   **fix-first-then-refresh**: each offending script is fixed before its
   outputs are refreshed, never refreshed as-is. Partly blocked on the
   colleague-owned signed-pair CSVs.

## Offender list — do not refresh until fixed

These consumers override the engines' NA semantics and will silently swallow
the harmonized rule's additional NAs. **Their outputs must not be refreshed
after Stage 2 until they are fixed by follow-up 2.** The full evidence is in
`docs/wayfinder/exposure-builder-refactor/assets/02-drift-map.md`. No warning
comments are added to the offending scripts; this file is the single record.

- The twelve live `upstream_downstream_*` scripts under
  `scripts/R/09_analysis/06_upstream_downstream/`, which re-reduce site-grain
  output with `na.rm = TRUE`.
- `scripts/R/09_analysis/05_news/did_trends_full.R`, which coerces missing
  evidence to zero exposure via `na.rm = TRUE` and `replace_na(..., 0)`.
- The four `agg_spill_yr.parquet` re-aggregations:
  `grid_long_difference_sales.R`, `grid_long_difference_rentals.R`,
  `hedonic_continuous_full.R`, and `hedonic_bins_full.R`. The last of these now
  reads `study_period` post-merge, but its sibling summation path over
  `agg_spill_yr` remains in the family until follow-up 2 rules on it.
- `scripts/R/09_analysis/03_repeat_sales/repeat_sales.R`, which carries an
  independent quarterised windowing scheme with `na.rm = TRUE` cross-site sums.
