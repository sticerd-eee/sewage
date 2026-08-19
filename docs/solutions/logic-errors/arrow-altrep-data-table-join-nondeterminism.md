---
title: Arrow ALTREP character keys make data.table joins nondeterministic
module: Cross-Section Exposure / Pipeline-wide
date: 2026-08-19
category: logic-errors
problem_type: logic_error
component: data-pipeline
symptoms:
  - "data.table::merge on a character key silently returns a strict subset of the true join result."
  - "Row counts vary across identical merge calls in one R process (e.g. 86,010 then 91,910 rows of a true 229,252)."
  - "No warning, error, or NA — the partial result looks like a clean inner join."
root_cause: environment_specific_behavior
resolution_type: workaround
severity: high
related_components: [arrow, data.table, grid-long-difference, dry-spills, rainfall-aggregation, prior-exposure, study-period-exposure]
tags: [arrow, altrep, data.table, merge, join, nondeterminism, parquet, r]
---

# Arrow ALTREP character keys make data.table joins nondeterministic

## Problem

On this machine (R 4.6.0, the `rv`-activated project library), a
`data.table` merge or keyed join whose key column is a **character vector
collected from arrow** (an ALTREP-backed string column from
`arrow::read_parquet()` or `open_dataset() |> collect()`) silently returns a
**nondeterministic subset** of the true join result. Observed during ticket 09
of the exposure-builder refactor: an inner join whose true result is 229,252
rows returned 86,010 rows on one call and 91,910 on the next, within a single
process, with no warning. `as.data.table()` on the collected tibble does **not**
remove the hazard — the bug reproduced through it.

Minimal reproduction:
`.scratch/exposure-builder-refactor/ticket09-logs/join_debug.R`.

## Safe patterns

All of these give the correct, deterministic result:

- `dplyr` joins (`inner_join`, `left_join`, ...).
- Base `match()` lookups.
- Full materialization before the join: `x[seq_along(x)]` on each column, which
  converts ALTREP vectors to plain R vectors.
- `options(arrow.use_altrep = FALSE)` set **before** collecting, so arrow hands
  back plain character vectors in the first place.

Integer, Date, and in-R-constructed character keys (e.g. `paste()`-built IDs)
have not shown the failure. Note that `as.character()` on an already-character
column is an identity operation and does not defeat ALTREP.

## Exposure check (static reading only, 2026-08-19)

A static sweep of every `scripts/R/` file using both arrow and data.table was
performed as part of ticket 10. Scope agreed with Jacopo: a check only, no
rebuilds — the ticket-06 and ticket-09 reconciliations already proved the
published exposure datasets correct (bit-identical across independent builds),
so canonical outputs needed no re-verification. Nothing was re-run.

Before the check, no script in `scripts/R/` set `arrow.use_altrep` or
deliberately materialized collected columns.

### Exposed paths (character key, arrow-derived, no protection) — now guarded

Each of these files received the one-line guard
`options(arrow.use_altrep = FALSE)` in its environment-setup function:

- `scripts/R/06_analysis_datasets/grid_long_difference_sales.R` — merges on
  `house_id` at lines ~251 and ~312; the spill-lookup loader deliberately
  collects lazily, so the key was ALTREP by construction.
- `scripts/R/06_analysis_datasets/grid_long_difference_rentals.R` — merges on
  `rental_id` at lines ~230 and ~291.
- `scripts/R/03_data_enrichment/aggregate_rainfall_stats.R` — keyed join on
  `(site_id, ngr)` at line ~216, with `ngr` a character column straight from
  `read_parquet`.
- `scripts/R/03_data_enrichment/identify_dry_spills.R` — keyed joins on
  `(site_id, ngr)` at lines ~194 and ~301.
- `scripts/R/03_data_enrichment/aggregate_dry_spill_stats.R` — merge at
  line ~276 whose computed `join_keys` include the character `water_company`.

The guard changes no join logic; it only makes arrow return plain vectors.
`scripts/R/testing/test_grid_long_difference_sales_contracts.R` asserts the
sales loader stays lazy until `collect()`; the option does not affect lazy
evaluation, only the representation of collected vectors.

### Uncertain paths (character key, protected only incidentally) — noted, not changed

These join on character IDs (`transaction_id`, `house_id`/`rental_id`, `ngr`,
`water_company`) where the key survives only via `data.table::copy()`, an
incidental row subset, or `setkey()` — none a deliberate materialization. It is
unknown whether these operations defeat ALTREP, and their outputs are either
already proven correct by reconciliation or owned by the analysis-layer
follow-ups, so they were left unchanged:

- `scripts/R/utils/prior_exposure_utils.R` — joins at lines ~363/367/369, ~514,
  ~598 (a rolling join), ~633, ~647, ~738, ~780, ~878, ~996, ~1260. The
  ticket-06 Stage-1 reconciliation proved these paths' outputs bit-identical,
  so they are correct as run; any future edit should add the altrep guard.
- `scripts/R/utils/cross_section_study_period_utils.R` — joins at lines ~674
  and ~722; same reconciliation evidence applies.
- `scripts/R/testing/verify_study_period_exposure_sources.R` — keyed join on
  `transaction_id` at line ~258. A silent drop here would make the verifier
  under-report divergence (a false green); worth the guard when the script is
  next touched.
- `scripts/R/03_data_enrichment/aggregate_dry_spill_stats.R` line ~232 and
  `aggregate_daily_spill_rainfall.R` line ~202 — `water_company` / `ngr` keys
  protected only by intervening aggregation or subsetting. The first file now
  carries the file-level guard anyway; the second's join sides both pass
  through `melt()`/`%in%`-subset reallocations.

### Safe by construction

`site_id` is coerced to integer throughout the exposure core; `x_idx`/`y_idx`,
`year`, and period IDs are integer; `spill_id` is `paste()`-built in R. The
site panels read through DuckDB, not arrow. The prior-exposure and study-period
contract tests use `setkey` for ordering only, never for keyed joins. The
Stage-2 sample-impact memo already guards itself (altrep off plus dplyr joins).

## Prevention

When joining arrow-collected data with data.table on a string key, either set
`options(arrow.use_altrep = FALSE)` before collecting or use dplyr joins.
Treat any new `X[Y, on = ...]` or `merge()` over a collected character column
as exposed until guarded.
