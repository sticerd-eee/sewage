---
title: Arrow collected columns make data.table joins nondeterministic
module: Cross-Section Exposure / Pipeline-wide
date: 2026-08-19
category: logic-errors
problem_type: logic_error
component: data-pipeline
symptoms:
  - "data.table::merge on a character key silently returns a strict subset of the true join result."
  - "Row counts vary across identical merge calls in one R process (e.g. 86,010 or 91,910 rows of a true 229,252)."
  - "setkey() reports success but leaves the key column unsorted."
  - "No warning, error, or NA — the partial result looks like a clean inner join."
root_cause: environment_specific_behavior
resolution_type: code_fix
severity: high
related_components: [arrow, data.table, grid-long-difference, dry-spills, rainfall-aggregation, prior-exposure, study-period-exposure]
tags: [arrow, altrep, data.table, merge, join, nondeterminism, parquet, r]
---

# Arrow collected columns make data.table joins nondeterministic

## Problem

On this machine (R 4.6.0, arrow 24.0.0, data.table 1.18.4, the `rv` project
library), a `data.table` merge or keyed join whose character key column comes
from **`arrow::open_dataset() |> dplyr::collect()` converted with
`as.data.table()`** silently returns a **nondeterministic subset** of the true
join result — for example 82,569 / 82,696 / 87,435 rows across three identical
calls whose true result is 229,252 rows. `setkey()` on such a table reports
success while leaving the column unsorted, which is the observable smoking gun.
There is no warning or error.

First seen in ticket 09 of the exposure-builder refactor and initially
attributed to arrow's ALTREP string vectors (hence this file's name). A probe
on 2026-08-19 (`scripts/R/testing/probe_altrep_join_safety.R`) **corrected
that attribution**:

- The collected column inspects as a **plain STRSXP**, not ALTREP, yet still
  breaks data.table.
- **`options(arrow.use_altrep = FALSE)` does NOT protect the
  `open_dataset() |> collect()` path.** It was previously believed sufficient;
  it is not.
- No incidental operation on the converted table rescues it: `data.table::copy()`,
  `setkey()`, and row subsetting all inherit the wrong behaviour.
- The single-file `arrow::read_parquet() |> as.data.table()` path joined
  correctly in the same probe. The verified failure shape is the
  multi-file/chunked `open_dataset()` collect.
- `chmatch()`, binary-search `dt[key == value]`, `match()`, and dplyr joins on
  the very same vectors are all correct — the failure is in data.table's
  join/sort machinery interacting with the collected column's memory.

Probe verdict table (real data, ~229k-row slice; two reps per pattern):

| conversion pattern | correct | deterministic |
|---|---|---|
| `as.data.table(collected)` | no | no |
| `copy(as.data.table(collected))` | no | no |
| `setkey` after conversion | no | no |
| row subset after conversion | no | no |
| `arrow.use_altrep = FALSE` then convert | no | no |
| `as.data.table(as.data.frame(collected))` | **yes** | yes |
| `data.table(col = collected$col)` (column extraction) | **yes** | yes |

## Safe patterns

- **`as.data.frame()` between `collect()` and `as.data.table()`** — the fix
  adopted across the pipeline; it materializes every column into ordinary R
  vectors.
- Column extraction (`data.table(k = tib$k, ...)`).
- dplyr joins, base `match()`, `chmatch()`.
- `options(arrow.use_altrep = FALSE)` is **not** a sufficient guard for
  `open_dataset()` collects. It remains in the five scripts guarded on
  2026-08-19 as harmless belt-and-braces for their `read_parquet()` reads, but
  it must never be relied on alone.

## Where the fix was applied (2026-08-19, static change only, nothing re-run)

Every live `open_dataset() |> collect() |> as.data.table()` site feeding
data.table joins got the `as.data.frame()` materialization:

- `scripts/R/utils/prior_exposure_utils.R` — the three loads in
  `prior_exposure_load_data` (transactions, lookup, events).
- `scripts/R/utils/cross_section_study_period_utils.R` —
  `study_period_read_parquet_columns()`, the single read path for the
  study-period engines.
- `scripts/R/testing/verify_study_period_exposure_sources.R` — both collects
  (a silent drop there would have under-reported divergence, a false green).
- `scripts/R/06_analysis_datasets/grid_long_difference_sales.R` —
  `load_spill_lookup_within_radius()`, the strongest exposed case.

The published exposure datasets themselves need no rebuild: the ticket-06 and
ticket-09 reconciliations proved them bit-identical across independent builds,
and the publication gates validate expected key sets, so a silent drop in a
past build would have failed the gate. The fix makes future runs safe by
construction instead of empirically lucky.

Paths on the `read_parquet()` shape (the two grid long-difference lookup
loads via `rio::import`/`read_parquet`, the three enrichment scripts joining
on `ngr`/`water_company`) joined correctly in the probe and additionally carry
the `arrow.use_altrep = FALSE` option from the first pass. If any of them is
ever moved onto `open_dataset()`, it must adopt the `as.data.frame()`
materialization.

## Prevention

When data collected from arrow will be joined, merged, or keyed with
data.table, always materialize first: `collect() |> as.data.frame() |>
as.data.table()`. Verify any new pattern with
`Rscript scripts/R/testing/probe_altrep_join_safety.R` (seconds, reads a
capped slice, rebuilds nothing). A `setkey()` that leaves the column unsorted
(`is.unsorted(dt$key)` after keying) is the cheap tell that a table is
affected.

Historical note: the original ticket-09 reproduction is at
`.scratch/exposure-builder-refactor/ticket09-logs/join_debug.R` (untracked
scratch); the committed probe supersedes it.
