---
module: Individual EDM Combiner
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "The combine script carried low-value readability noise such as repeated `tolower(.)` calls, a redundant `!is.null(.)` filter check, and a one-off helper for final NA-count logging."
  - "The second datetime parse branch repeated a `POSIXct` guard that was already handled earlier, adding branching without changing behaviour."
  - "The year-boundary rules in `clean_time_range()` were correct but embedded directly inside one dense `filter()` expression, making them harder to audit."
  - "The user wanted a readability refactor without relaxing strict datetime validation, staged Parquet export, or downstream output compatibility."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, edm, data-cleaning, refactor, script-simplicity, runtime-validation]
---

# Troubleshooting: Safely simplifying the individual EDM combiner with runtime validation

## Problem
`scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R` was already doing the right safety-critical work, but parts of the implementation had become harder to scan than necessary. The goal was not to change behaviour or relax validation. It was to make a narrow set of readability-only simplifications and then replay the script end to end to confirm that the combined EDM output still looked correct.

## Environment
- Module: Individual EDM Combiner / `scripts/R/02_data_cleaning/`
- Affected component: Repository tooling / cleaning script
- Key file: `scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R`
- Primary output: `data/processed/combined_edm_data.parquet`
- Date solved: 2026-03-10

## Symptoms
- `clean_data()` repeated `tolower(.)` across every `rename_with()` rule instead of normalising the candidate name once.
- The event-row filter used `!is.null(.)` inside `if_any()`, even though the columns being evaluated were already existing vectors.
- `clean_datetime_columns()` carried a one-off local helper just to log final NA counts.
- The second datetime parse branch still checked `inherits(original_values, "POSIXct")` even though the earlier branch already handled POSIXct values and `next`ed.
- `clean_time_range()` embedded its year-boundary rules directly inside one dense `filter()` call, which made a correct but important rule set harder to audit.

## What Didn't Work
**Direct solution:** this was handled as a constrained readability refactor rather than a broad simplification pass.

The change set was deliberately limited to places where control flow, validation boundaries, and output shape would remain unchanged. Safety-sensitive logic such as staged Parquet export, multi-format datetime parsing, incomplete-datetime dropping, and duration filtering was left intact.

## Solution
The working refactor made five narrow changes:

- In `clean_data()`, rewrote `rename_with()` so it computes `lower_name <- tolower(.)` once and applies the rename rules against that value.
- In the same function, simplified `filter(if_any(...))` to remove the redundant `!is.null(.)` check and keep only `!is.na(.)`.
- In `clean_datetime_columns()`, removed the one-off `log_na_counts()` helper and inlined the final NA-count computation and log message at the point of use.
- Removed the redundant `inherits(original_values, "POSIXct")` condition from the second datetime parse branch, because the earlier POSIXct guard already exits that path.
- In `clean_time_range()`, pulled the year-boundary rules into named booleans before `filter()`, so the exclusion logic is easier to read and audit.

**Code changes**:

```r
# Before
rename_with(
  ~ case_when(
    grepl("site", tolower(.)) & grepl("ea", tolower(.)) ~ "site_name_ea",
    grepl("start|begin", tolower(.)) ~ "start_time",
    TRUE ~ .
  )
) %>%
filter(if_any(c(start_time, end_time), ~ !is.na(.) & !is.null(.)))

if (inherits(original_values, "POSIXct") ||
  inherits(original_values, "character")) {
```

```r
# After
rename_with(
  ~ {
    lower_name <- tolower(.)

    case_when(
      grepl("site", lower_name) & grepl("ea", lower_name) ~ "site_name_ea",
      grepl("start|begin", lower_name) ~ "start_time",
      TRUE ~ .
    )
  }
) %>%
filter(if_any(c(start_time, end_time), ~ !is.na(.)))

if (inherits(original_values, "character")) {
```

```r
# Before
filter(
  !(lubridate::year(start_time) < year & lubridate::year(end_time) != year) &
    !(lubridate::year(end_time) > year & lubridate::year(start_time) != year) &
    end_time > as.POSIXct(paste0(year, "-01-01 00:00:00"), tz = "UTC")
)
```

```r
# After
start_year <- lubridate::year(df$start_time)
end_year <- lubridate::year(df$end_time)
year_start <- as.POSIXct(paste0(df$year, "-01-01 00:00:00"), tz = "UTC")
start_before_year_with_end_outside_year <- start_year < df$year & end_year != df$year
end_after_year_with_start_outside_year <- end_year > df$year & start_year != df$year
ends_after_year_start <- df$end_time > year_start

filter(
  !start_before_year_with_end_outside_year &
    !end_after_year_with_start_outside_year &
    ends_after_year_start
)
```

## Why This Works
The refactor improves readability without changing the script's behavioural contract.

1. The rename rules now show their intent more clearly because each candidate column name is normalised once before matching.
2. Removing `!is.null(.)` does not change filtering behaviour inside `if_any()` on existing columns; it only removes dead noise.
3. Inlining the final NA-count logging reduces indirection while preserving the same validation checkpoint and failure behaviour.
4. Removing the second POSIXct check does not widen parsing behaviour, because POSIXct values are still handled by the earlier branch and never reach the second one.
5. Naming the year-boundary predicates makes the time-range rules easier to inspect without changing the actual filter conditions.

The key point is that the simplification was structural, not semantic: validation remained strict, and the output contract was checked with a real end-to-end run.

## Prevention
- Treat readability-only refactors in ETL scripts as safe only when they do not change the script's contracts: expected input inventory, output path and schema, parsing order, row-dropping rules, and staged-write validation.
- Safe simplifications usually look like removing redundant conditions, replacing repeated expressions with named intermediates, inlining one-off helpers, and naming complex boolean filters before `filter()`.
- Do not simplify the parts of the script that carry correctness or recoverability: multi-format datetime parsing order, explicit failure on unparseable datetimes, row-drop logic for known non-event and incomplete datetime rows, time-range cleanup rules, and staged Parquet write/validation before promotion.
- Keep validation boundaries intact. If a refactor changes how code is written but not what it does, the same postconditions should still hold: no missing parsed datetimes, no nonpositive durations, full company-year coverage, and the same key output columns and types.
- Prefer small, reviewable refactors. Change one readability concern at a time, then replay the script and inspect the actual output rather than relying only on a successful exit code.
- When a simplification touches filtering or parsing, assume it could change row counts until a runtime check proves otherwise.
- Keep dataset-level warnings explicit. In this run, the only warning was one dropped `2022 Welsh Water` row with a single missing/non-event datetime; warnings like that should remain visible after future refactors rather than being absorbed into silent cleanup.

**Regression / review checks**:

```bash
Rscript --vanilla -e "parse(file='scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R')"
```

```bash
Rscript --vanilla scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R
```

```bash
Rscript --vanilla -e 'library(arrow); library(dplyr); df <- read_parquet("data/processed/combined_edm_data.parquet") %>% collect(); stopifnot(sum(is.na(df[["start_time"]])) == 0L, sum(is.na(df[["end_time"]])) == 0L, sum(df[["end_time"]] <= df[["start_time"]]) == 0L, n_distinct(df[["year"]]) == 3L, n_distinct(df[["water_company"]]) == 10L, nrow(distinct(df, year, water_company)) == 30L)'
```

```bash
tail -n 50 output/log/03_combine_individ_edm_data_2021-2023.log
```

- If this script keeps being refactored, add a focused regression check under `scripts/R/testing/` that asserts the post-run invariants above and flags any unexpected change in company-year coverage or datetime completeness.

## Related Issues
- See also: [individ-edm-yearly-combined-parquet-fix-20260310.md](./individ-edm-yearly-combined-parquet-fix-20260310.md)
- See also: [annual-return-combiner-simplification-20260310.md](./annual-return-combiner-simplification-20260310.md)
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)
- See also: [edm-api-combine-hardening-20260310.md](./edm-api-combine-hardening-20260310.md)

Relevant repository references:
- Script: [`scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R`](../../../scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R)
- Upstream producer: [`scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R`](../../../scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R)
- Downstream consumer: [`scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R`](../../../scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R)
- Pipeline overview: [`book/data_clean_documentation/01_pipeline.qmd`](../../../book/data_clean_documentation/01_pipeline.qmd)

## Verification Note
The updated script parsed successfully with:

```bash
Rscript --vanilla -e "parse(file = 'scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R')"
```

The full script then ran successfully, completing on `2026-03-10 18:10:34`.

The exported `data/processed/combined_edm_data.parquet` validated cleanly:
- `5,532,141` rows
- `15` columns
- `0` missing `start_time`
- `0` missing `end_time`
- `0` nonpositive durations
- `3` years: `2021`, `2022`, `2023`
- `10` water companies
- `30` company-year pairs
- overall date range from `2020-12-26 09:49:11` to `2024-01-01 00:02:00`

The only warning in the successful run was one dropped row for `2022_Welsh Water 2022` with a single missing/non-event datetime value. No parse failures or export errors occurred.
