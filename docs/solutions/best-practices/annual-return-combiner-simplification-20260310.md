---
module: Annual Return EDM
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "combine_annual_return_data.R split final output rules between clean_data() and main(), so readers had to inspect both functions to understand the output schema."
  - "The script logged the same load step twice and ended the cleaning pipeline with `%>% return()`, adding noise without changing behaviour."
  - "A workbook-specific cleanup (`x26`) and Welsh Water name standardisation lived in orchestration code instead of the cleaning boundary."
  - "export_data() used a catch-log-rethrow wrapper even though main() already owned fatal-error logging."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, annual-return, refactor, script-simplicity, data-cleaning]
---

# Troubleshooting: Simplifying the annual return combiner without changing its output contract

## Problem
`scripts/R/02_data_cleaning/combine_annual_return_data.R` was still producing the expected annual-return parquet, but the implementation had picked up avoidable noise. The final output rules were split between `clean_data()` and `main()`, the export step duplicated error handling already present at the top level, and small pieces of dead-weight syntax made the script harder to scan than the job required.

The goal of the refactor was to simplify the script while keeping the output contract intact: same primary parquet output, same raw-NGR backup column, and the same year-by-year processing flow.

## Environment
- Module: Annual Return EDM / `scripts/R/02_data_cleaning/`
- Affected component: Repository tooling / cleaning script
- Key file: `scripts/R/02_data_cleaning/combine_annual_return_data.R`
- Commit: `5ea669a` (`refactor: simplify annual return combiner`)
- Date solved: 2026-03-10

## Symptoms
- `clean_data()` handled most of the schema work, but `main()` still dropped `x26` and recoded `Dwr Cymru Welsh Water`, so the output rules were split across two functions.
- `load_data()` emitted the same log line twice for a single file read.
- `clean_data()` ended with `%>% return()`, which did not add behaviour but did add cognitive noise.
- `export_data()` wrapped a straightforward write in a local `tryCatch()` that only logged and rethrew, even though `main()` already handled fatal errors.
- Inspection of the raw workbooks showed that `x26` was a stray blank column from the 2022 Thames Water sheet rather than part of the intended schema.

## What Didn't Work
**Rejected approach 1:** Remove `outlet_discharge_ngr_og` because it looked write-only in-repo.  
- **Why it failed:** the cleanup goal was to simplify the script without changing the useful output contract, and the user explicitly wanted the raw NGR backup column preserved.

**Rejected approach 2:** Remove every helper-level `tryCatch()` in one pass.  
- **Why it failed:** `process_year()` still adds year-specific failure context, while only `export_data()` was clearly redundant within the requested scope.

**Rejected approach 3:** Delete the duplicate log and pipeline `return()`, but leave `x26` removal and Welsh Water recoding in `main()`.  
- **Why it failed:** that would keep the core schema/value normalisation split between cleaning and orchestration.

## Solution
The working refactor simplified the script by consolidating normalisation logic inside `clean_data()` and removing low-value wrapper code around the export step.

- Removed the duplicate `logger::log_info("Loading data for year {year}")` call from `load_data()`.
- Moved the `x26` cleanup into `clean_data()` with `select(-any_of("x26"))`, so workbook-specific column cleanup happens at the cleaning boundary rather than in `main()`.
- Moved the Welsh Water recode into `clean_data()`, so value standardisation now lives with the rest of the data-normalisation logic.
- Kept `outlet_discharge_ngr_og` in place while still cleaning the working `outlet_discharge_ngr` field.
- Removed the terminal `%>% return()` from `clean_data()` and let the last expression return naturally.
- Simplified `export_data()` to a straight write path and left fatal logging to `main()`.
- Removed the now-obsolete `x26` and Welsh Water cleanup blocks from `main()`, leaving it closer to orchestration-only.

**Code changes**:

```r
# Before
clean_data <- function(df) {
  df %>%
    janitor::clean_names() %>%
    rename_with(...) %>%
    filter(!is.na(water_company)) %>%
    mutate(
      outlet_discharge_ngr_og = outlet_discharge_ngr,
      ...
    ) %>%
    return()
}

main <- function() {
  ...
  combined_data <- map(CONFIG$years, process_year) %>% list_rbind()
  combined_data <- select(combined_data, -x26)
  combined_data <- combined_data %>%
    mutate(water_company = if_else(
      water_company == "Dwr Cymru Welsh Water",
      "Welsh Water",
      water_company
    ))
  export_data(combined_data)
}
```

```r
# After
clean_data <- function(df) {
  df %>%
    janitor::clean_names() %>%
    rename_with(...) %>%
    select(-any_of("x26")) %>%
    filter(!is.na(water_company)) %>%
    mutate(water_company = if_else(
      water_company == "Dwr Cymru Welsh Water",
      "Welsh Water",
      water_company
    )) %>%
    mutate(
      outlet_discharge_ngr_og = outlet_discharge_ngr,
      ...
    )
}

export_data <- function(data) {
  parquet_file <- file.path(CONFIG$output_dir, "annual_return_edm.parquet")
  dir.create(dirname(parquet_file), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(data, parquet_file)
  logger::log_info("Data exported to {parquet_file}")
}
```

## Why This Works
The refactor lines the code up with the actual responsibilities of the script.

1. `clean_data()` now owns the schema and value rules that define the final dataset, so readers no longer have to inspect `main()` to understand the output.
2. `select(-any_of("x26"))` handles the known stray workbook column without making the script brittle if that column disappears in a future workbook.
3. Keeping `outlet_discharge_ngr_og` preserves the raw NGR source while still allowing `outlet_discharge_ngr` to be cleaned for downstream matching.
4. Removing the duplicate log and pipeline `return()` lowers noise without changing behaviour.
5. Simplifying `export_data()` reduces log spam and nested error messages, while `main()` still remains the fatal-error boundary for the script.

## Prevention
- Keep `main()` orchestration-only wherever possible; business rules for schema and value normalisation should live in the relevant cleaning helper.
- Use lower-level `tryCatch()` only when it adds context that the top-level boundary cannot provide. Catch-log-rethrow wrappers with no extra value should be removed.
- Handle known workbook artefacts like extra empty columns at the cleaning boundary, and prefer tolerant selectors such as `any_of()` for one-off cleanup columns.
- Preserve raw backup columns only when they are intentionally part of the output contract; document that choice explicitly when refactoring.
- Review pipeline scripts for “split output logic” during refactors: if a reader needs both `clean_data()` and `main()` open to understand the final dataset, the boundary is probably wrong.

**Regression / review checks**:

```bash
Rscript --vanilla -e "parse(file='scripts/R/02_data_cleaning/combine_annual_return_data.R')"
```

```bash
Rscript --vanilla -e "source('scripts/R/02_data_cleaning/combine_annual_return_data.R', local = TRUE); initialise_environment(); raw <- rio::import_list('data/raw/edm_data/2022_annual_return_edm.xlsx', skip = 1, setclass = 'tbl')[['Thames Water 2022']]; out <- clean_data(raw); cat('has_x26=', 'x26' %in% names(out), '\n', sep=''); cat('has_outlet_discharge_ngr_og=', 'outlet_discharge_ngr_og' %in% names(out), '\n', sep='')"
```

- Add a focused regression check under `scripts/R/testing/` that asserts:
  - `x26` is absent after cleaning the 2022 Thames Water sheet
  - `outlet_discharge_ngr_og` is still present
  - `Dwr Cymru Welsh Water` is recoded to `Welsh Water`

## Related Issues
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)

Relevant repository references:
- Script: [`scripts/R/02_data_cleaning/combine_annual_return_data.R`](../../../scripts/R/02_data_cleaning/combine_annual_return_data.R)
- Pipeline overview: [ReadMe.md](../../../ReadMe.md)
- Pipeline placement: [01_pipeline.qmd](../../../book/data_clean_documentation/01_pipeline.qmd)

## Verification Note
The solution was verified as a targeted simplification refactor rather than a full pipeline replay.

- The updated script parsed successfully.
- A direct `clean_data()` smoke test on the 2022 Thames Water sheet confirmed:
  - `has_x26=FALSE`
  - `has_outlet_discharge_ngr_og=TRUE`
- Workbook inspection showed that `x26` was only present in the 2022 Thames Water sheet and was otherwise blank apart from two literal `"x"` markers.

No end-to-end annual-return pipeline run was executed in this session, so this note documents a verified structure/cleaning-boundary refactor rather than a full runtime replay.
