---
module: Individual EDM Raw File Converter
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "The documented year-level outputs `data/processed/edm_data_2021_2023/combined_edm_data_{year}.parquet` were never being written."
  - "The yearly combine step matched datasets with a text search against values like `\"2021\"`, so the year-specific selection was always empty in the pre-fix code."
  - "The script logged that year-specific combined datasets had been created even when no combined parquet files were written."
  - "The script carried unused parallel setup (`setup_parallel()` plus `furrr`/`future` dependencies) even though `main()` explicitly ran in sequential mode and never enabled a non-sequential plan."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, edm, parquet, data-cleaning, logic-error, refactor, script-simplicity]
---

# Troubleshooting: Fixing missing year-level combined parquet exports in the individual EDM raw converter

## Problem
`scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R` documented year-level combined outputs such as `combined_edm_data_2021.parquet`, but the script never actually wrote them. The company-level parquet files were exported, yet the year-level combine step silently skipped every year because it tried to recover year slices from a flattened list using the wrong matching logic.

The script also carried a second layer of avoidable complexity: it defined parallel-processing setup that was never enabled, while the main log explicitly said the script was running in sequential mode. That left the `furrr::future_map()` path and its related setup code acting as noise rather than real behaviour.

## Environment
- Module: Individual EDM Raw File Converter / `scripts/R/02_data_cleaning/`
- Affected component: Repository tooling / parquet export script
- Key file: `scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R`
- Expected outputs include per-company parquet files plus year-level combined parquet files under `data/processed/edm_data_2021_2023/`
- Date solved: 2026-03-10

## Symptoms
- The script header, `ReadMe.md`, and `book/data_clean_documentation/01_pipeline.qmd` all described year-level combined parquet outputs, but the output directory contained only the 30 company parquet files.
- In the pre-fix code, the year-combine step searched for strings like `"year = 2021"` inside values that were only `"2021"`, `"2022"`, and `"2023"`, so `year_combined` was always empty.
- The export path still logged success for year-specific combined datasets, which made the failure easy to miss unless the output directory was checked directly.
- The script defined `setup_parallel()` and imported `furrr`, `future`, `memuse`, and `parallelly`, but `main()` logged `"Sequential Mode"` and never enabled a non-sequential `future::plan()`.

## What Didn't Work
**Rejected approach 1:** Leave the flattened `combined_data` structure in place and only tweak the `grep()` pattern.  
- **Why it failed:** it would fix the immediate bug, but it would keep the more fragile "flatten everything, then recover per-year subsets later" structure that caused the issue in the first place.

**Rejected approach 2:** Keep the unused parallel setup and simply call `setup_parallel()`.  
- **Why it failed:** the script's actual operating mode in this repo was sequential, and the goal of the change was to simplify the implementation rather than add more runtime machinery. Keeping unused worker/RAM tuning would continue to obscure the real flow.

**Rejected approach 3:** Fix the year-selection bug but leave the commented-out `export_to_rdata()` block and the multi-pass export structure alone.  
- **Why it failed:** that would still leave dead code and duplicated traversal in a script whose main problem was already too much structure for a straightforward job.

## Solution
The working fix did four things:

- Replaced the broken year-selection logic so year-level exports are built from the correct year's data.
- Refactored `export_to_parquet()` into a single-pass year loop: for each year, write each company parquet file and accumulate that same year's cleaned data for the combined parquet output.
- Removed the unused parallel setup and switched the inactive `furrr::future_map()` calls to `purrr::map()`, so the implementation now matches the script's logged sequential behaviour.
- Deleted the dead commented-out `export_to_rdata()` block.

**Code changes**:

```r
# Before
combined_data[[length(combined_data) + 1]] <- company_data

year_combined <- combined_data[
  grep(
    paste0("year = ", year),
    sapply(combined_data, function(df) as.character(df$year[1]))
  )
]
```

```r
# After
for (year in names(data)) {
  year_data <- data[[year]]
  company_names <- names(year_data)
  year_combined <- vector("list", length(year_data))

  for (company_index in seq_along(company_names)) {
    company <- company_names[[company_index]]
    company_data <- year_data[[company]]

    arrow::write_parquet(company_data, parquet_file)

    company_combined_data <- standardise_date_columns(company_data)
    company_combined_data$year <- as.integer(year)
    company_combined_data$water_company <- company
    year_combined[[company_index]] <- company_combined_data
  }

  year_all_data <- dplyr::bind_rows(year_combined)
  arrow::write_parquet(year_all_data, year_combined_file)
}
```

```r
# Before
setup_parallel <- function() {
  future::plan(future::multisession, workers = n_workers)
}

furrr::future_map(file_paths, import_data, .options = furrr::furrr_options(seed = TRUE))
```

```r
# After
purrr::map(sheet_names, function(sheet_name) { ... })
purrr::map(file_paths, import_data)
```

## Why This Works
The refactor removes the need to reconstruct year groups after the fact. Each year's data stays grouped while it is being processed, so the combined parquet file for that year is built directly from the same company-level objects that were just exported. That eliminates the mismatch between stored year values and lookup patterns.

It also makes the script easier to trust operationally:

1. The year-level output contract is now produced by direct control flow rather than indirect filtering.
2. Company exports and year-level combined exports happen in the same loop, so there is one obvious place to inspect output behaviour.
3. The script's implementation now matches its declared runtime mode: sequential code, sequential logs, no unused worker or memory-tuning scaffolding.
4. Removing dead code and redundant passes lowers the chance of another silent export bug surviving inside bookkeeping logic.

## Prevention
- Keep the company-level export path and the year-level combined export path inside the same outer `year` loop when the outputs share the same grouping key. That avoids "flatten then recover" logic that is easy to get subtly wrong.
- When selecting subsets by year, compare typed values directly (`as.integer(year)` against stored `year`) rather than searching formatted strings with `grep()`.
- Do not leave unused execution modes in pipeline scripts. If a script is intended to run sequentially, remove dormant parallel setup and use `purrr::map()` directly; if parallelism is required, enable it explicitly and test it.
- Treat year-level outputs listed in the script header as part of the output contract. If the header says `combined_edm_data_{year}.parquet` should exist, add a focused regression check that asserts those files are produced.
- Add a lightweight regression harness under `scripts/R/testing/` that exercises `export_to_parquet()` with a small synthetic nested list and asserts:
  - company parquet files are written for each company-year input
  - `combined_edm_data_{year}.parquet` is written for each year
  - the combined output carries the expected `year` and `water_company` columns
- When refactoring ETL scripts, compare pre/post output inventories before committing. In this case, a quick file-list check would have made it obvious that the year-level combined parquet files were missing.

## Related Issues
- See also: [annual-return-combiner-simplification-20260310.md](./annual-return-combiner-simplification-20260310.md)
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)
- See also: [script-setup-runtime-package-cleanup-ingestion-20260310.md](./script-setup-runtime-package-cleanup-ingestion-20260310.md)

Relevant repository references:
- Script: [`scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R`](../../../scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R)
- Downstream consumer: [`scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R`](../../../scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R)
- Pipeline overview: [ReadMe.md](../../../ReadMe.md)
- Pipeline placement: [01_pipeline.qmd](../../../book/data_clean_documentation/01_pipeline.qmd)

## Verification Note
The fix was verified with targeted checks rather than a completed full raw-data replay.

- The updated script parsed successfully with `Rscript --vanilla -e "parse(file='scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R')"` .
- A synthetic-data export test confirmed that `export_to_parquet()` now writes both company parquet files and year-level combined parquet files such as `combined_edm_data_2021.parquet` and `combined_edm_data_2022.parquet`.
- A real-data run of `scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R` was started on 2026-03-10, but it did not complete during the session. The process remained active in the 2021 raw-data loading phase and was stopped before it reached the export stage.
- Before and after that aborted real-data run, the pre-existing company parquet outputs in `data/processed/edm_data_2021_2023/` had identical file inventories and identical SHA-256 hashes. So the attempted run did not alter any existing company parquet files.
- Because the full real-data run was not completed, this note documents a verified logic fix plus targeted export validation, not a fully replayed end-to-end regeneration of all raw-data outputs.
