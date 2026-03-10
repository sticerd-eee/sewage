---
module: EDM API Pipeline
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "Downstream 2024+ EDM scripts still installed packages or initialized environment state at runtime."
  - "The live fetch/config layer exposed an England-only nine-company contract, but downstream combine logic and notebook assets still assumed ten companies including Welsh Water."
  - "README and ingestion docs described the wrong raw API snapshot path and overstated live coverage."
  - "Stale out-of-scope artifacts such as `welsh_water` folders or Parquet files could appear on disk without one shared contract deciding how to treat them."
  - "Regression assets preserved copied assumptions instead of checking the active fetch/config contract directly."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, edm-api, pipeline-contracts, england-only, regression-checks]
---

# Troubleshooting: Aligning the live 2024+ EDM API pipeline to one England-only contract

## Problem
The live 2024+ EDM API fetch layer had already converged on an England-only nine-company contract, but the downstream processing, combine, integration, documentation, and notebook-style regression assets still encoded older assumptions. The result was contract drift: paths, company scope, dependency setup, and expected outputs were no longer defined in one place.

## Environment
- Module: EDM API Pipeline
- Affected component: repository tooling, downstream processing scripts, and contract docs
- Key files:
  - `scripts/config/api_config.R`
  - `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R`
  - `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`
  - `scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R`
  - `scripts/R/testing/test_edm_api_pipeline_contracts.R`
- Date solved: 2026-03-10

## Symptoms
- Three downstream scripts still used runtime package installation or startup logic that had already been cleaned up in the fetch layer.
- The live config defined nine in-scope England companies, but downstream code and notebooks still carried stale ten-company assumptions involving Welsh Water.
- The repository docs pointed to the wrong raw API path and described broader live coverage than the pipeline actually implemented.
- Raw folder discovery and per-company Parquet discovery were not both driven by one contract, so stale artifacts like `welsh_water` could drift into the workflow.
- The regression surface preserved notebook copies and prose assumptions instead of asserting the active config/process/combine contract directly.

## What Didn't Work
**Rejected approach 1:** Patch each downstream script and doc independently without introducing one shared contract.  
- **Why it failed:** it would leave multiple sources of truth for scope, paths, and expected companies, so drift would return on the next edit.

**Rejected approach 2:** Add Welsh Water immediately to restore a nominal ten-company live API story.  
- **Why it failed:** Welsh Water was not part of the confirmed live downloader contract, so this would have expanded the task from contract alignment into a separate ingestion/source decision.

**Rejected approach 3:** Leave runtime dependency handling embedded in each downstream script for convenience.  
- **Why it failed:** it preserved hidden environment side effects and contradicted the repository's fail-fast `renv::restore()` workflow.

## Solution
The working fix made `scripts/config/api_config.R` the single source of truth for the live 2024+ EDM API contract.

- Added explicit contract metadata for scope, storage paths, company IDs, company display names, and output filenames.
- Introduced `get_edm_api_contract()` and `compare_edm_api_company_ids()` so downstream scripts resolve on-disk inputs against the same contract instead of inferring scope ad hoc.
- Rewired `process_edm_api_json_to_parquet_2024_onwards.R` to use the contract's raw and processed paths, ignore stale out-of-scope company directories, and warn on missing expected ones.
- Rewired `combine_api_edm_data_2024_onwards.R` to use the contract's processed directory and combined output file, normalize company names from the contract mapping, and derive `year` via `lubridate::year()`.
- Pointed `combine_2021-2023_and_api_edm_data.R` at the contracted combined API Parquet file instead of a local path guess.
- Removed runtime package installation from the downstream scripts and aligned startup with `scripts/R/utils/script_setup.R`.
- Updated `ReadMe.md`, `book/data_clean_documentation/02_ingestion.qmd`, and the notebook wrappers so they describe and exercise the active England-only contract instead of preserving stale copied logic.
- Added `scripts/R/testing/test_edm_api_pipeline_contracts.R` as a lightweight regression check for scope, paths, Welsh Water handling, cleaning rules, and doc drift.

**Code changes**:

```r
# Single shared contract
EDM_API_CONTRACT <- get_edm_api_contract()

CONFIG <- list(
  input_dir = EDM_API_CONTRACT$raw_directory,
  output_dir = EDM_API_CONTRACT$processed_directory,
  output_file = EDM_API_CONTRACT$combined_file
)
```

```r
# Resolve actual on-disk companies against the contract
status <- compare_edm_api_company_ids(actual_company_ids, EDM_API_CONTRACT)

valid_company_ids <- status$valid_company_ids
missing_company_ids <- status$missing_company_ids
unexpected_company_ids <- status$unexpected_company_ids
```

```r
# Clean combine output with explicit downstream contract behavior
water_company = EDM_API_CONTRACT$company_id_to_name[water_company]
start_time = dplyr::if_else(start_time < contract_start, contract_start, start_time)
year = as.integer(lubridate::year(start_time))
```

**Verification commands**:

```bash
Rscript --vanilla scripts/R/testing/test_fetch_edm_api_data_2024_onwards.R
Rscript --vanilla scripts/R/testing/test_edm_api_pipeline_contracts.R
```

## Why This Works
The underlying problem was contract drift, not just bad wording or one broken script.

1. One shared contract removes duplicated path and scope definitions, so fetch, process, combine, integration, and docs all read from the same source.
2. Filtering happens at both the raw-directory and per-company-Parquet stages, so stale artifacts like `welsh_water` are excluded consistently instead of opportunistically.
3. The contract preserves stable snake_case IDs for file and directory names while exposing display-name mappings for downstream analysis output.
4. The combine stage now uses explicit dependencies, including `lubridate::year()`, which reduces clean-session failures caused by implicit package attachment.
5. A small regression script now checks the live contract directly, which is a better guard against drift than preserving old notebook copies.

## Prevention
- Keep `scripts/config/api_config.R` as the only approved source of truth for the live 2024+ scope, company list, raw path, processed path, and combined output path.
- Do not re-declare company coverage in downstream scripts; derive it from `get_edm_api_contract()`.
- Always resolve discovered folders and Parquet files against the shared contract before processing.
- Treat missing expected companies and unexpected company artifacts as explicit contract events in logs.
- Keep generic startup in `scripts/R/utils/script_setup.R`; do not add `install.packages()` or `renv::init()` calls to production pipeline scripts.
- Prefer explicit `pkg::fn` calls in contract-sensitive code paths.
- Keep notebook assets thin wrappers around active regression scripts rather than copied historical implementations.
- Keep docs synchronized with the exact contract language: England-only, nine in-scope companies, raw snapshots in `data/raw/edm_data/raw_api_responses`, combined API output in `data/processed/edm_api_data/combined_api_data.parquet`.
- If Welsh Water is ever added, treat that as a separate source-contract change and update config, tests, downstream normalization, and docs together.
- Consider promoting other hard-coded contract boundaries, such as the `2024-01-01` cutoff, into the shared contract if they may ever change.

## Related Issues
- See plan: [2026-03-09-refactor-align-edm-api-pipeline-contracts-plan.md](../../plans/2026-03-09-refactor-align-edm-api-pipeline-contracts-plan.md)
- See implementation todo: [009-complete-p1-align-edm-api-pipeline-contracts.md](../../../todos/009-complete-p1-align-edm-api-pipeline-contracts.md)
- See also: [script-setup-runtime-package-cleanup-ingestion-20260310.md](./script-setup-runtime-package-cleanup-ingestion-20260310.md)
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)
- Contract source: [`scripts/config/api_config.R`](../../../scripts/config/api_config.R)
- Regression check: [`scripts/R/testing/test_edm_api_pipeline_contracts.R`](../../../scripts/R/testing/test_edm_api_pipeline_contracts.R)
- User-facing docs: [ReadMe.md](../../../ReadMe.md)
- Ingestion docs: [02_ingestion.qmd](../../../book/data_clean_documentation/02_ingestion.qmd)

## Notes
- This note documents the solved contract-alignment refactor, not later hardening work on idempotence or failure semantics in the downstream combine steps.
- Welsh Water remains intentionally out of scope for the live 2024+ API pipeline unless a separate source-contract decision is made.

## Verification Note
This solution was verified on 2026-03-10 with the fetch regression script and the dedicated EDM API pipeline contract test. The note records the implemented England-only contract and the repository changes that brought downstream scripts, docs, and test assets into line with it.
