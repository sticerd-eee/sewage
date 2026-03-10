---
module: R Data Cleaning
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "Several `scripts/R/02_data_cleaning/*.R` entry scripts still used inconsistent file headers and startup blocks."
  - "Older cleaning scripts were installing packages or initialising `renv` at runtime instead of failing fast with `renv::restore()`."
  - "Logger setup and package attachment had drifted across the cleaning layer, mixing shared-helper and script-local patterns."
  - "The affected scripts did not consistently declare `REQUIRED_PACKAGES` and `LOG_FILE` near the top of the file."
root_cause: incomplete_setup
resolution_type: workflow_improvement
severity: medium
tags: [r, data-cleaning, script-setup, headers, logging, renv, dependency-management]
---

# Troubleshooting: Standardising headers and fail-fast setup across `02_data_cleaning` scripts

## Problem
The `scripts/R/02_data_cleaning/` layer had drifted into two different conventions.

Newer scripts already followed the shared bootstrap contract introduced on March 10, 2026, but several older entry scripts still used bespoke startup logic, runtime package installation, and inconsistent top-of-file headers. That made the data-cleaning layer harder to scan, harder to maintain, and less aligned with the repository's documented `renv::restore()` workflow.

The cleanup in this session covered:
- `scripts/R/02_data_cleaning/clean_zoopla_data.R`
- `scripts/R/02_data_cleaning/combine_annual_return_data.R`
- `scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R`
- `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R`
- `scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R`
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`

## Environment
- Module: `scripts/R/02_data_cleaning/`
- Shared helper:
  - `scripts/R/utils/script_setup.R`
- Files that required header cleanup plus setup cleanup:
  - `scripts/R/02_data_cleaning/clean_zoopla_data.R`
  - `scripts/R/02_data_cleaning/combine_annual_return_data.R`
  - `scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R`
  - `scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R`
- Files that required header cleanup only because setup was already compliant:
  - `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R`
  - `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`
- Date solved: 2026-03-10

## Symptoms
- The older cleaning scripts still used `install.packages()` inside production entrypoints.
- One script still called `renv::init()` during runtime setup.
- Local `load_packages()` and `setup_logging()` helpers duplicated bootstrap concerns already covered by `scripts/R/utils/script_setup.R`.
- Header blocks varied in structure and detail, so purpose, inputs, outputs, and log-file paths were not consistently discoverable.
- The API-side cleaning scripts already used the new setup pattern, which made the remaining drift across `02_data_cleaning/` more obvious.

## What Didn't Work
**Rejected approach 1:** Limit the cleanup to header rewrites only.  
- **Why it failed:** that would leave the more important setup drift in place, including runtime package installation and script-local logging bootstrap.

**Rejected approach 2:** Apply the same full bootstrap rewrite to every targeted script without checking current state.  
- **Why it failed:** the two 2024+ API scripts were already aligned with `script_setup.R`, so a blanket rewrite would add churn without improving behavior.

## Solution
The working fix was to separate cosmetic header standardisation from actual startup migration, and to use the shared helper wherever the older scripts still owned bootstrap concerns.

- Rewrote the top header blocks in all targeted `02_data_cleaning` scripts to the newer repository format:
  - title
  - purpose
  - author
  - date
  - `Date Modified: 2026-03-10`
  - concrete inputs
  - concrete outputs
  - source where clearly known
- Migrated the older cleaning scripts to the shared fail-fast bootstrap pattern:
  - upfront `here` availability check
  - `source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)`
  - `REQUIRED_PACKAGES`
  - `LOG_FILE`
  - `check_required_packages(REQUIRED_PACKAGES)`
- Replaced runtime package installation and `renv::init()` calls with fail-fast dependency validation that points users back to `renv::restore()`.
- Replaced script-local `setup_logging <- function(...)` definitions with thin `initialise_logging()` wrappers that delegate to the shared `setup_logging(...)` helper.
- Simplified `initialise_environment()` so it only attaches the packages still needed for unqualified calls in each script.
- Left the two API scripts' setup logic unchanged after review because they already matched the approved contract.
- Tightened `clean_zoopla_data.R` further by sourcing `postcode_processing_utils.R` with `local = TRUE` and moving package validation ahead of that source step.

**Code changes**:

```r
if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
```

```r
REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "fs",
  "glue",
  "here",
  "janitor",
  "logger",
  "purrr",
  "rio"
)

LOG_FILE <- here::here("output", "log", "08_combine_annual_return_data.log")

check_required_packages(REQUIRED_PACKAGES)
```

```r
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "INFO")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}
```

```r
initialise_environment()
initialise_logging()
```

## Why This Works
The underlying problem was incomplete rollout, not missing direction. The repository already had a shared bootstrap helper and one approved pattern, but the `02_data_cleaning` layer had only adopted it selectively.

This fix works because it restores one explicit contract across the cleaning scripts:

1. Startup checks are shared rather than reimplemented script by script.
2. Missing dependencies now fail clearly and immediately, which matches the repository's `renv::restore()` workflow.
3. Logging is initialized consistently without changing script-specific log file paths.
4. Header metadata is easier to scan, which makes inputs, outputs, and purpose easier to verify before editing or running a script.
5. Migration scope stays disciplined: only scripts that still had bootstrap drift were refactored, while already-compliant scripts were reviewed and left structurally unchanged.

## Prevention
- Treat `scripts/R/utils/script_setup.R` as the only approved place for generic dependency checks and logger initialisation.
- Start each production `scripts/R/*` entry script with the same bootstrap contract: `here` availability check, `source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)`, `REQUIRED_PACKAGES`, `LOG_FILE`, and `check_required_packages(REQUIRED_PACKAGES)`.
- Do not use `install.packages()`, `renv::init()`, or ad hoc `load_packages()` helpers inside production pipeline scripts.
- Keep script-local setup limited to thin wrappers such as `initialise_environment()` for package attachment when unqualified functions remain, and `initialise_logging()` for script-specific log thresholds.
- Prefer explicit `pkg::fn` usage for new code so package attachment needs shrink over time, but do not mix attachment and runtime installation.
- Standardise the file header at the same time as bootstrap cleanup so purpose, inputs, outputs, and log paths stay discoverable and consistent.
- Preserve business logic during migration; setup cleanup should change startup behaviour, not data-processing semantics.
- Migrate remaining scripts in small batches, starting with direct entry scripts under `scripts/R/02_data_cleaning/` before moving deeper into later pipeline stages.

**Validation commands**:

```bash
rg -n "install\\.packages\\(|renv::init\\(|load_packages <-|setup_logging <- function\\(" scripts/R/02_data_cleaning
```

```bash
rg -n "if \\(!requireNamespace\\(\"here\"|script_setup\\.R|REQUIRED_PACKAGES <-|LOG_FILE <-|check_required_packages\\(" scripts/R/02_data_cleaning
```

```bash
Rscript --vanilla -e "files <- c('scripts/R/02_data_cleaning/combine_annual_return_data.R','scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R','scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R','scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R','scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R'); invisible(lapply(files, parse)); cat('parsed', length(files), 'files\n')"
```

```bash
Rscript --vanilla -e "source('scripts/R/02_data_cleaning/combine_annual_return_data.R', local = new.env(parent = globalenv()))"
Rscript --vanilla -e "source('scripts/R/02_data_cleaning/convert_individ_raw_data_to_rdata_2021-2023.R', local = new.env(parent = globalenv()))"
Rscript --vanilla -e "source('scripts/R/02_data_cleaning/combine_individ_edm_data_2021-2023.R', local = new.env(parent = globalenv()))"
```

## Related Issues
- See also: [script-setup-runtime-package-cleanup-ingestion-20260310.md](./script-setup-runtime-package-cleanup-ingestion-20260310.md)
- See also: [england-only-edm-api-contract-alignment-20260310.md](./england-only-edm-api-contract-alignment-20260310.md)
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)

Relevant repository references:
- Shared bootstrap helper: [`scripts/R/utils/script_setup.R`](../../../scripts/R/utils/script_setup.R)
- Pipeline overview and script inventory: [ReadMe.md](../../../ReadMe.md)
- Pipeline placement: [01_pipeline.qmd](../../../book/data_clean_documentation/01_pipeline.qmd)
- Ingestion-stage rationale and 2024+ API context: [02_ingestion.qmd](../../../book/data_clean_documentation/02_ingestion.qmd)

Follow-up note:
- `CLAUDE.md` still describes an older generic R script structure and may need a convention refresh if this setup pattern is now the preferred repository standard.

## Verification Note
The solution was verified as a startup/tooling refactor rather than a full pipeline replay.

- All five requested scripts parsed successfully in one sweep.
- The three scripts whose setup changed sourced successfully after the refactor.
- The target set no longer contains the old setup anti-patterns:
  - `install.packages(`
  - `renv::init(`
  - `load_packages <-`
  - script-local `setup_logging <- function(...)`

No end-to-end data-processing runs were executed in this session, so this note documents a verified bootstrap/header migration rather than a full runtime replay of the `02_data_cleaning` pipeline.
