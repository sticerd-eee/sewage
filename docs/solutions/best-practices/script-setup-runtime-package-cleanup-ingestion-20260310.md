---
module: EDM Ingestion
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "Ingestion entry scripts duplicated dependency checks and logger setup."
  - "Startup code in the ingestion layer had drifted away from the repo's `renv::restore()` contract."
  - "The API downloader depended on an implicit `%||%` helper that was not defined locally."
  - "There was no shared, approved bootstrap pattern for new R scripts."
root_cause: missing_tooling
resolution_type: tooling_addition
severity: medium
tags: [r, ingestion, tooling, script-setup, renv, logging, dependency-management]
---

# Troubleshooting: Centralising R script setup for ingestion scripts without runtime installs

## Problem
The ingestion layer had started to accumulate duplicated startup logic across entry scripts. `scripts/R/01_data_ingestion/fetch_edm_api_data_2024_onwards.R` and `scripts/R/01_data_ingestion/edm_individ_data_standardisation_2021-2023.R` each managed dependency checks and logging separately, and the broader cleanup goal was to move the repository toward a single fail-fast setup pattern based on `renv::restore()`.

## Environment
- Module: EDM Ingestion
- Affected component: repository tooling and ingestion entry scripts
- Key files:
  - `scripts/R/utils/script_setup.R`
  - `scripts/R/01_data_ingestion/fetch_edm_api_data_2024_onwards.R`
  - `scripts/R/01_data_ingestion/edm_individ_data_standardisation_2021-2023.R`
- Date solved: 2026-03-10

## Symptoms
- The two ingestion entry scripts duplicated startup concerns such as dependency checks and logger initialization.
- The repository already documented `renv::restore()` as the intended dependency bootstrap path, but scripts still needed manual cleanup to fully align with that contract.
- `fetch_edm_api_data_2024_onwards.R` relied on an implicit `%||%`-style null-coalescing behavior that was not defined locally.
- There was no reusable helper to standardize startup when migrating the rest of `scripts/R/`.

## What Didn't Work
**Rejected approach 1:** Remove runtime install behavior one script at a time without introducing a shared helper.  
- **Why it failed:** it would still leave duplicated setup logic and no stable pattern for later rollout across `scripts/R/`.

**Attempted solution 2:** Implement optional console logging by passing a built appender into `logger::appender_tee()`.  
- **Why it failed:** `logger::appender_tee()` expects a file path and constructs the file appender internally. Passing an appender object caused an `invalid connection` error during the helper smoke test.

## Solution
The working fix was to split generic startup from script-specific logic.

- Added `scripts/R/utils/script_setup.R` as the single shared helper for startup concerns.
- Implemented `check_required_packages(required_packages)` to fail fast with a `renv::restore()` instruction instead of mutating the environment at runtime.
- Implemented `setup_logging(log_file, console = interactive(), threshold = "DEBUG")` to centralize logger initialization while keeping the existing log-file contract.
- Refactored both ingestion scripts to:
  - require `here` up front so project-root sourcing is explicit
  - source `script_setup.R`
  - declare `REQUIRED_PACKAGES` and `LOG_FILE`
  - call `check_required_packages(REQUIRED_PACKAGES)`
  - call `setup_logging(LOG_FILE)` inside `main()`
- Replaced the downloader's implicit `%||%` dependency with a local `coalesce_null()` helper so the script no longer depends on an unattached package operator.
- Fixed the helper's initial console tee bug by using `logger::appender_tee(log_file)` instead of trying to compose appenders manually.

**Code changes**:

```r
# New shared helper
check_required_packages <- function(required_packages) {
  missing_packages <- required_packages[!vapply(
    required_packages,
    requireNamespace,
    quietly = TRUE,
    FUN.VALUE = logical(1)
  )]

  if (length(missing_packages) > 0) {
    stop(
      paste0(
        "Missing required packages: ",
        paste(missing_packages, collapse = ", "),
        ". Install project dependencies first, e.g. `renv::restore()`."
      ),
      call. = FALSE
    )
  }

  invisible(required_packages)
}
```

```r
# Refactored downloader startup
if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c("dplyr", "fs", "here", "httr", "jsonlite", "logger", "lubridate")
LOG_FILE <- here::here("output", "log", "04_fetch_edm_api_data_2024_onwards.log")

check_required_packages(REQUIRED_PACKAGES)
```

```r
# Local replacement for implicit null-coalescing
coalesce_null <- function(value, default) {
  if (is.null(value)) {
    return(default)
  }

  value
}
```

**Validation commands**:

```bash
Rscript --vanilla -e "parse(file='scripts/R/utils/script_setup.R')"
Rscript --vanilla -e "source('scripts/R/01_data_ingestion/fetch_edm_api_data_2024_onwards.R', local = new.env(parent = globalenv()))"
Rscript --vanilla -e "source('scripts/R/01_data_ingestion/edm_individ_data_standardisation_2021-2023.R', local = new.env(parent = globalenv()))"
Rscript --vanilla -e "source('scripts/R/utils/script_setup.R'); check_required_packages(c('logger')); setup_logging(tempfile(), console = TRUE); logger::log_info('helper-test')"
```

## Why This Works
The underlying problem was missing shared tooling. Each script had started to solve environment setup independently, which made the startup path inconsistent and left room for hidden side effects.

This fix works because it restores one explicit contract:

1. Generic startup lives in one helper rather than drifting across entry scripts.
2. Missing dependencies now fail clearly and immediately, which matches the repository's documented `renv::restore()` workflow.
3. Logging is initialized consistently without changing the scripts' existing log file paths.
4. The downloader no longer relies on an implicit operator from an attached package environment.
5. The pattern is reusable for later migration across `scripts/R/` without changing script-specific business logic.

## Prevention
- Treat `scripts/R/utils/script_setup.R` as the only approved place for generic dependency checks and logger initialization.
- Do not use `install.packages()`, `renv::init()`, or broad `library()` calls inside production pipeline scripts.
- Standardize on explicit `pkg::fn` usage in production scripts so dependencies remain visible and stable.
- Keep utility files side-effect-light: sourcing a utility file should define helpers, not install packages or start work.
- Start each production script with:
  - a `here` availability check
  - `source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)`
  - a `REQUIRED_PACKAGES` vector
  - `check_required_packages(REQUIRED_PACKAGES)`
  - a `LOG_FILE` constant when the script logs
- Add a lightweight repository check such as `rg -n "install\\.packages\\(|renv::init\\(" scripts/R` during future cleanup passes.
- Migrate the rest of `scripts/R/` in batches, starting with remaining `01_data_ingestion` and `02_data_cleaning` scripts before moving deeper into the pipeline.

## Related Issues
- See also: [output-compatible-edm-standardisation-refactor-20260309.md](./output-compatible-edm-standardisation-refactor-20260309.md)

Relevant repository references:
- Script overview and dependency setup in [ReadMe.md](../../../ReadMe.md)
- Ingestion-stage rationale in [02_ingestion.qmd](../../../book/data_clean_documentation/02_ingestion.qmd)
- Pipeline placement in [01_pipeline.qmd](../../../book/data_clean_documentation/01_pipeline.qmd)
- Adjacent fail-fast config example in [`scripts/config/api_config.R`](../../../scripts/config/api_config.R)

## Verification Note
The solution was verified with static parse/source checks and a helper smoke test, including the interactive console-logging path. Full end-to-end ingestion runs were not executed in this session, so the note documents a verified startup/tooling refactor rather than a full runtime ingestion replay.
