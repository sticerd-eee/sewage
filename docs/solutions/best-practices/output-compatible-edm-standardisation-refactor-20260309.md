---
module: EDM Ingestion
date: 2026-03-09
problem_type: best_practice
component: tooling
symptoms:
  - "The 2021-2023 EDM standardisation script duplicated company-matching logic in both config and file-mapping code."
  - "Unused helper code remained in the script, including a company-cleaning function that did not match the active regex behavior."
  - "Temporary-directory cleanup and zip extraction were split across multiple thin wrappers, making the control flow harder to follow."
  - "The script mixed runtime package installation into the ingestion step instead of focusing only on file standardisation."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, edm-ingestion, refactor, output-compatibility, script-simplicity]
---

# Troubleshooting: Simplifying the EDM standardisation script without changing outputs

## Problem
The 2021-2023 EDM standardisation script in `scripts/R/01_data_ingestion/edm_individ_data_standardisation_2021-2023.R` still produced the expected renamed files, but the implementation had drifted into unnecessary complexity. The refactor goal was to simplify the script substantially without changing the generated filenames, destination paths, or the `temp_edm_extract` workflow used by downstream ingestion steps.

## Environment
- Module: EDM Ingestion
- Affected component: Repository tooling / ingestion script
- Key file: `scripts/R/01_data_ingestion/edm_individ_data_standardisation_2021-2023.R`
- Date solved: 2026-03-09

## Symptoms
- Company detection rules existed in two places: configuration and a separate `case_when()` matcher.
- `clean_company_name()` existed as a third matching path, but it was unused and inconsistent with the regex-based matching logic.
- Zip extraction tracked `extracted_dirs`, but `main()` ignored that return value and always scanned `temp_edm_extract` recursively.
- Duplicate metadata (`possible_duplicate`, `file_count`) was stored on every row even though it only supported one validation warning.
- Cleanup happened through multiple branches and a one-line wrapper instead of a single obvious exit path.
- The script installed packages at runtime, adding environment side effects unrelated to the actual file-standardisation task.

## What Didn't Work
**Rejected approach 1:** Keep both the config object and the hard-coded `case_when()` matcher, and only delete dead code.  
- **Why it failed:** it would still leave two sources of truth for the same company mapping rules.

**Rejected approach 2:** Keep `process_zip_files()` and `cleanup_temp_directory()` as structural wrappers for readability.  
- **Why it failed:** both were single-use wrappers that obscured the actual pipeline and carried bookkeeping `main()` did not need.

**Rejected approach 3:** Preserve runtime package installation for maximum backwards compatibility.  
- **Why it failed:** it kept hidden environment side effects inside an ingestion script in a repo that already uses a managed project environment.

## Solution
The working refactor reduced the file from 317 lines to 199 while preserving the output contract.

- Replaced `initialise_config()` with two top-level constants: `VALID_EXTENSIONS` and `COMPANY_PATTERNS`.
- Replaced the duplicated `case_when()` plus unused helper arrangement with a single `match_company()` function that evaluates the regex patterns in order.
- Flattened extraction into `extract_edm_archives()`, which recreates `temp_edm_extract`, unzips each archive, counts successful extractions, and returns the temp directory path directly.
- Simplified mapping into `build_file_mapping()`, which computes only the columns needed for the rename/move step and preserves the filename template `"{year}_{company}_edm.{extension}"`.
- Moved duplicate detection into `validate_mappings()` instead of persisting duplicate metadata in the mapping table.
- Replaced the separate cleanup helper with a single `on.exit()` registration in `main()` so cleanup happens in one place on both success and failure.
- Switched bare `here()` calls to `here::here()` so the script no longer depends on runtime package attachment order.
- Added a structured script header documenting purpose, inputs, outputs, and the output-compatibility contract so future refactors can see the non-negotiable behavior before reading the implementation.

**Code changes**:

```r
# Before
initialise_config <- function() {
  list(
    VALID_EXTENSIONS = c("csv", "xlsx", "xlsb"),
    WATER_COMPANIES = list(
      "anglian" = "anglian_water",
      "northumbrian|nwl" = "northumbrian_water"
    )
  )
}

clean_company_name <- function(filename, company_patterns) {
  str_detect(filename_lower, fixed(pattern))
}

company = case_when(
  str_detect(filename_lower, "anglian") ~ water_companies[["anglian"]],
  str_detect(filename_lower, "northumbrian|nwl") ~ water_companies[["northumbrian|nwl"]]
)
```

```r
# After
VALID_EXTENSIONS <- c("csv", "xlsx", "xlsb")
COMPANY_PATTERNS <- c(
  "anglian" = "anglian_water",
  "northumbrian|nwl" = "northumbrian_water"
)

match_company <- function(filename, company_patterns = COMPANY_PATTERNS) {
  filename_lower <- tolower(filename)
  matched_patterns <- names(company_patterns)[vapply(
    names(company_patterns),
    function(pattern) stringr::str_detect(filename_lower, pattern),
    logical(1)
  )]

  if (length(matched_patterns) == 0) return(NA_character_)
  unname(company_patterns[[matched_patterns[[1]]]])
}
```

```r
# Before
unzip_edm_files <- function(zip_path = here()) {
  extracted_dirs <- process_zip_files(zip_files, temp_dir)
  return(extracted_dirs)
}

cleanup_temp_directory <- function(data_path) {
  dir_delete(path(data_path, "temp_edm_extract"))
}
```

```r
# After
extract_edm_archives <- function(data_path, temp_dir = fs::path(data_path, "temp_edm_extract")) {
  if (fs::dir_exists(temp_dir)) fs::dir_delete(temp_dir)
  fs::dir_create(temp_dir)

  for (zip_file in zip_files) {
    utils::unzip(zip_file, exdir = temp_dir)
  }

  temp_dir
}

main <- function(data_path = here::here()) {
  temp_dir <- fs::path(data_path, "temp_edm_extract")

  on.exit({
    if (fs::dir_exists(temp_dir)) fs::dir_delete(temp_dir)
  }, add = TRUE)

  extract_edm_archives(data_path, temp_dir)
  mappings <- build_file_mapping(temp_dir)
  move_standardised_files(mappings, data_path)
}
```

**Verification note:** full end-to-end `Rscript` execution did not complete in this Codex sandbox, so behavioral verification here was static rather than runtime. Output compatibility was preserved by keeping the same company-pattern order, filename template, destination path, and `temp_edm_extract` directory contract.

## Why This Works
The refactor aligns the code structure with the real job of the script: unzip, detect year/company, rename/move, and clean up.

1. There is now one source of truth for company matching, so future edits cannot silently diverge between config and execution logic.
2. The extraction step no longer returns directory bookkeeping that the script never used; it simply prepares the temp directory and unzips archives into it.
3. Duplicate detection still happens, but only where it is needed for validation.
4. Cleanup is centralized in `main()`, so success and failure paths share the same temp-directory lifecycle.
5. The output contract remains stable because the rename template, company regex ordering, supported extensions, and destination path are unchanged.
6. The richer file header makes those compatibility requirements explicit at the top of the script, reducing the chance of an accidental downstream-breaking refactor.

## Prevention
- Keep each ingestion script responsible for one pipeline stage only.
- Make top-level orchestration read like a recipe: setup, extract, map, move, cleanup.
- Keep helpers only when they represent a real step, are reused, or isolate messy logic.
- Use one source of truth for filename-to-company mappings and other classification rules.
- Prefer explicit `pkg::fn` calls or a small fixed import block; do not install packages inside pipeline scripts.
- Keep temp-file lifecycle explicit and centralized.
- Put the output contract in the script header when a pipeline step has downstream filename or path dependencies.
- Do not change the filename contract `"{year}_{company}_edm.{extension}"` without checking downstream scripts and documentation.
- Do not change regex ordering casually; first match wins.
- Add a small fixture-based regression check under `scripts/R/testing/` that asserts the expected renamed filenames for a synthetic EDM input bundle.

## Related Issues
No related solution docs existed when this note was written; this is the first file under `docs/solutions/` in this repository.

Relevant repository references:
- Script overview in [README.md](../../../README.md)
- Ingestion-stage rationale in [02_ingestion.qmd](../../../book/data_clean_documentation/02_ingestion.qmd)
- Pipeline placement in [01_pipeline.qmd](../../../book/data_clean_documentation/01_pipeline.qmd)
