---
module: EDM API Pipeline
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "Fatal errors in the 2024+ EDM API combine step were logged but did not propagate to the process exit code."
  - "The canonical combined API Parquet could be overwritten by empty or invalid publish candidates."
  - "Irrelevant upstream Parquet schema differences could break binding before the cleaner narrowed columns."
  - "Malformed timestamp values were dropped without explicit parse or row-drop counts in logs."
root_cause: logic_error
resolution_type: code_fix
severity: high
tags: [r, edm-api, parquet, batch-pipeline, failure-semantics, publish-safety, schema-drift, regression-checks]
---

# Troubleshooting: Hardening the 2024+ EDM API combine step without over-engineering it

## Problem
The live 2024+ EDM API combine step was already aligned to the England-only contract, but it still had several operational hardening gaps. A failed run could log a fatal error and still exit successfully, an empty or invalid cleaned dataset could replace the canonical combined Parquet, irrelevant upstream schema drift could break the bind before cleaning narrowed the columns, and malformed timestamps could disappear through filtering without any explicit count in the logs.

The goal of the fix was to close those gaps without turning the script into a heavily abstracted mini-framework. The intended boundary stayed the same: keep the contract in `api_config.R`, keep partial company absence as warnings unless policy changes, and harden the combine step enough that schedulers and downstream scripts can trust its success or failure semantics.

## Environment
- Module: EDM API Pipeline / `scripts/R/02_data_cleaning/`
- Affected component: Repository tooling / batch combine script
- Key files:
  - `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`
  - `scripts/R/testing/test_edm_api_pipeline_contracts.R`
- Related contract source:
  - `scripts/config/api_config.R`
- Date solved: 2026-03-10

## Symptoms
- A missing input directory or other fatal failure produced a log entry, but `Rscript` still exited with status `0`.
- The combine path treated zero-input and zero-row outputs as publishable, which could replace the last known good combined Parquet with a bad artifact.
- The read path bound full Parquet schemas first, so irrelevant extra columns with conflicting types could sink the combine step before the cleaner selected the downstream-needed fields.
- Timestamp parsing relied on coercion plus filtering, so malformed values caused row loss without an explicit summary of what had been dropped.
- The hardening work needed to preserve the already-established contract rule that missing in-scope companies are warnings, not automatic fatal errors.

## What Didn't Work
**Rejected approach 1:** Make any missing in-scope company file a fatal combine error.  
- **Why it failed:** the earlier England-only contract note explicitly treated partial company availability as warning-only unless policy changes, so making this fatal would have changed pipeline policy rather than just hardening the combine step.

**Rejected approach 2:** Leave full-schema Parquet reads in place and rely on later `select()` to define the contract.  
- **Why it failed:** irrelevant upstream schema drift could still break the bind before the cleaner ever narrowed to the required columns.

**Rejected approach 3:** Fold atomic temp-file promotion into the same fix set.  
- **Why it failed:** atomic staging is valid follow-on hardening, but the primary bug was that clearly empty or invalid publish candidates were treated as successful outputs. Solving that core issue first kept the patch proportionate.

## Solution
The working fix hardened the combine step in four places while keeping the script structure small and local.

- Added `read_required_parquet_file()` and switched the combine path to read only `REQUIRED_INPUT_COLUMNS` before binding.
- Added `parse_api_timestamp_column()` so missing and unparseable timestamp values are counted and logged explicitly.
- Added `validate_publishable_combined_data()` to reject `NULL`, zero-row, or schema-incomplete outputs before publishing.
- Changed `main()` to fail closed on zero-input and zero-row-cleaned paths, and to rethrow fatal errors after logging so process exit semantics are correct.
- Added focused regression coverage in `scripts/R/testing/test_edm_api_pipeline_contracts.R` for:
  - irrelevant extra-column schema drift
  - timestamp parse/drop logging
  - non-zero exit on fatal failure
  - preserving an existing canonical output on no-input and zero-row paths

**Code changes**:

```r
parquet_data_list <- purrr::map(
  parquet_files,
  read_required_parquet_file,
  required_columns = REQUIRED_INPUT_COLUMNS
)
combined_data <- dplyr::bind_rows(parquet_data_list)
```

```r
validate_publishable_combined_data <- function(
    data_to_write,
    output_path,
    expected_columns = EXPECTED_OUTPUT_COLUMNS) {
  if (is.null(data_to_write) || nrow(data_to_write) == 0) {
    logger::log_error(
      "Cannot publish combined Parquet file to {output_path}: combined dataset is empty or NULL."
    )
    return(FALSE)
  }

  missing_columns <- setdiff(expected_columns, names(data_to_write))
  if (length(missing_columns) > 0) {
    logger::log_error(
      "Cannot publish combined Parquet file to {output_path}: missing expected output columns."
    )
    return(FALSE)
  }

  TRUE
}
```

```r
error = function(e) {
  logger::log_error("Fatal error during main execution: {conditionMessage(e)}")
  stop(conditionMessage(e), call. = FALSE)
}
```

## Why This Works
The root problem was not one isolated bug. The combine step had weak operational boundaries.

1. Narrowing required columns on read removes accidental coupling to irrelevant upstream schema changes, so the bind depends only on fields the cleaner actually uses.
2. Explicit publish validation prevents the canonical output from being replaced by an empty or structurally invalid dataset.
3. Rethrowing after logging restores correct batch-process semantics for wrappers, schedulers, and downstream pipeline stages that depend on the shell exit code.
4. Timestamp logging turns silent row loss into observable contract behaviour without forcing a heavier quarantine or side-artifact design.
5. Keeping missing-company handling as warnings preserves the already-documented England-only contract rather than quietly redefining it during a hardening pass.

## Prevention
- Keep `scripts/config/api_config.R` as the only source of truth for live 2024+ scope, paths, and company mappings.
- Keep `main()` responsible for fatal failure semantics. Helpers can return `NULL` or `FALSE`, but avoid deeper catch-log-rethrow layers unless they add context that the top-level boundary cannot.
- Always narrow Parquet reads to downstream-required columns before binding. Do not reintroduce full-schema binds and hope later cleaning will save them.
- Treat publish safety separately from contract completeness:
  - partial company availability stays a warning unless policy changes
  - empty or invalid combined output must fail closed
- Keep timestamp observability lightweight: log counts of missing or unparseable values and rows dropped, rather than adding per-row diagnostics or side artifacts by default.
- Prefer one reusable validation gate for publishable output instead of sprinkling ad hoc row-count and schema checks across the script.

**Regression / operational checks**:

```bash
Rscript --vanilla scripts/R/testing/test_edm_api_pipeline_contracts.R
```

- Confirm logs show:
  - the count of in-scope files discovered
  - any missing or unexpected company IDs
  - timestamp parse/drop summaries when relevant
  - explicit success or fatal failure
- When a run fails, verify the existing canonical combined Parquet still exists and its modification time has not changed.

## Related Issues
- See also: [england-only-edm-api-contract-alignment-20260310.md](./england-only-edm-api-contract-alignment-20260310.md)
- See also: [annual-return-combiner-simplification-20260310.md](./annual-return-combiner-simplification-20260310.md)
- See plan: [2026-03-09-refactor-align-edm-api-pipeline-contracts-plan.md](../../plans/2026-03-09-refactor-align-edm-api-pipeline-contracts-plan.md)

Solved review items:
- [001-pending-p1-propagate-fatal-combine-failures.md](../../../todos/001-pending-p1-propagate-fatal-combine-failures.md)
- [002-pending-p1-protect-last-known-good-combined-output.md](../../../todos/002-pending-p1-protect-last-known-good-combined-output.md)
- [003-pending-p2-narrow-and-validate-combine-schema.md](../../../todos/003-pending-p2-narrow-and-validate-combine-schema.md)
- [004-pending-p2-surface-timestamp-parse-drop-counts.md](../../../todos/004-pending-p2-surface-timestamp-parse-drop-counts.md)

Follow-on but out of scope for this fix:
- [007-pending-p2-stage-combined-edm-output-atomically.md](../../../todos/007-pending-p2-stage-combined-edm-output-atomically.md)

Relevant repository references:
- Script: [`scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`](../../../scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R)
- Regression check: [`scripts/R/testing/test_edm_api_pipeline_contracts.R`](../../../scripts/R/testing/test_edm_api_pipeline_contracts.R)
- Contract source: [`scripts/config/api_config.R`](../../../scripts/config/api_config.R)

## Verification Note
This solution was verified on 2026-03-10 with the focused EDM API contract test script.

- `Rscript --vanilla scripts/R/testing/test_edm_api_pipeline_contracts.R` passed.
- The updated regression coverage now exercises the exact hardening goals: non-zero exit on fatal failure, publish refusal on no-input and zero-row paths, schema narrowing before bind, and explicit timestamp parse/drop observability.

This note documents the solved combine-step hardening pass. It does not claim that the script now uses atomic file promotion; that remains a separate follow-on item rather than part of the fix set captured here.
