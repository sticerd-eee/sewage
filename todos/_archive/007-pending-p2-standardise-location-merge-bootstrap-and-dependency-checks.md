---
status: done
priority: p2
issue_id: "007"
tags: [code-review, r, quality, batch-pipeline, dependency-management]
dependencies: []
resolved_date: 2026-06-08
---

# Standardise location merge bootstrap and dependency checks

`merge_individ_annual_location.R` still uses legacy runtime bootstrap logic: it installs packages during normal execution, can call `renv::init()`, and does not preflight all packages it later uses. That makes the script non-deterministic and causes late failures in clean environments.

## Findings

- [`scripts/R/05_data_integration/merge_individ_annual_location.R:16`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L16) defines `initialise_environment()` with runtime package installation.
- [`scripts/R/05_data_integration/merge_individ_annual_location.R:18`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L18) calls `install.packages("renv")` when `renv` is absent.
- [`scripts/R/05_data_integration/merge_individ_annual_location.R:20`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L20) can call `renv::init()` during normal execution, which is repo-mutating and potentially interactive.
- [`scripts/R/05_data_integration/merge_individ_annual_location.R:24`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L24) omits `arrow` from `required_packages`, even though [`scripts/R/05_data_integration/merge_individ_annual_location.R:127`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L127) and [`scripts/R/05_data_integration/merge_individ_annual_location.R:1007`](../../scripts/R/05_data_integration/merge_individ_annual_location.R#L1007) use `arrow::read_parquet()` / `arrow::write_parquet()`.
- Repository guidance now prefers `scripts/R/utils/script_setup.R` plus fail-fast dependency validation rather than runtime installation.

## Proposed Solutions

### Option 1: Replace the local bootstrap with `script_setup.R`

**Approach:** Source the shared setup helper, declare a `REQUIRED_PACKAGES` vector including `arrow`, and fail with a clear `renv::restore()` message when anything is missing.

**Pros:**
- Aligns with current repository standard.
- Removes runtime mutation and network dependence.
- Fails early and predictably.

**Cons:**
- Requires touching package references/import style in the script.

**Effort:** 1-3 hours

**Risk:** Low

---

### Option 2: Keep local setup but remove installation behavior

**Approach:** Retain `initialise_environment()` but replace `install.packages()` / `renv::init()` with `requireNamespace()` checks and add `arrow`.

**Pros:**
- Smaller change set.
- Still fixes the worst operational risk.

**Cons:**
- Continues bootstrap duplication.
- Leaves more room for future drift versus the shared helper.

**Effort:** 1-2 hours

**Risk:** Low

---

### Option 3: Incrementally move to explicit namespace usage

**Approach:** Reduce package attachment breadth and call functions with `pkg::fn`, using the shared preflight only for installed-package checks.

**Pros:**
- Cleaner long-term dependency surface.
- Fewer namespace conflicts.

**Cons:**
- Larger refactor.
- Not required to solve the immediate runtime risk.

**Effort:** 3-6 hours

**Risk:** Medium

## Recommended Action

**To be filled during triage.**

## Technical Details

**Affected files:**
- `scripts/R/05_data_integration/merge_individ_annual_location.R`
- `scripts/R/utils/script_setup.R`

**Related components:**
- Repository-wide script bootstrap pattern
- Headless/batch execution of R data jobs

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../../scripts/R/05_data_integration/merge_individ_annual_location.R)
- Shared helper: [`scripts/R/utils/script_setup.R`](../../scripts/R/utils/script_setup.R)
- Related learning: [`docs/solutions/best-practices/data-cleaning-script-header-bootstrap-standardisation-20260310.md`](../../docs/solutions/best-practices/data-cleaning-script-header-bootstrap-standardisation-20260310.md)
- Related learning: [`docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md`](../../docs/solutions/best-practices/script-setup-runtime-package-cleanup-ingestion-20260310.md)

## Acceptance Criteria

- [x] The script does not call `install.packages()` or `renv::init()` during normal execution.
- [x] All required packages, including `arrow`, are validated before any data work begins.
- [x] Missing dependencies fail with a clear instruction to run `rv sync`.
- [x] The script can be sourced or run headlessly without interactive setup side effects.

## Work Log

### 2026-06-08 - Resolved and Archived

**By:** Codex

**Actions:**
- Replaced local runtime package setup with the shared `script_setup.R` helper.
- Added explicit `REQUIRED_PACKAGES`, including `arrow`.
- Standardised the script header and logging bootstrap.
- Verified the script parses and can be sourced without running `main()`.
- Moved this completed todo to `todos/_archive/`.

### 2026-03-10 - Initial Discovery

**By:** Codex

**Actions:**
- Reviewed the local bootstrap in `merge_individ_annual_location.R`.
- Compared it with the repository’s newer shared setup pattern.
- Traced late `arrow` usage that is not covered by the current required-package list.

**Learnings:**
- The merge script is lagging behind the rest of the EDM pipeline’s bootstrap cleanup.
- The operational risk is both environmental mutation and late failure.

## Notes

- This is important but not as immediately merge-blocking as the confirmed data-loss and idempotence defects.
