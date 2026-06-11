---
status: pending
priority: p2
issue_id: "006"
tags: [code-review, r, testing, regression, matching]
dependencies: ["001", "002", "003", "004"]
renamed_from: "old duplicate 005 location-merge regression-test TODO"
---

# Add real regression tests for location-merge stage handoffs

## Problem Statement

The current test artifact for this script is an `.rmd` that largely re-embeds production logic instead of asserting stage-handoff invariants. That setup provides weak protection against the merge-blocking bugs found in review, because the notebook can drift with the implementation and still appear to validate the script.

## Findings

- `scripts/R/testing/test_merge_individ_annual_location.rmd` copies substantial chunks of production code, including orchestration helpers for windfall, max-match, and fuzzy matching.
- There is no focused regression check asserting invariants such as:
  - annual rows with both spill metrics `NA` are preserved in a defined bucket
  - annual-only company-years survive `run_many_to_many_max_match()`
  - `run_fuzzy_matching()` returns cleanly when no unmatched events remain
  - event-only company-years survive or fail explicitly during exact matching
- The repo's broader ETL learnings emphasise output-shape validation and runtime invariants, but this script lacks comparable targeted checks.

## Proposed Solutions

### Option 1: Add focused function-level tests under `scripts/R/testing/`

**Approach:** Create small synthetic-data checks that source the production script and assert the stage-handoff invariants directly.

**Pros:**
- Directly targets the discovered failure modes.
- Fast to run and easy to keep aligned with production.

**Cons:**
- Adds a small amount of dedicated test maintenance.

**Effort:** 2-4 hours

**Risk:** Low

---

### Option 2: Convert the notebook into an assertion-driven regression harness

**Approach:** Keep the `.rmd`, but remove copied implementations and replace them with sourced production functions plus explicit `stopifnot()` checks.

**Pros:**
- Reuses an existing artifact.
- Preserves the exploratory narrative.

**Cons:**
- Notebooks are still weaker than small direct regression scripts for quick verification.

**Effort:** 3-5 hours

**Risk:** Medium

---

### Option 3: Add post-run dataset invariant checks only

**Approach:** Keep the current notebook and add output-level row-count and schema checks after full script execution.

**Pros:**
- Broader end-to-end coverage.

**Cons:**
- Harder to localise failures.
- Can miss stage-specific loss modes if final outputs still look plausible.

**Effort:** 2-3 hours

**Risk:** Medium

## Recommended Action

To be filled during triage.

## Technical Details

**Affected files:**
- `scripts/R/testing/test_merge_individ_annual_location.rmd`
- `scripts/R/05_data_integration/merge_individ_annual_location.R`

**Related components:**
- Windfall matching
- Many-to-many max-match
- Fuzzy matching
- Final export contract

**Database changes (if any):**
- No database changes

## Resources

- **Current test artifact:** `scripts/R/testing/test_merge_individ_annual_location.rmd`
- **Related learning:** `docs/solutions/best-practices/individ-edm-combiner-safe-readability-refactor-validation-20260310.md`
- **Related investigation:** `scripts/R/testing/investigate_partial_availability_missingness.qmd`

## Acceptance Criteria

- [ ] There is a targeted regression test for the all-`NA` annual-metrics case.
- [ ] There is a targeted regression test for an annual-only company-year reaching max-match.
- [ ] There is a targeted regression test for the empty-event fuzzy-stage case.
- [ ] There is a targeted regression test for an event-only company-year reaching windfall matching.
- [ ] Tests source production code rather than duplicating it.

## Work Log

### 2026-03-10 - Review Discovery

**By:** Codex

**Actions:**
- Reviewed `scripts/R/testing/test_merge_individ_annual_location.rmd`.
- Confirmed that the notebook mirrors large portions of production logic instead of acting as a compact regression harness.
- Mapped missing tests directly to the four concrete stage-handoff bugs found in review.

**Learnings:**
- The current "test" artifact can drift with the implementation and give false confidence.
- Small synthetic-data assertions would protect this script much more effectively than copied notebook code.
