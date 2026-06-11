---
status: pending
priority: p1
issue_id: "004"
tags: [code-review, data-integrity, matching, performance, r, pipeline]
dependencies: []
merged_from:
  - "004: Keep event-only companies in windfall output"
  - "010: Restrict windfall matching to valid site keys"
---

# Harden windfall matching stage

## Merge Note

This TODO consolidates the old `004` windfall contract issue and the old `010` weak-key issue: event-only companies should survive as unmatched events, and exact matching should never fall through to weak company/year-only keys.

## Problem Statement

The windfall exact-match stage can both drop unmatched event rows before matching and create false-positive matches with weak keys. `merge_events_with_annual_returns()` only loops over annual-return companies, while `run_windfall_match()` generates all shared-column key combinations, allowing fallthrough to keys such as `water_company|year`.

## Findings

- [`merge_individ_annual_location.R:414`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L414) builds `company_year_combinations` from `unique(annual_return_df$water_company)` and `CONFIG$years`.
- Event-only companies never enter the matching loop.
- Synthetic input with events for `A` and `B`, but annual data only for `A`, returned `events_unmatched_rows=0`, dropping the `B` event.
- Current production inputs may overlap fully, but the function contract is wrong for renamed, partial, or future company inventories.
- [`merge_individ_annual_location.R:318`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L318) builds keys from all shared columns instead of a curated list of valid site identifiers.
- [`merge_individ_annual_location.R:334`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L334) only requires uniqueness on the annual side; it does not require a strong identifier beyond grouping columns.
- Synthetic input with two unrelated event rows and one annual row in the same company-year produced `weak_key_matched_rows=2`, `weak_key_unmatched_events=0`, and `weak_key_match_key=water_company|year`.
- Current inputs have 9 shared event/annual columns, meaning up to 511 key combinations per company-year; adding shared columns doubles or quadruples the search space.
- The largest current company-year event group has hundreds of thousands of rows, so weak-key fallthrough is a correctness and scaling risk.

## Proposed Solutions

### Option 1: Fix loop domain and use curated exact-match keys

**Approach:** Build the windfall loop domain from the union of event and annual `(water_company, year)` pairs, pass through one-sided empty subsets as unmatched, and replace unrestricted key generation with an ordered whitelist that always includes at least one strong site identifier.

**Pros:**
- Preserves event-only companies as unmatched.
- Prevents matches based only on blocking variables.
- Makes exact-match policy reviewable and bounded.

**Cons:**
- Requires agreement on valid identifier combinations.
- Requires explicit handling of one-sided empty subsets.

**Effort:** 3-5 hours

**Risk:** Low

---

### Option 2: Smaller patch with generated-key filtering

**Approach:** Append untouched event-only rows after the current loop and filter generated keys to require at least one strong identifier, with a cap on generated keys.

**Pros:**
- Smaller immediate patch.

**Cons:**
- More indirect than fixing the loop and key policy directly.
- Remaining key space can still grow unless tightly bounded.

**Effort:** 2-4 hours

**Risk:** Medium

## Recommended Action

To be filled during triage.

## Technical Details

**Affected files:**
- [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)

**Related components:**
- Windfall exact-match stage
- Unmatched event carry-forward into later stages
- Stage-handoff regression tests for location merge

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)
- Related test gap: [`006-pending-p2-add-stage-handoff-regression-tests-for-location-merge.md`](006-pending-p2-add-stage-handoff-regression-tests-for-location-merge.md)

## Acceptance Criteria

- [ ] Event-only companies remain present in `events_unmatched`.
- [ ] The function handles event-only, annual-only, and shared company-year subsets correctly.
- [ ] Windfall matching never uses `water_company|year` alone as a match key.
- [ ] Every exact-match key includes at least one strong site identifier.
- [ ] Synthetic unrelated records in the same company-year remain unmatched.
- [ ] The key search space is bounded and logged.
- [ ] Regression checks cover mismatched company inventories and weak-key fallthrough.

## Work Log

### From 004 - Keep event-only companies in windfall output

**2026-03-10 - Initial Discovery, by Codex**

**Actions:**
- Reviewed the windfall loop domain in [`merge_individ_annual_location.R:414`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L414).
- Reproduced the silent drop with a synthetic event-only company.

**Learnings:**
- The loss happens before any matching logic runs.
- This bug stays hidden as long as both upstream datasets use identical company inventories.

### From 010 - Restrict windfall matching to valid site keys

**2026-04-23 - Review Discovery, by Codex**

**Actions:**
- Reviewed windfall key generation and matching semantics.
- Reproduced weak-key fallthrough with synthetic data.
- Compared key generation with current shared-column count.

**Learnings:**
- The issue is not only performance: weak fallback keys can create false-positive site matches.
- A curated key list would address both correctness and runtime risk.

## Notes

- This should block rerunning or trusting refreshed outputs until fixed.
