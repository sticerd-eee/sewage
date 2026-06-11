---
status: pending
priority: p1
issue_id: "002"
tags: [code-review, data-integrity, matching, r, pipeline]
dependencies: []
merged_from:
  - "002: Keep annual-only company-years through max matching"
  - "011: Preserve max-match annual candidates that lose selection"
---

# Preserve annual rows through max matching

## Merge Note

This TODO consolidates the old `002` and old `011` max-match annual-row loss modes: annual-only company-years that never enter the loop, and annual candidates that join but lose the one-best-row selection.

## Problem Statement

The many-to-many max matching stage can silently drop unmatched annual-return records. `run_many_to_many_max_match()` iterates only over company-years present in `events_unmatched`, and `run_max_match()` can discard annual rows that joined but were not selected as the best match. Both paths cause annual site-years to disappear before fuzzy matching and export.

## Findings

- [`merge_individ_annual_location.R:597`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L597) builds `company_years` from `unique(events_unmatched$water_company)` and `unique(events_unmatched$year)` only.
- [`merge_individ_annual_location.R:637`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L637) only binds rows emitted from those iterations.
- Synthetic input with one unmatched event in `A-2021` and one annual-only row in `B-2022` returned `annual_rows=0`, losing the `B-2022` annual row.
- [`merge_individ_annual_location.R:568`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L568) keeps only annual rows that never joined at all when `keep_joined_losses = FALSE`.
- The default `FALSE` path means annual candidates that joined but lost max selection are removed from all outputs.
- Synthetic input with one event and two annual rows sharing the same join key returned `max_matches=1` and `max_annual_unmatched=0`.
- The same duplicated-candidate input with `keep_joined_losses = TRUE` returned `max_annual_unmatched_keep_losses=1`.
- Current annual-return data has 90 duplicate full shared-key groups covering 195 annual rows, so the losing-candidate shape is realistic.
- This is the same class of one-sided iteration bug as the windfall stage, but later in the pipeline.

## Proposed Solutions

### Option 1: Fix the max-match iteration and unmatched contract directly

**Approach:** Build `company_years` from the union of event and annual `(water_company, year)` pairs, pass empty-side subsets through as unmatched, and define the remaining annual set as annual rows not selected by `best_matches`.

**Pros:**
- Fixes both silent drops at their source.
- Aligns "unmatched annual" with "not selected as a match".
- Preserves the existing per-company-year flow.

**Cons:**
- Downstream fuzzy matching may see more annual rows.
- Requires careful row-accounting checks around duplicates.

**Effort:** 2-4 hours

**Risk:** Low

---

### Option 2: Smaller compatibility patch

**Approach:** Post-append annual-only company-years after the current loop and call `run_max_match()` with `keep_joined_losses = TRUE`.

**Pros:**
- Smaller behavioral patch.

**Cons:**
- Leaves brittle iteration/default behavior in place.
- Easier for future callers to regress.

**Effort:** 1-2 hours

**Risk:** Medium

## Recommended Action

To be filled during triage.

## Technical Details

**Affected files:**
- [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)

**Related components:**
- Many-to-many max matching
- Fuzzy matching handoff
- Annual unmatched export

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)

## Acceptance Criteria

- [ ] Annual-only company-years remain present in `annual_unmatched` after `run_many_to_many_max_match()`.
- [ ] The function behaves correctly when one side of a company-year subset is empty.
- [ ] Annual candidates that join but are not selected remain in `annual_unmatched`.
- [ ] Row accounting holds for max-match inputs: annual input rows equal selected annual rows plus unmatched annual rows, allowing for explicitly documented duplicates if any.
- [ ] Regression checks cover mixed event-only, annual-only, and shared company-year inputs.
- [ ] A regression check covers one event with two annual candidates sharing a join key.
- [ ] Downstream fuzzy matching still handles the larger unmatched annual set safely.

## Work Log

### From 002 - Keep annual-only company-years through max matching

**2026-03-10 - Initial Discovery, by Codex**

**Actions:**
- Reviewed the company-year iteration domain in [`merge_individ_annual_location.R:597`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L597).
- Reproduced the loss with a synthetic `A-2021` event row and `B-2022` annual row.

**Learnings:**
- The bug is silent because final `rbindlist()` only sees loop outputs, not untouched annual rows.
- This can understate unmatched annual spill records before fuzzy matching begins.

### From 011 - Preserve max-match annual candidates that lose selection

**2026-04-23 - Review Discovery, by Codex**

**Actions:**
- Reviewed the `keep_joined_losses` branch in `run_max_match()`.
- Reproduced annual loss with synthetic duplicated annual candidates.
- Counted duplicate full shared-key groups in current annual-return data.

**Learnings:**
- The default branch treats "joined but not selected" as disposable, but the pipeline output contract needs a complete unmatched annual bucket.

## Notes

- This is a silent data-loss bug and should block refreshed canonical outputs.
