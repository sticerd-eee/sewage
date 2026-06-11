---
status: pending
priority: p1
issue_id: "003"
tags: [code-review, fuzzy-matching, performance, r, reliability, pipeline]
dependencies: []
merged_from:
  - "003: Guard fuzzy stage when no events remain"
  - "012: Bound fuzzy candidate generation"
---

# Harden fuzzy matching stage

## Merge Note

This TODO consolidates the old `003` fuzzy empty-result correctness issue and the old `012` fuzzy candidate-bound issue: valid empty/no-candidate inputs should not crash, and large company-year blocks should not materialize unbounded Cartesian candidate sets.

## Problem Statement

`run_fuzzy_matching()` can crash when fuzzy matching yields no joinable rows, and the candidate-generation step can create very large company-year Cartesian products. The stage needs an explicit no-op/empty-result contract and bounded candidate generation.

## Findings

- [`merge_individ_annual_location.R:846`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L846) builds `blocks` from `prepped$events[[block_var]]`.
- If `events_unmatched` is empty, `bind_rows()` produces a zero-column tibble for `fuzzy_merges`.
- The reconstruction join assumes `group_id_annual` exists and errors with `Join columns in x must be present in the data. Problem with group_id_annual.`
- Synthetic empty-event input triggers that error.
- Synthetic non-empty inputs with no usable comparator columns also trigger the same error because every block returns `NULL`.
- [`merge_individ_annual_location.R:1049`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L1049) calls `run_fuzzy_matching()` unconditionally from `main()`.
- [`merge_individ_annual_location.R:694`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L694) calls `pair_blocking()` using only a `water_company`/`year` block.
- Existing logs show fuzzy batches as high as 155,236 candidate pairs, including a batch that selected 0 matches.
- Current outputs still leave 124,368 final unmatched events and 742 unmatched annual rows; row-preservation fixes may increase fuzzy-stage inputs.
- The stage has no hard candidate cap, chunking, stronger blocking keys, or top-k candidate policy.

## Proposed Solutions

### Option 1: Add explicit empty-stage handling and candidate caps

**Approach:** Return `final_res` unchanged when either side is empty; create a typed empty fuzzy result when blocks produce no selected pairs; add a documented per-block candidate ceiling and log/skip or fail closed when exceeded.

**Pros:**
- Fixes valid-state crashes.
- Prevents runaway memory/runtime behavior.
- Makes oversized blocks visible in logs.

**Cons:**
- Requires a deliberate policy for oversized fuzzy blocks.

**Effort:** 3-6 hours

**Risk:** Low to medium

---

### Option 2: Add stronger blocking or top-k prefilters

**Approach:** Use additional blocking keys when available, such as permit prefixes, normalized site-name buckets, site IDs, NGR/grid prefixes, or cheap top-k candidate filters before EM scoring.

**Pros:**
- Reduces candidate volume while keeping fuzzy matching useful.
- Gives deterministic limits per annual record or block.

**Cons:**
- Requires validation that plausible matches are not silently discarded.

**Effort:** 1-2 days

**Risk:** Medium

## Recommended Action

To be filled during triage.

## Technical Details

**Affected files:**
- [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)

**Related components:**
- Fuzzy linkage stage
- Pipeline orchestration in `main()`
- Location-merge regression/stress tests
- Candidate-count logging

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)
- Existing log: `output/log/10_merge_individ_annual_location.log`

## Acceptance Criteria

- [ ] `run_fuzzy_matching()` returns a valid result when no event rows remain.
- [ ] `run_fuzzy_matching()` returns a valid result when no annual rows remain.
- [ ] `run_fuzzy_matching()` returns a valid result when no comparator columns or candidate pairs exist.
- [ ] `main()` can complete successfully after exact/max matching exhausts all events.
- [ ] Fuzzy candidate generation has a documented maximum per block.
- [ ] Oversized blocks fail closed, skip with explicit unmatched carry-forward, or chunk safely.
- [ ] Candidate-count logging includes the block identifier.
- [ ] Regression checks cover empty-event, empty-annual, both-empty, zero-comparator, and large company-year block inputs.
- [ ] Stronger blocking does not silently discard plausible matches without an explicit policy.

## Work Log

### From 003 - Guard fuzzy stage when no events remain

**2026-03-10 - Initial Discovery, by Codex**

**Actions:**
- Reviewed the empty-input path in [`merge_individ_annual_location.R:846`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L846).
- Reproduced failure with a synthetic `final_res` containing zero unmatched events.

**Learnings:**
- The crash is caused by a schema assumption on `fuzzy_merges`, not by `reclin2`.
- This is a valid pipeline end-state, so the function needs an explicit no-op branch.

**2026-03-10 - Scope Expansion, by Codex**

**Actions:**
- Re-ran `run_fuzzy_matching()` with non-empty event and annual inputs sharing only blocking columns.
- Confirmed the same `group_id_annual` join error when no usable comparator set exists.

**Learnings:**
- The real fault boundary is schema-less `fuzzy_merges`, not just "no unmatched events remain".

### From 012 - Bound fuzzy candidate generation

**2026-04-23 - Review Discovery, by Codex**

**Actions:**
- Reviewed fuzzy candidate generation and blocking.
- Checked current output sizes and existing fuzzy-stage logs.
- Compared the stage with row-preservation fixes that may increase unmatched inputs.

**Learnings:**
- Correctness fixes that preserve rows will likely put more pressure on this fuzzy stage, so candidate bounds should be handled before or alongside those changes.

## Notes

- This should block merge if valid empty inputs still crash or large blocks can exhaust a full pipeline run.
