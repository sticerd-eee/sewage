---
status: pending
priority: p2
issue_id: "008"
tags: [code-review, r, publish-safety, batch-pipeline, data-integrity]
dependencies: []
---

# Stage location merge outputs atomically

`export_results()` writes four Parquet outputs directly into the live output directory one file at a time. If the process fails partway through, consumers can observe a mixed snapshot containing some files from the new run and some from the previous run.

## Findings

- [`scripts/R/05_data_integration/merge_individ_annual_location.R:991`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L991) defines `export_results()` as a direct publish step.
- [`scripts/R/05_data_integration/merge_individ_annual_location.R:1007`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L1007), [`:1010`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L1010), [`:1013`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L1013), and [`:1016`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L1016) write four final Parquet files sequentially.
- There is no staging directory, manifest validation, or rollback path if the third or fourth write fails.
- The repository’s EDM hardening notes already treat fail-closed publish behavior as the preferred pattern for canonical Parquet outputs.

## Proposed Solutions

### Option 1: Write to a temporary directory and swap on success

**Approach:** Write all four Parquets to a temp/staging directory, validate existence/schema, then move them into place only after the full set succeeds.

**Pros:**
- Preserves an all-or-nothing publish boundary.
- Keeps old outputs intact if the run fails.
- Scales well if more outputs are added later.

**Cons:**
- Requires a small amount of publish orchestration code.

**Effort:** 2-4 hours

**Risk:** Low

---

### Option 2: Stage each file with a temp name and final rename

**Approach:** Write `*.tmp.parquet` files alongside the targets and rename each to the final path only after all temporary writes succeed.

**Pros:**
- Smaller directory-management change.
- Works with the existing output layout.

**Cons:**
- More careful cleanup needed on failure.
- Slightly weaker than a full directory-level publish.

**Effort:** 1-3 hours

**Risk:** Medium

---

### Option 3: Add a publish manifest and validation gate

**Approach:** Keep current writes but only mark the run publishable after a manifest file confirms all expected outputs and row/schema checks.

**Pros:**
- Better diagnostics.
- Useful even with another staging approach.

**Cons:**
- Does not by itself prevent consumers from reading partial outputs.

**Effort:** 2-3 hours

**Risk:** Medium

## Recommended Action

**To be filled during triage.**

## Technical Details

**Affected files:**
- `scripts/R/05_data_integration/merge_individ_annual_location.R`

**Related components:**
- `data/processed/matched_events_annual_data/`
- Downstream readers of `matched_events_annual_data.parquet`, `events_unmatched.parquet`, `annual_unmatched.parquet`, and `site_metadata.parquet`

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)
- Related learning: [`docs/solutions/best-practices/edm-api-combine-hardening-20260310.md`](../docs/solutions/best-practices/edm-api-combine-hardening-20260310.md)

## Acceptance Criteria

- [ ] A failed export does not replace any subset of the previous successful output bundle.
- [ ] A successful export publishes a complete, internally consistent set of four Parquet files.
- [ ] Temporary artifacts are cleaned up or safely isolated on failure.
- [ ] A focused regression/manual test documents the partial-failure behavior.

## Work Log

### 2026-03-10 - Initial Discovery

**By:** Codex

**Actions:**
- Reviewed `export_results()` and traced the file-write order.
- Compared the publish behavior with the repository’s fail-closed Parquet guidance.
- Confirmed there is no staging or rollback around the four-file publish boundary.

**Learnings:**
- The risk here is not just a failed file write; it is a mixed snapshot that downstream analysis can consume without noticing.
- This script needs a publish boundary rather than just more logging.

## Notes

- This is operationally important because the output is a bundle, not a single artifact.
