---
status: pending
priority: p1
issue_id: "001"
tags: [code-review, data-contract, data-integrity, r, pipeline]
dependencies: []
merged_from:
  - "001: Preserve annual rows with missing spill metrics"
  - "013: Validate finalise inputs and annual unmatched site IDs"
---

# Preserve and identify annual unmatched rows

## Merge Note

This TODO consolidates the old `001` annual-metric row-preservation issue and the old `013` finalisation/output-contract issue. It covers missing spill metrics, finalisation input validation, and retention of canonical or documented year-specific site identifiers in `annual_unmatched.parquet`.

## Problem Statement

The location-merge pipeline can silently lose or weaken unmatched annual-return records. `add_zero_spill_stats()` drops annual rows when spill-metric predicates evaluate to `NA`, while `finalise_merged_data()` reads hidden inputs late and trims unmatched annual rows without preserving a canonical `site_id` contract. Together these bugs make annual-unmatched output incomplete and harder for downstream consumers to interpret.

## Findings

- [`merge_individ_annual_location.R:381`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L381) classifies unmatched annual rows using `spill_hrs_ea == 0 & spill_count_ea == 0` and `spill_hrs_ea != 0 | spill_count_ea != 0`.
- Rows where both metrics are `NA` fail both predicates and are dropped from both outputs.
- Synthetic `NA/NA`, `0/NA`, and `NA/0` inputs to `add_zero_spill_stats()` all returned `matched_rows=0` and `annual_nonzero_rows=0`.
- The current annual-return input contains 4,076 rows where both spill metrics are `NA`; mixed `0/NA` and `NA/0` cases are latent in the current data.
- [`investigate_partial_availability_missingness.qmd:190`](../scripts/R/testing/investigate_partial_availability_missingness.qmd#L190) documents the same mechanism behind partial-availability gaps.
- [`merge_individ_annual_location.R:106`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L106) validates only the event and annual-return inputs in `load_data()`.
- [`merge_individ_annual_location.R:923`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L923) reads `annual_return_lookup.parquet` inside finalisation, after the expensive matching stages.
- [`merge_individ_annual_location.R:924`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L924) rereads annual returns instead of using already loaded annual data.
- [`merge_individ_annual_location.R:966`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L966) only renames `outlet_discharge_ngr` for unmatched annual rows before trimming to `CONFIG$keep_cols`.
- The current `annual_unmatched.parquet` has 742 rows and no `site_id` column, even though `annual_return_lookup.parquet` contains canonical site identifiers.

## Proposed Solutions

### Option 1: Preserve unknown-metric annual rows and validate finalisation inputs

**Approach:** Carry `NA/NA`, `0/NA`, and `NA/0` rows forward in a defined unmatched annual bucket; preflight the annual lookup before matching starts; and pass loaded lookup/annual metadata into finalisation.

**Pros:**
- Stops silent annual row loss.
- Fails before expensive matching when lookup inputs are missing or malformed.
- Makes annual-unmatched output easier to consume downstream.

**Cons:**
- May require renaming or documenting `annual_unmatched_nonzero`.
- Requires a small finalisation signature or data-flow change.

**Effort:** 2-4 hours

**Risk:** Low

---

### Option 2: Add a dedicated missing-metrics bucket

**Approach:** Split unmatched annual rows into zero, non-zero, and unknown-metric outputs, then propagate all three through finalisation/export.

**Pros:**
- Makes missing-data state explicit.
- Gives the cleanest downstream contract.

**Cons:**
- Touches more functions and output contracts.
- Requires broader downstream validation.

**Effort:** 3-6 hours

**Risk:** Medium

## Recommended Action

To be filled during triage.

## Technical Details

**Affected files:**
- [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)
- [`scripts/R/testing/investigate_partial_availability_missingness.qmd`](../scripts/R/testing/investigate_partial_availability_missingness.qmd)

**Related components:**
- Annual-return availability logic
- `data/processed/annual_return_lookup.parquet`
- `data/processed/matched_events_annual_data/annual_unmatched.parquet`
- Downstream readers of annual-unmatched outputs

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/05_data_integration/merge_individ_annual_location.R`](../scripts/R/05_data_integration/merge_individ_annual_location.R)
- Investigation notebook: [`scripts/R/testing/investigate_partial_availability_missingness.qmd`](../scripts/R/testing/investigate_partial_availability_missingness.qmd)
- Lookup producer: [`scripts/R/03_data_enrichment/create_annual_return_lookup.R`](../scripts/R/03_data_enrichment/create_annual_return_lookup.R)

## Acceptance Criteria

- [ ] Rows with both spill metrics missing are preserved in a defined output bucket.
- [ ] Rows with asymmetric missing zero-value metrics (`0/NA`, `NA/0`) are preserved in a defined output bucket.
- [ ] No annual-return site-year is silently dropped by `add_zero_spill_stats()`.
- [ ] Missing or malformed `annual_return_lookup.parquet` fails before matching starts.
- [ ] `finalise_merged_data()` does not reread annual returns unnecessarily.
- [ ] `annual_unmatched.parquet` includes a canonical `site_id` or a documented year-specific ID contract.
- [ ] Focused regression checks cover `0/0`, `NA/NA`, non-zero, `0/NA`, and `NA/0` cases.
- [ ] A regression check covers annual-unmatched rows retaining site identifiers after finalisation.
- [ ] Downstream consumers still parse the resulting output contract correctly.

## Work Log

### From 001 - Preserve annual rows with missing spill metrics

**2026-03-10 - Initial Discovery, by Codex**

**Actions:**
- Reviewed [`merge_individ_annual_location.R:381`](../scripts/R/05_data_integration/merge_individ_annual_location.R#L381).
- Reproduced the bug with a synthetic `NA`-metrics annual row.
- Cross-checked the investigation notebook that already traces this failure mode.

**Learnings:**
- The loss happens before export, so downstream files cannot recover these site-years.
- The repository already has evidence that this affects real partial-availability cases.

**2026-03-10 - Scope Expansion, by Codex**

**Actions:**
- Re-tested `add_zero_spill_stats()` with synthetic `0/NA` and `NA/0` inputs.
- Queried `annual_return_edm.parquet` for mixed-missingness cases.

**Learnings:**
- The underlying defect is broader than the original `NA/NA` case.
- The live dataset currently exhibits `NA/NA`, but not `0/NA` or `NA/0`.

### From 013 - Validate finalise inputs and annual unmatched site IDs

**2026-04-23 - Review Discovery, by Codex**

**Actions:**
- Reviewed finalisation input reads and output trimming.
- Checked the current annual-unmatched output schema.
- Confirmed the annual lookup contains canonical `site_id` fields.

**Learnings:**
- The finalisation stage owns both a hidden runtime dependency and an output-contract gap.

## Notes

- This is a silent data-loss and data-contract issue, not just a reporting cleanup.
