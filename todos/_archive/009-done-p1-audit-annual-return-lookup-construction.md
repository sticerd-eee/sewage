---
status: done
priority: p1
issue_id: "009"
tags: [code-review, data-integrity, matching, annual-return, r, pipeline]
dependencies: []
resolved_date: 2026-06-11
---

# Audit annual return lookup construction

## Problem Statement

`create_annual_return_lookup.R` builds cross-year annual-return site identifiers from pairwise matches. The deterministic path runs, but the graph-to-lookup collapse can silently handle ambiguous components by keeping the first site ID for a year and later appending the other IDs as singletons. That can under-link sites that the selected edge graph itself says are related.

## Findings

- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R:852`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L852) groups by `(component, year)` and uses `first(site_id)`, so components containing multiple same-year IDs are collapsed without an audit trail.
- A review run on 2026-06-09 found 113 `(component, year)` cases with more than one annual-return ID after deterministic matching and MST construction.
- The singleton append step later represents every raw annual-return ID exactly once, so rows are not lost outright. The risk is that ambiguous graph components are split partly through `first(site_id)` and partly as singleton canonical sites.
- Example conflict: one 2024 Anglian Water record was connected to different pre-2024 records through different high-scoring edges for `BRANTHAM - FACTORY LANE (B) TPS`; the current lookup keeps one same-year ID and does not surface the ambiguity.
- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R:802`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L802) assumes every match dataframe has site ID and match metadata columns. If optional RF matching returns a bare zero-column `tibble()`, lookup construction can fail.
- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R:755`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L755) loads `readRDS(model_file)$m_rf`, while training saves `list(model = m_rf, importance = ...)`.
- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R:1002`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1002) tries to load an existing RF model even when `compute_rf_matching = FALSE`, so the default path can unexpectedly run RF if a model file exists and is loaded successfully.
- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R:1032`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R#L1032) assigns canonical `site_id` with `row_number()` after graph/pivot output ordering rather than after an explicit stable sort.

## Proposed Solutions

### Option 1: Fail closed on same-year component conflicts

**Approach:** Before pivoting the membership table, count distinct site IDs by `(component, year)`. If any count is greater than one, write a conflict audit table and stop unless an explicit resolution mode is supplied.

**Pros:**
- Prevents silent under-linking.
- Gives a concrete review artifact for ambiguous sites.
- Keeps the deterministic lookup trustworthy.

**Cons:**
- Requires triage of existing conflicts before refreshing canonical outputs.
- May interrupt the current pipeline until the conflict policy is agreed.

**Effort:** 2-4 hours

**Risk:** Low

---

### Option 2: Resolve conflicted components by edge evidence

**Approach:** Split ambiguous graph components into valid one-ID-per-year subcomponents using edge weights, identifier strength, and manual review flags, then export both resolved lookup rows and discarded/ambiguous edges.

**Pros:**
- Preserves more linkage information than a hard stop.
- Makes the resolution policy explicit and reproducible.

**Cons:**
- Needs careful policy design.
- More scope than a sanity-check patch.

**Effort:** 1-2 days

**Risk:** Medium

---

### Option 3: Harden optional RF matching separately

**Approach:** Make RF execution conditional only on `compute_rf_matching`, load the saved model from `$model`, and ensure empty RF pair outputs carry the expected zero-row schema or are skipped by `build_lookup_from_matches()`.

**Pros:**
- Prevents optional code paths from breaking future runs.
- Makes default deterministic runs insensitive to stale RF artifacts.

**Cons:**
- Does not solve the deterministic component conflicts by itself.

**Effort:** 1-3 hours

**Risk:** Low

## Recommended Action

Start with Option 1 and Option 3. The same-year component conflicts should block trusting refreshed lookup outputs until they are either resolved or explicitly documented.

## Technical Details

**Affected files:**
- [`scripts/R/03_data_enrichment/create_annual_return_lookup.R`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R)

**Related components:**
- `data/processed/annual_return_lookup.parquet`
- `data/processed/annual_return_lookup_edges.parquet`
- Downstream annual-return to canonical-site mapping
- `create_unique_spill_sites.R`

**Database changes (if any):**
- No

## Resources

- Script under review: [`scripts/R/03_data_enrichment/create_annual_return_lookup.R`](../../scripts/R/03_data_enrichment/create_annual_return_lookup.R)
- Downstream lookup consumer: [`scripts/R/03_data_enrichment/create_unique_spill_sites.R`](../../scripts/R/03_data_enrichment/create_unique_spill_sites.R)

## Acceptance Criteria

- [x] Lookup construction detects and reports all components with more than one site ID for the same year.
- [x] The pipeline fails or requires an explicit resolution mode when same-year component conflicts exist.
- [x] Conflict diagnostics include component, year, site IDs, edge metadata, match levels, join keys, and enough annual-return identifiers for manual review.
- [x] Empty RF match outputs cannot break lookup construction.
- [x] RF model loading uses the same object key that model saving writes.
- [x] `compute_rf_matching = FALSE` never runs RF matching because a saved model happens to exist.
- [x] Canonical `site_id` assignment uses a stable documented ordering.

## Work Log

### 2026-06-11 - Resolved and Archived

**By:** Codex

**Actions:**
- Resolved same-year component conflicts with year-constrained graph construction and documented the remaining pre-resolution conflicts as audit evidence rather than canonical lookup data.
- Added conflict audit outputs and a post-resolution safety gate so any future same-year conflict after resolution fails with diagnostics.
- Hardened optional RF behavior so RF remains flag-gated, model loading uses the saved `$model` object, and empty RF outputs cannot break lookup construction.
- Added focused annual-return lookup and combiner contract tests that cover the lookup integrity and RF contract fixes.
- Moved this completed todo to `todos/_archive/`.

**Learnings:**
- The annual-return lookup now enforces exact yearly ID coverage and zero duplicate yearly IDs through code and contract tests.
- The RF path is safe to keep available for evaluated future use while staying off in production by default.

### 2026-06-09 - Review Discovery

**By:** Codex

**Actions:**
- Parsed `create_annual_return_lookup.R` successfully.
- Checked the annual-return parquet schema through the project `rv` R environment.
- Ran deterministic matching and lookup construction in-memory without exporting outputs.
- Counted same-year component conflicts and inspected an example ambiguous component.
- Reviewed optional RF path contracts and downstream lookup consumers.

**Learnings:**
- The current deterministic run covers every raw annual-return ID once after singleton appending, but ambiguous matched components can still be split silently.
- The most important issue is not syntax or row loss; it is an unobserved resolution choice in the graph collapse.
- The optional RF code path has save/load and empty-output contract drift that should be fixed before enabling it.

## Notes

- Review command outputs showed Arrow CPU-info warnings under the sandbox, but the project library and data read succeeded.
- `output/` and `data/` are symlinks to Dropbox-backed project storage, so this todo is the durable in-repo review artifact.
