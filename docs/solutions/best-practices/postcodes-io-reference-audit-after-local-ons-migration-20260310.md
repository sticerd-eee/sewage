---
module: System
date: 2026-03-10
problem_type: best_practice
component: development_workflow
symptoms:
  - "The active postcode-enrichment code had already moved to local ONS lookups, but it was still unclear whether the repository was fully free of legacy `postcodes.io` / `PostcodesioR` references."
  - "Targeted searches found no legacy helpers in the main pipeline scripts, yet executable notebooks, current docs, generated artifacts, and `renv.lock` still contained the old API names."
  - "Some user-facing text still described property geocoding through `PostcodesioR` or `postcodes.io`, creating a mismatch between the documented and actual pipeline."
  - "Generated outputs such as `book/_site/search.json` and LaTeX minted fragments preserved old strings even after the source migration, making raw grep results ambiguous."
root_cause: incomplete_setup
resolution_type: workflow_improvement
severity: low
tags: [postcodes-io, postcodesior, ons-postcode-directory, migration-cleanup, reference-audit]
---

# Troubleshooting: Auditing residual `postcodes.io` references after migrating active postcode processing to local ONS lookups

## Problem
After the LR and Zoopla postcode cleaners moved from the legacy `postcodes.io` / `PostcodesioR` workflow to local ONS postcode lookups, the remaining question was narrower than the migration itself: was the repository fully clean, or was only the runtime code clean? A repo-wide audit resolved that ambiguity. The active pipeline had been migrated, but stale references still remained in exploratory notebooks, current documentation, dependency metadata, and generated artifacts.

## Environment
- Module: system-wide postcode migration follow-up
- Affected surfaces:
  - runtime pipeline scripts
  - executable testing notebooks
  - current docs and manuscript text
  - dependency metadata
  - generated artifacts
- Legacy identifiers audited:
  - `PostcodesioR`
  - `postcodes.io`
  - `api.postcodes.io`
  - `bulk_postcode_lookup`
  - `get_postcode_data`
  - `cleanup_postcode_cache`
- Active pipeline checked:
  - `scripts/R/01_*`
  - `scripts/R/02_*`
  - `scripts/R/03_*`
  - `scripts/R/05_*`
  - `scripts/R/06_*`
  - `scripts/R/utils`
- Date solved: 2026-03-10

## Symptoms
- The main pipeline no longer called `PostcodesioR`, `bulk_postcode_lookup()`, `get_postcode_data()`, or `cleanup_postcode_cache()`, but repo-wide search still surfaced those strings elsewhere.
- `scripts/R/testing/test_merge_house_price_data.Rmd` and `scripts/R/testing/test_extract_postcode_data.Rmd` still contained executable legacy API examples.
- `README.md`, `book/data_clean_documentation/01_pipeline.qmd`, and `docs/overleaf/103_appendix_data.tex` still described postcode enrichment through `PostcodesioR` or `postcodes.io`.
- `renv.lock` still retained a `PostcodesioR` entry, and generated files such as `book/_site/search.json` and `docs/overleaf/_minted/*` still echoed the old workflow.
- The result was a false impression that the migration might be incomplete even though the active pipeline had already switched to local ONS inputs.

## What Didn't Work
**Assumption 1:** If the main cleaners were migrated, all legacy references would be gone.  
- **Why it failed:** the code migration was narrower than a full repository cleanup. Tests, docs, generated outputs, and lockfile metadata were left behind.

**Assumption 2:** A raw repo-wide grep hit means the old API is still active.  
- **Why it failed:** some hits were only historical notes or generated files. The audit needed runtime-versus-stale classification, not a flat hit count.

## Solution
The working solution was to treat this as a classification audit rather than a yes/no grep exercise.

1. Search the whole repository for the legacy identifiers.
2. Re-run the search only in the active pipeline folders.
3. Classify the remaining hits into five surfaces:
   - active runtime code
   - executable but non-canonical notebooks
   - current docs and manuscript text
   - dependency metadata
   - generated or historical artifacts
4. Apply a two-tier pass/fail rule:
   - Tier 1 must be clean before saying the migration is complete.
   - Tier 2 is informational and may require regeneration or explicit archival treatment.

**Commands used**:

```bash
rg -n -i "PostcodesioR|postcodes\.io|api\.postcodes\.io|bulk_postcode_lookup|get_postcode_data|cleanup_postcode_cache" .

rg -n -i "PostcodesioR|postcodes\.io|api\.postcodes\.io|bulk_postcode_lookup|get_postcode_data|cleanup_postcode_cache" \
  scripts/R/01_* scripts/R/02_* scripts/R/03_* scripts/R/05_* scripts/R/06_* scripts/R/utils \
  --glob '!**/*.html'
```

**Audit conclusion**:
- The active pipeline is clean: no legacy API hits remained in `scripts/R/01_*`, `scripts/R/02_*`, `scripts/R/03_*`, `scripts/R/05_*`, `scripts/R/06_*`, or `scripts/R/utils`.
- The remaining references fell into identifiable non-runtime buckets:
  - executable notebooks:
    - `scripts/R/testing/test_merge_house_price_data.Rmd`
    - `scripts/R/testing/test_extract_postcode_data.Rmd`
  - current docs and manuscript text:
    - `README.md`
    - `book/data_clean_documentation/01_pipeline.qmd`
    - `docs/overleaf/103_appendix_data.tex`
  - dependency metadata:
    - `renv.lock`
  - generated or archival material:
    - `book/_site/search.json`
    - `docs/overleaf/_minted/*`
    - existing postcode migration notes in `docs/solutions/best-practices/`

**Practical conclusion**:
- **Yes**, the active postcode-processing pipeline has been migrated off the legacy `postcodes.io` / `PostcodesioR` path.
- **No**, the repository as a whole is not yet free of all legacy references.

## Why This Works
The audit works because it answers the operational question directly. A migration is not complete just because the main scripts changed, and a repository is not still broken just because a raw grep finds old names. The important distinction is whether the old identifiers survive in active runtime surfaces, in executable sidecar material, or only in historical or generated text.

That distinction turns an ambiguous repo-wide search into a reliable maintenance rule:

1. Zero hits in active code means the runtime dependency is gone.
2. Hits in executable notebooks, docs, or `renv.lock` mean the migration cleanup is incomplete.
3. Hits in generated files or historical solution notes should be handled as regeneration or archival context, not mistaken for live pipeline debt.

## Prevention
- Treat dependency migrations as five-surface work:
  - runtime code
  - tests and notebooks
  - current docs and manuscript text
  - dependency metadata
  - generated artifacts
- Keep one denylist of legacy identifiers and reuse it for every audit:
  - `PostcodesioR`
  - `postcodes.io`
  - `api.postcodes.io`
  - `bulk_postcode_lookup`
  - `get_postcode_data`
  - `cleanup_postcode_cache`
- Do not treat historical solution notes as stale debt. They can mention old workflows, but only as explicit historical context.
- Update `renv.lock` at the same time as code-level dependency removal rather than leaving metadata cleanup for later.
- After doc changes, regenerate committed derived outputs or exclude them from blocking checks until regeneration is complete.
- Prefer a reusable audit test or script over ad hoc one-off searches.

**Recommended future checks**:

```bash
# Tier 1: must be clean
rg -n -i "PostcodesioR|postcodes\.io|api\.postcodes\.io|bulk_postcode_lookup|get_postcode_data|cleanup_postcode_cache" \
  scripts/R/01_* scripts/R/02_* scripts/R/03_* scripts/R/05_* scripts/R/06_* scripts/R/utils \
  --glob '!**/*.html'

rg -n -i "PostcodesioR|postcodes\.io|bulk_postcode_lookup|get_postcode_data|cleanup_postcode_cache" \
  scripts/R/testing --glob '*.Rmd' --glob '*.qmd'

rg -n -i "PostcodesioR|postcodes\.io|api\.postcodes\.io" \
  README.md book docs/overleaf \
  --glob '!**/_site/**' --glob '!**/_minted/**'

rg -n -i "PostcodesioR|postcodes\.io" renv.lock
```

```bash
# Tier 2: informational, then regenerate or ignore intentionally historical files
rg -n -i "PostcodesioR|postcodes\.io" \
  book/_site docs/overleaf/_minted scripts/R/testing \
  --glob '*.html' --glob 'search.json'
```

## Related Issues
- See also: [lr-house-price-local-ons-postcode-lookup-20260310.md](./lr-house-price-local-ons-postcode-lookup-20260310.md)
- See also: [zoopla-local-ons-postcode-lookup-20260310.md](./zoopla-local-ons-postcode-lookup-20260310.md)
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)

Relevant repository references:
- Audit target: `scripts/R/utils/postcode_processing_utils.R`
- Remaining executable legacy notebooks:
  - `scripts/R/testing/test_merge_house_price_data.Rmd`
  - `scripts/R/testing/test_extract_postcode_data.Rmd`
- Remaining current docs:
  - `README.md`
  - `book/data_clean_documentation/01_pipeline.qmd`
  - `docs/overleaf/103_appendix_data.tex`
- Remaining metadata:
  - `renv.lock`

## Verification Note
This audit was verified on 2026-03-10 by combining repo-wide search with narrowed active-pipeline search.

- The active pipeline search returned no legacy API hits.
- The remaining hits were reproducibly grouped into notebooks, authored docs, dependency metadata, and generated or historical artifacts.
- The audit therefore answered the original follow-up precisely: the runtime migration is complete, but the repository cleanup is not yet complete.
