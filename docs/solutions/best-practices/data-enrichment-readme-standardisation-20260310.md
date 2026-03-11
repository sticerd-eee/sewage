---
module: R Data Enrichment
date: 2026-03-10
problem_type: best_practice
component: documentation
symptoms:
  - "The `README.md` entry for `03_data_enrichment/` did not follow the `Input` / `Output` / `Purpose` format already used in `02_data_cleaning/`."
  - "The enrichment-layer script inventory mixed high-level method notes with incomplete file-level output descriptions."
  - "The README section omitted `aggregate_daily_spill_rainfall.R` even though the script exists in `scripts/R/03_data_enrichment/`."
  - "Some enrichment entries described output directories or generic datasets instead of the actual parquet or workbook targets written by the scripts."
root_cause: inadequate_documentation
resolution_type: documentation_update
severity: low
tags: [readme, data-enrichment, pipeline-documentation, script-inventory, best-practice]
---

# Troubleshooting: Standardising the `03_data_enrichment` README section to match the pipeline inventory house style

## Problem
The repository README already had a clear pattern for the `02_data_cleaning/` layer: each entry script was documented with a short `Input`, `Output`, and `Purpose` contract. The neighbouring `03_data_enrichment/` section had not kept pace with that standard. It mixed prose styles, under-specified several outputs, and missed one script entirely.

That drift mattered because `03_data_enrichment/` is where multiple downstream analysis datasets are assembled. If the README is meant to be the quick orientation layer for the pipeline, then the enrichment section needs to be just as precise and scan-friendly as the cleaning section.

## Environment
- Module: `README.md` / `scripts/R/03_data_enrichment/`
- Section updated: `#### 03_data_enrichment/ - Data Enrichment & Aggregation Layer`
- Script inventory checked on 2026-03-10:
  - `aggregate_spill_stats.R`
  - `create_annual_return_lookup.R`
  - `create_unique_spill_sites.R`
  - `aggregate_rainfall_stats.R`
  - `identify_dry_spills.R`
  - `aggregate_dry_spill_stats.R`
  - `aggregate_daily_spill_rainfall.R`
- Date solved: 2026-03-10

## Symptoms
- The `03_data_enrichment/` section did not match the formatting contract already established by `02_data_cleaning/`.
- Readers had to infer whether a line was describing inputs, methods, or outputs because several entries mixed those concerns together.
- `aggregate_daily_spill_rainfall.R` was absent from the README even though it is part of the current enrichment-layer script set.
- Output descriptions were inconsistent: some used actual filenames, while others only named a folder or a generic derived dataset.

## What Didn't Work
**Rejected approach 1:** Reflow the existing prose without checking the current scripts on disk.  
- **Why it failed:** that would preserve the missing `aggregate_daily_spill_rainfall.R` entry and would not catch the file-level output gaps.

**Rejected approach 2:** Copy the `02_data_cleaning/` layout mechanically but keep directory-level output summaries.  
- **Why it failed:** the result would look cleaner, but it would still underspecify what each enrichment script actually writes.

## Solution
The working fix was to treat the scripts themselves as the source of truth and then rewrite the README section to match the established house style.

- Replaced the mixed-format `03_data_enrichment/` prose block with one entry per script.
- Standardised every entry to the same three fields used in `02_data_cleaning/`:
  - `Input`
  - `Output`
  - `Purpose`
- Added the missing `aggregate_daily_spill_rainfall.R` entry to make the section complete for the current directory contents.
- Tightened each output description to use the actual configured write targets where they are defined in the scripts.
- Kept the section at README level: precise enough to orient a reader quickly, but not overloaded with implementation detail.

**Updated documentation targets**:

```text
README.md
docs/solutions/best-practices/data-enrichment-readme-standardisation-20260310.md
```

## Why This Works
This works because it restores one consistent documentation contract across adjacent pipeline layers.

1. The README becomes easier to scan because every entry answers the same three questions.
2. The enrichment inventory becomes complete for the current script directory rather than a partial summary.
3. File-level outputs are easier to verify against downstream dependencies when they are named explicitly.
4. The `02_data_cleaning/` and `03_data_enrichment/` sections now read as parts of one coherent pipeline description rather than two different documentation styles.

## Prevention
- When a new entry script is added under a numbered pipeline folder, update the matching README section in the same change.
- Use the `Input` / `Output` / `Purpose` pattern for all script inventories in `README.md`.
- Derive output descriptions from `CONFIG` objects and export functions instead of memory or older docs.
- Prefer explicit file targets over vague folder summaries when a script writes stable named outputs.
- Record non-trivial README contract cleanups in `docs/solutions/` so future documentation work can follow the same precedent.

**Review checks**:

```bash
ls -1 scripts/R/03_data_enrichment
```

```bash
rg -n "#### 03_data_enrichment/|aggregate_daily_spill_rainfall|aggregate_dry_spill_stats|create_annual_return_lookup" README.md
```

```bash
rg -n "write_parquet|write.xlsx|rio::export|lookup_parquet|edge_metadata_parquet|output_file|output_files" scripts/R/03_data_enrichment/*.R
```

## Related Issues
- Style precedent: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)

Relevant repository references:
- Pipeline overview: [`README.md`](../../../README.md)
- Enrichment scripts: [`scripts/R/03_data_enrichment`](../../../scripts/R/03_data_enrichment)

## Verification Note
This was a documentation-only change.

- The `03_data_enrichment/` section was checked against the current `scripts/R/03_data_enrichment/` directory contents.
- Each README output path was cross-checked against the relevant script configuration or export call.
- No code paths, datasets, or generated analysis outputs were changed in this session.
