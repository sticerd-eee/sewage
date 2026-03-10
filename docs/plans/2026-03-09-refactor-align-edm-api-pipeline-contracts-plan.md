---
title: "refactor: Align 2024+ EDM API pipeline contracts"
type: refactor
status: completed
date: 2026-03-09
---

# refactor: Align 2024+ EDM API pipeline contracts

## Overview

The immediate downloader fixes are done: the fetch script now builds ArcGIS query URLs consistently, rejects API error payloads, and no longer mutates the environment at runtime. The remaining work is narrower and should be treated as contract alignment across the rest of the 2024+ API pipeline rather than more emergency bug fixing in the fetcher itself.

There are two follow-on areas worth addressing:

1. The downstream 2024+ processing scripts still install packages at runtime, which repeats the exact ingestion-side-effect pattern already removed from the fetch layer.
2. The pipeline’s company-coverage and storage-path contracts are still inconsistent across config, downstream scripts, and documentation.

## Problem Statement / Motivation

The current repository state still has a few avoidable inconsistencies:

- `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R:15-40` still installs packages at runtime.
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R:15-39` still installs packages at runtime.
- `scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R:17` still installs packages at runtime inside the integration layer that consumes the 2024+ API output.
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R:66-76` hard-codes a ten-company list including `Welsh Water`, while the live API config currently defines nine companies.
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R` uses `year(start_time)` in cleaning logic but does not explicitly declare `lubridate` in its required package set, which is fragile in a clean session.
- `scripts/config/api_config.R` still ends with a dangling “API 10: Welsh Water” placeholder rather than either a real Welsh entry or an explicit England-only scope statement.
- The current raw and processed data directories reflect this mismatch in practice: there are nine API company folders/files, not ten.
- `ReadMe.md:70` and `ReadMe.md:144-146` still describe the API data as covering “all 10 WaSCs” and say raw JSON snapshots are written to `data/processed/edm_api_data/`, while the actual raw snapshot path is `data/raw/edm_data/raw_api_responses`.
- `book/data_clean_documentation/02_ingestion.qmd` correctly describes the two-step raw-to-processed flow at a high level, but it does not resolve the current company-coverage ambiguity.
- `scripts/R/testing/test_api.Rmd` still embeds a stale copy of the pre-fix fetch logic, so it is no longer a reliable regression asset for the current downloader.
- `scripts/R/testing/test_06_combine_api_edm_data_2024_onwards.Rmd` still reflects the older ten-company assumption, so the downstream contract is not being regression-tested against the current England-only configuration.

The institutional learning in `docs/solutions/best-practices/output-compatible-edm-standardisation-refactor-20260309.md` points in the same direction: keep ingestion scripts side-effect free, keep one source of truth for classifications/contracts, and add small regression checks rather than relying on live runs.

## Proposed Solution

### Workstream 1: Remove Runtime Package Installation from Remaining 2024+ Scripts

Apply the same fail-fast dependency policy already used in the fetcher to:

- `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R`
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R`
- `scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R`

Both scripts should:

- stop with a clear `renv::restore()` message when dependencies are missing
- avoid `install.packages()` and `renv` initialization during normal execution
- avoid attaching more packages than they need

### Workstream 2: Make Company Coverage an Explicit Contract

Decide and encode one of these two contracts:

#### Option A: England-only live API pipeline

- Treat the 2024+ live API pipeline as the England-only National Storm Overflows Hub feed.
- Keep the current nine-company fetch config.
- Remove or guard `Welsh Water` assumptions in downstream 2024+ combine logic and docs.
- Replace the dangling Welsh placeholder in `api_config.R` with an explicit scope comment so the omission is intentional rather than ambiguous.

#### Option B: England + Welsh live API pipeline

- Add a Welsh Water ingestion source explicitly.
- Decide whether it comes from a public ArcGIS/Stream-compatible endpoint or a separate Welsh Water source.
- Extend tests, company mappings, and documentation accordingly.

Unless there is a clear requirement to ingest Welsh Water now, Option A is the lower-risk path. The external evidence currently points to a split public-data landscape: Water UK’s National Storm Overflows Hub is framed as England-wide, while Welsh Water maintains its own separate storm overflow map.

### Workstream 2b: Add a Coverage Assertion, Not Just a Name Mapping

Regardless of the chosen scope, add one small defensive check in the 2024+ processing/combine path so a missing company does not pass silently.

This can be as simple as:

- declaring the expected company set in one place
- comparing it with the directories/parquet files actually present
- warning or failing explicitly when expected companies are missing

### Workstream 3: Align Documentation and Add a Small Contract Check

After the coverage decision:

- update `ReadMe.md` so the raw snapshot output path matches the actual pipeline
- update `ReadMe.md` and `book/data_clean_documentation/02_ingestion.qmd` so the coverage claim matches the implemented contract
- fix the stale/garbled prose in `book/data_clean_documentation/02_ingestion.qmd`
- add one light-weight validation script or assertion that checks documented/declared company coverage against the current config used by the 2024+ pipeline
- either retire or refresh `scripts/R/testing/test_api.Rmd` so it does not preserve the pre-fix fetch implementation as a misleading test artifact

This should be intentionally small. The goal is to catch drift, not to create a large metadata framework.

## Technical Considerations

- The fetch/config pair is now using the service-root ArcGIS convention. Do not reintroduce mixed URL contracts downstream.
- The downstream scripts currently assume the project environment can be mutated at run time. That should be removed for consistency and reproducibility.
- The company list in `combine_api_edm_data_2024_onwards.R` is being used for name normalization, not fetch orchestration. If Welsh Water remains out of scope for live 2024+ ingestion, this list should not imply that Welsh raw API files will exist.
- The current pipeline has no completeness assertion for expected companies. That makes company omissions look like ordinary partial data rather than contract failures.
- Stale raw folders or parquet outputs for out-of-scope companies can currently be picked up from disk. The plan should define whether these are skipped with a warning or treated as hard errors.
- The combine step should not rely on ambient package attachment for `year()`. Make the dependency explicit or namespace the call.
- If Welsh Water ingestion is later added, treat it as a deliberate feature with its own source-contract check, rather than silently widening the existing England-focused assumptions.

## System-Wide Impact

- **Interaction graph**: `fetch_edm_api_data_2024_onwards.R` writes raw `.json.gz` snapshots to `data/raw/edm_data/raw_api_responses`; `process_edm_api_json_to_parquet_2024_onwards.R` reads those raw snapshots and writes company parquet files to `data/processed/edm_api_data`; `combine_api_edm_data_2024_onwards.R` reads those parquet files and creates the consolidated 2024+ API parquet. Any coverage or path drift at the fetch layer propagates into processing, combine, and later EDM merges.
- **Error propagation**: runtime package installation failures currently appear as environment/setup behavior inside processing and combine rather than as clear operator errors. That makes failures less reproducible.
- **State lifecycle risks**: unclear company coverage can produce silent omissions rather than hard failures. The user may see a combined dataset that looks complete but is only complete for nine companies.
- **API surface parity**: the same company/path contract is described in the fetch config, downstream combine logic, README, and book documentation. They should all say the same thing.
- **Integration test scenarios**: at minimum, test one config-normalization path, one body-validation failure path, one synthetic `json.gz` to per-company parquet path, one expected-company coverage assertion, one stale out-of-scope artifact scenario, and one doc/contract check that would fail if coverage or raw snapshot paths drift again.

## Acceptance Criteria

- [x] `process_edm_api_json_to_parquet_2024_onwards.R` does not call `install.packages()` or `renv` during normal execution.
- [x] `combine_api_edm_data_2024_onwards.R` does not call `install.packages()` or `renv` during normal execution.
- [x] `combine_2021-2023_and_api_edm_data.R` does not call `install.packages()` or `renv` during normal execution.
- [x] The 2024+ live API company-coverage contract is explicit: England-only nine companies.
- [x] Downstream company normalization logic matches the chosen coverage contract.
- [x] The 2024+ processing/combine path warns explicitly when expected companies are missing.
- [x] The 2024+ processing/combine path has explicit behavior for stale out-of-scope artifacts such as `welsh_water/` raw folders or parquet files.
- [x] The combine step declares all functions it depends on, including `lubridate::year()`.
- [x] `ReadMe.md` no longer states the wrong raw snapshot path for the fetch step.
- [x] `ReadMe.md` and `book/data_clean_documentation/02_ingestion.qmd` no longer over-claim company coverage relative to the implemented pipeline.
- [x] A small regression or validation check exists so future edits cannot silently reintroduce these contract mismatches.
- [x] The stale helper notebook/test asset that still contains the old fetch logic is updated to wrap the active regression script.
- [x] The downstream combine test asset reflects the current company-scope contract instead of the older ten-company assumption.

## Success Metrics

- A fresh reader can determine from code and docs exactly which companies the 2024+ API pipeline covers.
- Running the 2024+ API pipeline no longer mutates the R environment at any stage.
- The pipeline documentation and actual directory flow agree on where raw snapshots live and where processed parquet outputs live.
- A missing company in the 2024+ live API path becomes a visible contract violation rather than a silent omission.
- A clean-session combine run does not depend on implicitly attached packages.

## Dependencies & Risks

- The main unresolved dependency is the scope decision on Welsh Water.
- If Welsh Water should be included, the correct source and its schema/availability need to be confirmed before code changes are made.
- If Welsh Water should not be included, docs and downstream naming logic need to say that plainly rather than implying future support.
- There is a small risk of over-engineering this into a general metadata layer. Avoid that. A simple declared contract and one small check are enough.

## Sources & References

### Internal References

- `scripts/R/01_data_ingestion/fetch_edm_api_data_2024_onwards.R`
- `scripts/config/api_config.R`
- `scripts/R/02_data_cleaning/process_edm_api_json_to_parquet_2024_onwards.R:15-40`
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R:15-39`
- `scripts/R/02_data_cleaning/combine_api_edm_data_2024_onwards.R:66-76`
- `scripts/R/05_data_integration/combine_2021-2023_and_api_edm_data.R:17`
- `scripts/R/testing/test_api.Rmd`
- `scripts/R/testing/test_06_combine_api_edm_data_2024_onwards.Rmd`
- `ReadMe.md:70`
- `ReadMe.md:144-146`
- `book/data_clean_documentation/02_ingestion.qmd`
- `docs/solutions/best-practices/output-compatible-edm-standardisation-refactor-20260309.md`

### External References

- Water UK, “Water industry launches world-first interactive storm overflows map” (22 November 2024): https://www.water.org.uk/news-views-publications/news/water-industry-launches-world-first-interactive-storm-overflows-map
- Dŵr Cymru Welsh Water, “Storm overflow map”: https://corporate.dwrcymru.com/en/community/environment/storm-overflow-map
- Esri, “Uniting the UK Water Sector Through Open Data” (coverage phrased as nine wastewater companies in England): https://www.esri.com/en-us/industries/blog/articles/uniting-the-uk-water-sector-through-open-data

## Recommended Next Step

Treat this as one small refactor plan with two execution chunks:

1. Apply the no-runtime-install cleanup to the downstream processing/combine scripts.
2. Resolve the company-coverage contract and then update the docs plus one small validation check in the same PR.

If you want to minimize scope further, split the Welsh Water / documentation contract work into a second PR after the downstream dependency cleanup.
