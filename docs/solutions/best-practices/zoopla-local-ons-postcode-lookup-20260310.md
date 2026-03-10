---
module: Zoopla Rental Pipeline
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "The Zoopla cleaner still relied on the shared Postcodes.io/cache workflow even after the LR pipeline had moved to a local ONS postcode lookup."
  - "A first validation pass showed the main ONS postcode file was more complete than the current Zoopla output, but the local helper failed before completion on ONS code-name tables."
  - "The ONS code-name CSVs for some geographies are ragged, so `data.table::fread()` fell back to `V1`, `V2`, ... headers and the expected code/name columns were not found."
  - "The cleaner still pointed at a stale singular output filename while downstream code already consumed `zoopla_rentals.parquet`."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, zoopla, rentals, postcode-lookup, ons-postcode-directory, data-completeness]
---

# Troubleshooting: Migrating the Zoopla rental cleaner to the local ONS postcode lookup after proving completeness

## Problem
The Zoopla rental cleaner was still on the old API/cache postcode path after the LR cleaner had already moved to the local ONS postcode directory. Before removing the legacy path, the change needed a proof step: the local ONS workflow had to beat the current Zoopla output on completeness, and it had to run end to end without breaking on the ONS code-name lookup files.

## Environment
- Module: Zoopla rental cleaning pipeline
- Affected files:
  - `scripts/R/02_data_cleaning/clean_zoopla_data.R`
  - `scripts/R/utils/postcode_processing_utils.R`
  - `ReadMe.md`
- Key inputs:
  - `data/raw/zoopla/rentals_safeguarded_2014-2022.csv`
  - `data/raw/zoopla/rentals_safeguarded_2023.csv`
  - `data/raw/uk_postcodes/2602_uk_postcodes.csv`
  - `data/raw/uk_postcodes/Documents/`
- Canonical output:
  - `data/processed/zoopla/zoopla_rentals.parquet`
- Date solved: 2026-03-10

## Symptoms
- The current processed rentals parquet had `444` rows with missing postcode-derived geography fields, covering `317` distinct postcodes.
- A read-only comparison against the local ONS snapshot showed the main `2602` postcode file could match all `453,454` distinct postcodes in the cleaned 2021-2023 Zoopla sample, covering all `1,450,255` rows.
- The first end-to-end local helper run failed at the label lookup step because `fread()` interpreted ragged ONS lookup CSVs like `CTRY Country names and codes UK as at 05_25.csv` as `V1...V10`, so the expected `CTRY25CD` and `CTRY25NM` columns were unavailable.
- `clean_zoopla_data.R` still referenced `PostcodesioR`, `get_postcode_data()`, `cleanup_postcode_cache()`, and the stale singular output path `zoopla_rental.parquet`.

## What Didn't Work
**Rejected approach 1:** Switch Zoopla to the local ONS helper immediately because LR had already moved.  
- **Why it failed:** the main postcode file was fine, but the label lookup step was not yet robust to the actual ONS document CSV format.

**Rejected approach 2:** Judge the migration only on postcode-keyed coordinate coverage and ignore the label fields.  
- **Why it failed:** downstream Zoopla analyses use label-style geography fields such as `lsoa` and `msoa`, so the local helper had to complete the full enrichment contract.

**Rejected approach 3:** Keep the old API/cache code around in case the local path underperformed.  
- **Why it failed:** once the comparison showed the local ONS path was strictly more complete, keeping dead API/cache code in the active cleaner only preserved confusion.

## Solution
The working fix was a gated migration: prove the ONS postcode snapshot was more complete first, then repair the ONS label-file reader, then switch the Zoopla cleaner and remove the old API/cache path.

- Verified in read-only comparison that the local `2602` ONS postcode snapshot matched:
  - `453,454 / 453,454` distinct postcodes
  - `1,450,255 / 1,450,255` cleaned Zoopla rows
- Confirmed that the existing canonical rentals parquet only had postcode-derived geography on `1,449,811` rows, leaving `444` rows unmatched for those fields.
- Reworked `load_ons_name_lookup()` to read ONS code-name CSVs with `read.csv(..., check.names = FALSE, fill = TRUE)` and then subset to the required code/name columns before tibble conversion.
- Added an explicit missing-column error in `load_ons_name_lookup()` so future ONS bundle changes fail with the actual available named columns.
- Removed the old Postcodes.io/cache helpers from `postcode_processing_utils.R`:
  - `process_postcodes()`
  - `get_postcode_data()`
  - `cleanup_postcode_cache()`
- Dropped the `PostcodesioR` dependency from the shared postcode utility and from `clean_zoopla_data.R`.
- Switched `clean_zoopla_data.R` to the same local ONS join path used by LR:
  - add `CONFIG$local_postcode_lookup_path`
  - call `get_local_postcode_data_for_sales(df, lookup_path = ...)`
  - join on normalised `postcode`
  - return postcode diagnostics invisibly
- Kept `main(refresh_postcodes = FALSE)` callable for compatibility, but it now logs that refresh is ignored because the cleaner uses local files.
- Corrected the cleaner’s canonical output path to `data/processed/zoopla/zoopla_rentals.parquet`, matching downstream scripts and the rebuilt parquet.
- Updated `ReadMe.md` so the shared postcode utility description no longer claims Zoopla still depends on the old API/cache workflow.

**Code changes**:

```r
lookup_data <- utils::read.csv(
  lookup_path,
  check.names = FALSE,
  fill = TRUE,
  na.strings = c("", "NA"),
  stringsAsFactors = FALSE
)

lookup_data[expected_cols] %>%
  tibble::as_tibble() %>%
  dplyr::transmute(
    code = dplyr::na_if(as.character(.data[[code_col]]), ""),
    !!output_col := dplyr::na_if(as.character(.data[[name_col]]), "")
  )
```

```r
postcode_result <- get_local_postcode_data_for_sales(
  df,
  lookup_path = CONFIG$local_postcode_lookup_path
)

final_data <- left_join(
  df,
  postcode_result$postcode_data,
  by = "postcode"
) %>%
  mutate(rental_id = row_number()) %>%
  relocate(rental_id, .before = 1)
```

## Why This Works
The migration only became safe once it was treated as two separate questions.

1. Is the local ONS postcode snapshot more complete than the old output?
2. Can the shared local helper actually consume the ONS bundle end to end?

The answer to the first question was yes immediately. The answer to the second became yes after the label-file reader stopped assuming that `fread()` would preserve the named headers in ragged CSVs. Once that reader was fixed, the local ONS path produced the full postcode-derived geography contract for the entire cleaned Zoopla sample, making the API/cache workflow genuinely obsolete for this pipeline.

## Prevention
- Validate a new local lookup path against the current canonical output before removing the old implementation.
- Treat ONS document CSVs as messy external inputs; do not assume `fread()` will always preserve named headers when files have trailing empty columns.
- Fail fast with the actual available named columns when an ONS code-name table changes.
- If downstream scripts already consume a canonical output path, update the cleaner config to match that contract at the same time as the logic migration.
- Remove dead fallback code once the replacement path is verified, otherwise the repository description will drift away from the real pipeline.

## Related Issues
- See also: [lr-house-price-local-ons-postcode-lookup-20260310.md](./lr-house-price-local-ons-postcode-lookup-20260310.md)
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)

Relevant repository references:
- Zoopla cleaner: [`clean_zoopla_data.R`](../../../scripts/R/02_data_cleaning/clean_zoopla_data.R)
- Shared postcode utilities: [`postcode_processing_utils.R`](../../../scripts/R/utils/postcode_processing_utils.R)
- Current canonical output: [`zoopla_rentals.parquet`](../../../data/processed/zoopla/zoopla_rentals.parquet)
- Verification log: [`clean_zoopla_data.log`](../../../output/log/clean_zoopla_data.log)
- Pipeline overview: [ReadMe.md](../../../ReadMe.md)

## Verification Note
This solution was verified on 2026-03-10 with both read-only comparison checks and a full runtime rebuild.

- A read-only comparison on the cleaned 2021-2023 Zoopla sample found:
  - `453,454 / 453,454` distinct postcodes matched in the local ONS snapshot
  - `1,450,255 / 1,450,255` rows matched for `easting`, `northing`, `latitude`, `longitude`, `lsoa`, and `msoa`
- The pre-migration canonical rentals parquet had only `1,449,811` non-missing rows for those same fields.
- A full run of `scripts/R/02_data_cleaning/clean_zoopla_data.R` completed successfully and rewrote `data/processed/zoopla/zoopla_rentals.parquet`.
- The rebuilt canonical parquet contains:
  - `1,450,255` rows
  - `453,454` distinct postcodes
  - full non-missing coverage for `easting`, `northing`, `latitude`, `longitude`, `lsoa`, and `msoa`
- Logged label-field NA counts among matched postcodes were:
  - `country=0`
  - `nhs_ha=0`
  - `region=16154`
  - `lsoa=0`
  - `msoa=0`
  - `admin_county=307495`
  - `admin_ward=0`

The remaining `region` and `admin_county` gaps reflect the underlying ONS geography coverage rather than postcode lookup misses.
