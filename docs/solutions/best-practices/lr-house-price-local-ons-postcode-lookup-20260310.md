---
module: Land Registry House Price Pipeline
date: 2026-03-10
problem_type: best_practice
component: tooling
symptoms:
  - "The Land Registry cleaner still treated postcode enrichment as an API-style geocoding problem instead of a deterministic local lookup problem."
  - "The first local ONS implementation used postcode introduction and termination dates, which cut LR completeness sharply despite valid-looking postcodes."
  - "With date filtering enabled, the candidate build missed 2,155 distinct postcodes and rejected 9,716 postcode-month combinations."
  - "The repository needed a faster, reproducible replacement for `postcodes.io` that could preserve the downstream `house_price.parquet` geography contract."
root_cause: logic_error
resolution_type: code_fix
severity: medium
tags: [r, land-registry, house-price, postcode-lookup, ons-postcode-directory, postcodesio, data-completeness]
---

# Troubleshooting: Replacing LR house-price postcode enrichment with a fully local ONS lookup without date filtering

## Problem
The Land Registry house-price cleaner had been built around the shared `PostcodesioR` and `postcodes.io` workflow, and the first attempt to replace it with a local ONS lookup copied over a stricter idea of postcode validity than the LR pipeline actually needed. For HM Land Registry Price Paid data, the practical requirement was a unique postcode match with stable downstream geography fields, not a postcode-month validity window.

## Environment
- Module: Land Registry house-price cleaning pipeline
- Affected component: local postcode enrichment and canonical `house_price.parquet` generation
- Key files:
  - `scripts/R/02_data_cleaning/clean_lr_house_price_data.R`
  - `scripts/R/utils/postcode_processing_utils.R`
  - `data/raw/uk_postcodes/2602_uk_postcodes.csv`
  - `data/raw/uk_postcodes/Documents/`
- Date solved: 2026-03-10

## Symptoms
- The original repository documentation still described `clean_lr_house_price_data.R` as geocoding through `postcodes.io` with shared cache and retry logic.
- A local ONS candidate built with sale-month validity filtering missed `2,155` distinct postcodes and left `3,660` LR rows unmatched.
- The same date-filtered candidate rejected `9,716` postcode-month combinations, covering `22,762` rows, even though many of the postcodes were otherwise structurally valid.
- The user explicitly cared about postcode uniqueness and completeness more than introduction-date semantics because HM Land Registry Price Paid data can reflect older postcode usage.

## What Didn't Work
**Rejected approach 1:** Keep the current `postcodes.io` path as the LR default because it already produced the canonical parquet.  
- **Why it failed:** it kept LR postcode enrichment dependent on an API-oriented workflow when a local ONS file was available, newer, faster, and more reproducible.

**Rejected approach 2:** Use the local ONS file but require `intro_ym <= sale_ym <= term_ym` for each sale.  
- **Why it failed:** it solved the wrong problem. The validity filter removed many usable postcode matches and produced a large completeness regression for LR without serving the user’s actual matching rule.

**Rejected approach 3:** Keep the ONS rollout permanently as a sidecar candidate file plus comparison log.  
- **Why it failed:** that was useful for evaluation, but once the preferred lookup contract was clear it left the LR cleaner in an in-between state instead of restoring one canonical output path.

**Rejected approach 4:** Keep `postcode_data.rds` as a compatibility layer for label-style fields during the ONS rollout.  
- **Why it failed:** the legacy RDS covered fewer postcodes than the current sales inputs, so some sales matched the ONS postcode file but still lost `lsoa`/`msoa`/`region` and were dropped downstream.

## Solution
The working fix was to treat LR postcode enrichment as a local postcode-keyed join and to promote the newer ONS snapshot directly into the canonical LR cleaner.

- Switched the LR lookup source from `data/raw/uk_postcodes/2511_uk_postcodes.csv` to `data/raw/uk_postcodes/2602_uk_postcodes.csv`.
- Reworked the local ONS loader to read the abbreviated ONSPD columns directly:
  - `pcds` / `pcd8` for postcode
  - `gridind` for positional quality
  - `east1m`, `north1m`, `lat`, `long` for coordinates
- Extended the local ONS loader to keep the ONS geography code columns needed to rebuild label-style fields:
  - `ctry25cd`, `hlth19cd`, `rgn25cd`
  - `lsoa11cd`, `msoa11cd`
  - `cty25cd`, `wd25cd`
- Normalised postcodes to uppercase without spaces and added an explicit uniqueness guard that stops if the ONS file contains duplicate normalised postcodes.
- Removed all LR-specific postcode date-validity logic:
  - no `sale_ym`
  - no `Date of Introduction` / `Date of Termination` filtering
  - no postcode-month join key
- Joined LR sales to postcode enrichment on `postcode` only.
- Rebuilt label-style geography fields from the ONS `Documents/` code-name tables instead of the legacy RDS:
  - `CTRY25CD -> CTRY25NM`
  - `HLTHAUCD -> HLTHAUNM`
  - `RGN25CD -> RGN25NM`
  - `LSOA11CD -> LSOA11NM`
  - `MSOA11CD -> MSOA11NM`
  - `CTY25CD -> CTY25NM` with pseudo county labels dropped back to `NA`
  - `WD25CD -> WD25NM`
- Removed `postcode_label_lookup_path` / `compatibility_path` from the LR cleaner.
- Restored the default LR output to the canonical `data/processed/house_price.parquet` path and removed the candidate/comparison branch from the normal run.
- Kept `main(refresh_postcodes = FALSE)` callable, but LR now logs that refresh is ignored because the cleaner uses local files.

**Code changes**:

```r
# ONS loader keyed by postcode only
lookup_data <- data.table::fread(
  lookup_path,
  select = c(
    "pcds", "pcd8", "gridind", "east1m", "north1m", "lat", "long",
    "ctry25cd", "hlth19cd", "rgn25cd", "lsoa11cd", "msoa11cd",
    "cty25cd", "wd25cd"
  ),
  na.strings = c("", "NA"),
  showProgress = interactive()
) %>%
  tibble::as_tibble() %>%
  dplyr::transmute(
    postcode = normalise_postcode(dplyr::coalesce(.data[["pcds"]], .data[["pcd8"]])),
    quality = suppressWarnings(as.integer(.data[["gridind"]])),
    easting = suppressWarnings(as.integer(.data[["east1m"]])),
    northing = suppressWarnings(as.integer(.data[["north1m"]])),
    longitude = suppressWarnings(as.numeric(.data[["long"]])),
    latitude = suppressWarnings(as.numeric(.data[["lat"]])),
    ctry25cd = dplyr::na_if(.data[["ctry25cd"]], ""),
    hlth19cd = dplyr::na_if(.data[["hlth19cd"]], ""),
    rgn25cd = dplyr::na_if(.data[["rgn25cd"]], ""),
    lsoa11cd = dplyr::na_if(.data[["lsoa11cd"]], ""),
    msoa11cd = dplyr::na_if(.data[["msoa11cd"]], ""),
    cty25cd = dplyr::na_if(.data[["cty25cd"]], ""),
    wd25cd = dplyr::na_if(.data[["wd25cd"]], "")
  )
```

```r
# LR enrichment joined by postcode only
label_lookup <- build_local_postcode_label_lookup(
  ons_lookup = ons_lookup,
  lookup_path = lookup_path
)

postcode_result <- get_local_postcode_data_for_sales(
  raw_data,
  lookup_path = CONFIG$local_postcode_lookup_path
)

final_data <- left_join(
  raw_data,
  postcode_result$postcode_data,
  by = "postcode"
) %>%
  mutate(house_id = row_number()) %>%
  relocate(house_id, .before = transaction_id)
```

**Verification commands**:

```bash
Rscript --vanilla -e 'source("scripts/R/utils/postcode_processing_utils.R"); source("scripts/R/02_data_cleaning/clean_lr_house_price_data.R")'
Rscript --vanilla -e 'source("scripts/R/utils/postcode_processing_utils.R"); x <- load_local_postcode_lookup("data/raw/uk_postcodes/2602_uk_postcodes.csv"); cat("rows=", nrow(x), " unique=", dplyr::n_distinct(x$postcode), "\n", sep="")'
Rscript --vanilla scripts/R/02_data_cleaning/clean_lr_house_price_data.R
```

## Why This Works
The earlier failure came from enforcing a temporal postcode-validity rule that the LR pipeline did not actually need.

1. The user’s true contract was uniqueness, not sale-month validity.
2. The local ONS file is deterministic, faster, and based on a newer authoritative postcode directory snapshot than the old API-based path.
3. Matching on postcode only recovers almost all usable LR postcodes while avoiding the artificial losses introduced by introduction-date checks.
4. Rebuilding label-style fields from the ONS code-name tables preserves the existing `house_price.parquet` geography interface without mixing in an older, incomplete label source.
5. The uniqueness guard prevents silent ambiguity if a future postcode snapshot stops being one-row-per-postcode after normalisation.

Coordinate revisions are acceptable in this workflow. The change intentionally prefers the newer ONS source over parity with older `postcodes.io`-derived coordinates.

## Prevention
- Treat the LR postcode join contract explicitly as part of the pipeline design. If the requirement is unique postcode lookup, do not add temporal validity rules without a demonstrated downstream need.
- Prefer local ONS postcode snapshots over API calls for bulk LR enrichment when reproducibility and speed matter.
- When onboarding a new ONS snapshot, check three things before promotion:
  - normalised postcode uniqueness
  - distinct-postcode coverage against LR sales
  - non-missing counts for core spatial fields
- If downstream still needs label-style geography fields, derive them from the same ONS bundle via code-name tables rather than mixing newer postcode rows with older label sources.
- When a new local lookup reduces completeness materially, investigate the matching rule before assuming the newer source is worse.
- Log postcode coverage and ONS misses on every LR run so completeness regressions are visible immediately.
- Do not reintroduce candidate-only output or comparison-log logic to the default LR path unless you are running a deliberate source-comparison exercise.
- Update user-facing documentation such as `ReadMe.md` when the canonical enrichment contract changes, so the written pipeline description does not lag behind the code.

## Related Issues
- See also: [data-cleaning-script-header-bootstrap-standardisation-20260310.md](./data-cleaning-script-header-bootstrap-standardisation-20260310.md)
- See also: [script-setup-runtime-package-cleanup-ingestion-20260310.md](./script-setup-runtime-package-cleanup-ingestion-20260310.md)

Relevant repository references:
- LR cleaner: [`clean_lr_house_price_data.R`](../../../scripts/R/02_data_cleaning/clean_lr_house_price_data.R)
- Shared postcode utilities: [`postcode_processing_utils.R`](../../../scripts/R/utils/postcode_processing_utils.R)
- Investigation log: [`house_price_local_postcode_comparison.log`](../../../output/log/house_price_local_postcode_comparison.log)
- Verification log: [`clean_lr_house_price_data.log`](../../../output/log/clean_lr_house_price_data.log)
- Current repository description of LR cleaning: [ReadMe.md](../../../ReadMe.md)
- Nearby manuscript doc still describing the old API path: [`103_appendix_data.tex`](../../../docs/overleaf/103_appendix_data.tex)

## Verification Note
This solution was verified on 2026-03-10 with syntax and file-level checks.

- The ONS `2602` loader was confirmed unique after postcode normalisation.
- The edited R files passed a syntax parse after the final ONS-only changes.
- A file-level verification over the 2021-2023 LR sales inputs and ONS bundle found `943,291 / 943,347` distinct postcodes matched in ONS, with only `56` distinct postcodes missing covering `84` rows.
- Among ONS-matched postcodes, `lsoa` and `msoa` were fully populated from ONS code-name tables; `region` remained missing only for the same non-English cases that do not carry English region codes.
- On the `941,024` postcodes overlapping with the old RDS, `country` matched perfectly, `region` differed on `1`, `lsoa` on `447`, and `msoa` on `169`, reflecting updated ONS assignments rather than missing label rows.
- `admin_county` pseudo labels were normalised back to `NA`; `admin_ward` can still differ where the newer ONS ward lookup is more complete than the old API-derived output.
