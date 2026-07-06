# CH2 merge baseline capture report

Date: 2026-07-05

Branch: `jo/merge-ch2-baseline`

## Archived pre-baseline outputs

The pre-existing matched-output directory was archived before the old merge
script was run:

`data/processed/matched_events_annual_data_archive_2026-07-05_ch2_pre_baseline`

Archived files:

- `matched_events_annual_data.parquet`
- `events_unmatched.parquet`
- `annual_unmatched.parquet`
- `site_metadata.parquet`

## Current-input baseline outputs

The old `scripts/R/05_data_integration/merge_individ_annual_location.R` script
was run unchanged on the regenerated CH1 lookup and current event/annual inputs.
The resulting baseline outputs were copied to:

`data/processed/matched_events_annual_data_baseline_2026-07-05_ch2_current_inputs`

U7 should read the baseline outputs and records from that directory. It contains:

- `matched_events_annual_data.parquet`
- `events_unmatched.parquet`
- `annual_unmatched.parquet`
- `site_metadata.parquet`
- `ch2_baseline_row_counts.csv`
- `ch2_baseline_match_methods.csv`
- `ch2_input_fingerprints.csv`
- `ch2_baseline_manifest.md`

## Baseline row counts

| Artifact | Rows |
|---|---:|
| `matched_events_annual_data.parquet` | 7,117,911 |
| `events_unmatched.parquet` | 273,693 |
| `annual_unmatched.parquet` | 1,599 |
| `site_metadata.parquet` | 51,346 |

## Match-method distribution

| Match method | Rows |
|---|---:|
| `windfall` | 6,565,204 |
| `max` | 478,531 |
| `fuzzy` | 74,176 |

The matched-row total is well above the stale January output size and is the
like-for-like baseline for U7.

## Input fingerprints

| Artifact | Size bytes | Mtime | SHA-256 |
|---|---:|---|---|
| `data/processed/combined_edm_data.parquet` | 206,261,449 | 2026-06-09 14:03:12 BST | `0912fbd02c0938018eac18786aa69a30c3045967387d15f6bb18eab5ba99bb30` |
| `data/processed/annual_return_edm.parquet` | 3,573,569 | 2026-06-15 13:09:58 BST | `fec810ab2a85ffa06eb9a3684c820d8a3bf706894770b340eeef4ff1a9f31c22` |
| `data/processed/annual_return_lookup.parquet` | 504,264 | 2026-07-05 01:17:34 BST | `9dc421536cc304d56376599c0253e58ee78e669536703e6ce33cf2dbdf1a67fe` |

## Commands run

```bash
Rscript scripts/R/05_data_integration/merge_individ_annual_location.R
```

The July 5 log segment in `output/log/10_merge_individ_annual_location.log`
ends with `Merge process completed successfully`; no July 5 `WARN` or `ERROR`
lines were present in the log.
