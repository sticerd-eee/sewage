# CH1 annual-return lookup ambiguity diagnostic report

Date: 2026-07-05

Branch: `jo/merge-ch1-lookup-regen`

## Inputs

- Archived stale lookup:
  `data/processed/archive/2026-07-05-merge-ch1-lookup-regen/annual_return_lookup.parquet`
- Regenerated lookup:
  `data/processed/annual_return_lookup.parquet`
- Annual returns:
  `data/processed/annual_return_edm.parquet`
- Event records:
  `data/processed/combined_edm_data.parquet`

The stale lookup was archived before regeneration. The archive and regenerated
lookup have the same SHA-256:

`9dc421536cc304d56376599c0253e58ee78e669536703e6ce33cf2dbdf1a67fe`

The regenerated lookup has mtime `2026-07-05 01:17:34`; the archived stale lookup
preserves mtime `2026-06-10 17:00:02`.

## Diagnostic definition

The diagnostic re-derives the origin evidence at annual-return name grain:
`water_company`, `year`, and byte-identical `site_name_ea`. A group is ambiguous
when it contains more than one annual-return row. The supplied lookup is used as
a coverage gate: every annual-return row must resolve through the lookup before
the diagnostic is computed.

The top-two spill-hours ratio excludes groups whose runner-up `spill_hrs_ea` is
zero or missing; this reproduces the origin's 61.6 percent calibration and median
ratio of 4.6.

## Results

| Metric | Archived stale lookup | Regenerated lookup | Delta |
|---|---:|---:|---:|
| Lookup links | 57,865 | 57,865 | 0 |
| Canonical site IDs | 15,430 | 15,430 | 0 |
| Ambiguous groups | 3,116 | 3,116 | 0 |
| Ambiguous annual rows | 7,872 | 7,872 | 0 |
| Identical NGR share | 64.6% | 64.6% | 0.0 pp |
| Identical `permit_reference_ea` share | 71.7% | 71.7% | 0.0 pp |
| `activity_reference` differs share | 58.7% | 58.7% | 0.0 pp |
| Groups with positive runner-up hours | 2,286 | 2,286 | 0 |
| Top-two hours ratio >= 3x | 61.6% | 61.6% | 0.0 pp |
| Median top-two hours ratio | 4.60 | 4.60 | 0.00 |
| Event rows in ambiguous annual names | 1,063,854 | 1,063,854 | 0 |
| Share of all event rows | 14.4% | 14.4% | 0.0 pp |
| Share of named event rows | 15.7% | 15.7% | 0.0 pp |

## Commands run

```bash
rv sync
Rscript scripts/R/testing/diagnose_annual_return_lookup_ambiguity.R --lookup data/processed/archive/2026-07-05-merge-ch1-lookup-regen/annual_return_lookup.parquet --label archived_stale_lookup --check-origin
Rscript scripts/R/03_data_enrichment/create_annual_return_lookup.R
Rscript scripts/R/testing/test_annual_return_lookup_contracts.R
Rscript scripts/R/testing/diagnose_annual_return_lookup_ambiguity.R --lookup data/processed/annual_return_lookup.parquet --label regenerated_lookup --check-origin
```

## Assessment

The archived stale lookup reproduces the origin figures required by CH1:
3,116 ambiguous groups, 64.6 percent identical NGR, 71.7 percent identical
`permit_reference_ea`, and 61.6 percent top-two hours ratio >= 3x.

The regenerated lookup has zero diagnostic delta from the archived stale lookup.
The D3/D5 parameters therefore still hold, and the CH1 stop condition is not
triggered.
