# Residual review findings: jo/long-run-repeat-sales

Source: ce-code-review run `20260919-220338-359d522d` (19 September 2026) over
`docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd`.
Fourteen findings were raised; twelve were applied and verified by a full
re-estimation render. The two below were accepted and left in place.

## Not applied

- **P2, performance — history and mapping parquet files are read twice (content hash, then load).**
  `capture_input_provenance()` hashes each input's bytes and `load_history()`/`load_mapping()`
  then read the columns. The reviewer suggested caching the digest by path, mtime and size.
  Not applied on purpose: KTD8 and acceptance example AE4 require content identity, and the
  fixture `fingerprint_input_content_change_with_same_size_and_mtime` proves a same-size,
  same-mtime byte change is detected. The extra read costs a few seconds per render.

- **P3, cosmetic — stale-bundle message prints `pdftools NA -> 3.8.0` for a package absent in the old bundle.**
  `fingerprint_differences()` indexes the old bundle's package vector by name, so a name that is
  missing yields `NA` rather than the recorded `"absent"` label. The comparison itself is correct
  and the render is refused as required. Left in place because any further edit to the
  fingerprinted `helpers-bundle` chunk invalidates the just-built bundle and needs another
  16-minute re-estimation; fix it together with the next computational change.
