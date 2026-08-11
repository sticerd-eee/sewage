# Code review: 10km site match scripts (house sales & rentals)

- **Date:** 2026-07-06
- **Scripts reviewed:** `scripts/R/04_feature_engineering/10km_site_house_sale_match.R`, `scripts/R/04_feature_engineering/10km_site_rental_match.R`
- **Review type:** whole-file sanity check (multi-agent review: correctness, performance, maintainability, project-standards, testing, institutional-learnings; every finding below survived an independent validation pass)
- **Status:** partially resolved — the 2026-08-11 grain migration fixed the Site Group input and count contracts; radius, dependency, logging, memory, and performance findings remain as recorded below.

---

## Summary

One critical error: **the rental match was actually run at 5 km, not the 10 km stated in the file name, header, comments, and log messages.** This is confirmed in the shipped data: `spill_rental_lookup.parquet` has a maximum `distance_m` of exactly 5,000, while `spill_house_lookup.parquet` reaches 10,000. Six findings total; the rest are moderate. The core spatial-join logic is sound — several suspected failure modes were traced and ruled out (see "Verified non-issues").

---

## Findings

### 1. [P0] Rental match runs at 5 km, not the documented 10 km

- **Where:** `10km_site_rental_match.R:52` (`CONFIG$radius_km = 5`)
- **What happens:** `main()` passes `CONFIG$radius_km` straight through to the spatial join, so every rental was matched to spill sites within 5 km only. The file name, the header comment, the roxygen docs (`@param radius_km ... default 10`), and the log line `"10km radius match"` all claim 10 km.
- **Evidence:** empirically confirmed twice (reviewer and independent validator): `max(distance_m)` = 5,000 in `data/processed/zoopla/spill_rental_lookup.parquet` vs 10,000 in `data/processed/spill_house_lookup.parquet`. No documentation anywhere states 5 km is intentional for rentals.
- **Research impact:** rentals and sales lookups are built at different radii. Current downstream scripts re-filter to ≤ 250–1,000 m, so published estimates at those radii are unaffected, but any rental analysis beyond 5 km would silently lose matches, and sales/rentals comparisons of match counts or exposure at wider radii are not like-for-like.
- **Suggested fix:** decide the intended rental radius. If 10 km: set `CONFIG$radius_km = 10`, re-run the script, regenerate `spill_rental_lookup.parquet`, and re-run downstream rental datasets. If 5 km is intentional: rename the file, and correct the header, docs, and log messages, and document the sales/rentals asymmetry. Either way, add a log line stating the numeric radius actually used.

### 2. [P1] House script: function default is 5 km while its own docstring says 10 km

- **Where:** `10km_site_house_sale_match.R:173` (`process_spatial_data <- function(data, radius_km = 5)`)
- **What happens:** the roxygen comment one line above says "default: 10". The shipped output is correct only because `main()` (line 221) explicitly passes `radius_km = 10`. Any future caller relying on the documented default gets a 5 km match. This latent trap is very likely how the rental script (adapted from this one in September) ended up running at 5 km.
- **Suggested fix:** change the default to 10 (or remove the default entirely), and preferably adopt the rental script's pattern of a single `CONFIG$radius_km` so the radius lives in exactly one place.

### 3. [P2] Unused package `rnrfa` loaded

- **Where:** `10km_site_house_sale_match.R:19`
- **What happens:** `rnrfa` (a hydrology/NGR package) is in `required_packages` but nothing in the script uses it — the NGR parsing that needs it happens upstream. If missing, the script would even install it via the fallback in finding 4.
- **Suggested fix:** remove `"rnrfa"` from `required_packages`.

### 4. [P2] `install.packages()` fallback bypasses rv package management (repo-wide pattern)

- **Where:** `10km_site_house_sale_match.R:24-26` and `10km_site_rental_match.R:24-26`
- **What happens:** AGENTS.md states "R package management uses `rv`", but `initialise_environment()` silently installs an unpinned CRAN version of any missing package, undermining reproducibility. The validator confirmed this identical boilerplate exists in dozens of scripts across the pipeline, so this is a repo-wide convention to fix globally, not a defect unique to these two scripts.
- **Suggested fix:** decide once for the pipeline — drop the `install.packages()` fallback and fail loudly with a "run `rv sync`" message. Track as a pipeline-wide cleanup rather than patching only these two files.

### 5. [P2] Properties with missing coordinates are dropped silently

- **Where:** `10km_site_house_sale_match.R:119` and `10km_site_rental_match.R:118`
- **What happens:** houses/rentals with NA easting or northing (postcodes that failed the ONS lookup upstream) are filtered out with no logging, count, or CSV export — in contrast to spill sites, which get a `log_warn` and a dropped-sites CSV a few lines above in the same scripts. Dropped properties are indistinguishable downstream from properties with zero spill-site matches.
- **Suggested fix:** mirror the spill-site pattern: count the rows failing the coordinate filter and `log_warn` the count (a CSV export is optional given the volume).

### 6. [P2] No contract/verification script for either output

- **Where:** gap in `scripts/R/testing/` (convention established by e.g. `test_create_unique_spill_sites_ch8.R`, `test_merge_matching_contracts.R`)
- **What happens:** nothing checks the two lookup parquets. A one-line assertion that `max(distance_m) <= radius_km * 1000` with the intended radius would have caught finding 1 at build time. `test_house_price_sewage_merge.Rmd` is an exploratory notebook with zero assertions, not a test.
- **Suggested fix:** add `scripts/R/testing/test_10km_site_match_contracts.R` asserting, for both parquets: all `distance_m` ≤ intended radius; no duplicate (`house_id`/`rental_id`, `site_id`) pairs; `n_site_groups` ≥ 0 and equal to the per-property count of non-NA `site_id`; and the two scripts' effective radii match each other (or match their documented values).
- **Grain-contract resolution (2026-08-11):** both producers now read the unique Site Group projection from `site_group_crosswalk.parquet`, preserve the left-side row count through metadata attachment, and write `n_site_groups`. `test_site_group_consumer_contracts.R` covers uniqueness, no fanout, and the clean count-name migration. The intended-radius contract remains open and is not claimed by that test.

### 7. [P2] Peak memory roughly doubles at the 208-million-row output scale

- **Where:** `10km_site_house_sale_match.R:143-166` and `10km_site_rental_match.R:144-168`
- **What happens:** `purrr::map` accumulates all chunk results in a list, then `bind_rows` materialises a second full copy before `write_parquet` writes a single 207,903,117-row (3.7 GB) table. The run completes on the current machine, so this is a robustness/headroom concern, not a failure.
- **Suggested fix (optional):** write each chunk to disk as it completes (`arrow::write_dataset` with per-chunk files, or a `ParquetFileWriter`), instead of accumulating and binding.

### 8. [P3, optional] The spatial join uses no spatial index at all on projected coordinates — a ~16x speedup is available

- **Where:** `10km_site_house_sale_match.R:146-150` and `10km_site_rental_match.R:148-152` (the `st_join(..., join = st_is_within_distance, dist = radius_m)` call)
- **What happens:** source-verified in the sf package: on a projected CRS such as British National Grid (EPSG:27700), `st_is_within_distance` takes a brute-force code path in GEOS — a plain nested loop computing the distance between every property in the chunk and every spill site, with no spatial index (unlike `st_intersects` and most other predicates, which build an STRtree over the second argument). Benchmarks at these data sizes (2,000-row chunks against ~15k sites) measured roughly 400 milliseconds per chunk for the current predicate versus roughly 25 milliseconds for the indexed alternative.
- **Correctness is unaffected** — results are identical; this is purely runtime.
- **Suggested fix (only if re-running becomes frequent):** replace the predicate with the standard equivalent trick — buffer one side by the radius and join with `st_intersects` (e.g. `st_join(chunk, st_buffer(spill_sites_sf, radius_m), join = st_intersects)`), which engages the STRtree index built over the spill sites. Keep the existing `st_distance` step for the actual distances. The current orientation (chunking the large property table, keeping the small site table whole as the second argument) is already the right one.

---

## Verified non-issues (checked and ruled out)

- **Chunking cannot corrupt the per-property outlet count.** `house_id`/`rental_id` are `row_number()` identifiers, so a property can never span two chunks; the per-chunk `group_by` count is globally correct.
- **The lookup join cannot duplicate rows.** The shared Site Group projection is asserted unique on `site_id` before either spatial join, and both producers assert row conservation after metadata attachment. The repeated Site Group key in `unique_spill_sites.parquet` is deliberately not used for this task.
- **NA join keys are harmless here.** Unmatched properties get NA `site_id` from the left spatial join, and the Site Group projection has no NA keys, so no spurious matches arise; `n_site_groups` correctly counts only non-NA `site_id`.
- **Chunk reordering by `split()`** only permutes output row order, which downstream ID-based joins ignore.
- **`if_else()` evaluating `st_distance()` eagerly on unmatched rows** was examined and judged low-risk (the failure mode would be a loud chunk error, not silent corruption); noted as a residual risk only.
- **The count rename was clean:** active analysis consumers use `n_site_groups`; the legacy count name is retained only in negative contract assertions.

## Residual risks / adjacent observations (outside the two scripts)

- `rental_panel_within_radius.R:97-99` reads `zoopla_rentals.parquet` from `data/processed/` rather than `data/processed/zoopla/` where the cleaning script writes it — worth a separate check.
- `docs/pipeline_documentation.md` describes both matching scripts without stating the radius each actually uses.
- The house script hardcodes `chunk_size <- 2000` locally while the rental script exposes it via `CONFIG$chunk_size`; the house script also builds its output path with `file.path(CONFIG$processed_dir, ...)` while the rental script defines a full `CONFIG$output_path` — harmless, but the two scripts will keep drifting apart without consolidation.
- `st_distance` behaviour on empty geometries was reasoned from sf semantics, not executed against the installed sf/GEOS versions.

## Suggested order of work

1. Resolve finding 1 (decide the rental radius, regenerate the rental lookup if 10 km was intended, then re-run downstream rental datasets).
2. Fix finding 2 at the same time (one-line default change, same root cause).
3. Add the contract test from finding 6 so radius drift can never ship silently again.
4. Findings 3, 5, 7 are quick, independent quality improvements; finding 4 belongs in a pipeline-wide cleanup task.
