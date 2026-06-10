---
status: done
priority: p1
issue_id: "008"
tags: [code-review, r, annual-return, record-linkage, rf-matching, igraph, reclin2, data-quality]
dependencies: []
---

# Fix annual return lookup: RF matching bugs and same-year component conflicts

Code review (2026-06-10) of `scripts/R/03_data_enrichment/create_annual_return_lookup.R`.
Verified against project library (R 4.6.0, dplyr 1.2.1, reclin2 0.6.0, igraph 2.3.1, ranger 0.18.0).

The deterministic windfall path is mostly sound. The RF path is broken in three independent ways,
and one silent data-quality issue fires on the real data even with RF disabled.

---

## P1 fixes (must do before RF path is usable)

### 1. Saved RF model loaded with wrong list key — always returns NULL (line 755)

Line 523 saves `saveRDS(list(model = m_rf, importance = ...), model_file)`, but line 755 reads
`readRDS(model_file)$m_rf`. `$m_rf` does not partial-match `model` → NULL.
`file.exists()` runs first, so once the file exists the function returns NULL before reaching the
training branch even with `train_if_missing = TRUE`. Main then logs the misleading "no model available".

**Fix:**
```r
# line 755
return(readRDS(model_file)$model)   # was: $m_rf
```
Add `stopifnot(inherits(m, "ranger"))` after loading so a future mismatch fails loudly.

**Must fix together with #5** — fixing #1 alone makes a stray model file silently activate RF on
a `compute_rf_matching = FALSE` default run.

---

### 2. RF training labels poisoned: unequal known-matches labeled "match" (lines 485–510)

`reclin2::cmp_identical()` returns TRUE / **FALSE** / NA (it is literally `x == y`).
The label line `ifelse(is.na(match_id), 0L, 1L)` maps FALSE → 1 → "match". So a blocked pair
where left is known-match #5 and right is known-match #7 (two different sites) becomes a positive.
With ~97% of 2023/2024 rows carrying labels and blocking only on `water_company`, the vast majority
of candidate pairs have both sides labeled non-NA — so millions of garbage pairs become positives
and the ~14k true matches are a rounding error. The model learns "everything matches".

Verified empirically: `cmp_identical()(c(1,1,2,NA), c(1,2,NA,NA))` → `TRUE FALSE NA NA` → labels `1 1 0 0`.

**Fix:**
```r
# lines 507-510
pairs[, match_id := factor(fifelse(match_id %in% TRUE, 1L, 0L),
                           levels = c(0,1), labels = c("non_match","match"))]
```

---

### 3. Empty tibble() from RF skip paths crashes build_lookup_from_matches (line 813)

`run_rf_matching` stores `tibble()` (zero columns) for any skipped pair; `perform_rf_matching`
returns `list(matches = tibble())` for zero blocked pairs or zero threshold-passing matches.
In `build_lookup_from_matches`, `grep("^join_keys_", names(tibble()))` is `character(0)` and
`filter(df, !is.na(.data[[character(0)]]))` errors on dplyr 1.2.1 with:
*"Must subset the data pronoun with a string, not an empty character vector."*
The run dies at step 5 after all windfall and RF compute is spent.

**Fix:** Drop empty frames at the top of `build_lookup_from_matches`:
```r
match_dfs <- purrr::keep(match_dfs, ~ nrow(.x) > 0 &&
                           any(grepl("^join_keys_", names(.x))))
```

---

### 4. Same-year sites in one component silently collapsed by first() — fires on real data (line 853)

Edges only connect different years, but transitive chains pull two same-year sites into one component.
`group_by(component, year) %>% summarise(site_id = first(site_id))` keeps an arbitrary winner; the
loser is re-appended as an unrelated singleton while `edge_metadata` still records its original link —
the two output tables contradict each other.

**This fires on real data.** The `annual_return_lookup_conflict_summary.parquet` records 96 conflicted
component-years. The current clean on-disk outputs were produced by conflict-resolution code
(`resolution_dropped/kept_edges.parquet`, Jun 9) that **does not exist in the repo** — a fresh run of
the committed script re-introduces those 96 conflicts silently.

**Fix:** Integrate the conflict-resolution logic into the script (or a companion script).
Minimum viable:
```r
# before pivot in build_lookup_from_matches
dupes <- membership_tbl %>% count(component, year) %>% filter(n > 1)
if (nrow(dupes) > 0) {
  logger::log_warn("{nrow(dupes)} component-year conflicts; removing weakest edges")
  # remove lowest-weight edge per conflicted component and recompute components
  # or at minimum: export conflicts and exclude those components
}
```

---

## P2 fixes (data quality / silent misbehavior)

### 5. compute_rf_matching = FALSE doesn't disable RF if a model file exists (lines 753, 1002)

The flag only gates `train_if_missing`; the model is loaded unconditionally whenever the file exists.
The comment at line 407 ("executed only if compute_rf_matching is TRUE") is false.
**Fixing #1 without this makes the default run silently probabilistic.**

**Fix:** Gate the entire RF block on the flag in `main()`:
```r
if (compute_rf_matching) {
  m_rf <- load_or_train_rf_model(data_list, ...)
  # ... RF matching block
}
```

---

### 6. "TBC"/"N/A" placeholders join as real values in windfall matching (line ~283)

TBC/NA cleaning exists only in the RF helpers (lines 474-480, 601-608); the deterministic joins see
raw values, and `"TBC" == "TBC"` matches since `na_matches = "never"` only guards actual NA.
Real-data counts: 1,258 TBC `permit_reference_ea`, 682 `permit_reference_wa_sc`, 414 `site_name_ea`.
At deep cascade levels a single TBC-keyed pair per side in a water company yields a spurious 1:1 match.

**Fix:** Move the cleaning into `prepare_data_list` so it applies to all paths.

---

### 7. Upstream: unmapped 2021 header variant silently nulls site_name_wa_sc for 2,577 rows

In `combine_annual_return_data.R`, the 2021 workbook header `site_name_wa_sc_operational_name_optional`
is not in `CONFIG$column_name_mapping` (only `..._operational_optional` is mapped, line ~84).
Result: 2021 `site_name_wa_sc` coverage is 62% vs ≥95% for other years; 2,577 sites can only match
on weaker key combinations, silently degrading all 2021 cross-year matching.

**Fix (in `combine_annual_return_data.R`):**
```r
"site_name_wa_sc_operational_name_optional" = "site_name_wa_sc"
```
Regenerate the parquet afterward.

---

### 8. RF "unmatched" IDs re-derived from conflict-lossy prelim lookup (line 991)

`perform_windfall_matching` already returns exact per-pair unmatched IDs but `run_all_windfall_matches`
discards them (line 393). `main()` re-derives "unmatched" via setdiff against the prelim lookup, which
has already been through the #4 collapse — re-feeding matched sites to RF, manufacturing extra edges
and compounding same-year conflicts.

**Fix:** Propagate `unmatched_left_ids`/`unmatched_right_ids` from `run_all_windfall_matches` rather
than re-deriving them.

---

## P3 / housekeeping (lower urgency)

- **Line 638:** `n`, `m`, `inplace` are not `select_n_to_m` arguments (reclin2 0.6.0 confirmed) — silently swallowed by `...`. Remove them.
- **Line 494:** `threshold_default` is dead — `cmp_list` covers all variables so `default_comparator` never fires. Fix: `lapply(match_vars, function(x) jaro_winkler_na(threshold_default))`.
- **Lines 353 & 1007:** `year_pairs` `expand.grid` block duplicated — extract a `make_year_pairs(years)` helper.
- **Line 660:** `match_type` vocabulary inconsistent (RF: `"one_to_one"`, windfall: `"1_to_1"`); `match_level` scales are incomparable across methods; 2023_2024 config shifts all level numbers by 1 vs other pairs.
- **Line 905:** Empty lookup → `max(numeric(0))` = `-Inf` singleton IDs (no error). Add a guard.
- **Line 24:** fastLink, rnrfa, pbapply, hms, sf attached but never used. Remove from `pkgs`.
- **Line 86:** `YEARS <- CONFIG$years` defined but never referenced. Remove.
- **Line 890:** `append_singleton_sites` reads `CONFIG$years` instead of deriving years from its `data_list` arg. Fix: `years <- as.integer(sub("df", "", names(data_list)))`.

---

## Things that look like bugs but are fine

- **2023↔2024 unique_id_2023 join:** parquet natively carries `unique_id_2023` for 2024 rows; the 2023 mutate overwrites from `unique_id`. Both sides populated. dplyr suffix collision repaired automatically.
- **setdiff across types** (integer IDs vs character lookup): coerces correctly (verified).
- **`mst(g, weights = -E(g)$weight)`:** valid max-spanning-forest on igraph 2.3.1 (verified).
- **`components()$membership[from]` by vertex name:** works correctly (verified).
- **`paste0(join_keys, collapse = "|")` inside `mutate()`:** resolves to the function arg (no column), fragile but correct.
- **`site_id_<yr>` fallback:** parquet already carries all four columns; `row_number()` never fires.
- **ranger prediction on pairs data.table with extra columns:** selects by name, no error (verified).
- **`reclin2::link(keep_from_pairs=)`, `pair_blocking(add_xy=)`:** both valid in installed version.
