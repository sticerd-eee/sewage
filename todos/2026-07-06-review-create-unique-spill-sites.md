# Code review: `create_unique_spill_sites.R` (2026-07-06)

Sanity-check review of the whole script `scripts/R/03_data_enrichment/create_unique_spill_sites.R`
(post works-crosswalk migration, commit `ee11509`). Three reviewers (correctness, data-contract,
maintainability) plus sub-agents that traced the upstream key spaces and audited every downstream
consumer. Findings are ordered by severity. Each item is a checkbox so this file doubles as a todo list.

**Status (2026-08-11): resolved by the Canonical Spill Site grain migration.** The original review below is retained as defect evidence. The replacement builder is keyed by `site_id_canonical`, derives exact lookup coverage and Site Group membership with fail-closed checks, uses Annual Return row presence for availability, and delegates commissioning to the classified evidence resolver.

---

## P1 — should fix

### 1. [x] `resolve_commission_date()` lets a "to be installed" future date beat an actual earlier commission date

- **Where:** `create_unique_spill_sites.R:298-325` (resolution rules), interacting with
  `parse_commission_date()` and `get_commission_date_precision()`.
- **What happens:** Rule 1 only drops future-installation candidates when a non-future text
  exists in a *strictly later* reporting year. When the "to be installed by [Month] [Year]" text
  comes from the most recent reporting year, the future candidates survive. The future text then
  matches the month-year regex, so it scores precision 3, beating year-level texts like
  "Commissioned in 2017" (precision 2), and the recency tie-break at rule 3 also favours it.
  The function returns the *promised* installation date as `edm_commission_date`, contradicting
  the intent of rule 0 ("drop future installation-only values").
- **Verified:** reproduced with the script's own functions:
  `resolve_commission_date(c("Mar 2021", "to be installed by December 2023"), c(2021, 2023))`
  returns `2023-12-01`; same for `"Commissioned in 2017"` in place of `"Mar 2021"`.
  On the real data, sites 779 and 5495 currently resolve to `2023-12-01` despite having
  non-future texts ("Mar 2021" and "Commissioned in 2017" respectively).
- **Suggested fix:** in rule 1, prefer non-future candidates whenever any exist
  (`if (nrow(non_future) > 0) candidates <- non_future`), instead of conditioning on a later year.
  If the intended semantics are "a later still-not-installed report overrides an earlier claimed
  date", then return `NA` in that branch — but a text flagged `is_future` should never be the
  returned commission date.
- **Downstream impact:** `edm_commission_date` feeds `edm_commission_timeline.R` and
  `edm_commission_cumulative.R` (descriptive figures). Re-run those after fixing.

## P2 — worth fixing

### 2. [x] Month-name date parsing is locale-dependent

- **Where:** `create_unique_spill_sites.R:174-176, 189-191` (`as.Date(date_str, format = "%d %b %Y")`).
- **What happens:** `%b`/`%B` parsing depends on `LC_TIME`. Under a non-English locale
  (probe-verified with `it_IT.UTF-8`), `parse_commission_date("May 2021")` fails the month-year
  parse and silently falls through to the any-year fallback, returning `2021-01-01` while the
  precision score stays 3. Output is correct on the current machine (`en_GB`) but the script is
  not reproducible across machines/locales — a real concern for replication packages.
- **Suggested fix:** map the captured month token yourself, e.g.
  `month_num <- match(tolower(substr(month_str, 1, 3)), tolower(month.abb))` and build the date
  with `sprintf("%s-%02d-01", year_str, month_num)`; or wrap the parse in
  `withr::with_locale(c(LC_TIME = "C"), ...)`.

### 3. [x] Upstream: a lookup site whose rows fall outside the configured years can silently vanish from the crosswalk

- **Where:** `scripts/R/05_data_integration/merge_individ_annual_location.R:211-267`
  (`filter_years_with_guard`), affecting the site universe this script builds.
- **What happens:** the crosswalk's `site_id` space is provably drawn from the annual-return
  lookup's `site_id` space (component representative = min member id; hard `stop()` if an annual
  row fails to link). The one unguarded path: annual rows dropped by the `year %in% years` filter
  take their lookup site id with them, with only a warning. No assertion checks that every lookup
  `site_id` is reachable from the crosswalk (as `site_id` or inside `site_id_members`).
- **Suggested fix:** add a coverage assertion in the merge script (or a testing-script check)
  that `lookup$site_id ⊆ {crosswalk$site_id} ∪ {parsed site_id_members}`.

### 4. [x] Consumer: `aggregate_daily_spill_rainfall.R` selects only `available_year_2021/2022/2023`, omitting 2024

- **Where:** `scripts/R/03_data_enrichment/aggregate_daily_spill_rainfall.R:87-88`.
- **What happens:** the output schema now carries `available_year_2024`, but this consumer's
  `select()` stops at 2023. Possibly intentional (rainfall window ends in 2023?) — confirm intent;
  if stale drift, add the 2024 column.

## P3 — low priority / hygiene

### 5. [x] Any-4-digit-year fallback mangles Excel-serial text

- **Where:** `create_unique_spill_sites.R:219` (parser fallback) and `:258` (precision scorer).
- **What happens:** `parse_commission_date("44531")` returns `4453-01-01` with precision 2
  (the `\d{4}` extraction grabs the first four digits of a five-digit Excel date serial).
  Exactly one such row exists in the current data and another candidate wins its site's
  resolution, so the impact is latent, not present in today's output.
- **Suggested fix:** constrain both regexes to `\b(19|20)\d{2}\b`, and optionally decode 5-digit
  serials via `as.Date(as.numeric(text), origin = "1899-12-30")` (44531 → 2021-12-01).

### 6. [x] `purrr` and `tibble` are listed in `REQUIRED_PACKAGES` but never used

- **Where:** `create_unique_spill_sites.R:35-50`. No `purrr::`/`tibble::` calls and no
  unqualified usage anywhere in the file. Remove them (keep `rnrfa` — it is used by `ngr_utils.R`).

### 7. [x] Hardcoded `2021:2024` defaults duplicated across six function signatures

- **Where:** lines 332, 448, 514, 638-639, 759-760, 852-853.
- **What happens:** `main()` always passes `CONFIG$availability_years`/`CONFIG$metadata_years`,
  so the defaults are dead in the normal path — but any interactive call or partial refactor that
  drops an argument silently reverts to 2021:2024. When 2025 data arrives, editing CONFIG alone
  will not update the defaults.
- **Suggested fix:** drop the literal defaults, or define `DEFAULT_YEARS <- 2021:2024` once and
  reference it.

### 8. [x] `annual_status == "reported_na"` counts as an available/reporting year — undocumented choice

- **Where:** `create_unique_spill_sites.R:352` (`is_reporting_year = annual_status != "absent"`).
- **What happens:** the status domain is exactly `absent / reported_na / reported_positive /
  reported_zero` (verified against the parquet and `merge_outputs_utils.R:133-139`). A
  `reported_na` year (return filed, both spill metrics NA) is treated as reporting. Defensible,
  but worth a one-line comment stating it is intentional.

### 9. [x] Log-file prefix "13" no longer matches the documented pipeline position (step 14)

- **Where:** `create_unique_spill_sites.R:52`; `docs/pipeline_documentation.md` lists this script
  as step 14. Sibling scripts' prefixes are also out of sync with the doc's ordinal list
  (apparently a deliberate stable-per-script convention). Either renumber or note the convention.

---

## Migration closure evidence (2026-08-11)

- Items 1, 2, and 5 are covered by `test_edm_commission_contracts.R`: a reviewed 63-form golden fixture, locale-independent month parsing, explicit Excel serial decoding, future/actual chronology, pre-2016 evidence, and closed status/precision/date combinations.
- Item 3 is covered by the canonical builder and `reconcile_site_grain_migration.R`: every Annual Return Lookup ID appears once in canonical output and once in Site Group membership, with the group ID equal to its smallest member.
- Item 4 is superseded at the correct grain. Rainfall consumers obtain requested-year availability from Site Group annual status rather than from canonical rows.
- Items 6, 7, and 9 were removed as stale implementation assumptions: the current dependency list reflects real utility use, configured years have one owner, and the log uses the plain script name.
- Item 8 is now explicit: canonical availability means Annual Return row presence, including a row whose spill metrics are missing; closure evidence never carries availability forward.
- Operation percentage and reason conflicts are detected before collapse and yield `NA` plus year-specific conflict flags. Commission observations are resolved from the full canonical history, independent of row order.
- Group-keyed consumers use the unique Site Group projection. No consumer deduplicates repeated `unique_spill_sites.site_id` to select canonical metadata.

*Run artifacts: `/tmp/compound-engineering/ce-code-review/20260706-234714-681b1af5/`*
