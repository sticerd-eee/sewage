# Rebuild plan: merge_individ_annual_location.R

**Date:** 2026-07-04
**Branch:** `jo/merge_indivi_annual`
**Status:** decisions finalised (grill session 2026-07-04); execution plan at
`docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md`
(this document remains the decision record)
**Supersedes:** the fix-in-place programme in `todos/011-pending-p0-location-merge-findings.md`
(the rebuild resolves or obsoletes all 25 findings; mapping at the end of this document)

## Purpose

Rebuild `scripts/R/05_data_integration/merge_individ_annual_location.R` from scratch.
The script links event-level EDM spill records to annual-return site records so that
events carry site identity, coordinates, and annual EA totals. The old implementation
matched per event row through three stages (windfall key enumeration, max-to-one merge,
EM fuzzy matching) and accumulated 25 review findings, including silent row loss and an
unvalidated recall-maximising tiebreak.

Every design decision below was made explicitly with evidence; none should be silently
revisited during implementation. Empirical figures come from profiling runs on
2026-07-04 (agents: input profile, consumer map, prior-run evidence, ambiguity
diagnostic; diagnostic scripts in the session scratchpad).

## Evidence base (key facts the decisions rest on)

- Events: 7.38M rows, 2021–2024. Identifier coverage: `site_name_ea` 91.8%,
  `unique_id` ~0% in 2021–22 / 96–100% in 2023–24, `permit_reference_ea` 1.9%.
  Events carry **no coordinates**. The event feed is **positives-only** (no zero-spill
  records).
- Annual returns: 57.9k site-year rows (~14.5k/yr), identifier-rich
  (permit ~99%, name ~99%, NGR ~100%). `site_id_YYYY` = within-year row number.
- Cross-year lookup: 15,430 canonical `site_id` clusters over `site_id_2021..2024`.
- Prior run: 97.8% of events matched — windfall 89.3%, **max-to-one 9.8% (529k events,
  no quality signal)**, fuzzy 0.9% (median EM posterior 0.714). 124k events unmatched.
- Ambiguity diagnostic: 3,116 (company, year, name) groups where one event-side name
  maps to >1 annual row (7,872 annual rows; ~15.7% of event rows). Within these groups:
  **64.6% byte-identical NGRs, 71.7% identical `permit_reference_ea`**, 58.7% differ on
  `activity_reference` → predominantly *one works filing per-outlet rows*, not distinct
  sites. Top-two candidate `spill_hrs_ea` ratio ≥3× in 61.6% of groups (median 4.6×).
- Temporal unique_id bridge (2023–24 event names → unique_id → earlier years): resolves
  only ~2% of ambiguous groups end-to-end. **Rejected as a pipeline stage.**
- Downstream: 8 consumer scripts read the matched event file; all use `site_id`,
  `start_time`/`end_time`; none use match diagnostics. `site_metadata.parquet` has zero
  readers. Annual-only pseudo-rows in the matched file are load-bearing **only** for
  `create_unique_spill_sites.R` (site-year discovery / availability evidence).

## Decisions (all signed off 2026-07-04)

### D1. Grain: site crosswalk, not per-event matching

Matching happens once per distinct identifier tuple / site, producing a crosswalk.
The event-level file is a mechanical join of events × crosswalk. Rationale: events at
the same site must resolve identically; decisions become auditable at the grain where
they are made.

### D2. Governing principle: precision first

A match must be justified by evidence in the data; ambiguity is surfaced, never
resolved by heuristic magnitude. The old max-to-one merge (pick candidate with largest
`spill_hrs_ea`) is **dropped**: it selects on (roughly) the treatment variable, biasing
attached totals upward, with no per-match quality signal. Unmatched residuals are a
first-class, reason-coded output.

### D3. Works register (year-invariant collapse of same-works outlets)

The dominant "ambiguity" is a grain mismatch: events observe **works** (name only),
annual returns observe **outlets**. Resolution: build a works register *on top of* the
cross-year lookup and match everything at works grain.

- Nodes: canonical lookup `site_id`s. Edge between two site_ids iff in **any** year
  they file annual rows with same company + same normalised `site_name_ea` **and** at
  least one corroborator:
  - identical `permit_reference_ea` (works-level legal document), **or**
  - NGR distance ≤ **250 m** (converted to easting/northing; identical NGR trivially
    passes). Threshold in CONFIG.
- **No permit-only edges** (same permit + different names = distinguishable assets;
  the event data can tell them apart, so do not collapse).
- Connected components = works. Membership is year-invariant by construction (fixes
  panel flicker: a one-year name variant cannot split a works; every matching tier in
  every year resolves to the same works).
- Same-name pairs 250 m–1 km apart are **not** merged but listed in the near-miss
  report for eyeball review.
- Emitted identifier: `site_id` = deterministic representative member (smallest member
  `site_id`), keeping the downstream join key unchanged. Crosswalk carries
  `site_id_members`, `n_outlets`, and `n_outlets_reporting` per works-year.
- Works-year EA totals = **sum across member outlets' annual rows** for that year.
  Documented caveat: EA 12/24 block counting is per-outlet, so summed counts slightly
  overcount simultaneous multi-outlet spills; hours sum cleanly. Only downstream use of
  these totals is the NLO carry-forward heuristic, which is unaffected.
- The register is built **inside this stage** (its parameters belong to this merge),
  not inside `create_annual_return_lookup.R` (which stays outlet-grain).

### D4. Matching tier 1: ordered exact-key ladder (replaces 2^n−1 enumeration)

Matching unit: distinct event-side tuple per company-year over
(`site_name_ea`, `site_name_wa_sc`, `permit_reference_ea`, `permit_reference_wa_sc`,
`activity_reference`, `unique_id`, `site_code`). Each tuple walks an ordered whitelist;
a rung succeeds iff its matching annual rows all belong to **exactly one works**:

1. `unique_id` (constructed identifier; 2023–24)
2. `permit_reference_ea` (+ `activity_reference` when the event carries it)
3. `site_name_ea` (workhorse; register absorbs the within-works collision mode)
4. `site_name_wa_sc` (fallback for the ~8% of events with no EA name)

Rules: light normalisation only (trim, case-fold, whitespace-collapse — nothing that
could merge two real sites), applied identically on both sides; **no key weaker than a
site identifier** (no company|year fallthrough, ever); if two usable keys resolve to
*different* works → reason `key_conflict`, unmatched, surfaced (upstream data-error
probe). `match_method` records the rung used.

### D5. Matching tier 2: aggregate-agreement (only for cross-works name collisions)

For tuples whose candidates span multiple distinct works (~35% of former ambiguity):

- Compare **hours** (event-side summed durations vs candidate works' `spill_hrs_ea`);
  counts only as a sanity check (12/24 convention makes them non-comparable directly).
- Accept candidate c iff relative error(c) ≤ **0.25** and runner-up error ≥ **2×**
  error(c) (absolute floor to avoid 0-vs-0 fake separation). Thresholds in CONFIG.
- ~0 event hours vs multiple ~0 candidates → `agreement_uninformative`, unmatched.
- `match_quality` = the relative error (re-filterable downstream).
- **No `edm_operation_percent` scaling in v1** (its failure mode is false acceptance);
  partial-coverage rejects land in the near-miss report.

### D6. No EM fuzzy stage; no temporal-bridge stage

Fuzzy bought 0.9% of events at median posterior 0.714 — replaced by a human-reviewable
near-miss report + manual-overrides file. The temporal unique_id bridge resolves ~2% of
ambiguous groups — not worth a stage; its cases also surface in the near-miss report.

### D7. Outputs (clean break; no mixed-grain compatibility rows)

1. **`site_works_crosswalk.parquet`** — canonical artefact. One row per works-year:
   representative `site_id`, members, `n_outlets`, `n_outlets_reporting`, NGR +
   easting/northing, summed EA totals, `annual_status`, coverage qualifiers
   (`edm_operation_percent`, `no_full_years_edm_data`, commission date),
   match map (`match_method`, `match_quality`, reason codes). Replaces the unread
   `site_metadata.parquet`.
2. **`matched_events_annual_data.parquet`** — **pure event grain** (every row a real
   event; no NA-timestamp pseudo-rows), same consumer-critical columns as today.
3. **`events_unmatched.parquet`** — with `reason` ∈ {`no_usable_key`,
   `name_spans_works`, `agreement_failed`, `agreement_uninformative`, `key_conflict`}.
4. **`annual_unmatched.parquet`** — derived convenience view (works-years with
   reported positive spills, no matched events), now carrying `site_id`.
5. **`near_miss_report.parquet`** — agreement rejects with candidate errors,
   string-near name pairs among unmatched, borderline register splits (250 m–1 km).

Column policy: event columns keep native names and values (`unique_id` never
overwritten); annual-side attachments arrive under explicit names; no
coalesce-prefer-annual anywhere.

### D8. True zero vs missing: explicit `annual_status` per works-year

Because the event feed is positives-only, absence of events is ambiguous; the annual
side disambiguates. Taxonomy: `reported_zero` (return present, both metrics 0),
`reported_positive`, `reported_na` (return present, metrics NA — the 4,076 rows the old
script silently dropped), `absent` (no return that year). Coverage qualifiers stay
attached rather than being folded into the binary.

### D9. Downstream migration in this project

`create_unique_spill_sites.R` is migrated to read site-year existence, availability
evidence, and zero/NA status from the crosswalk (not pseudo-rows). The other seven
consumers get a verify-only pass (they already ignore NA-timestamp rows). Consumers:
`create_unique_spill_sites.R`, `aggregate_spill_stats.R`, `identify_dry_spills.R`,
`aggregate_daily_spill_rainfall.R`, `cross_section_prior_to_sale.R`,
`house_spill_prior_to_sale.R`, `cross_section_prior_to_rental.R`,
`rental_spill_prior_to_rental.R`.

### D10. Engineering conventions (mirror the annual-return-lookup refactor)

- Orchestrator keeps current path/name; concerns extracted to sourced utils:
  `utils/merge_works_register_utils.R`, `utils/merge_matching_utils.R`,
  `utils/merge_outputs_utils.R`.
- Dependencies: drop `reclin2` and `rio` (all I/O via `arrow` — closes the P0
  nanoparquet blocker); add `stringdist` only if the near-miss name pass needs it.
- Preflight validation of all three inputs (existence + required columns) before any
  matching; atomic publish (staging dir → validate → swap) per
  `docs/solutions/best-practices/edm-api-combine-hardening-20260310.md`;
  all year-dependent constructs derived from `CONFIG$years`; row-accounting
  assertions at every stage boundary (silent loss becomes a crash).
- Logging: per-stage counts (tuples per rung, register edges by justification,
  agreement accept/reject); log filename prefix aligned to `05_`.

### D11. Testing contract (`scripts/R/testing/test_merge_individ_annual_contracts.R`)

Sources production utils (never re-implements); synthetic micro-fixtures assert:

1. Row accounting: every tuple matched or reason-coded; every works-year exactly once
   with a status; nothing vanishes (kills findings-2/4/5/6 bug class).
2. Register invariance: year-invariant membership; name variant cannot split a works;
   representative id deterministic across reruns.
3. Ladder discipline: unrelated same-company-year records never match; `key_conflict`
   fires on contradictory keys; normalisation-only variants do match.
4. Agreement edges: near-equal → refuse; 0-vs-0 → uninformative; NA metrics never
   crash or drop (findings-7/8 class).
5. Status taxonomy on 0/NA/absent fixtures.
6. Export atomicity under simulated mid-publish failure.

Plus an integration smoke test on one real company-year asserting the event-file
schema (names + types) that the eight consumers rely on.

### D12. Acceptance criteria

- **Reconciliation report** (old vs new): match rate by tier and year; fate of the old
  529k max-matched events (register-absorbed / agreement-matched / unmatched+reason);
  site count change (a drop is expected and correct — outlet collapse); coordinate
  churn distribution, with every event whose assigned location moves >1 km listed.
- **Hard gates:** old exact-tier matches preserved in substance (any works change
  appears in an explained exceptions list, expected to be register collapses); zero
  unexplained event-row loss; all contract tests green.
- **Soft gate:** overall match rate expected mid-90s%; if materially lower, adjust
  CONFIG tolerances as an explicit, logged decision informed by the near-miss report.
- **Downstream:** after migrating `create_unique_spill_sites.R`, diff its site
  inventory and availability flags old-vs-new to the same explain-every-change
  standard.

## Prerequisites (state as of 2026-07-04)

- [x] Todo 009 lookup fixes — landed on `main` (`b4493f2`..`5692af6`), archived done.
- [x] `origin/main` merged into `jo/merge_indivi_annual` (done during grill session).
- [ ] Regenerate `data/processed/annual_return_lookup.parquet` with the fixed builder
      (on-disk file dated 2026-06-10 17:00 predates the final fix commits of 00:05
      2026-06-11) — must happen before the works register is built or validated.

## Implementation phases

1. **Regenerate lookup** (prerequisite above); re-run the ambiguity diagnostic against
   it to confirm the D3/D5 parameters still hold (numbers should move only slightly).
2. **Works register** (`merge_works_register_utils.R`) + its contract tests.
3. **Key ladder + aggregate agreement** (`merge_matching_utils.R`) + contract tests.
4. **Panel assembly, statuses, outputs, atomic publish** (`merge_outputs_utils.R`) +
   orchestrator + smoke test.
5. **Reconciliation report** vs old outputs; review against D12 gates (user decision
   point on the soft gate).
6. **Migrate `create_unique_spill_sites.R`**; verify remaining seven consumers; diff
   downstream outputs.
7. Retire `todos/011` with a findings→resolution map; update
   `pipeline_documentation.md`.

## Findings map (todo 011 → this plan)

Resolved by design: 2, 3, 5, 6 (ladder/union domains: D4, D1), 4, 16 (statuses: D8),
7, 8, 9 (max-match internals: stage deleted, D2/D5), 10, 12, 13, 14 (fuzzy: stage
deleted, D6), 15 (column policy: D7), 21 (crosswalk replaces site_metadata: D7),
24 (naming: D7). Resolved by engineering: 1 (arrow-only I/O), 11, 18, 19, 20 (D10),
17, 22, 23, 25 (rewrite removes). Testing checklist → D11.
