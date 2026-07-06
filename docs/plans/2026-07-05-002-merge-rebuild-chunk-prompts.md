# Chunk prompts: merge_individ_annual_location rebuild

One copy-pasteable prompt per execution chunk of
[the rebuild plan](2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md).
Each prompt is self-contained for a fresh thread. Launch order and allowed
parallelism are defined in the plan's "Execution Chunking" section: CH1 first;
then CH2 + CH3 + CH4 + CH5 in parallel; CH6 after CH3–CH5; CH7 after CH2 + CH6
(user gate review — do not launch CH8+ until it signs off); CH8 + CH9 in
parallel; CH10 last.

---

## CH1 — Regenerate lookup, calibrate diagnostic (U1)

```text
You are working in the R economics research pipeline at the repo root. Read, in this
order: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(the governing plan — your unit is U1; also read Requirements, KTDs, and Execution
Chunking), and docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md
(locked decision record, especially the Evidence base section whose figures you must
reproduce).

Task: execute U1 exactly as written. (1) Archive data/processed/annual_return_lookup.parquet
before touching anything. (2) Write a repeatable ambiguity-diagnostic script under
scripts/R/testing/ (re-derived from the origin's evidence description) and run it against
the ARCHIVED stale lookup first: it must reproduce the origin's figures (3,116 ambiguous
groups; 64.6% identical NGR; 71.7% identical permit; 61.6% of groups with >=3x top-two
hours ratio) before you proceed. (3) Regenerate the lookup by running
scripts/R/03_data_enrichment/create_annual_return_lookup.R unchanged, then run
scripts/R/testing/test_annual_return_lookup_contracts.R (pinned baselines in the plan;
drift acceptable only if explained). (4) Re-run the diagnostic on the regenerated lookup
and write up the deltas.

Environment: R 4.6.0 via rv (rv sync); run everything with plain Rscript, never
--vanilla (it hides the rv library and arrow disappears). Branch:
jo/merge-ch1-lookup-regen. Do not modify any production script.

STOP CONDITION: if the diagnostic deltas are material (the plan's D3/D5 parameters would
no longer hold), stop and report — do not proceed and do not tune parameters yourself.
Done when: lookup regenerated and contract tests green; calibration and delta write-up
committed alongside the diagnostic script.
```

---

## CH2 — Baseline capture (U2)

```text
You are working in the R economics research pipeline at the repo root. Read
docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md — your
unit is U2; also read the Execution Chunking section. Prerequisite: CH1 has landed (the
regenerated data/processed/annual_return_lookup.parquet is on disk and its contract
tests passed).

Task: execute U2 exactly as written. (1) Archive the current
data/processed/matched_events_annual_data/ directory to a sibling archive location —
the old outputs are evidence, archive BEFORE any run. (2) Run the existing (old)
scripts/R/05_data_integration/merge_individ_annual_location.R once, UNMODIFIED, on
current inputs to produce the like-for-like reconciliation baseline. nanoparquet is in
the dependencies, so its rio::import path works. This is a long compute job over 7.38M
events — expect it. (3) Record the baseline's row counts and match-method distribution,
and record input fingerprints (path, size, mtime, checksum) of combined_edm_data.parquet,
annual_return_edm.parquet, and annual_return_lookup.parquet. Store the records where U7's
reconciliation will read them (a small parquet/CSV next to the archive is fine — document
the location in your commit).

Environment: R 4.6.0 via rv; plain Rscript, never --vanilla. Branch:
jo/merge-ch2-baseline. Do not modify any production script.

STOP CONDITION: if the old script stalls or exhausts memory on the regenerated inputs
(its fuzzy stage has never run at this input size), stop and report rather than patching
the old script. Done when: archive + baseline + fingerprint records committed, with the
baseline matched-row total recorded (sanity: well above the January 5.41M rows).
```

---

## CH3 — Works register utils (U3)

```text
You are working in the R economics research pipeline at the repo root. Read, in this
order: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your unit is U3; read Requirements R3, KTD-8/KTD-10/KTD-11, Contract A in Execution
Chunking), the origin decision record docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md
(D3 is your spec), and the structural template scripts/R/03_data_enrichment/create_annual_return_lookup.R
with scripts/R/utils/annual_return_lookup_graph_utils.R and
scripts/R/testing/test_annual_return_lookup_contracts.R (mirror their conventions:
roxygen docs, pure functions, bespoke assert helpers, fixture style).

Task: implement U3. Create scripts/R/utils/ngr_utils.R by hoisting clean_ngr() and
parse_bng_coordinates() from scripts/R/03_data_enrichment/create_unique_spill_sites.R
(do NOT modify create_unique_spill_sites.R itself — it switches over in a later chunk;
interim duplication is accepted by the plan). Create
scripts/R/utils/merge_works_register_utils.R implementing the register per D3 and the
plan: corroborated edges (identical permit_reference_ea OR BNG distance <= 250 m from
CONFIG), no permit-only edges, connected components, smallest-member representative,
near-miss (250 m–1 km) list, per-edge justification logging. Your public output must
satisfy Contract A in the plan's Execution Chunking section — if you need to change the
contract, edit that plan section in your branch and say so loudly in your report.
Write scripts/R/testing/test_merge_works_register_contracts.R covering every U3 test
scenario listed in the plan.

Files you own (touch nothing else): merge_works_register_utils.R, ngr_utils.R,
test_merge_works_register_contracts.R. Environment: R 4.6.0 via rv; plain Rscript,
never --vanilla. Branch: jo/merge-ch3-works-register. Done when: your contract-test
file passes standalone via Rscript and every U3 test scenario is covered.
```

---

## CH4 — Matching ladder and agreement tier (U4)

```text
You are working in the R economics research pipeline at the repo root. Read, in this
order: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your unit is U4; read Requirements R2/R4/R5/R6, KTD-2 through KTD-7, and Contracts A/B
in Execution Chunking — KTD-2/3/4/5 pin semantics you must implement exactly), the
origin decision record docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md
(D4/D5/D6), and the conventions template scripts/R/03_data_enrichment/create_annual_return_lookup.R
plus scripts/R/testing/test_annual_return_lookup_contracts.R.

Task: implement U4 in scripts/R/utils/merge_matching_utils.R. Distinct tuples per
company-year over the six id columns in KTD-6; normalisation = trim, case-fold,
whitespace-collapse plus the placeholder-to-NA pattern from create_annual_return_lookup.R
prepare_data_list(); ladder rungs unique_id (same-year ONLY, per KTD-2) → permit+activity
→ permit-only (KTD-3) → site_name_ea → site_name_wa_sc, with register-mediated resolution,
match-to-absent, and the KTD-4 conflict rule; agreement tier per KTD-5 reusing
calculate_spill_hours() from scripts/R/utils/spill_aggregation_utils.R; manual-overrides
contract per KTD-7. Build test-first against micro-fixtures (the plan's Execution note).
The works register may not exist yet in your thread: consume it through Contract A and
build register fixtures conforming to it. Your decision output must satisfy Contract B —
any contract change must be edited into the plan's Execution Chunking section and flagged
loudly. Write scripts/R/testing/test_merge_matching_contracts.R covering every U4 test
scenario in the plan.

Files you own (touch nothing else): merge_matching_utils.R,
test_merge_matching_contracts.R. Environment: R 4.6.0 via rv; plain Rscript, never
--vanilla. Branch: jo/merge-ch4-matching. Done when: your contract-test file passes
standalone and every U4 scenario, including the cross-year unique_id and key_conflict
fixtures, is covered.
```

---

## CH5 — Outputs assembly and atomic publish (U5)

```text
You are working in the R economics research pipeline at the repo root. Read, in this
order: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your unit is U5; read Requirements R7/R8/R9, KTD-9/KTD-10, Contracts A/B/C in Execution
Chunking), the origin decision record docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md
(D7/D8), docs/solutions/best-practices/edm-api-combine-hardening-20260310.md (the
publish-gate pattern), and scripts/R/testing/test_edm_api_pipeline_contracts.R (the
subprocess kill-test pattern you must reuse).

Task: implement U5 in scripts/R/utils/merge_outputs_utils.R. Crosswalk = full works x
CONFIG$years grid with annual_status taxonomy, summed works-year totals (12/24 overcount
caveat documented), n_outlets, n_outlets_reporting, water_company, NGR per KTD-10,
coverage qualifiers, match map. Matched-events file = pure event grain, native columns
preserved, annual attachments under explicit names, no coalesce-prefer-annual.
events_unmatched (5 reason codes), annual_unmatched, near_miss_report. Publish per KTD-9
exactly: preflight narrowed reads, fail-closed gate modelled on
validate_publishable_combined_data(), staging dir, double directory rename
(canonical -> .prev, staging -> canonical). Consume upstream data via Contracts A and B
with fixtures (register and matcher may not have landed in your thread). Write
scripts/R/testing/test_merge_outputs_contracts.R covering every U5 test scenario,
including the subprocess mid-promotion kill test asserting the KTD-9 invariant.

Files you own (touch nothing else): merge_outputs_utils.R,
test_merge_outputs_contracts.R. Environment: R 4.6.0 via rv; plain Rscript, never
--vanilla. Branch: jo/merge-ch5-outputs. Done when: your contract-test file passes
standalone, including schema parity (empty vs populated) for all five outputs.
```

---

## CH6 — Orchestrator rebuild and integration smoke test (U6)

```text
You are working in the R economics research pipeline at the repo root. Prerequisites:
CH3, CH4, CH5 have merged (the three merge_* utils and their module contract tests are
on your branch's base). Read: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your unit is U6; read R11/R12, KTD-8/KTD-9/KTD-11, Execution Chunking), the structural
template scripts/R/03_data_enrichment/create_annual_return_lookup.R (mirror its section
structure exactly: banner, here-guard + script_setup.R, REQUIRED_PACKAGES, CONFIG,
sourced-utils block, pure functions, main() with tryCatch, sys.nframe() guard), and
docs/solutions/best-practices/data-cleaning-script-header-bootstrap-standardisation-20260310.md.

Task: rewrite scripts/R/05_data_integration/merge_individ_annual_location.R in place
(same path/name) as the thin orchestrator wiring the three utils: preflight -> works
register -> ladder -> agreement -> overrides -> outputs assembly -> gate -> staged swap,
with row-accounting assertions at every stage boundary and per-stage logged counts.
REQUIRED_PACKAGES arrow-only I/O — no reclin2, no rio. LOG_FILE =
output/log/05_merge_individ_annual_location.log. Every path and threshold in a flat
CONFIG. Then write scripts/R/testing/test_merge_individ_annual_contracts.R as a THIN
RUNNER that sources the three module test files plus your integration smoke test on one
real company-year, pinning the matched-file schema and arrow types the consumers rely on
(site_id int32, start_time/end_time timestamp[us, UTC], spill_hrs_ea/spill_count_ea
double, unique_id string, ngr, water_company, year), and a subprocess exit-code test with
a sabotaged CONFIG. Finally do a full real run: it must complete with all five outputs
published and per-stage counts logged. Do NOT overwrite the CH2 archive or fingerprint
records.

Files you own: the orchestrator and the runner test file. Environment: R 4.6.0 via rv;
plain Rscript, never --vanilla. Branch: jo/merge-ch6-orchestrator. Done when: the full
runner is green via Rscript and a complete real run has published all five outputs.
```

---

## CH7 — Reconciliation and gate review (U7)

```text
You are working in the R economics research pipeline at the repo root. Prerequisites:
CH2 (baseline + fingerprints) and CH6 (new outputs published) have landed. Read:
docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md (your
unit is U7; read R13 and the origin's D12), and locate CH2's archive, baseline outputs,
and fingerprint records.

Task: implement scripts/R/testing/reconcile_merge_rebuild.R and produce the
reconciliation report under output/. FIRST assert the recorded input fingerprints still
match the on-disk inputs — on mismatch, stop and report that a re-baseline is required;
do not compare anything. Report: match rate by tier and year, both including and
excluding annual_status = absent matches (the excluding variant is the like-for-like
number); the fate of the baseline's max-to-one events (register-absorbed /
agreement-matched / matched-to-absent / unmatched by reason); site count change;
coordinate churn distribution with every event moving >1 km listed and attributed via
the KTD-10 rule; an explained-exceptions list for any old exact-tier match that changed
works. Evaluate the hard gates (exact-tier preservation with explained exceptions; zero
unexplained event-row loss; contract tests green).

STOP CONDITION: you do NOT decide the soft gate. Present the gate results — hard gates
pass/fail with evidence, the overall match rate against the mid-90s expectation, and any
CONFIG tolerance proposals grounded in the near-miss report — and stop for the user's
decision. Downstream chunks must not start until the user signs off.

Environment: R 4.6.0 via rv; plain Rscript, never --vanilla. Branch:
jo/merge-ch7-reconciliation.
```

---

## CH8 — Migrate create_unique_spill_sites.R (U8)

```text
You are working in the R economics research pipeline at the repo root. Prerequisite:
the CH7 gate review has been signed off by the user. Read:
docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md (your
unit is U8; read R10, KTD-8, Contract C, and the origin's D9/D12), then
scripts/R/03_data_enrichment/create_unique_spill_sites.R in full (note
build_matched_site_data() and apply_nlo_carryforward()).

Task: migrate create_unique_spill_sites.R off the removed pseudo-rows. Site-year
existence and available_year_YYYY flags come from crosswalk works-years with
annual_status != 'absent'; fallback water_company/ngr come from the crosswalk; NLO
carryforward caps read works-year totals and status from the crosswalk (it consumes
positivity, so the summed-hours caveat does not affect it). Switch the script's private
clean_ngr()/parse_bng_coordinates() copies to sourcing scripts/R/utils/ngr_utils.R.
Add the U8 test scenarios from the plan as fixtures (status taxonomy -> availability
flags; fallback population; carryforward blocking). Then rerun the script and diff its
site inventory and availability flags old-vs-new to the explain-every-change standard —
expected changes are register collapses; anything unexplained blocks.

Files you own (touch nothing else): scripts/R/03_data_enrichment/create_unique_spill_sites.R
and its fixtures/diff script. Environment: R 4.6.0 via rv; plain Rscript, never
--vanilla. Branch: jo/merge-ch8-unique-sites. Done when: the script runs green on the
new crosswalk and the old-vs-new diff is fully explained in your report.
```

---

## CH9 — Migrate aggregate_spill_stats.R (U9)

```text
You are working in the R economics research pipeline at the repo root. Prerequisite:
the CH7 gate review has been signed off by the user. Runs in parallel with CH8 — do not
touch CH8's files. Read: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your unit is U9; read R10, KTD-1, Contract C), then
scripts/R/03_data_enrichment/aggregate_spill_stats.R in full (the metadata split around
lines 74–100, complete_data_observations() around lines 255–264, and the EA-fallback
coalesces around lines 279–353) and scripts/R/utils/spill_aggregation_utils.R
(prepare_spill_data()).

Task: migrate aggregate_spill_stats.R per U9. The event branch reads the pure-event
matched file unchanged; the metadata branch reads works-year rows from the crosswalk, so
reported_zero and reported_na site-years enter the yearly/monthly/quarterly grids
explicitly; the coalesce fills from crosswalk totals under their explicit names. Replace
rio::import with arrow. The thirteen descriptive columns complete_data_observations()
currently fills (site_name_ea through edm_commission_date) are DELIBERATELY dropped from
the aggregated outputs — the plan verified nothing downstream consumes them; record the
removal as an expected change. Add the U9 test scenarios from the plan as fixtures.
Then rerun and diff agg_spill_yr/mo/qtr old-vs-new: expected changes are the
works-collapse site count drop, preserved reported-zero site-years, and the column
removals; anything unexplained blocks.

Files you own (touch nothing else): scripts/R/03_data_enrichment/aggregate_spill_stats.R,
scripts/R/utils/spill_aggregation_utils.R only if its NA-filter needs a (likely vacuous)
adjustment, and your fixtures/diff script. Environment: R 4.6.0 via rv; plain Rscript,
never --vanilla. Branch: jo/merge-ch9-agg-stats. Done when: the script runs green and
the old-vs-new diff is fully explained in your report.
```

---

## CH10 — Verify remaining consumers, documentation, retirement (U10 + U11)

```text
You are working in the R economics research pipeline at the repo root. Prerequisites:
CH8 and CH9 have merged. Read: docs/plans/2026-07-05-001-refactor-merge-individ-annual-works-crosswalk-plan.md
(your units are U10 and U11; read R9/R10/R13) and the origin decision record
docs/plans/2026-07-04-002-refactor-merge-individ-annual-location-rebuild-plan.md
(its Findings map section feeds the todo retirement).

Task, part 1 (U10, verify-only — modify none of these six scripts): rerun and diff
scripts/R/03_data_enrichment/identify_dry_spills.R,
scripts/R/03_data_enrichment/aggregate_daily_spill_rainfall.R, and the four
scripts/R/06_analysis_datasets/ scripts (cross_section_prior_to_sale,
house_spill_prior_to_sale, cross_section_prior_to_rental, rental_spill_prior_to_rental).
Each must run green on the new matched file; output diffs must be attributable to the
reconciliation's explained changes (works collapse, matched-set changes) — anything
structural blocks. Note (do not fix) identify_dry_spills.R's rio::import read; it is a
deferred follow-up in the plan's Scope Boundaries.

Task, part 2 (U11): retire todos/011-pending-p0-location-merge-findings.md to done
status using the findings-to-resolution map in the origin document; fix the stale
execution order in docs/pipeline_documentation.md (correct order: lookup -> merge ->
unique-spill-sites -> aggregate-spill-stats; it currently lists the merge before the
lookup); delete scripts/R/testing/test_merge_individ_annual_location.rmd (superseded by
the contract tests); confirm no orphaned references to site_metadata.parquet remain in
docs.

Environment: R 4.6.0 via rv; plain Rscript, never --vanilla. Branch:
jo/merge-ch10-verify-docs. Done when: all six consumers run green with explained diffs,
and the documentation/retirement checklist above is complete.
```
