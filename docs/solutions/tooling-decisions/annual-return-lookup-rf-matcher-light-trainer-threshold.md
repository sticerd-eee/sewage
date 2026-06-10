---
title: "Annual return lookup RF probabilistic matcher: keep off, light trainer at threshold 0.90"
date: 2026-06-10
category: tooling-decisions
module: Annual Return Lookup
problem_type: tooling_decision
component: tooling
severity: medium
applies_when:
  - "Deterministic engine 1 leaves a residual of unmatched site-years and RF is considered to recover them."
  - "A new RF model is being trained on annual-return blocked pairs."
  - "The compute_rf_matching flag or saved model path is being modified."
  - "Shadow-run results show large counts of RF-only links in the final lookup."
  - "Memory or runtime budget for model training is a constraint."
tags:
  - annual-return
  - record-linkage
  - random-forest
  - probabilistic-matching
  - reclin2
  - ranger
  - tooling-decision
  - r
related_components:
  - annual-return-lookup
  - create-unique-spill-sites
  - record-linkage-graph
---

# Annual return lookup RF probabilistic matcher: keep off, light trainer at threshold 0.90

## Context

`scripts/R/03_data_enrichment/create_annual_return_lookup.R` links sewage-spill sites across
annual EDM returns 2021–2024 with two engines: (1) a deterministic "windfall" matcher that
requires exact 1:1 evidence agreement, and (2) an optional random-forest probabilistic matcher
(reclin2 + ranger) intended to rescue the leftovers. On 2026-06-10 (branch
`jo/annual_lookup_check`) the RF engine was evaluated end-to-end for the first time — match
attribution, a clean holdout, a no-partner stress test, and a shadow run on the real leftover
pool — and the results fixed its role going forward.

Why the evaluation was needed — the RF engine's history made its contribution unknowable:

- The old contract was **model-file-triggered**: `load_or_train_rf_model()` loaded any saved
  model file regardless of the `compute_rf_matching` flag (the flag only gated *training*).
  A 320 MB model trained 2025-04-15 sat at `output/code_testing/rf_model_2023_2024.rds`.
- The old loader also had a **save/load key mismatch** — saved as `list(model = m_rf, ...)`
  but read back via `$m_rf`, returning `NULL` — so cached-model loads silently *skipped* RF
  while looking like they ran it (session history). Net effect: whether RF contributed to any
  given historical lookup was not determinable without forensics.
- The April 2025 model was trained with the **poison-label bug**: pairs of two *different*
  known-matched sites were labeled as matches, corrupting the training set (fixed on this
  branch via `data.table::fifelse(match_id %in% TRUE, ...)`). Its match quality was never
  measured. Treat that model file as retired; never silently load it.

What didn't work along the way:

- **The production trainer at full blocking scope.** Blocking on `water_company` only
  generates 25,668,532 candidate pairs for 2023×2024; ranger grows all 500 trees but is
  SIGKILLed (exit 137) on a 16 GB machine during the OOB prediction-error pass. Scaling
  hardware is the wrong first response — downsample the training set instead.
- **Trusting the production threshold (0.05).** Recovery precision at 0.05 looks fine
  (0.999), but the no-partner decoy test shows a 4.2% false-link rate per site. The
  threshold must be set on the decoy test, not the recovery test.
- **Treating a high RF probability as confirmation of a true link.** Identifier-identical
  monitor rows score ≈ 1.0 because every feature agrees — that signals shared *works*
  identity, not the same *monitor* (see the companion convention doc below).

## Guidance

**Decision (2026-06-10): keep the RF engine, extracted to `scripts/R/utils/` and sourced only
when `compute_rf_matching = TRUE`; off in production; light trainer; default threshold 0.90.**
Implemented by `docs/plans/2026-06-10-004-refactor-annual-return-lookup-decomposition-plan.html`
(unit U4).

1. **Train with the light trainer, not the full pair set.** Keep ALL ground-truth positives
   (14,086 `unique_id` 2023↔2024 pairs), add hard negatives (non-matches with any name
   Jaro-Winkler ≥ 0.80 or any id-field JW ≥ 0.90), pad with ~10:1 random easy negatives.
   ~0.96M rows, trains in ~90 s under 3 GB with holdout quality equal to the full trainer.
   Build pairs per company block to bound peak RAM.

   ```r
   # sketch: light-trainer composition (per company block)
   positives <- pairs |> filter(label == 1)                     # all ground-truth links
   hard_neg  <- pairs |> filter(label == 0,
                                name_jw_max >= 0.80 | id_jw_max >= 0.90)
   easy_neg  <- pairs |> filter(label == 0,
                                name_jw_max < 0.80, id_jw_max < 0.90) |>
                slice_sample(n = 10 * nrow(positives))
   train_set <- bind_rows(positives, hard_neg, easy_neg)
   ```

2. **Re-validate the threshold after every retraining; never inherit it.** Negative
   undersampling inflates predicted probabilities (rare-events / King–Zeng logic), so the
   old 0.05 and the new 0.90 are not on the same scale. Choose the operating point from the
   **no-partner decoy test** (pools where every proposal is wrong by construction), because
   the real leftover pool is dominated by sites with no true partner.

   | threshold | recovery precision | recovery recall | false links per partner-less site |
   |---|---|---|---|
   | 0.05 (old production) | 0.999 | 0.999 | 4.2% |
   | 0.50 | 0.999 | 0.994 | 1.4% |
   | 0.90 (new default) | 1.000 | 0.941 | 0.3% |
   | 0.95 | 1.000 | 0.916 | 0.2% |

3. **Guard against identifier-identical groups.** Proposals whose evidence fields are all
   exactly equal and whose identifier group has multiple rows per year are monitor-multiples
   — flag them ambiguous, never link (they were 41 of the 50 links in the shadow run).

4. **Expose the ranger memory knobs** instead of hardcoding: `num.trees`, `max.depth`,
   `sample.fraction`, and `oob.error` (set `oob.error = FALSE` on large training sets — the
   OOB pass is what OOMs; the holdout harness measures quality instead).

5. **Run the evaluation harness before any production enablement** (holdout recovery +
   no-partner decoy; `scripts/R/testing/evaluate_rf_matching.R` once the refactor plan lands).
   Feature importance for orientation: site names dominate, then `permit_reference_ea`, NGR.

## Why This Matters

Engine 1 already links 96.4–99.9% of sites per year (15,430 canonical sites; 13,606 span all
four years; 774 singletons: 515/178/20/61 across 2021–2024, zero RF edges in production). The
shadow run on those real 774 leftovers proposed 52 links, 50 kept — decomposed: **41
identifier-identical monitor-multiples** (arbitrary pairings, see companion doc), **7
low-probability junk** admitted by the 0.05 threshold, and **~2 credible fuzzy matches**
("SLOANE STREET, RANELAGH GDNS, LONDON" ↔ "RANELAGH CSO", prob 0.57). A 2-in-774 genuine
yield does not justify always-on ML machinery, but the validated trainer and harness make RF
cheap insurance for 2025 data, where renamed/re-permitted sites may need fuzzy rescue.

## When to Apply

- Considering RF (or any probabilistic matcher) for annual-return leftovers: run the
  identifier-duplicate diagnostic first; most "unmatched" 2021/2022 rows are untrackable
  monitor-multiples, not fuzzy-match candidates.
- Enabling RF for a new year pair: (a) retrain with the light trainer, (b) re-validate the
  threshold on a fresh holdout + decoy test, (c) apply the ambiguity guard, (d) review the
  proposed pairs by eye before merging.
- Touching `compute_rf_matching`, the model path, or the trainer: RF must stay strictly
  flag-controlled — a model file on disk must never activate it.

## Examples

Shadow-run decomposition — what RF's 50 kept links actually were:

| Category | Count | Example |
|---|---|---|
| Identifier-identical monitor-multiples (prob ≥ 0.9) | 40 | Same works, different monitors — not a real link |
| Identifier-identical monitor-multiples (prob < 0.9) | 1 | Same category, borderline score |
| Junk proposals (prob < 0.5) | 7 | "PAKENHAM ROAD CSO" ↔ "EDGBASTON - WHEELEYS RD (CSO)", prob 0.11 |
| Credible fuzzy matches | 2 | "SLOANE STREET, RANELAGH GDNS, LONDON" ↔ "RANELAGH CSO", prob 0.57 |

Operational gotcha (session history): `Rscript --vanilla` bypasses `.Rprofile` and therefore
the `rv`-managed project library — packages like `arrow` vanish. Use plain `Rscript` (or `rv`)
when running pipeline scripts or tests.

## Related

- `docs/solutions/conventions/annual-return-rows-are-monitor-level-not-works-level.md` —
  companion convention; explains the 41 artifact links and the ambiguity guard.
- `docs/solutions/logic-errors/annual-return-lookup-same-year-component-conflicts.md` —
  the graph-resolution invariant the RF engine feeds into.
- `docs/plans/2026-06-10-004-refactor-annual-return-lookup-decomposition-plan.html` —
  refactor plan; unit U4 implements this decision, U8 cross-checks documentation.
- `todos/009-pending-p1-audit-annual-return-lookup-construction.md` — the audit that opened
  the RF investigation; this doc resolves its RF sub-items.
- `todos/_archive/008-pending-p1-fix-annual-return-lookup-rf-and-conflict-bugs.md` — the RF
  bug inventory (load-key mismatch, poison labels, empty-tibble contract) behind this decision.
- Evaluation artifacts (ephemeral, session of 2026-06-10): `/tmp/ce_rf_eval/` — light-trainer
  implementation, holdout/decoy logs, reviewable pair list (`rf_review_pairs.xlsx`). The
  designs are reproduced in the refactor plan if these are gone.
