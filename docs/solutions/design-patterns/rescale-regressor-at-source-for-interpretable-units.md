---
title: "Rescale a regressor at the source so fixest reports scaled coefficients automatically"
date: 2026-06-28
category: design-patterns
module: "Spill exposure analysis (06_analysis_datasets construction + 09_analysis reporting)"
problem_type: design_pattern
component: tooling
severity: medium
applies_when:
  - "A reported coefficient is on an awkward unit (e.g. daily average) and you want a more interpretable one (e.g. per week)"
  - "The rescale is a constant linear factor on the regressor (w = c * d)"
  - "You want the scaled coefficient, SE, CI, stars, N, and R-squared without hand-computing the transform across many tables"
  - "The same regressor flows through level terms, interactions, and derived aggregates in several analysis areas"
related_components:
  - documentation
tags: [r, fixest, modelsummary, units-rescaling, regressors, interpretability, data-construction]
---

# Rescale a regressor at the source so fixest reports scaled coefficients automatically

## Context

Sewage-spill exposure was measured with daily-average regressors:
`spill_count_daily_avg` (events/day) and `spill_hrs_daily_avg` (hours/day) at the
overflows near a property, each built as `total / n_days_in_window`. Every
continuous coefficient was therefore "per +1 daily-average unit" — one extra
spill *per day, every day* — which is technically correct but far outside the
support of the data and unintuitive for readers. This was raised as
[issue #20](https://github.com/sticerd-eee/sewage/issues/20), "Fix interpretation
of main spill coefficient."

Candidate reporting units were per-year, per-month, and per-week. **Per-week** was
chosen because it is close to one standard deviation and intuitive — but only
after the "≈1 SD" claim was actually verified numerically on the
fixed-effect-retained sample (fixest drops singletons, so the SD must be computed
on the same observations the coefficient is estimated from): sales SD ≈ 0.126
spills/day so one spill/week (1/7 ≈ 0.1429) ≈ 1.14 SD; rentals ≈ 1.23 SD at 250 m.
(session history)

The naive way to produce per-week numbers is to fit the daily model and divide the
printed coefficient by 7 by hand. That is fragile: you must also divide the SE and
both CI bounds by 7, leave the t-stat / p-value / stars / N / R² untouched, and
redo all of it for every level term *and* every interaction (`spill:post`,
`spill:log(Articles)`, `spill:direction`), in every table.

## Guidance

**Don't rescale the *output*. Rescale the *regressor* once, at the data source, and
let `fixest`/`modelsummary` emit everything already-scaled.**

Build the weekly regressor as `daily_avg * 7` in the same `:=` that creates the
daily average, store **both** columns, then point the reporting formulas at the
weekly column:

```r
# In each analysis-dataset builder (scripts/R/06_analysis_datasets/*.R):
result[, `:=`(
  spill_count_daily_avg  = spill_count / n_days_in_window,
  spill_hrs_daily_avg    = spill_hrs   / n_days_in_window,
  spill_count_weekly_avg = (spill_count / n_days_in_window) * 7,
  spill_hrs_weekly_avg   = (spill_hrs   / n_days_in_window) * 7
)]
```

In reporting scripts the change is a variable swap in the formula plus matching
`coef_map`/label/notes keys — no arithmetic on fitted values:

```r
# Before: feols(log_price ~ spill_count_daily_avg  * direction + ...)
# After:  feols(log_price ~ spill_count_weekly_avg * direction + ...)
coef_labels_count <- c(spill_count_weekly_avg = "Spills per week (avg.)")
```

**The math that makes this exact.** If `w = 7 * d` then `y = β_d·d = (β_d/7)·w`, so
the coefficient on `w` is exactly `β_d / 7` — the per-spill/week effect — and fixest
computes the scaled SE, CI, stars, p-values, N, and R² automatically. The scale
propagates through interactions and through derived aggregates because the
underlying *column* carries it.

**Keep the daily column.** It stays the canonical exposure for
`book/scaled_effects.qmd` (which keeps its 1-SD interpretation) and any
non-reporting use. The dataset carries both; only reporting models switch to weekly.

**Invariants** (a temporary `validate_spill_rescaling.R` asserted these, then was
deleted once passing — validation steps are temporary by design):

| Quantity | Relationship to daily |
|---|---|
| Coefficient β | `β_week = β_daily / 7` (exact, ≤ 1e-6 rel. error) |
| Std. error | `SE_week = SE_daily / 7` |
| 95% CI bounds | both `× 1/7` |
| t-stat, p-value, significance stars | **unchanged** |
| Observations (N), R² / adj. R² | **unchanged** |
| Coefficient sign | **unchanged** |
| % price effect | `100 * (exp(β_week) - 1)` — one exponentiation of the reported coef, no `/7` buried inside |
| Non-spill coefficients | **unchanged** |

## Why This Matters

- **Correctness by construction.** Doing the transform through the regressor makes
  it one factor in one place; fixest derives every downstream quantity
  consistently. There is no way to scale the coefficient but forget the SE.
- **Interactions and aggregates come along for free.** `spill:post`,
  `spill:log(Articles)`, `spill:direction`, and summed
  `upstream_count`/`downstream_count` aggregates all inherit the scale because they
  are built from the rescaled column — no per-term bookkeeping.
- **Tables are emitted directly.** `modelsummary` reads the already-scaled
  coefficient/SE/CI/stars/N/R² straight from the fitted model. No "rescale this
  cell" post-processing layer, which would be a second place for units to drift.
- **Readable contrasts.** "+1 spill/week" is an on-support change (~1.1–1.2 SD)
  where "+1 spill/day" was far off-support, so the reported numbers describe a
  realistic intervention.

## When to Apply

- A coefficient needs reporting in different units and the rescale is a **constant
  linear factor** on the regressor (`w = c * d`).
- You need SE, CI, significance, N, and R² to stay mutually consistent with the
  reported coefficient (essentially always for a regression table).
- The same rescaled term appears as a level term, in interactions, and/or in
  derived aggregates — the source-level approach scales all of them at once.

Do **not** use this for nonlinear reparameterizations, for categorical/bin terms
(rescaling a continuous unit is meaningless there — keep them daily), or where a
downstream consumer legitimately needs the original unit (keep both columns and let
consumers pick).

## Examples

**Construction site** (`cross_section_prior_to_sale.R` and three sibling builders,
commit `006630a`) — two `* 7` lines added directly beneath the daily averages:

```r
result[, `:=`(
  spill_count_daily_avg  = spill_count / n_days_in_window,
  spill_hrs_daily_avg    = spill_hrs   / n_days_in_window,
  spill_count_weekly_avg = (spill_count / n_days_in_window) * 7,   # added
  spill_hrs_weekly_avg   = (spill_hrs   / n_days_in_window) * 7    # added
)]
```

**Derived directional aggregate** (`upstream_downstream_prior.R`) — the regressor
name is unchanged, only the summed column flips daily → weekly, so `coef_map` keys
are untouched and only labels change:

```r
# Before:
upstream_count = sum(spill_count_daily_avg  * (direction == 1), na.rm = TRUE),
# After:
upstream_count = sum(spill_count_weekly_avg * (direction == 1), na.rm = TRUE),
```

The result: a hedonic coefficient and its SE both move to exactly 1/7 of the daily
values, while stars, Observations, and R² are byte-for-byte identical to the daily
run. The SE collapsing into the third decimal (`0.01` → `0.001`) is precisely what
motivated the uniform 3-dp precision change — see
[fit-wide-latex-regression-tables.md](fit-wide-latex-regression-tables.md).

## Pitfalls (from implementation, session history)

- **Don't treat "≈1 SD" as a premise.** The original assistant assumed it; the user
  caught it and required a numeric check on the FE-retained sample.
- **Rescale spill *hours* identically.** They were initially left out of scope and
  had to be folded back in (`spill_hrs_weekly_avg = spill_hrs_daily_avg * 7`).
- **Move `coef_map` / `coef_labels` *keys*, not just labels.** modelsummary
  silently drops the spill row (no error) if the key still names the daily term.
- **Use whole-word substitution** (`\bspill_count_daily_avg\b`) so categorical
  companions like `spill_count_daily_avg_bin` keep their daily values — per-week
  scaling applies to continuous terms only.
- **Audit literal "Daily spill" headers and "per day" notes inside scripts.** A
  Phase-2 prose grep over the manuscript skips R scripts, so script-embedded text
  needs a separate pass.
- **Watch shared effect-size utils.** `windowed_article_effect_size_utils.R`
  hard-codes `spill_count_daily_avg` and computes effects using `sd(daily)`; any
  rename must specify whether it should use daily or weekly SD.

## Related

- [fit-wide-latex-regression-tables.md](fit-wide-latex-regression-tables.md) — the
  3-dp precision standard that this rescale forced (shrunk coefficients collapse at
  2 dp).
- `docs/plans/2026-06-24-002-feat-rescale-spill-effect-plan.md` — source plan with
  the full scaling table, invariants, and adversarial-review findings.
- [issue #20](https://github.com/sticerd-eee/sewage/issues/20) — "Fix interpretation
  of main spill coefficient" (the motivating ticket).
</content>
