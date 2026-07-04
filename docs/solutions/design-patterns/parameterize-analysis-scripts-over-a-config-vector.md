---
title: Parameterize analysis scripts over a swept config dimension
date: 2026-06-15
category: design-patterns
module: scripts/R/09_analysis (intensive-margin analysis)
problem_type: design_pattern
component: development_workflow
severity: medium
applies_when:
  - "An analysis script must report results across several values of one parameter (radius, bandwidth, sample window, FE level)"
  - "You want robustness-check variants alongside the baseline without clobbering existing outputs"
  - "A script hardcodes the swept value in an output filename, LaTeX label, or table note"
related_components:
  - tooling
tags: [r, parameterization, purrr, analysis-scripts, output-naming, robustness-checks, tabularray]
---

# Parameterize analysis scripts over a swept config dimension

## Context

The intensive-margin analysis scripts each fixed a single house-to-overflow
[Near-Overflow Radius](../../../CONCEPTS.md) (`RAD <- 250L`), wrote fixed-name
LaTeX tables (`hedonic_count_continuous_prior.tex`), and hardcoded `"250m"` in
the captions, `\label{}`s, and notes. GitHub issue #15 asked to re-run the main
results at a 500m threshold (and we also wanted 1000m as a robustness check).
Re-running with a different `RAD` would have silently overwritten the 250m
tables and produced labels/notes that disagreed with the data — and there was no
clean way to keep all three radii side by side.

## Guidance

Turn the script into a loop over a configurable vector, and make every output
self-identify by the swept value.

1. **Config vector at the top** — the single control surface the reader edits:
   ```r
   # Radii (m) to run; edit to restrict.
   RADII <- c(250L, 500L, 1000L)
   ```
2. **Wrap the per-value body in a function, drive it with `purrr::walk`** —
   side-effecting iteration (we write files, we don't collect return values):
   ```r
   run_for_radius <- function(RAD) {
     # load cross-section at `RAD`, estimate, export ...
   }
   purrr::walk(RADII, run_for_radius)
   ```
   `walk` vs a plain `for` loop is stylistic; the load-bearing move is the
   function wrapper, which isolates each iteration (no repeated global-env
   mutation) and scopes intermediates locally.
3. **Stamp the swept value into every output** so nothing is clobbered and each
   artifact is self-describing:
   - filename: `paste0("hedonic_count_continuous_prior_", RAD, "m.tex")`
   - LaTeX label: `...-", RAD, "m}` (keeps labels unique if several values land in one document)
   - prose in notes: `paste0("... within ", RAD, "m of a storm overflow ...")`
4. **Hoist parameter-independent prep OUT of the loop** so it runs once. Inputs
   that don't depend on the swept value — property characteristics, the Google
   Trends series, cumulative article counts — load before the function; the
   closure reuses them.

## Why This Matters

- **No silent clobbering.** A fixed output name means the second run overwrites
  the first; the manuscript then shows whichever value ran last, with no signal.
  Value-stamped names let all variants coexist and make a stale file obvious.
- **Labels and notes can't drift from the data.** A hardcoded `"250m"` left in a
  500m table is a correctness bug in the manuscript that no test catches.
  Deriving the string from the parameter keeps prose and numbers in lockstep.
- **Robustness checks become a one-line edit.** Want only 500m? `RADII <- 500L`.
- **Cheaper reruns.** Hoisting invariant prep out of the loop avoids reloading
  large parquet inputs once per value.

## When to Apply

- Any analysis script that should report results across several values of one
  parameter (distance threshold, bandwidth, time window, fixed-effect level).
- Whenever you catch a script hardcoding the swept value in an output path, a
  `\label{}`, or a caption/note.

## Examples

**Before** — single value, fixed outputs (clobbers on rerun):
```r
RAD <- 250L
dat <- open_dataset(...) |> filter(radius == RAD) |> collect()
# ... estimate ...
writeLines(tbl, file.path(out, "hedonic_count_continuous_prior.tex"))  # fixed name
# notes hardcode "within 250m ..."; label hardcodes tbl:hedonic-...-prior
```

**After** — config vector + `walk` + stamped outputs, invariant prep hoisted:
```r
RADII <- c(250L, 500L, 1000L)
# radius-independent prep loaded ONCE here (property data, trends, articles) ...

run_for_radius <- function(RAD) {
  dat <- open_dataset(...) |> filter(radius == RAD) |> collect()
  # ... estimate ...
  notes <- paste0("... within ", RAD, "m of a storm overflow ...")
  tbl   <- sub("label=\\{[^}]*\\}",
               paste0("label={tbl:hedonic-count-continuous-prior-", RAD, "m}"), tbl)
  writeLines(tbl, file.path(out, paste0("hedonic_count_continuous_prior_", RAD, "m.tex")))
}
purrr::walk(RADII, run_for_radius)
```

**Adjacent sub-pattern (same robustness spirit): auto-fit wide tables instead of
hand-tuning font sizes.** A natural-width table can overflow the text margin when
columns or content grow, forcing manual font shrinking. In tabularray, convert
natural-width `Q[]` columns to elastic `X[c]` so the table always fills
`\linewidth` regardless of column count or font — keep the first (label) column
natural:
```r
table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)
table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)
```

## Related

- `docs/solutions/design-patterns/fit-wide-latex-regression-tables.md` — full treatment of the table auto-fit sub-pattern referenced above (`X[c]` for paper, `\resizebox` for slides).
- `docs/solutions/developer-experience/exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md` — same `05_news` scripts; `fixest` radius-bin context.
- `docs/solutions/developer-experience/exploratory-extensive-margin-news-notebook-plot-first-comparison-refactor-20260319.md` — config-driven comparison grid + radius-cap context.
- GitHub issue #15 — the motivating task (re-run intensive-margin results at a 500m threshold); this pattern satisfies it without clobbering the 250m outputs.
- `docs/solutions/design-patterns/analysis-results-static-html-reports.md` — the radius-suffixed artifacts this pattern produces are consumed by the radius-sweep results report (`scripts/python/build_intensive_margin_html_report.py`), which writes the swept radius generically as the buffer `B` in its specifications.
