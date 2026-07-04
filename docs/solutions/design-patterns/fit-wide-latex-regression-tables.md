---
title: "Fit wide LaTeX regression tables: X[c] columns for paper, call-site resizebox for slides"
date: 2026-06-15
last_updated: 2026-06-28
category: design-patterns
module: "LaTeX table output (modelsummary/tabularray; scripts/R/09_analysis; paper + slides)"
problem_type: design_pattern
component: documentation
severity: medium
applies_when:
  - "A wide modelsummary/tabularray regression table overflows the text margin"
  - "You are hand-tuning font size or colsep by trial and error to make a table fit"
  - "The same post-processing idiom is copy-pasted across many modelsummary scripts and has drifted"
  - "Coefficient precision is set inconsistently across scripts, or a rescale collapses cells to 0.00"
  - "Deciding how to fit a table in the paper (talltblr) versus on a beamer slide (tabular)"
related_components:
  - tooling
  - testing_framework
tags: [latex, tabularray, modelsummary, table-formatting, dry, precision, resizebox, beamer]
---

# Fit wide LaTeX regression tables: X[c] columns for paper, call-site resizebox for slides

## Context

`modelsummary` emits wide regression tables (12+ columns for our sales+rentals
panels) as a tabularray `talltblr` whose columns are natural-width `Q[]`. With
that many columns the table runs past `\textwidth`. The instinct is to shrink
`\fontsize` and `colsep` by trial and error until it fits — fragile, and it
breaks again whenever a column or a longer number is added. A second trap is
reaching for `\resizebox` on the `talltblr`, which misbehaves (see below).

Worse, the fix below started life **copy-pasted** into every table script (and a
single file would carry the idiom three or four times, once per emitted table),
so changing the fit policy meant editing dozens of identical blocks. As of commit
`cd4864f` this is all centralized in one helper (see Guidance). The weekly-spill
rescale forced the issue: dividing every coefficient by 7 pushes terms into the
third decimal (`-0.04` → `-0.006`), so at 2-dp formatting those cells collapse to
`0.00` — a project-wide precision sweep was needed, and a sweep is the right moment
to hoist the duplicated fit logic into one place. See
[rescale-regressor-at-source-for-interpretable-units.md](rescale-regressor-at-source-for-interpretable-units.md).

## Guidance

**Width is a column-spec problem, not a font problem — and the fit logic belongs in
one shared helper, not copy-pasted into every script.**

Define the precision setting and the tabularray post-processor **once** in
`scripts/R/09_analysis/utils_table_formatting.R`; every table script `source()`s it
and routes its LaTeX through it. The module exports:

- `fmt_table <- modelsummary::fmt_decimal(3)` — the single precision setting
  (uniform 3 dp across all modelsummary tables; change depth in one place). Pass
  `fmt = fmt_table` at every call site.
- `fit_tblr_latex(latex, ..., notes, label)` — the post-processor that, in one call,
  inserts `[H]` float placement, splices a `label` into the caption block, converts
  `Q[]` → `X[c]` (keeping the label column `l`), **counts the resulting `X[c]`
  columns and applies an adaptive font/colsep tier**, splices custom `notes`, scrubs
  `-0.000` → `0.000`, and **`stop()`s loudly** if any expected anchor (caption
  block, inner-open marker, note block) or the `\fontsize`/`\selectfont` commands
  are missing.
- `adaptive_tblr_layout(n_x_cols)` — column-count tiers: `≥10 → 8pt / colsep 2pt`;
  `7–9 → 9pt / 3pt`; `≤6 → 11pt / 4pt`. Font is derived purely from column count, so
  there are **zero hand-tuned font overrides** anywhere.

The two pre-existing shared post-processors (`utils_radius_robustness_table.R`,
`extensive_margin_news_utils.R::patch_modelsummary_latex()`) were refactored to
**delegate** to `fit_tblr_latex()` rather than carry their own copy, so there is
exactly one implementation — proven to work for both the `output = "latex"` path and
the tinytable file-roundtrip path. Tests live in `tests/test_table_formatting.R`
(plain sourced script, not testthat): synthetic `Q[]` fixtures assert the tier
selection at 6/8/12 columns, the override path, the `-0.000` scrub, the
missing-anchor `stop()`, and that `fmt_table` yields 3-dp strings (blanking `NA`).

The mechanics the helper now owns, unchanged in principle:

1. **Paper (`talltblr`): auto-fit with elastic `X[c]` columns.** Convert the
   natural-width `Q[]` columns modelsummary emits into elastic `X[c]` columns so
   tabularray sizes the table to exactly `\linewidth`; keep the first (label)
   column natural (`l`). Post-process the generated LaTeX string:
   ```r
   table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)
   table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)
   ```
   The table now fills the line width and cannot overflow, regardless of column
   count or font size.

2. **Do NOT `\resizebox` a `talltblr`/`longtblr`.** In these environments the
   caption and notes live *inside* the environment (the `caption={}` and
   `note{}=` keys). Wrapping the whole thing in `\resizebox{\linewidth}{!}{...}`
   scales the caption and notes along with the table body (and defeats the
   environment's page-breaking). `\resizebox` is only appropriate for a plain
   `tabular` whose caption/notes sit *outside* it.

3. **Font and colsep are density levers, not the fit mechanism — and they are now
   chosen automatically from column count.** Once `X[c]` owns the width, a smaller
   font keeps numbers from overflowing the now-narrow columns. `fit_tblr_latex()`
   counts the `X[c]` it just produced and picks the `\fontsize`/`colsep` tier, so
   you never hand-tune (and never re-introduce the drift where same-width tables
   ended up at different sizes). Pass an explicit `cell_font`/`colsep` only to
   override.

4. **Slides (plain `tabular`): use call-site `\resizebox`.** The beamer-table
   converter (`scripts/python/convert_paper_tables_to_beamer.py`) rewrites the
   `talltblr` into a plain `tabular` fragment. Slide decks then size each table
   with `\inputslidetable[<x>]{...}`, which expands to
   `\resizebox{<x>\linewidth}{!}{...}` and defaults to `x = 1`.

## Why This Matters

- **`X[c]` guarantees fit with zero font-guessing** and survives adding columns
  or longer entries; hand-tuned `\fontsize` is a moving target that silently
  overflows again the next time the table grows.
- **One edit changes every table.** Precision depth, float placement, column-fit
  policy, and font tiering live in one file. Bumping 3 dp → 4 dp, or retuning the
  font breakpoints, is a single-line change instead of a 50-site sweep.
- **Eliminates silent drift.** Hand-tuned fonts had already diverged (same column
  count, different sizes; some cramped). A column-count rule makes styling
  deterministic — every 12-column table is 8pt, every 6-column table is 11pt, no
  special cases.
- **Fail-loud beats silent no-op.** The old inline paths had no validation and
  would silently skip a patch if an anchor was missing, shipping an unstyled or
  unlabeled table. The helper asserts the font commands landed and `stop()`s on a
  missing anchor, so a malformed modelsummary upgrade surfaces immediately.
- **The resizebox-on-`talltblr` trap looks broken.** It shrinks or enlarges the
  caption and notes with the body, so they no longer match surrounding text.
  The fix is not a better resizebox call — it is knowing the environment
  boundary: `talltblr`/`longtblr` keep caption+notes inside (use `X[c]`), plain
  `tabular` keeps them outside (use `\resizebox`).

## When to Apply

- Any wide tabularray table in the paper, especially `modelsummary` regression
  output with many model columns.
- Whenever you find yourself adjusting a font size to make a table fit — switch
  to `X[c]` instead.
- When deciding how a table should fit in the paper versus on a slide.

## Examples

At a call site — pass `fmt_table`, then route the LaTeX through the helper. The
~25-line inline post-processing block collapses to one call (plus any genuinely
script-specific touches):

```r
source(here::here("scripts", "R", "09_analysis", "utils_table_formatting.R"))

tab <- modelsummary::modelsummary(models, fmt = fmt_table, coef_map = coef_labels,
                                  output = "latex", notes = " ")
tab <- fit_tblr_latex(tab, label = "tbl:hedonic-count-continuous-prior", notes = custom_notes)
# do NOT pass cell_font -> let the adaptive rule pick the size from column count
# script-specific touches stay local, e.g.:
tab <- gsub("Upstream &", "\\\\quad Upstream &", tab)
writeLines(tab, output_path)
```

The `fmt_decimal(2)` → `fmt_table` swap simultaneously deepens every cell to 3 dp,
and dropping the hard-coded `11pt` lets the adaptive rule choose the size.

Under the hood, the one-time column-spec rewrite the helper performs to make a
table auto-fit:
```r
# modelsummary emits: colspec={Q[]Q[]...Q[]}  (natural width -> can overflow)
table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)            # all columns elastic
table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)  # label col natural
# result: colspec={l X[c] X[c] ... }  -> fills \linewidth exactly
```

Same table family, two environments, two fit mechanisms:

| Output | Environment | Caption/notes | Fit mechanism |
|--------|-------------|---------------|---------------|
| Paper  | `talltblr`  | inside env    | `X[c]` elastic columns |
| Slide  | plain `tabular` | outside env | `\inputslidetable[<x>]{...}` call-site resizing |

## Related

- `scripts/R/09_analysis/utils_table_formatting.R` — the shared helper (`fmt_table`, `fit_tblr_latex`, `adaptive_tblr_layout`) that is now the canonical implementation of everything above.
- `tests/test_table_formatting.R` — tier-selection, override, `-0.000` scrub, and missing-anchor `stop()` tests for the helper.
- [rescale-regressor-at-source-for-interpretable-units.md](rescale-regressor-at-source-for-interpretable-units.md) — the weekly-spill rescale that forced the 3-dp precision standard.
- `docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md` — companion pattern; mentions the `X[c]` rewrite as an adjacent sub-pattern.
- `docs/plans/2026-06-26-001-feat-table-fit-and-precision-plan.md` — plan with the full keep/trade-off/decision rationale (uniform 3 dp, adaptive font tiers, rejected alternatives).
- `scripts/python/convert_paper_tables_to_beamer.py` — converts paper `talltblr` tables into plain slide `tabular` fragments (ignores colspec/font, so the helper's changes are transparent to it).
