---
title: "Fit wide LaTeX regression tables: X[c] columns for paper, call-site resizebox for slides"
date: 2026-06-15
category: design-patterns
module: LaTeX table output (modelsummary/tabularray; paper + slides)
problem_type: design_pattern
component: documentation
severity: medium
applies_when:
  - "A wide modelsummary/tabularray regression table overflows the text margin"
  - "You are hand-tuning font size or colsep by trial and error to make a table fit"
  - "Deciding how to fit a table in the paper (talltblr) versus on a beamer slide (tabular)"
related_components:
  - tooling
tags: [latex, tabularray, talltblr, modelsummary, table-formatting, resizebox, beamer]
---

# Fit wide LaTeX regression tables: X[c] columns for paper, call-site resizebox for slides

## Context

`modelsummary` emits wide regression tables (12+ columns for our sales+rentals
panels) as a tabularray `talltblr` whose columns are natural-width `Q[]`. With
that many columns the table runs past `\textwidth`. The instinct is to shrink
`\fontsize` and `colsep` by trial and error until it fits — fragile, and it
breaks again whenever a column or a longer number is added. A second trap is
reaching for `\resizebox` on the `talltblr`, which misbehaves (see below).

## Guidance

**Width is a column-spec problem, not a font problem.** Handle it structurally,
then use font/colsep only for density.

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

3. **Font and colsep are density levers, not the fit mechanism.** Once `X[c]`
   owns the width, set a small `cells = {font = \fontsize{8pt}{9pt}\selectfont}`
   and `colsep=2pt` for readability and whitespace — not to prevent overflow.

4. **Slides (plain `tabular`): use call-site `\resizebox`.** The beamer-table
   converter (`scripts/python/convert_paper_tables_to_beamer.py`) rewrites the
   `talltblr` into a plain `tabular` fragment. Slide decks then size each table
   with `\inputslidetable[<x>]{...}`, which expands to
   `\resizebox{<x>\linewidth}{!}{...}` and defaults to `x = 1`.

## Why This Matters

- **`X[c]` guarantees fit with zero font-guessing** and survives adding columns
  or longer entries; hand-tuned `\fontsize` is a moving target that silently
  overflows again the next time the table grows.
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

Paper table — the one-time column-spec rewrite that makes it auto-fit:
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

- `docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md` — companion pattern; mentions the `X[c]` rewrite as an adjacent sub-pattern (this doc is the full treatment).
- `scripts/python/convert_paper_tables_to_beamer.py` — converts paper `talltblr` tables into plain slide `tabular` fragments.
