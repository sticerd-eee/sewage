---
title: Static HTML result reports from generated analysis outputs
date: 2026-06-23
category: design-patterns
module: Analysis result reporting
problem_type: design_pattern
component: documentation
severity: medium
applies_when:
  - "An empirical analysis has already produced regression tables or model artifacts"
  - "A report should explain results without rerunning the analysis pipeline"
  - "The report needs coefficient-only summaries plus full tables for auditability"
related_components:
  - tooling
  - scripts/R/09_analysis
tags: [reports, html, regression-tables, model-outputs, verification, research-workflow]
---

# Static HTML result reports from generated analysis outputs

## Context

The windowed article-salience work produced new regression artifacts first, then
needed a reader-facing report analogous to the existing intensive-margin results
report. The useful workflow was to treat the generated LaTeX/model outputs as the
source of truth and build a static HTML report from them, rather than rerunning
analysis or editing model artifacts during report polish.

The prior windowed-salience session made this separation explicit: the report
plan said the current generated LaTeX tables were authoritative, the first
static report was verified with an HTML parser and spot checks, and later report
revisions adjusted presentation without modifying analysis scripts or outputs
(session history).

## Guidance

Start from an established report in `docs/reports/` when one exists. Reuse its
layout conventions, CSS, table density, and section ordering so new result
reports feel like part of the same research product instead of one-off pages.

Keep the report generation contract narrow:

1. **Generated outputs are the source of truth.** Read coefficients, standard
   errors, stars, captions, and notes from existing artifacts under
   `output/tables/` or equivalent generated-output directories.
2. **Do not rerun analysis while writing the report.** Report generation should
   not call the model scripts, alter regression objects, or rewrite LaTeX source
   artifacts. If the underlying numbers are wrong, fix and verify the analysis
   first, then regenerate the report from the corrected outputs.
3. **Lead with coefficient-only summary tables.** The first substantive section
   should show only the coefficient of interest and its standard error, grouped
   by market and specification. Put the specification panel, such as
   `Property controls + MSOA FE`, above the variable row so the reader sees both
   the identifying term and the model family.
4. **Name the variable of interest, not just the specification.** For intensive
   margin reports, label the summary row with the interaction such as
   `Daily spill count x log (Articles measure)`. For extensive margin reports,
   label it as `Near bin x log (Articles measure)`. The columns can then carry
   the salience measures: cumulative, 3m, 6m, 12m, or other variants.
5. **Put full regression tables below the summary.** Keep the summary scannable,
   then include full tables grouped by analysis section so a reader can audit
   controls, fixed effects, observations, and adjacent coefficients without
   leaving the report.
6. **Document the regression specifications before the full tables.** Include a
   compact equation and define each term: outcome, exposure, public-attention
   measure, interaction, property controls, location fixed effects, month fixed
   effects, and clustering level. Match the notation and rendering style of the
   reference report; if equations are complex, use a repeatable renderer such as
   MathJax rather than hand-written plain-text formulae.
7. **Promote to a builder script when hand-written HTML gets complex.** A static
   HTML file is fine for a small one-off report. Once the report needs repeated
   regeneration, large embedded tables, or systematic conversion from LaTeX,
   add a Python builder under `scripts/python/` and commit the generated report
   separately from the analysis scripts.
8. **Treat LaTeX parsing and escaping as part of the reporting contract.** If a
   builder patches `modelsummary` or `tabularray` output, prefer literal string
   splicing for inserted notes over regex replacement strings that can swallow
   backslashes. Add content checks for the commands, labels, and formula strings
   the report depends on.

## Why This Matters

Separating analysis from reporting prevents a presentation edit from quietly
changing model outputs. It also makes review easier: analysis commits can be
tested with R/model smoke runs, while report commits can be checked with static
HTML, parser, and content checks.

A coefficient-only summary protects the reader from hunting through wide
regression tables for the interaction that answers the question. The full tables
remain available below, so the report is both readable and auditable.

The variable label matters because `Property controls + LSOA FE` is not the
estimand. It tells the reader the model family; the interaction row tells the
reader what economic relationship is being summarized.

## When to Apply

- After any analysis branch has already produced stable regression tables,
  figures, or model-output artifacts.
- When a report compares robustness variants, salience measures, radii, windows,
  or fixed-effect specifications.
- When a report should be regenerated from existing artifacts but should not
  trigger a computationally expensive model rerun.
- When a static report is being committed alongside, but separately from,
  analysis scripts and generated outputs.

## Examples

Use this structure for result reports:

```text
Summary
  Intensive margin
    coefficient-only table:
      panel row: Property controls + MSOA FE
      variable row: Daily spill count x log (Articles measure)
      standard-error row
      panel row: Property controls + LSOA FE
      variable row: Daily spill count x log (Articles measure)
      standard-error row

  Extensive margin
    coefficient-only table:
      panel row: Property controls + MSOA FE
      variable row: Near bin x log (Articles measure)
      standard-error row
      panel row: Property controls + LSOA FE
      variable row: Near bin x log (Articles measure)
      standard-error row

Intensive margin
  specification block
  cumulative full regression table
  3m full regression table
  6m full regression table
  12m full regression table

Extensive margin
  specification block
  cumulative full regression table
  3m full regression table
  6m full regression table
  12m full regression table
```

Useful static checks after creating or regenerating the report:

```bash
python3 - <<'PY'
from html.parser import HTMLParser
from pathlib import Path
HTMLParser().feed(Path("docs/reports/report.html").read_text())
PY

python3 -m py_compile scripts/python/build_report.py

rg -n "Daily spill count|Near bin|Property controls \\+ LSOA FE" docs/reports/report.html
rg -n "Output files|Main read" docs/reports/report.html
git status --short
git diff --stat
```

For static HTML, also prefer HTML entities for signs and symbols when the report
is intended to stay ASCII-compatible: `&minus;`, `&times;`, `&ndash;`, and
`&nbsp;`.

## Related

- `docs/solutions/design-patterns/fit-wide-latex-regression-tables.md` --
  formatting pattern for wide modelsummary/tabularray outputs that often feed
  these reports.
- `docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md`
  -- companion pattern for producing the named robustness artifacts that reports
  summarize.
- `docs/solutions/design-patterns/output-specific-research-figure-sizing-and-verification.md`
  -- analogous principle for verifying figures in their final output context.
- `docs/solutions/developer-experience/exploratory-extensive-margin-news-notebook-render-stabilisation-20260319.md`
  -- adjacent HTML rendering and output-existence workflow for exploratory
  analysis notebooks.
- `docs/solutions/developer-experience/exploratory-extensive-margin-news-notebook-plot-first-comparison-refactor-20260319.md`
  -- adjacent lesson on making result reports readable before presenting the
  full audit trail.
- `docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html` --
  reference structure for coefficient summaries plus full tables.
- `docs/reports/2026-06-23-001-windowed-article-salience-results-report.html` --
  report created from generated cumulative/windowed article-salience outputs.
- GitHub issue #6 -- related regression-table reporting context for adding MSOA
  specifications to publicity tables.
