#!/usr/bin/env python3
"""Build the windowed article-salience HTML report from published LaTeX tables.

The report compares cumulative LexisNexis article coverage with trailing
3-, 6-, and 12-month article-count measures for the public-attention
specifications. It reuses the table parser and styling from
``build_intensive_margin_html_report.py``; specification equations are emitted
as TeX and rendered in-browser by MathJax. Regression estimates are not re-run
here; this script only reads the existing ``output/tables/*.tex`` artifacts and
writes the HTML report.

Usage:
    python3 scripts/python/build_windowed_article_salience_html_report.py

Output:
    docs/reports/2026-06-23-001-windowed-article-salience-results-report.html
"""

from __future__ import annotations

import html
import re
from pathlib import Path

import build_intensive_margin_html_report as base

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = REPO_ROOT / "output" / "tables"
OUTPUT_PATH = (
    REPO_ROOT
    / "docs"
    / "reports"
    / "2026-06-23-001-windowed-article-salience-results-report.html"
)

REPORT_DATE = "2026-06-23"
REPORT_AUTHOR = "Jacopo Olivieri"

MEASURES = [
    ("Cumulative", "did_articles_prior_250m.tex"),
    ("3m", "did_articles_windowed_prior_3m.tex"),
    ("6m", "did_articles_windowed_prior_6m.tex"),
    ("12m", "did_articles_windowed_prior_12m.tex"),
]

EXTENSIVE_MEASURES = [
    ("Cumulative", "did_articles_prior_extensive.tex"),
    ("3m", "did_articles_windowed_prior_extensive_3m.tex"),
    ("6m", "did_articles_windowed_prior_extensive_6m.tex"),
    ("12m", "did_articles_windowed_prior_extensive_12m.tex"),
]

EQUATIONS = {
    "article_salience": (
        r"\begin{aligned}"
        r"A_t^{\mathrm{cum}}&=\log\!\left(\sum_{\tau=t_0}^{t}N_\tau\right),"
        r"&A_t^{(w)}&=\log\!\left(\sum_{\tau=t-w+1}^{t}N_\tau\right),"
        r"\quad w\in\{3,6,12\},\\[2pt]"
        r"A_t^a&\in\{A_t^{\mathrm{cum}},A_t^{(3)},A_t^{(6)},A_t^{(12)}\}"
        r"\end{aligned}"
    ),
    "windowed_intensive": (
        r"\log p_{it}=\alpha+\beta\,\overline{S}^{250}_{it}"
        r"+\kappa\bigl(\overline{S}^{250}_{it}\times A_t^a\bigr)"
        r"+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}"
    ),
    "windowed_extensive": (
        r"\log p_{it}=\alpha+\phi\,\mathrm{Near}_{i}"
        r"+\kappa\bigl(\mathrm{Near}_{i}\times A_t^a\bigr)"
        r"+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}"
    ),
}

base.EQUATIONS.update(EQUATIONS)

WHERE_DEFS = {
    "article_salience": [
        ("<em>N</em><sub>&tau;</sub>", "LexisNexis sewage-spill article count in month &tau;"),
        (
            "<em>A</em><sup>cum</sup><sub>t</sub>",
            "log cumulative article coverage from January 2021 through transaction month <em>t</em>",
        ),
        (
            "<em>A</em><sup>(w)</sup><sub>t</sub>",
            "log trailing-window coverage over months <em>t-w+1</em> through <em>t</em>, for <em>w</em> = 3, 6, or 12",
        ),
        (
            "<em>A</em><sup>a</sup><sub>t</sub>",
            "one of the four salience measures used in the comparison: cumulative, 3m, 6m, or 12m",
        ),
    ],
    "windowed_intensive": [
        (
            "<em>p</em><sub>it</sub>",
            "sale price or weekly asking rent for transaction <em>i</em> in month <em>t</em>",
        ),
        (
            '<span class="ov">S</span><sup>250</sup><sub>it</sub>',
            "average daily spill events across overflows within 250&nbsp;m, January 2021 to the transaction date",
        ),
        (
            "<em>&kappa;</em>",
            "coefficient of interest: how article salience changes the spill-price gradient",
        ),
        (
            "<strong>X</strong><sub>i</sub>, <em>&delta;</em><sub>&ell;(i)</sub>, <em>&lambda;</em><sub>m(t)</sub>",
            "property controls, location fixed effects, and transaction-month fixed effects",
        ),
        (
            "salience main effect",
            "included in no-FE columns and absorbed when transaction-month fixed effects are present",
        ),
    ],
    "windowed_extensive": [
        (
            "<em>p</em><sub>it</sub>",
            "sale price or weekly asking rent for transaction <em>i</em> in month <em>t</em>",
        ),
        (
            "Near<sub>i</sub>",
            "indicator equal to one for properties 0-500&nbsp;m from the nearest mapped overflow, with 1000-2000&nbsp;m as the far bin",
        ),
        (
            "<em>&kappa;</em>",
            "coefficient of interest: how article salience changes the near-far price gap",
        ),
        (
            "<strong>X</strong><sub>i</sub>, <em>&delta;</em><sub>&ell;(i)</sub>, <em>&lambda;</em><sub>m(t)</sub>",
            "property controls, location fixed effects, and transaction-month fixed effects",
        ),
        (
            "salience main effect",
            "included in no-FE columns and absorbed when transaction-month fixed effects are present",
        ),
    ],
}


def _postprocess_table_math(rendered: str) -> str:
    """Restore simple subscript display in parsed LaTeX table labels and notes."""
    for suffix in ("3m", "6m", "12m"):
        rendered = rendered.replace(
            f"Articles_{{{suffix}}}", f"Articles<sub>{suffix}</sub>"
        )
    return rendered


def _equation_html(key: str) -> str:
    tex = html.escape(base.EQUATIONS[key])
    return f'    <div class="eqn mathjax">\\[\n{tex}\n\\]</div>'


def _where_html(key: str) -> str:
    items = "\n".join(
        f"        <li>{symbol} &mdash; {description}</li>"
        for symbol, description in WHERE_DEFS[key]
    )
    return (
        '    <div class="eqn-where"><span class="where-lead">where</span>\n'
        f"      <ul>\n{items}\n      </ul>\n"
        "    </div>"
    )


def _equations_html(keys: list[str]) -> str:
    parts: list[str] = []
    for key in keys:
        parts.append(_equation_html(key))
        parts.append(_where_html(key))
    return "\n".join(parts)


def _measure_caption(parsed_caption: str, measure: str) -> str:
    return f"{html.escape(measure)} &mdash; {parsed_caption}"


def render_measure_table(filename: str, measure: str) -> str:
    path = TABLES_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing table: {path}")

    parsed = base.parse_table(path)
    rendered = base.render_table(parsed, f"{Path(filename).stem}-measure.tex")
    old = f"<figcaption>{parsed['caption']}</figcaption>"
    new = f"<figcaption>{_measure_caption(parsed['caption'], measure)}</figcaption>"
    rendered = rendered.replace(old, new, 1)
    return _postprocess_table_math(rendered)


def _find_interaction_row(parsed: dict, label_fragment: str) -> tuple[list[str], list[str]]:
    body = parsed["body"]
    for idx, row in enumerate(body):
        raw_label = row[0] if row else ""
        if "\\times" in raw_label and label_fragment in raw_label:
            if idx + 1 >= len(body):
                raise ValueError(f"Interaction row has no standard-error row: {raw_label}")
            return row, body[idx + 1]
    raise ValueError(f"Could not find interaction row containing: {label_fragment}")


def _summary_cells(files: list[tuple[str, str]], label_fragment: str, fe: str) -> tuple[list[str], list[str]]:
    col_idx = {
        "msoa": (4, 10),
        "lsoa": (6, 12),
    }[fe]

    sales: list[str] = []
    rentals: list[str] = []
    sales_se: list[str] = []
    rentals_se: list[str] = []

    for _, filename in files:
        parsed = base.parse_table(TABLES_DIR / filename)
        coef_row, se_row = _find_interaction_row(parsed, label_fragment)
        sales.append(base.clean_cell(coef_row[col_idx[0]]))
        sales_se.append(base.clean_cell(se_row[col_idx[0]]))
        rentals.append(base.clean_cell(coef_row[col_idx[1]]))
        rentals_se.append(base.clean_cell(se_row[col_idx[1]]))

    return sales + rentals, sales_se + rentals_se


def render_summary_table(kind: str, files: list[tuple[str, str]], row_label: str, note: str) -> str:
    table_id = f"summary-{kind}-table"
    caption = (
        "Intensive margin: daily spill count &times; article salience"
        if kind == "intensive"
        else "Extensive margin: near bin &times; article salience"
    )
    fragment = "Daily spill count" if kind == "intensive" else "Near bin"
    measures = [measure for measure, _ in files]
    measure_headers = "".join(f'<th scope="col">{html.escape(m)}</th>' for m in measures)

    rows: list[str] = []
    for fe, fe_label in [
        ("msoa", "Property controls + MSOA FE"),
        ("lsoa", "Property controls + LSOA FE"),
    ]:
        coef_cells, se_cells = _summary_cells(files, fragment, fe)
        coef_html = "".join(f"<td>{cell}</td>" for cell in coef_cells)
        se_html = "".join(f"<td>{cell}</td>" for cell in se_cells)
        rows.extend(
            [
                f'      <tr class="panel-row"><th class="panel-head" colspan="9">{fe_label}</th></tr>',
                f'      <tr><th scope="row" class="rowlab">{row_label}</th>{coef_html}</tr>',
                f'      <tr class="statrow"><th scope="row" class="rowlab"></th>{se_html}</tr>',
            ]
        )

    return f"""  <figure class="tbl" id="{table_id}">
    <figcaption>{caption}</figcaption>
    <div class="table-wrap">
    <table>
      <thead>
      <tr><th scope="col"></th><th scope="col" class="spanner" colspan="4">House Sales</th><th scope="col" class="spanner" colspan="4">House Rentals</th></tr>
      <tr><th scope="col"></th>{measure_headers}{measure_headers}</tr>
      </thead>
      <tbody>
{chr(10).join(rows)}
      </tbody>
    </table>
    </div>
    <p class="tnote">{note}</p>
  </figure>"""


def build_toc() -> str:
    return """    <nav class="toc">
      <strong>Contents</strong>
      <ol>
      <li><a href="#summary">Summary</a>
        <ol>
        <li><a href="#summary-intensive">Intensive margin</a></li>
        <li><a href="#summary-extensive">Extensive margin</a></li>
        </ol>
      </li>
      <li><a href="#article-salience">Article salience measures</a></li>
      <li><a href="#intensive-margin">Intensive margin</a></li>
      <li><a href="#extensive-margin">Extensive margin</a></li>
      </ol>
    </nav>"""


def build_summary_html() -> str:
    intensive_note = (
        "<strong>Notes:</strong> Each cell reports the coefficient of interest from "
        "the stated fixed-effect specification; LSOA-clustered standard errors are "
        "in parentheses. The coefficient of interest is daily spill count &times; "
        "log article salience. Cumulative and windowed article measures differ in "
        "scale, so compare signs and precision rather than raw magnitudes. "
        "*** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1."
    )
    extensive_note = intensive_note.replace(
        "daily spill count &times; log article salience",
        "near bin &times; log article salience",
    )
    return "\n".join(
        [
            '    <h2 id="summary">Summary<a class="toptop" href="#top">top</a></h2>',
            (
                '    <p class="sec-intro">This section reports only the coefficient '
                "of interest from each public-attention specification. Full "
                "regression tables appear below.</p>"
            ),
            '    <h3 class="subsec" id="summary-intensive">Intensive margin</h3>',
            render_summary_table(
                "intensive",
                MEASURES,
                "Daily spill count<br>&times; log (Articles measure)",
                intensive_note,
            ),
            '    <h3 class="subsec" id="summary-extensive">Extensive margin</h3>',
            render_summary_table(
                "extensive",
                EXTENSIVE_MEASURES,
                "Near bin<br>&times; log (Articles measure)",
                extensive_note,
            ),
        ]
    )


def render_family(family_id: str, title: str, subtitle: str, tables: list[tuple[str, str]]) -> str:
    figures = "\n".join(render_measure_table(filename, measure) for measure, filename in tables)
    return f"""    <section class="fam" id="{family_id}">
      <details open>
        <summary>{html.escape(title)} <span class="fam-meta">&middot; {html.escape(subtitle)} &middot; {len(tables)} tables</span></summary>
        <div class="fam-body">
{figures}
        </div>
      </details>
    </section>"""


def build_sections_html() -> str:
    return "\n".join(
        [
            '    <h2 id="article-salience">Article Salience Measures<a class="toptop" href="#top">top</a></h2>',
            (
                '    <p class="sec-intro">The comparison uses the cumulative '
                "LexisNexis article-count measure from the main public-attention "
                "specification and trailing 3-, 6-, and 12-month measures that are "
                "inclusive of the transaction month.</p>"
            ),
            _equations_html(["article_salience"]),
            '    <h2 id="intensive-margin">Intensive Margin<a class="toptop" href="#top">top</a></h2>',
            (
                '    <p class="sec-intro">The intensive-margin models estimate how '
                "transaction prices vary with nearby spill intensity, allowing the "
                "spill-price gradient to shift with article salience.</p>"
            ),
            _equations_html(["windowed_intensive"]),
            render_family(
                "fam-intensive-article-salience",
                "Intensive margin - article salience measures",
                "cumulative / 3m / 6m / 12m",
                MEASURES,
            ),
            '    <h2 id="extensive-margin">Extensive Margin<a class="toptop" href="#top">top</a></h2>',
            (
                '    <p class="sec-intro">The extensive-margin models compare '
                "properties whose nearest mapped overflow is in the 0-500 m near "
                "bin with properties in the 1000-2000 m far bin, allowing the "
                "near-far gap to shift with article salience.</p>"
            ),
            _equations_html(["windowed_extensive"]),
            render_family(
                "fam-extensive-article-salience",
                "Extensive margin - article salience measures",
                "cumulative / 3m / 6m / 12m",
                EXTENSIVE_MEASURES,
            ),
        ]
    )


def build_html(summary_html: str, sections_html: str) -> str:
    n_tables = len(MEASURES) + len(EXTENSIVE_MEASURES)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Windowed article salience results</title>
  <script>
    window.MathJax = {{
      tex: {{
        displayMath: [["\\\\[", "\\\\]"]],
        inlineMath: [["\\\\(", "\\\\)"]],
        processEscapes: true
      }}
    }};
  </script>
  <script defer src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-chtml.js"></script>
  <style>{base.CSS}  </style>
</head>
<body>
  <div class="page" id="top">
    <header>
      <h1>Windowed article salience results</h1>
      <p class="subtitle">Cumulative and trailing 3-, 6-, and 12-month LexisNexis article-count salience measures for intensive- and extensive-margin public-attention specifications.</p>
      <div class="meta">
        <div><dt>Date</dt><dd>{REPORT_DATE}</dd></div>
        <div><dt>Author</dt><dd>{REPORT_AUTHOR}</dd></div>
        <div><dt>Sample</dt><dd>England, 2021&ndash;2023</dd></div>
        <div><dt>Tables</dt><dd>{n_tables} regression tables</dd></div>
      </div>
    </header>
    <main>
{build_toc()}
{summary_html}
{sections_html}
    </main>
    <footer>
      Generated from <code>output/tables/*.tex</code> by
      <code>scripts/python/build_windowed_article_salience_html_report.py</code>.
      Re-run the windowed article scripts, then this builder, to refresh after
      the tables are regenerated. Significance:
      <sup class="stars">***</sup>&nbsp;p&lt;0.01,
      <sup class="stars">**</sup>&nbsp;p&lt;0.05,
      <sup class="stars">*</sup>&nbsp;p&lt;0.1.
    </footer>
  </div>
</body>
</html>
"""


def main() -> None:
    summary_html = build_summary_html()
    sections_html = build_sections_html()
    full = build_html(summary_html, sections_html)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(full)
    print(f"Wrote {len(MEASURES) + len(EXTENSIVE_MEASURES)} tables to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
