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

import csv
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
EFFECT_SIZE_PATH = TABLES_DIR / "did_articles_windowed_prior_effect_sizes.csv"
EXTENSIVE_EFFECT_SIZE_PATH = (
    TABLES_DIR / "did_articles_windowed_prior_extensive_effect_sizes.csv"
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

EFFECT_ROW_SPECS = {
    "intensive": [
        (
            "effect_iqr_pct",
            "effect_iqr_se_pct",
            "IQR salience<br>&times; 1 SD spills (%)",
        ),
        (
            "effect_sd_pct",
            "effect_sd_se_pct",
            "1 SD salience<br>&times; 1 SD spills (%)",
        ),
    ],
    "extensive": [
        ("effect_iqr_pct", "effect_iqr_se_pct", "IQR salience (%)"),
        ("effect_sd_pct", "effect_sd_se_pct", "1 SD salience (%)"),
    ],
}

SUMMARY_EFFECT_CSS = """
    tbody tr.serow th.rowlab,
    tbody tr.serow td,
    tbody tr.effectse th.rowlab,
    tbody tr.effectse td,
    tbody tr.statrow:has(> th.rowlab:empty) th.rowlab,
    tbody tr.statrow:has(> th.rowlab:empty) td {
      color: var(--muted);
      background: #f4f7f7;
    }
    tbody tr.effectrow th.rowlab,
    tbody tr.effectrow td {
      color: var(--muted);
      background: #fbfcfc;
      font-size: 12px;
    }
    tbody tr.effectrow th.rowlab { padding-left: 20px; }
    ol.summary-scale { color: var(--muted); font-size: 13px; line-height: 1.55; margin: 0 0 18px 22px; padding: 0; max-width: 1100px; }
    ol.summary-scale li { margin: 3px 0; }
"""

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


def load_effect_sizes(
    path: Path,
    expected_margin: str,
) -> dict[tuple[str, str, str], dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing effect-size summary: {path}. Re-run the corresponding "
            "windowed article R script before rebuilding the HTML report."
        )

    rows: dict[tuple[str, str, str], dict[str, str]] = {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "margin",
            "market",
            "fixed_effects",
            "measure",
            "p_value",
            "effect_iqr_pct",
            "effect_iqr_se_pct",
            "effect_sd_pct",
            "effect_sd_se_pct",
        }
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Effect-size summary {path} is missing columns: "
                f"{', '.join(sorted(missing))}"
            )

        for row in reader:
            if row["margin"] != expected_margin:
                raise ValueError(
                    f"Expected margin {expected_margin!r} in {path}, "
                    f"found {row['margin']!r}."
                )
            key = (row["fixed_effects"], row["market"], row["measure"])
            rows[key] = row

    return rows


def _format_effect(value: str) -> str:
    number = float(value)
    rounded = round(number, 2)
    if rounded == 0:
        rounded = 0.0
    return f"{rounded:.2f}".replace("-", "−")


def _stars_from_p_value(value: str) -> str:
    p_value = float(value)
    if p_value < 0.01:
        return '<sup class="stars">***</sup>'
    if p_value < 0.05:
        return '<sup class="stars">**</sup>'
    if p_value < 0.1:
        return '<sup class="stars">*</sup>'
    return ""


def _summary_effect_rows(
    effect_sizes: dict[tuple[str, str, str], dict[str, str]],
    fe: str,
    measures: list[str],
    estimate_metric: str,
    se_metric: str,
) -> tuple[list[str], list[str]]:
    estimate_cells: list[str] = []
    se_cells: list[str] = []
    for market in ("sales", "rentals"):
        for measure in measures:
            key = (fe, market, measure)
            if key not in effect_sizes:
                raise ValueError(
                    "Missing effect-size row for "
                    f"fixed_effects={fe!r}, market={market!r}, measure={measure!r}"
                )
            row = effect_sizes[key]
            estimate_cells.append(
                f"{_format_effect(row[estimate_metric])}{_stars_from_p_value(row['p_value'])}"
            )
            se_cells.append(f"({_format_effect(row[se_metric])})")
    return estimate_cells, se_cells


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


def render_summary_table(
    kind: str,
    files: list[tuple[str, str]],
    row_label: str,
    note: str,
    effect_sizes: dict[tuple[str, str, str], dict[str, str]],
) -> str:
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
                f'      <tr class="statrow serow"><th scope="row" class="rowlab"></th>{se_html}</tr>',
            ]
        )
        for estimate_metric, se_metric, effect_label in EFFECT_ROW_SPECS[kind]:
            effect_cells, effect_se_cells = _summary_effect_rows(
                effect_sizes,
                fe,
                measures,
                estimate_metric,
                se_metric,
            )
            effect_html = "".join(f"<td>{cell}</td>" for cell in effect_cells)
            effect_se_html = "".join(f"<td>{cell}</td>" for cell in effect_se_cells)
            rows.append(
                f'      <tr class="effectrow"><th scope="row" class="rowlab">'
                f"{effect_label}</th>{effect_html}</tr>"
            )
            rows.append(
                f'      <tr class="statrow effectse"><th scope="row" '
                f'class="rowlab"></th>{effect_se_html}</tr>'
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
    intensive_effect_sizes = load_effect_sizes(EFFECT_SIZE_PATH, "intensive")
    extensive_effect_sizes = load_effect_sizes(
        EXTENSIVE_EFFECT_SIZE_PATH,
        "extensive",
    )
    intensive_note = (
        "<strong>Notes:</strong> The coefficient row reports the coefficient of "
        "interest from the stated fixed-effect specification; LSOA-clustered "
        "standard errors are in parentheses. Effect rows transform the same "
        "coefficient into approximate percent changes using transaction-sample "
        "contrasts: the IQR salience row uses the interquartile range of the "
        "article measure, and the 1 SD salience row uses its standard deviation; "
        "both intensive-margin rows also scale by one standard deviation of daily "
        "spill exposure. Scaled standard errors are in parentheses under each "
        "effect row, and significance stars inherit the underlying interaction "
        "p-value. Raw coefficients are not directly comparable across "
        "salience windows because the article measures differ in empirical "
        "support. *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1."
    )
    extensive_note = (
        "<strong>Notes:</strong> The coefficient row reports the coefficient of "
        "interest from the stated fixed-effect specification; LSOA-clustered "
        "standard errors are in parentheses. Effect rows transform the same "
        "coefficient into approximate percent changes in the near-far price/rent "
        "gap using transaction-sample contrasts: the IQR salience row uses "
        "the interquartile range of the article measure, and the 1 SD salience "
        "row uses its standard deviation. Scaled standard errors are in "
        "parentheses under each effect row, and significance stars inherit the "
        "underlying interaction p-value. Raw coefficients are not directly "
        "comparable across salience windows because the article measures differ "
        "in empirical support. *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1."
    )
    return "\n".join(
        [
            '    <h2 id="summary">Summary<a class="toptop" href="#top">top</a></h2>',
            (
                '    <p class="sec-intro">This section reports the coefficient '
                "of interest from each public-attention specification and "
                "comparable effect-size transformations. Full regression tables "
                "appear below; the model specifications are repeated here so the "
                "summary coefficients can be read directly.</p>"
            ),
            (
                '    <p class="sec-intro">The summary tables also report scaled '
                "effect sizes, allowing the cumulative, three-month, six-month, "
                "and twelve-month salience measures to be compared on a common "
                "empirical scale.</p>"
            ),
            (
                '    <ol class="summary-scale">'
                "<li><strong>Intensive margin:</strong> the scaled rows show the "
                "implied percentage change associated with an interquartile-range "
                "or one-standard-deviation increase in article salience, evaluated "
                "at a one-standard-deviation increase in daily spill exposure.</li>"
                "<li><strong>Extensive margin:</strong> the scaled rows show the "
                "implied percentage change in the near&ndash;far price or rent gap "
                "associated with an interquartile-range or one-standard-deviation "
                "increase in article salience.</li>"
                "</ol>"
            ),
            '    <h3 class="subsec" id="summary-intensive">Intensive margin</h3>',
            _equations_html(["windowed_intensive"]),
            render_summary_table(
                "intensive",
                MEASURES,
                "Daily spill count<br>&times; log (Articles measure)",
                intensive_note,
                intensive_effect_sizes,
            ),
            '    <h3 class="subsec" id="summary-extensive">Extensive margin</h3>',
            _equations_html(["windowed_extensive"]),
            render_summary_table(
                "extensive",
                EXTENSIVE_MEASURES,
                "Near bin<br>&times; log (Articles measure)",
                extensive_note,
                extensive_effect_sizes,
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
  <style>{base.CSS}{SUMMARY_EFFECT_CSS}  </style>
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
      Generated from <code>output/tables/*.tex</code> and
      <code>output/tables/*effect_sizes.csv</code> by
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
