#!/usr/bin/env python3
"""Build a single self-contained HTML report of all radius-swept results tables.

Parses the published tabularray LaTeX tables in ``output/tables/`` and re-renders
them as styled, selectable HTML tables in one document. No LaTeX or R is needed and
no regressions are re-run -- the numbers come straight from the ``.tex``.

Scope: every specification family that was run across the full house-to-site radius
sweep (250m / 500m / 1000m), tied to GitHub issue #15. The first section is a
summary of the preferred specification (the cross-radius robustness tables);
subsequent sections present all models with all their per-radius tables.

Out of scope (no radius sweep): repeat sales, long difference, dry spills,
descriptive tables, and the distance-bin / decay upstream-downstream (`ud_*`) tables.

Handles both `talltblr` and standalone `longtblr` environments, variable column
counts (7 / 9 / 13), `Q[]` and `X[c]` colspecs, multi-column spanners and full-width
panel rows (`cell{r}{c}={c=N}`), `\\textbf{}` / `\\quad` cell markup, in-cell line
breaks, `\\num{}` / math, significance stars, and thousands grouping.

Usage:
    python3 scripts/python/build_intensive_margin_html_report.py

Output:
    docs/reports/2026-06-22-001-intensive-margin-results-tables-report.html

The generator is reusable: re-run it after the tables are regenerated to refresh
the report.
"""

from __future__ import annotations

import html
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = REPO_ROOT / "output" / "tables"
OUTPUT_PATH = (
    REPO_ROOT
    / "docs"
    / "reports"
    / "2026-06-22-001-intensive-margin-results-tables-report.html"
)

REPORT_DATE = "2026-06-22"
REPORT_AUTHOR = "Jacopo Olivieri"

# Row labels that make up the specification / goodness-of-fit block at the foot of a
# regression table (muted styling + a divider rule above the first one).
STAT_LABELS = {
    "Property controls",
    "Location FE",
    "Time FE",
    "MSOA FE",
    "LSOA FE",
    "Observations",
    "Adj. R-squared",
}

RADII = ["250m", "500m", "1000m"]


def fam(base: str, title: str) -> dict:
    """A block = one specification family shown across the three radii."""
    return {"kind": "fam", "base": base, "title": title,
            "files": [f"{base}_{r}.tex" for r in RADII]}


def one(file: str) -> dict:
    """A block = a single standalone table (e.g. a cross-radius robustness summary)."""
    return {"kind": "single", "file": file}


def synth(fn: str, title: str) -> dict:
    """A block built by a synthesis function rather than read from a .tex file."""
    return {"kind": "synth", "fn": fn, "title": title}


# Lead section: the cross-radius SUMMARY tables for the main spec, by analysis.
SUMMARY = {
    "id": "summary",
    "title": "Summary",
    "intro": (
        "This section presents a robustness analysis of the main specification across "
        "three house-to-site radii (250, 500, and 1,000 m), reporting the key "
        "coefficients of interest for each. Full regression tables are provided in the "
        "sections below."
    ),
    "subsections": [
        {
            "id": "summary-hedonics",
            "title": "Cross-sectional hedonics",
            "blocks": [
                one("hedonic_count_continuous_prior_radius_robustness.tex"),
            ],
        },
        {
            "id": "summary-upstream-downstream",
            "title": "Upstream vs downstream",
            "blocks": [
                synth("ud_summary",
                      "Upstream vs downstream spill exposure: robustness to house-to-site radius"),
                synth("nearest_summary",
                      "Nearest spill site: robustness to house-to-site radius"),
            ],
        },
        {
            "id": "summary-public-attention",
            "title": "Public attention",
            "blocks": [
                one("did_trends_prior_radius_robustness.tex"),
                one("did_articles_prior_radius_robustness.tex"),
            ],
        },
    ],
}

# Full analysis sections: every per-radius table, with each analysis's robustness.
SECTIONS = [
    {
        "id": "cross-sectional-hedonics",
        "title": "Cross-sectional hedonics",
        "specs": ["hedonic"],
        "blocks": [
            fam("hedonic_count_continuous_prior", "Daily spill count"),
            fam("hedonic_hrs_continuous_prior", "Daily spill hours"),
        ],
        "subsections": [
            {
                "id": "hedonics-robustness",
                "title": "Robustness",
                "specs": ["hedonic_qtr"],
                "blocks": [
                    fam("hedonic_count_continuous_prior_qtr_fe", "Quarterly fixed effects — count"),
                    fam("hedonic_hrs_continuous_prior_qtr_fe", "Quarterly fixed effects — hours"),
                ],
            },
        ],
    },
    {
        "id": "upstream-downstream",
        "title": "Upstream vs downstream",
        "subsections": [
            {
                "id": "ud-directional",
                "title": "Directional spill exposure",
                "specs": ["updown", "updown_weighted"],
                "note": "The distance-weighted specification replaces the unweighted "
                        "upstream/downstream exposures with the inverse-river-distance-weighted "
                        "objects above.",
                "blocks": [
                    fam("hedonic_count_continuous_prior_direction", "Direction split — count"),
                    fam("hedonic_hrs_continuous_prior_direction", "Direction split — hours"),
                    fam("hedonic_count_continuous_prior_direction_msoa", "Direction split, MSOA FE — count"),
                    fam("hedonic_hrs_continuous_prior_direction_msoa", "Direction split, MSOA FE — hours"),
                    fam("hedonic_count_continuous_prior_direction_msoa_both", "Direction split, weighted & unweighted panels — count"),
                    fam("hedonic_hrs_continuous_prior_direction_msoa_both", "Direction split, weighted & unweighted panels — hours"),
                    fam("hedonic_count_continuous_prior_direction_weighted", "Direction split, inverse-distance weighted — count"),
                    fam("hedonic_hrs_continuous_prior_direction_weighted", "Direction split, inverse-distance weighted — hours"),
                    fam("hedonic_count_continuous_prior_direction_weighted_msoa", "Direction split, weighted, MSOA FE — count"),
                    fam("hedonic_hrs_continuous_prior_direction_weighted_msoa", "Direction split, weighted, MSOA FE — hours"),
                ],
            },
            {
                "id": "ud-nearest",
                "title": "Nearest spill site",
                "specs": ["nearest"],
                "blocks": [
                    fam("hedonic_count_continuous_prior_nearest_site", "Nearest site — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site", "Nearest site — hours"),
                    fam("hedonic_count_continuous_prior_nearest_site_distance", "Nearest site, distance — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site_distance", "Nearest site, distance — hours"),
                ],
            },
            {
                "id": "ud-robustness",
                "title": "Robustness",
                "specs": ["nearest"],
                "note": "Same nearest-site specification as above, estimated with lateral "
                        "vs along-river distance cuts and on the single-spill-site sample.",
                "blocks": [
                    fam("hedonic_count_continuous_prior_nearest_site_lateral", "Nearest site, lateral — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site_lateral", "Nearest site, lateral — hours"),
                    fam("hedonic_count_continuous_prior_nearest_site_river", "Nearest site, along-river — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site_river", "Nearest site, along-river — hours"),
                    fam("hedonic_count_continuous_prior_nearest_site_distance_lateral", "Nearest site, distance × lateral — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site_distance_lateral", "Nearest site, distance × lateral — hours"),
                    fam("hedonic_count_continuous_prior_nearest_site_distance_river", "Nearest site, distance × along-river — count"),
                    fam("hedonic_hrs_continuous_prior_nearest_site_distance_river", "Nearest site, distance × along-river — hours"),
                    fam("hedonic_count_continuous_prior_one_site", "Single nearest site — count"),
                    fam("hedonic_hrs_continuous_prior_one_site", "Single nearest site — hours"),
                    fam("hedonic_count_continuous_prior_one_site_distance", "Single nearest site, distance — count"),
                    fam("hedonic_hrs_continuous_prior_one_site_distance", "Single nearest site, distance — hours"),
                ],
            },
        ],
    },
    {
        "id": "public-attention",
        "title": "Public attention",
        "specs": ["news_discrete", "news_cont"],
        "blocks": [
            fam("did_trends_prior", "Pre/post Google Trends peak"),
            fam("did_articles_prior", "Log cumulative news coverage"),
        ],
        "subsections": [
            {
                "id": "public-attention-robustness",
                "title": "Robustness (extensive margin)",
                "specs": ["extensive_discrete", "extensive_cont"],
                "blocks": [
                    one("did_trends_prior_extensive_radius_robustness.tex"),
                    one("did_articles_prior_extensive_radius_robustness.tex"),
                ],
            },
        ],
    },
]

# --------------------------------------------------------------------------- #
# LaTeX -> text/HTML helpers
# --------------------------------------------------------------------------- #

_BR = ""       # in-cell line break
_B_OPEN = ""   # <strong>
_B_CLOSE = ""  # </strong>
_IND = ""      # \quad indentation


def _strip_outer_braces(s: str) -> str:
    """Drop a single pair of braces wrapping the whole string (e.g. ``{a \\ b}``)."""
    if not (s.startswith("{") and s.endswith("}")):
        return s
    depth = 0
    for i, ch in enumerate(s):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[1:-1] if i == len(s) - 1 else s
    return s


def _math_and_macros(s: str) -> str:
    """Resolve the handful of LaTeX macros that appear in these tables."""
    s = re.sub(r"\\num\{([^}]*)\}", r"\1", s)
    s = re.sub(r"\\text\{([^}]*)\}", r"\1", s)
    s = re.sub(r"\\mathrm\{([^}]*)\}", r"\1", s)
    s = (
        s.replace("\\times", "×")
        .replace("\\log", "log")
        .replace("\\geq", "≥")
        .replace("\\leq", "≤")
        .replace("\\%", "%")
        .replace("\\&", "&")
        .replace("\\,", " ")
        .replace("\\;", " ")
        .replace("~", " ")
    )
    s = s.replace("$", "")
    return s


def _prettify_numbers(s: str) -> str:
    """Group thousands in standalone integers and use a real minus sign."""
    s = re.sub(
        r"(?<![\w.,])\d{4,}(?![\w.,])",
        lambda m: format(int(m.group()), ","),
        s,
    )
    s = re.sub(r"(?<!\w)-(?=\d)", "−", s)
    return s


def clean_cell(raw: str) -> str:
    """Turn one LaTeX table cell into display-ready HTML."""
    s = raw.strip()
    if not s:
        return ""
    s = _strip_outer_braces(s)
    s = re.sub(r"\\textbf\{([^}]*)\}", _B_OPEN + r"\1" + _B_CLOSE, s)
    s = s.replace("\\quad ", _IND).replace("\\quad", _IND)
    s = s.replace("\\\\", _BR)  # in-cell LaTeX line break
    s = _math_and_macros(s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    s = _prettify_numbers(s)
    s = html.escape(s)
    s = re.sub(r"(\*{1,3})$", r'<sup class="stars">\1</sup>', s)
    s = (
        s.replace(_BR, "<br>")
        .replace(_B_OPEN, "<strong>")
        .replace(_B_CLOSE, "</strong>")
        .replace(_IND, "&nbsp;&nbsp;")
    )
    return s


def clean_note(raw: str) -> str:
    """Render the table ``note{}`` / caption block as escaped HTML, keeping bold."""
    s = raw.strip()
    m = re.match(r"\\footnotesize\s*\{(.*)\}\s*$", s, re.S)
    if m:
        s = m.group(1)
    s = re.sub(r"\\textbf\{([^}]*)\}", _B_OPEN + r"\1" + _B_CLOSE, s)
    s = s.replace("\\\\", " ")
    s = _math_and_macros(s)
    s = s.replace("--", "–")
    s = re.sub(r"\s+", " ", s).strip()
    s = html.escape(s)
    s = s.replace(_B_OPEN, "<strong>").replace(_B_CLOSE, "</strong>")
    return s


def _expand_indices(spec: str) -> list[int]:
    """Expand a tabularray index spec like ``2,4-6,8-10`` into a list of ints."""
    out: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


# --------------------------------------------------------------------------- #
# Table parsing + rendering
# --------------------------------------------------------------------------- #


def _radius_short(filename: str) -> str:
    m = re.search(r"_(\d+)m\.tex$", filename)
    return f"{int(m.group(1))} m" if m else ""


def _anchor_id(label: str, fallback: str) -> str:
    base = label if label else fallback
    return re.sub(r"[^A-Za-z0-9_-]", "-", base.replace(":", "-"))


def _split_cells(line: str) -> list[str]:
    return re.split(r"(?<!\\)&", line)


def parse_table(path: Path) -> dict:
    text = path.read_text()

    cap_m = re.search(r"caption=\{(.*?)\},", text, re.S)
    caption = clean_note(cap_m.group(1)) if cap_m else path.stem
    # Drop a trailing "(500m)" radius parenthetical baked into some captions.
    caption = re.sub(r"\s*\(\d+\s*m\)\s*$", "", caption)
    lab_m = re.search(r"label=\{(.*?)\}", text)
    label = lab_m.group(1) if lab_m else ""

    note = ""
    note_idx = text.find("note{}={")
    if note_idx != -1:
        start = note_idx + len("note{}={")
        depth = 1
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    note = clean_note(text[start:i])
                    break

    # Merged-cell / column-spanner map from the cell{R}{C}={c=N} directives.
    spanmap: dict[tuple[int, int], int] = {}
    for m in re.finditer(r"cell\{([^}]+)\}\{([^}]+)\}=\{([^}]*)\}", text):
        cm = re.search(r"c=(\d+)", m.group(3))
        if not cm:
            continue
        span = int(cm.group(1))
        for r in _expand_indices(m.group(1)):
            for c in _expand_indices(m.group(2)):
                spanmap[(r, c)] = span

    # Body rows = every source line that ends in the LaTeX row terminator "\\".
    # This is agnostic to the environment (talltblr / longtblr) and delimiter style.
    body: list[list[str]] = []
    for line in text.splitlines():
        line = line.rstrip()
        if line.endswith("\\\\"):
            body.append(_split_cells(line[:-2]))

    ncols = max((len(r) for r in body), default=0)
    return {
        "caption": caption,
        "label": label,
        "note": note,
        "ncols": ncols,
        "spanmap": spanmap,
        "body": body,
    }


def render_table(parsed: dict, filename: str) -> str:
    ncols = parsed["ncols"]
    spanmap = parsed["spanmap"]
    body = parsed["body"]

    radius = _radius_short(filename)
    title = f"{radius} — {parsed['caption']}" if radius else parsed["caption"]
    anchor = _anchor_id(parsed["label"], filename)

    rows_html: list[str] = []
    prev_kind = "header"
    for ri, cells in enumerate(body, start=1):
        is_header = ri <= 2
        label0 = clean_cell(cells[0]) if cells else ""
        is_panel = (not is_header) and spanmap.get((ri, 1), 1) >= 2
        is_stat = (not is_header) and (not is_panel) and label0 in STAT_LABELS

        if is_panel:
            # Full-width section/panel header taken from column 1.
            rows_html.append(
                f'      <tr class="panel-row"><th class="panel-head" '
                f'colspan="{ncols}">{label0}</th></tr>'
            )
            prev_kind = "panel"
            continue

        rendered: list[str] = []
        c = 1
        while c <= ncols:
            span = spanmap.get((ri, c), 1)
            raw = cells[c - 1] if c - 1 < len(cells) else ""
            txt = clean_cell(raw)
            colspan_attr = f' colspan="{span}"' if span > 1 else ""
            if is_header:
                cls = ' class="spanner"' if span > 1 else ""
                rendered.append(f'<th scope="col"{cls}{colspan_attr}>{txt}</th>')
            elif c == 1:
                rendered.append(f'<th scope="row" class="rowlab">{txt}</th>')
            else:
                rendered.append(f"<td{colspan_attr}>{txt}</td>")
            c += span

        tr_classes: list[str] = []
        if is_header:
            kind = "header"
        elif is_stat:
            tr_classes.append("statrow")
            if prev_kind not in ("stat", "panel", "header"):
                tr_classes.append("block-top")
            kind = "stat"
        else:
            kind = "coef"
        cls_attr = f' class="{" ".join(tr_classes)}"' if tr_classes else ""
        rows_html.append(f"      <tr{cls_attr}>{''.join(rendered)}</tr>")
        prev_kind = kind

    thead = "\n".join(rows_html[:2])
    tbody = "\n".join(rows_html[2:])
    note_html = f'\n    <p class="tnote">{parsed["note"]}</p>' if parsed["note"] else ""

    return f"""  <figure class="tbl" id="{anchor}">
    <figcaption>{title}</figcaption>
    <div class="table-wrap">
    <table>
      <thead>
{thead}
      </thead>
      <tbody>
{tbody}
      </tbody>
    </table>
    </div>{note_html}
  </figure>"""


# --------------------------------------------------------------------------- #
# Page assembly
# --------------------------------------------------------------------------- #

CSS = """
    :root {
      --bg: #f7f8f4; --paper: #ffffff; --ink: #18201c; --muted: #5b675f;
      --line: #d9ded5; --accent: #245f73; --accent-soft: #e2f0f3; --ok: #1f6b43;
      --shadow: 0 14px 40px rgba(24, 32, 28, 0.08);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink); line-height: 1.55; font-size: 15px; }
    .page {
      width: min(1080px, calc(100vw - 32px)); margin: 36px auto 48px; background: var(--paper);
      border: 1px solid var(--line); box-shadow: var(--shadow); border-radius: 8px; overflow: hidden;
    }
    header { padding: 34px 38px 28px; border-bottom: 1px solid var(--line); background: linear-gradient(180deg, #ffffff 0%, #f5faf8 100%); }
    main { padding: 30px 38px 36px; }
    h1 { margin: 0 0 8px; font-size: 30px; line-height: 1.14; font-weight: 720; }
    .subtitle { margin: 0; color: var(--muted); font-size: 15px; max-width: 74ch; }
    h2 { margin: 44px 0 8px; padding-top: 24px; border-top: 2px solid var(--line); font-size: 22px; line-height: 1.25; }
    .sec-intro { color: var(--muted); margin: 0 0 14px; max-width: 84ch; font-size: 14px; }
    h3.grp { margin: 22px 0 8px; font-size: 16px; color: var(--accent); }
    h3.subsec { margin: 28px 0 8px; font-size: 17px; color: var(--ink); border-bottom: 1px solid var(--line); padding-bottom: 5px; }
    .eqn { margin: 4px 0 18px; padding: 10px 12px; overflow-x: auto; text-align: center; background: #fbfcfa; border: 1px solid var(--line); border-radius: 6px; }
    .eqn svg { max-width: 100%; height: auto; vertical-align: middle; }
    .eqn code { background: none; border: 0; font-size: 0.95em; }
    p.eqn-note { text-align: center; margin: -8px 2px 16px; }
    .eqn-where { margin: -8px 2px 18px; font-size: 12.5px; color: var(--muted); }
    .eqn-where .where-lead { font-style: italic; }
    .eqn-where ul { margin: 4px 0 0; padding-left: 18px; list-style: none; }
    .eqn-where li { margin: 2px 0; max-width: 92ch; text-indent: -12px; }
    .eqn-where li::before { content: "– "; color: var(--muted); }
    .ov { text-decoration: overline; }
    p, li { max-width: 88ch; }
    code { color: #26342e; background: #eef1ec; border: 1px solid #dfe5dc; padding: 0.06rem 0.28rem; border-radius: 4px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.9em; }
    .meta { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-top: 22px; }
    .meta div { border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; background: rgba(255,255,255,0.72); min-width: 0; }
    .meta dt { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    .meta dd { margin: 0; font-weight: 650; word-break: break-word; }
    .callout { border: 1px solid var(--line); border-left: 4px solid var(--accent); background: var(--accent-soft); border-radius: 6px; padding: 14px 16px; margin: 22px 0 6px; }
    .callout p { margin: 0 0 6px; } .callout p:last-child { margin-bottom: 0; }
    nav.toc { border: 1px solid var(--line); border-radius: 6px; padding: 16px 18px; margin: 24px 0 8px; background: #fbfcfa; }
    nav.toc strong { display: block; font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); margin-bottom: 10px; }
    nav.toc ol { margin: 0; padding-left: 20px; }
    nav.toc > ol > li { margin: 7px 0 2px; font-weight: 650; }
    nav.toc ol ol { padding-left: 18px; margin: 4px 0 10px; }
    nav.toc ol ol li { font-weight: 500; }
    nav.toc a { color: var(--accent); text-decoration: none; }
    nav.toc a:hover { text-decoration: underline; }
    .toptop { float: right; font-size: 12px; font-weight: 500; }

    section.fam { border-top: 1px dashed var(--line); margin-top: 18px; }
    section.fam > details { margin: 0; }
    section.fam > details > summary { cursor: pointer; list-style: revert; padding: 12px 2px 8px; font-weight: 680; font-size: 15.5px; }
    section.fam > details > summary::-webkit-details-marker { color: var(--accent); }
    .fam-meta { color: var(--muted); font-weight: 500; font-size: 12.5px; }
    .fam-body { padding-left: 4px; }

    figure.tbl { margin: 18px 0 24px; }
    figure.tbl figcaption { font-weight: 620; font-size: 14px; margin-bottom: 9px; line-height: 1.3; color: #1f2a25; }
    .table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 6px; }
    table { border-collapse: collapse; width: 100%; font-size: 12.5px; font-variant-numeric: tabular-nums; }
    thead th { background: #f1f5f2; color: var(--ink); font-weight: 650; padding: 6px 8px; text-align: center; border-bottom: 1px solid var(--line); white-space: nowrap; }
    thead th.spanner { border-bottom: 1px solid #c4ccc2; font-weight: 700; }
    tbody th.rowlab { text-align: left; font-weight: 500; padding: 5px 10px 5px 12px; white-space: nowrap; }
    tbody td { text-align: center; padding: 5px 8px; white-space: nowrap; }
    tbody tr:nth-child(even) { background: #fafbf9; }
    tbody tr.statrow th.rowlab, tbody tr.statrow td { color: var(--muted); }
    tbody tr.block-top th, tbody tr.block-top td { border-top: 1px solid var(--line); }
    tbody tr.panel-row th.panel-head { text-align: left; background: var(--accent-soft); color: #143b48; font-weight: 700; padding: 6px 12px; border-top: 1px solid var(--line); }
    sup.stars { font-size: 0.72em; }
    p.tnote { color: var(--muted); font-size: 12px; line-height: 1.5; margin: 10px 2px 0; max-width: none; }

    footer { padding: 22px 38px 30px; border-top: 1px solid var(--line); color: var(--muted); font-size: 12.5px; background: #fbfcfa; }
    @media (max-width: 760px) { .meta { grid-template-columns: repeat(2, 1fr); } main, header, footer { padding-left: 20px; padding-right: 20px; } }
"""


def build_toc() -> str:
    def sub_list(subs: list) -> str:
        lis = "\n".join(
            f'        <li><a href="#{s["id"]}">{html.escape(s["title"])}</a></li>'
            for s in subs
        )
        return f"        <ol>\n{lis}\n        </ol>\n"

    items = [
        f'      <li><a href="#{SUMMARY["id"]}">{html.escape(SUMMARY["title"])}</a>\n'
        f'{sub_list(SUMMARY["subsections"])}      </li>'
    ]
    for sec in SECTIONS:
        if sec.get("subsections"):
            items.append(
                f'      <li><a href="#{sec["id"]}">{html.escape(sec["title"])}</a>\n'
                f'{sub_list(sec["subsections"])}      </li>'
            )
        else:
            items.append(
                f'      <li><a href="#{sec["id"]}">{html.escape(sec["title"])}</a></li>'
            )
    return (
        '    <nav class="toc">\n      <strong>Contents</strong>\n      <ol>\n'
        + "\n".join(items)
        + "\n      </ol>\n    </nav>"
    )


def render_block(block: dict) -> tuple[str, int]:
    """Render one block: a 3-radii family, a single table, or a synthesized table."""
    if block["kind"] == "synth":
        if block["fn"] == "ud_summary":
            return render_ud_summary()
        if block["fn"] == "nearest_summary":
            return render_nearest_summary()
        return "", 0
    if block["kind"] == "fam":
        figs, n = [], 0
        for fname in block["files"]:
            path = TABLES_DIR / fname
            if not path.exists():
                print(f"  WARNING: missing table, skipping: {fname}")
                continue
            figs.append(render_table(parse_table(path), fname))
            n += 1
        anchor = "fam-" + re.sub(r"[^A-Za-z0-9_-]", "-", block["base"])
        html_block = (
            f'    <section class="fam" id="{anchor}">\n'
            f"      <details open>\n"
            f'        <summary>{html.escape(block["title"])} '
            f'<span class="fam-meta">· 250 / 500 / 1000 m · {n} tables</span></summary>\n'
            f'        <div class="fam-body">\n' + "\n".join(figs) + "\n        </div>\n"
            f"      </details>\n"
            f"    </section>"
        )
        return html_block, n
    fname = block["file"]
    path = TABLES_DIR / fname
    if not path.exists():
        print(f"  WARNING: missing table, skipping: {fname}")
        return "", 0
    return render_table(parse_table(path), fname), 1


def render_blocks(blocks: list) -> tuple[str, int]:
    htmls, n = [], 0
    for b in blocks:
        h, c = render_block(b)
        if h:
            htmls.append(h)
            n += c
    return "\n".join(htmls), n


# --------------------------------------------------------------------------- #
# Synthesized cross-radius summaries (built from per-radius tables)
# --------------------------------------------------------------------------- #
# Column map within a body row (1-indexed, label = 0): sales prop+MSOA = 4,
# prop+LSOA = 6; rentals prop+MSOA = 10, prop+LSOA = 12.


def _extract_summary_rows(base: str, rad: str, row_specs: list, panel_a_only: bool) -> dict:
    """Pull coef+SE for selected rows from a per-radius table.

    `row_specs` is a list of (label, predicate(raw_first_cell)). Returns
    {label: {"MSOA": (cs, ss, cr, sr), "LSOA": (...)}}.
    """
    parsed = parse_table(TABLES_DIR / f"{base}_{rad}.tex")
    body = parsed["body"]
    if panel_a_only:
        start = next((i for i, c in enumerate(body) if c and "Panel A" in c[0]), 0)
        end = next(
            (j for j in range(start + 1, len(body)) if body[j] and "Panel B" in body[j][0]),
            len(body),
        )
        region = body[start + 1 : end]
    else:
        region = body

    def cell(row, idx):
        return clean_cell(row[idx]) if idx < len(row) else ""

    out: dict = {}
    for label, pred in row_specs:
        for k, row in enumerate(region):
            raw = row[0] if row else ""
            if pred(raw):
                se = region[k + 1] if k + 1 < len(region) else []
                out[label] = {
                    "MSOA": (cell(row, 4), cell(se, 4), cell(row, 10), cell(se, 10)),
                    "LSOA": (cell(row, 6), cell(se, 6), cell(row, 12), cell(se, 12)),
                }
                break
    return out


def render_synth_summary(base, panel_a_only, row_specs, caption, anchor, note) -> tuple[str, int]:
    """Build a cross-radius summary table mirroring the *_radius_robustness layout."""
    radii = ["250m", "500m", "1000m"]
    data = {r: _extract_summary_rows(base, r, row_specs, panel_a_only) for r in radii}
    labels = [lab for lab, _ in row_specs]

    def val(r, label, fe, idx):
        return data[r].get(label, {}).get(fe, ("", "", "", ""))[idx]

    def two_rows(fe, label):
        coef = "".join(f"<td>{val(r, label, fe, 0)}</td>" for r in radii) + "".join(
            f"<td>{val(r, label, fe, 2)}</td>" for r in radii
        )
        se = "".join(f"<td>{val(r, label, fe, 1)}</td>" for r in radii) + "".join(
            f"<td>{val(r, label, fe, 3)}</td>" for r in radii
        )
        return (
            f'      <tr><th scope="row" class="rowlab">{html.escape(label)}</th>{coef}</tr>\n'
            f'      <tr class="statrow"><th scope="row" class="rowlab"></th>{se}</tr>'
        )

    panels = []
    for fe, fe_label in [
        ("MSOA", "Property controls + MSOA FE"),
        ("LSOA", "Property controls + LSOA FE"),
    ]:
        panels.append(
            f'      <tr class="panel-row"><th class="panel-head" colspan="7">{fe_label}</th></tr>'
        )
        for label in labels:
            panels.append(two_rows(fe, label))
    tbody = "\n".join(panels)

    radcols = "".join(f'<th scope="col">{r}</th>' for r in radii)
    thead = (
        '      <tr><th scope="col"></th>'
        '<th scope="col" class="spanner" colspan="3">House Sales</th>'
        '<th scope="col" class="spanner" colspan="3">House Rentals</th></tr>\n'
        f'      <tr><th scope="col"></th>{radcols}{radcols}</tr>'
    )
    return f"""  <figure class="tbl" id="{anchor}">
    <figcaption>{html.escape(caption)}</figcaption>
    <div class="table-wrap">
    <table>
      <thead>
{thead}
      </thead>
      <tbody>
{tbody}
      </tbody>
    </table>
    </div>
    <p class="tnote">{note}</p>
  </figure>""", 1


def render_ud_summary() -> tuple[str, int]:
    return render_synth_summary(
        "hedonic_count_continuous_prior_direction_msoa_both",
        True,
        [
            ("Upstream", lambda raw: "Upstream" in raw and "\\times" not in raw),
            ("Downstream", lambda raw: "Downstream" in raw and "\\times" not in raw),
        ],
        "Upstream vs downstream spill exposure: robustness to house-to-site radius",
        "tbl-ud-direction-radius-robustness",
        "<strong>Notes:</strong> Cross-radius summary of the upstream/downstream "
        "directional hedonic estimates, synthesised from the unweighted panel (Panel A) "
        "of the directional tables at each radius. Each cell is the coefficient on the "
        "daily spill count for upstream / downstream river waters, from the "
        "fully-saturated specification with property controls and the stated fixed "
        "effects, estimated separately for house sale prices (log transaction price) and "
        "house rentals (log weekly asking rent). Heteroskedasticity-robust standard "
        "errors in parentheses. *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1.",
    )


def render_nearest_summary() -> tuple[str, int]:
    return render_synth_summary(
        "hedonic_count_continuous_prior_nearest_site_distance",
        False,
        [
            ("Daily spill count", lambda raw: raw.strip() == "Daily spill count"),
            ("Daily count × Upstream", lambda raw: "\\times" in raw),
        ],
        "Nearest spill site: robustness to house-to-site radius",
        "tbl-nearest-site-radius-robustness",
        "<strong>Notes:</strong> Cross-radius summary of the nearest-site directional "
        "specification, synthesised from the per-radius tables. Reported are the "
        "coefficient on the daily spill count at the nearest (downstream) site and its "
        "interaction with an indicator for the nearest site lying upstream, from the "
        "fully-saturated specification with property controls and the stated fixed "
        "effects, for house sale prices (log transaction price) and house rentals (log "
        "weekly asking rent). Heteroskedasticity-robust standard errors in parentheses. "
        "*** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1.",
    )


# --------------------------------------------------------------------------- #
# Regression specifications (paper equations) -> inline SVG
# --------------------------------------------------------------------------- #

TEXBIN = "/Library/TeX/texbin"
EQ_CACHE = Path(__file__).resolve().parent / "equation_cache"

EQUATIONS = {
    "hedonic": r"\log p_{it}=\alpha+\beta\,\overline{S}^{250}_{it}+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\varepsilon_{it}",
    "hedonic_qtr": r"\log p_{it}=\alpha+\beta\,\overline{S}^{250}_{it}+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{q(t)}+\varepsilon_{it}",
    "updown": r"\log p_{it}=\alpha+\beta_U\,\overline{S}^{U,250}_{it}+\beta_D\,\overline{S}^{D,250}_{it}+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\varepsilon_{it}",
    "updown_weighted": (
        r"\begin{aligned}"
        r"\log p_{it}&=\alpha+\beta_U\,\overline{S}^{U,w}_{it}+\beta_D\,\overline{S}^{D,w}_{it}"
        r"+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\varepsilon_{it},\\[2pt]"
        r"\overline{S}^{U,w}_{it}&=\frac{1}{D_t}\sum_{c\in\mathcal{C}^{U,250}_{i}}\omega_{ic}"
        r"\sum_{\tau=t_0}^{t}S_{c\tau},\qquad\omega_{ic}=\frac{1}{d_{ic}}"
        r"\end{aligned}"
    ),
    "nearest": r"\log p_{it}=\alpha+\beta_D\,\overline{S}^{N}_{it}+\theta\,U_i+\beta_{UD}\bigl(\overline{S}^{N}_{it}\times U_i\bigr)+\rho\,d_i+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\varepsilon_{it}",
    "news_discrete": r"\log p_{it}=\alpha+\beta\,\overline{S}^{250}_{it}+\kappa\bigl(\overline{S}^{250}_{it}\times \mathit{Post}_t\bigr)+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}",
    "news_cont": r"\log p_{it}=\alpha+\beta\,\overline{S}^{250}_{it}+\kappa\bigl(\overline{S}^{250}_{it}\times A_t\bigr)+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}",
    "extensive_discrete": r"\log p_{it}=\alpha+\phi\,\mathrm{Near}_i+\kappa\bigl(\mathrm{Near}_i\times \mathit{Post}_t\bigr)+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}",
    "extensive_cont": r"\log p_{it}=\alpha+\phi\,\mathrm{Near}_i+\kappa\bigl(\mathrm{Near}_i\times A_t\bigr)+\mathbf{X}_{i}'\gamma+\delta_{\ell(i)}+\lambda_{m(t)}+\varepsilon_{it}",
}


def _compile_equation(latex: str) -> str:
    """Compile a LaTeX equation to a self-contained inline SVG (paths, no fonts)."""
    latexbin, dvisvgm = f"{TEXBIN}/latex", f"{TEXBIN}/dvisvgm"
    if not (os.path.exists(latexbin) and os.path.exists(dvisvgm)):
        return ""
    doc = (
        "\\documentclass[border=2pt,12pt]{standalone}\n"
        "\\usepackage{amsmath,amssymb}\n"
        "\\begin{document}\n"
        f"$\\displaystyle {latex}$\n"
        "\\end{document}\n"
    )
    tmp = tempfile.mkdtemp()
    env = dict(os.environ, PATH=f"{TEXBIN}:{os.environ.get('PATH', '')}")
    try:
        (Path(tmp) / "eq.tex").write_text(doc)
        subprocess.run(
            [latexbin, "-interaction=nonstopmode", "-halt-on-error", "eq.tex"],
            cwd=tmp, env=env, capture_output=True, text=True,
        )
        if not (Path(tmp) / "eq.dvi").exists():
            return ""
        subprocess.run(
            [dvisvgm, "--no-fonts", "--exact-bbox", "-o", "eq.svg", "eq.dvi"],
            cwd=tmp, env=env, capture_output=True, text=True,
        )
        svg_file = Path(tmp) / "eq.svg"
        if not svg_file.exists():
            return ""
        svg = svg_file.read_text()
        return svg[svg.find("<svg"):].strip()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def equation_svg(key: str) -> str:
    """Return cached inline SVG for an equation, compiling it once if needed."""
    cache = EQ_CACHE / f"{key}.svg"
    if cache.exists():
        return cache.read_text()
    svg = _compile_equation(EQUATIONS[key])
    if svg:
        EQ_CACHE.mkdir(parents=True, exist_ok=True)
        cache.write_text(svg)
    return svg


# "where" definitions for each equation's terms (slide/paper style). Each entry is
# (symbol_html, definition_html); HTML is intentional, so not escaped. The base
# (hedonic) entry defines the common terms; later entries define their new terms and
# point back to the baseline for the rest.
_REF_HEDONIC = ("remaining terms", "as in the baseline hedonic specification")

DEFS = {
    "hedonic": [
        ("<em>p</em><sub>it</sub>", "log sale price (sales) or log weekly asking rent (rentals) for transaction <em>i</em> on date <em>t</em>"),
        ('<span class="ov">S</span><sup>250</sup><sub>it</sub>', "average daily spill events across overflows within 250&nbsp;m, January 2021 to the transaction date"),
        ("<em>β</em>", "coefficient of interest — the change in log price/rent per one-unit increase in average daily spill exposure"),
        ("<strong>X</strong><sub>i</sub>, <em>γ</em>", "property characteristics (type, size, tenure, new-build status) and their coefficients"),
        ("<em>δ</em><sub>ℓ(i)</sub>", "location fixed effects (e.g. MSOA or LSOA)"),
        ("<em>α</em>, <em>ε</em><sub>it</sub>", "constant and error term"),
    ],
    "hedonic_qtr": [
        ("<em>λ</em><sub>q(t)</sub>", "quarter-of-sale fixed effects"),
        _REF_HEDONIC,
    ],
    "updown": [
        ('<span class="ov">S</span><sup>U,250</sup><sub>it</sub>, <span class="ov">S</span><sup>D,250</sup><sub>it</sub>', "average daily spill exposure from upstream / downstream overflows within 250&nbsp;m"),
        ("<em>β</em><sub>U</sub>, <em>β</em><sub>D</sub>", "upstream / downstream price gradients"),
        _REF_HEDONIC,
    ],
    "updown_weighted": [
        ('<span class="ov">S</span><sup>U,w</sup><sub>it</sub>, <span class="ov">S</span><sup>D,w</sup><sub>it</sub>', "inverse-river-distance-weighted upstream / downstream exposure"),
        ("<em>ω</em><sub>ic</sub> = 1/<em>d</em><sub>ic</sub>", "weight on overflow <em>c</em>; <em>d</em><sub>ic</sub> is the along-river distance from property <em>i</em> to overflow <em>c</em>"),
        ("other terms", "as in the upstream/downstream specification above"),
    ],
    "nearest": [
        ('<span class="ov">S</span><sup>N</sup><sub>it</sub>', "average daily spills at the nearest overflow <em>N</em>, January 2021 to the transaction date"),
        ("<em>U</em><sub>i</sub>", "indicator = 1 if the nearest overflow is upstream of the property"),
        ("<em>d</em><sub>i</sub>", "along-river distance between the snapped property and overflow points"),
        ("<em>β</em><sub>D</sub>, <em>β</em><sub>D</sub>&nbsp;+&nbsp;<em>β</em><sub>UD</sub>", "price gradient for a downstream / upstream nearest overflow"),
        ("<em>β</em><sub>UD</sub>, <em>θ</em>, <em>ρ</em>", "upstream×exposure interaction, the upstream level shift, and the river-distance coefficient"),
        _REF_HEDONIC,
    ],
    "news_discrete": [
        ("Post<sub>t</sub>", "indicator = 1 for transactions in or after August 2022 (the public-attention peak)"),
        ("<em>κ</em>", "change in the spill–price gradient after the peak"),
        ("<em>λ</em><sub>m(t)</sub>", "transaction-month fixed effects (absorb Post<sub>t</sub> when included)"),
        _REF_HEDONIC,
    ],
    "news_cont": [
        ("<em>A</em><sub>t</sub>", "log cumulative UK news articles on sewage spills up to month <em>t</em>"),
        ("<em>κ</em>", "change in the spill–price gradient with cumulative coverage"),
        ("other terms", "as in the public-attention specification above"),
    ],
    "extensive_discrete": [
        ("Near<sub>i</sub>", "indicator = 1 if the property is near (0–500&nbsp;m) vs far (1000–2000&nbsp;m) from the nearest overflow"),
        ("<em>φ</em>, <em>κ</em>", "the near–far price gap and its change after the attention peak"),
        ("Post<sub>t</sub>, <em>λ</em><sub>m(t)</sub>", "post-peak indicator and transaction-month fixed effects"),
        _REF_HEDONIC,
    ],
    "extensive_cont": [
        ("Near<sub>i</sub>", "near (0–500&nbsp;m) vs far (1000–2000&nbsp;m) from the nearest overflow"),
        ("<em>A</em><sub>t</sub>, <em>κ</em>", "log cumulative articles to month <em>t</em>, and the change in the near–far gap with coverage"),
        ("other terms", "as above"),
    ],
}


def _where_html(key: str) -> str:
    defs = DEFS.get(key)
    if not defs:
        return ""
    items = "\n".join(f"        <li>{sym} — {desc}</li>" for sym, desc in defs)
    return (
        '    <div class="eqn-where"><span class="where-lead">where</span>\n'
        f"      <ul>\n{items}\n      </ul>\n    </div>"
    )


def equations_html(keys: list) -> str:
    parts = []
    for k in keys:
        svg = equation_svg(k)
        if svg:
            parts.append(f'    <div class="eqn">{svg}</div>')
        else:
            parts.append(f'    <div class="eqn"><code>{html.escape(EQUATIONS[k])}</code></div>')
        where = _where_html(k)
        if where:
            parts.append(where)
    return "\n".join(parts)


def build_summary_html() -> tuple[str, int]:
    parts = [
        f'    <h2 id="{SUMMARY["id"]}">{html.escape(SUMMARY["title"])}'
        f'<a class="toptop" href="#top">↑ top</a></h2>',
        f'    <p class="sec-intro">{html.escape(SUMMARY["intro"])}</p>',
    ]
    n = 0
    for sub in SUMMARY["subsections"]:
        h, c = render_blocks(sub["blocks"])
        n += c
        parts.append(
            f'    <h3 class="subsec" id="{sub["id"]}">{html.escape(sub["title"])}</h3>'
        )
        parts.append(h)
    return "\n".join(parts), n


def build_section_html(sec: dict) -> tuple[str, int]:
    parts = [
        f'    <h2 id="{sec["id"]}">{html.escape(sec["title"])}'
        f'<a class="toptop" href="#top">↑ top</a></h2>'
    ]
    if sec.get("intro"):
        parts.append(f'    <p class="sec-intro">{html.escape(sec["intro"])}</p>')
    if sec.get("specs"):
        parts.append(equations_html(sec["specs"]))
    if sec.get("note"):
        parts.append(f'    <p class="tnote eqn-note">{sec["note"]}</p>')
    n = 0
    if sec.get("blocks"):
        h, c = render_blocks(sec["blocks"])
        n += c
        parts.append(h)
    for sub in sec.get("subsections", []):
        parts.append(
            f'    <h3 class="subsec" id="{sub["id"]}">{html.escape(sub["title"])}</h3>'
        )
        if sub.get("specs"):
            parts.append(equations_html(sub["specs"]))
        if sub.get("note"):
            parts.append(f'    <p class="tnote eqn-note">{sub["note"]}</p>')
        h, c = render_blocks(sub["blocks"])
        n += c
        parts.append(h)
    return "\n".join(parts), n


def build_html(toc: str, summary_html: str, sections_html: str, n_tables: int) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Results tables &mdash; house-to-site radius sweep</title>
  <style>{CSS}  </style>
</head>
<body>
  <div class="page" id="top">
    <header>
      <h1>Results tables: house-to-site radius sweep</h1>
      <p class="subtitle">Sewage spill results across house-to-site radii of
      250&nbsp;m, 500&nbsp;m, and 1000&nbsp;m, with a main-results summary first.</p>
      <div class="meta">
        <div><dt>Date</dt><dd>{REPORT_DATE}</dd></div>
        <div><dt>Author</dt><dd>{REPORT_AUTHOR}</dd></div>
        <div><dt>Sample</dt><dd>England, 2021&ndash;2023</dd></div>
        <div><dt>Tables</dt><dd>{n_tables}</dd></div>
      </div>
    </header>
    <main>
{toc}
{summary_html}
{sections_html}
    </main>
    <footer>
      Generated from <code>output/tables/*.tex</code> by
      <code>scripts/python/build_intensive_margin_html_report.py</code>.
      Re-run that script to refresh after the tables are regenerated.
      Significance: <sup class="stars">***</sup>&nbsp;p&lt;0.01,
      <sup class="stars">**</sup>&nbsp;p&lt;0.05, <sup class="stars">*</sup>&nbsp;p&lt;0.1.
    </footer>
  </div>
</body>
</html>
"""


def main() -> None:
    summary_html, n_summary = build_summary_html()
    section_htmls, n_sections = [], 0
    for sec in SECTIONS:
        h, c = build_section_html(sec)
        section_htmls.append(h)
        n_sections += c
    n_tables = n_summary + n_sections

    full = build_html(build_toc(), summary_html, "\n".join(section_htmls), n_tables)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(full)
    print(f"Wrote {n_tables} tables to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
