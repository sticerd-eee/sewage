"""Build the quarter-FE tables PDF: the February 2026 table, then the paper's
current quarter-FE table with LSOA-clustered, MSOA-clustered, and
heteroskedasticity-robust SEs (from results/qtr_fe_alt_se.json)."""
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DROPBOX = Path.home() / "Library" / "CloudStorage" / "Dropbox"
FEB_TABLE = DROPBOX / "01_projects" / "sewage" / "output" / "tables" / "hedonic_count_continuous_prior_qtr_fe.tex"
PAPER_TABLE = DROPBOX / "Apps" / "Overleaf" / "Sewage in Our Waters" / "tables" / "hedonic_count_continuous_prior_qtr_fe_250m.tex"
PDFLATEX = shutil.which("pdflatex") or "/Library/TeX/texbin/pdflatex"
TABLES = HERE / "results" / "tables"

SE_TYPES = {
    "lsoa": ("standard errors clustered by LSOA", "Standard errors clustered by LSOA are reported in parentheses."),
    "msoa": ("standard errors clustered by MSOA", "Standard errors clustered by MSOA are reported in parentheses."),
    "robust": ("heteroskedasticity-robust standard errors", "Heteroskedasticity-robust standard errors are reported in parentheses."),
}
PREAMBLE = r"""\documentclass[11pt]{article}
\usepackage[landscape,margin=1.5cm]{geometry}
\usepackage[T1]{fontenc}
\usepackage{lmodern}
\usepackage{float}
\usepackage{booktabs}
\usepackage[group-separator={,}]{siunitx}
\usepackage{tabularray}
\UseTblrLibrary{booktabs}
\UseTblrLibrary{siunitx}
\usepackage{caption}
\pagestyle{empty}
\begin{document}
"""


def stars(p):
    return "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""


def num(x):
    return f"\\num{{{x:.3f}}}"


def alt_table(template, rows, key):
    caption, sentence = SE_TYPES[key]
    c1, c7 = rows[0], rows[6]
    blank = " &  &  &  &  & "
    t = re.sub(r"caption=\{[^}]*\}", f"caption={{Hedonic estimates with quarter fixed effects: {caption}}}", template, count=1)
    t = re.sub(r"label=\{[^}]*\}", f"label={{tbl:qtr-fe-{key}}}", t, count=1)
    t = t.replace("Conley standard errors with a 1 km cutoff are reported in parentheses.", sentence)
    t = re.sub(r"^Constant & .*$", lambda _: f"Constant & {num(c1['c_coef'])}{stars(c1['c_p'])}{blank} & {num(c7['c_coef'])}{stars(c7['c_p'])}{blank} \\\\", t, flags=re.M)
    t = re.sub(r"^& \(\\num\{[0-9.]+\}\) &  &.*$", lambda _: f"& ({num(c1['c_se'])}){blank} & ({num(c7['c_se'])}){blank} \\\\", t, flags=re.M)
    t = re.sub(r"^Spills per week \(avg\.\) & .*$", lambda _: "Spills per week (avg.) & " + " & ".join(num(r["coef"]) + stars(r["p"]) for r in rows) + " \\\\", t, flags=re.M)
    t = re.sub(r"^& \(\\num\{-?[0-9.]+\}\) & \(.*$", lambda _: "& " + " & ".join(f"({num(r['se'])})" for r in rows) + " \\\\", t, flags=re.M)
    if sentence not in t:
        raise ValueError(f"SE note not replaced for {key}")
    return t


res = json.loads((HERE / "results" / "qtr_fe_alt_se.json").read_text())
template = PAPER_TABLE.read_text()
TABLES.mkdir(parents=True, exist_ok=True)
shutil.copy(FEB_TABLE, TABLES / "feb2026_qtr_fe.tex")
names = ["feb2026_qtr_fe"]
for key in SE_TYPES:
    rows = sorted((r for r in res if r["se_type"] == key), key=lambda r: (r["panel"] != "Sales", r["col"]))
    (TABLES / f"qtr_fe_{key}.tex").write_text(alt_table(template, rows, key))
    names.append(f"qtr_fe_{key}")

with tempfile.TemporaryDirectory() as tmp:
    body = "\n\\clearpage\n".join(f"\\input{{{TABLES / n}}}" for n in names)
    (Path(tmp) / "main.tex").write_text(PREAMBLE + body + "\n\\end{document}\n")
    for _ in range(2):
        subprocess.run([PDFLATEX, "-interaction=nonstopmode", "main.tex"], cwd=tmp, check=True, stdout=subprocess.DEVNULL)
    out = HERE.parent / f"{HERE.name}-qtr-fe-tables.pdf"
    shutil.copy(Path(tmp) / "main.pdf", out)
print(out)
