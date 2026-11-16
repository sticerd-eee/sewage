"""Build the hedonic fixed-effects specifications page from results/fe_grid.json."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
grid = json.loads((HERE / "results" / "fe_grid.json").read_text())

html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hedonic estimates under alternative fixed effects</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.css">
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.js"></script>
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/contrib/auto-render.min.js" onload="renderMathInElement(document.body, {{delimiters: [{{left: '$$', right: '$$', display: true}}, {{left: '\\\\(', right: '\\\\)', display: false}}]}})"></script>
<style>
:root {{
  --ink: #1d1f23; --muted: #6b7078; --faint: #9aa0a8; --rule: #e3e5e8;
  --bg: #ffffff; --hover: #f3f5f8; --accent: #2f5d8a;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--bg); color: var(--ink);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  font-variant-numeric: tabular-nums; }}
main {{ max-width: 1240px; margin: 0 auto; padding: 48px 24px 72px; }}
h1 {{ font-size: 22px; font-weight: 600; letter-spacing: -0.01em; margin: 0 0 6px; }}
h2 {{ font-size: 15px; font-weight: 600; margin: 56px 0 12px; }}
h2.first {{ margin-top: 32px; }}
.intro {{ margin: 0 0 20px; max-width: 900px; }}
.intro p {{ margin: 0 0 6px; }}
.intro ul {{ margin: 0; padding-left: 20px; }}
.intro li {{ margin: 0 0 6px; }}
.intro ol, .intro li ul {{ margin: 6px 0 0; padding-left: 22px; }}
.intro ol li {{ margin: 0 0 2px; }}
.lede {{ color: var(--muted); margin: 0 0 28px; max-width: 760px; }}
.bar {{ display: flex; flex-wrap: wrap; align-items: center; gap: 18px; margin: 0 0 18px; color: var(--muted); font-size: 13px; }}
.toggle {{ display: inline-flex; border: 1px solid var(--rule); border-radius: 6px; overflow: hidden; }}
.toggle button {{ font: inherit; border: 0; background: none; padding: 5px 12px; color: var(--muted); cursor: pointer; }}
.toggle button.on {{ background: var(--ink); color: #fff; }}
.key {{ display: inline-flex; align-items: center; gap: 6px; }}
.key i {{ width: 14px; height: 10px; border: 1.5px solid var(--accent); border-radius: 2px; display: inline-block; }}
.grids {{ display: grid; grid-template-columns: 1fr 1fr; gap: 40px; }}
@media (max-width: 900px) {{ .grids {{ grid-template-columns: 1fr; }} }}
table {{ border-collapse: collapse; width: 100%; }}
caption {{ text-align: left; font-weight: 600; padding: 0 0 8px; }}
thead th {{ font-weight: 500; color: var(--muted); font-size: 12px; text-align: center; padding: 6px 4px; border-bottom: 1px solid var(--ink); }}
thead th.corner {{ text-align: left; }}
tbody th {{ text-align: left; font-weight: 400; color: var(--ink); padding: 8px 8px 8px 0; white-space: nowrap; font-size: 13px; }}
tbody td {{ text-align: center; padding: 7px 4px; }}
tbody tr {{ border-bottom: 1px solid var(--rule); }}
tbody tr.sep {{ border-top: 1.5px solid #8a9099; }}
tbody tr.sep > * {{ padding-top: 14px; border-top: 1.5px solid #8a9099; }}
td .c {{ display: block; }}
td .s {{ display: block; color: var(--muted); font-size: 12px; }}
td .v {{ display: block; color: var(--faint); font-size: 11px; margin-top: 1px; white-space: nowrap; }}
td.na {{ color: var(--faint); }}
td.hl, td.hl.head {{ background: var(--hover); }}
td.head {{ box-shadow: inset 0 0 0 1.5px var(--accent); border-radius: 3px; }}
.grid td:not(.na) {{ cursor: default; }}
.notes {{ color: var(--muted); font-size: 12.5px; margin-top: 22px; max-width: 900px; }}
.notes p {{ margin: 0 0 6px; }}
.spec {{ margin: 0 0 36px; max-width: 900px; }}
.spec .katex-display {{ margin: 0 0 16px; font-size: 1.12em; }}
.spec ul {{ margin: 0; padding: 0; list-style: none; }}
.spec li {{ position: relative; padding: 3px 0 3px 18px; }}
.spec ul.sub {{ margin: 2px 0 0; color: var(--muted); font-size: 13px; }}
.spec li::before {{ content: "–"; position: absolute; left: 0; color: var(--muted); }}
</style>
</head>
<body>
<main>
<h1>Hedonic estimates under alternative fixed effects</h1>

<h2 class="first">Specification</h2>

<div class="spec">
$$\\log p_{{it}} = \\alpha + \\beta\\,\\overline{{S}}^{{250}}_{{it}} + \\boldsymbol{{X}}_i'\\gamma + \\delta_{{\\ell(i)}} + \\lambda_{{m(t)}} + \\mu_{{d(i)m(t)}} + \\varepsilon_{{it}}$$
<ul>
  <li>\\(p_{{it}}\\): sale price or weekly asking rent for transaction \\(i\\) on date \\(t\\)</li>
  <li>\\(\\overline{{S}}^{{250}}_{{it}}\\): average weekly spill events within 250m, Jan. 2021 to transaction date</li>
  <li>\\(\\boldsymbol{{X}}_i\\): property characteristics (type, size, tenure, and new build status)</li>
  <li>\\(\\delta_{{\\ell(i)}}\\): location fixed effects (LSOA, MSOA, or district)</li>
  <li>\\(\\lambda_{{m(t)}}\\): time fixed effects, where \\(m(t)\\) is the year, year-quarter, or year-month of date \\(t\\)</li>
  <li>\\(\\mu_{{d(i)m(t)}}\\): location-by-time fixed effects, where \\(d(i)\\) is the MSOA or district of property \\(i\\)</li>
</ul>
</div>

<h2>Main Hedonic Results</h2>
<div class="intro">
  <p>The table can be read as follows:</p>
  <ul>
    <li>Each cell reports the estimate of \\(\\beta\\) from a separate regression. Columns vary the location fixed effects \\(\\delta_{{\\ell(i)}}\\), while rows vary the time fixed effects \\(\\lambda_{{m(t)}}\\) or location-by-time fixed effects \\(\\mu_{{d(i)m(t)}}\\).</li>
    <li>Each cell reports three statistics:
      <ol>
        <li>The estimated coefficient \\(\\hat{{\\beta}}\\).</li>
        <li>Its standard error (in parentheses), with the estimation method selectable using the buttons.</li>
        <li>Within-variation in \\(\\overline{{S}}^{{250}}_{{it}}\\) after absorbing the corresponding FEs [in square brackets].</li>
      </ol>
    </li>
    <li>Location fixed effects, from smallest to largest:
      <ul>
        <li>LSOA: Census statistical area with 1,000–3,000 residents.</li>
        <li>MSOA: Census statistical area grouping several LSOAs, with 5,000–15,000 residents.</li>
        <li>District: Local government administrative area comprising multiple MSOAs.</li>
      </ul>
    </li>
  </ul>
</div>

<div class="bar">
  <span>Standard errors</span>
  <span class="toggle" role="group" aria-label="Standard errors">
    <button type="button" data-se="conley" class="on">Conley (1km)</button>
    <button type="button" data-se="msoa">MSOA clustered</button>
    <button type="button" data-se="lsoa">LSOA clustered</button>
    <button type="button" data-se="robust">Heteroskedasticity-robust</button>
  </span>
  <span>Property characteristics</span>
  <span class="toggle ctl" role="group" aria-label="Property characteristics">
    <button type="button" data-ctl="1" class="on">Included</button>
    <button type="button" data-ctl="0">Excluded</button>
  </span>
  <span class="key"><i></i> Paper main specification</span>
</div>

<div class="grids">
  <table class="grid" id="t-Sales"></table>
  <table class="grid" id="t-Rentals"></table>
</div>

<div class="notes">
  <p>Sample: properties within 250m of a storm overflow, 2021–2024 (sales) / 2021–2023 (rentals). Dependent variable: log price (sales) or log weekly rent (rentals). Spill exposure: average weekly spill events (12/24 count) from Jan. 2021 to transaction date. Property characteristics: type, new build status, and tenure (sales) / type, bedrooms, and bathrooms (rentals). SEs in parentheses, selected with the buttons: Conley (1km cutoff), clustered by MSOA or LSOA, or heteroskedasticity-robust (HC1). *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.1.</p>
</div>

</main>

<script>
const DATA = {json.dumps(grid)};
const LOCS = ["None", "LAD", "MSOA", "LSOA"];
const TIMES = [
  ["None", "None"], ["Year", "Year"], ["Year-quarter", "Year-quarter"], ["Year-month", "Year-month"],
  ["LAD x year", "District × year"], ["LAD x year-quarter", "District × year-quarter"], ["LAD x year-month", "District × year-month"],
  ["MSOA x year-quarter", "MSOA × year-quarter"]
];
const SEP = new Set(["Year", "LAD x year"]);
let se = "conley", ctl = true;
const st = p => p < 0.01 ? "***" : p < 0.05 ? "**" : p < 0.1 ? "*" : "";
const f4 = x => (x < 0 ? "−" : "") + Math.abs(x).toFixed(4);
const find = (panel, loc, time) => DATA.find(r => r.panel === panel && r.controls === ctl && r.location === loc && r.time === time);

function render() {{
  for (const panel of ["Sales", "Rentals"]) {{
    let h = `<caption>${{panel}}</caption><thead><tr><th class="corner">Time FE</th>` +
      LOCS.map(l => `<th>${{l === "None" ? "No location FE" : l === "LAD" ? "District FE" : l + " FE"}}</th>`).join("") + `</tr></thead><tbody>`;
    for (const [t, label] of TIMES) {{
      h += `<tr${{SEP.has(t) ? ' class="sep"' : ""}}><th scope="row">${{label}}</th>`;
      for (const l of LOCS) {{
        const r = find(panel, l, t);
        if (!r) {{ h += `<td class="na">—</td>`; continue; }}
        const head = ctl && l === "LSOA" && t === "None" ? " head" : "";
        h += `<td class="k${{head}}" data-k="${{l}}|${{t}}" title="N = ${{r.n.toLocaleString("en-GB")}}">` +
          `<span class="c">${{f4(r.coef)}}${{st(r["p_" + se])}}</span>` +
          `<span class="s">(${{r["se_" + se].toFixed(4)}})</span>` +
          `<span class="v">[${{(100 * r.share_var_left).toFixed(1)}}%]</span></td>`;
      }}
      h += `</tr>`;
    }}
    document.getElementById("t-" + panel).innerHTML = h + `</tbody>`;
  }}
}}
render();

document.querySelectorAll(".toggle button").forEach(b => b.addEventListener("click", () => {{
  if (b.dataset.se) se = b.dataset.se; else ctl = b.dataset.ctl === "1";
  b.parentElement.querySelectorAll("button").forEach(x => x.classList.toggle("on", x === b));
  render();
}}));
document.querySelector(".grids").addEventListener("mouseover", e => {{
  const td = e.target.closest("td.k");
  document.querySelectorAll("td.hl").forEach(x => x.classList.remove("hl"));
  if (td) document.querySelectorAll(`td[data-k="${{td.dataset.k}}"]`).forEach(x => x.classList.add("hl"));
}});
document.querySelector(".grids").addEventListener("mouseleave", () =>
  document.querySelectorAll("td.hl").forEach(x => x.classList.remove("hl")));
</script>
</body>
</html>
"""
html = html.replace("<td><span class=\"c\">-", "<td><span class=\"c\">−")
out = HERE.parent / f"{HERE.name}.html"
out.write_text(html)
print(out)
