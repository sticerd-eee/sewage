# Hedonic estimates under alternative fixed effects

This folder holds the page sent to Clare in November 2026 and the quarter
fixed effects tables PDF that goes with it. Both are robustness checks on the
paper's main hedonic specification: log price on average weekly spill events
within 250m, from January 2021 to the transaction date.

## Outputs

- `../2026-11-16-001-hedonic-fe-specifications.html` is the results page. It
  shows sales and rentals estimates for every combination of location fixed
  effects (none, district, MSOA, and LSOA) and time fixed effects (none, year,
  year-quarter, year-month, year plus calendar quarter, year plus calendar
  month, district by each of these, and MSOA by year-quarter). Buttons switch between four standard errors and toggle the
  property characteristics. It is gitignored.
- `../2026-11-16-001-hedonic-fe-specifications-qtr-fe-tables.pdf` has four
  pages. Page 1 is the February 2026 quarter fixed effects table, compiled
  unchanged from its original file. Pages 2 to 4 are the paper's current
  quarter fixed effects table with standard errors clustered by LSOA,
  clustered by MSOA, and heteroskedasticity-robust. It is gitignored.
- `results/fe_grid.json` and `results/qtr_fe_alt_se.json` hold the estimates,
  and `results/tables/` holds the `.tex` tables in the PDF. These are
  committed, so the page and PDF can be rebuilt without rerunning the
  regressions.

## Run order

From the repository root:

```bash
Rscript docs/reports/2026-11-16-001-hedonic-fe-specifications/hedonic_fe_grid.R
Rscript docs/reports/2026-11-16-001-hedonic-fe-specifications/hedonic_qtr_fe_alt_se.R
python3 docs/reports/2026-11-16-001-hedonic-fe-specifications/build_page.py
python3 docs/reports/2026-11-16-001-hedonic-fe-specifications/build_tables_pdf.py
```

The grid script runs 164 regressions and takes a few minutes. The page loads
KaTeX from a CDN to typeset the equations, so it needs a network connection
to display them.

## Inputs

- `data/processed/house_price.parquet` and
  `data/processed/zoopla/zoopla_rentals.parquet` for prices, dates, and
  property characteristics.
- `data/processed/cross_section/{sales,rentals}/prior_to_{sale,rental}` for
  spill exposure at the 250m radius.
- `build_tables_pdf.py` also reads two tables from Dropbox: the February 2026
  table in `01_projects/sewage/output/tables/` and the paper's current quarter
  fixed effects table in the Overleaf `tables/` folder, which it uses as the
  template for pages 2 to 4.

## Definitions

- District is the local authority district, taken from the LSOA name.
- Calendar quarter and calendar month are the quarter (1 to 4) and month (1 to
  12) of the year, which capture seasonality separately from the year.
- The within-variation share is the variance of spill exposure after
  absorbing a cell's fixed effects, divided by its total variance in the same
  sample.
- Conley standard errors use a 1km cutoff (`spatial_vcov()`). Robust standard
  errors are HC1. Location-by-time fixed effects that absorb the column's
  location fixed effects are not estimated. District-by-time fixed effects
  already absorb district fixed effects, so the page shows those runs under
  District FE and leaves No location FE empty in those rows.
