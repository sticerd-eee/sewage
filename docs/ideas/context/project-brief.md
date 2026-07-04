# Project Brief: Sewage Spills and Housing Markets in England

This brief is the factual foundation for a research ideation exercise. It describes the current state of the paper draft ("Sewage in Our Waters", coauthored with Balboni and Dhingra at the London School of Economics) and of the analysis repository at `/Users/jacopoolivieri/projects/sewage/`. It describes what exists; it does not propose ideas. Sources are the LaTeX draft files in the Overleaf folder (`/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/`) and the repository itself. All quotations are verbatim from the draft as of 2026-07-04.

**Reviewed and corrected by Jacopo at Checkpoint 1 (2026-07-04).** The corrections: the data inventory in Section 3 was rebuilt around the datasets the analysis scripts actually load (the data folder contains stale sets, including all of `data/final/`); the hydraulics instrument has genuinely not been started (Section 4.6); `105_identification_strategies_comparison.tex` is simply the section that compares results across the different identification tests; and the welfare model is at the very beginning of its development (Section 5).

---

## 1. Research question as currently framed

The introduction (`01_introduction.tex`, five sentences long — it is a stub, not a full introduction) frames the paper as follows:

> "Sewage releases may have both local and downstream impacts on environmental amenities and public health, but welfare impacts are difficult to quantify given their non-market characteristics and limited data on sewage spill events. We use hedonic estimation and a novel dataset — comprising the location and timing of over 1 million spill events, 3.15 million sales, and 1.45 million rental transactions from 2021–2023 — to infer the welfare costs associated with sewage spills. ... We simulate policy counterfactuals to consider how these estimates might inform policy decisions regarding infrastructure investment, regulation, and enforcement in the water industry."

The research-question section (`06_research_question.tex`) lists four policy counterfactuals, each posed as a subsection heading with mostly placeholder text underneath:

1. "What are aggregate welfare gains from eliminating all sewage spills?" — motivated by a "'quantification gap'" in assessing the gains from sewer upgrades, whose engineering costs are known.
2. "What are aggregate welfare gains from eliminating dry spills?" — to inform penalties and enforcement for illegal dry-weather discharges.
3. "Could the regulator improve aggregate welfare by reallocating sewage spills?" — holding total discharge volume fixed but redistributing it across space.
4. "How far might local versus national regulation and enforcement account for the level and distribution of sewage spills?"

The section contains bracketed to-do notes for the second, third, and fourth questions (e.g. "[What does the engineering literature suggest about how far sewage can be reallocated across space?]"), indicating these counterfactuals are aspirations rather than implemented exercises. Named candidate counterfactuals include "More storage tanks, Thames Tideway Tunnel, DWMP" (Drainage and Wastewater Management Plans).

---

## 2. Institutional setting in brief

What an outside economist needs to know, drawn from `02_background_context.tex` and `102_appendix_dry_spills.tex`:

**Combined sewers and storm overflows.** Much of England's sewer network is a combined system, largely Victorian in origin, in which household wastewater and rain runoff share the same pipes. During heavy rain the combined flow can exceed pipe or treatment capacity. Permitted relief points called Combined Sewer Overflows (CSOs; the draft uses "storm overflow" interchangeably) then discharge untreated or partially treated sewage into rivers and coastal waters. Discharges are largely automatic — triggered when water overtops a weir — not discretionary. Four design features determine when an overflow triggers: pipe diameter and slope, gravity and topography, weir height, and upstream catchment load; all are essentially predetermined legacies of the installed system.

**Water companies and regulators.** Ten private Water and Sewerage Companies operate England's sewer network. The Environment Agency (EA) is the environmental regulator; it issues environmental permits for each overflow under the Environmental Permitting Regulations 2016, specifying (among other things) a minimum "pass forward flow" to treatment before a spill is permitted. New CSOs are effectively prohibited; removing one requires major capital works (storage tanks, interceptor tunnels such as the GBP 4.13 billion Thames Tideway Tunnel, sewer separation). The UK government estimates full separation of wastewater and stormwater systems in England would cost GBP 350–600 billion.

**Event Duration Monitoring (EDM).** Following a 2012 European Commission infringement action (case C-301/10) over CSOs discharging in "normal and common" weather, the Environment Minister in 2013 instructed companies to install monitors on the vast majority of storm overflows by 2020; roughly 12,000 of about 15,000 overflows were monitored by that deadline. The Environment Act 2021 required EDMs at all overflow sites by end-2023 and mandated annual publication of the data. An EDM records the start and end timestamp of each discharge event. The EA's "12/24 counting methodology" converts continuous discharges into event counts: the first 12 hours of continuous discharge is one event, and each subsequent 24-hour block is an additional event.

**Dry spills.** A discharge is classified as a "dry spill" when it begins on a day with less than 0.25 mm of rainfall on that day and the preceding day. Dry spills are potentially unlawful (less diluted, discharged into lower-flow rivers) and can trigger EA investigation and enforcement. The draft counts 39,983 possible dry spills over 2021–2023, about 3% of all spills, and replicates the EA's published dry-spill counts closely (19,910 estimated versus roughly 20,000 published for 2022) using a 3-kilometre-by-3-kilometre rainfall grid around each overflow.

**Descriptive facts worth knowing** (`03_motivating_evidence.tex`):
- Overflows are widely dispersed; 5% of the English population lives within 250 metres, and 16% within 500 metres, of an overflow that spilled during 2021–2023.
- Spills are highly persistent within site: overflows largely stay in the same annual spill-count quartile year to year. The draft itself concludes that "exposure to sewage spills can largely be viewed as a cross-sectional feature of a location, and there is unlikely to be a meaningful dynamic relationship between individual spill events and changes in house prices."
- Wet and dry spill locations are closely correlated, "suggesting that dry spills (like wet spills) may be largely driven by capacity issues rather than discretionary releases."
- Spill probability responds mainly to rainfall on the current and previous day; conditional on a spill occurring, duration varies little with rainfall intensity.
- Public attention (Google Trends and LexisNexis news counts) peaked in August 2022 and stayed elevated afterwards.

---

## 3. Data inventory

**Corrected at Checkpoint 1 (2026-07-04).** The data folder is messy and contains stale datasets; presence under `data/` does not mean a dataset is current. In particular, `data/final/dat_panel_house` is an old, stale dataset and must not be used, and the `dat_complete_event`, `dat_complete_hedonic`, `dat_panel_house_mo`, `dat_panel_house_nearest`, `dat_panel_house_weighted`, and `dat_panel_site` families in `data/processed/` are not referenced by any current analysis script and should be treated as superseded. **The authoritative definition of "data in use" is what the analysis scripts in `scripts/R/09_analysis/` load** (the loader is `scripts/R/09_analysis/00_data_load/load_data_sewage.R`; the individual analysis scripts load further files directly). The inventory below is built from those scripts.

Units and schemas below were verified by reading the parquet files directly. Time coverage is 2021–2023 for the core analysis sample (EDM raw data extends into 2024+).

### Datasets loaded by the current analysis (`scripts/R/09_analysis/`)

All paths are relative to `data/processed/` unless stated otherwise.

| Dataset | Location | Unit of observation | Key variables |
| --- | --- | --- | --- |
| General panel | `general_panel/{sales,rentals}/` (Arrow dataset, partitioned by `radius` = 250/500/1000 m) | House-by-quarter spine (256.2 million rows for sales across radii); panel unit is the house, `qtr_id_transfer` marks the transaction quarter | `house_id`, `site_id`, `qtr_id`, `qtr_id_transfer`, `distance_m`, `within_radius` |
| Within-radius panel | `within_radius_panel/{sales,rentals}/` (Arrow dataset, partitioned by `radius` and `period_type` = quarterly/monthly) | Spill-site-by-house-by-period spine (41.9 million rows for sales); panel unit is the spill site | `site_id`, `house_id`, `month_id`, `qtr_id`, `distance_m` |
| House sales | `house_price.parquet` | Sale transaction (3,175,951 rows) | Land Registry Price Paid fields, geocoded coordinates, LSOA/MSOA |
| Zoopla rentals | `zoopla/zoopla_rentals.parquet` | Rental listing (1,450,255 rows) | Listing price, bedrooms/bathrooms, EPC rating, listing/rented dates, coordinates (restricted data) |
| Spill aggregates | `agg_spill_stats/agg_spill_{mo,qtr,yr}.parquet` and `agg_spill_dry_{mo,qtr,yr}.parquet` | Spill site by month/quarter/year | `spill_count`, `spill_hrs`; the `_dry_` files add dry-spill counts and hours under weak/strict definitions, at two rainfall radii (`r0`/`r1`) and two day-windows (`d01`/`d0123`), plus rainfall-category (dry/moderate/heavy/very heavy) splits, EA site metadata (receiving water, shellfish and bathing water flags, EDM commission date and operation percentage) |
| Spill statistics | `agg_spill_stats/agg_spill_stats_{mo,qtr}.parquet` | Spill site by period | Logged outcomes and treated-group threshold indicators (`thr_*`/`d_*` at p50/p75/p90/mean/max, defined at quarterly, yearly, and full-sample frequencies) for spills and dry spills |
| Cross-sections | `cross_section/{sales,rentals}/{all_years,prior_to_sale,prior_to_sale_house_site,prior_to_rental,prior_to_rental_rental_site}/` | Property (3,175,951 rows for sales), several exposure windows | `spill_count`, `spill_hrs`, daily/weekly averages, `n_spill_sites`, min/mean distance |
| Long differences | `long_difference/long_diff_grid_{house_sales,rentals}.parquet` | 250-metre grid cell | Changes in prices/rents and spill exposure, 2021–2023 |
| Repeat transactions | `repeated_transactions/repeated_{sales,rentals}.parquet` | Repeat-transaction pair | Price changes and exposure changes between consecutive transactions of the same property |
| Rainfall | `rainfall/rainfall_agg_mo.parquet`, `rainfall_data_cleaned.parquet`, `spill_blocks_rainfall_yr.parquet`, `spill_site_grid_lookup.parquet` | Site or grid cell by period | HadUK-Grid derived rainfall aggregates and the site-to-grid lookup |
| LexisNexis news | `lexis_nexis/search1_monthly.parquet` | Month (221 rows, about 2007–2025) | `article_count` per month from the Boolean sewage-news query (restricted data) |
| Lookups | `spill_house_lookup.parquet`, `zoopla/spill_rental_lookup.parquet`, `unique_spill_sites.parquet` | Site-house / site-rental pairs; canonical spill site (15,492 rows) | Distances; site coordinates, water company, EDM commission date, per-year monitor operation percentages |
| Upstream/downstream distances | `upstream_downstream/output/18-03/river_filter/spill_{house,rental}_signed.csv` (repository root, not `data/`) | Spill-site-by-house (or rental) pair | Signed river distance, upstream/downstream direction |
| Raw inputs used directly | `data/raw/google_trends/google_trends_uk.xlsx`, `data/raw/haduk_rainfall_data/`, `data/raw/rivers/OSRivers_shapefile/`, `data/raw/shapefiles/msoa_bcg_2021/` | — | Google Trends attention series, HadUK rainfall, Ordnance Survey Open Rivers network, MSOA boundaries |

**How the loader assembles analysis data** (`load_data_sewage.R`): it joins the general panel (at a chosen radius, 250 m by default) with transactions (`house_price.parquet` or `zoopla_rentals.parquet`) on house/rental and quarter, and with quarterly spill aggregates on site and quarter, producing in-memory frames `dat_sales`, `dat_rent`, `dat_sales_within`, `dat_rent_within`. The within-radius versions use the site-level panel spine instead.

### Upstream pipeline outputs (inputs to the sets above; authoritative but not loaded by the analysis directly)

| Dataset | Location | Unit of observation | Key variables |
| --- | --- | --- | --- |
| `combined_edm_data` | `combined_edm_data.parquet` (and `.RData`) | Individual spill event (7,383,972 rows, 2021–2024) | `water_company`, site names/permit references, `start_time`, `end_time`, `event_duration_in_hours`, site identifiers |
| `annual_return_edm` | `annual_return_edm.parquet` | Annual-Return Site (site-by-reporting-year; 57,865 rows, 2021–2024) | Permit references, outlet National Grid Reference, receiving water name, shellfish/bathing water flags, `edm_commission_date`, EA spill count and hours, EDM operation percentage, cross-year `site_id_2021...site_id_2024` |
| `consent_discharges_db` | `consent_discharges_db.csv` (and `.RData`) | Discharge consent record (permit-outlet-effluent) | Permit reference, discharge NGR, receiving water and environment type, effluent type, permit status, issued/effective/revocation dates |
| `spills_with_rainfall_metrics` | `spills_with_rainfall_metrics.parquet` | Spill event (6,596,486 rows, 2021–2023) | `spill_id`, rainfall at closest grid cell and 9-cell maxima on day 0, days 0–1, days 0–3 (the dry-spill classification inputs) |
| `UpstreamDownstream` | `UpstreamDownstream/spill_house_signed_with_lateral.csv` | Spill-site-by-house pair | `signed_dist_m` (river distance), `direction` (upstream/downstream), lateral distances of site and house from the river |
| `Rivers` | `Rivers/*.parquet`, `river_lines_england.gpkg` | River segment (80,066 line geometries) plus flow tables | Ordnance Survey Open Rivers network with flow direction |
| `lexis_nexis` articles | `lexis_nexis/search1_articles.parquet` | Article | Article-level records behind the monthly counts (restricted data) |

Other processed assets exist (dry-spill aggregates, never-spilled sites, population statistics, a random-forest model object `rf_model_2023_2024.rds`, shapefiles, cached upstream/downstream computations, and `_old/` folders) but are not part of the current analysis chain.

**Underlying raw assets combined by the project:** Environment Agency EDM spill records (2021–2024 plus a live API feed), EA discharge consents, Met Office HadUK-Grid daily rainfall, HM Land Registry Price Paid sales, Zoopla/CDRC rental listings (restricted), and LexisNexis news exports (restricted).

**Gaps visible in the inventory:** no wastewater-treatment-works catchment boundaries, treatment-capacity (population equivalent / flow-to-full-treatment) data, elevation rasters, or impervious-surface data are present in `data/processed/`, even though the hydraulics instrument and the model (Sections 4 and 5 below) require all of them. The Surfers Against Sewage alert data mentioned in the draft is not in the repository. No health, water-quality, or bathing-water outcome data are present.

---

## 4. Identification designs currently in play

The draft is explicit that the baseline correlations are not causal and works through a sequence of designs. Its own headline internal summary is the identification-strategies comparison table (`105_identification_strategies_comparison.tex`, which is a five-line wrapper that inputs `tables/strategies_comparison.tex` — the table itself is substantive, not a stub).

### 4.1 Baseline hedonic cross-section (`03_motivating_evidence.tex`)

Exposure is average weekly spill events at overflows within 250 metres, from 1 January 2021 to the transaction date:

```
log p_it = alpha + beta * Sbar^250_it + X_i' gamma + delta_l(i) + epsilon_it
```

where `Sbar^250_it = (1/W_t) * sum over overflows c within 250m, sum over days tau, of S_c,tau` and `S_c,tau` uses the 12/24 counting rule. Headline: "In the most conservative specification, one additional spill per week (equivalent to approximately 1.2 standard deviations of exposure) is associated with 0.8% lower house prices (column 6), while the corresponding rental estimate is 0.6% lower." The comparison table reports the preferred specification (property controls plus LSOA fixed effects) as a raw coefficient of −0.06*** with a one-standard-deviation effect of −0.7% for sales, and a specification range across columns of −3.9% to −0.7%. Acknowledged weakness: "endogenous siting of sewage spill outlets" and spatially correlated unobservables; the coefficients attenuate substantially with location fixed effects, and rental estimates "attenuate towards zero once location fixed effects are included."

### 4.2 Time-series designs that produce nulls (`03_motivating_evidence.tex`)

- **Grid-cell long differences 2021–2023** (250-metre grid): "the coefficients on changes in spill counts or hours are very close to zero." The comparison table records 0.000 and an implied one-standard-deviation effect of 0.0%.
- **Repeat sales following Palmquist (1982)**, with Bailey–Muth–Nourse time dummies and a depreciation adjustment: "coefficients on changes in spill counts or spill hours are small and statistically indistinguishable from zero" (the comparison table shows −0.001*, implied −1.8% per standard deviation, so marginally significant in the preferred specification).

The draft attributes these nulls to spill persistence: "the persistence of spills within locations ... may limit our scope to exploit time series variation." This is the central empirical tension of the paper: the credible within-location variation is nearly nonexistent, and the cross-sectional variation is confounded.

### 4.3 Upstream versus downstream exposure (`04_causal_impacts.tex`)

Uses river flow direction as a source of exogeneity (pollution flows one way), snapping houses and overflows to the Ordnance Survey Open Rivers network, excluding tidal rivers and lakes, and requiring river distance below 1 kilometre. The estimating equation is:

```
log p_it = alpha + beta_U * Sbar^{U,250}_it + beta_D * Sbar^{D,250}_it + X_i' gamma + delta_l(i) + epsilon_it
```

Headline: "In the baseline specification (columns 1 and 7), one additional spill per week in upstream river waters is associated with 1.7% lower sale prices and 2.1% lower rents, while one additional spill per week in downstream river waters is associated with 2.6% lower sale prices and 1.3% lower rents." Weaknesses acknowledged in the text: for sales, downstream estimates are *larger* than upstream ones — the wrong pattern if river transport of pollution is the mechanism; inverse-river-distance weighting "attenuates the coefficient magnitude and statistical significance" and the downstream weighted coefficient "is negligible across all specifications."

A nearest-site variant interacts nearest-overflow exposure with an upstream indicator and controls for river distance:

```
log p_it = alpha + beta_D * Sbar^N_it + theta * U_i + beta_UD * (Sbar^N_it x U_i) + rho * d_i + X_i' gamma + delta_l(i) + epsilon_it
```

Headline: "the upstream interaction implies 2.8% lower sale prices and 1.5% lower rents relative to a downstream nearest site." This is the pattern consistent with the pollution-transport mechanism, and the comparison table's preferred entry is −0.15***, implied −1.5% per standard deviation for sales — but the draft notes the interaction loses significance with LSOA fixed effects, and the specification range spans −2.1% to +0.6%.

### 4.4 Publicity / salience difference-in-differences (`04_causal_impacts.tex`)

Exploits the August 2022 peak in media coverage and Google searches, following the information-quasi-experiment logic of Davis (2004):

```
log p_it = alpha + beta * Sbar^250_it + kappa * (Sbar^250_it x Post_t) + X_i' gamma + delta_l(i) + lambda_m(t) + epsilon_it
```

plus a continuous version interacting exposure with `A_t = log(cumulative LexisNexis articles up to month t)`. Result, in the draft's own words: the interaction "is negative and significant in the specifications without fixed effects (columns 1 and 2), but is attenuated and loses significance once location and time fixed effects are included. For rents, the corresponding interaction is small and not statistically significant." The section also documents Surfers Against Sewage real-time alert data as a possible future attention measure (historical alerts from 2019 have reportedly been agreed for provision but are not yet in hand).

### 4.5 Policy reforms (placeholder)

A subsection sketches event studies around EDM monitor rollout (2015–2020 under the Asset Management Plan 6 investment programme; commissioning dates are in the EA data) and the Thames Tideway Tunnel phases. It is explicitly a to-do list in red text ("JACOPO: look into timing of Event Duration Monitors... Event studies around opening of each phase of Thames Tideway Tunnel?"). A long historical-instruments section (Victorian outfall placement from 1890s Ordnance Survey maps; sewer-path circuitousness) is present but entirely commented out.

### 4.6 The hydraulics/rainfall instrument (`05_hydraulics_instrument.tex`)

This is the flagship prospective design and one of the two binding constraints. The draft states the problem it solves:

> "The key econometric challenge is that spills are highly persistent at a given CSO... Most of the identifying variation is cross-sectional... With transaction-level data and house fixed effects, all that cross-sectional variation is wiped out... But without house fixed effects, there is a risk of confounding neighbourhood quality with sewage spills — the CSOs that spill most are probably in older, denser, poorer urban areas."

**Construction.** Three spatial layers: wastewater treatment works (WwTW) catchments determine how much hydraulic stress rainfall generates ("How much stress is generated?"); CSOs within the catchment differ in predetermined vulnerability ("Where is it released?"); houses are exposed through river-network connectivity, not sewer connectivity ("Who is affected?"). The instrument is the triple interaction:

```
Z_cq = R_w(c)q x CapacityConstraint_w(c) x Vulnerability_c
```

with first stage

```
S_cq = pi * Z_cq + alpha_c + lambda_w(c)q + eta_cq
```

where `alpha_c` are CSO fixed effects and `lambda_w(c)q` are WwTW-by-quarter fixed effects, so "identification comes from comparing how the same rainfall shock affects CSOs that differ in predetermined vulnerability." Predicted spills map to houses via river-distance weights, `Ehat_iq = sum_c omega_ic * Shat_cq`, and the second stage is

```
log p_it = alpha + beta * Ehat_iq(t) + X_i' gamma + delta_l(i) + lambda_q(t) + epsilon_it
```

**Proposed instrument components.** WwTW level: designed Population Equivalent (PE, from the Urban Waste Water Treatment Regulations dataset), the load ratio `LoadRatio_w0 = ActualPE_w0 / DesignedPE_w0`, and Flow-to-Full-Treatment (FFT) permits. CSO level: baseline spill frequency `Spill_c0` (acknowledged as reflecting "long-run infrastructure quality, which correlates with neighbourhood quality") and, preferred, elevation-based hydraulic slack: the available gradient `(z_c − z_w)/L_cw` versus the minimum self-cleansing slope `S_min ≈ 1/500`, normalised within catchment as `E_cw = max(0, S_min − S_cw) / max over c' in w of the same object`, measurable from the OS Terrain 5 digital elevation model and Hoffmann et al. (2025) catchment data with a 1.3 urban tortuosity factor. Extensions: pumping provisions, geological soil permeability (HOST classification), impervious surface. House-level exposure weights: `f(d_hc, Upstream_hc, Tidal_hc)`, e.g. exponential decay `Upstream_hc × exp(−d_hc/kappa) × (1 − Tidal_hc)`.

**Acknowledged threats** (the draft's own list): rainfall directly affecting prices (flooding, dampness) — addressed with rainfall controls in the second stage; infrastructure correlated with neighbourhood quality — MSOA fixed effects and balance tests; river proximity as an amenity — river-geography controls and upstream placebos. The draft also flags a time-aggregation dilemma: buyers capitalise long-run expected spill frequency (arguing for annual or three-year averages), but "annual aggregation reduces identifying variation and weakens the quasi-experimental rainfall × stress design."

**Status.** The entire section is design, notation, TikZ diagrams, and tables of intended measures. No first-stage regression has been estimated; no instrument components have been constructed; the required WwTW catchment, elevation, PE/FFT, and soil data are not in the repository's processed data. Confirmed by Jacopo at Checkpoint 1: implementation of this design has not begun.

---

## 5. State of the welfare model section

This is the second binding constraint. Two documents exist.

### 5.1 `07_model.tex` — "A Quantitative Spatial Equilibrium with Inherited Sewer Infrastructure"

A two-period quantitative spatial model in the style of Monte, Redding and Rossi-Hansberg (2018), with one addition: an "infrastructure transmission technology" `D_t = T(I^V, E_t, R_t)` mapping inherited Victorian infrastructure `I^V`, the modern urban equilibrium `E_t`, and rainfall `R_t` into sewage-spill exposure `D_t`, which enters residential utility as a disamenity.

Components: (i) a Victorian planner chooses infrastructure to minimise lifetime sanitation costs subject to gravity constraints (`S_e = (z_u − z_v)/l_e ≥ S_min`) and capacity sized to historical loads; (ii) a canonical modern block — CES production with agglomeration, gravity goods trade, Fréchet idiosyncratic preferences over residence-workplace pairs, commuting costs, housing market clearing, endogenous amenity spillovers, multiple skill groups with group-specific disutility of exposure `psi_g`; (iii) a hydraulic block where load `Q_wt = l_H N_wt + l_F L_wt + phi_R R_wt + phi_I I_wt R_wt`, hydraulic pressure `H_ct = a_c (1 + rho_E E_c^0) Q_w(c)t`, spills `Spill_ct = (H_ct − K_c)^+`, and house exposure `D_ht = sum_c omega_hc Spill_ct`; (iv) a hedonic equation derived from within-neighbourhood indifference, `log p_ht = alpha_j(h)t + X_h' xi − delta * Dtilde_ht + u_ht` with `delta = chi / beta_p^g`, so "the hedonic coefficient measures the marginal disutility of exposure scaled by price sensitivity"; (v) welfare as Fréchet expected utility, with the money-metric approximation `CV_g^GE ≈ Nbar_g (ybar_g^0 / beta_w^g)(log U_g^1 − log U_g^0)`, contrasted with the capitalisation object `CV^House ≈ sum_h delta p_h (D_h^0 − D_h^1)`; and (vi) a proposition showing rainfall shocks transmit heterogeneously through predetermined hydraulic vulnerability, which "motivates the empirical instrument" — i.e. the model is built to rationalise the Section 5 instrument.

### 5.2 `106_model_logit.tex` — the recent logit appendix

A rewritten version of the same model. What it adds or changes relative to `07_model.tex`:

- **Logit rather than Fréchet sorting.** Indirect utility is additive in logs with Type-I extreme value shocks, giving standard logit choice probabilities; the earlier version's multiplicative Fréchet structure is replaced.
- **Explicit two-skill CES production** (`H`/`L` with share parameters `theta_i^g`), where `07_model.tex` had a generic multi-group production function.
- **An explicit exposure decomposition** `D_ht = Dbar_j(h)t + Dtilde_ht`, with the neighbourhood average entering the sorting margin and only the within-neighbourhood deviation capitalised into prices. The within-neighbourhood exposure component is written in terms of demeaned weights: `Dtilde_ht = sum_c (omega_hc − omegabar_j(h)c) Spill_ct`, and correspondingly the model-implied instrument is demeaned within neighbourhood: `Z_ht = sum_c (omega_hc − omegabar_j(h)c)[R_w(c)t × Stress^0_w(c) × E_c^0]`.
- **A full sewer-network flow formulation** (node-level conservation of flow, pipe capacities, routing weights) is drafted but commented out, with the reduced form `Spill_ct = max{0, E_c Q_w(c)t − K_c}` retained.
- **A welfare decomposition** `CV^GE = CV^direct + CV^sorting + CV^housing + CV^productivity`, plus the statement that "in environments with limited mobility and small changes in neighbourhood composition, the capitalization measure provides a close approximation to welfare."
- **A structured threats-to-identification section** organised through the model (direct rainfall effects; historical infrastructure correlated with current disadvantage; endogenous population and hydraulic load; productivity shocks; impervious surfaces; river amenities), arguing the three-layer network structure disciplines generic rainfall-vulnerability confounds.

### 5.3 What is assumed and what is missing

- Both versions are purely theoretical: **no parameter is calibrated or estimated, no quantitative counterfactual is computed, and no section connects model objects to specific datasets** beyond the hedonic coefficient `delta`. The counterfactuals of `06_research_question.tex` (eliminating spills, eliminating dry spills, reallocating spills) are not set up as model exercises anywhere.
- The Victorian planner problem is stylised and never solved or taken to data; it functions as a narrative justification for treating infrastructure as predetermined.
- The hydraulic block assumes a specific functional form (piecewise-linear threshold spills) with parameters (`l_H, l_F, phi_R, phi_I, rho_E, K_c`) that have no measurement strategy stated.
- Housing supply `H_j(r)` is left as an unspecified upward-sloping function. Dry spills — half of the policy motivation — do not appear in the model at all: the hydraulic block generates only rainfall-driven spills.
- The two documents are partially redundant and use inconsistent notation (Fréchet `epsilon_g` and `psi_g` versus logit `lambda_g`; `CV^House` versus `CV^capitalization`), and no choice between them is recorded in the draft.

---

## 6. What the draft claims versus what it currently shows

A candid gap list, sourced to sections:

1. **"We use hedonic estimation ... to infer the welfare costs associated with sewage spills" (Section 1 / `01_introduction.tex`).** No welfare cost has been computed. The model (`07_model.tex`, `106_model_logit.tex`) defines welfare objects but nothing is estimated or calibrated, and the hedonic coefficient that would feed `CV^capitalization` is unstable across specifications (Sections 3–4 of the draft).
2. **"We simulate policy counterfactuals" (`01_introduction.tex`).** No counterfactual simulation exists. `06_research_question.tex` is a list of headings with bracketed to-do notes.
3. **Causal language versus associational evidence.** `04_causal_impacts.tex` is titled "Causal impacts of spills on house prices," but its own results narrative is associational ("is associated with") and the two designs that use plausibly exogenous time variation — long differences and repeat sales (`03_motivating_evidence.tex`) — yield nulls, which the draft attributes to spill persistence. The draft itself concedes in `03_motivating_evidence.tex` that exposure "can largely be viewed as a cross-sectional feature of a location."
4. **The upstream/downstream design gives mixed signs.** In the all-sites specification, downstream exposure is more negative than upstream exposure for sales (`04_causal_impacts.tex`, Table discussion), the opposite of the pollution-transport prediction; the sign pattern flips in the nearest-site interaction specification, and that interaction loses significance with LSOA fixed effects. Distance weighting attenuates everything.
5. **The salience results do not survive fixed effects.** Both publicity specifications (`04_causal_impacts.tex`) lose significance once location and month fixed effects are included, and rents show nothing.
6. **The hydraulics instrument is a design document, not a result.** `05_hydraulics_instrument.tex` contains no estimates. Its required inputs (WwTW catchments, designed Population Equivalent, Flow-to-Full-Treatment, elevation, soil permeability) are absent from `data/processed/`. The instrument's core notation (`f(·)` for two different functions, quarterly versus annual timing) is unsettled, and the draft itself flags the tension that annual aggregation — which it argues is conceptually right for capitalisation — "weakens the quasi-experimental rainfall × stress design."
7. **The model and the empirics are not yet consistent.** The model implies only within-neighbourhood exposure deviations are capitalised (`106_model_logit.tex`), but the headline empirical estimates come from specifications whose identifying variation shrinks toward zero precisely as the fixed effects approach that within-neighbourhood ideal (LSOA fixed effects columns).
8. **Dry spills are motivationally central but analytically peripheral.** `102_appendix_dry_spills.tex` establishes measurement (replicating EA counts) and descriptives, and dry spills anchor two of the four research questions, but no price or welfare analysis of dry spills exists, and the model has no mechanism for them. The draft's own descriptive finding — that dry-spill locations correlate closely with wet-spill locations and "may be largely driven by capacity issues rather than discretionary releases" (`03_motivating_evidence.tex`) — sits awkwardly with the enforcement-focused framing of research question 2.
9. **Policy-reform designs are placeholders.** The EDM rollout and Thames Tideway Tunnel event studies (`04_causal_impacts.tex`) are red-text to-dos; the historical Victorian-instrument material is fully commented out.
10. **Headline magnitudes span an order of magnitude.** Across the identification-strategies comparison table (`105_identification_strategies_comparison.tex` / `tables/strategies_comparison.tex`), the implied one-standard-deviation effect on sale prices ranges from 0.0% (long differences) through −0.7% (preferred hedonic) to −3.9% (weakest-control hedonic), with the repeat-sales point at −1.8% only marginally significant. The paper does not yet have a single preferred causal estimate to feed the welfare calculation.
