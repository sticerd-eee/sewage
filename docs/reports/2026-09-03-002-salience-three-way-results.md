# Coastal / bathing / inland: three-way results

Date: 3 September 2026. Requested follow-up to the four-way salience review.

Historical snapshot: this memo uses the **exclusive** three-way definition.
The paper now uses overlapping All Bathing, All Coastal and All Inland groups,
reproduced in the [executable salience report](2026-09-03-003-heterogeneity-by-salience-report.qmd).
The exclusive three-way section was later removed from that report; this memo
is the retained record of the three-way results.

Bathing takes precedence: all ever-designated bathing locations are pooled; coastal and inland contain non-bathing locations only. The coast threshold is 2 km, Greater London is excluded, and unresolved bathing evidence counts as non-bathing. Missing coast evidence remains excluded. Sales cover 2021–2024 and rentals 2021–2023.

Coefficients and 95% confidence intervals below are in original log units. Extensive attention reports Near × Attention, intensive attention reports average weekly spill count × Attention, and hedonic reports average weekly spill count. Post begins in August 2022; Articles is log cumulative UK coverage.

Attention models retain property controls, LSOA and month fixed effects, and LSOA-clustered inference. Hedonics retain property controls and LSOA fixed effects, with no time fixed effects and heteroskedasticity-robust inference. All intervals use the fitted model’s t degrees of freedom and are pointwise, without multiplicity adjustment.

The extensive and hedonic classifications use the nearest Site Group; intensive uses any bathing Site Group and minimum coast distance within 250 m. Extensive compares 0–500 m with >1,000–2,000 m; intensive and hedonic use properties within 250 m.

## Extensive-margin Public Attention (primary)

| Market | Stratum | N | Post coefficient | Post 95% CI | Articles coefficient | Articles 95% CI |
|---|---|---:|---:|---|---:|---|
| Sales | Coastal non-bathing | 241,780 | -0.01245 | [-0.02287, -0.00202] | -0.00265 | [-0.00590, 0.00060] |
| Sales | Bathing (coastal + inland) | 193,843 | -0.00938 | [-0.02645, 0.00769] | -0.00176 | [-0.00570, 0.00217] |
| Sales | Inland non-bathing | 1,320,899 | 0.00203 | [-0.00284, 0.00689] | 0.00029 | [-0.00119, 0.00178] |
| Rentals | Coastal non-bathing | 58,912 | -0.00829 | [-0.01729, 0.00071] | -0.00250 | [-0.00579, 0.00078] |
| Rentals | Bathing (coastal + inland) | 41,966 | -0.00096 | [-0.01091, 0.00900] | 0.00276 | [-0.00078, 0.00629] |
| Rentals | Inland non-bathing | 299,211 | 0.00231 | [-0.00180, 0.00642] | 0.00184 | [0.00026, 0.00341] |

## Baseline hedonic

| Market | Stratum | N | Coefficient | 95% CI |
|---|---|---:|---:|---|
| Sales | Coastal non-bathing | 21,294 | 0.00248 | [-0.01465, 0.01961] |
| Sales | Bathing (coastal + inland) | 15,384 | -0.02376 | [-0.04812, 0.00061] |
| Sales | Inland non-bathing | 122,960 | -0.00725 | [-0.01395, -0.00055] |
| Rentals | Coastal non-bathing | 6,191 | -0.01540 | [-0.02792, -0.00289] |
| Rentals | Bathing (coastal + inland) | 2,674 | -0.02138 | [-0.04946, 0.00669] |
| Rentals | Inland non-bathing | 30,695 | -0.02472 | [-0.02919, -0.02024] |

## Intensive-margin Public Attention (secondary)

| Market | Stratum | N | Post coefficient | Post 95% CI | Articles coefficient | Articles 95% CI |
|---|---|---:|---:|---|---:|---|
| Sales | Coastal non-bathing | 21,160 | 0.01251 | [-0.01228, 0.03730] | 0.00039 | [-0.00673, 0.00750] |
| Sales | Bathing (coastal + inland) | 15,630 | -0.00098 | [-0.03014, 0.02818] | -0.00150 | [-0.00807, 0.00507] |
| Sales | Inland non-bathing | 122,846 | 0.00267 | [-0.00839, 0.01373] | -0.00114 | [-0.00420, 0.00191] |
| Rentals | Coastal non-bathing | 6,164 | -0.00026 | [-0.01772, 0.01720] | -0.00210 | [-0.00641, 0.00220] |
| Rentals | Bathing (coastal + inland) | 2,715 | 0.00610 | [-0.02326, 0.03545] | 0.00158 | [-0.00756, 0.01071] |
| Rentals | Inland non-bathing | 30,678 | 0.00780 | [0.00123, 0.01437] | 0.00299 | [0.00095, 0.00504] |

## Interpretation and verification

Pooling bathing locations does not reveal a clear attention response: every bathing attention interval spans zero. Pooled bathing hedonic estimates are negative in both markets, but both intervals include zero. Coastal non-bathing sales retain the negative extensive Post estimate; intensive inland non-bathing rentals retain positive interactions. The results do not establish a general salience gradient, and between-stratum differences have not been tested.

Pooling changes estimation weights and allows shared controls/time effects across the two bathing categories. The pooled group is dominated by coastal bathing observations; it can obscure distinct freshwater relationships. Sample sizes need not equal sums of the previous regression Ns because fixed-effect singleton removals can change.

All 20 coastal/inland non-bathing models reproduce the prior coefficients, standard errors and Ns at numerical tolerance 1e-10. Before/after London counts match the corresponding old cells; all extensive near/far × pre/post support cells are nonempty. All 30 reported Ns and 54 coefficient/CI entries in the five LaTeX tables were checked against saved models. Classification and small-negative formatting regression checks passed.

The three-way estimation code was removed from the executable salience report
when this comparison was superseded; it remains available in the repository
history of that report. The saved artifacts it produced are:

- [Saved models](../../output/regs/salience_three_way.rds)
- [Results including N, coefficient and 95% CI](../../output/logs/salience_three_way_results.csv)
- [Sample audit](../../output/logs/salience_three_way_cell_counts.csv)
- [Reproduction audit](../../output/logs/salience_three_way_reproduction.csv)
