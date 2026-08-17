---
id: 07
title: "Drift map: independent exposure aggregations across the repo"
type: research
status: closed
assignee: jacopo
blocked-by: []
---

## Question

Which scripts implement spill-exposure aggregation independently of
`prior_exposure_utils.R`, and where do their methodologies (windowing,
radius semantics, missingness handling, averaging denominators, sum
stability) diverge from the engine's? Which scripts merely consume published
outputs and carry no drift risk?

## Resolution (2026-08-17)

Resolved by a repo-wide search agent; full findings in
[assets/02-drift-map.md](../assets/02-drift-map.md). Headline: besides the
two engines this map already covers, four scripts independently re-implement
study-period summation over `agg_spill_yr.parquet` with inconsistent NA
conventions (`grid_long_difference_*`, `hedonic_continuous_full`,
`hedonic_bins_full`, plus `did_trends_full`'s NA-to-zero coercion), the
twelve-script `upstream_downstream_*` family re-reduces the engine's
site-grain output with `na.rm = TRUE` (inverting NA-poisoning), and
`repeat_sales.R` carries a fifth, quarterised windowing scheme. The panel
and grid builders and the `*_prior*` analysis families are pure consumers.
The findings feed the measurement-core boundaries (ticket 03), the
downstream boundary (ticket 05), and the variant model (ticket 08); the
analysis-layer fixes themselves stay out of scope on this map.
