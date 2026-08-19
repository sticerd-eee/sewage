---
id: 01
title: "Consumer and contract inventory for the six exposure datasets"
type: research
status: closed
assignee: research-agent
blocked-by: []
---

## Question

What is the complete fact base the plan's compatibility requirements must
cite? Specifically, for each of the six datasets (`prior_to_sale`,
`prior_to_rental`, `prior_to_sale_house_site`, `prior_to_rental_rental_site`,
`study_period`, `study_period_ea`):

1. Every consumer in the repo — analysis scripts, testing scripts, figure
   scripts, book/documentation builds — with the columns each one actually
   reads and any masking or exclusion logic it applies itself (for example
   the hedonic scripts' manual `annual_returns_na_then_absent` exclusion).
2. The exact published schema and key grain, and which contract or
   verification scripts pin them (`test_prior_exposure_contracts.R`,
   `test_cross_section_study_period_contracts.R`,
   `verify_id_artifact_match_rates.R`, and anything else).
3. What branch `jo/cross-section-individual-edm` changes relative to main in
   any of the above — builders, utilities, tests, and analysis scripts.
4. Which analysis outputs (figures, tables, reports) are downstream of these
   datasets and would need regeneration after a semantic change.

Findings go to `assets/01-consumer-inventory.md` as the asset this ticket
links; the answer comment summarizes the headline facts.

## Answer

Answered 2026-08-17. The full inventory is in
[assets/01-consumer-inventory.md](../assets/01-consumer-inventory.md).

The radius-grain prior datasets have seventeen consumers for `prior_to_sale`
and sixteen for `prior_to_rental` (three hedonic scripts, seven news scripts,
one book chapter, three exploratory testing scripts of which one is
sales-only, and three contract or verification scripts); each site-grain
dataset has nineteen live consumers plus nine archived upstream-downstream
scripts; `study_period` has one analysis consumer on main
(`cross_sectional_plots.R`) plus three contract or verification scripts, and
`study_period_ea` exists only on branch `jo/cross-section-individual-edm`,
where its sole reader is the new comparison verifier. Live consumers never
read `has_missing_site` or `site_missing`; they inherit the NA policy by
dropping rows with NA exposure averages after filtering to one radius and, at
the radius grain, `n_spill_sites > 0`. The only manual
`annual_returns_na_then_absent` exclusion is the branch version of
`hedonic_continuous_prior.R`. Schemas and grains are pinned literally by
`prior_exposure_utils.R` and `cross_section_study_period_utils.R` and enforced
by `test_prior_exposure_contracts.R`,
`test_cross_section_study_period_contracts.R`, and
`verify_id_artifact_match_rates.R`, whose fourteen-artifact inventory covers
five of the six datasets but not `study_period_ea`. The branch changes 17
files (+1,607/−141): event-based study-period builders with a 2021–2023
rental window, new `_ea` builders, an `exposure_source` dispatch in the
shared utility, roughly 480 new contract lines, the new
`verify_study_period_exposure_sources.R`, and the migration of
`hedonic_bins_full.R` onto `study_period`. Downstream of a semantic NA change
sit the full hedonic, news, upstream-downstream, and pair-count table
families, the cross-section binscatter figures, the `scaled_effects.qmd` book
chapter, and the deck and Overleaf copies routed through
`convert_paper_tables_to_beamer.py`, as itemized in the asset's section 5.
