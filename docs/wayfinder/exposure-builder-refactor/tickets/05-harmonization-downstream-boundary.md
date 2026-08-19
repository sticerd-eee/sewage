---
id: 05
title: "Where does the plan's responsibility for downstream analyses end?"
type: grilling
status: closed
assignee: claude
blocked-by: [01]
---

## Question

The harmonized rule adds NAs to the radius datasets that feed the main
hedonic cross-sections, and the site-grain datasets that feed the
upstream/downstream family. Using the consumer inventory from ticket 01,
decide the plan's deliverable boundary:

1. Which analysis scripts need a code change (for example, the hedonic
   scripts' manual `annual_returns_na_then_absent` exclusion becomes
   redundant — is removing it in scope?), versus a plain re-run.
2. Whether regenerating downstream outputs (figures, tables, deck assets)
   is inside the plan's tail, or explicitly deferred with a named follow-up.
3. How the Stage-2 sample-impact memo in `docs/reports/` relates to the
   paper's data section — what it must quantify so the sample change is
   citable.
4. The drift map ([assets/02-drift-map.md](../assets/02-drift-map.md),
   items 3 and 5) shows consumers that do not merely propagate the
   engine's NAs but actively override them: the twelve
   `upstream_downstream_*` scripts re-reduce site-grain output with
   `na.rm = TRUE`, and `did_trends_full.R` coerces missing evidence to
   zero exposure. The harmonized rule's extra NAs will be silently
   swallowed by these paths. Decide whether the plan's boundary treats
   them as scripts needing a code change, or only states the canonical
   convention (NA-poisoning propagates; zero-coercion is forbidden) that a
   follow-up effort applies.

## Resolution (2026-08-17, grilled with Jacopo)

The plan's deliverable boundary is narrow: the plan ends when the six
datasets are rebuilt under the harmonized rule and the contract and
verification suite passes. No analysis outputs regenerate inside the plan.

1. **Code changes versus re-runs.** The only in-scope analysis code change
   is removing the manual `!annual_returns_na_then_absent` exclusion from
   `hedonic_continuous_prior.R` (currently lines 160 and 203) in Stage 2,
   because the harmonized rule makes those rows NA exposure and the
   existing NA filters already drop them. The removal is backed by a
   contract assertion that the flag implies NA exposure, so the redundancy
   is proven rather than assumed. Every other live consumer is a plain
   re-run, deferred to the follow-ups below.

2. **Regeneration boundary.** Narrow, per Jacopo: re-running the analysis
   battery and refreshing the paper and deck outputs is his own follow-up
   after the plan executes. The single carve-out is the Stage-2 memo (see
   point 3), which estimates its before-and-after comparison inside its
   own `.qmd` — it does not modify or re-run any analysis script and does
   not regenerate any published table or figure.

3. **The Stage-2 sample-impact memo.** A `.qmd` in `docs/reports/`
   (following the lagged-attention report precedent) that quantifies, per
   dataset and radius: rows before and after, which rows change NA status,
   and the exposure distribution of the removed rows — plus
   before-and-after coefficients and standard errors for the preferred
   250-metre continuous hedonic in both markets, estimated within the
   memo itself. It gives the paper's data section every number it needs
   (sample counts, exclusion shares, the rule stated in one sentence) but
   drafts no paper prose. This memo is the substance of the Stage-2
   sign-off checkpoint.

4. **The NA-overriding consumers.** The plan states the canonical
   convention and names the offenders, but fixes nothing (fixes are
   already out of scope on the map). The convention — missing evidence
   stays NA all the way into the regression sample; `na.rm = TRUE`
   re-reduction and zero-coercion of missing evidence are nonconforming —
   is added to `CONCEPTS.md`, the durable vocabulary. The offender list —
   the twelve `upstream_downstream_*` scripts, `did_trends_full.R`, and
   the four `agg_spill_yr` re-aggregations — goes in a named section of
   the plan document with an explicit warning that their outputs must not
   be refreshed after Stage 2 until they are fixed, and is mirrored into a
   follow-up file in `todos/`. No warning comments are added to the
   offending scripts themselves.

5. **Two named follow-ups, not one.** First, the post-Stage-2 standard
   analysis battery refresh (Jacopo's, runnable the day Stage 2 lands).
   Second, the analysis-layer NA-convention cleanup, explicitly ordered
   fix-first-then-refresh, and partly blocked on the colleague-owned
   signed-pair CSVs. Keeping them separate prevents the upstream-downstream
   tables from being refreshed in the same sweep as the clean outputs.
