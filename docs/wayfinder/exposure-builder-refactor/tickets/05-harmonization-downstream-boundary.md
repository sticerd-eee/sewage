---
id: 05
title: "Where does the plan's responsibility for downstream analyses end?"
type: grilling
status: open
assignee:
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
