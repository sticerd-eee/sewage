---
id: 01
title: "What changed since the first run's context pack?"
type: research
status: closed
assignee: claude
blocked-by: []
---

## Question

The first run's context pack was compiled around 2026-07-04, and the repo has moved
since (location-merge rebuild on the works crosswalk, repeat-transactions rebuild,
new news and daily spill-rainfall pipeline scripts). Inventory, without opening any
`docs/ideas/run1/` content: (a) the current state of the paper draft — sections, headline
estimates, what the model section now contains; (b) the analysis datasets the
current `scripts/R/09_analysis/` loader actually reads, their grain, coverage, and
key variables; (c) pipeline changes since 2026-07-04 that alter what an ideation
context pack must say. Output: a markdown delta memo (linked asset) that the run-2
context-pack agents' instructions will be written against, so the fresh project
brief is compiled from current facts rather than the first run's assumptions.

## Resolution (2026-07-09)

Resolved by three parallel research agents (paper draft, analysis data surface,
repo history since 2026-07-04), none of which opened `docs/ideas/run1/`. The full
inventory is the linked asset
[Context delta memo](../assets/01-context-delta-memo.md); the run-2 context-pack
agents' instructions should be written against that memo, not against the first
run's context pack.

The findings that correct assumptions this planning effort was carrying:

1. The repeat-transactions rebuild has NOT landed. It is a plan document on the
   unmerged branch `jo/repeat-transactions-rebuild`; on `main` the repeat IDs are
   still positional, the summary stage still exists, and the rental spill lookup
   is still at 5 km. A context pack must describe it as a pending proposal. This
   surfaced a new decision, ticketed as
   [09 — freeze main as-is or land the repeat-transactions rebuild first?](09-repeat-rebuild-timing.md).
2. The news and daily spill-rainfall scripts are not new code — only their
   documentation changed (execution order renumbered to 40 steps on 2026-07-07).
3. The location-merge rebuild IS merged (2026-07-06): matching now happens at
   works grain via the works register; five clean-break artifacts replaced the
   old outputs; `agg_spill_dry_qtr.parquet` and both exposure panels now carry
   the works-grain `site_id` plus new crosswalk-sourced EA columns.
4. The `scripts/R/09_analysis/` loader is dormant — no analysis script sources it
   and the orchestrator has it commented out; the active analyses read a wider
   set of parquet paths directly. The memo inventories both surfaces, which
   matters for the data-probe protocol's "touchable datasets" list.
5. The paper draft lives outside the repo on Dropbox/Overleaf
   (`/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in
   Our Waters`); recent effort is concentrated on the model section, whose
   compensating-variation welfare block exists but is commented out of the
   compiled PDF; a fuller alternative model draft (`106_model_logit.tex`) is not
   compiled at all.
