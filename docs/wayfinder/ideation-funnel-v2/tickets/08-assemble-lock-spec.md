---
id: 08
title: "Assemble and lock the run-2 spec"
type: task
status: closed
assignee: claude
resolved: 2026-07-09
blocked-by: [01, 02, 03, 04, 05, 06, 07, 09, 10]
---

## Question

Write the locked, self-contained execution spec as a plan document in
`docs/plans/` (dated 2026-07, "research ideation funnel v2"). It must let a fresh
session — with no context from this planning effort or the first run — execute
end-to-end: the quarantine runbook (move `docs/ideas/run1/` and the run-1 plan doc out
of the repo, restore at the comparison stage); phase-by-phase workflow structure
with agent counts, workflow-run seams (one run per phase, the panel split in two
at the originality gate) and pause-and-resume instructions in place of any
quality-cut fallback (including unattended auto-resume: on a usage limit the
orchestrating session schedules a wakeup for the window reset and relaunches the
phase, stopping overnight only at checkpoints), workflow script skeletons for each run, full prompt texts
for every agent role (including the framing-free data-asset inventory as a
separate context-pack artifact for the open lens), and the writing-style
requirement baked into generator and refiner prompts; the four checkpoints and
what Jacopo sees at each; guardrails (no implementation, repo writes only under
the fresh `docs/ideas/run1/`, read-only data, ledger discipline); the probe protocol;
the scoring and aggregation rules; the budget and effort table; and the
cross-run comparison stage. All-Fable with no per-agent model overrides. The map
is done when Jacopo signs this document off as launch-ready.

Blocked by: all design tickets (01–07), the repeat-rebuild timing decision (09),
and the Checkpoint 2/3 presentation format (10).

## Resolution (2026-07-09)

The locked execution spec is written at
`docs/plans/2026-07-09-001-research-ideation-funnel-v2.md`. It is self-contained
for a fresh session: quarantine runbook with a concrete holding path
(`/Users/jacopoolivieri/sewage-run1-quarantine`); the full output tree; all five
phases with the two-run panel split at the originality gate; the seven-lens /
fourteen-angle generation loop with dedupe, lens retirement, dry test, and the
five-round cap; the complete probe protocol; the min-rule, median-of-three
potential scoring, near-miss definition, brief cap, and rescue catch-up searcher;
all four checkpoint procedures with decision-file formats and the incomplete
marker; the budget envelope and the full thinking-effort table (all-Fable, no
model overrides); pause-and-resume with unattended auto-resume via scheduled
wakeup; six workflow script skeletons; and full prompt texts for all fifteen agent
roles, with the writing-style block inserted into every human-facing role and the
framing-free data-asset inventory as a separate Phase-0 artifact feeding only the
open lens's first angle.

Two spec-level defaults added while assembling, both flagged for Jacopo's
sign-off review rather than separately grilled:

1. The quarantine also moves `docs/wayfinder/ideation-funnel-v2/` (this planning
   trail) to the holding folder, mechanically enforcing the charter's independence
   requirement instead of relying on prompts alone; it returns in the same restore
   commit.
2. Refiners do not run probes. Run 1 allowed refinement-stage probes, but the
   probe-protocol decision ("only the feasibility engineer and the feasibility
   refuter, ever") postdates and supersedes it; the spec follows the stricter
   locked rule.

The map's destination now waits only on Jacopo's sign-off, ticketed as
[11 — sign off the run-2 spec as launch-ready](11-spec-signoff.md).
