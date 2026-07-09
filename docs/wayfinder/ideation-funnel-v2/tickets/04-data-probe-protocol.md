---
id: 04
title: "Define the light data-probe protocol"
type: grilling
status: closed
assignee: jacopo
resolved: 2026-07-09
blocked-by: [01]
---

## Question

The charter allows "very very light data tests, only when needed to verify an idea
or a feasibility score" — not systematic pilot regressions. Pin down with Jacopo
what that means operationally: the trigger test an evaluator must pass before a
probe is allowed (what counts as "needed"); what a probe may do (row counts,
coverage checks, a single cross-tab or first-stage F?) and what it may not
(anything resembling analysis); which datasets are touchable (presumably only what
`scripts/R/09_analysis/` loads, read-only, per run 1's corrected guardrail — the
delta memo from "What changed since the first run's context pack?" supplies the
current list); where probe code lives (session scratchpad only); a per-probe token
and wall-clock cap; and how probe evidence is recorded on the candidate file so
scores stay auditable.

The panel design ([Design the adversarial evaluation panel](03-adversarial-panel-design.md))
added a second probe consumer: the feasibility refuter (prosecutor stage) runs
read-only probes to attack candidates' data claims before the feasibility engineer
scores. The protocol must cover both consumers — the refuter's probes are expected
to be the heavier of the two.

Blocked by: [What changed since the first run's context pack?](01-context-delta-inventory.md).

## Resolution (2026-07-09)

Grilled with Jacopo; every recommended answer was accepted except the per-probe
caps, where he chose the looser tier.

1. **Who may probe.** Only the feasibility engineer and the feasibility refuter.
   Generators, the dedupe judge, the potential referees, and the originality
   searchers never touch data. (Rescue work at Checkpoint 2 does not probe
   either — the "those two only" rule was chosen over the rescue-pass variant.)

2. **Trigger — the decisive-claim test.** Before running anything, the prober
   must write down: (a) the specific factual claim about the data being checked
   (existence of a variable, coverage of a period, approximate sample size, join
   feasibility); (b) that the context pack does not already answer it; and
   (c) how the feasibility score or attack would change if the claim fails. All
   three or no probe.

3. **Scope — descriptive only.** Allowed: row counts, distinct-key counts, date
   and geography coverage, missingness rates, join overlap rates, and a single
   small cross-tab or one-variable summary. Forbidden: any regression (including
   a first-stage F), any plot, anything estimating a relationship between
   variables. A first-stage F was explicitly considered and rejected as a pilot
   regression by another name.

4. **Touchable data.** Anything under `data/processed/`, strictly read-only;
   `data/raw/` and everything outside the repo's data directory are off-limits.
   This deliberately widens run 1's loader-only guardrail: the `09_analysis`
   loader is dormant and narrower than the active analysis surface, and run-2
   ideas from the data-asset lens may hinge on dormant assets (LexisNexis
   outputs, the consent-discharges database), which should be feasibility-
   checkable rather than scored blind.

5. **Per-probe caps (looser tier, chosen over the recommended 5-minute one).**
   One probe = one question = one script. Up to 10 minutes wall-clock per
   execution; at most 3 executions, where retries fix script bugs and never ask
   a new question; roughly 40k tokens of agent effort per probe, stated as
   prompt guidance rather than harness-enforced. Sized so a chunked pass over
   the largest files (the ~600 MB site-day panel, the 250M-row general panel)
   can succeed.

6. **Per-candidate quotas.** Feasibility engineer: at most 1 probe per
   candidate. Refuter: at most 3 per candidate. Worst case 4 probes per
   candidate; with the loose per-probe caps, the quotas do the real
   gatekeeping.

7. **Code location.** Probe scripts are written and run only in the session
   scratchpad — nothing under the repo, nothing committed — but the exact script
   text and its raw output are preserved verbatim in the evidence record, so
   probes stay reproducible after the scratchpad is gone.

8. **Evidence recording.** Each probe appends one structured block to a "Probe
   evidence" section of the candidate file with fixed fields: who ran it (scorer
   or refuter); the pre-registered decisive claim (all three trigger elements,
   written before execution); the script; the raw output; a one-line verdict
   (claim confirmed / refuted / inconclusive); and the effect on the score or
   attack. Checkpoint reviewers read the verdict line and zoom the rest on
   demand.

Spec-level default, not a grilled decision: probes run in R via `Rscript` with
`arrow`, matching the repo's tooling.
