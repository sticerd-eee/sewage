---
id: 07
title: "Set token budget and thinking-effort allocation"
type: grilling
status: closed
assignee: jacopoolivieri
resolved: 2026-07-09
blocked-by: [03, 05]
---

## Question

With panel design and generation mechanics fixed, cost the run and allocate
effort. Decide with Jacopo: a rough per-phase agent count and token envelope
(generation rounds, dedupe, panel, refinement, comparison) and whether any phase
gets a hard budget guard in its workflow script; the thinking-effort table per
agent role (run 1 used xhigh for the hostile-referee gatekeeper, high for
generators and refiners, medium for context/feasibility/novelty, low for dedupe —
revisit under the new panel sizes); pacing against Claude Max 20x usage windows,
including which checkpoints double as natural pause points and the expectation
that each phase is a separate resumable Workflow run; and the fallback lever if
the budget binds mid-run (run 1's designated lever was gating novelty checks
behind the other two criteria).

Blocked by: [Design the adversarial evaluation panel](03-adversarial-panel-design.md)
and [Design the loop-until-dry generation mechanics](05-loop-until-dry-mechanics.md).

## Resolution (2026-07-09)

Settled in a live grilling session with Jacopo; each decision below was put to him
individually. One recommendation was overridden: the fallback if usage windows
bind is to pause and resume, not to cut quality.

1. **Budget posture.** No hard token guards in any workflow script. The structural
   caps already locked by earlier tickets carry the load: five generation rounds,
   ten devil's-advocate briefs, originality searchers gated behind the other two
   criteria, and the probe limits. Each phase logs its token spend so drift is
   visible at checkpoints. On Claude Max 20x the binding constraint is usage
   windows, not dollars, and a hard guard that kills a panel mid-scoring would
   cost more in resume friction than it saves.

2. **Envelope form.** The spec states each phase's expected and worst-case agent
   counts as the primary envelope, with one indicative token figure per phase
   (computed at roughly 60k tokens per agent) as context for reading the spend
   logs, not as a target. The derived counts: Phase 0 context pack, 3–4 agents
   (context pack, framing-free data-asset inventory, pre-existing-notes
   register); Phase 1 generation, 14 generators in round one (seven lenses, two
   angles each), realistically about 30–45 generator calls over the loop with
   lens retirement and at worst 70, plus up to five dedupe judges and one
   curator; Phase 2 panel, three potential referees, roughly 4–6 batched
   feasibility refuters, one feasibility engineer, roughly 15–25 gated
   originality searchers, and up to ten devil's advocates; Phase 3 refinement,
   10–15 refiners plus one synthesis agent; Phase 4 comparison, one read-only
   agent. Realistic run total is roughly 80–110 agents (about 5–7M tokens);
   the hard worst case is near 140 agents (about 9M tokens). Run 1 used about 40.

3. **Thinking-effort table.** All agents on Fable, effort as the only knob:
   generators high; the three potential referees xhigh; refiners and synthesis
   high; devil's advocates high (judgment-dense briefs, small block); the
   comparison agent high (its verdicts go straight to Checkpoint 4); the
   per-round dedupe judge medium, up from run 1's low, because it now applies
   the design/dataset/claim novelty rubric that drives lens retirement and loop
   termination; curator, feasibility refuter, feasibility engineer, originality
   searchers, and context-pack agents all medium. Only three roles moved from
   their run-1 levels (dedupe judge, devil's advocates, comparison agent), each
   because its judgment now gates something downstream.

4. **Pacing.** Each phase is its own resumable Workflow run. The panel phase
   additionally splits into two runs at the originality gate: run A scores
   potential and feasibility; run B launches the gated originality searchers and
   the devil's advocates. The gate is already a hard sequencing point in the
   panel design, so the seam costs nothing and puts a clean window boundary
   inside the most expensive phase. The four checkpoints remain the human pause
   points on top of these seams.

5. **Fallback if windows bind (override).** Pause instead of cut. No quality
   lever exists anywhere in the run: if a usage window binds mid-phase, the
   phase stops at its nearest seam and resumes in the next window via the
   workflow resume mechanism. Jacopo explicitly accepted the possible multi-day
   stall in exchange for zero quality loss; the spec must instruct the executing
   session never to improvise a cut. Run 1's designated lever (gating originality
   checks) is already baked into the panel design as the default and is therefore
   not available as an emergency lever.

6. **Unattended resume (amendment, same day).** The pause is automatic, not
   manual: on hitting a usage limit, the orchestrating session records the
   interrupted run's id, schedules its own wakeup for the window reset, and
   relaunches the phase with the workflow resume mechanism when the window
   reopens. It stops for the night only at checkpoints, which always wait for
   Jacopo. This lets the run continue overnight while he sleeps, bounded by the
   next human gate; it requires the orchestrating session to stay open on his
   machine, and the spec must state both the instruction and that requirement.
