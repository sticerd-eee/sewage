---
id: 00
title: "Charter grilling: destination and strategic frame"
type: grilling
status: closed
assignee: jacopo
blocked-by: []
---

## Question

What is this effort finding its way to, and what are the strategic decisions that
frame every downstream design ticket for the second, more ambitious research
ideation funnel?

## Resolution (2026-07-09)

Settled in a live grilling session with Jacopo. Every decision below was put to him
explicitly and confirmed.

1. **Destination.** The map ends with a locked, self-contained execution spec — a
   plan document a fresh session can run without any context from this session or
   the first run. The run itself happens separately, which also guarantees the
   independence requirement: the executing session never sees this planning context.
   Pilot or dry-run validation of the spec was explicitly declined.

2. **Independence.** Generation and evaluation are fully blind to the first run's
   outputs (the 18 candidates and 12 proposals in `docs/ideas/run1/`). A final synthesis
   stage compares the new ranked list against the old one, treating overlap as
   corroboration signal rather than duplication to suppress.

3. **Where the ambition goes.** Extra tokens buy (a) wider generation and
   (b) deeper adversarial evaluation. Data work is limited to very light probes,
   run only when a specific idea's verification or feasibility score needs them —
   not systematic pilot regressions. A richer final deliverable format was declined:
   survivors still get 1–2 page mini-proposals plus a ranked index.

4. **Scope.** The two binding constraints (identification/empirics and the welfare
   model) remain the primary targets, plus one explicitly unconstrained "open" lens
   that may propose anything raising top-5 publication odds.

5. **Jacopo's pre-existing notes** (Obsidian sewage notes, `todos/`, old plans) are
   used flag-only: generators never see them; a freshly compiled register is used
   by the dedupe stage to annotate candidates with "matches existing note X".

6. **Quarantine for blindness.** At run start the orchestrating session moves
   `docs/ideas/run1/` and `docs/plans/2026-07-04-001-research-ideation-funnel.md` to a
   holding folder outside the repo. Run 2 writes to a fresh `docs/ideas/run1/` tree. At
   the final comparison stage the run-1 files return and the two runs are
   reorganized as siblings (`docs/ideas/run1/`, `docs/ideas/run2/`) in one commit.
   Known residual: run-1 files remain in git history; accepted, since blind agents
   have no prompt-side reason to dig there.

7. **Checkpoints.** Four human gates: (1) context-pack sign-off, (2) a new light
   post-dedupe skim where Jacopo reads candidate one-liners and can kill obvious
   junk before the expensive panel runs, (3) scored-ledger review with veto/rescue
   rights, (4) final read of proposals and ranked index.

8. **Scale.** Roughly twice run 1: six to seven lenses, two independent generators
   per lens with different angles, then loop-until-dry rounds (stop after two
   consecutive rounds add nothing new). Expect 60–80 raw ideas, roughly 35–45
   distinct candidates after dedupe, and likely 10–15 panel survivors reaching the
   proposal stage.

9. **Model constraint carried over from run 1.** All agents run on Fable
   (claude-fable-5); workflow scripts must not set per-agent model overrides.
   Thinking effort is tuned per agent role instead.

10. **Tracker.** This wayfinder map lives as local markdown in `docs/wayfinder/`,
    not GitHub issues, keeping planning noise off the shared coauthor repo.
