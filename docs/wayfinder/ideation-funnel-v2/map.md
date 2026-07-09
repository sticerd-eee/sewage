---
label: wayfinder:map
title: "Ideation Funnel v2 — locked spec"
created: 2026-07-09
tickets: tickets/
---

# Ideation Funnel v2 — locked spec

## Destination

A locked, self-contained execution spec (plan document in `docs/plans/`) for a
second, more ambitious research-ideation funnel — one a fresh session can run
end-to-end with no context from this planning effort or from the first run. The
map is done when Jacopo signs the spec off as launch-ready.

## Notes

- Domain: research ideation for the sewage-spills paper (Balboni & Dhingra
  coauthors); targets are the two binding constraints — identification/empirics
  and the welfare model — plus one open lens.
- All agents run on Fable (claude-fable-5); no per-agent model overrides —
  thinking effort is the tuning knob.
- HITL tickets run through /grilling, one question at a time, recommended answer
  attached. Jacopo prefers checkpointed multi-agent runs.
- Standing writing requirement: complete plain sentences in anything Jacopo or
  coauthors read; no shorthand, no arrow chains.
- Tracker: local markdown. Tickets live in `tickets/NN-slug.md` with frontmatter
  (`status`, `assignee`, `blocked-by`). A ticket is claimed when `assignee` is
  set; the frontier is any open ticket with empty `assignee` whose `blocked-by`
  ids are all closed.
- Independence rule for every downstream session: nothing from the first run's
  `docs/ideas/` may leak into generation or evaluation design artifacts that the
  executing agents will read (the quarantine at run time enforces this for the
  run itself).

## Decisions so far

- [Charter grilling: destination and strategic frame](tickets/00-charter-grilling.md) —
  destination is a locked spec run separately; blind generation with an
  end-stage cross-run comparison; ambition goes to wider generation and deeper
  adversarial panels with only very light on-demand data probes; scope is the two
  binding constraints plus an open lens; pre-existing notes are flag-only at
  dedupe; run-1 files are quarantined outside the repo during the run and return
  as `docs/ideas/run1/`; four checkpoints; roughly twice run 1's scale with
  loop-until-dry generation; map tracked as local markdown in `docs/wayfinder/`.
- [What changed since the first run's context pack?](tickets/01-context-delta-inventory.md) —
  delta memo delivered as [assets/01-context-delta-memo.md](assets/01-context-delta-memo.md);
  the location-merge rebuild is merged (works-grain `site_id`, five clean-break
  artifacts, crosswalk EA columns in the quarterly aggregation), but the
  repeat-transactions rebuild is an unmerged plan, the news and spill-rainfall
  scripts changed in documentation only, the `09_analysis` loader is dormant
  (analyses read parquet paths directly), and the paper draft lives on
  Dropbox/Overleaf with the model section active and its welfare block commented
  out. Surfaced the new ticket "Freeze main as-is or land the repeat-transactions
  rebuild first?".
- [Design the lens set for generation](tickets/02-lens-set-design.md) — seven
  lenses: the run-1 identification, welfare, and data-asset lenses survive;
  literature import is reworked to import framings with an easier-here argument;
  hostile referee is demoted to a generator angle inside identification; new
  regulation-and-firm-behaviour and distributional-incidence lenses fill the
  supply-side and incidence gaps; the open lens escapes the constraints framing
  structurally (angle a sees only a framing-free data-asset inventory, angle b
  reframes the full brief), with a shared feasibility tether on both angles.
- [Design the adversarial evaluation panel](tickets/03-adversarial-panel-design.md) —
  the three run-1 criteria stand; three hostile referees vote on potential (median
  score) with single specialists elsewhere; the min-rule (≥4 on all three) survives;
  near-misses get capped devil's-advocate briefs that inform Checkpoint-2 rescues
  without changing scores; feasibility becomes prosecutor-then-judge with a read-only
  refutation pass; originality searchers are gated behind the other two criteria;
  potential scoring stays full-slate comparative.

- [Define the light data-probe protocol](tickets/04-data-probe-protocol.md) —
  probes are exclusive to the feasibility engineer (1 per candidate) and the
  feasibility refuter (3 per candidate), gated by a written decisive-claim test;
  descriptive operations only (no regressions, not even a first-stage F);
  anything under `data/processed/` read-only, deliberately wider than run 1's
  loader-only rule; scratchpad-only scripts with script and output preserved
  verbatim in a structured probe-evidence block on the candidate file; caps of
  10 minutes and 3 executions per probe at roughly 40k tokens.
- [Freeze main as-is or land the repeat-transactions rebuild first?](tickets/09-repeat-rebuild-timing.md) —
  freeze; the launch never waits on the rebuild, and the context pack says
  nothing about it — it plainly describes the repeat artifacts as they stand on
  main at quarantine-snapshot time (whichever state that is), and the probe
  protocol treats them as ordinary touchable datasets.
- [Design the loop-until-dry generation mechanics](tickets/05-loop-until-dry-mechanics.md) —
  rounds after the first see the pool as a gist-level exclusion list; one
  dedupe-judge per round with a novelty rubric (different design, dataset, or
  claim); a round is dry under two novel ideas, and the loop stops after two
  consecutive dry rounds; lenses retire individually after two rounds without a
  novel idea; hard cap of five rounds with a logged cap-hit naming still-productive
  lenses; append-only raw ledger as audit trail plus a curator-agent checkpoint
  skim (full coverage, no ranking, flags not filters, one shape-of-the-pool
  paragraph).
- [Design the cross-run comparison stage](tickets/06-cross-run-comparison-design.md) —
  idea-level matching across both full candidate pools with reporting restricted
  to pairs touching a surviving proposal; required structured verdicts (coverage
  gap vs. judgment call for run-1-only proposals, genuinely new vs. wider-net
  artifact for run-2-only ones); the run-2 ranking is annotate-only, with the
  agent's combined shortlist as a labelled memo section; end state is
  `docs/ideas/run1/` and `docs/ideas/run2/` as intact siblings with the memo at
  `docs/ideas/comparison-run1-run2.md` and all link fixes in one commit; a single
  read-only comparison agent writes the memo, which joins the Checkpoint-4
  package, and Jacopo's sign-off releases the restore commit.
- [Set token budget and thinking-effort allocation](tickets/07-budget-and-effort-allocation.md) —
  no hard token guards anywhere (the structural caps from earlier decisions carry
  the load, with per-phase spend logging); envelopes stated as agent counts with
  indicative token figures (realistically 80–110 agents and 5–7M tokens, worst
  case near 140 and 9M, versus run 1's ~40); effort table adopted with three
  upgrades from run 1 (dedupe judge to medium, devil's advocates and the
  comparison agent to high); one Workflow run per phase with the panel split in
  two at the originality gate; and — overriding the recommendation — no quality
  fallback if usage windows bind: pause at the nearest seam and resume in the
  next window, with the orchestrating session auto-resuming at window reset via
  a scheduled wakeup so the run continues unattended overnight, stopping only at
  checkpoints.
- [Define the presentation format for Checkpoints 2 and 3](tickets/10-checkpoint-2-3-format.md) —
  at both checkpoints Jacopo responds conversationally and the session transcribes
  into a per-checkpoint decision file confirmed by read-back, with silence meaning
  the mechanical default, ambiguity resolved by follow-up questions before anything
  is written, and a partial review blocking the next phase via an explicit
  incomplete marker; Checkpoint 2 is a flags-and-counts briefing plus the curator
  skim as the sole required read; Checkpoint 3 is a briefing plus one
  outcome-ordered review document (advancing slate, rescue queue with
  devil's-advocate briefs inline, compact kill table) written by a dedicated
  high-effort curator agent under full-coverage, no-recommendation constraints;
  rescue rights cover any kill, with a catch-up originality searcher backfilled
  automatically for rescues that skipped the gate.
- [Assemble and lock the run-2 spec](tickets/08-assemble-lock-spec.md) — the locked,
  self-contained execution spec is written at
  `docs/plans/2026-07-09-001-research-ideation-funnel-v2.md`, folding in every
  decision above plus full prompt texts for all fifteen agent roles, six workflow
  script skeletons, the quarantine runbook, checkpoint decision-file formats, and
  the unattended pause-and-resume procedure; two spec-level defaults (quarantining
  this planning trail too, and no refiner probes) are flagged in the resolution for
  the sign-off review.
- [Sign off the run-2 spec as launch-ready](tickets/11-spec-signoff.md) — Jacopo
  signed the spec off as launch-ready with no changes, confirming all four review
  points (planning-trail quarantine, no refiner probes, the holding path
  `/Users/jacopoolivieri/sewage-run1-quarantine`, and the run branch
  `jo/ideation-run2`). **The map is complete**: the destination — a locked,
  self-contained execution spec at
  `docs/plans/2026-07-09-001-research-ideation-funnel-v2.md` — is reached, and
  the run launches from it in a fresh session.

## Not yet specified

- Nothing. The map is complete; no tickets remain open.

## Out of scope

- Executing the funnel itself — the destination is the spec; the run happens in
  a fresh session launched from it.
- Pilot or dry-run validation of the spec before the full run (declined at
  destination-setting in the charter).
- Any richer final deliverable than 1–2 page mini-proposals plus a ranked index
  (declined in the charter).
- Editing run-1 outputs, beyond the temporary quarantine and the final
  reorganization into `docs/ideas/run1/`.
