---
id: 03
title: "Design the adversarial evaluation panel"
type: grilling
status: closed
assignee: claude
blocked-by: []
---

## Question

How does the deeper adversarial panel score and kill candidates? Run 1 used one
specialist per criterion (potential, feasibility, originality) with a min-rule:
advance requires at least 4 of 5 on all three, no appeal. To decide with Jacopo:
whether the three criteria stand or change; panel size per criterion (e.g. three
independent hostile referees voting on potential instead of one); the aggregation
rule (keep the min-rule, move to majority vote per criterion, or a ranked cut);
whether near-miss kills get a devil's-advocate rescue round and what bounds it;
whether an adversarial refutation pass attacks feasibility claims before scoring;
how originality checks scale to ~35–45 candidates (per-candidate searchers were
the biggest spend block in run 1 — keep, batch, or gate behind the other two
criteria); and whether scores stay comparative (batch evaluation) or go absolute.

## Resolution (2026-07-09)

Settled in a live grilling session with Jacopo; each point below was put to him
individually and confirmed.

1. **Criteria.** Potential, feasibility, and originality stand unchanged. Run 2's
   extra ambition goes into how deeply each criterion is judged, not into new
   criteria; constraint-fit is already enforced upstream by the lens design.

2. **Panel size.** Three independent hostile referees vote on potential; feasibility
   and originality keep one specialist each. Potential is the judgment-laden
   gatekeeper where independent votes buy real error-reduction; feasibility is
   probe-driven and originality is search-driven, so extra judges there would mostly
   duplicate work.

3. **Aggregation.** The candidate's potential score is the median of the three
   referees' 1–5 scores, which neutralizes a single harsh or generous outlier. The
   run-1 min-rule then stands: advance requires ≥4 on all three criteria. Both steps
   are computed deterministically by the workflow script — no agent judgment in the
   rule itself.

4. **Rescue round.** A near-miss is a candidate with exactly one criterion at 3 and
   the other two at ≥4. One devil's-advocate agent per near-miss writes a half-page
   best case attacking the killing review's reasoning. Briefs never change scores or
   advancement; they attach to the ledger so Jacopo's Checkpoint-2 rescues are
   informed rather than gut calls. Capped at 10 briefs — the worst near-misses by
   median potential are dropped first, and the cap trigger is logged.

5. **Feasibility refutation.** Feasibility becomes prosecutor-then-judge. A refuter,
   batched by dataset and limited to read-only probes, actively tries to break each
   candidate's data claims (does the variation exist, are the cells big enough, is
   the named dataset in hand at the right grain). The feasibility engineer then
   scores with the refutation memo as evidence, running its own checks only where
   the memo is silent.

6. **Originality gating.** Potential and feasibility run first; per-candidate novelty
   searchers at run-1 depth run only on candidates scoring ≥4 on both, plus
   near-misses so rescue briefs carry full three-criterion evidence. Expected result
   is roughly half the searchers rather than double, on the panel's biggest run-1
   spend block.

7. **Scoring mode.** Potential stays full-slate comparative: each of the three
   referees independently sees all ~35–45 one-page pitches and scores against the
   field, with the median absorbing scale drift between referees. Feasibility and
   originality stay evidence-based, effectively absolute, as in run 1.

Consequences for the rest of the map: the light data-probe protocol (ticket 04) now
has a second consumer — the feasibility refuter — alongside the feasibility engineer,
and the Checkpoint-2 package now includes the devil's-advocate briefs, noted in the
map's checkpoint-format fog entry.
