---
id: 05
title: "Design the loop-until-dry generation mechanics"
type: grilling
status: closed
assignee: jacopoolivieri
blocked-by: [02]
---

## Question

How do the generation rounds actually loop? The charter fixes the shape (two
generators per lens, extra rounds until two consecutive rounds add nothing new,
expecting 60–80 raw ideas). To decide with Jacopo: what "nothing new" means
mechanically — who judges that a round's ideas are all duplicates of the pool, and
by what key (title similarity is too weak, full semantic dedupe per round costs an
agent each time); whether later rounds see the accumulated idea pool (cheap
dedupe, weaker independence between rounds) or stay blind with dedupe deferred to
the barrier (stronger independence, more waste); round size after the first
(all lenses again, or only lenses that were still productive); a hard cap on
rounds so the loop cannot run away; and how the ledger absorbs 60–80 raw entries
without becoming unreadable at the post-dedupe skim checkpoint.

Blocked by: [Design the lens set for generation](02-lens-set-design.md).

## Resolution (2026-07-09)

Settled in a live grilling session with Jacopo; each decision below was put to him
individually and confirmed. One recommendation was overridden: the checkpoint skim
is written by a curator agent rather than assembled mechanically.

1. **Pool visibility.** Generation rounds after the first are not blind. Each later
   round receives the accumulated pool as a gist-level exclusion list — titles plus
   one-line gists — framed as "do not resubmit these; find what they miss".
   Within-lens angle independence and the run-1 quarantine are unaffected; only
   independence from the run's own earlier rounds is given up, deliberately.

2. **Dry test.** One dedupe-judge agent runs per round (not per idea). It receives
   the pool gist list and the round's new ideas in full, and marks each new idea
   either as a duplicate or minor variant of a named pooled entry, or as genuinely
   novel. Novelty means a different design, dataset, or claim — not new wording —
   and that rubric is written into the judge's prompt. A round is dry when it adds
   fewer than two genuinely novel ideas across all lenses; the loop stops after two
   consecutive dry rounds. The two-idea floor (rather than strict zero) tolerates
   judge noise so the loop can actually terminate.

3. **Round size after round one.** Lenses retire individually: a lens that
   contributes no genuinely novel idea for two consecutive rounds sits out the rest
   of the loop. Surviving lenses re-run with both of their angles, preserving the
   paired-angle design from the lens-set ticket. The global stop rule applies on
   top of retirement.

4. **Hard cap.** Five rounds total — round one plus at most four extra. Worst case
   is roughly seventy generator calls plus five judge calls; lens retirement keeps
   the realistic cost well below that. If the cap fires while lenses are still
   productive, the executing session writes a cap-hit line into the ledger naming
   the still-productive lenses, so Jacopo sees at the checkpoint that generation
   was truncated rather than exhausted.

5. **Ledger and checkpoint skim.** The record and the view are split. The ledger
   proper is an append-only table, one row per raw idea — id, lens, angle, round,
   title, one-line gist, and the judge's verdict (novel, or duplicate-of with the
   canonical id) — kept unpruned as the audit trail. A curator agent then writes
   the checkpoint skim document on top of it: canonical ideas grouped by lens, each
   with title, a two-to-three-sentence plain-language gist written by the curator,
   a link to the full candidate file, and the absorbed-duplicate count. The
   curator's hard constraints: full coverage (it may not drop any canonical idea);
   no ranking, scoring, or recommending — evaluation belongs to the adversarial
   panel later; flags rather than filters (suspected missed duplicates become flag
   lines, both ideas stay in). It may open with one short shape-of-the-pool
   paragraph describing which lenses ran deep and where ideas clustered.
   Pre-existing-notes overlap flags from the dedupe stage also surface in the skim
   as flag lines, per the charter.

Standing writing requirement reaffirmed by Jacopo during this session, applying to
every artifact the funnel puts in front of him: complete sentences, terms spelled
out, no arrow chains or invented shorthand labels, identifiers introduced with
their own plain-language clause, and summaries that open with the outcome.
