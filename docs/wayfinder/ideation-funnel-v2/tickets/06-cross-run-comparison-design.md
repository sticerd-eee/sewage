---
id: 06
title: "Design the cross-run comparison stage"
type: grilling
status: closed
assignee: jacopoolivieri
blocked-by: []
---

## Question

The last stage of the run restores the quarantined run-1 files and compares the
two ranked lists, treating overlap as corroboration. Decide with Jacopo what that
comparison produces: matching granularity (idea-level pairing between the two
candidate pools, or only proposal-level); what the comparison memo says about
ideas that appear in both runs (corroborated), only in run 1 (did run 2's blindness
miss them, or did the panel rightly kill them?), and only in run 2 (genuinely new,
or artifacts of the wider net); whether the comparison agent may adjust the run-2
ranking or only annotate it; and the exact restore-and-reorganize procedure that
ends with `docs/ideas/run1/` and `docs/ideas/run2/` as siblings in one commit,
including what happens to inbound links pointing at the old run-1 paths.

## Resolution (2026-07-09, grilling with Jacopo)

1. **Matching granularity: asymmetric.** Matching runs at idea level across
   both full candidate pools (kills included on both sides), but the memo only
   reports pairs and orphans where at least one side survived to the proposal
   stage. This catches every case where the runs disagree about something one
   of them believed in, while keeping mutual kills out of the memo.

2. **Memo content: structured verdicts, no rerun trigger.** For every
   run-1-only proposal the comparison agent must classify it as a *coverage
   gap* (run 2 never generated the idea) or a *judgment call* (run 2 generated
   it and its panel killed it, with the kill reasoning quoted for audit). For
   every run-2-only proposal it must classify it as *genuinely new* or a
   *wider-net artifact*. Corroborated pairs get a rank-agreement note. Each
   verdict carries a one-paragraph argument. No pre-committed threshold for
   recommending supplementary generation; the memo may recommend it in prose.

3. **Ranking: annotate only, plus a labelled combined shortlist.** The run-2
   ranked index is never edited — it remains the provably uncontaminated blind
   artifact. The memo ends with the comparison agent's own cross-run shortlist
   as a clearly labelled final section, understood as a run-1-contaminated
   opinion, not a run output. The shortlist is a section of the memo, not a
   separate file.

4. **End state: flat siblings, memo at top level, one commit.** The
   quarantined run-1 tree returns intact (preserving `candidates/`,
   `proposals/`, `context/`, `ledger.md` and their relative links) as
   `docs/ideas/run1/`; run-2 outputs land as `docs/ideas/run2/` with the same
   internal structure; the memo lives at `docs/ideas/comparison-run1-run2.md`.
   The only inbound links to old `docs/ideas/run1/` paths are in the wayfinder
   map/tickets and `docs/plans/2026-07-04-001`; they are rewritten to `run1/`
   paths in the same single commit as the restore, run-2 placement, and memo.

5. **Sequencing: memo joins the Checkpoint-4 package; single agent.** After
   the panel and proposal writing finish, one comparison agent — read-only on
   the finalized run-2 artifacts, reading run 1 from its quarantine location —
   writes the memo. Checkpoint 4 becomes one sitting: Jacopo reads the
   proposals, the ranked index, and the comparison memo together, and his
   sign-off releases the single restore-and-reorganize commit. The charter's
   four checkpoints stand; run 1 returns to the repo at the same moment the
   effort concludes. Contamination is prevented by construction: the only
   run-1-aware agent cannot write run-2 files.

The comparison agent's full prompt text folds into spec assembly
([08](08-assemble-lock-spec.md)) with the other role prompts.
