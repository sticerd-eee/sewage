---
id: 10
title: "Define the presentation format for Checkpoints 2 and 3"
type: grilling
status: closed
assignee: jacopoolivieri
resolved: 2026-07-09
blocked-by: []
---

## Question

What exactly is Jacopo shown at Checkpoint 2 (post-dedupe skim with kill rights
before the panel) and Checkpoint 3 (scored-ledger review with veto and rescue
rights), and how are his kills, vetoes, and rescues recorded so the next phase's
workflow run picks them up mechanically? The inputs are already fixed by earlier
decisions — Checkpoint 2 receives the curator's skim document over the
append-only ledger plus any cap-hit flag; Checkpoint 3 receives the scored
ledger with devil's-advocate briefs attached to near-miss kills and structured
probe-evidence blocks on the candidate files. To decide: the reading order and
document layout for each package; whether decisions are recorded inline in the
ledger, in a separate decision file, or conversationally with the orchestrating
session transcribing; and what the executing session does with an ambiguous or
partial response. Checkpoints 1 and 4 are already fixed (Checkpoint 1 by the
loop-mechanics decisions, Checkpoint 4 by the comparison-stage decisions).

## Resolution (2026-07-09)

Settled in a live grilling session with Jacopo; each decision below was put to him
individually and confirmed. All recommendations were accepted.

1. **Recording mechanism.** At both checkpoints Jacopo responds conversationally;
   the orchestrating session transcribes his decisions into a per-checkpoint
   decision file — one line per candidate id with the action taken and his stated
   reason — reads the file back to him for confirmation, and launches the next
   phase only after he confirms. The decision file is the single machine-readable
   input the next workflow run consumes. The ledger stays append-only and untouched.

2. **Default rule.** Silence means the mechanical default. At Checkpoint 2 an
   unmentioned canonical idea advances to the panel; at Checkpoint 3 an unmentioned
   candidate advances or dies exactly as the min-rule scored it. Jacopo speaks only
   to exceptions, and the decision file explicitly records "all others: default" so
   the next run has no ambiguity.

3. **Ambiguous and partial responses.** Ambiguity never enters the decision file:
   the session resolves it with a targeted follow-up question in the moment (for
   example, "that matches ideas 12, 17, and 23 — kill all three?") and writes only
   resolved decisions. If Jacopo stops mid-review, the session writes the confirmed
   decisions plus an explicit "checkpoint incomplete" marker, and the next workflow
   run refuses to start on an incomplete decision file until he resumes and the
   file is confirmed complete.

4. **Checkpoint 2 package.** The orchestrating session opens with a short briefing
   message — counts of raw ideas, canonical ideas, and absorbed duplicates, the
   cap-hit flag if generation was truncated, and any missed-duplicate or
   pre-existing-notes flags — so anything unusual is impossible to miss. The
   curator's skim document is then the single required read, in its existing order
   (shape-of-the-pool paragraph first, then canonical ideas grouped by lens). The
   full ledger and the individual candidate files are drill-down links only.

5. **Checkpoint 3 package.** The session opens with a briefing — how many
   candidates advanced, how many were killed, how many near-misses received
   devil's-advocate briefs, plus any cap triggers — then presents one review
   document ordered by decision type: first the advancing candidates (the veto
   review), sorted by median potential, each with its scores and a two-sentence
   recap; then the rescue queue, each near-miss with its scores, the killing
   review's core objection in one sentence, and its devil's-advocate brief inline;
   then the remaining kills as a compact table with scores and a one-line kill
   reason. Full reviews and probe evidence stay drill-down links on the candidate
   files.

6. **Checkpoint 3 author.** A dedicated checkpoint-3 curator agent, running at
   high thinking effort, writes the review document from the scored ledger, the
   panel reviews, and the briefs — mirroring the Checkpoint 2 curator decision and
   under the same hard constraints: full coverage (every candidate appears in its
   section), no re-scoring or second-guessing the panel, no recommendations about
   Jacopo's vetoes and rescues, flags rather than filters. Scores and outcomes are
   copied verbatim from the ledger.

7. **Rescue scope.** Jacopo may rescue any killed candidate, not just the briefed
   near-misses — the checkpoint exists precisely to override the mechanical rule.
   A rescued candidate that skipped the originality searcher (gated behind
   potential and feasibility) gets its searcher run before it joins the surviving
   slate, so everything downstream carries full three-criterion evidence. The
   decision file records the rescue and the next workflow run schedules the
   catch-up searcher automatically.
