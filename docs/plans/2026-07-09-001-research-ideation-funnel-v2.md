# Research Ideation Funnel v2 — Locked Execution Spec

**Date:** 2026-07-09
**Status:** Locked — signed off as launch-ready by Jacopo on 2026-07-09
**Owner:** Jacopo
**Predecessor:** `docs/plans/2026-07-04-001-research-ideation-funnel.md` (run 1 — quarantined during this run; see Section 4)

---

## 1. How to use this document

This specification is self-contained. It is written for a fresh Claude Code session
that has **no context** from the planning effort that produced it and **no knowledge
of the first ideation run's outputs**. Everything the executing session needs — the
quarantine procedure, the phase structure, the workflow script skeletons, the full
prompt text for every agent role, the checkpoint procedures, the guardrails, and the
budget — is in this document. Do not go looking for additional planning context; by
design there is none available to you (the first run's outputs are quarantined at
Step Q2, and the planning trail is quarantined with them).

Three standing rules for the executing session:

1. **Nothing in this spec is optional and there is no quality lever.** If resources
   bind (usage windows), you pause and resume (Section 14). You never trim a panel,
   skip a searcher, shrink a round, or improvise any other cut. This is an explicit
   instruction from Jacopo, who accepted a possible multi-day stall in exchange for
   zero quality loss.
2. **All agents run on Fable (`claude-fable-5`).** Workflow scripts must not set
   per-agent model overrides; agents inherit the session model. Thinking effort is
   the only per-role tuning knob, and the effort for every role is fixed in
   Section 13.
3. **Every artifact a human will read is written in complete plain sentences.**
   No shorthand, no arrow chains (`A → B`), no invented abbreviations. Identifiers
   are introduced with their own plain-language clause. Summaries open with the
   outcome. This requirement is baked into the generator, refiner, curator, and
   comparison prompts in Appendix A, and it applies equally to anything the
   orchestrating session itself writes for Jacopo (briefings, decision files,
   read-backs).

---

## 2. Objective, scope, and deliverables

Generate, adversarially evaluate, and refine research ideas that raise the odds of
the sewage-spills paper (coauthors Clare Balboni and Swati Dhingra) publishing in a
top-5 economics journal. Effort concentrates on the two agreed binding constraints:

1. **Identification credibility and empirics** — the causal designs (event studies,
   the hydraulics/rainfall instrument) and what more the data can support.
2. **The welfare model** — the step from hedonic price effects to welfare statements
   is the thinnest part of the paper.

In addition, one explicitly **open lens** may propose anything that raises top-5
publication odds, escaping the binding-constraints framing entirely (Section 8.2,
lens 7).

Ideas requiring new data acquisition are admissible if the acquisition path is spelled
out. There are no hard vetoes: effort and delay are captured in the feasibility score,
not by exclusion.

**This run is blind to the first run.** A first ideation funnel ran in early July 2026
and produced candidates and proposals under `docs/ideas/`. Run 2's generation and
evaluation must not see any of it — that is what the quarantine (Section 4) enforces.
Overlap between the runs is treated at the end as corroboration signal, via a
dedicated comparison stage (Section 12), never as duplication to suppress during the
run.

**End deliverables:**

- One 1–2 page mini-proposal per surviving idea, plus a ranked index, in
  `docs/ideas/proposals/` (expected 10–15 survivors).
- A cross-run comparison memo, `docs/ideas/comparison-run1-run2.md`, written after
  the run-2 artifacts are final.
- The final repository state: `docs/ideas/run1/` and `docs/ideas/run2/` as siblings,
  with the comparison memo beside them, landed in a single commit released by
  Jacopo's Checkpoint-4 sign-off.

**Expected scale** (twice run 1): 60–80 raw ideas, roughly 35–45 distinct candidates
after dedupe, 10–15 panel survivors reaching the proposal stage.

---

## 3. Guardrails (apply to every agent in every phase)

- **No implementation.** Ideas are refined, not built. No agent writes analysis code
  into the repository, edits pipeline scripts, or modifies the paper draft.
- **Repo writes only under the fresh `docs/ideas/` tree.** After the quarantine step
  empties it, run 2 owns `docs/ideas/` and writes nowhere else in the repository
  (the single restore commit at the very end is the one exception, and it is executed
  by the orchestrating session, not by a workflow agent).
- **Data is strictly read-only, and only under `data/processed/`.** `data/raw/` and
  everything outside the repository's data directory are off-limits. This deliberately
  widens run 1's loader-only rule: the `scripts/R/09_analysis/` loader is dormant and
  narrower than the active analysis surface, and ideas from the data-asset lens may
  hinge on dormant assets (LexisNexis outputs, the consent-discharges database) that
  should be feasibility-checkable rather than scored blind. Which agents may touch
  data at all, and how, is fixed by the probe protocol (Section 10) — most agents
  never touch data.
- **Ledger discipline.** Every raw idea gets a row in `docs/ideas/ledger.md` the
  moment it is proposed (format in Appendix C.1). The ledger is append-only: rows are
  never edited or deleted, and corrections are new rows or flag lines.
- **All-Fable.** No per-agent model overrides anywhere (Section 1, rule 2).
- **Quarantine integrity.** No agent may read the quarantine holding folder except
  the single comparison agent in Phase 4, and that agent may not write run-2 files.

---

## 4. Quarantine runbook

Performed by the orchestrating session **before anything else runs**, so that no
context-pack agent can see run-1 material. The holding folder lives outside the
repository:

```
QUARANTINE=/Users/jacopoolivieri/sewage-run1-quarantine
```

- **Q1.** Create the holding folder: `mkdir -p "$QUARANTINE"`.
- **Q2.** Move the run-1 outputs and the run-1 plan out of the repository:
  ```bash
  mv docs/ideas "$QUARANTINE/ideas"
  mv docs/plans/2026-07-04-001-research-ideation-funnel.md "$QUARANTINE/"
  mv docs/wayfinder/ideation-funnel-v2 "$QUARANTINE/wayfinder-ideation-funnel-v2"
  ```
  The third move takes the planning trail that produced this spec out of the
  executing agents' possible reading surface, mechanically enforcing the
  independence requirement rather than relying on prompts alone.
- **Q3.** Record the snapshot: note the current `main` commit hash in the run log
  (Appendix C.4). This is the **quarantine-snapshot commit**; the context pack
  describes the repository exactly as it stands at this commit. In particular, the
  context pack describes the repeat-transactions artifacts plainly as they exist on
  `main` at this moment — whatever state that is — with **no forward-looking
  engineering caveats** about pending rebuild plans. (This is a deliberate, locked
  decision: the pack is a description of facts on disk, not of engineering intent.)
- **Q4.** Recreate the fresh output tree (Section 5) and commit the removal + fresh
  skeleton on a run branch (`jo/ideation-run2`), so the working tree is clean for
  the run. Run-1 files remain in git history; this residual is known and accepted —
  blind agents have no prompt-side reason to dig there, and no agent is permitted to
  run `git log`/`git show` archaeology on `docs/ideas/`.
- **Q5.** The quarantine holds until Checkpoint 4. The restore procedure is Step R1–R4
  in Section 12.4. Nothing returns earlier, and only the Phase-4 comparison agent may
  read `$QUARANTINE` in the meantime.

---

## 5. Output tree

```
docs/ideas/
├── ledger.md                     # append-only: one row per raw idea + spend/cap/flag lines
├── run-log.md                    # orchestrator log: snapshot hash, phase starts/ends, spend, pauses
├── context/
│   ├── project-brief.md          # Phase 0 output (a) — the full brief
│   ├── data-asset-inventory.md   # Phase 0 output (b) — framing-free; sole input to open-lens angle (a)
│   └── already-considered.md     # Phase 0 output (c) — flag-only register; dedupe judge input ONLY
├── candidates/
│   └── NN-slug.md                # one file per canonical (deduped) idea; reviews + probe evidence appended
├── checkpoints/
│   ├── checkpoint-1-decisions.md
│   ├── checkpoint-2-skim.md      # curator's skim document
│   ├── checkpoint-2-decisions.md
│   ├── checkpoint-3-review.md    # checkpoint-3 curator's review document
│   ├── checkpoint-3-decisions.md
│   └── checkpoint-4-decisions.md
└── proposals/
    ├── 00-ranked-index.md        # final ranked summary (the blind artifact — never edited after Phase 3)
    └── NN-slug.md                # mini-proposals for survivors
```

At the restore commit this whole tree becomes `docs/ideas/run2/` (Section 12.4).

---

## 6. Run structure at a glance

| Phase | Workflow runs | Agents (realistic) | Human gate after |
| --- | --- | --- | --- |
| 0 — Context pack | one | 3 | **Checkpoint 1** — context sign-off |
| 1 — Generation loop | one | ~30–45 generators + ≤5 dedupe judges + 1 curator | **Checkpoint 2** — post-dedupe skim, kill rights |
| 2 — Adversarial panel | two (A: scoring; B: gated originality + briefs + curator) | 3 referees + ~4–6 refuters + 1 engineer + ~15–25 searchers + ≤10 advocates + 1 curator | **Checkpoint 3** — scored review, veto/rescue rights |
| 3 — Refinement | one | 10–15 refiners + 1 synthesis | — (flows into Phase 4) |
| 4 — Comparison | one | 1 comparison agent | **Checkpoint 4** — final read; sign-off releases restore commit |

Every phase is a separate, resumable Workflow run. The panel phase splits into two
runs at the originality gate, which puts a clean pause seam inside the most expensive
phase. Checkpoints always wait for Jacopo; they are the only overnight stops
(Section 14).

---

## 7. Phase 0 — Context pack (one workflow run, then Checkpoint 1)

Three agents, run in parallel (prompts in Appendix A.1–A.3):

- **(a) Project-brief compiler** (effort: medium) writes `context/project-brief.md`.
  It reads the paper draft from the Overleaf folder at
  `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters`
  (the manuscript lives outside the git repository; recency comes from file
  modification times), plus `README.md`, `AGENTS.md`, `CONCEPTS.md`,
  `docs/pipeline_documentation.md`, and the analysis surface under
  `data/processed/`. The brief covers: the research question as currently framed;
  the full data inventory with grain, coverage, and key variables; the
  identification designs in play and their known weaknesses; the state of the model
  section (including which blocks are commented out of the compiled PDF); headline
  estimates as currently stated in the draft; and what the draft claims versus what
  it currently shows. Per Section 4 Q3, all datasets are described exactly as they
  stand at the snapshot commit, with no forward-looking engineering flags.
- **(b) Framing-free data-asset inventory** (effort: medium) writes
  `context/data-asset-inventory.md`. This agent **must not read the paper draft, the
  book, or any analysis results** — only pipeline documentation, `CONCEPTS.md`
  glossary entries, and the parquet artifacts' own metadata (schemas, row counts,
  coverage). The output is a neutral catalogue of what data exists, at what grain,
  over what coverage, with no mention of the paper's research question, designs,
  estimates, or the binding-constraints framing. It exists so that the open lens's
  "different paper" generator can receive a genuinely framing-free view of the
  assets.
- **(c) Already-considered register compiler** (effort: medium) writes
  `context/already-considered.md`. It reads Jacopo's Obsidian notes at
  `/Users/jacopoolivieri/Documents/poodle_obsidian_db/projects/sewage/`, the repo's
  `todos/`, `docs/plans/` (excluding this spec), docs reports, and any `notes/` in
  the Overleaf folder. The register lists every idea, extension, robustness plan, or
  rejected direction already on record, one line each with a source pointer.
  **Usage is flag-only:** generators never see this file; only the dedupe judge
  reads it, to annotate candidates with "matches existing note X" flags. Register
  matches are flags, never kills.

**Checkpoint 1.** Jacopo reads all three artifacts, corrects wrong premises, and adds
anything the notes missed. His corrections are transcribed into
`checkpoints/checkpoint-1-decisions.md` and applied to the context files, then read
back for confirmation. Nothing else runs until he signs off.

---

## 8. Phase 1 — Generation loop (one workflow run, then Checkpoint 2)

### 8.1 Loop mechanics

- **Round 1:** all 7 lenses × 2 angles = 14 generators run in parallel, each blind to
  its paired angle. Each proposes 4–6 ideas.
- **Dedupe judge (one per round, effort: medium):** receives the accumulated pool as
  a gist list plus the round's new ideas in full, and marks each new idea either a
  duplicate/minor variant of a named pooled entry, or genuinely novel. **Novelty
  rubric (verbatim in its prompt):** an idea is novel only if it differs in design,
  dataset, or claim — new wording is not novelty. The judge also checks new canonical
  ideas against `context/already-considered.md` and emits flag lines ("matches
  existing note X") for the ledger. For each novel idea the judge assigns the next
  canonical id and writes `candidates/NN-slug.md` containing the generator's pitch
  verbatim under a standard header.
- **Later rounds are not blind to the pool.** Each round ≥2 generator receives the
  accumulated pool as a gist-level exclusion list (titles plus one-line gists),
  framed as "do not resubmit these; find what they miss". Within-lens angle
  independence and run-1 blindness are unaffected.
- **Lens retirement:** a lens contributing no genuinely novel idea for two
  consecutive rounds sits out the rest of the loop. Surviving lenses re-run with
  **both** angles.
- **Dry test and stop rule:** a round is dry when it adds fewer than two genuinely
  novel ideas across all lenses. The loop stops after two consecutive dry rounds.
- **Hard cap:** five rounds total (round 1 plus at most four more). If the cap fires
  while lenses are still productive, the workflow writes a cap-hit line into the
  ledger naming the still-productive lenses, so Checkpoint 2 shows generation was
  truncated rather than exhausted.

### 8.2 The seven lenses and their paired angles

Full generator prompts are in Appendix A.4. Each lens splits into two angles that
differ in **method of attack**, run blind to each other:

1. **Identification and causal design.** Angle (a) constructive design-finding:
   sharpen or replace current designs, new instruments or natural experiments in the
   setting, designs the EDM data uniquely permits. Angle (b) hostile referee: write
   the top-5 rejection letter for the current draft, then convert each fatal
   objection into a research idea that pre-empts it.
2. **Welfare model and theory.** Angle (a) top-down model building: what does the
   top-5 version of this model section look like; structures that map reduced-form
   estimates to welfare (sorting, information frictions, dynamic capitalization).
   Angle (b) bottom-up measurement-first bounding: start from what is credibly
   estimated and ask what welfare statements can be bounded or point-identified with
   minimal structure.
3. **Data-asset exploitation.** Angle (a) depth in existing assets — including what
   the works-grain merge newly permits — dynamics, dry spills, news and salience,
   upstream/downstream variation. Angle (b) new acquisitions: concrete new datasets
   worth acquiring, each with a realistic acquisition path.
4. **Literature import (reworked).** Imports framings and welfare-measurement
   strategies rather than raw techniques; every candidate must state why the import
   is **easier here** than in the source paper. Angle (a) within-field: recent top-5
   environmental and urban economics. Angle (b) adjacent-field: public finance,
   industrial organization, health, spatial economics. Both angles use web search
   and the Zotero library.
5. **Regulation and firm behaviour** (the supply side run 1 never touched):
   enforcement, fines, Ofwat price reviews, the incentives that generate spills, and
   the regulatory counterfactual the damage estimates feed. Angle (a) empirical
   policy variation; angle (b) incentive modelling.
6. **Distributional incidence:** who bears the spill burden and whom counterfactual
   policies help, across income, tenure, demographics, and place. Angle (a)
   incidence measurement; angle (b) policy incidence — damage-weighted versus
   equity-weighted enforcement.
7. **The open lens** escapes the constraints framing structurally, not rhetorically.
   Angle (a) "the different paper" receives **only**
   `context/data-asset-inventory.md` — not the project brief, the draft's argument,
   or the binding-constraints framing — and asks what the best paper hiding in these
   data is. Angle (b) "the reframe" receives the full brief and asks what framing,
   headline claim, or title-level recasting makes the existing results a
   general-interest top-5 paper. **Shared feasibility tether on both angles:** every
   idea must name the specific data it runs on (existing assets or at most one
   realistic acquisition) and state the headline result a top-5 editor would care
   about; no field work, no experiments, no implausible data.

Generators never touch data and never see the already-considered register.

### 8.3 Ledger and curator skim

The record and the view are split. The **ledger** (`ledger.md`) is the append-only
audit trail: one row per raw idea with id, lens, angle, round, title, one-line gist,
and the judge's verdict (novel, or duplicate-of with the canonical id) — format in
Appendix C.1. It is never pruned.

After the loop terminates, a **curator agent** (effort: medium; prompt A.6) writes
`checkpoints/checkpoint-2-skim.md` on top of the ledger: canonical ideas grouped by
lens, each with title, a two-to-three-sentence plain-language gist written by the
curator, a link to the candidate file, and the absorbed-duplicate count. Hard
constraints: **full coverage** (no canonical idea may be dropped); **no ranking,
scoring, or recommending** (evaluation belongs to the panel); **flags rather than
filters** (suspected missed duplicates become flag lines, both ideas stay in;
pre-existing-notes flags from the dedupe stage also surface here). It may open with
one short shape-of-the-pool paragraph describing which lenses ran deep and where
ideas clustered.

### 8.4 Checkpoint 2 — post-dedupe skim with kill rights

The orchestrating session opens with a short **briefing message**: counts of raw
ideas, canonical ideas, and absorbed duplicates; the cap-hit flag if generation was
truncated; and any missed-duplicate or pre-existing-notes flags — so anything unusual
is impossible to miss. The curator's skim is the single required read; the ledger and
candidate files are drill-down links only.

Jacopo responds conversationally. The session transcribes his decisions into
`checkpoints/checkpoint-2-decisions.md` (format in Appendix B.1), resolves any
ambiguity with targeted follow-up questions **before** writing (for example, "that
matches ideas 12, 17, and 23 — kill all three?"), reads the file back to him, and
launches Phase 2 only after he confirms. **Silence means the mechanical default:** an
unmentioned canonical idea advances to the panel, and the file records "all others:
default" explicitly. If Jacopo stops mid-review, the session writes the confirmed
decisions plus an explicit "checkpoint incomplete" marker, and the Phase-2 workflow
refuses to start until he resumes and the file is confirmed complete.

---

## 9. Phase 2 — Adversarial panel (two workflow runs, then Checkpoint 3)

Criteria are unchanged from run 1 — **potential, feasibility, originality** — each
scored 1–5 (5 exceptional, 4 good, 3 moderate, 2 weak, 1 poor). **Advance requires
≥4 on all three (the min-rule).** Both the median and the min-rule are computed
deterministically by the workflow script; no agent judgment sits in the rule itself.

### 9.1 Run A — potential and feasibility

- **Potential — three independent hostile referees** (effort: xhigh; prompt A.7).
  Each referee independently sees **all** candidate one-page pitches (full-slate
  comparative scoring against the field) and scores every candidate 1–5. The
  candidate's potential score is the **median** of the three, which neutralizes a
  single harsh or generous outlier.
- **Feasibility — prosecutor-then-judge.** First the **feasibility refuter**
  (effort: medium; prompt A.8), batched by dataset (roughly 4–6 batch agents),
  actively tries to break each candidate's data claims: does the variation exist,
  are the cells big enough, is the named dataset in hand at the right grain. It may
  run read-only probes (at most 3 per candidate) under the probe protocol
  (Section 10) and writes a refutation memo per candidate. Then the **feasibility
  engineer** (effort: medium; prompt A.9) scores each candidate with the refutation
  memo as evidence, running its own checks (at most 1 probe per candidate) only
  where the memo is silent. Scores account for data-in-hand versus acquisition risk,
  and effort.

Every review is appended to the candidate's file with a one-line justification and
its evidence.

**Run A ends here** — this is the deliberate pause seam inside the most expensive
phase.

### 9.2 The originality gate (deterministic, in-script)

Candidates proceed to originality searchers if and only if:

- median potential ≥ 4 **and** feasibility ≥ 4 (the gated survivors), **or**
- exactly one of {median potential, feasibility} equals 3 and the other is ≥ 4
  (**provisional near-misses** — they get searchers so any rescue at Checkpoint 3
  carries full three-criterion evidence).

Everything else is killed on two criteria without an originality search. Expected
volume: roughly 15–25 searchers instead of one per candidate.

### 9.3 Run B — originality, devil's advocates, aggregation, curator

- **Originality searchers** (effort: medium; prompt A.10) — one per gated candidate.
  Each searches the Zotero library (via MCP) and the web for published and working
  papers, cites the nearest neighbours found, states the daylight between them and
  the idea, and scores 1–5.
- **Min-rule aggregation** (in-script): advance requires ≥4 on all three criteria.
  **Near-miss** is now defined exactly: one criterion at 3 and the other two ≥ 4.
- **Devil's advocates** (effort: high; prompt A.11) — one per near-miss, **capped at
  10 briefs**; if there are more near-misses, the worst by median potential are
  dropped first and the cap trigger is logged in the ledger. Each writes a half-page
  best case attacking the killing review's reasoning. Briefs never change scores or
  advancement; they attach to the ledger so Checkpoint-3 rescues are informed rather
  than gut calls. Advocates never probe data.
- **Checkpoint-3 curator** (effort: high; prompt A.12) writes
  `checkpoints/checkpoint-3-review.md` from the scored ledger, the panel reviews,
  and the briefs, ordered by decision type: first the **advancing candidates** (the
  veto review), sorted by median potential, each with its scores and a two-sentence
  recap; then the **rescue queue**, each near-miss with its scores, the killing
  review's core objection in one sentence, and its devil's-advocate brief inline;
  then the **remaining kills** as a compact table with scores and a one-line kill
  reason. Hard constraints mirror the Checkpoint-2 curator: full coverage, no
  re-scoring or second-guessing the panel, no recommendations about vetoes or
  rescues, flags rather than filters; scores and outcomes copied verbatim from the
  ledger. Full reviews and probe evidence stay drill-down links on candidate files.

### 9.4 Checkpoint 3 — scored review with veto and rescue rights

Briefing first (how many advanced, killed, briefed near-misses, any cap triggers),
then the review document. Same recording mechanism as Checkpoint 2: conversational
response, transcription into `checkpoints/checkpoint-3-decisions.md` (format B.2),
ambiguity resolved by follow-up before writing, read-back, explicit
"all others: default", incomplete marker blocks Phase 3.

**Rescue scope:** Jacopo may rescue **any** killed candidate, not just briefed
near-misses — the checkpoint exists precisely to override the mechanical rule. A
rescued candidate that skipped the originality searcher gets its **catch-up
searcher** run automatically at the start of Phase 3, before it joins the surviving
slate, so everything downstream carries full three-criterion evidence.

---

## 10. Probe protocol

The only agents that ever touch data are the **feasibility engineer** and the
**feasibility refuter**. Generators, the dedupe judge, potential referees,
originality searchers, devil's advocates, curators, refiners, the synthesis agent,
and the comparison agent never do. Rescue work at Checkpoint 3 does not probe.

1. **Trigger — the decisive-claim test.** Before running anything, the prober writes
   down: (a) the specific factual claim about the data being checked (existence of a
   variable, coverage of a period, approximate sample size, join feasibility);
   (b) that the context pack does not already answer it; and (c) how the feasibility
   score or attack would change if the claim fails. All three or no probe.
2. **Scope — descriptive only.** Allowed: row counts, distinct-key counts, date and
   geography coverage, missingness rates, join overlap rates, and a single small
   cross-tab or one-variable summary. Forbidden: any regression (including a
   first-stage F), any plot, anything estimating a relationship between variables.
3. **Touchable data.** Anything under `data/processed/`, strictly read-only.
   `data/raw/` and everything outside the repo's data directory are off-limits.
   The repeat-transaction artifacts are ordinary touchable datasets like any other.
4. **Per-probe caps.** One probe = one question = one script. Up to 10 minutes
   wall-clock per execution; at most 3 executions, where retries fix script bugs and
   never ask a new question; roughly 40k tokens of agent effort per probe (prompt
   guidance, not harness-enforced). Sized so a chunked pass over the largest files
   (the ~600 MB site-day panel, the 250M-row general panel) can succeed.
5. **Per-candidate quotas.** Feasibility engineer: at most 1 probe per candidate.
   Refuter: at most 3 per candidate. The quotas do the real gatekeeping.
6. **Code location.** Probe scripts are written and run only in the session
   scratchpad — nothing under the repo, nothing committed — but the exact script
   text and its raw output are preserved verbatim in the evidence record.
7. **Evidence recording.** Each probe appends one structured block to a
   "Probe evidence" section of the candidate file (format in Appendix C.3): who ran
   it; the pre-registered decisive claim (all three trigger elements, written before
   execution); the script; the raw output; a one-line verdict (claim confirmed /
   refuted / inconclusive); and the effect on the score or attack.
8. **Tooling default.** Probes run in R via `Rscript` with `arrow`, matching the
   repository's tooling.

---

## 11. Phase 3 — Refinement (one workflow run)

First, any **catch-up originality searchers** owed to Checkpoint-3 rescues run
(Section 9.4). Then one **refiner** per surviving idea (effort: high; prompt A.13)
writes `proposals/NN-slug.md`, 1–2 pages:

1. The claim, and why it raises top-5 odds (tied to a binding constraint, or for
   open-lens ideas, the general-interest case).
2. Exact data required; whether it is in hand (name the `data/processed/` dataset)
   or an acquisition with a realistic path.
3. Empirical design or model sketch — estimating equation or model ingredients.
4. The three strongest referee objections and how the design answers them.
5. Rough effort estimate (person-weeks) and a concrete first test to run.
6. The panel's scores and evidence, carried over.

Refiners do **not** run probes (Section 10 names the only two probing roles; run 1's
refinement-probe allowance does not carry over).

A final **synthesis agent** (effort: high; prompt A.14) writes
`proposals/00-ranked-index.md`: ranked list, one paragraph per proposal, cross-idea
synergies, and a suggested sequencing. After Phase 3 completes, the ranked index is
frozen — it is the provably uncontaminated blind artifact and is never edited again.

---

## 12. Phase 4 — Cross-run comparison and Checkpoint 4

### 12.1 The comparison agent

One agent (effort: high; prompt A.15), the **only run-1-aware agent in the entire
run**. It reads run 1 from `$QUARANTINE/ideas/` and the finalized run-2 artifacts
read-only; it **cannot write run-2 files** — its sole output is the memo, written to
the session scratchpad and placed by the orchestrating session at restore time.

### 12.2 Matching and verdicts

Matching runs at **idea level across both full candidate pools** (kills included on
both sides), but the memo only reports pairs and orphans where at least one side
survived to the proposal stage — mutual kills stay out. Required structured
verdicts, each carried by a one-paragraph argument:

- **Run-1-only proposals:** *coverage gap* (run 2 never generated the idea) or
  *judgment call* (run 2 generated it and its panel killed it, with the kill
  reasoning quoted for audit).
- **Run-2-only proposals:** *genuinely new* or *wider-net artifact*.
- **Corroborated pairs:** a rank-agreement note.

No pre-committed threshold triggers supplementary generation; the memo may recommend
it in prose.

### 12.3 Ranking is annotate-only

The run-2 ranked index is never edited. The memo ends with the comparison agent's own
cross-run shortlist as a clearly labelled final section, understood as a
run-1-contaminated opinion, not a run output.

### 12.4 Checkpoint 4 and the restore commit

Checkpoint 4 is one sitting: Jacopo reads the proposals, the ranked index, and the
comparison memo together. Decisions are transcribed into
`checkpoints/checkpoint-4-decisions.md` with read-back, as at the other checkpoints.
His sign-off releases the single restore-and-reorganize commit, executed by the
orchestrating session:

- **R1.** `git mv docs/ideas docs/ideas-run2-tmp` then restore run 1:
  `mv "$QUARANTINE/ideas" docs/ideas/run1` (the tree returns intact — `candidates/`,
  `proposals/`, `context/`, `ledger.md` and their relative links preserved), and
  `mv docs/ideas-run2-tmp docs/ideas/run2`.
- **R2.** Place the memo at `docs/ideas/comparison-run1-run2.md`.
- **R3.** Restore the quarantined plan and planning trail:
  `mv "$QUARANTINE/2026-07-04-001-research-ideation-funnel.md" docs/plans/` and
  `mv "$QUARANTINE/wayfinder-ideation-funnel-v2" docs/wayfinder/ideation-funnel-v2`.
  Rewrite the inbound links that point at old `docs/ideas/` paths — they exist only
  in the wayfinder map/tickets and `docs/plans/2026-07-04-001` — to `docs/ideas/run1/`
  paths.
- **R4.** One commit containing all of R1–R3. Run 1 returns to the repository at the
  same moment the effort concludes.

---

## 13. Budget, agent counts, and thinking effort

### 13.1 Posture

**No hard token guards in any workflow script.** The structural caps already locked
carry the load: five generation rounds, lens retirement, ten devil's-advocate briefs,
originality gated behind the other two criteria, and the probe quotas. Each phase
logs its token spend to `run-log.md` (via the Workflow `budget.spent()` reading at
phase end) so drift is visible at checkpoints. On Claude Max 20x the binding
constraint is usage windows, not dollars; the response to a binding window is
Section 14, never a cut.

### 13.2 Envelope (agent counts primary; token figures indicative at ~60k/agent)

| Phase | Expected agents | Worst case | Indicative tokens |
| --- | --- | --- | --- |
| 0 — Context pack | 3 | 4 | ~0.2M |
| 1 — Generation loop | 14 round-one generators; ~30–45 generator calls over the loop; ≤5 dedupe judges; 1 curator | 70 generator calls + 5 judges + 1 curator | ~2–3M |
| 2 — Panel (runs A+B) | 3 potential referees; ~4–6 feasibility refuter batches; 1 feasibility engineer; ~15–25 originality searchers; ≤10 devil's advocates; 1 checkpoint-3 curator | full searcher slate + brief cap | ~2–3M |
| 3 — Refinement | 10–15 refiners + 1 synthesis (+ catch-up searchers) | ~16 + rescues | ~1M |
| 4 — Comparison | 1 | 1 | ~0.1M |
| **Total** | **~80–110 agents (~5–7M tokens)** | **~140 agents (~9M tokens)** | |

Run 1 used about 40 agents.

### 13.3 Thinking-effort table (all agents on Fable; effort is the only knob)

| Role | Effort |
| --- | --- |
| Lens generators (all rounds) | high |
| Potential referees (×3) | xhigh |
| Refiners and synthesis agent | high |
| Devil's advocates | high |
| Comparison agent | high |
| Checkpoint-3 curator | high |
| Dedupe judge (per round) | medium |
| Checkpoint-2 curator | medium |
| Feasibility refuter | medium |
| Feasibility engineer | medium |
| Originality searchers (incl. catch-up) | medium |
| Context-pack agents (all three) | medium |

---

## 14. Pacing, pause-and-resume, and unattended auto-resume

- Each phase is its own resumable Workflow run; the panel splits into runs A and B at
  the originality gate (Section 9). These seams plus the four checkpoints are the
  only stopping points.
- **If a usage window binds mid-phase:** the phase stops where it is. The
  orchestrating session records the interrupted run's id in `run-log.md`, schedules
  its own wakeup (ScheduleWakeup) for the usage-window reset time, and when it fires,
  relaunches the phase with `Workflow({scriptPath, resumeFromRunId})` — completed
  agent calls return cached, only unfinished work re-runs. **Never improvise a
  quality cut**; no such lever exists in this design (run 1's fallback, gating
  originality, is already the default here).
- **Unattended overnight operation:** the pause-and-resume cycle runs without Jacopo.
  The session stops for the night only at checkpoints, which always wait for him.
  This requires the orchestrating Claude Code session to stay open on his machine
  for the duration of the run — state that at launch so he leaves it running.
- Every pause and resume gets a line in `run-log.md` (timestamp, phase, run id,
  reason).

---

## 15. Workflow script skeletons

Skeletons only — the executing session fills in the prompt constants from Appendix A
and the file-reading/writing glue. Structure, phases, effort levels, gates, and caps
are fixed and must not be altered. No `model:` overrides anywhere. All scripts follow
the Workflow tool's rules (no `Date.now()`; pass timestamps via `args`).

### 15.1 Phase 0

```js
export const meta = {
  name: 'ideation2-phase0-context',
  description: 'Run-2 context pack: brief, framing-free inventory, register',
  phases: [{ title: 'Context' }],
}
phase('Context')
const [brief, inventory, register] = await parallel([
  () => agent(PROMPT_PROJECT_BRIEF,   { label: 'project-brief',   effort: 'medium' }),
  () => agent(PROMPT_DATA_INVENTORY,  { label: 'data-inventory',  effort: 'medium' }),
  () => agent(PROMPT_REGISTER,        { label: 'register',        effort: 'medium' }),
])
return { brief, inventory, register }   // orchestrator writes the three context/ files
```

### 15.2 Phase 1 — generation loop

```js
export const meta = {
  name: 'ideation2-phase1-generation',
  description: 'Loop-until-dry generation: 7 lenses x 2 angles, per-round dedupe, curator skim',
  phases: [{ title: 'Generate' }, { title: 'Curate' }],
}
// args: { lensAngles: [...14 briefs], contextPack: {...}, inventoryOnlyBrief: {...} }
const pool = []            // canonical ideas: {id, lens, title, gist}
let activeLenses = new Set(args.lensAngles.map(a => a.lens))
const lensDrySpells = {}   // lens -> consecutive rounds without a novel idea
let consecutiveDry = 0, round = 0
while (round < 5 && consecutiveDry < 2 && activeLenses.size > 0) {
  round += 1
  const exclusion = pool.map(p => `${p.title} — ${p.gist}`).join('\n')
  const roundIdeas = await parallel(
    args.lensAngles.filter(a => activeLenses.has(a.lens)).map(a => () =>
      agent(generatorPrompt(a, round, exclusion), {
        label: `gen:${a.lens}:${a.angle}:r${round}`, phase: 'Generate',
        effort: 'high', schema: IDEAS_SCHEMA })))
  const judged = await agent(dedupeJudgePrompt(pool, roundIdeas.filter(Boolean)), {
    label: `dedupe:r${round}`, phase: 'Generate', effort: 'medium', schema: JUDGE_SCHEMA })
  // judge output: per raw idea -> {novel | duplicateOf}, register flags, candidate files written
  const novelByLens = tallyNovel(judged)                 // plain code
  pool.push(...judged.novel)
  for (const lens of activeLenses) {
    lensDrySpells[lens] = novelByLens[lens] > 0 ? 0 : (lensDrySpells[lens] || 0) + 1
    if (lensDrySpells[lens] >= 2) activeLenses.delete(lens)   // lens retirement
  }
  consecutiveDry = judged.novel.length < 2 ? consecutiveDry + 1 : 0
  log(`round ${round}: ${judged.novel.length} novel, pool ${pool.length}`)
}
if (round === 5 && activeLenses.size > 0) log(`CAP HIT — still productive: ${[...activeLenses]}`)
phase('Curate')
const skim = await agent(curatorPrompt(pool), { label: 'curator', effort: 'medium' })
return { pool, skim, capHit: round === 5 && activeLenses.size > 0, stillProductive: [...activeLenses] }
```

### 15.3 Phase 2, run A — scoring

```js
export const meta = {
  name: 'ideation2-phase2a-scoring',
  description: 'Panel run A: 3 potential referees (median), feasibility prosecutor-then-judge',
  phases: [{ title: 'Potential' }, { title: 'Feasibility' }],
}
// args: { candidates: [...], datasetBatches: [...] }
const [potentialVotes, refutations] = await parallel([
  () => parallel([1, 2, 3].map(i => () =>
    agent(potentialRefereePrompt(args.candidates), {
      label: `potential:referee${i}`, phase: 'Potential', effort: 'xhigh', schema: SCORES_SCHEMA }))),
  () => parallel(args.datasetBatches.map(b => () =>
    agent(refuterPrompt(b), {
      label: `refute:${b.dataset}`, phase: 'Feasibility', effort: 'medium', schema: REFUTATION_SCHEMA }))),
])
const medians = computeMedians(potentialVotes.filter(Boolean))       // deterministic
const feasibility = await agent(feasibilityEngineerPrompt(args.candidates, refutations), {
  label: 'feasibility-engineer', phase: 'Feasibility', effort: 'medium', schema: SCORES_SCHEMA })
return { medians, feasibility, refutations }
```

### 15.4 Phase 2, run B — gate, originality, briefs, curator

```js
export const meta = {
  name: 'ideation2-phase2b-gate',
  description: 'Panel run B: originality gate, searchers, devil briefs, min-rule, checkpoint-3 review',
  phases: [{ title: 'Originality' }, { title: 'Briefs' }, { title: 'Curate' }],
}
// args: { candidates, medians, feasibility }
const gated = args.candidates.filter(c => passesGate(c, args.medians, args.feasibility))
log(`originality gate: ${gated.length}/${args.candidates.length} candidates get searchers`)
const originality = await parallel(gated.map(c => () =>
  agent(originalityPrompt(c), { label: `orig:${c.id}`, phase: 'Originality',
    effort: 'medium', schema: ORIG_SCHEMA })))
const outcomes = applyMinRule(args.candidates, args.medians, args.feasibility, originality)
const nearMisses = outcomes.filter(o => o.nearMiss)
  .sort((a, b) => b.medianPotential - a.medianPotential).slice(0, 10)   // cap 10, worst dropped
if (outcomes.filter(o => o.nearMiss).length > 10) log('BRIEF CAP HIT — worst near-misses dropped')
phase('Briefs')
const briefs = await parallel(nearMisses.map(n => () =>
  agent(devilsAdvocatePrompt(n), { label: `brief:${n.id}`, effort: 'high' })))
phase('Curate')
const review = await agent(checkpoint3CuratorPrompt(outcomes, briefs), {
  label: 'cp3-curator', effort: 'high' })
return { outcomes, briefs, review }
```

### 15.5 Phase 3 — refinement

```js
export const meta = {
  name: 'ideation2-phase3-refinement',
  description: 'Catch-up searchers for rescues, one refiner per survivor, synthesis index',
  phases: [{ title: 'CatchUp' }, { title: 'Refine' }, { title: 'Synthesize' }],
}
// args: { survivors, rescuesNeedingSearcher }
const catchUps = await parallel(args.rescuesNeedingSearcher.map(c => () =>
  agent(originalityPrompt(c), { label: `orig-catchup:${c.id}`, phase: 'CatchUp',
    effort: 'medium', schema: ORIG_SCHEMA })))
phase('Refine')
const proposals = await parallel(args.survivors.map(s => () =>
  agent(refinerPrompt(s), { label: `refine:${s.id}`, phase: 'Refine', effort: 'high' })))
phase('Synthesize')
const index = await agent(synthesisPrompt(args.survivors), { label: 'synthesis', effort: 'high' })
return { catchUps, proposals, index }
```

### 15.6 Phase 4 — comparison

```js
export const meta = {
  name: 'ideation2-phase4-comparison',
  description: 'Single read-only cross-run comparison agent',
  phases: [{ title: 'Compare' }],
}
phase('Compare')
const memo = await agent(comparisonPrompt(args.quarantinePath), {
  label: 'comparison', effort: 'high' })
return { memo }   // orchestrator places the memo file at restore time
```

---

## Appendix A — Full prompt texts

Placeholders in `{{DOUBLE_BRACES}}` are filled by the orchestrating session or the
workflow script at launch. The writing-style block below is inserted verbatim
wherever a prompt says `{{STYLE}}`:

> Write for a reader who has not seen your working context. Use complete plain
> sentences. Spell out every term; introduce every identifier with its own
> plain-language clause. Do not use arrow chains, invented shorthand, or
> abbreviations of your own coinage. Open every summary with the outcome.

### A.1 Project-brief compiler (Phase 0a, medium)

> You are compiling the project brief for a research-ideation exercise on an
> economics paper about English sewage spills and housing markets. Your output is
> the primary context document that idea-generating agents will rely on, so it must
> be accurate, current, and complete. Read: the LaTeX manuscript at
> `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our
> Waters` (compiled order is defined in `_main.tex`; note which inputs are commented
> out — report commented-out blocks as present-but-disabled); `README.md`,
> `AGENTS.md`, `CONCEPTS.md`, and `docs/pipeline_documentation.md` in the
> repository; and the analysis-ready datasets under `data/processed/` (schemas, row
> counts, grain, coverage — read parquet metadata, do not process data). Write a
> brief containing: (1) the research question as currently framed; (2) a data
> inventory with grain, coverage, and key variables for every analysis-relevant
> dataset, including dormant assets no analysis currently reads; (3) the
> identification designs in play and their known weaknesses; (4) the state of the
> model section, including which blocks exist but are disabled; (5) the headline
> estimates as stated in the draft, with their specifications; (6) what the draft
> claims versus what it currently shows. Describe every dataset exactly as it stands
> on disk right now; do not speculate about planned changes, pending rebuilds, or
> engineering intent, and do not add forward-looking caveats. Do not read anything
> under `docs/ideas/` (it does not exist during this run) and do not use git history
> to reconstruct removed files. {{STYLE}}

### A.2 Framing-free data-asset inventory (Phase 0b, medium)

> You are cataloguing the data assets of an empirical economics project. You must
> not read the paper draft, the Quarto book under `book/`, any file under
> `docs/reports/`, or any analysis results — your output must contain no trace of
> the project's research question, hypotheses, designs, estimates, or framing. Read
> only: `docs/pipeline_documentation.md`, the glossary in `CONCEPTS.md`, and the
> artifacts under `data/processed/` themselves (parquet schemas, row counts,
> partitioning, coverage — metadata only, no data processing). Produce a neutral
> inventory: for each dataset, what one row is, the keys, the coverage in time and
> space, the important variables with plain-language descriptions, approximate
> size, and how it relates to the other datasets (join keys, provenance). Include
> dormant assets that nothing currently reads. Write it so a stranger could learn
> what data exists here without learning what the owners are trying to show with
> it. {{STYLE}}

### A.3 Already-considered register compiler (Phase 0c, medium)

> You are compiling a register of research ideas already on record for the sewage
> project, to be used only for flagging duplicates later — never to kill ideas.
> Read: every note under
> `/Users/jacopoolivieri/Documents/poodle_obsidian_db/projects/sewage/`; the
> repository's `todos/` (including `_archive/`); `docs/plans/` (skip
> `2026-07-09-001-research-ideation-funnel-v2.md`, the document driving this run);
> rendered reports under `docs/reports/`; and any `notes/` folder in the Overleaf
> directory at `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/
> Sewage in Our Waters`. List every idea, extension, robustness plan, or rejected
> direction you find: one line each — a plain-language description followed by a
> source pointer (file path, heading). Do not editorialize, rank, or filter; a
> shallow half-sentence idea still gets a line. Do not read anything under
> `docs/ideas/`. {{STYLE}}

### A.4 Generator template (Phase 1, high — 14 instances, one per lens-angle)

The template below is instantiated with the per-angle briefs from Section 8.2. The
open lens's angle (a) instance receives `{{CONTEXT}}` = the framing-free data-asset
inventory **only**; every other instance receives `{{CONTEXT}}` = the full project
brief. `{{EXCLUSION_BLOCK}}` is empty in round 1.

> You are one of several idea generators in a research-ideation exercise for an
> economics paper targeting a top-5 journal. Your assigned lens and method of
> attack: {{ANGLE_BRIEF}}. Context document: {{CONTEXT}}.
>
> {{EXCLUSION_BLOCK — rounds 2+ only:}} The following ideas are already in the
> pool. Do not resubmit them or minor variants of them; your job this round is to
> find what they miss: {{POOL_GIST_LIST}}.
>
> Propose 4 to 6 ideas. For each, provide: a title; a one-paragraph pitch; why it
> materially raises the odds of publishing in a top-5 journal
> {{CONSTRAINT_CLAUSE — for lenses 1–6: "and which binding constraint
> (identification/empirics or the welfare model) it targets"; for the open lens:
> "as a general-interest paper"}}; a sketch of the empirical design or model
> change; and the data needed, naming specific datasets in hand or, at most, one
> realistic acquisition with its path. No field work, no experiments, no
> implausible data. You have no access to data — reason from the context document
> only. Quality over quantity: a sharp, referee-proof idea beats two vague ones.
> {{LENS_4_CLAUSE — literature-import instances only: "For every idea, state
> explicitly why executing the imported framing is easier in this setting than in
> the source paper."}} {{STYLE}}

### A.5 Dedupe judge (Phase 1, per round, medium)

> You are the dedupe judge for round {{N}} of an idea-generation loop. Inputs: the
> accumulated pool of canonical ideas (titles and one-line gists): {{POOL}}; this
> round's new ideas in full: {{NEW_IDEAS}}; and a register of pre-existing ideas
> from the project's notes: {{REGISTER}}. For each new idea, decide: is it a
> duplicate or minor variant of a named pooled entry, or genuinely novel? Novelty
> means a different empirical design, a different dataset, or a different claim —
> new wording of an existing entry is not novelty. For each novel idea, assign the
> next canonical id, write `docs/ideas/candidates/NN-slug.md` containing the
> generating agent's pitch verbatim under a header with id, lens, angle, and round,
> and give a one-line gist for the pool list. For each duplicate, name the
> canonical id it duplicates. Separately, flag any new canonical idea that matches
> an entry in the register — a flag line naming the register entry, never a kill.
> Return the full verdict table; every raw idea must receive exactly one verdict.

### A.6 Checkpoint-2 curator (Phase 1, medium)

> You are writing the skim document a human will read to review an idea pool before
> an expensive evaluation panel runs. Input: the append-only ledger of raw ideas
> with dedupe verdicts, and the canonical candidate files. Hard constraints: full
> coverage — you may not drop or collapse any canonical idea; no ranking, scoring,
> or recommending — evaluation belongs to a later panel, not to you; flags rather
> than filters — if you suspect two canonical ideas are really duplicates, write a
> flag line and keep both. Structure: open with one short paragraph on the shape of
> the pool (which lenses ran deep, where ideas clustered); then canonical ideas
> grouped by lens, each with its title, a two-to-three-sentence plain-language gist
> you write yourself, a link to its candidate file, and the count of absorbed
> duplicates. Surface any pre-existing-notes flags from the dedupe stage as flag
> lines under the affected idea. {{STYLE}}

### A.7 Potential referee (Phase 2A, xhigh — three independent instances)

> You are a hostile referee for a top-5 economics journal, evaluating the
> publication potential of research ideas for a paper on sewage spills and housing
> markets. Here is the full slate of {{K}} candidate pitches: {{ALL_PITCHES}}.
> Score every candidate 1–5 (5 exceptional, 4 good, 3 moderate, 2 weak, 1 poor) on
> one question only: would pursuing this idea materially raise the paper's odds at
> a top-5 journal? Score comparatively against this slate, not against an abstract
> standard — your scores should spread across the range. For each candidate, give
> a one-to-three-sentence justification in the voice of a referee report: what the
> idea buys, and what a referee would still object to. You are one of three
> independent referees; do not hedge toward a middle score. You have no access to
> data or to the other referees' views.

### A.8 Feasibility refuter (Phase 2A, medium — batched by dataset)

> You are the prosecution in a feasibility trial. For each candidate idea in your
> batch (all of which claim to use {{DATASET_GROUP}}), actively try to break its
> data claims: does the claimed variation exist, are the cells big enough, is the
> named dataset actually in hand at the grain the idea needs, do the claimed joins
> work? You may run read-only probes on `data/processed/` under the probe protocol
> given below, with a hard quota of at most 3 probes per candidate. {{PROBE_PROTOCOL
> — Sections 10.1–10.4, 10.6–10.8 verbatim}}. For each candidate write a refutation
> memo: the strongest attacks you found, each marked as confirmed by a probe,
> asserted from the context pack, or untested; append each probe's evidence block
> to the candidate's file. You do not score — a separate judge will weigh your memo.
> Attack honestly: a claim you tried and failed to break is stated as such.

### A.9 Feasibility engineer (Phase 2A, medium)

> You are the judge in a feasibility trial. For each candidate: the pitch, and the
> prosecution's refutation memo: {{CANDIDATES_WITH_MEMOS}}. Score feasibility 1–5
> (5 exceptional, 4 good, 3 moderate, 2 weak, 1 poor), weighing: whether the data
> is in hand versus an acquisition with risk; the refutation memo's confirmed
> attacks; and realistic effort. Where the memo is silent on a decisive question,
> you may run at most 1 read-only probe per candidate under the probe protocol
> given below. {{PROBE_PROTOCOL — same insert as A.8}}. Append your review to each
> candidate file with a one-line justification and your evidence; probes get
> evidence blocks. Your score is final for this criterion — do not defer to the
> refuter; weigh it.

### A.10 Originality searcher (Phase 2B and catch-up, medium — one per gated candidate)

> You are checking the originality of one research idea: {{PITCH}}. Search the
> Zotero library (via the Zotero MCP tools) and the web for published papers and
> working papers close to it. Cite the nearest neighbours you find — author, year,
> title, venue — and state precisely the daylight between each neighbour and this
> idea: what the idea adds that the neighbour does not have. Score originality 1–5
> (5 exceptional, 4 good, 3 moderate, 2 weak, 1 poor). An idea that is a known
> design applied to a novel setting with a novel data asset can still score well if
> the daylight is real; an idea whose exact claim exists in a working paper cannot.
> Append your review, with the neighbour list, to the candidate file.

### A.11 Devil's advocate (Phase 2B, high — one per capped near-miss)

> A research idea was killed by an evaluation panel on exactly one criterion, and a
> human reviewer will decide whether to rescue it. Idea: {{PITCH}}. Full scores:
> {{SCORES}}. The killing review: {{KILLING_REVIEW}}. Write a half-page best case
> for this idea that attacks the killing review's reasoning directly: where its
> logic is weakest, what evidence it ignored or misread, and what version of the
> idea survives the objection. You are not re-scoring and your brief changes
> nothing mechanically — it informs the human's rescue decision. Argue honestly and
> concretely; a brief that overclaims will be discounted. {{STYLE}}

### A.12 Checkpoint-3 curator (Phase 2B, high)

> You are writing the review document a human will read to exercise veto and rescue
> rights over a scored idea slate. Inputs: the scored ledger, the panel reviews on
> each candidate file, and the devil's-advocate briefs: {{INPUTS}}. Hard
> constraints: full coverage — every candidate appears in exactly one section; no
> re-scoring or second-guessing the panel; no recommendations about what the human
> should veto or rescue; flags rather than filters; scores and outcomes copied
> verbatim from the ledger. Structure, in order: (1) the advancing candidates,
> sorted by median potential, each with its three scores and a two-sentence recap
> of what it is and what the panel liked; (2) the rescue queue — each near-miss
> with its scores, the killing review's core objection compressed to one sentence,
> and its devil's-advocate brief inline in full; (3) the remaining kills as a
> compact table: id, title, scores, one-line kill reason. Point to candidate files
> for full reviews and probe evidence rather than inlining them. {{STYLE}}

### A.13 Refiner (Phase 3, high — one per survivor)

> You are writing the mini-proposal for a research idea that survived adversarial
> evaluation, for the paper's coauthors to read. Candidate file, including the
> pitch, all panel reviews, probe evidence, and scores: {{CANDIDATE_FILE}}. Project
> brief: {{BRIEF}}. Write a 1–2 page proposal with exactly these six parts: (1) the
> claim, and why it raises top-5 odds — tied explicitly to a binding constraint
> (identification/empirics or the welfare model), or for open-lens ideas, the
> general-interest case; (2) the exact data required, naming the `data/processed/`
> dataset if in hand, or the acquisition and its realistic path; (3) the empirical
> design or model sketch — an estimating equation or the model ingredients; (4) the
> three strongest referee objections and how the design answers each; (5) a rough
> effort estimate in person-weeks and the concrete first test to run; (6) the
> panel's scores and key evidence, carried over verbatim. You have no data access;
> build on the probe evidence already on file. {{STYLE}}

### A.14 Synthesis agent (Phase 3, high)

> You are writing the ranked index over {{K}} mini-proposals: {{PROPOSALS}}. and the
> scored ledger: {{LEDGER_EXTRACT}}. Produce `00-ranked-index.md`: a ranked list —
> your ranking, informed by the panel's scores but exercising judgment about the
> paper's overall portfolio — with one paragraph per proposal stating what it is
> and why it sits where it sits; then cross-idea synergies (for example, one
> dataset acquisition unlocking two proposals); then a suggested sequencing for the
> coauthors. Every proposal appears exactly once. {{STYLE}}

### A.15 Comparison agent (Phase 4, high)

> Two blind research-ideation runs were performed on the same economics paper, and
> you are the only agent permitted to see both. Run 1 lives at {{QUARANTINE}}/ideas/
> (candidates, proposals, ledger, ranked index). Run 2 lives at `docs/ideas/` in the
> repository. Both are read-only to you; your only output is a comparison memo
> returned as your final message. Match ideas at idea level across both full
> candidate pools, kills included. Report only pairs and orphans where at least one
> side survived to the proposal stage. For every run-1-only proposal, classify it
> as a coverage gap (run 2 never generated the idea) or a judgment call (run 2
> generated it and its panel killed it — quote the kill reasoning for audit). For
> every run-2-only proposal, classify it as genuinely new or a wider-net artifact.
> For corroborated pairs, note rank agreement between the two indexes. Every
> verdict carries a one-paragraph argument. You may recommend supplementary
> generation in prose if the pattern warrants it; there is no mechanical trigger.
> Do not edit or re-rank anything. End the memo with a clearly labelled final
> section, "Cross-run shortlist (comparison agent's opinion)", containing your own
> combined shortlist — labelled as a run-1-contaminated opinion, not a run output.
> {{STYLE}}

---

## Appendix B — Decision-file formats

### B.1 `checkpoints/checkpoint-2-decisions.md`

```markdown
# Checkpoint 2 decisions — confirmed by read-back on {{DATE}}

- C07: kill — Jacopo: "duplicate in spirit of C03, weaker version"
- C19: kill — Jacopo: "we tried this in 2025, data cells too thin"
- All others: default (advance to panel)

Status: COMPLETE
```

One line per exception, candidate id first, then the action (`kill`), then Jacopo's
stated reason verbatim. The final `All others: default` line and the
`Status: COMPLETE` line are mandatory. If the review was interrupted, the status
line reads `Status: INCOMPLETE — checkpoint must resume before Phase 2`, and the
Phase-2 workflow refuses to launch.

### B.2 `checkpoints/checkpoint-3-decisions.md`

Same conventions. Actions are `veto` (kill an advancing candidate) and `rescue`
(revive any killed candidate — not only briefed near-misses). Every rescue line
notes whether the candidate already has an originality search; if not, the Phase-3
run schedules the catch-up searcher automatically:

```markdown
# Checkpoint 3 decisions — confirmed by read-back on {{DATE}}

- C04: veto — Jacopo: "coauthors will not want to touch Ofwat politics this year"
- C22: rescue (has originality search) — Jacopo: "the brief is right, the refuter misread the panel grain"
- C31: rescue (needs catch-up originality search) — Jacopo: "worth it even at feasibility 3"
- All others: default (min-rule outcome stands)

Status: COMPLETE
```

### B.3 Checkpoints 1 and 4

Free-form transcriptions of Jacopo's corrections (Checkpoint 1) and final
instructions (Checkpoint 4), same read-back-then-confirm procedure, same
`Status: COMPLETE` requirement before the next step runs.

---

## Appendix C — Record formats

### C.1 Ledger row (one per raw idea, append-only)

```markdown
| id | lens | angle | round | title | gist | verdict |
| R041 | welfare | b | 2 | Bounding CV from rental sorting | <one line> | novel → C23 |
| R042 | welfare | a | 2 | Dynamic capitalization model | <one line> | duplicate-of C09 |
```

Raw ids `RNNN` are per raw submission; canonical ids `CNN` are assigned by the
dedupe judge. Flag lines, cap-hit lines, and per-phase spend lines are appended as
plain bullet lines beneath the table, never edits to it.

### C.2 Candidate file (`candidates/NN-slug.md`)

Header (id, title, lens, angle, round, status), the generator's pitch verbatim,
then appended sections in arrival order: `## Panel reviews` (potential ×3 with the
median stated, feasibility refutation memo, feasibility score, originality),
`## Probe evidence` (blocks per C.3), `## Devil's-advocate brief` (if any),
`## Checkpoint decisions` (if any).

### C.3 Probe-evidence block

```markdown
### Probe {{n}} — {{scorer|refuter}}, {{DATE}}
**Decisive claim (pre-registered):** (a) the factual claim; (b) why the context
pack does not answer it; (c) how the score/attack changes if it fails.
**Script:** ```r
<verbatim Rscript source>
```
**Raw output:** ```
<verbatim output>
```
**Verdict:** claim confirmed | refuted | inconclusive.
**Effect on score/attack:** <one line>.
```

### C.4 Run log (`run-log.md`)

Append-only lines: quarantine-snapshot commit hash; each phase's workflow run id,
start, end, and token spend (`budget.spent()` at phase end); every pause (usage
window, run id recorded) and resume (wakeup fired, resumed from run id); checkpoint
open/confirm timestamps.

---

*This spec is the destination of the wayfinder map "Ideation Funnel v2 — locked
spec" (`docs/wayfinder/ideation-funnel-v2/`, quarantined during the run). It locks
the decisions of tickets 00–10; launch requires Jacopo's sign-off on this document.*
