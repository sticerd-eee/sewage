# Research Ideation Funnel — Multi-Agent Plan

**Date:** 2026-07-04
**Status:** Approved design, not yet run
**Owner:** Jacopo

## Objective

Generate, adversarially evaluate, and refine research ideas that raise the odds of the
sewage-spills paper publishing in a top-5 economics journal. Effort concentrates on the
two agreed binding constraints:

1. **Identification credibility and empirics** — the causal designs (event studies,
   hydraulics/rainfall instrument) and what more the data can support.
2. **The welfare model** — currently a rough first draft; the step from hedonic price
   effects to welfare statements is the thinnest part of the paper.

The data asset (EDM, consents, HadUK rainfall, Land Registry, Zoopla rentals,
LexisNexis news) is a strength; ideas requiring *new* data acquisition are admissible
if the acquisition path is spelled out. No hard vetoes — effort and delay are captured
in the feasibility score, not by exclusion.

**End deliverable:** 5–8 mini-proposals (1–2 pages each) plus a ranked index, in
`docs/ideas/proposals/`.

## Guardrails (apply to every agent in every phase)

- **No implementation.** Ideas are refined, not built.
- **Repo writes only under `docs/ideas/`.** All throwaway probe code lives in the
  session scratchpad, never in `scripts/` or anywhere else in the repo.
- **Data is read-only.** Probes and pilot regressions use existing analysis-ready
  datasets in `data/final/` only — no raw-data processing, no new pipelines.
- **Ledger discipline.** Every idea gets a row in `docs/ideas/ledger.md` the moment it
  is proposed (id, title, one-liner, lens, status). Agents check the ledger and the
  already-considered register before proposing; duplicates are flagged, not counted.

## Note-taking scheme

```
docs/ideas/
├── ledger.md                  # single source of truth: id, title, one-liner, lens,
│                              # P/F/O scores, verdict, status
├── context/
│   ├── project-brief.md       # Phase 0 output (a)
│   └── already-considered.md  # Phase 0 output (b)
├── candidates/
│   └── NN-slug.md             # one file per generated idea (pitch + panel reviews appended)
└── proposals/
    ├── 00-ranked-index.md     # final ranked summary
    └── NN-slug.md             # mini-proposals for survivors
```

## Phase 0 — Context pack (2–3 agents, then CHECKPOINT 1)

Run as one small workflow.

**Output (a): `context/project-brief.md`** — compiled by one agent reading:
- The paper: `01_introduction.tex`, `04_causal_impacts.tex`, `05_hydraulics_instrument.tex`,
  `06_research_question.tex`, `07_model.tex` (the two core sections quoted at length,
  others summarised), plus the appendices on dry spills and identification comparison.
- Repo: `README.md`, `AGENTS.md`, `CONCEPTS.md`, `data/final/` inventory (what
  analysis-ready datasets exist, their units, coverage, key variables).
- Current results: headline estimates and designs actually in the draft.

Brief contents: research question as currently framed; data inventory with coverage;
identification designs in play and their known weaknesses; state of the model section;
what the draft claims vs. what it shows.

**Output (b): `context/already-considered.md`** — compiled by a second agent reading:
- Obsidian: `/Users/jacopoolivieri/Documents/poodle_obsidian_db/projects/sewage/` (all notes).
- Repo: `todos/`, `docs/plans/`, `docs/` reports, `notes/` in the Overleaf folder.

Register contents: every idea, extension, robustness plan, or rejected direction already
on record, each with a one-line description and a source pointer. This is the dedupe
baseline — generators must not re-propose entries as new (they MAY build on them if the
extension is substantive, citing the register entry).

**CHECKPOINT 1:** Jacopo reviews both files, corrects wrong premises, adds anything the
notes missed. Nothing else runs until sign-off.

## Phase 1 — Generation + panel evaluation (one workflow, then CHECKPOINT 2)

### Generation (5 lens agents, in parallel)

Each generator receives the project brief + already-considered register and proposes
**4–6 ideas** with: title, one-paragraph pitch, why it targets a binding constraint,
sketch of design/model change, data needed. Lenses:

1. **Identification & causal design** — sharpen or replace current designs; new
   instruments/natural experiments in the setting; designs the EDM data uniquely permits.
2. **Welfare model & theory** — model structures that map the reduced-form estimates to
   welfare; sorting, information frictions, dynamic capitalization; what the top-5
   version of this model section looks like.
3. **Data-asset exploitation** — dynamics, dry spills, news/salience via LexisNexis,
   upstream/downstream variation, and concrete *new* datasets worth acquiring.
4. **Literature import** — scan recent top-5 environmental/urban/public papers (web
   search + Zotero) for techniques, framings, or welfare approaches that port here.
5. **Hostile referee** — write the top-5 rejection letter for the current draft, then
   convert each fatal objection into a research idea that pre-empts it.

### Dedupe (1 agent, barrier)

Merge near-duplicates across lenses, check against the register, assign ids, write one
`candidates/NN-slug.md` per distinct idea, target ~20 distinct candidates. Log what was
merged or flagged.

### Panel evaluation (min-rule, one criterion per specialist)

Scales are anchored 1–5: 5 exceptional, 4 good, 3 moderate, 2 weak, 1 poor.
**Advance requires ≥4 on ALL THREE criteria.** A 3 or below on any single criterion
kills the idea (mirrors how referees reject). No appeal round.

- **Potential (hostile-referee evaluator):** would this materially raise top-5 odds?
  Batch evaluation (all candidates together) so scores are comparative, not absolute.
- **Feasibility (empirical engineer):** runs read-only recon and quick pilot
  regressions on `data/final/` where an idea's key variation can be checked cheaply.
  Scores account for data-in-hand vs. acquisition risk, and effort. Batched by dataset.
- **Originality (novelty checker):** one agent per candidate (each needs its own
  searches): Zotero library via MCP + web search for published and working papers.
  Cites the nearest neighbours found and states the daylight between them and the idea.

Each review is appended to the candidate's file with a one-line justification and
evidence (probe output, papers found). The workflow script aggregates scores into the
ledger deterministically — no agent judgment in the min-rule itself.

**CHECKPOINT 2:** Jacopo reviews the scored ledger. He may veto survivors or rescue
killed ideas (rescues get a note explaining the override). Refinement runs only on the
post-review list.

## Phase 2 — Refinement (one agent per survivor + synthesis, then CHECKPOINT 3)

One agent per surviving idea (expected 5–8) writes `proposals/NN-slug.md`, 1–2 pages:

1. The claim, and why it raises top-5 odds (explicitly tied to a binding constraint).
2. Exact data required; whether it is in hand (name the `data/final/` dataset) or an
   acquisition with a realistic path.
3. Empirical design or model sketch — estimating equation or model ingredients.
4. The three strongest referee objections and how the design answers them.
5. Rough effort estimate (person-weeks) and a concrete first test to run.
6. The panel's scores and evidence, carried over.

Refinement agents may run further scratchpad probes to sharpen the first test.

A final synthesis agent writes `proposals/00-ranked-index.md`: ranked list, one
paragraph per proposal, cross-idea synergies (e.g. one dataset unlocking two ideas),
and a suggested sequencing.

**CHECKPOINT 3:** final read; outcome feeds the next coauthor meeting.

## Cost and scale (approximate agent counts)

| Phase | Agents | Notes |
| --- | --- | --- |
| 0 — Context pack | 2–3 | heavy reading, no search |
| 1 — Generation | 5 | lens agents, web/Zotero search for lenses 4–5 |
| 1 — Dedupe | 1 | barrier step |
| 1 — Panel | ~24–26 | 2 batch evaluators + ~20 per-idea novelty checkers (search-heavy) |
| 2 — Refinement | 6–9 | one per survivor + synthesis |

The panel's novelty checks are the biggest spend block — this was a deliberate choice
(3-specialist panel over single evaluator). If cost needs trimming at Checkpoint 2,
the lever is rescoping novelty checks to survivors-of-the-other-two-criteria only
(saves ~half the novelty agents; the workflow script can order feasibility + potential
first and only novelty-check ideas that cleared both).

## Model and thinking effort

**All agents run on Fable (claude-fable-5)** — Jacopo's hard constraint. Workflow
scripts must NOT set per-agent model overrides; agents inherit the session model.
Thinking effort is set per agent:

| Agents | Effort | Rationale |
| --- | --- | --- |
| 5 lens generators | high | idea quality is the product |
| Potential evaluator (hostile referee) | xhigh | comparative top-5 judgment, main gatekeeper; single batch agent so the upgrade is nearly free |
| Phase 2 refinement + synthesis | high | final deliverable for coauthors |
| Phase 0 context-pack agents | medium | faithful reading/summarising |
| Feasibility engineer | medium | value comes from probes, not reasoning depth |
| ~20 novelty checkers | medium | search coverage + comparison; biggest block, main saving |
| Dedupe agent | low | mechanical merging against the register |

## Execution notes

- Jacopo is on Claude Max 20x; run as designed (no panel reordering needed unless
  usage windows bind — the novelty-check trim in the cost section remains the fallback).
- Each phase is a separate Workflow run (deterministic fan-out, resumable via
  `resumeFromRunId` if interrupted; scripts persisted under the session directory).
- Probes run in R (project uses `rv`; see `AGENTS.md` for repo conventions).
- Generator/refiner prompts must instruct agents to write for a reader who has not
  seen the working context: complete sentences, terms spelled out, no invented
  shorthand or arrow chains (per Jacopo's standing writing-style requirement).
