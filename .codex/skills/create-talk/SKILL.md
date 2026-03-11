---
name: create-talk
description: Create or heavily revise Beamer talks for the sewage project from the manuscript and supporting outputs. Use when Codex needs to draft a job-market, seminar, short, or lightning talk, reshape an existing slide deck, or turn the linked Overleaf paper into a presentation while keeping every claim traceable to the paper.
---

# Create Talk

Create or refactor a talk deck with the paper as the only authority for factual claims.

## Default Paths

Prefer manuscript and slide sources in the linked Overleaf repo:
- paper: `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/_main.tex`
- slides: `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/slides/`

Fallback mirrors in this repo:
- `docs/overleaf/_main.tex`
- `docs/overleaf/slides/`

If a format-specific deck already exists, edit it in place. Otherwise create `[format]_talk.tex` in the target `slides/` directory.

Save review notes to `docs/overleaf/slides/quality_reports/[format]_talk_review.md` in this repo unless the user asks to keep them elsewhere.

## Formats

| Format | Main Slides | Duration | Scope |
|--------|-------------|----------|-------|
| `job-market` | 40-50 | 45-60 min | Full question, institutional context, identification, main results, robustness, mechanism, and contribution |
| `seminar` | 25-35 | 30-45 min | Motivation, design, headline results, a few high-value robustness checks, and implications |
| `short` | 10-15 | 12-15 min | Question, data, empirical approach, headline result, one or two supporting pieces, and takeaway |
| `lightning` | 3-5 | 3-5 min | Hook, one result, why it matters |

## Workflow

1. Resolve the requested format and the paper path. If the user does not specify a format, infer it only when the surrounding request makes that obvious; otherwise ask.
2. Read the paper sections, source tables, existing slide decks, and relevant figure assets before drafting.
3. If the task is substantial, split it into creator and critic passes:
   - optionally use one explorer or worker to mine the paper and supporting outputs for facts, figures, and candidate visuals;
   - optionally use a separate explorer to critique the resulting deck for pacing and fidelity;
   - keep final outline decisions and final edits in the main thread.
4. Build an outline before editing the deck. Decide what must be in the main deck and what belongs in backup.
5. Draft or revise the slides with these constraints:
   - one idea per slide;
   - figures beat tables, and tables usually belong in backup;
   - every quantitative claim must be traceable to the paper or the source tables;
   - definitions of spill measures, dry spills, and the 12/24-hour counting method must be stated clearly when they matter for interpretation;
   - adapt the talk to the audience: job-market talks need more identification detail, short and lightning talks need ruthless cutting.
6. Prefer existing visual assets from the paper and analysis pipeline:
   - `figures/`, `maps/`, and `tables/` in the linked Overleaf repo;
   - `output/figures/` and related analysis outputs in this repo when the slide needs a newer or cleaner figure.
7. Compile with the engine implied by the existing deck or the last successful log. Do not switch engines casually just to make a draft compile.
8. After drafting, run a critic pass. For major changes, use `$slide-excellence` or apply the same review dimensions manually.
9. Fix critical issues first: compilation failures, unsupported claims, misleading labels, missing assets, or severe slide-density problems.

## Project Anchors

Use the paper's actual contribution and design rather than generic seminar boilerplate. The recurring building blocks in this project include:
- sewage spills and housing outcomes in England;
- EDM spill records matched to property prices and rents;
- hedonic specifications, repeat sales, long differences, upstream/downstream comparisons, and media-attention variation;
- regulatory and institutional context around water companies and storm overflows.

Do not invent coefficients, sample sizes, policy claims, or robustness results. Pull them from the manuscript or source tables before placing them on slides.

## Output Standard

The finished deck should:
- meet the format's slide budget;
- have a clear narrative arc from motivation to takeaway;
- surface only claims that can be defended from the paper;
- leave detailed tables, extra specifications, and discussant bait for backup slides.
