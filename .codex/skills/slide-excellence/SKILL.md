---
name: slide-excellence
description: Review research slide decks for narrative flow, visual quality, quantitative fidelity, proofreading, and domain substance before meetings, talks, or submission milestones. Use when Codex needs to audit Beamer or Quarto slides, especially sewage-project decks in the linked Overleaf `slides/` directory or this repo's `docs/overleaf/slides/`, and produce a prioritized review report.
---

# Slide Excellence

Run a full quality review of a slide deck and synthesize the findings into one report.

## Default Paths

Resolve the deck in this order:
1. The user-specified path.
2. `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/slides/`
3. `docs/overleaf/slides/`

For manuscript and source-of-truth checks, prefer:
- `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/_main.tex`
- `docs/overleaf/_main.tex` as fallback

Write review reports to `docs/overleaf/slides/quality_reports/` in this repo unless the user asks to save elsewhere.

## Review Dimensions

Review the deck across these dimensions:
1. Narrative and pacing
2. Visual and layout quality
3. Quantitative fidelity
4. Proofreading and consistency
5. Substance and domain correctness
6. TikZ or diagram quality, if relevant

## Workflow

1. Resolve the target deck and count main slides versus backup slides.
2. Read the deck, the linked paper sections, and the specific tables, figures, or output files that support any quantitative claims.
3. For substantial decks, prefer parallel explorer agents with disjoint scopes such as visual audit, quantitative fidelity, proofreading, and substance review. Keep the main thread responsible for synthesis and report writing.
4. Review quantitative content aggressively:
   - coefficients, standard errors, confidence intervals, p-values, Ns, and effect-size wording;
   - treatment and exposure definitions such as `spill_count`, `spill_hrs`, dry spills, and the 12/24-hour counting rule;
   - identification claims involving hedonic specifications, repeat sales, long differences, upstream/downstream comparisons, media attention, or the proposed rainfall-hydraulics IV;
   - axis labels, table labels, notes, sample statements, and causal language.
5. Review presentation quality:
   - overflow, tiny text, table legibility, inconsistent notation, or visually dense frames;
   - section transitions, pacing, and whether each slide carries one clear idea;
   - spelling, grammar, citations, and terminology consistency;
   - whether backup slides cover predictable discussant or seminar questions.
6. If the deck compiles locally, inspect the log for overflow, missing assets, or engine-specific warnings.
7. Write a combined report with severity labels and concrete fixes.

## Project-Specific Checks

- Treat the paper as authoritative. Do not bless any slide claim you cannot trace to the manuscript, source tables, or generated figures.
- Check that institutional claims about England's sewer network, EDM monitoring, permits, and water companies are accurate.
- Check that housing-market claims match the actual estimands and sample restrictions in the paper.
- Flag places where log-point approximations, omitted standard-error conventions, or compressed column mappings could mislead an audience even if the underlying numbers are correct.

## Report Shape

Use this structure:

```markdown
# Slide Excellence Review: [filename]

## Overall Quality Score: [EXCELLENT / GOOD / NEEDS WORK / POOR]

| Dimension | Critical | Medium | Low |
|-----------|----------|--------|-----|
| Narrative/Pacing | | | |
| Visual/Layout | | | |
| Quantitative Fidelity | | | |
| Proofreading | | | |
| Substance | | | |

## Critical Issues

## Medium Issues

## Low-Priority Improvements

## Recommended Next Steps
```

Order findings by severity. Cite the relevant slide or file path when possible. Prefer actionable fixes over generic advice.

## Quality Rubric

| Score | Critical | Medium | Meaning |
|-------|----------|--------|---------|
| Excellent | 0-2 | 0-5 | Ready to present |
| Good | 3-5 | 6-15 | Minor refinements |
| Needs Work | 6-10 | 16-30 | Significant revision |
| Poor | 11+ | 31+ | Major restructuring |
