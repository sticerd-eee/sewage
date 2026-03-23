---
name: econ-table-figure-notes
description: "Write or revise captions, notes, and `Source:` lines for economics tables and figures while preserving this repository's preferred house style. Use when Codex needs to draft, tighten, or review manuscript-quality notes for regression tables, descriptive tables, maps, heatmaps, or figures in this repository."
---

# Economics Table And Figure Notes

Use this skill to write table notes and figure notes that are self-contained, technically precise, and aligned with the note style already used in this repository.

## Use the bundled references

1. Read `references/repo_examples.md` first.
2. Read `references/econ_guidance.md` second.
3. Read the target script, `.tex`, `.qmd`, `.Rmd`, or generated output file.

Treat the repository examples as the primary style anchor. Use the external guidance to sharpen conventions, catch omissions, and resolve ambiguous cases.

## Core Philosophy

- Make the object readable without forcing the reader to search the main text for basic mechanics.
- Use the caption for the `what`, the note for the `how`, and the `Source:` line for the `where from`.
- Match the level of detail in this repository's existing notes. Do not collapse to thin boilerplate.
- Keep notes descriptive rather than argumentative. Explain construction and interpretation mechanics, not the paper's sales pitch.
- State non-obvious transformations explicitly: logs, winsorisation, trimming, conditioning, weighting, aggregation, pooling, bins, quantiles, spatial radii, or timing windows.
- Preserve manuscript conventions already in use: spelling, punctuation, panel references, and LaTeX formatting.

## Workflow

1. Classify the object.
   Use one of two branches:
   - regression table;
   - any other figure or table.

2. Recover the factual ingredients from source files.
   Extract:
   - unit of observation;
   - geography and period;
   - sample restrictions;
   - panel or column mapping;
   - dependent variable, plotted quantity, or summary statistic;
   - key regressor, treatment, or exposure construction;
   - transformations, trimming, winsorisation, weighting, conditioning, or pooling;
   - inference convention, if relevant;
   - provenance, if a `Source:` line is needed.

3. Draft caption, note, and source line separately.
   - Caption: keep short; name the object and subject.
   - `Notes:`: explain how to read the object.
   - `Source:`: add provenance only, after the note.

4. Remove duplication.
   Do not repeat the caption sentence verbatim in the note. Use the note to add scope, construction, and reading instructions.

5. Run the stand-alone test.
   Check whether a referee could answer the following from the object plus note alone:
   - What is being shown or estimated?
   - For whom, where, and when?
   - How were the key quantities constructed?
   - What differs across panels or columns?
   - How should uncertainty or significance be read, if relevant?

## Branch 1: Regression Tables

Default structure:

1. Open with `This table presents ...`
2. State the sample, geography, period, and any important spatial or institutional restriction.
3. Define the dependent variable, including how it varies across columns or panels.
4. Define the main regressor, treatment, or exposure variable in plain language, including timing and radius if relevant.
5. Mention controls, fixed effects, weights, aggregation, or panel differences if they matter for reading coefficients.
6. State the standard-error or inference convention.
7. Add significance-star boilerplate if stars are used in the table.

Include extra explanation when needed:

- Explain sample changes across columns or outcomes.
- Explain conditional, cumulative, weighted, binned, or transformed variables directly.
- Explain differences across panels when the construction changes.

Resolve the journal-style tension as follows:

- Preserve the repository's current house style by default, including significance-star boilerplate where stars are shown.
- If the user explicitly asks for strict AEA-style output, remove stars and the star legend instead of carrying them over automatically.

## Branch 2: Any Other Figure Or Table

Default structure:

1. Open with `This figure displays ...`, `This figure shows ...`, or `This table presents ...`
2. State the unit, geography, and period.
3. Explain panel mapping only when it materially helps the reader.
4. Define the statistic, aggregation level, or visual encoding.
5. Explain non-obvious transformations or sample restrictions.
6. Add a `Source:` line if provenance would otherwise be unclear.

Common cases:

- Descriptive table: define ambiguous statistics and distinguish conditional from unconditional measures.
- Map: define the geographic unit, shading measure, zero category, and any winsorisation or top-coding.
- Transition matrix or heatmap: define states, transitions, conditioning, pooled sample, and cell interpretation.
- Regression-based figure: state whether the plot shows fitted values, binscatter means, residualised relationships, coefficients, or event-study estimates; explain trimming, bandwidth, or subgroup construction when relevant.

## Caption, Note, And Source Split

Use this split unless the user specifies a different house rule:

- Caption: concise title or noun phrase.
- `Notes:`: self-contained explanation of sample, construction, transformations, and interpretation mechanics.
- `Source:`: provenance only.

Use a `Source:` line when the figure or table reproduces material, relies on external data whose provenance should be surfaced, or would otherwise leave the reader unsure where the object comes from. For objects generated directly from the paper's own analysis pipeline, a source line is optional unless the user asks for one.

## Editing Rules

- Do not invent facts not supported by the source files.
- Do not change variable labels, citations, or code unless asked.
- Do not add interpretive claims such as `this shows strong persistence` unless the note already needs a short orientation sentence to explain how to read the object.
- Avoid vague filler such as `for illustrative purposes`, `it can be seen that`, or `various controls`.
- Prefer one compact paragraph over bullets unless the manuscript already uses bullet-style notes.

## Quick Templates

Regression table:

`Notes: This table presents ... The sample includes ... The dependent variable is ... [by columns or panels]. [Main regressor] is measured as ... [Controls / FE / weights / aggregation if needed]. [SE or clustering convention]. [Stars if used].`

Other figure or table:

`Notes: This figure/table shows/presents ... for ... Panel ... [if needed]. [Statistic / aggregation / visual encoding]. [Transformations / trimming / winsorisation / conditioning / pooling].`

Source line:

`Source: Authors' calculations using ...`
