---
name: humaniser
description: Remove AI-sounding or overly formulaic patterns from academic and research prose while preserving meaning, citations, and document structure. Use when Codex needs to humanise text in `.qmd`, `.Rmd`, `.md`, `.txt`, or prose sections of `.tex` files; when the user explicitly asks to humanise, de-AI, or make text sound less machine-written; or when prose in this repository or the linked Overleaf paper repo reads generic, over-signposted, triplet-heavy, or stylistically flat.
---

# Humaniser

## Overview

Revise prose so it sounds like a careful human draft rather than a generic model output. Edit conservatively: preserve claims, evidence, citations, notation, and document scaffolding while improving cadence, specificity, and stylistic variation.

## Scope

Apply this skill to prose only.

Default target files in this repo:
- `.qmd`, `.Rmd`, `.md`, `.txt`
- prose passages inside `.tex`
- prose comments or narrative text embedded in analysis files when the user clearly wants wording changes rather than code changes

Do not rewrite:
- code, equations, or chunk options
- YAML frontmatter, Quarto metadata, labels, or cross-references
- citations, bibliography keys, footnotes, or inline code unless they are plainly broken
- generated files in `data/`, `output/`, or `book/_site/` unless the user explicitly asks

When the user refers to manuscript prose, check whether the target file actually lives in the linked Overleaf repository at `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters` rather than this analysis repo.

## Workflow

### 1. Read the whole target before editing

Assess whether the writing is actually a problem. If only a few mild tells appear, make a light pass rather than forcing variation.

### 2. Identify machine-like patterns

Look for patterns such as:
- Structural tics: repetitive triplets, numbered reasoning in prose, uniform paragraph lengths, formulaic conclusion paragraphs
- Lexical tells: stock verbs and metaphors such as "underscores", "navigate", "landscape", "multifaceted", or inflated intensifiers
- Rhetorical tells: empty hedges, performative excitement, reflexive false balance, hollow "important questions" sentences
- Formatting tells: over-signposting, section previews, colon-led lists, definitional openings that add no value

Treat these as heuristics, not automatic errors. Academic prose can legitimately be formal.

### 3. Rewrite conservatively

Prefer edits that:
- replace generic stock phrasing with direct wording
- vary sentence length and paragraph rhythm
- cut throat-clearing and redundant transitions
- remove summaries that merely repeat what surrounding text already says
- keep domain vocabulary, citations, and technical precision intact

Aim for prose that sounds specific, slightly asymmetric, and authored. Do not inject chatty voice, journalistic flair, or unnecessary informality.

### 4. Preserve document mechanics

In `.tex`, `.qmd`, and `.Rmd` files:
- keep citation commands, labels, and cross-references untouched
- keep fenced code blocks and R chunks byte-stable unless the user asked for code edits
- avoid changing heading structure unless repetition or signposting is itself the problem

### 5. Summarise the pass

After editing, report the main patterns fixed and note any places you intentionally left alone because the formal structure was appropriate.

## Editing Standard

Use a light touch by default.

Good edits usually do one or more of the following:
- collapse a bloated setup sentence into a direct claim
- replace abstract nouns with the concrete object or action
- break an overly symmetrical sentence pattern
- delete one transition rather than substituting another
- keep one well-placed signpost if it genuinely helps navigation

Avoid:
- changing substantive meaning
- inventing evidence or stronger claims
- removing necessary qualification in empirical writing
- flattening all personality into sterile "good style"
- over-correcting every list, transition, or hedge just because it appears once

## Typical Triggers

Use this skill for requests like:
- "humanise this section"
- "make this sound less AI-generated"
- "de-slop this Quarto draft"
- "tighten this literature review without losing the citations"
- "rewrite this paragraph so it sounds like my own draft"
