---
name: ankane-readme-writer
description: Create or rewrite README files for Ruby gems in Andrew Kane's README style. Use when drafting a gem README, tightening existing gem docs, or reordering sections into the standard installation, quick start, usage, contributing, and license flow.
---

# Ankane README Writer

## Goal

Write concise README files for Ruby gems in the style Andrew Kane uses across his projects.

## Workflow

1. Identify the gem name, one-line tagline, installation command, and fastest working example.
2. Build sections in this order: header, installation, quick start, usage, options if needed, upgrading if needed, contributing, license.
3. Put the shortest runnable path in Quick Start with almost no prose between code blocks.
4. Expand Usage with one basic example and one advanced example only if the gem needs both.
5. Keep prose imperative and short. Cut filler before adding more explanation.
6. Mark any missing badge URLs, usernames, or placeholder values clearly instead of inventing them.

## Required Structure

- Header:
  Title, one-sentence tagline, and up to four badges.
- Installation:
  Show the Gemfile line first. Add any setup command only if required.
- Quick Start:
  Show the fastest working setup. Avoid long narrative text.
- Usage:
  Keep examples single-purpose. Split different concepts into separate code fences.
- Options:
  Add only when configuration deserves its own section.
- Upgrading:
  Add only for versioned migration guidance.
- Contributing and License:
  Keep both short and standard.

## Writing Rules

- Use imperative voice.
- Keep most sentences under 15 words.
- Prefer one code fence per idea.
- Let code carry the explanation where possible.
- Keep inline comments lowercase and brief.
- Remove HTML comments before finishing.

## Final Check

- Section order matches the standard template.
- The README starts with the clearest happy path.
- Examples are runnable and consistent with the gem API.
- Placeholder values are obvious and easy to replace.
- The document is shorter after editing, not longer by default.
