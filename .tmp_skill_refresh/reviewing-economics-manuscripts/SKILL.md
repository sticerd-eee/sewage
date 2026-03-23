---
name: reviewing-economics-manuscripts
description: Produce Refine-like parallel referee feedback for economics and adjacent technical manuscripts from PDF, Markdown, or LaTeX sources. Use when Codex needs a full-coverage manuscript review with parallel specialists, chunk-by-chunk coverage, verifier-backed ranking, and a saved Markdown report plus JSON findings sidecar grounded in exact source locations.
---

# Reviewing Economics Manuscripts

Run a review-only, parallel manuscript audit that saves a Markdown report and a JSON findings sidecar beside the manuscript.

This skill is parallel-only. If the current environment lacks subagent support or the current instructions disallow subagents, stop rather than imitating a Refine-like review in one thread.

## Execution Safety

- Resolve every `scripts/` and `references/` path from the skill directory, not from the current repository working directory.
- Skill root: `/Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/`
- Script entrypoints:
  - `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/build_manifest.py <target> [--output <manifest.json>] [--pretty]`
  - `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/merge_findings.py <findings-a.json> <findings-b.json> ... --output <merged-findings.json>`
  - `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/render_report.py <manifest.json> <merged-findings.json> [--output <report.md>] [--synthesis <synthesis.json>]`

## Quick Start

1. Resolve the target manuscript.
2. Build a manifest with `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/build_manifest.py <target>`.
3. Read `review_packets` and `coverage_status` from the manifest. Treat them as the run contract.
4. Run the global specialist passes from [references/subagent_roles.md](references/subagent_roles.md).
5. Run one `ChunkReviewer` pass for every chunk listed in `coverage_units`. Do not skip chunks because they look low-risk.
6. Run `FindingVerifier` on every `High` or `Critical` candidate finding and on any finding with conflicting or thin evidence.
7. Merge the worker outputs with `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/merge_findings.py <findings-a.json> <findings-b.json> ... --output <merged-findings.json>`.
8. Render the final report with `python3 /Users/jacopoolivieri/.codex/skills/reviewing-economics-manuscripts/scripts/review_manuscript/render_report.py <manifest.json> <merged-findings.json> [--output <report.md>] [--synthesis <synthesis.json>]`.

Default manuscript roots:
- the user-specified path
- `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/_main.tex`

Default output path:
- `<paper-repo>/review_reports/` for the linked Overleaf paper repo
- otherwise `<source-dir>/review_reports/`

## Workflow

### 1. Resolve and normalize sources

Accept:
- a single `.pdf`, `.md`, `.markdown`, `.tex`, or `.latex` file
- a manuscript directory

Directory resolution rules:
- if the user passes a file path explicitly, honor it
- otherwise prefer a top-level manuscript PDF when one is clearly present
- otherwise fall back to `_main.tex`, `main.tex`, `paper.tex`, or `manuscript.tex`

Build the document manifest first. Treat the manifest as the shared source of truth for every later step.

The manifest builder:
- resolves LaTeX include graphs
- records missing and circular includes
- extracts section structure, labels, refs, citations, equations, theorem-like environments, figures, tables, and diagnostics when possible
- chunks the document for exhaustive local review
- emits `coverage_units`, `coverage_status`, and deterministic `review_packets`
- records lossy extraction notes for PDF inputs

### 2. Require full parallel mode

The target behavior is a Refine-like full review, not a single generic prompt and not a hotspot-only audit.

If subagent support is available in the current environment and allowed by the current instructions:
- spawn the specialist reviewers listed in [references/subagent_roles.md](references/subagent_roles.md)
- use the default `quality-mixed` role map in [references/agent_execution_profiles.md](references/agent_execution_profiles.md) unless the user explicitly asks for a different speed, cost, or model tradeoff
- give each reviewer only the manifest slices, chunks, and inventories relevant to its role
- keep the role map fixed for the full run
- require each reviewer to emit only structured findings that follow [references/finding_schema.md](references/finding_schema.md)

If subagent support is unavailable or disallowed:
- stop
- explain that full Refine-like mode requires parallel workers in this environment
- do not silently downgrade to a single-agent imitation

### 3. Enforce coverage discipline

Coverage is mandatory:
- every chunk in `coverage_units` must receive one `ChunkReviewer` pass
- the final synthesis must report completed chunk coverage
- do not claim exhaustive review if any chunk is missing

Verification is mandatory:
- verify every `High` or `Critical` candidate finding
- verify any finding with contradictory evidence, weak quotation support, or model disagreement
- allow the verifier to confirm, narrow, or reject candidate findings

### 4. Merge, rank, and render

Collect all worker outputs as JSON files and merge them with the merge script.

The merger must:
- validate schema v2 findings
- assign stable finding IDs
- deduplicate overlapping findings deterministically
- remove verifier-rejected findings from the final ranked set
- rank retained findings by verification state, severity, claim-threat score, confidence, and evidence strength
- derive `3-6` high-level concerns from the retained finding set

Render the final Markdown report from:
- the manifest
- the merged findings file
- a synthesis JSON artifact when available

The synthesis artifact should include:
- `overall_feedback`
- `outline`
- `high_level_concerns`
- `coverage_summary`
- `worker_count`
- optional `central_claim`

## Reference Map

Read only what the current step needs:

- [references/review_rubric.md](references/review_rubric.md) for review priorities and severity, confidence, and verification rules
- [references/subagent_roles.md](references/subagent_roles.md) for reviewer decomposition and chunk assignment
- [references/agent_execution_profiles.md](references/agent_execution_profiles.md) for default model and `reasoning_effort` choices
- [references/finding_schema.md](references/finding_schema.md) for worker JSON output requirements
- [references/report_template.md](references/report_template.md) for final Markdown report structure

## Review Standard

Follow the economics-first rubric in [references/review_rubric.md](references/review_rubric.md).

Prioritize:
- mathematical and logical correctness
- notation and definition consistency
- empirical consistency across tables, figures, and specifications
- identification language and causal overreach
- internal references and buildability
- numbers, signs, units, and denominator consistency
- exposition clarity and central-claim stability

Do not spend time on:
- bibliography formatting
- external fact-checking
- drafting new content
- editing manuscript source files

## Output Contract

Always emit:
- a Markdown report
- a JSON findings sidecar

The final report must contain these sections in order:
- `Overall Feedback`
- `Outline`
- `High-Level Concerns`
- `Detailed Feedback By Relevance`
- `Detailed Feedback By Position`
- `Coverage Ledger`
- `Source Resolution Notes`
- `Coverage Gaps`

Every retained detailed finding must include:
- stable finding ID
- category and subcategory
- severity
- status
- verification state
- confidence
- claim-threat score
- chunk ID
- source path and line span
- short quoted evidence
- concise explanation
- why it matters
- suggested action

## Error Handling

Record partial coverage rather than guessing.

Treat these as explicit notes in the final report:
- missing includes
- circular includes
- unresolved labels or citations
- unsupported macros or lossy extraction
- external tables, figures, or generated artifacts that could not be verified
- chunks that were planned but not actually reviewed
