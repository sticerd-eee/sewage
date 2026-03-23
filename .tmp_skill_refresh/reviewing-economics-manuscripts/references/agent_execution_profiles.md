# Agent Execution Profiles

Use these settings when the environment supports choosing subagent model and reasoning effort.

## Default Profile: `quality-mixed`

Use `quality-mixed` by default. This profile prioritizes review quality over runtime and assumes a full-coverage review rather than a lightweight audit.

Global rules:
- use `gpt-5.4` with `high` reasoning for orchestration, packet routing, and synthesis
- use `gpt-5.4` with `high` as the default floor for reviewer passes
- promote substantive reasoning roles to `xhigh`
- keep the role map fixed within a run so outputs are comparable and reproducible
- do not use Codex-oriented models for this workflow unless the user explicitly requests them

## Global Reviewer Map

### CentralClaimAndPositioning

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

### ExpositionAndClarity

- model: `gpt-5.4`
- reasoning_effort: `high`

### EmpiricalConsistency

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

### IdentificationAndCausalLanguage

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

### MathematicalAndLogicalReasoning

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

### NotationAndDefinitions

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

### InternalReferencesAndBuild

- model: `gpt-5.4`
- reasoning_effort: `high`

### NumbersUnitsAndSigns

- model: `gpt-5.4`
- reasoning_effort: `high`

### CrossDocumentConsistency

- model: `gpt-5.4`
- reasoning_effort: `high`

## Local Chunk Reviewer Map

### ChunkReviewer

- default model: `gpt-5.4`
- default reasoning_effort: `high`

Promote a chunk pass to `xhigh` when the chunk or adjacent context contains:
- theorem or lemma exposition
- proof logic
- dense equation blocks
- identification arguments or fragile empirical interpretation

## Verification Reviewer Map

### FindingVerifier

- model: `gpt-5.4`
- reasoning_effort: `xhigh`

Use the verifier to confirm, narrow, or reject:
- every `High` or `Critical` candidate finding
- any finding with weak quotation support
- any finding with conflicting evidence or disagreement across workers

## Override Rules

- if the user explicitly asks for a different model family, keep the same reviewer split unless they ask for a uniform setup
- if the chosen model does not support the requested reasoning level, use the highest supported lower level and note the downgrade in synthesis
- if the user asks for a uniform setup, use `gpt-5.4` with `high` for all roles except `MathematicalAndLogicalReasoning`, `NotationAndDefinitions`, `EmpiricalConsistency`, `IdentificationAndCausalLanguage`, and `FindingVerifier`, which should remain `xhigh`
