---
name: bug-reproduction-validator
description: Reproduce and validate bug reports before fixing them. Use when investigating reported behavior, deciding whether an issue is a real bug, collecting evidence, or narrowing down environment-specific or data-specific failures.
---

# Bug Reproduction Validator

## Goal

Determine whether a reported issue is real, reproducible, and worth fixing before implementation starts.

## Workflow

1. Extract the reported steps, expected behavior, actual behavior, and environment details.
2. Read the relevant code, tests, and docs before guessing what should happen.
3. Build the smallest safe reproduction you can.
4. Run the repro more than once and vary the inputs around the failure boundary.
5. Collect evidence: logs, failing output, screenshots, test results, or data state.
6. Classify the result before proposing a fix.

## Classification

- Confirmed bug:
  The behavior reproduces and clearly diverges from intended behavior.
- Cannot reproduce:
  The issue does not occur with the available steps or artifacts.
- Not a bug:
  The system behaves as designed.
- Environmental issue:
  The failure depends on configuration, deployment, browser, or service state.
- Data issue:
  The failure depends on a particular record, fixture, or corrupt state.
- User error:
  The report reflects incorrect usage or a misunderstanding.

## Evidence Standards

- Show the exact steps you ran.
- Prefer the smallest artifact that proves the result.
- Distinguish observed facts from hypotheses.
- Note what additional evidence would settle remaining uncertainty.
- Use browser tooling only when the bug is visual or interaction-driven.

## Output

- Reproduction status
- Steps taken
- Findings
- Root cause or leading hypothesis
- Evidence
- Recommended next step

## Guardrails

- Do not mutate production systems to reproduce a bug.
- Use temporary instrumentation only when necessary and remove it before handoff.
- Treat flaky behavior as evidence. Record the failure rate if you can measure it.
