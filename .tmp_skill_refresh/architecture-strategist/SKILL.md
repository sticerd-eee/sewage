---
name: architecture-strategist
description: Review code changes for architectural fit and design integrity. Use after structural refactors, service additions, boundary changes, or PRs where layering, dependencies, contracts, or patterns may drift.
---

# Architecture Strategist

## Goal

Review changes against the codebase's actual architecture and call out design risks before they harden into maintenance debt.

## Workflow

1. Read the local architecture signals first: README files, docs, entrypoints, and surrounding modules.
2. Map the changed components, their dependencies, and the direction of those dependencies.
3. Compare the change against existing boundaries, naming patterns, lifecycle ownership, and public interfaces.
4. Separate concrete regressions from optional improvements. Do not blur the two.
5. Prefer small, actionable recommendations over abstract design commentary.

## Check These Areas

- Boundaries:
  Confirm modules and services own coherent responsibilities.
- Dependency direction:
  Look for circular references, inward leaks, or new cross-layer reach-through.
- Interfaces:
  Check whether public APIs, events, schemas, or contracts changed safely.
- Abstraction level:
  Catch logic that sits at the wrong layer or mixes policy with plumbing.
- Change shape:
  Prefer consistency with established local patterns unless the change clearly improves them.
- Long-term cost:
  Note scalability, operability, or maintenance risks introduced by the design.

## Output

- Lead with findings ordered by severity and include file references when available.
- State explicitly when no architectural findings are present.
- Follow with assumptions or open questions that could change the conclusion.
- Keep any summary brief.

## Guardrails

- Base the review on the repository's real patterns, not a generic architecture ideal.
- Treat missing documentation as a signal, not automatic proof of a flaw.
- Flag significant new patterns that deserve documentation or tests.
