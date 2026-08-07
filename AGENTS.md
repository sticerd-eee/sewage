# Repository Guidelines

This is a research pipeline for an economics project investigating the effects of sewage spills on house prices.

## Project Structure
- `scripts/R/01_*` to `scripts/R/06_*`: ordered data pipeline.
- `scripts/R/09_analysis/`: main analysis scripts.
- `scripts/R/utils/`: shared helpers.
- `data/raw`: immutable inputs; do not overwrite.
- `docs/solutions/`: documented solutions to past problems (bugs, best practices,
  conventions), organized by category with YAML frontmatter (`module`, `tags`,
  `problem_type`) — relevant when implementing or debugging in documented areas.
- `CONCEPTS.md`: shared domain vocabulary (entities, named processes, status concepts).
- LaTeX manuscript lives outside this repo and syncs to Overleaf via Dropbox
  (`Dropbox/Apps/Overleaf/Sewage in Our Waters`). The absolute path is
  machine-specific: set it in your personal `CLAUDE.local.md` (not tracked).

## Conventions
- Use `here::here(...)` for project-rooted paths.
- Prefer tidyverse-style R and `snake_case`.
- Preserve numeric pipeline prefixes.

## Git Workflow
- Use task-specific branches for nontrivial changes.
- Branch names should describe the task, prefixed by initials.
  Example: `jo/spill-site-house-2km-aggregation`.

## Environment
- R package management uses `rv`.
- Use R 4.6.0.