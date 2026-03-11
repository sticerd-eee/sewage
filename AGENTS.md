# Repository Guidelines

## Project Structure & Module Organization
This repository is a research pipeline centered on sewage-spill and housing analyses.

- `scripts/R/01_*` to `scripts/R/06_*`: core data pipeline (ingestion -> final analysis datasets).
- `scripts/R/09_analysis/`: regression, descriptive, repeat-sales, and news analyses.
- `scripts/R/utils/`: shared helpers (for example postcode and spill aggregation utilities).
- `scripts/R/testing/`: exploratory and validation notebooks (`.Rmd`/`.qmd`).
- `book/`: Quarto book source; rendered site goes to `book/_site/`.
- `data/raw`, `data/processed`, `data/final`: immutable inputs, intermediate files, and analysis-ready outputs.
- `output/`: generated tables, figures, logs, and model artifacts.
- `scripts/stata/` and `raw_to_final/`: Stata conversion/finalization workflows.

## Related Project Paths
- Paper LaTeX repository: `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters`
- Use that Overleaf-synced repository for manuscript `.tex`, bibliography, and paper-figure/table tasks; do not assume the LaTeX paper source lives in this analysis repository.

## Build, Test, and Development Commands
- `R -q -e "renv::restore()"`: install/lock R dependencies from `renv.lock`.
- `Rscript scripts/R/02_data_cleaning/clean_lr_house_price_data.R`: run a single pipeline step.
- `bash scripts/R/09_analysis/run_all_analysis.sh --dry-run`: preview analysis run order.
- `bash scripts/R/09_analysis/run_all_analysis.sh`: execute analysis scripts in configured order.
- `quarto render book`: build the project website/book locally.
- `uv venv .venv && source .venv/bin/activate && uv pip install -r scripts/python/requirements.txt`: create a local Python environment for `scripts/python/`.

## Coding Style & Naming Conventions
- Prefer `snake_case` for script and object names (`aggregate_spill_stats.R`, `qtr_id`).
- Keep numeric pipeline prefixes for ordered R folders (`01_data_ingestion`, `02_data_cleaning`, etc.).
- In R, follow tidyverse style: pipes, explicit package calls where useful, and readable function blocks.
- Use project-rooted paths via `here::here(...)`; avoid hard-coded absolute paths.

## Testing Guidelines
- No formal CI test suite is currently enforced; validation is script/notebook-driven.
- Add focused checks in `scripts/R/testing/` using `test_*.Rmd` naming.
- Run targeted checks with `Rscript -e "rmarkdown::render('scripts/R/testing/test_aggregate_spill_stats.Rmd')"` and `quarto render scripts/R/testing/sales_repeat_purchases.qmd`.
- Validate outputs by confirming regenerated files in `output/` and expected logs in `output/log/`.

## Commit & Pull Request Guidelines
- Follow existing history style: concise, imperative summaries (for example `fix: ...`, `refactor: ...`, `Added ...`).
- Keep commits scoped to one logical pipeline or analysis change.
- PRs should include what changed and why, impacted scripts/paths, rerun commands, output evidence (table/figure paths and screenshots for Quarto/book changes), and a linked issue/task when available.

## Skills
Repository-local Codex skills for this project live under `.codex/skills/`.

### Available skills
- `humaniser`: Remove AI-sounding or overly formulaic patterns from academic and research prose while preserving meaning, citations, and document structure. Use for `.qmd`, `.Rmd`, `.md`, `.txt`, and prose sections of `.tex` files when text reads generic, over-signposted, or obviously LLM-written. (file: `/Users/jacopoolivieri/Library/CloudStorage/Dropbox/01_projects/sewage/.codex/skills/humaniser/SKILL.md`)

### How to use skills
- Trigger this skill when the user names `$humaniser` or asks to humanise, de-AI, de-slop, or make prose sound less machine-written.
- Prefer this repo-local skill over similarly named skills elsewhere.
- Apply it only to prose. Do not rewrite code, equations, chunk options, YAML frontmatter, citations, labels, or generated outputs unless the user explicitly asks.
