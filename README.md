# Sewage in Our Waters

## Overview

This repository contains the data pipeline, analysis code, and supporting documentation for an economics research project on the effect of sewage spills on house prices and rents in England.

## Project Structure

The project is organised as follows:

```text
├── book/ # Quarto book source for the project website and data-cleaning notes
│
├── data/ # All project data
│   ├── final/ # Analysis-ready datasets
│   ├── processed/ # Intermediate processed data files
│   ├── raw/ # Original data inputs
│   └── temp/ # Temporary processing files
│
├── docs/ # Project documentation, plans, reports, and manuscript-side assets
│
├── output/ # Generated tables, figures, maps, logs, and model artefacts
│
├── scripts/
│   ├── config/ # Configuration files
│   ├── python/ # Optional Python utilities, managed with uv
│   ├── R/
│   │   ├── 01_data_ingestion/ # Raw data collection scripts
│   │   ├── 02_data_cleaning/ # Data cleaning and standardisation scripts
│   │   ├── 03_data_enrichment/ # Data aggregation and enrichment scripts
│   │   ├── 04_feature_engineering/ # Spatial analysis and feature engineering scripts
│   │   ├── 05_data_integration/ # Data integration and merging scripts
│   │   ├── 06_analysis_datasets/ # Final dataset assembly scripts
│   │   ├── 09_analysis/ # Descriptive, regression, and auxiliary analysis scripts
│   │   ├── testing/ # Validation notebooks and targeted checks
│   │   └── utils/ # Shared helpers
│   └── stata/ # Stata scripts
│
├── sources/ # Reference materials and data documentation
│
├── AGENTS.md # Repository guidance
├── README.md # Project README
├── rv.lock # R dependency lockfile
└── sewage.Rproj # RStudio project file
```

## Data

The only restricted datasets currently used in this project are `data/raw/zoopla/` and `data/raw/lexis_nexis/`. All other active project data inputs are public and should be treated as such.

### Dataset List

| Data Directory | Source | Access | Notes | Citation |
| --- | --- | --- | --- | --- |
| `data/raw/edm_data/` | UK Government: Environment Agency | Public | Historical company EDM files (2021-2024) plus live API snapshots for the nine England companies in the National Storm Overflows Hub feed (2024+). | [Environment Agency EDM](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac) |
| `data/raw/ea_consents/` | UK Government: Environment Agency | Public | Site locations, permit details, and discharge consent information under the Environmental Permit Regulations. | [EA Consents Data](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109) |
| `data/raw/haduk_rainfall_data/` | UK Government: Met Office | Public | Daily precipitation data used to construct rainfall indicators and identify dry spills. | [Met Office HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) |
| `data/raw/lr_house_price/` | UK Government: HM Land Registry | Public | Property transaction records for England and Wales used for the sales-side analysis. | [Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) |
| `data/raw/zoopla/` | WhenFresh / CDRC / Zoopla | Restricted | Safeguarded rental listings used for the rental-side analysis. | Internal safeguarded access |
| `data/raw/lexis_nexis/` | LexisNexis | Restricted | News coverage exports used in the information and media-attention analysis. | Subscription access |

#### Event Duration Monitoring

- **Data Files:** `data/raw/edm_data/`
- **Source:** [UK Government: Environment Agency](https://environment.data.gov.uk/dataset/21e15f12-0df8-4bfc-b763-45226c16a8ac)
- **Format:** XLSX, XLSB, CSV (historical), JSON (API)
- **Coverage:** 2021-2024+ individual sewage overflow events
- **Notes:** Historical data (2021-2024) comes from annual EDM archives containing company files. Live API snapshots (2024+) come from the England-only National Storm Overflows Hub feed for nine companies and are stored under `data/raw/edm_data/raw_api_responses/`. Welsh Water's separate public map is not part of this live API pipeline.

#### Consented Discharges to Controlled Waters with Conditions

- **Data Files:** `data/raw/ea_consents/`
- **Source:** [UK Government: Environment Agency](https://www.data.gov.uk/dataset/55b8eaa8-60df-48a8-929a-060891b7a109/consented-discharges-to-controlled-waters-with-conditions)
- **Format:** ACCDB (exported to CSV)
- **Notes:** Provides permit requirements, consent-holder information, effluent types, and location data in OS National Grid Reference format.

#### HadUK-Grid Rainfall Data

- **Data Files:** `data/raw/haduk_rainfall_data/`
- **Source:** [UK Government: Met Office](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid)
- **Format:** NetCDF
- **Coverage:** 2020-2023 daily rainfall data
- **Notes:** Used to construct site-level rainfall indicators and dry-spill classifications.

#### Land Registry House Prices

- **Data Files:** `data/raw/lr_house_price/`
- **Source:** [UK Government: HM Land Registry](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file)
- **Format:** CSV
- **Coverage:** 2021-2024+ property transactions
- **Notes:** Property transactions are enriched using local ONS postcode data and provide the core house-sales outcome measure.

#### Zoopla Rental Data

- **Data Files:** `data/raw/zoopla/`
- **Source:** WhenFresh / CDRC / Zoopla
- **Format:** CSV
- **Coverage:** Current pipeline uses safeguarded rental data for 2021-2023
- **Notes:** This is a restricted dataset and should not be treated as publicly shareable project input.

#### LexisNexis News Data

- **Data Files:** `data/raw/lexis_nexis/`
- **Source:** LexisNexis
- **Format:** Raw search exports and derived files
- **Notes:** This is a restricted dataset used for the news and information-attention analyses.

## Computational Requirements

### Software Requirements

- **R (version 4.6.0+)**
  - Uses `rv` for R package management.
  - Install and sync the R environment with `rv sync`.
- **Python**
  - Uses `uv` for package management:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r scripts/python/requirements.txt
```

- **Quarto**
  - Build the project website locally with `quarto render book`.

## Project Workspace Setup

To setup the project on a new machine, follow the steps below.

1. **Clone the repository** into a local directory:

   ```bash
   git clone https://github.com/sticerd-eee/sewage.git
   cd sewage
   ```

   Do not clone into a folder managed by a continuous file-sync service (e.g. Dropbox, OneDrive, iCloud Drive, Google Drive), as background file syncing can interfere with Git and corrupt the repository.

2. **Get access to the project Dropbox folder.**

   If you do not have access, contact one of the project members listed at the bottom of this README.

3. **Create symbolic links to the shared data and output folders.**

   The `data/` and `output/` directories are stored in Dropbox and are not tracked by Git. From the root of the cloned repository, run:

   ```bash
   ln -s "<your Dropbox>/sewage/data" data
   ln -s "<your Dropbox>/sewage/output" output
   ```

   Replace `<your Dropbox>` with the path to your local Dropbox directory.

4. **Restore the R project environment.**

   The project uses the R package `rv` to manage its R packages. The `rv.lock` file records the package versions required to reproduce the project environment consistently across machines.

   Ensure that R 4.6.0 and `rv` are installed. Then, from the root of the repository, run:

   ```bash
   rv sync
   ```

   This installs or updates the local project library so that the R environment is identical across machines.

## Git and GitHub

The project uses Git for version control and GitHub for collaboration. The repository should contain everything needed to reproduce the empirical analysis, including the code, configuration, documentation, and software-environment definitions. Data files, generated outputs, and machine-specific artefacts should remain outside Git.

### Files Tracked in Git

Commit files that define, document, or implement the reproducible research workflow:

- **Code and configuration:** source code, reusable functions, and non-sensitive configuration files, including `scripts/R/`, `scripts/python/`, `scripts/stata/`, and `scripts/config/`
- **Documentation and reference materials:** project documentation, research notes, website source files, and repository guidance, including `book/`, `docs/`, `sources/`, `README.md`, and `AGENTS.md`
- **Environment and project files:** dependency definitions and project metadata needed to reproduce the computational environment, including `rv.lock`, `scripts/python/requirements.txt`, and `sewage.Rproj`

### Files Excluded from Git

Do not commit files that contain project data, sensitive information, generated results, or machine-specific state:

- **Data and generated outputs:** raw, intermediate, and analysis-ready data in `data/`; tables, figures, logs, and other generated files in `output/`
- **Restricted or sensitive files:** restricted datasets, credentials, API keys, access tokens, `.env` files, and any configuration files containing confidential information
- **Local and machine-generated files:** virtual environments, caches, editor settings, and operating-system metadata, including `.venv/`, `__pycache__/`, `.Rproj.user/`, `.vscode/`, and `.DS_Store`

These exclusions are defined in the repository’s tracked `.gitignore` file. Since this file is shared through Git, the same exclusion rules apply to every collaborator who clones the repository.

### Git and GitHub Workflow

The main branch should always contain the current stable version of the codebase, so development should never happen directly on it. Make each self-contained change on a dedicated branch, then merge it into main via a GitHub pull request.

1. **Create a branch for the task.**

   Start from an up-to-date copy of `main`, then create a task-specific branch:

   ```bash
   git switch main # Switch to the local main branch
   git pull --ff-only # Download and apply the latest remote changes
   git switch -c jo/clean-news-data # Create and switch to a new task-specific branch
   ```

   Name branches using the format `initials/short-task`, where `short-task` is a concise, hyphenated description of the work. For example:

   - `jo/clean-news-data`
   - `az/hydraulic-iv`

   Each branch should be a single, self-contained task. Unrelated changes should be made on separate branches.

2. **Review changes as you work.**

   Inspect the state of the repository regularly:

   ```bash
   git status # Show the current branch and any staged, unstaged, or untracked files
   git diff # Review changes that have not yet been staged
   ```

   Before committing, review the files that have been staged:

   ```bash
   git diff --staged # Review changes that will be included in the next commit
   ```

   This helps identify unintended changes.

3. **Commit changes in logical checkpoints.**

   Each commit should represent a small, coherent change that can be understood independently. Stage only the files relevant to that change:

   ```bash
   git add scripts/R/02_data_cleaning/clean_news_data.R # Stage a specific file for inclusion in the next commit
   git commit -m "Clean and standardise news data" # Commit the staged changes with a clear, succinct message
   ```

   Write concise commit messages that describe what the commit does.

4. **Push the branch to GitHub.**

   Once the task is completed and ready to share, push the branch:

   ```bash
   git push -u origin jo/clean-news-data
   ```

   After the initial push, subsequent commits can be pushed with:

   ```bash
   git push
   ```

5. **Open a pull request.**

   Open a pull request from the task branch into `main`. The pull-request description should briefly explain:

   - the purpose of the change;
   - the main files or components modified;
   - any other potentially useful information, such as remaining limitations, follow-up work, or implications for collaborators.

6. **Merge and clean up the branch.**

   Once the changes have been reviewed and are ready to incorporate, merge the pull request through GitHub and delete the remote branch. Merge using GitHub's "Create a merge commit" option (not "Squash and merge"), so the branch's commits are preserved in `main`'s history and `git branch -d` can confirm the branch is merged. Then update the local copy of `main`:

   ```bash
   git switch main # Return to the local main branch
   git pull --ff-only # Download the newly merged changes from GitHub
   git branch -d jo/clean-news-data # Delete the completed local task branch
   ```

   Begin subsequent work from this updated version of `main`.

### Dropbox and Concurrent Work

The project stores `data/` and `output/` in Dropbox because (i) some datasets are large and (ii) parts of the data-construction pipeline are computationally intensive. This allows collaborators to share processed data and generated outputs without each person having to rerun the entire pipeline locally.

Git branches isolate changes to tracked code, but not to the shared Dropbox folders. Collaborators working on different branches therefore read from and write to the same files in `data/` and `output/`, so one person may overwrite data or outputs used by another. In particular, upstream changes can affect downstream work even when the relevant code changes are on separate branches.

Before running code that writes to shared files — especially upstream data-processing scripts — coordinate with collaborators who may depend on those files. Where practical, write experimental data and outputs to temporary, branch-specific, or user-specific paths, and update the shared versions only when the changes are ready for others to use.

## Agentic Coding Tools

Agentic coding tools such as Claude Code and Codex run on the local machine, where they can read and edit any file in the repository, including gitignored files.

These tools load convention files that provide persistent, project-specific context. The universal standard, used by Codex, Gemini, and others, is `AGENTS.md`; Claude Code (helpfully) uses `CLAUDE.md` instead. Both OpenAI and Anthropic recommend keeping these files succinct, limited to essential project-specific facts that the agent cannot infer from the code itself. Because this project uses both Codex and Claude Code, the shared instructions live in `AGENTS.md` and the root `CLAUDE.md` imports it:

```md
@AGENTS.md
```

Claude Code additionally supports `CLAUDE.local.md`, a gitignored file for personal, machine-specific instructions. This is useful for context that applies only to your setup. For example, if you sync the project's Overleaf through Dropbox, you can record the local path there so the agent can update results in the manuscript directly.

## Contact

- Jacopo Olivieri, LSE, `j.olivieri@lse.ac.uk`
- Alina Zeltikova, LSE, `A.Zeltikova@lse.ac.uk`
