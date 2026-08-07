# Git, Dropbox, and Claude Code: how we work on this repo

This is a short guide to how the project is organized and what the rules are. If you follow the five rules at the end, you cannot really break anything.

## The one idea everything follows from

We keep two kinds of material in two different places, because they need different things:

- **Code and documentation live in git** (this repository, on GitHub). Git is good at tracking small text files, showing who changed what and why, and merging two people's work on the same files.
- **Data and outputs live in the shared Dropbox** (`Dropbox/01_projects/sewage/`). Dropbox is good at syncing large binary files, which git is bad at. Git history keeps every version of every file forever, so a 500 MB dataset committed once bloats the repository for everyone, permanently.

The repository connects to Dropbox through two symbolic links at its root: `data` and `output`. To your code they look like ordinary folders inside the project, so `here::here("data", ...)` works, but the files actually live in Dropbox and never touch git. The git repository itself must **not** live inside Dropbox — Dropbox syncing the `.git` folder while git is writing to it corrupts repositories. Clone it somewhere like `~/projects/`.

## What to commit and what not to commit

**Commit:**

- R and Python scripts (`scripts/`)
- Documentation, plans, meeting notes, reports (`docs/`)
- The Quarto book (`book/`)
- Project configuration and lockfiles (`rv.lock`, `uv.lock`, `pyproject.toml`, `.gitignore` itself). The lockfiles are what let another machine reproduce the exact same package versions, so changes to them should always be committed.
- Shared project instructions for Claude Code (see the Claude section below)

**Never commit:**

- Anything in `data/` or `output/`. These belong in Dropbox.
- Data files anywhere else in the tree (`.csv`, `.dta`, `.xlsx`, `.RData`, `.zip`, ...). If a script produces one, write it to `output/`.
- Rendered artefacts that a script or LaTeX can regenerate (`.pdf`, `.log`, `.aux`, caches). We commit the recipe, not the meal.
- Machine-specific or personal files: `.Rhistory`, `.Renviron`, `.env`, `.DS_Store`, editor settings, your personal Claude settings.
- Confidential or administrative documents: data-access applications, email correspondence (`.eml`), ethics forms, user agreements. These contain personal details and the repository is public — they live in the shared Dropbox (`docs/data_requests/` there) and nowhere else. Remember that .gitignore is accident prevention, not a security boundary: anything committed stays in the public git history even after deletion, so check twice before committing documents.

The rule of thumb: **commit anything a collaborator needs to reproduce your work that they cannot regenerate themselves; ignore anything that is data, generated, or personal.**

## What .gitignore actually does

`.gitignore` is just a list of filename patterns that git pretends not to see. It is our safety net for the rules above: `/data` and `/output` are listed, and so are the bulky file extensions (`*.csv`, `*.dta`, `*.xlsx`, ...), so even if a stray data file ends up somewhere in the project, `git add` will skip it silently. You rarely need to touch it. Two things are worth knowing:

- It only affects *untracked* files. If a file was already committed before a pattern was added, git keeps tracking it until someone runs `git rm --cached <file>`.
- If git refuses to add a file you genuinely want committed, check whether a pattern in `.gitignore` is catching it before forcing anything.

## How Claude Code ties in

Claude Code is a tool that edits files; it does not change any of the rules above. Treat a Claude session like a fast collaborator: it writes code into the working directory, and *you* review and commit the result under the same rules as hand-written code.

The files involved:

- **`AGENTS.md`** (repository root) — the shared project instructions: pipeline structure, conventions, git workflow. Committed, so everyone's agent gets the same rules. This filename is the cross-tool standard (Codex and others read it too).
- **`CLAUDE.md`** — a small committed file that imports `AGENTS.md`, because Claude Code loads `CLAUDE.md` specifically. You should not need to touch it.
- **`CLAUDE.local.md`** — your personal instructions for this project (machine-specific paths, personal preferences). Gitignored; Claude loads it in addition to the shared files. Create it if you need it.
- **`CONCEPTS.md`** — shared domain vocabulary. Committed.
- **`docs/solutions/`** — documented fixes to past problems that Claude consults. Committed.
- **`.claude/skills/`** — reusable project skills. Shared, committed.
- **`.claude/settings.local.json`**, `.claude/worktrees/`, session files — personal and machine-specific. Ignored.

One warning that applies to all of these: gitignoring a file only keeps it off GitHub — its contents are still sent to the model when Claude loads it. Credentials, API keys, and tokens do not belong in any instructions file, local or shared. Put them in `.Renviron` or environment variables.

Practical habits with Claude:

- Run `git status` and `git diff` before committing what a session produced. Commit only what you understand.
- Ask Claude to work on a branch (it will follow the branch convention in `AGENTS.md`).
- Claude writes outputs to `output/` like any script would; those land in Dropbox, not git, automatically.

## The five rules

1. **Pull before you start, push when you stop.** `git pull` at the start of a session, push your branch at the end. This is what prevents painful merges with two or three of us.
2. **Work on a branch named `<initials>/<task>`** (for example `jo/spill-site-aggregation`), then merge to `main` via a pull request when the task is done.
3. **Commit small and often, with messages that say why.** A commit should be one logical change. "Fix DST handling in spill duration merge" beats "updates".
4. **Never commit data or outputs.** If `git status` shows a `.csv`, `.dta`, or anything from `data/` or `output/`, stop — something is in the wrong place.
5. **Review everything before it goes in — especially Claude's work.** `git diff` is the last checkpoint before a change becomes part of the shared history.

## Setting up on a new machine

1. Install git, R 4.6.0, and `rv`. Clone the repository **outside** Dropbox:

   ```bash
   git clone git@github.com:sticerd-eee/sewage.git ~/projects/sewage
   ```

2. Make sure the shared Dropbox folder `01_projects/sewage` is synced on your machine, then create the two symlinks from the repository root (adjust the Dropbox path to where it lives on your machine):

   ```bash
   cd ~/projects/sewage
   ln -s ~/Library/CloudStorage/Dropbox/01_projects/sewage/data data
   ln -s ~/Library/CloudStorage/Dropbox/01_projects/sewage/output output
   ```

3. Restore the R environment with `rv sync` (and `uv sync` for Python helpers).

4. Open the project (`sewage.Rproj`) and check that `here::here("data")` resolves.
