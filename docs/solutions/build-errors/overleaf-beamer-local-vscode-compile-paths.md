---
title: Overleaf Beamer slides local VSCode compile paths
date: 2026-06-16
category: build-errors
module: LaTeX/Overleaf slide compilation
problem_type: build_error
component: tooling
symptoms:
  - "Local `latexmk` from the `slides/` folder failed with `File 'slides/tables/...' not found`."
  - "After path fixes, `latexmk` exposed missing slide assets such as `spill_avg_annual_count_2021_2023.pdf`."
  - "A manual appendix `\\sectionpage` failed with `Arithmetic overflow` during the final Beamer build."
root_cause: config_error
resolution_type: config_change
severity: medium
related_components: [documentation, overleaf, vscode, beamer]
tags: [latex, overleaf, vscode, beamer, latexmk, graphicspath, input-path, slides]
---

# Overleaf Beamer slides local VSCode compile paths

## Problem

`slides/short_pres.tex` needed to compile both in VSCode on the local laptop and
in Overleaf online. Local VSCode/LaTeX Workshop ran `latexmk` from the `slides/`
subdirectory, while some slide inputs and graphics were written as if the TeX
job started at the Overleaf project root.

That mismatch made a valid Overleaf project fail locally, and each fix exposed
the next real build blocker until the deck compiled end to end.

## Symptoms

- Local `latexmk` produced `File 'slides/tables/hedonic_count_continuous_prior_250m.tex' not found`.
- The same deck expected root-level assets such as `figures/...`, `maps/...`, and `slides/tables/...`.
- After the table path was fixed, the build reached a missing figure: `spill_avg_annual_count_2021_2023.pdf`.
- After assets were present, Beamer stopped at the appendix divider with `Arithmetic overflow`.
- A successful verification run needed `latexmk` plus `biber`, producing `short_pres.pdf` with 32 pages.

## What Didn't Work

**Only relying on `\graphicspath{{../figures/}{../maps/}}`.**
That lets bare graphics names resolve from a subfolder-local compile, but it
does nothing for `\input{slides/tables/...}`. TeX still looks for
`slides/tables/...` relative to `slides/`, which becomes `slides/slides/tables/...`.

**Changing individual table calls by hand.**
Fixing one `\inputslidetable` call would leave the same root/subfolder mismatch
waiting elsewhere. The deck needed a reusable path rule, not one-off rewrites.

**Treating every missing file as a path bug.**
The average-annual map names were legitimate current slide inputs, but the PDFs
also needed to exist in an Overleaf-visible asset directory. Once the generated
PDFs were present in `maps/`, LaTeX found them through the search path.

**Keeping the generated appendix `\sectionpage`.**
After path and asset issues were fixed, the starred appendix section page failed
with `Arithmetic overflow`. The section page was decorative rather than
load-bearing, so replacing it was safer than debugging theme internals.

## Solution

Mirror the pattern that already worked in the `climate traps` Overleaf deck:
make a subfolder-local TeX run search the Overleaf project root for inputs, and
include the root-level asset directories in `\graphicspath`.

In `slides/short_pres.tex`, keep the bibliography relative to `slides/`, then
add a project-root fallback for graphics and inputs:

```tex
\addbibresource{../refs.bib}

% --- Package configuration --------------------------------------------------
\graphicspath{{../}{../figures/}{../maps/}}
% Same ../ fallback for \input, so project-root paths like slides/tables/...
% resolve when compiling locally from slides/.
\makeatletter
\def\input@path{{../}}
\makeatother
```

This supports all of these forms from a local `slides/` compile:

```tex
\includegraphics{spill_count_persistence.pdf}              % ../figures/
\includegraphics{spill_avg_annual_count_2021_2023.pdf}    % ../maps/
\includegraphics{figures/upstream_downstream_diagram.pdf} % ../ root fallback
\input{slides/tables/population_exposure}                 % ../ root fallback
```

Keep generated slide assets in folders that Overleaf sees. For this deck, the
average-annual map PDFs must be synced under the Overleaf project's `maps/`
directory:

```text
maps/spill_avg_annual_count_2021_2023.pdf
maps/dry_spill_avg_annual_count_2021_2023.pdf
```

Finally, replace the fragile appendix `\sectionpage` with a plain title frame:

```tex
\appendix
\AtBeginSection[]{}
\section*{Appendix}

% Appendix title page
\begin{frame}[plain,noframenumbering]
  \centering
  \vfill
  {\usebeamerfont{title}Appendix\par}
  \vfill
\end{frame}
```

Verify from the same working directory VSCode/LaTeX Workshop uses:

```bash
cd "/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters/slides"
latexmk -g -pdf -interaction=nonstopmode -halt-on-error -file-line-error short_pres.tex
```

The successful run produced `short_pres.pdf` with 32 pages and no fatal LaTeX
errors.

## Why This Works

LaTeX resolves `\input` and graphics paths relative to the TeX job's current
working directory, not relative to the Overleaf project root in the abstract.
VSCode compiled `short_pres.tex` from `slides/`, while Overleaf users often
reason about paths from the project root.

The `\graphicspath` line handles graphics lookup. Including `../` covers calls
that already include a root-level directory prefix, such as
`figures/upstream_downstream_diagram.pdf`; including `../figures/` and
`../maps/` covers bare filenames stored in those asset folders.

The `\input@path` fallback handles root-style `\input` calls. When TeX cannot
find `slides/tables/population_exposure` from the local `slides/` directory, it
also tries `../slides/tables/population_exposure`, which is the correct path
from the local working directory and the same project-root path Overleaf users
expect.

The appendix replacement removes a nonessential Beamer theme calculation from
the build. It preserves the visible divider slide without depending on
`\sectionpage` behavior for a starred appendix section.

## Prevention

- For Overleaf decks with TeX files inside subdirectories, add both graphics and
  input root fallbacks in the preamble before accumulating root-style paths.
- Keep root-style paths consistent: `slides/tables/...`, `figures/...`, and
  `maps/...` should all point to directories that exist in the Overleaf project.
- When a missing file appears after a path fix, check whether the asset exists
  before changing TeX paths again.
- Use `latexmk -g` after a failed run so stale `.fdb_latexmk` state does not
  report the old error as current.
- Prefer simple plain frames for appendix dividers if `\sectionpage` produces
  theme-level arithmetic errors.

## Related Issues

- `docs/solutions/design-patterns/fit-wide-latex-regression-tables.md` covers a
  different Beamer/LaTeX concern: fitting wide regression tables on slides. It
  is related to slide robustness but does not cover compile path portability.
- `docs/solutions/design-patterns/output-specific-research-figure-sizing-and-verification.md`
  covers the separate figure-sizing problem exposed by these same map assets:
  paper and Beamer slide PDFs need output-specific dimensions, margins, and
  legend sizes, especially when maps are used as side-by-side half-slide panels.
- `docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md`
  covers output naming discipline for generated LaTeX artifacts, which helps
  prevent stale or clobbered slide inputs.
- GitHub issue search was not run for this local Overleaf/VSCode build fix; no
  issue reference was involved in the solved problem.
