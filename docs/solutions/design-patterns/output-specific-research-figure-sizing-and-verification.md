---
title: "Output-specific research figure sizing and verification"
date: 2026-06-19
last_updated: 2026-06-19
category: design-patterns
module: Research figure exports (R/ggplot2; paper + Beamer)
problem_type: design_pattern
component: documentation
severity: medium
applies_when:
  - "A figure is used in both the manuscript and Beamer slides"
  - "Labels are readable in the source PDF but too small after LaTeX inclusion"
  - "Annotations overlap plotted series, vertical reference lines, or panel margins"
  - "A map or choropleth is shown as a side-by-side half-width Beamer panel"
  - "Standalone map exports have excessive whitespace after tightening labels"
  - "Choosing between PDF, PNG, and SVG for paper or slide graphics"
related_components:
  - tooling
  - latex
  - beamer
  - overleaf
  - maps
tags: [r, ggplot2, figures, maps, latex, beamer, pdf, verification, sizing]
---

# Output-specific research figure sizing and verification

## Context

The public-attention figure started as one large PDF export that was scaled down
inside the manuscript. At the manuscript inclusion size, axis labels, event
labels, and the legend were hard to read from a distance. The same figure also
needed to work in `slides/short_pres.tex`, where `width=0.75\linewidth` left too
much white space and `width=0.95\linewidth` made the Beamer frame noticeably
overfull once the title and notes were included.

The important lesson is that readability is determined at the final LaTeX
inclusion size, not at the standalone PDF size opened in a viewer. A figure that
looks acceptable by itself can fail once it is embedded in the paper or slides.

The same issue recurred for the sewage-spill choropleth maps. The slide assets
`spill_avg_annual_count_2021_2023_london_inset_slides.pdf` and
`dry_spill_avg_annual_count_2021_2023_london_inset_slides.pdf` were initially
exported as large slide-style PDFs, then used side by side in
`slides/short_pres.tex` as `0.49\textwidth` subfigures. Beamer shrank each map
to a half-slide panel, making legend labels too small. A first pass then
overcorrected by making the legend dominate the maps. The final fix was to
export the slide maps near their half-panel display size, keep the substantive
legend titles, and tune the legend font back down after checking the rendered
Beamer frame.

The spill phase diagrams exposed the same scaling problem through a different
geometry. The heatmap panel is square because it uses fixed coordinates, but
the original paper PDF was exported on a much wider canvas. When the manuscript
included that wide PDF at `.9\linewidth`, LaTeX shrank the whole graphic,
including the axis text, legend, and cell labels. The fix was not to make the
source fonts huge. It was to make the paper export close to the actual
manuscript display width, reduce wasted horizontal canvas, and then use
moderate text sizes similar to the public-attention figure.

## Guidance

Use PDF as the canonical export for paper and Beamer figures. It keeps vector
lines and text crisp under LaTeX scaling. Use PNG only as a rendered preview or
for contexts that require raster images. Avoid SVG as the project default for
LaTeX/Overleaf outputs because font handling and conversion paths are more
fragile than direct PDF inclusion.

Create separate figure variants when the same underlying plot is used in
different output media. For `google_trends_article_counts_combined`, the paper
and slide variants share the same data and plotting function but have separate
settings for filename, dimensions, font sizes, line widths, axis titles, margins,
and annotation positions:

```r
PLOT_VARIANTS <- list(
  paper = list(
    file_name = "google_trends_article_counts_combined.pdf",
    width_cm = 14.5,
    height_cm = 9.0,
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_text_size = 8.5,
    event_text_size_pt = 8.5,
    line_width = 0.55,
    event_positions = list(
      event1 = list(label_date = "2021-08-01", label_y = 60, line_y = 60)
    )
  ),
  slides = list(
    file_name = "google_trends_article_counts_combined_slides.pdf",
    width_cm = 13.2,
    height_cm = 6.6,
    axis_title_size = 10,
    axis_text_size = 8.5,
    legend_text_size = 9,
    event_text_size_pt = 7.8,
    line_width = 0.65,
    event_positions = list(
      event1 = list(label_date = "2021-08-01", label_y = 65, line_y = 65)
    )
  )
)
```

Treat these values as output-specific starting points, not universal font rules.
The check that matters is the rendered manuscript or slide page. In the final
public-attention figure, the slide PDF is designed for a Beamer call near:

```tex
\includegraphics[width=0.9\linewidth,keepaspectratio]{google_trends_article_counts_combined_slides.pdf}
```

For the tested frame, `0.9\linewidth` was the better compromise: it fills the
slide substantially, keeps the notes visible, and produces a smaller overfull
warning than `0.95\linewidth`. If the notes, title, or theme changes, re-run the
same verification rather than carrying the width forward by habit.

For fixed-aspect plots such as phase diagrams, inspect both the object geometry
and the LaTeX call site before choosing dimensions. The effective rendered text
size is approximately:

```text
effective_text_size_pt = source_text_size_pt * displayed_width / native_pdf_width
```

That ratio explains why `spill_count_persistence.pdf` looked smaller than
`google_trends_article_counts_combined.pdf` in the manuscript even when their
source font settings were close. The phase diagram had a much wider native PDF,
so the same `.9\linewidth` inclusion applied more shrinkage. A square heatmap
inside a wide canvas is especially vulnerable: the panel may be fixed and
well-proportioned, while the surrounding canvas silently determines the scale at
which all text is embedded.

For the spill persistence phase diagrams, keep the paper export close to the
manuscript-native width and keep the slide export close to the Beamer column
where it is actually shown:

```r
PLOT_VARIANTS <- list(
  paper = list(
    width_cm = 14.5,
    height_cm = 11.5,
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_title_size = 9.5,
    legend_text_size = 8.5,
    cell_text_size_pt = 8.8,
    legend_title_position = "top"
  ),
  slides = list(
    width_cm = 7.2,
    height_cm = 6.5,
    axis_title_size = 9.5,
    axis_text_size = 8.5,
    legend_title_size = 8.5,
    legend_text_size = 7.5,
    cell_text_size_pt = 8.0,
    legend_title_position = "top"
  )
)
```

The paper dimensions produce PDFs around `411 x 325 pt`, close to the
public-attention paper figure's native width. The slide dimensions produce PDFs
around `204 x 184 pt`, matching the right-hand Beamer column use case. Because
the slide canvas is already close to final display size, slide source fonts
should not be inflated. Earlier slide settings with larger axis, legend, and
cell-label text looked too big because there was little remaining LaTeX
downscaling to absorb them.

For horizontal colorbar legends, put the title above the bar when space is
tight. This avoids left-title crowding where the title and first tick can read
as one string, while preserving a visible legend in both paper and slide
variants.

For side-by-side map panels, the slide export should usually be smaller, not
larger. In `spill_maps_inset.R`, the slide maps are not full-slide figures: they
are included as two `0.49\textwidth` subfigures on the same Beamer frame. The
working export therefore uses a half-panel-native canvas:

```r
PLOT_WIDTH <- 7
PLOT_HEIGHT <- 9.7

# Slide maps are used as side-by-side half-width Beamer panels.
# Export near that final display size so LaTeX does not shrink labels away.
SLIDE_PLOT_WIDTH <- 7.0
SLIDE_PLOT_HEIGHT <- 5.6
```

The corresponding slide legend settings keep the labels readable without making
the colorbar the visual subject of the map:

```r
if (variant == "slides") {
  return(list(
    legend_title_size = 8.8,
    legend_text_size = 8.3,
    legend_key_width = 2.0,
    legend_key_height = 0.30,
    legend_bar_width = 4.8,
    legend_bar_height = 0.20,
    legend_margin = margin(t = 0, b = 0),
    plot_margin = margin(t = 1, r = 2, b = 1, l = 2)
  ))
}
```

Do not shorten labels merely to solve a layout problem when the title carries
substantive meaning. For the map slides, the final legend titles stayed explicit:

```r
"Average annual spill count (log)"
"Average annual dry spill count (log)"
```

At the same time, fix paper whitespace separately. The non-slide map variant
needed a shorter paper canvas and tighter margins, not larger text. Its final
settings used `PLOT_HEIGHT <- 9.7`, `legend_margin = margin(t = 6, b = 0)`, and
`plot_margin = margin(t = 2, r = 8, b = 4, l = 3)`. Treat canvas dimensions,
plot margins, legend margins, and legend font sizes as separate controls; do not
try to solve all fit problems by changing only text size.

Do not solve label-line collisions with white text boxes. Move the annotation
geometry instead. Store label x positions, label y positions, and dashed-line
heights in variant-specific settings, then place labels in data space. This lets
the paper and slide versions differ when the same annotation needs different
spacing after resizing.

For event labels:

- Put each label on the side of the reference line with the most open space.
- Keep the label anchor close to its dashed line when that is the visual cue.
- Raise or lower the dashed line and text independently when nearby series
  peaks would otherwise run through the label.
- Inspect the rendered result; ggplot coordinates that look mathematically tidy
  can still be visually wrong.

Verify in three layers before copying to Overleaf:

1. Regenerate the PDFs from the source script.
2. Render each standalone PDF to PNG with `pdftoppm` and inspect the image.
3. Compile temporary LaTeX wrappers that match the real manuscript and Beamer
   inclusion sizes, render those PDFs to PNG, and inspect the embedded result.

Use temporary files outside the repo, for example under
`/private/tmp/sewage-figure-resize-check/`, and delete them after verification.
The verification target should be the actual call-site geometry, such as
`.9\linewidth` in the manuscript and the exact Beamer frame used in
`slides/short_pres.tex`.

Useful verification commands:

```bash
Rscript --vanilla scripts/R/09_analysis/01_descriptive/google_trends_article_counts_combined.R
```

```bash
pdfinfo output/figures/google_trends_article_counts_combined.pdf
```

```bash
pdfinfo output/figures/google_trends_article_counts_combined_slides.pdf
```

```bash
pdfinfo output/figures/spill_count_persistence.pdf
```

```bash
pdfinfo output/figures/spill_count_persistence_slides.pdf
```

```bash
pdftoppm -png -r 220 output/figures/google_trends_article_counts_combined_slides.pdf /private/tmp/sewage-figure-resize-check/slides-direct
```

```bash
latexmk -pdf -interaction=nonstopmode -halt-on-error slide_check.tex
```

For the side-by-side map slide, use a temporary Beamer wrapper that preserves
the actual `0.49\textwidth` subfigure geometry and the notes block. A standalone
PDF render can look good while the combined frame still clips tick labels or
pushes notes off the slide.

Keep the R script project-local. It should write to `output/figures/`; do not
hard-code a Dropbox or Overleaf path into the script. Copying final PDFs into
the Overleaf `figures/` or `maps/` directory is a release step, not part of
figure generation. When iterating on local output, leave the Dropbox/Overleaf
folder untouched until the final assets are chosen.

## Why This Matters

Figures in this project often carry substantive interpretation through labels,
event markers, legends, and small time-series movements. If the export is sized
for a standalone viewer rather than for its final LaTeX context, the reader sees
small text, wasted slide space, or labels crossing the data. Those are not
cosmetic failures: they change how quickly a seminar audience or paper reader
can understand the empirical point.

Separate paper and slide exports also prevent a false compromise. The manuscript
needs a compact figure with readable labels after inclusion in a text page.
Slides need a wider figure that uses Beamer space efficiently and remains
legible from a distance. For side-by-side map panels, "slide-specific" may mean
a smaller half-panel-native PDF rather than a wider full-slide PDF. One PDF can
rarely be optimal for both.

## When to Apply

- Any ggplot figure that appears in both the manuscript and the slide deck.
- Any map or choropleth whose paper version is included as a manuscript panel
  but whose slide version is included side by side with another map.
- Any figure with direct annotations, event labels, or vertical reference lines.
- Any figure where LaTeX scales the graphic substantially away from its exported
  size.
- Any time a figure is being tuned by eye in a standalone PDF viewer rather than
  in the rendered paper or slide context.

## Examples

The public-attention figure ended with these conventions:

| Output | File | Export size | Intended inclusion |
|--------|------|-------------|--------------------|
| Paper | `google_trends_article_counts_combined.pdf` | `14.5 x 9.0 cm` | manuscript figure at about `.9\linewidth` |
| Slides | `google_trends_article_counts_combined_slides.pdf` | `13.2 x 6.6 cm` | Beamer frame at `0.9\linewidth` |
| Paper map | `spill_avg_annual_count_2021_2023_london_inset.pdf` | `7.0 x 9.7 cm` | manuscript subfigure at `0.43\textwidth` |
| Slide map | `spill_avg_annual_count_2021_2023_london_inset_slides.pdf` | `7.0 x 5.6 cm` | Beamer subfigure at `0.49\textwidth` |
| Paper phase diagram | `spill_count_persistence.pdf` | `14.5 x 11.5 cm` | manuscript figure at `.9\linewidth` |
| Slide phase diagram | `spill_count_persistence_slides.pdf` | `7.2 x 6.5 cm` | Beamer right-hand column at `width=\linewidth` |

The slide-specific export is deliberately not just the paper PDF scaled up. It
uses its own filename, aspect ratio, labels, line widths, and event positions so
the Beamer call can be simple and stable. For map panels, the slide-specific
export is also not necessarily larger than the paper export: it should match the
panel geometry in which Beamer will display it. For fixed-aspect heatmaps, the
paper-specific export can also be smaller than a generic large plotting canvas:
matching the manuscript inclusion size protects labels without requiring
oversized source typography.

Temporary Beamer check:

```tex
\begin{frame}[c,label=app-media-figure]{Public attention to sewage spills over time}
  \centering
  \includegraphics[width=0.9\linewidth,keepaspectratio]{google_trends_article_counts_combined_slides.pdf}
  \vspace{-0.5em}
  \notes[0.9]{Monthly public attention to sewage spills in England, 2018--2024. Pink line: Google Trends search interest for ``sewage spill'' on a 0--100 scale. Teal line: UK LexisNexis article counts, scaled so the peak aligns with Google Trends.}
\end{frame}
```

Temporary Beamer check for side-by-side maps:

```tex
\begin{frame}{Sewage spills are widespread and dispersed}
  \vfill
  \centering
  \begin{figure}
    \centering
    \begin{subfigure}[t]{0.49\textwidth}
      \centering
      \includegraphics[width=\linewidth]{spill_avg_annual_count_2021_2023_london_inset_slides.pdf}
    \end{subfigure}
    \hfill
    \begin{subfigure}[t]{0.49\textwidth}
      \centering
      \includegraphics[width=\linewidth]{dry_spill_avg_annual_count_2021_2023_london_inset_slides.pdf}
    \end{subfigure}
  \end{figure}

  \notes[0.95]{Average annual sewage spill counts aggregated to MSOAs in England over 2021--2023.}
\end{frame}
```

## Related

- `docs/solutions/design-patterns/fit-wide-latex-regression-tables.md` covers
  the analogous table lesson: fit should be handled structurally at the output
  boundary rather than by trial-and-error shrinking.
- `docs/solutions/build-errors/overleaf-beamer-local-vscode-compile-paths.md`
  covers how Beamer resolves graphic paths when compiling locally versus in
  Overleaf.
- `docs/solutions/design-patterns/parameterize-analysis-scripts-over-a-config-vector.md`
  covers the broader pattern of parameterizing analysis scripts over named
  output variants.
- GitHub issue search was attempted for related figure/LaTeX issues, but the
  local environment could not connect to `api.github.com`; no issue reference
  was used.
