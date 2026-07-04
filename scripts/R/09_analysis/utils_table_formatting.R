# ==============================================================================
# Shared helper: regression table formatting
# ==============================================================================
# Centralizes modelsummary precision and the tabularray post-processing used by
# the analysis scripts.

fmt_table <- modelsummary::fmt_decimal(3)

adaptive_tblr_layout <- function(n_x_cols) {
  if (n_x_cols >= 10L) {
    return(list(
      cell_font = "\\fontsize{8pt}{9pt}\\selectfont",
      colsep = "2pt"
    ))
  }

  if (n_x_cols >= 7L) {
    return(list(
      cell_font = "\\fontsize{9pt}{10pt}\\selectfont",
      colsep = "3pt"
    ))
  }

  list(
    cell_font = "\\fontsize{11pt}{12pt}\\selectfont",
    colsep = "4pt"
  )
}

fit_tblr_latex <- function(latex,
                           colsep = NULL,
                           rowsep = NULL,
                           cell_font = NULL,
                           width = NULL,
                           hspan = NULL,
                           notes = NULL,
                           label = NULL,
                           dp = 3) {
  if (!is.character(latex)) {
    latex <- as.character(latex)
  }
  latex <- paste(latex, collapse = "\n")
  force(dp)

  latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", latex)

  if (!is.null(label)) {
    if (!grepl("caption=\\{([^}]*)\\},", latex, perl = TRUE)) {
      stop("Could not find tabularray caption block in modelsummary output.",
           call. = FALSE)
    }

    latex <- sub(
      "caption=\\{([^}]*)\\},",
      paste0("caption={\\1},\nlabel={", label, "},"),
      latex
    )
  }

  inner_open <- regexpr("\\{\\s*%% tabularray inner open\\n", latex, perl = TRUE)
  if (inner_open[[1]] < 0L) {
    stop("Could not find tabularray inner-open block in modelsummary output.",
         call. = FALSE)
  }

  latex <- gsub("Q\\[\\]", "X[c] ", latex)
  latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", latex)

  n_x_cols <- lengths(regmatches(latex, gregexpr("X\\[c\\]", latex, perl = TRUE)))
  layout <- adaptive_tblr_layout(n_x_cols)
  if (is.null(cell_font)) {
    cell_font <- layout$cell_font
  }
  if (is.null(colsep)) {
    colsep <- layout$colsep
  }

  layout_patch <- paste0(
    if (!is.null(width)) paste0("width=", width, ",\n") else "",
    if (!is.null(hspan)) paste0("hspan = ", hspan, ",\n") else "",
    "colsep=", colsep, ",\n",
    if (!is.null(rowsep)) paste0("rowsep=", rowsep, ",\n") else "",
    "cells   = {font = ", cell_font, "},\n"
  )

  insert_at <- inner_open[[1]] + attr(inner_open, "match.length") - 1L
  latex <- paste0(
    substr(latex, 1L, insert_at),
    layout_patch,
    substr(latex, insert_at + 1L, nchar(latex))
  )

  if (
    !grepl("\\fontsize", latex, fixed = TRUE) ||
      !grepl("\\selectfont", latex, fixed = TRUE)
  ) {
    stop("Patched LaTeX table is missing expected font commands.", call. = FALSE)
  }

  if (!is.null(notes)) {
    if (!grepl("note\\{\\}=\\{\\s*\\},", latex, perl = TRUE)) {
      stop("Could not find empty tabularray note block in modelsummary output.",
           call. = FALSE)
    }

    latex <- sub("note\\{\\}=\\{\\s*\\},", notes, latex)
  }

  latex <- gsub("-0\\.000", "0.000", latex)

  latex
}
