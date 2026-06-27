source(file.path("scripts", "R", "09_analysis", "utils_table_formatting.R"))

assert_contains <- function(x, pattern) {
  if (!grepl(pattern, x, fixed = TRUE)) {
    stop("Expected output to contain: ", pattern, call. = FALSE)
  }
}

assert_not_contains <- function(x, pattern) {
  if (grepl(pattern, x, fixed = TRUE)) {
    stop("Expected output not to contain: ", pattern, call. = FALSE)
  }
}

make_tblr <- function(n_model_cols, value = "-0.000") {
  q_cols <- paste(rep("Q[]", n_model_cols + 1L), collapse = "")
  paste(
    "\\begin{table}",
    "\\begin{talltblr}[",
    "caption={Fixture},",
    "note{}={ },",
    "]{",
    "%% tabularray inner open",
    paste0("colspec={", q_cols, "},"),
    "}",
    paste0("Term & ", value, " \\\\"),
    "\\end{talltblr}",
    "\\end{table}",
    sep = "\n"
  )
}

notes <- "note{}={\\\\footnotesize{\\\\textbf{Notes:}}},"

wide <- fit_tblr_latex(
  make_tblr(12L),
  label = "tbl:fixture-wide",
  notes = notes
)
assert_contains(wide, "\\begin{table}[H]")
assert_contains(wide, "label={tbl:fixture-wide},")
assert_contains(wide, "colspec={l X[c]")
assert_contains(wide, "colsep=2pt,")
assert_contains(wide, "cells   = {font = \\fontsize{8pt}{9pt}\\selectfont},")
assert_contains(wide, "\\footnotesize{\\textbf{Notes:}}")
assert_contains(wide, "0.000")
assert_not_contains(wide, "-0.000")

medium <- fit_tblr_latex(make_tblr(8L), label = "tbl:fixture-medium")
assert_contains(medium, "colsep=3pt,")
assert_contains(medium, "cells   = {font = \\fontsize{9pt}{10pt}\\selectfont},")

narrow <- fit_tblr_latex(make_tblr(6L), label = "tbl:fixture-narrow")
assert_contains(narrow, "colsep=4pt,")
assert_contains(narrow, "cells   = {font = \\fontsize{11pt}{12pt}\\selectfont},")

explicit <- fit_tblr_latex(
  make_tblr(12L),
  label = "tbl:fixture-explicit",
  colsep = "5pt",
  rowsep = "0.1pt",
  width = "0.9\\linewidth",
  hspan = "even",
  cell_font = "\\fontsize{7pt}{8pt}\\selectfont"
)
assert_contains(explicit, "width=0.9\\linewidth,")
assert_contains(explicit, "hspan = even,")
assert_contains(explicit, "colsep=5pt,")
assert_contains(explicit, "rowsep=0.1pt,")
assert_contains(explicit, "cells   = {font = \\fontsize{7pt}{8pt}\\selectfont},")

missing_anchor <- tryCatch(
  fit_tblr_latex("\\begin{table}\ncaption={Fixture},", label = "tbl:bad"),
  error = identity
)
if (!inherits(missing_anchor, "error")) {
  stop("Expected missing inner-open anchor to raise an error.", call. = FALSE)
}

formatted <- fmt_table(c(12.26, -0.0057, 0.0014, 0.21, NA))
expected <- c("12.260", "-0.006", "0.001", "0.210", NA)
if (!identical(formatted, expected)) {
  stop(
    "Unexpected fmt_table output: ",
    paste(formatted, collapse = ", "),
    call. = FALSE
  )
}

cat("Table formatting tests passed.\n")
