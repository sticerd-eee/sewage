# Load only the report's definition chunks; no regressions or publication.
report <- here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report.qmd")
code <- tempfile(fileext = ".R")
knitr::purl(report, output = code, documentation = 0L, quiet = TRUE)
sys.source(code, envir = environment())
unlink(code)
