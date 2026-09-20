# Report storage must fail closed without ever fitting missing results.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report",
                  "report_storage.R"))
expect_error <- function(expr, pattern) {
  error <- tryCatch(force(expr), error = identity)
  stopifnot(inherits(error, "error"), grepl(pattern, conditionMessage(error)))
}
root <- tempfile("salience-storage-")
dir.create(root)
bundle <- list(settings = list(london = "excluded", groups_overlap = TRUE,
                               config = list(coast_rule_m = 2000L)),
               models = list(), counts = data.frame(), results = data.frame())
expect_error(read_salience_snapshot("absent", "overlapping", root), "unavailable")
saveRDS(bundle, file.path(root, "fixture.rds"))
entry <- list(path = "fixture.rds", sha256 = salience_file_hash(file.path(root, "fixture.rds")),
              family = "overlapping", profile = "legacy_tidal", london = "excluded",
              bathing = "ever_reported_2021_2024")
jsonlite::write_json(list(schema_version = 1L, artifacts = list(fixture = entry)),
                     file.path(root, "manifest.json"), auto_unbox = TRUE)
stopifnot(identical(read_salience_snapshot("fixture", "overlapping", root), bundle))
expect_error(read_salience_snapshot("fixture", "exclusive", root), "family")
entry$profile <- "refined_open_coast"
jsonlite::write_json(list(schema_version = 1L, artifacts = list(fixture = entry)),
                     file.path(root, "manifest.json"), auto_unbox = TRUE)
expect_error(read_salience_snapshot("fixture", "overlapping", root), "profile")
entry$profile <- "legacy_tidal"
entry$path <- "../fixture.rds"
jsonlite::write_json(list(schema_version = 1L, artifacts = list(fixture = entry)),
                     file.path(root, "manifest.json"), auto_unbox = TRUE)
expect_error(read_salience_snapshot("fixture", "overlapping", root), "path")
entry$path <- "fixture.rds"
jsonlite::write_json(list(schema_version = 1L, artifacts = list(fixture = entry)),
                     file.path(root, "manifest.json"), auto_unbox = TRUE)
saveRDS(list(stale = TRUE), file.path(root, "fixture.rds"))
expect_error(read_salience_snapshot("fixture", "overlapping", root), "hash")
unlink(root, recursive = TRUE)

# Execute extracted definitions while live reads and estimators are forbidden.
for (fn in c("feols", "feglm")) {
  trace(fn, tracer = quote(stop("Unexpected model fitting during report extraction")),
        where = asNamespace("fixest"), print = FALSE)
}
for (fn in c("read_parquet", "open_dataset")) {
  trace(fn, tracer = quote(stop("Unexpected data read during report extraction")),
        where = asNamespace("arrow"), print = FALSE)
}
definitions <- new.env(parent = globalenv())
sys.source(here::here("scripts", "R", "testing", "salience_report_test_setup.R"), definitions)
stopifnot(is.function(definitions$run_salience_extensive))

snapshot_root <- file.path(salience_report_root(), "historical")
if (file.exists(file.path(snapshot_root, "manifest.json"))) {
  manifest <- jsonlite::read_json(file.path(snapshot_root, "manifest.json"))
  paths <- c(vapply(manifest$artifacts, `[[`, "", "path"),
             vapply(manifest$exports, `[[`, "", "path"))
  live <- here::here("output", paths)
  before <- vapply(live, salience_file_hash, "")
  render_env <- new.env(parent = globalenv())
  render_env$params <- list(mode = "render-only", reestimate = FALSE)
  figure_path <- paste0(tempfile("report-figures-"), "/")
  knitr::opts_chunk$set(fig.path = figure_path)
  # Raw knit() otherwise embeds chunk errors and still returns successfully.
  # A traced estimator/data-read error must fail this behavioral render check.
  knitr::opts_chunk$set(error = FALSE)
  output <- tempfile(fileext = ".md")
  knitr::knit(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report.qmd"),
              output = output, envir = render_env, quiet = TRUE)
  stopifnot(identical(before, vapply(live, salience_file_hash, "")))
  unlink(output)
  unlink(figure_path, recursive = TRUE)
  cat("Canonical saved-bundle render ran without data reads, fitting or production writes.\n")
} else {
  cat("Canonical render skipped: historical snapshot is absent.\n")
}
for (fn in c("feols", "feglm")) untrace(fn, where = asNamespace("fixest"))
for (fn in c("read_parquet", "open_dataset")) untrace(fn, where = asNamespace("arrow"))
cat("Salience report storage contracts passed.\n")
