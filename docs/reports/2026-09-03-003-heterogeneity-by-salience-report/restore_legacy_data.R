source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))
source(here::here("scripts", "R", "utils", "dataset_publication_utils.R"))

# Restore only the exact inventoried files. The caller must use the matching
# legacy source snapshot; refined consumers deliberately continue to reject it.
restore_legacy_data <- function(manifest_path, destination_root) {
  manifest <- jsonlite::read_json(manifest_path)
  entries <- manifest$recovery
  paths <- vapply(entries, `[[`, "", "path")
  if (!identical(manifest$profile, "legacy_tidal") || !length(entries) || anyDuplicated(paths) ||
      any(grepl("(^/|(^|/)\\.\\.(/|$))", paths))) stop("Invalid legacy recovery inventory.")
  for (entry in entries) {
    path <- file.path(manifest$recovery_root, entry$path)
    if (!file.exists(path) || !identical(salience_file_hash(path), entry$sha256)) stop("Recovery source hash mismatch.")
  }
  for (entry in entries) {
    target <- file.path(destination_root, entry$path)
    dir.create(dirname(target), recursive = TRUE, showWarnings = FALSE)
    candidate <- tempfile(".recovery-", tmpdir = dirname(target))
    if (!file.copy(file.path(manifest$recovery_root, entry$path), candidate)) stop("Cannot stage recovery file.")
    expected_hash <- entry$sha256
    publish_validated_file(candidate, target, function(path) {
      if (!identical(salience_file_hash(path), expected_hash)) stop("Recovered artifact hash mismatch.")
    })
  }
  invisible(paths)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 1L) stop("Supply an explicit destination data directory for the complete legacy recovery set.")
  restore_legacy_data(file.path(salience_report_root(), "historical", "manifest.json"), args[[1L]])
}
