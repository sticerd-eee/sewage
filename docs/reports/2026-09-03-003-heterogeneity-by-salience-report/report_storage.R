# Saved historical evidence is immutable. Readers never fall back to live exports.
salience_report_root <- function() {
  here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report")
}

salience_file_hash <- function(path) {
  digest::digest(file = path, algo = "sha256")
}

# Publish the compact geographic evidence used by render-only report mode.
write_geography_evidence_manifest <- function() {
  root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
  paths <- c("geography_review.json", "final_coverage_summary.csv", "threshold_review.csv",
    "solway_nearest_shores.csv", paste0("final-", c("thames", "severn", "morecambe", "poole", "dee", "solway"), ".png"))
  review <- jsonlite::read_json(file.path(root, "geography_review.json"))
  if (!identical(review$status, "approved") || !identical(review$unresolved_relevant_segments, 0L) ||
      !all(file.exists(file.path(root, paths)))) stop("Geography review evidence is incomplete.")
  manifest <- list(status = "complete", artifacts = lapply(paths, function(path)
    list(path = path, sha256 = salience_file_hash(file.path(root, path)))))
  source(here::here("scripts", "R", "utils", "dataset_publication_utils.R"), local = TRUE)
  candidate <- file.path(root, "final_geometry_manifest.json.candidate")
  jsonlite::write_json(manifest, candidate, auto_unbox = TRUE, pretty = TRUE)
  publish_validated_file(candidate, file.path(root, "final_geometry_manifest.json"), function(path) {
    observed <- jsonlite::read_json(path)
    if (!identical(observed$status, "complete") || length(observed$artifacts) != length(paths))
      stop("Incomplete geography evidence manifest.")
  })
  invisible(manifest)
}

read_salience_snapshot <- function(prefix, family,
                                   root = file.path(salience_report_root(), "historical")) {
  manifest_path <- file.path(root, "manifest.json")
  if (!file.exists(manifest_path)) {
    stop("Historical results unavailable: missing snapshot manifest. Run the explicit ",
         "preservation command before replacing any production results.", call. = FALSE)
  }
  manifest <- jsonlite::read_json(manifest_path, simplifyVector = FALSE)
  if (!identical(manifest$schema_version, 1L)) stop("Unknown historical manifest schema.")
  entry <- manifest$artifacts[[prefix]]
  if (is.null(entry)) stop("Historical result unavailable: ", prefix, call. = FALSE)
  if (!identical(entry$family, family)) stop("Historical group family mismatch: ", prefix)
  if (!identical(entry$profile, "legacy_tidal")) stop("Historical coast profile mismatch: ", prefix)
  if (!identical(entry$bathing, "ever_reported_2021_2024")) stop("Historical bathing policy mismatch.")
  if (is.null(entry$path) || grepl("(^/|(^|/)\\.\\.(/|$))", entry$path)) {
    stop("Invalid historical artifact path.")
  }
  path <- file.path(root, entry$path)
  if (!file.exists(path)) stop("Historical result unavailable: ", prefix)
  if (!startsWith(normalizePath(path), paste0(normalizePath(root), "/"))) {
    stop("Historical artifact path escapes the snapshot.")
  }
  if (!identical(salience_file_hash(path), entry$sha256)) stop("Historical artifact hash mismatch: ", prefix)
  bundle <- readRDS(path)
  if (family == "overlapping" &&
      (!isTRUE(bundle$settings$groups_overlap) ||
       !identical(bundle$settings$london, entry$london) ||
       !identical(as.numeric(bundle$settings$config$coast_rule_m), 2000))) {
    stop("Historical overlapping settings mismatch: ", prefix)
  }
  bundle
}

# The report's optional historical reproduction writes only inside the report.
salience_report_output <- function(...) {
  file.path(salience_report_root(), "historical-reproduction", ...)
}

salience_historical_export <- function(path) {
  root <- file.path(salience_report_root(), "historical")
  manifest <- jsonlite::read_json(file.path(root, "manifest.json"))
  matches <- Filter(function(entry) identical(entry$path, path), manifest$exports)
  if (length(matches) != 1L) stop("Historical reference export unavailable: ", path)
  target <- file.path(root, path)
  if (!file.exists(target) || !identical(salience_file_hash(target), matches[[1L]]$sha256)) {
    stop("Historical reference export hash mismatch: ", path)
  }
  target
}

assert_salience_legacy_inputs <- function() {
  root <- file.path(salience_report_root(), "historical")
  manifest <- jsonlite::read_json(file.path(root, "manifest.json"))
  # A future consumer switch requires explicitly restoring the recorded source
  # snapshot before reproducing. Never silently run legacy data through new code.
  for (entry in manifest$source_snapshot) {
    if (!startsWith(entry$path, "scripts/R/") || startsWith(entry$path, "scripts/R/testing/")) next
    path <- here::here(entry$path)
    if (!file.exists(path) || !identical(salience_file_hash(path), entry$sha256)) {
      stop("Legacy source generation differs: ", entry$path,
           ". Restore the matching historical source snapshot in an isolated checkout.")
    }
  }
  for (entry in manifest$recovery) {
    path <- here::here("data", entry$path)
    if (!file.exists(path) || !identical(salience_file_hash(path), entry$sha256)) {
      stop("Legacy data generation differs: ", entry$path,
           ". Use the complete recorded recovery set; never mix generations.")
    }
  }
  invisible(manifest)
}

validate_salience_report_mode <- function(mode, reestimate = FALSE) {
  if (isTRUE(reestimate)) {
    stop("The reestimate parameter is retired. Use mode:historical-reproduction explicitly.", call. = FALSE)
  }
  match.arg(mode, c("render-only", "historical-reproduction", "refinement-estimation"))
}

validate_salience_geography_audit <- function(root) {
  path <- file.path(root, "candidate_manifest.json")
  if (!file.exists(path)) return(FALSE)
  manifest <- jsonlite::read_json(path)
  if (!identical(manifest$status, "candidate_not_published") || !length(manifest$artifacts)) {
    stop("Geography audit manifest is incomplete or has an unexpected status.")
  }
  expected <- c("coverage_summary.csv", "alignment_summary.csv", "alignment_by_company.csv",
                paste0(c("thames", "severn", "morecambe", "poole", "dee", "solway"), ".png"),
                "source_manifest.json")
  actual <- vapply(manifest$artifacts, `[[`, "", "path")
  if (anyDuplicated(actual) || !setequal(actual, expected)) {
    stop("Geography audit manifest has incomplete or duplicate artifact keys.")
  }
  for (entry in manifest$artifacts) {
    target <- file.path(root, entry$path)
    if (!file.exists(target) || !identical(salience_file_hash(target), entry$sha256)) {
      stop("Geography audit artifact hash mismatch: ", entry$path)
    }
  }
  TRUE
}
