# Additive open-coast evidence and provenance shared by builders and consumers.
open_coast_site_columns <- function() {
  c("distance_to_open_coast_m", "open_coast_status", "geometry_generation", "site_generation")
}

open_coast_radius_columns <- function() {
  c("min_open_coast_dist_m", "max_open_coast_dist_m", "n_open_coast_known",
    "n_open_coast_missing", "site_generation")
}

open_coast_site_generation <- function(data, location_hash) {
  values <- as.data.frame(data[order(data$site_id), setdiff(names(data), "site_generation")])
  rownames(values) <- NULL
  digest::digest(list(values = values, location_hash = location_hash), algo = "sha256")
}

validate_open_coast_companion_manifest <- function(path, expected_generation) {
  manifest_path <- file.path(path, "_open_coast_manifest.json")
  if (!file.exists(manifest_path)) stop("Radius input provenance is absent.")
  manifest <- jsonlite::read_json(manifest_path)
  if (!identical(manifest$status, "complete") || !identical(manifest$site_generation, expected_generation) ||
      length(manifest$outputs) != 3L || !length(manifest$inputs)) stop("Incompatible radius input provenance.")
  for (entry in manifest$inputs) {
    input <- if (startsWith(entry$path, "/")) entry$path else here::here(entry$path)
    if (!file.exists(input) || !identical(digest::digest(file = input, algo = "sha256"), entry$sha256))
      stop("Radius source input changed: ", entry$path)
  }
  expected <- paste0("radius=", c(250L, 500L, 1000L), "/part-0.parquet")
  if (!setequal(vapply(manifest$outputs, `[[`, "", "path"), expected)) stop("Incomplete radius output inventory.")
  for (entry in manifest$outputs) {
    output <- file.path(path, entry$path)
    if (!file.exists(output) || !identical(digest::digest(file = output, algo = "sha256"), entry$sha256))
      stop("Radius output hash mismatch.")
  }
  invisible(manifest)
}

single_open_coast_generation <- function(data, column = "site_generation") {
  if (!column %in% names(data) || !nrow(data) || anyNA(data[[column]]) ||
      any(!grepl("^[a-f0-9]{64}$", data[[column]])) || length(unique(data[[column]])) != 1L) {
    stop("Missing or mixed open-coast ", column, ".", call. = FALSE)
  }
  unique(data[[column]])
}

validate_open_coast_sites <- function(data) {
  if (!all(open_coast_site_columns() %in% names(data))) {
    stop("Refined Site Group characteristics are absent.", call. = FALSE)
  }
  single_open_coast_generation(data)
  single_open_coast_generation(data, "geometry_generation")
  distance <- data$distance_to_open_coast_m
  status <- data$open_coast_status
  if (anyNA(status) || any(!status %in% c("validated", "missing_location", "unresolved_shore", "outside_coverage")) ||
      any((status == "validated") != is.finite(distance)) ||
      any(is.nan(distance)) || any(!is.na(distance) & (!is.finite(distance) | distance < 0))) {
    stop("Inconsistent open-coast distance and evidence status.", call. = FALSE)
  }
  invisible(data)
}

validate_open_coast_radius <- function(data, expected_generation = NULL) {
  if (!all(open_coast_radius_columns() %in% names(data))) {
    stop("Refined radius evidence is absent.", call. = FALSE)
  }
  generation <- single_open_coast_generation(data)
  if (!is.null(expected_generation) && !identical(generation, expected_generation)) {
    stop("Radius and Site Group generations differ.", call. = FALSE)
  }
  known <- data$n_open_coast_known
  missing <- data$n_open_coast_missing
  if (anyNA(known) || anyNA(missing) || any(known < 0 | missing < 0) ||
      any(known != floor(known) | missing != floor(missing)) ||
      any(known + missing != data$n_spill_sites)) {
    stop("Open-coast evidence counts must reconcile to all contributing sites.", call. = FALSE)
  }
  lo <- data$min_open_coast_dist_m
  hi <- data$max_open_coast_dist_m
  if (any(is.finite(lo) != (known > 0)) || any(is.finite(hi) != (known > 0)) ||
      any(!is.na(lo) & (!is.finite(lo) | lo < 0)) ||
      any(!is.na(hi) & (!is.finite(hi) | hi < 0)) || any(is.nan(lo) | is.nan(hi)) || any(lo > hi, na.rm = TRUE)) {
    stop("Open-coast summaries disagree with known evidence counts.", call. = FALSE)
  }
  invisible(data)
}
