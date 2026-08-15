# ==============================================================================
# Transaction-ID Artifact Match-Rate Verification
# ==============================================================================
#
# Purpose: Certify referential integrity after the positional-to-hash ID rebuild.
#          Every nonmissing transaction ID in each declared regenerated artifact
#          must match its declared cleaned source. Artifacts are scanned in
#          bounded Arrow record batches; canonical data is read only.
#
# Output:
#   - output/log/id_artifact_match_rates.csv
#
# This is a one-time transition tripwire, not a substitute for producer-specific
# completeness and correctness contract tests.
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}
if (!requireNamespace("arrow", quietly = TRUE) ||
    !requireNamespace("dplyr", quietly = TRUE) ||
    !requireNamespace("tibble", quietly = TRUE)) {
  stop("Packages `arrow`, `dplyr`, and `tibble` are required.", call. = FALSE)
}

id_field_type <- function(schema, id) {
  positions <- match(id, schema$names)
  if (is.na(positions)) {
    stop("Dataset is missing transaction ID column `", id, "`.", call. = FALSE)
  }
  schema$fields[[positions]]$type$ToString()
}

read_unique_source_ids <- function(source_path, id) {
  if (!file.exists(source_path) && !dir.exists(source_path)) {
    stop("Cleaned source does not exist: ", source_path, call. = FALSE)
  }
  dataset <- arrow::open_dataset(source_path)
  source_type <- id_field_type(dataset$schema, id)
  if (!identical(source_type, "string")) {
    stop(
      "Cleaned source `", id, "` must be Arrow utf8/string; found ",
      source_type, ".",
      call. = FALSE
    )
  }
  ids <- dataset |>
    dplyr::select(dplyr::all_of(id)) |>
    dplyr::collect() |>
    dplyr::pull(dplyr::all_of(id))
  if (anyNA(ids) || any(!nzchar(ids))) {
    stop("Cleaned source `", id, "` contains missing or empty IDs.", call. = FALSE)
  }
  if (anyDuplicated(ids)) {
    stop("Cleaned source `", id, "` contains duplicate IDs.", call. = FALSE)
  }
  ids
}

verify_id_artifact <- function(name, artifact_path, id, source_ids) {
  if (!file.exists(artifact_path) && !dir.exists(artifact_path)) {
    stop("Regenerated artifact does not exist: ", artifact_path, call. = FALSE)
  }
  if (!is.character(source_ids) || anyNA(source_ids) || anyDuplicated(source_ids)) {
    stop("`source_ids` must be unique, nonmissing character IDs.", call. = FALSE)
  }

  dataset <- arrow::open_dataset(artifact_path)
  id_type <- id_field_type(dataset$schema, id)
  if (!identical(id_type, "string")) {
    stop(
      "Artifact `", name, "` must expose `", id,
      "` as Arrow utf8/string; found ", id_type, ".",
      call. = FALSE
    )
  }

  reader <- dataset |>
    dplyr::select(dplyr::all_of(id)) |>
    arrow::as_record_batch_reader()
  total_rows <- 0
  nonmissing_ids <- 0
  matched_ids <- 0
  missing_ids <- 0
  unmatched_examples <- character()

  repeat {
    batch <- reader$read_next_batch()
    if (is.null(batch)) break
    values <- as.data.frame(batch)[[id]]
    total_rows <- total_rows + length(values)
    present <- !is.na(values) & nzchar(values)
    missing_ids <- missing_ids + sum(!present)
    if (any(present)) {
      present_values <- values[present]
      matched <- present_values %in% source_ids
      nonmissing_ids <- nonmissing_ids + length(present_values)
      matched_ids <- matched_ids + sum(matched)
      if (any(!matched) && length(unmatched_examples) < 10L) {
        unmatched_examples <- unique(c(
          unmatched_examples,
          present_values[!matched]
        ))
        unmatched_examples <- head(unmatched_examples, 10L)
      }
    }
  }
  reader$Close()

  match_rate <- if (nonmissing_ids == 0) NA_real_ else matched_ids / nonmissing_ids
  tibble::tibble(
    artifact = name,
    artifact_path = normalizePath(artifact_path, mustWork = TRUE),
    id_column = id,
    id_type = id_type,
    total_rows = as.double(total_rows),
    nonmissing_ids = as.double(nonmissing_ids),
    missing_ids = as.double(missing_ids),
    matched_ids = as.double(matched_ids),
    unmatched_ids = as.double(nonmissing_ids - matched_ids),
    match_rate = match_rate,
    unmatched_examples = paste(unmatched_examples, collapse = ";")
  )
}

verify_id_artifacts <- function(
    specs,
    required_match_rate = 1,
    report_path = NULL) {
  if (!is.numeric(required_match_rate) || length(required_match_rate) != 1L ||
      is.na(required_match_rate) || required_match_rate < 0 ||
      required_match_rate > 1) {
    stop("`required_match_rate` must lie in [0, 1].", call. = FALSE)
  }
  if (!is.list(specs) || length(specs) == 0L) {
    stop("`specs` must be a non-empty artifact specification list.", call. = FALSE)
  }

  source_cache <- new.env(parent = emptyenv())
  results <- lapply(specs, function(spec) {
    required <- c("name", "artifact_path", "source_path", "id")
    if (!all(required %in% names(spec))) {
      stop("Each artifact spec must name: ", paste(required, collapse = ", "), ".", call. = FALSE)
    }
    cache_key <- paste(normalizePath(spec$source_path, mustWork = FALSE), spec$id)
    if (!exists(cache_key, envir = source_cache, inherits = FALSE)) {
      assign(
        cache_key,
        read_unique_source_ids(spec$source_path, spec$id),
        envir = source_cache
      )
    }
    verify_id_artifact(
      spec$name,
      spec$artifact_path,
      spec$id,
      get(cache_key, envir = source_cache, inherits = FALSE)
    )
  })
  report <- dplyr::bind_rows(results)

  if (!is.null(report_path)) {
    dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
    utils::write.csv(report, report_path, row.names = FALSE, na = "")
  }

  failed <- is.na(report$match_rate) |
    report$match_rate < required_match_rate |
    report$unmatched_ids > 0
  if (any(failed)) {
    labels <- paste0(
      report$artifact[failed], "=",
      sprintf("%.6f%%", 100 * report$match_rate[failed])
    )
    stop(
      "ID artifact match rate below required ",
      sprintf("%.0f%%", 100 * required_match_rate),
      ": ", paste(labels, collapse = ", "),
      call. = FALSE
    )
  }

  report
}

default_id_artifact_specs <- function() {
  processed <- here::here("data", "processed")
  sale_source <- file.path(processed, "house_price.parquet")
  rental_source <- file.path(processed, "zoopla", "zoopla_rentals.parquet")
  sale_long_run_source <- file.path(processed, "house_price_long_run.parquet")
  rental_long_run_source <- file.path(
    processed, "zoopla", "zoopla_rentals_long_run.parquet"
  )
  spec <- function(name, path, source, id) {
    list(name = name, artifact_path = path, source_path = source, id = id)
  }

  list(
    spec("spill_house_lookup", file.path(processed, "spill_house_lookup.parquet"), sale_source, "house_id"),
    spec("spill_rental_lookup", file.path(processed, "zoopla", "spill_rental_lookup.parquet"), rental_source, "rental_id"),
    spec("repeated_sales", file.path(processed, "repeated_transactions", "repeated_sales.parquet"), sale_long_run_source, "house_id"),
    spec("repeated_rentals", file.path(processed, "repeated_transactions", "repeated_rentals.parquet"), rental_long_run_source, "rental_id"),
    spec("study_period_sales", file.path(processed, "cross_section", "sales", "study_period"), sale_source, "house_id"),
    spec("study_period_rentals", file.path(processed, "cross_section", "rentals", "study_period"), rental_source, "rental_id"),
    spec("prior_to_sale", file.path(processed, "cross_section", "sales", "prior_to_sale"), sale_source, "house_id"),
    spec("prior_to_rental", file.path(processed, "cross_section", "rentals", "prior_to_rental"), rental_source, "rental_id"),
    spec("prior_to_sale_house_site", file.path(processed, "cross_section", "sales", "prior_to_sale_house_site"), sale_source, "house_id"),
    spec("prior_to_rental_rental_site", file.path(processed, "cross_section", "rentals", "prior_to_rental_rental_site"), rental_source, "rental_id"),
    spec("within_radius_sales", file.path(processed, "within_radius_panel", "sales"), sale_source, "house_id"),
    spec("within_radius_rentals", file.path(processed, "within_radius_panel", "rentals"), rental_source, "rental_id"),
    spec("general_panel_sales", file.path(processed, "general_panel", "sales"), sale_source, "house_id"),
    spec("general_panel_rentals", file.path(processed, "general_panel", "rentals"), rental_source, "rental_id")
  )
}

main <- function() {
  report_path <- here::here("output", "log", "id_artifact_match_rates.csv")
  report <- verify_id_artifacts(
    default_id_artifact_specs(),
    required_match_rate = 1,
    report_path = report_path
  )
  print(report[, c(
    "artifact", "id_column", "id_type", "total_rows", "nonmissing_ids",
    "missing_ids", "matched_ids", "unmatched_ids", "match_rate"
  )])
  message("All declared ID-keyed artifacts match their cleaned inputs at 100%.")
  invisible(report)
}

if (sys.nframe() == 0L) main()
