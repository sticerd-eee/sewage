############################################################
# Prior-Exposure Publication Utilities
# Project: Sewage
############################################################

schema_signature <- function(schema) {
  stats::setNames(
    vapply(schema$fields, function(field) field$type$ToString(), character(1)),
    schema$names
  )
}

#' Publish a complete radius-partitioned prior-exposure generation.
#'
#' The candidate is written and validated beside the canonical directory before
#' the canonical generation is moved. Publication assumes one writer per path.
#'
#' @param data Complete in-memory candidate.
#' @param output_path Canonical Arrow dataset directory.
#' @param expected_schema Literal on-disk Arrow schema, including Hive radius.
#' @param expected_radii Exact configured integer radius set.
#' @param rename_path Injectable directory-rename seam used by focused tests.
#' @return `output_path`, invisibly.
publish_prior_exposure_dataset <- function(
    data, output_path, expected_schema, expected_radii,
    rename_path = file.rename) {
  expected_rows <- nrow(data)
  if (is.null(expected_rows) || expected_rows == 0L) {
    stop("Cannot publish an empty prior-exposure candidate.", call. = FALSE)
  }

  expected_radii <- as.numeric(expected_radii)
  if (length(expected_radii) == 0L || anyNA(expected_radii) ||
      any(!is.finite(expected_radii)) || any(expected_radii < 0) ||
      any(expected_radii != floor(expected_radii)) ||
      any(expected_radii > .Machine$integer.max) ||
      anyDuplicated(expected_radii)) {
    stop("expected_radii must be unique, nonnegative integers.", call. = FALSE)
  }
  expected_radii <- sort(as.integer(expected_radii))

  parent_dir <- dirname(output_path)
  dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
  stage_path <- tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = parent_dir
  )
  on.exit({
    if (dir.exists(stage_path)) {
      cleanup_status <- unlink(stage_path, recursive = TRUE)
      if (cleanup_status != 0L && dir.exists(stage_path)) {
        warning("Could not remove prior-exposure stage: ", stage_path, call. = FALSE)
      }
    }
  }, add = TRUE)

  arrow::write_dataset(
    data,
    path = stage_path,
    format = "parquet",
    partitioning = "radius"
  )

  staged <- tryCatch(
    arrow::open_dataset(stage_path),
    error = function(error) {
      stop(
        "Staged prior-exposure dataset could not be reopened: ",
        conditionMessage(error),
        call. = FALSE
      )
    }
  )
  actual_signature <- schema_signature(staged$schema)
  expected_signature <- schema_signature(expected_schema)
  if (!identical(actual_signature, expected_signature)) {
    stop(
      "Staged prior-exposure schema mismatch. Expected ",
      paste(names(expected_signature), expected_signature, collapse = ", "),
      "; found ",
      paste(names(actual_signature), actual_signature, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  staged_rows <- staged |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(.data$n)
  if (!identical(as.numeric(staged_rows), as.numeric(expected_rows))) {
    stop(
      "Staged prior-exposure row count mismatch: expected ", expected_rows,
      ", found ", staged_rows, ".",
      call. = FALSE
    )
  }

  staged_radii <- staged |>
    dplyr::distinct(.data$radius) |>
    dplyr::collect() |>
    dplyr::pull(.data$radius) |>
    as.integer() |>
    sort()
  if (!identical(staged_radii, expected_radii)) {
    stop(
      "Staged prior-exposure radius mismatch: expected ",
      paste(expected_radii, collapse = ", "), "; found ",
      paste(staged_radii, collapse = ", "), ".",
      call. = FALSE
    )
  }

  previous_path <- paste0(output_path, ".prev")
  canonical_exists <- dir.exists(output_path)
  previous_exists <- dir.exists(previous_path)
  if (!canonical_exists && previous_exists) {
    stop(
      "Interrupted prior-exposure publication: canonical is absent; recoverable prior generation: ",
      previous_path,
      call. = FALSE
    )
  }

  if (canonical_exists) {
    if (previous_exists) {
      remove_status <- unlink(previous_path, recursive = TRUE)
      if (remove_status != 0L || dir.exists(previous_path)) {
        stop(
          "Failed to remove older prior-exposure backup: ", previous_path,
          call. = FALSE
        )
      }
    }

    preserved <- isTRUE(rename_path(output_path, previous_path))
    if (!preserved || dir.exists(output_path) || !dir.exists(previous_path)) {
      recoverable <- if (dir.exists(previous_path)) {
        paste0(" Recoverable prior generation: ", previous_path, ".")
      } else {
        ""
      }
      stop(
        "Failed to preserve the canonical prior-exposure generation.",
        recoverable,
        call. = FALSE
      )
    }
  }

  promoted <- isTRUE(rename_path(stage_path, output_path))
  if (promoted && dir.exists(output_path) && !dir.exists(stage_path)) {
    return(invisible(output_path))
  }

  if (canonical_exists) {
    restored <- isTRUE(rename_path(previous_path, output_path))
    if (restored && dir.exists(output_path) && !dir.exists(previous_path)) {
      stop(
        "Failed to promote the staged prior-exposure dataset; the prior generation was restored.",
        call. = FALSE
      )
    }
    stop(
      "Failed to promote the staged prior-exposure dataset and failed to restore the prior generation. Recoverable prior generation: ",
      previous_path,
      call. = FALSE
    )
  }

  stop("Failed to promote the first prior-exposure generation.", call. = FALSE)
}
