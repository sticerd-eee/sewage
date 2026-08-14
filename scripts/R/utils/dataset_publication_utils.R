############################################################
# Validated Dataset Publication Utilities
# Project: Sewage
############################################################

dataset_publication_remove <- function(path, remove_path) {
  error <- NULL
  tryCatch(
    remove_path(path),
    error = function(condition) error <<- condition
  )
  list(removed = !dir.exists(path), error = error)
}

dataset_publication_assert_sibling_stage <- function(stage_path, output_path) {
  if (!is.character(stage_path) || length(stage_path) != 1L ||
      is.na(stage_path) || !nzchar(stage_path) || !is.character(output_path) ||
      length(output_path) != 1L || is.na(output_path) || !nzchar(output_path)) {
    stop("stage_path and output_path must each be one nonempty path.", call. = FALSE)
  }
  stage_parent <- normalizePath(dirname(stage_path), mustWork = TRUE)
  output_parent <- normalizePath(dirname(output_path), mustWork = TRUE)
  if (!identical(stage_parent, output_parent)) {
    stop("The publication stage must be a sibling of the canonical path.", call. = FALSE)
  }
  if (basename(stage_path) %in% c(basename(output_path), paste0(basename(output_path), ".prev"))) {
    stop("The publication stage must be distinct from canonical and .prev.", call. = FALSE)
  }
  invisible(TRUE)
}

dataset_publication_validate <- function(validate, path, context) {
  tryCatch(
    {
      validate(path)
      invisible(TRUE)
    },
    error = function(error) {
      stop(
        context, ": ", conditionMessage(error),
        call. = FALSE
      )
    }
  )
}

dataset_publication_check_state <- function(output_path) {
  previous_path <- paste0(output_path, ".prev")
  canonical_exists <- dir.exists(output_path)
  previous_exists <- dir.exists(previous_path)
  if (!canonical_exists && previous_exists) {
    stop(
      "Interrupted publication state: canonical is absent; recoverable prior generation: ",
      previous_path,
      call. = FALSE
    )
  }
  if (canonical_exists && previous_exists) {
    stop(
      "Publication state is ambiguous: both canonical and .prev are present: ",
      output_path, " and ", previous_path,
      call. = FALSE
    )
  }
  list(
    canonical_exists = canonical_exists,
    previous_exists = previous_exists,
    previous_path = previous_path
  )
}

#' Publish one fully validated sibling dataset generation.
#'
#' The product owns `validate`; this utility owns only the four-state path
#' preflight, atomic directory renames, restoration, and cleanup of the backup
#' created by this attempt.
publish_validated_dataset <- function(
    stage_path, output_path, validate,
    rename_path = file.rename,
    remove_path = function(path) unlink(path, recursive = TRUE)) {
  if (!is.function(validate) || !is.function(rename_path) ||
      !is.function(remove_path)) {
    stop("validate, rename_path, and remove_path must be functions.", call. = FALSE)
  }
  if (!dir.exists(stage_path)) {
    stop("Publication stage does not exist: ", stage_path, call. = FALSE)
  }
  dataset_publication_assert_sibling_stage(stage_path, output_path)

  state <- dataset_publication_check_state(output_path)
  previous_path <- state$previous_path
  canonical_exists <- state$canonical_exists

  dataset_publication_validate(
    validate,
    stage_path,
    "Staged candidate validation failed"
  )

  created_backup <- FALSE
  if (canonical_exists) {
    preserved <- tryCatch(
      isTRUE(rename_path(output_path, previous_path)),
      error = function(error) FALSE
    )
    if (!preserved || dir.exists(output_path) || !dir.exists(previous_path)) {
      readable <- c(
        if (dir.exists(output_path)) output_path,
        if (dir.exists(previous_path)) previous_path
      )
      stop(
        "Failed to preserve the canonical generation before promotion. Readable path(s): ",
        paste(readable, collapse = ", "),
        call. = FALSE
      )
    }
    created_backup <- TRUE
  }

  promoted <- tryCatch(
    isTRUE(rename_path(stage_path, output_path)),
    error = function(error) FALSE
  )
  promoted <- promoted && dir.exists(output_path) && !dir.exists(stage_path)
  if (!promoted) {
    if (created_backup) {
      restored <- tryCatch(
        isTRUE(rename_path(previous_path, output_path)),
        error = function(error) FALSE
      )
      restored <- restored && dir.exists(output_path) && !dir.exists(previous_path)
      if (restored) {
        stop(
          "Failed to promote the staged dataset; the prior canonical was restored.",
          call. = FALSE
        )
      }
      stop(
        "Failed to promote the staged dataset and failed to restore the prior canonical. ",
        "Recoverable path: ", previous_path,
        call. = FALSE
      )
    }
    stop("Failed to promote the first dataset generation.", call. = FALSE)
  }

  final_error <- tryCatch(
    {
      validate(output_path)
      NULL
    },
    error = identity
  )
  if (inherits(final_error, "error")) {
    rejected_cleanup <- dataset_publication_remove(output_path, remove_path)
    if (created_backup) {
      if (rejected_cleanup$removed) {
        restored <- tryCatch(
          isTRUE(rename_path(previous_path, output_path)),
          error = function(error) FALSE
        )
        restored <- restored && dir.exists(output_path) && !dir.exists(previous_path)
        if (restored) {
          stop(
            "Final validation failed; the prior canonical was restored: ",
            conditionMessage(final_error),
            call. = FALSE
          )
        }
      }
      readable <- c(
        if (dir.exists(output_path)) output_path,
        if (dir.exists(previous_path)) previous_path
      )
      stop(
        "Final validation failed and automatic restoration did not complete. ",
        "Readable path(s): ", paste(readable, collapse = ", "),
        ". Validation error: ", conditionMessage(final_error),
        call. = FALSE
      )
    }
    if (rejected_cleanup$removed) {
      stop(
        "Final validation failed for the first generation; the rejected canonical was removed: ",
        conditionMessage(final_error),
        call. = FALSE
      )
    }
    stop(
      "Final validation failed for the first generation and rejected-candidate cleanup failed. ",
      "Readable rejected path: ", output_path,
      ". Validation error: ", conditionMessage(final_error),
      call. = FALSE
    )
  }

  if (created_backup) {
    backup_cleanup <- dataset_publication_remove(previous_path, remove_path)
    if (!backup_cleanup$removed) {
      stop(
        "Publication cleanup incomplete: validated canonical and temporary backup remain readable at ",
        output_path, " and ", previous_path, ".",
        call. = FALSE
      )
    }
  }
  invisible(output_path)
}
