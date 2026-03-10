check_required_packages <- function(required_packages) {
  missing_packages <- required_packages[!vapply(
    required_packages,
    requireNamespace,
    quietly = TRUE,
    FUN.VALUE = logical(1)
  )]

  if (length(missing_packages) > 0) {
    stop(
      paste0(
        "Missing required packages: ",
        paste(missing_packages, collapse = ", "),
        ". Install project dependencies first, e.g. `renv::restore()`."
      ),
      call. = FALSE
    )
  }

  invisible(required_packages)
}

setup_logging <- function(log_file, console = interactive(), threshold = "DEBUG") {
  dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

  appender <- if (isTRUE(console)) {
    logger::appender_tee(log_file)
  } else {
    logger::appender_file(log_file)
  }

  logger::log_appender(appender)
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(threshold)

  invisible(log_file)
}
