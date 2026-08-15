# ==============================================================================
# DuckDB Input Refresh Utilities
# ==============================================================================

#' Configure a stable, existing DuckDB spill directory beside the database
#'
#' @param con A DBI connection to DuckDB.
#' @param db_path Path to the persistent DuckDB database.
#' @return The normalized spill-directory path, invisibly.
configure_duckdb_temp_directory <- function(con, db_path) {
  spill_dir <- file.path(dirname(normalizePath(db_path, mustWork = FALSE)), "duckdb_temp")
  dir.create(spill_dir, recursive = TRUE, showWarnings = FALSE)
  spill_dir <- normalizePath(spill_dir, mustWork = TRUE)

  DBI::dbExecute(
    con,
    paste(
      "SET temp_directory =",
      DBI::dbQuoteString(con, spill_dir)
    )
  )

  invisible(spill_dir)
}

#' Replace one named DuckDB relation with a parquet-backed view
#'
#' Only the named relation is replaced. Other persistent database relations are
#' left untouched, and the replacement is transactional so a failed view
#' creation restores the prior relation.
#'
#' @param con A DBI connection to DuckDB.
#' @param relation_name Name of the relation to replace.
#' @param parquet_path Path to the current parquet file.
#' @return NULL, invisibly.
refresh_duckdb_parquet_view <- function(con, relation_name, parquet_path) {
  if (!is.character(relation_name) || length(relation_name) != 1L ||
      is.na(relation_name) || !nzchar(relation_name)) {
    stop("`relation_name` must be one nonempty string.", call. = FALSE)
  }
  if ((!file.exists(parquet_path) && !dir.exists(parquet_path))) {
    stop("Parquet input does not exist: ", parquet_path, call. = FALSE)
  }

  relation <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT table_type FROM information_schema.tables",
      "WHERE table_schema = 'main' AND table_name = ?"
    ),
    params = list(relation_name)
  )
  if (nrow(relation) > 1L) {
    stop("DuckDB relation name is ambiguous: ", relation_name, call. = FALSE)
  }

  quoted_name <- DBI::dbQuoteIdentifier(con, relation_name)
  quoted_path <- DBI::dbQuoteString(
    con,
    normalizePath(parquet_path, mustWork = TRUE)
  )

  DBI::dbWithTransaction(con, {
    if (nrow(relation) == 1L) {
      drop_kind <- switch(
        relation$table_type[[1L]],
        "BASE TABLE" = "TABLE",
        "VIEW" = "VIEW",
        stop(
          "Unsupported DuckDB relation type for `", relation_name, "`: ",
          relation$table_type[[1L]],
          call. = FALSE
        )
      )
      DBI::dbExecute(
        con,
        paste("DROP", drop_kind, quoted_name)
      )
    }
    DBI::dbExecute(
      con,
      paste0(
        "CREATE VIEW ", quoted_name,
        " AS SELECT * FROM read_parquet(", quoted_path, ")"
      )
    )
  })

  invisible(NULL)
}
