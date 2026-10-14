# ==============================================================================
# Annual Return canonical-mapping utilities
# ==============================================================================

#' Finalise an Annual Return lookup join without an identifier fallback
#'
#' @param mapped_rows Annual Return rows after joining the year-specific lookup.
#' @param context Label used in failure diagnostics.
#' @return `mapped_rows` with an integer `site_id_canonical` column.
finalise_annual_lookup_mapping <- function(
    mapped_rows,
    context = "Annual Return"
) {
  required_columns <- c("year", "year_site_id", "site_id_canonical")
  missing_columns <- setdiff(required_columns, names(mapped_rows))
  if (length(missing_columns) > 0L) {
    stop(
      context, " mapping lacks required column(s): ",
      paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  canonical_id <- suppressWarnings(as.integer(mapped_rows$site_id_canonical))
  uncovered <- is.na(mapped_rows$year_site_id) | is.na(canonical_id)
  if (any(uncovered)) {
    uncovered_years <- sort(unique(as.integer(mapped_rows$year[uncovered])))
    year_label <- paste(uncovered_years, collapse = ", ")
    examples <- unique(paste0(
      mapped_rows$year[uncovered], "/",
      ifelse(
        is.na(mapped_rows$year_site_id[uncovered]),
        "<missing>",
        as.character(mapped_rows$year_site_id[uncovered])
      )
    ))
    examples <- paste(utils::head(examples, 5L), collapse = ", ")

    stop(
      context, " rows for ", year_label,
      " exist without lookup coverage (", sum(uncovered),
      " unmatched; year/site examples: ", examples, ").",
      call. = FALSE
    )
  }

  mapped_rows$site_id_canonical <- canonical_id
  mapped_rows
}
