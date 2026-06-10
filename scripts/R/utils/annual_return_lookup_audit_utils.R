############################################################
# Annual Return Lookup: Conflict-Audit Utilities
# Project: Sewage
# Date: 10/06/2026
# Author: Jacopo Olivieri
############################################################

#' Conflict-audit subsystem for the annual-return lookup builder
#' (scripts/R/03_data_enrichment/create_annual_return_lookup.R).
#'
#' Defines functions only. The only function with side effects is
#' export_conflict_audit(), which writes the audit files at the paths
#' the caller passes in; audit paths live in the numbered script's
#' CONFIG and are handed to these utilities as arguments.
#'
#' Contract surface - the six pre-resolution audit outputs (written by
#' export_conflict_audit() under data/processed/):
#'   - annual_return_lookup_conflict_summary.parquet
#'   - annual_return_lookup_conflict_records.parquet
#'   - annual_return_lookup_conflict_edges.parquet
#'   - annual_return_lookup_resolution_kept_edges.parquet
#'   - annual_return_lookup_resolution_dropped_edges.parquet
#'   - annual_return_lookup_conflicts.xlsx
#' The same writer serves the post-resolution audit via the path
#' remapping in post_resolution_conflict_audit_paths().
#'
#' Required packages (loaded by the calling script): dplyr, tibble,
#' tidyr, purrr, glue, logger, arrow, rio.
#'
#' Co-sourcing requirement: empty_conflict_audit() calls
#' attach_pre_resolution_components() from
#' annual_return_lookup_graph_utils.R, which in turn uses this file's
#' schema prototypes. Both files must be sourced into the same
#' environment (the numbered script does this).

# Canonical zero-row schema prototypes
############################################################

# One prototype per edge/audit table family. Every empty builder and every
# select() over these families derives from the prototypes, so zero-row and
# nonzero exports of the same logical table always share names, order, and
# arrow types. Do NOT delete the empty builders in favour of letting dplyr
# produce empties: dplyr does not reproduce these schemas on empty input
# (e.g. max() on a zero-row group returns -Inf, a double, where the schema
# declares integer).

# Kept edges of the year-constrained resolution forest.
EDGE_RESOLUTION_KEPT_PROTOTYPE <- tibble::tibble(
  component = integer(),
  year_from = integer(),
  site_id_from = character(),
  year_to = integer(),
  site_id_to = character(),
  from = character(),
  to = character(),
  match_method = character(),
  match_type = character(),
  match_level = integer(),
  join_keys = character(),
  n_keys = integer(),
  evidence_field_count = integer(),
  field_priority_score = numeric(),
  raw_score = numeric(),
  edge_priority = integer(),
  weight = numeric(),
  resolution_order = integer()
)

# Dropped edges of the year-constrained resolution forest.
EDGE_RESOLUTION_DROPPED_PROTOTYPE <- tibble::tibble(
  final_component_from = integer(),
  final_component_to = integer(),
  year_from = integer(),
  site_id_from = character(),
  year_to = integer(),
  site_id_to = character(),
  from = character(),
  to = character(),
  match_method = character(),
  match_type = character(),
  match_level = integer(),
  join_keys = character(),
  n_keys = integer(),
  evidence_field_count = integer(),
  field_priority_score = numeric(),
  raw_score = numeric(),
  edge_priority = integer(),
  weight = numeric(),
  resolution_order = integer(),
  drop_reason = character(),
  duplicate_years = character()
)

# Conflict-audit summary (one row per conflicted component-year).
CONFLICT_SUMMARY_PROTOTYPE <- tibble::tibble(
  component = integer(),
  year = integer(),
  component_year_n_site_ids = integer(),
  site_ids = character(),
  component_n_vertices = integer(),
  component_n_years = integer(),
  component_n_conflicted_years = integer(),
  component_conflicted_years = character(),
  component_max_site_ids_in_year = integer(),
  water_companies = character(),
  n_water_companies = integer(),
  n_site_name_ea = integer(),
  n_site_name_wa_sc = integer(),
  n_permit_reference_ea = integer(),
  n_permit_reference_wa_sc = integer(),
  n_activity_reference = integer(),
  n_outlet_discharge_ngr = integer(),
  site_name_ea_values = character(),
  site_name_wa_sc_values = character(),
  permit_reference_ea_values = character(),
  permit_reference_wa_sc_values = character(),
  activity_reference_values = character(),
  outlet_discharge_ngr_values = character()
)

# Conflict-audit records (one row per vertex of a conflicted component).
# vertex is part of the canonical schema: the historical zero-row builder
# omitted it while nonzero exports carried it (resolved drift).
CONFLICT_RECORDS_PROTOTYPE <- tibble::tibble(
  vertex = character(),
  component = integer(),
  year = integer(),
  site_id = character(),
  component_year_n_site_ids = integer(),
  is_conflicted_component_year = logical(),
  component_n_vertices = integer(),
  component_n_years = integer(),
  component_n_conflicted_years = integer(),
  component_conflicted_years = character(),
  component_max_site_ids_in_year = integer(),
  year_site_id = integer(),
  water_company = character(),
  site_name_ea = character(),
  site_name_wa_sc = character(),
  permit_reference_ea = character(),
  permit_reference_wa_sc = character(),
  activity_reference = character(),
  outlet_discharge_ngr = character(),
  unique_id = character(),
  unique_id_2023 = character(),
  primary_id = character()
)

# Conflict-audit edges (source edges of conflicted components).
# resolution_order is part of the canonical schema and is NA for the
# pre-resolution MST view (resolved drift).
CONFLICT_EDGES_PROTOTYPE <- tibble::tibble(
  component = integer(),
  year_from = integer(),
  site_id_from = character(),
  year_to = integer(),
  site_id_to = character(),
  match_method = character(),
  match_type = character(),
  match_level = integer(),
  join_keys = character(),
  weight = numeric(),
  n_keys = integer(),
  evidence_field_count = integer(),
  field_priority_score = numeric(),
  raw_score = numeric(),
  edge_priority = integer(),
  resolution_order = integer()
)

# Final exported edge metadata of the canonical lookup.
EDGE_METADATA_EXPORT_PROTOTYPE <- tibble::tibble(
  component = integer(),
  year_from = integer(),
  site_id_from = character(),
  year_to = integer(),
  site_id_to = character(),
  match_method = character(),
  match_type = character(),
  match_level = integer(),
  join_keys = character(),
  edge_priority = integer(),
  evidence_field_count = integer(),
  field_priority_score = numeric(),
  resolution_order = integer(),
  weight = numeric()
)

#' Conform a table to a zero-row schema prototype
#'
#' Adds any missing prototype columns as typed NA and returns the columns
#' in prototype order, so zero-row and nonzero exports of one table family
#' share identical schemas.
#' @param tbl Table to conform
#' @param prototype Zero-row prototype tibble
#' @return Tibble with exactly the prototype's columns, order, and types
conform_to_prototype <- function(tbl, prototype) {
  for (col in intersect(names(prototype), names(tbl))) {
    if (!identical(class(tbl[[col]]), class(prototype[[col]]))) {
      stop(glue::glue(
        "Column {col} has class {paste(class(tbl[[col]]), collapse = '/')} ",
        "but its prototype declares {paste(class(prototype[[col]]), collapse = '/')}; ",
        "fix the producing code rather than the prototype."
      ))
    }
  }
  for (col in setdiff(names(prototype), names(tbl))) {
    tbl[[col]] <- rep(prototype[[col]][NA_integer_], nrow(tbl))
  }
  select(tbl, all_of(names(prototype)))
}

#' Collapse unique non-missing values into a readable audit string
#' @param x Vector of values
#' @return Character scalar
collapse_unique_values <- function(x) {
  vals <- sort(unique(stats::na.omit(as.character(x))))
  if (length(vals) == 0) {
    return(NA_character_)
  }
  paste(vals, collapse = "; ")
}

#' Extract sorted reporting years from a named annual-return data list
#' @param data_list Named list with entries like df2021
#' @return Integer vector of reporting years
data_list_years <- function(data_list) {
  nm <- names(data_list)
  if (is.null(nm) || !all(grepl("^df\\d{4}$", nm))) {
    bad <- if (is.null(nm)) "<unnamed>" else paste(nm[!grepl("^df\\d{4}$", nm)], collapse = ", ")
    stop(glue::glue("data_list names must follow the dfYYYY convention; offending entries: {bad}"))
  }
  sort(as.integer(sub("^df", "", nm)))
}

#' Convert the yearly annual-return data list to an identifier lookup
#' @param data_list Named list of yearly dataframes
#' @return Tibble with one row per year-specific annual-return site ID
build_annual_identifier_lookup <- function(data_list) {
  years <- data_list_years(data_list)

  purrr::map_dfr(years, function(yr) {
    df <- data_list[[paste0("df", yr)]]
    site_col <- paste0("site_id_", yr)

    get_col <- function(col) {
      if (col %in% names(df)) {
        return(as.character(df[[col]]))
      }
      rep(NA_character_, nrow(df))
    }

    tibble(
      year = as.integer(yr),
      site_id = as.character(df[[site_col]]),
      year_site_id = suppressWarnings(as.integer(df[[site_col]])),
      water_company = get_col("water_company"),
      site_name_ea = get_col("site_name_ea"),
      site_name_wa_sc = get_col("site_name_wa_sc"),
      permit_reference_ea = get_col("permit_reference_ea"),
      permit_reference_wa_sc = get_col("permit_reference_wa_sc"),
      activity_reference = get_col("activity_reference"),
      outlet_discharge_ngr = get_col("outlet_discharge_ngr"),
      unique_id = get_col("unique_id"),
      unique_id_2023 = get_col("unique_id_2023"),
      primary_id = get_col("primary_id")
    )
  })
}


#' Build audit tables for lookup components with duplicate same-year IDs
#' @param membership_tbl Vertex membership table from the component graph
#' @param edge_metadata Component edge metadata
#' @param data_list Named list of yearly dataframes
#' @param kept_edges Constrained-resolution kept edges
#' @param dropped_edges Constrained-resolution dropped edges
#' @param resolution_component_scope Component namespace used to filter
#'   resolution edges: pre-resolution MST components or final forest components
#' @return List containing summary, records, and edge audit tables
build_lookup_conflict_audit <- function(
    membership_tbl,
    edge_metadata,
    data_list,
    kept_edges = tibble(),
    dropped_edges = tibble(),
    resolution_component_scope = c("pre", "final")) {
  resolution_component_scope <- match.arg(resolution_component_scope)

  membership_clean <- membership_tbl %>%
    mutate(
      year = as.integer(year),
      site_id = as.character(site_id)
    )

  conflict_counts <- membership_clean %>%
    count(component, year, name = "component_year_n_site_ids") %>%
    filter(component_year_n_site_ids > 1)

  if (nrow(conflict_counts) == 0) {
    return(list(
      summary = CONFLICT_SUMMARY_PROTOTYPE,
      records = CONFLICT_RECORDS_PROTOTYPE,
      edges = CONFLICT_EDGES_PROTOTYPE,
      kept_edges = kept_edges %>% slice(0),
      dropped_edges = dropped_edges %>% slice(0)
    ))
  }

  conflicted_components <- distinct(conflict_counts, component)

  component_context <- membership_clean %>%
    semi_join(conflicted_components, by = "component") %>%
    group_by(component) %>%
    summarise(
      component_n_vertices = n(),
      component_n_years = n_distinct(year),
      .groups = "drop"
    ) %>%
    left_join(
      conflict_counts %>%
        group_by(component) %>%
        summarise(
          component_n_conflicted_years = n(),
          component_conflicted_years = paste(sort(unique(year)), collapse = "; "),
          component_max_site_ids_in_year = max(component_year_n_site_ids),
          .groups = "drop"
        ),
      by = "component"
    )

  annual_identifiers <- build_annual_identifier_lookup(data_list)

  conflict_records <- membership_clean %>%
    semi_join(conflicted_components, by = "component") %>%
    left_join(conflict_counts, by = c("component", "year")) %>%
    mutate(
      component_year_n_site_ids = tidyr::replace_na(
        component_year_n_site_ids,
        1L
      ),
      is_conflicted_component_year = component_year_n_site_ids > 1
    ) %>%
    left_join(component_context, by = "component") %>%
    left_join(annual_identifiers, by = c("year", "site_id")) %>%
    arrange(component, year, year_site_id) %>%
    conform_to_prototype(CONFLICT_RECORDS_PROTOTYPE)

  conflict_summary <- conflict_records %>%
    filter(is_conflicted_component_year) %>%
    group_by(component, year) %>%
    summarise(
      component_year_n_site_ids = first(component_year_n_site_ids),
      site_ids = paste(sort(unique(site_id)), collapse = "; "),
      component_n_vertices = first(component_n_vertices),
      component_n_years = first(component_n_years),
      component_n_conflicted_years = first(component_n_conflicted_years),
      component_conflicted_years = first(component_conflicted_years),
      component_max_site_ids_in_year = first(component_max_site_ids_in_year),
      water_companies = collapse_unique_values(water_company),
      n_water_companies = n_distinct(water_company, na.rm = TRUE),
      n_site_name_ea = n_distinct(site_name_ea, na.rm = TRUE),
      n_site_name_wa_sc = n_distinct(site_name_wa_sc, na.rm = TRUE),
      n_permit_reference_ea = n_distinct(permit_reference_ea, na.rm = TRUE),
      n_permit_reference_wa_sc = n_distinct(permit_reference_wa_sc, na.rm = TRUE),
      n_activity_reference = n_distinct(activity_reference, na.rm = TRUE),
      n_outlet_discharge_ngr = n_distinct(outlet_discharge_ngr, na.rm = TRUE),
      site_name_ea_values = collapse_unique_values(site_name_ea),
      site_name_wa_sc_values = collapse_unique_values(site_name_wa_sc),
      permit_reference_ea_values = collapse_unique_values(permit_reference_ea),
      permit_reference_wa_sc_values = collapse_unique_values(permit_reference_wa_sc),
      activity_reference_values = collapse_unique_values(activity_reference),
      outlet_discharge_ngr_values = collapse_unique_values(outlet_discharge_ngr),
      .groups = "drop"
    ) %>%
    arrange(component, year) %>%
    conform_to_prototype(CONFLICT_SUMMARY_PROTOTYPE)

  conflict_edges <- edge_metadata %>%
    semi_join(conflicted_components, by = "component") %>%
    mutate(
      year_from = as.integer(year_from),
      year_to = as.integer(year_to)
    ) %>%
    arrange(
      component,
      year_from,
      suppressWarnings(as.integer(site_id_from)),
      year_to,
      suppressWarnings(as.integer(site_id_to))
    ) %>%
    conform_to_prototype(CONFLICT_EDGES_PROTOTYPE)

  # Explicit dispatch on the component namespace: the pre-resolution view
  # filters on pre_component; the final view filters kept edges on
  # component and dropped edges on their final_component endpoints. Any
  # other shape is a caller bug and stops loudly.
  filter_resolution_edges <- function(edge_tbl) {
    if (nrow(edge_tbl) == 0) {
      return(edge_tbl)
    }

    if (resolution_component_scope == "pre") {
      if (!"pre_component" %in% names(edge_tbl)) {
        stop("Pre-resolution edge filtering requires a pre_component column.")
      }
      return(edge_tbl %>%
        filter(pre_component %in% conflicted_components$component) %>%
        arrange(pre_component, resolution_order))
    }

    if ("component" %in% names(edge_tbl)) {
      return(edge_tbl %>%
        semi_join(conflicted_components, by = "component") %>%
        arrange(component, resolution_order))
    }

    if (all(c("final_component_from", "final_component_to") %in% names(edge_tbl))) {
      return(edge_tbl %>%
        filter(
          final_component_from %in% conflicted_components$component |
            final_component_to %in% conflicted_components$component
        ) %>%
        arrange(final_component_from, final_component_to, resolution_order))
    }

    stop(
      "Final-resolution edge filtering requires a component or ",
      "final_component_from/final_component_to columns."
    )
  }

  list(
    summary = conflict_summary,
    records = conflict_records,
    edges = conflict_edges,
    kept_edges = filter_resolution_edges(kept_edges),
    dropped_edges = filter_resolution_edges(dropped_edges)
  )
}

#' Return an all-empty conflict audit built from the canonical prototypes
#'
#' Used by zero-edge and zero-match runs so the caller can still refresh
#' every audit file on disk with zero-row tables.
#' @return List shaped like build_lookup_conflict_audit() output
empty_conflict_audit <- function() {
  list(
    summary = CONFLICT_SUMMARY_PROTOTYPE,
    records = CONFLICT_RECORDS_PROTOTYPE,
    edges = CONFLICT_EDGES_PROTOTYPE,
    kept_edges = attach_pre_resolution_components(
      EDGE_RESOLUTION_KEPT_PROTOTYPE, tibble()
    ),
    dropped_edges = attach_pre_resolution_components(
      EDGE_RESOLUTION_DROPPED_PROTOTYPE, tibble()
    )
  )
}

#' List the files export_conflict_audit() writes for a given path set
#' @param paths Configuration list with audit output paths
#' @return Character vector of file paths
conflict_audit_files <- function(paths) {
  c(
    paths$conflict_summary_parquet,
    paths$conflict_records_parquet,
    paths$conflict_edges_parquet,
    paths$resolution_kept_edges_parquet,
    paths$resolution_dropped_edges_parquet,
    paths$conflict_excel_output
  )
}

#' Remove stale post-resolution conflict-audit files after a healthy run
#'
#' Post-resolution diagnostics exist only when the final safety net has
#' tripped; a successful run deletes leftovers from earlier failed runs
#' instead of writing empty ones.
#' @param paths Configuration list with audit output paths
#' @return Invisibly the removed file paths
clear_post_resolution_conflict_audit <- function(paths) {
  files <- conflict_audit_files(post_resolution_conflict_audit_paths(paths))
  existing <- files[file.exists(files)]
  if (length(existing) > 0) {
    removed <- suppressWarnings(file.remove(existing))
    if (!all(removed)) {
      stop(glue::glue(
        "Could not remove stale post-resolution conflict-audit files: ",
        "{paste(basename(existing[!removed]), collapse = ', ')}. ",
        "Their presence would wrongly signal a tripped safety net - ",
        "remove them manually (close any program holding them open) and rerun."
      ))
    }
    logger::log_info(
      "Removed {length(existing)} stale post-resolution conflict-audit files"
    )
  }
  invisible(existing)
}

#' Export annual-return lookup conflict audit tables
#' @param conflict_audit List from build_lookup_conflict_audit()
#' @param paths Configuration list with audit output paths
#' @return Invisibly TRUE after files are written
export_conflict_audit <- function(conflict_audit, paths) {
  dir.create(paths$output_dir, recursive = TRUE, showWarnings = FALSE)

  arrow::write_parquet(conflict_audit$summary, paths$conflict_summary_parquet)
  arrow::write_parquet(conflict_audit$records, paths$conflict_records_parquet)
  arrow::write_parquet(conflict_audit$edges, paths$conflict_edges_parquet)
  arrow::write_parquet(
    conflict_audit$kept_edges,
    paths$resolution_kept_edges_parquet
  )
  arrow::write_parquet(
    conflict_audit$dropped_edges,
    paths$resolution_dropped_edges_parquet
  )

  rio::export(
    list(
      summary = conflict_audit$summary,
      records = conflict_audit$records,
      edges = conflict_audit$edges,
      kept_edges = conflict_audit$kept_edges,
      dropped_edges = conflict_audit$dropped_edges
    ),
    paths$conflict_excel_output
  )

  if (nrow(conflict_audit$summary) == 0) {
    logger::log_info(
      "Wrote empty annual-return lookup conflict audit to {paths$conflict_excel_output}"
    )
  } else {
    logger::log_warn(
      "Wrote annual-return lookup conflict audit to {paths$conflict_excel_output}"
    )
  }
  invisible(TRUE)
}

#' Return output paths for post-resolution conflict audit files
#' @param paths Configuration list
#' @return Configuration list with conflict-audit paths remapped
post_resolution_conflict_audit_paths <- function(paths) {
  utils::modifyList(paths, list(
    conflict_summary_parquet = paths$post_resolution_conflict_summary_parquet,
    conflict_records_parquet = paths$post_resolution_conflict_records_parquet,
    conflict_edges_parquet = paths$post_resolution_conflict_edges_parquet,
    resolution_kept_edges_parquet = paths$post_resolution_kept_edges_parquet,
    resolution_dropped_edges_parquet = paths$post_resolution_dropped_edges_parquet,
    conflict_excel_output = paths$post_resolution_conflict_excel_output
  ))
}

#' Stop lookup construction when ambiguous same-year component conflicts exist
#' @param conflict_audit List from build_lookup_conflict_audit()
#' @param audit_path Path to the conflict-audit workbook written for this audit
#' @param written_files Paths of the diagnostic files written for this audit;
#'   when supplied, the stop message names each file
#' @return Invisible NULL when no conflicts exist
stop_if_lookup_conflicts <- function(
    conflict_audit,
    audit_path,
    written_files = character(0)) {
  n_component_years <- nrow(conflict_audit$summary)
  if (n_component_years == 0) {
    return(invisible(NULL))
  }

  n_components <- n_distinct(conflict_audit$summary$component)
  logger::log_error(
    "Annual-return lookup has {n_components} conflicted components across {n_component_years} component-years"
  )
  file_clause <- if (length(written_files) > 0) {
    glue::glue(
      "Diagnostic files written to {dirname(audit_path)}: ",
      "{paste(basename(written_files), collapse = ', ')}. "
    )
  } else {
    glue::glue(
      "Conflict audit files were written to {dirname(audit_path)} ",
      "(workbook: {basename(audit_path)}). "
    )
  }
  stop(glue::glue(
    "Annual-return lookup has {n_components} conflicted components ",
    "across {n_component_years} component-years. ",
    file_clause,
    "Resolve or explicitly split these components before refreshing the canonical lookup."
  ))
}

#' Return an empty lookup table with the canonical year columns
#' @param years Reporting years included in the lookup
#' @return Empty lookup tibble
empty_lookup_table <- function(years) {
  lookup_tbl <- tibble(component = integer())
  for (site_col in paste0("site_id_", years)) {
    lookup_tbl[[site_col]] <- character()
  }
  lookup_tbl
}

#' Return an empty edge metadata table with the export schema
#' @return Empty edge metadata tibble
empty_edge_metadata <- function() {
  EDGE_METADATA_EXPORT_PROTOTYPE
}
