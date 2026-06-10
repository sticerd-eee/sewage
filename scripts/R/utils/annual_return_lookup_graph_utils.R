############################################################
# Annual Return Lookup: Graph-Resolution Utilities
# Project: Sewage
# Date: 10/06/2026
# Author: Jacopo Olivieri
############################################################

#' Graph machinery for the annual-return lookup builder
#' (scripts/R/03_data_enrichment/create_annual_return_lookup.R).
#'
#' Defines functions only - no file reads or writes happen here.
#'
#' Inputs (passed as arguments by the caller):
#'   - candidate edge tables (from/to vertices like "2024_121" plus
#'     match metadata and edge weights)
#'   - vertex component-membership tables
#'
#' Provides the three-view seams of the lookup design:
#'   - build_unconstrained_mst_components(): legacy MST diagnostic view
#'   - build_year_constrained_spanning_forest(): canonical lookup forest
#'     (bitmask year-membership union-find; do not rewrite as joins -
#'     a join-based variant was tried historically and was too slow)
#'   - attach_pre_resolution_components(): bridges both views for audits
#'
#' Required packages (loaded by the calling script): dplyr, tibble,
#' stringr, igraph, stats.

#' Parse the reporting year from a year-specific graph vertex
#' @param vertex Character vector like "2024_121"
#' @return Integer vector of years
vertex_year <- function(vertex) {
  as.integer(stringr::str_extract(vertex, "^\\d{4}"))
}

#' Parse the annual-return site ID from a year-specific graph vertex
#' @param vertex Character vector like "2024_121"
#' @return Character vector of site IDs
vertex_site_id <- function(vertex) {
  stringr::str_remove(vertex, "^\\d{4}_")
}

#' Attach component and year/site diagnostics to an edge table
#' @param edge_tbl Edge table with from/to columns
#' @param membership_tbl Vertex component membership table
#' @return Edge table with component and parsed endpoint columns
attach_edge_components <- function(edge_tbl, membership_tbl) {
  if (nrow(edge_tbl) == 0) {
    return(edge_tbl %>%
      mutate(
        component = integer(),
        year_from = integer(),
        site_id_from = character(),
        year_to = integer(),
        site_id_to = character()
      ))
  }

  component_lookup <- membership_tbl %>%
    select(vertex, component)

  edge_tbl %>%
    left_join(component_lookup, by = c("from" = "vertex")) %>%
    rename(component_from = component) %>%
    left_join(component_lookup, by = c("to" = "vertex")) %>%
    rename(component_to = component) %>%
    mutate(
      component = dplyr::if_else(
        component_from == component_to,
        component_from,
        NA_integer_
      ),
      year_from = vertex_year(from),
      site_id_from = vertex_site_id(from),
      year_to = vertex_year(to),
      site_id_to = vertex_site_id(to)
    )
}

#' Keep the legacy unconstrained MST view for pre-resolution conflict audit
#' @param edges_df Candidate edge table
#' @return List with membership_tbl and edge_metadata
build_unconstrained_mst_components <- function(edges_df) {
  empty_membership <- tibble(
    vertex = character(),
    component = integer(),
    year = integer(),
    site_id = character()
  )

  if (nrow(edges_df) == 0) {
    return(list(
      membership_tbl = empty_membership,
      edge_metadata = tibble()
    ))
  }

  g <- igraph::graph_from_data_frame(edges_df, directed = FALSE)
  g_mst <- igraph::mst(g, weights = -igraph::E(g)$weight)
  comps <- igraph::components(g_mst)

  membership_tbl <- tibble(
    vertex = names(comps$membership),
    component = as.integer(comps$membership),
    year = vertex_year(vertex),
    site_id = vertex_site_id(vertex)
  )

  edge_metadata <- igraph::as_data_frame(g_mst, what = "edges") %>%
    attach_edge_components(membership_tbl) %>%
    select(
      component, year_from, site_id_from, year_to, site_id_to,
      match_method, match_type, match_level, join_keys, weight,
      any_of(c(
        "n_keys", "evidence_field_count", "field_priority_score",
        "raw_score", "edge_priority"
      ))
    )

  list(
    membership_tbl = membership_tbl,
    edge_metadata = edge_metadata
  )
}

#' Build a deterministic maximum spanning forest subject to one row per year
#' @param edges_df Candidate edge table with from/to and edge weights
#' @return List with kept_edges, dropped_edges, and membership_tbl
build_year_constrained_spanning_forest <- function(edges_df) {
  empty_membership <- tibble(
    vertex = character(),
    component = integer(),
    year = integer(),
    site_id = character()
  )

  if (nrow(edges_df) == 0) {
    return(list(
      kept_edges = tibble(),
      dropped_edges = tibble(),
      membership_tbl = empty_membership
    ))
  }

  ordered_edges <- edges_df %>%
    mutate(
      edge_sort_from = pmin(from, to),
      edge_sort_to = pmax(from, to)
    ) %>%
    arrange(
      desc(edge_priority),
      desc(evidence_field_count),
      desc(field_priority_score),
      match_level,
      desc(weight),
      edge_sort_from, edge_sort_to, join_keys, match_method, match_type
    ) %>%
    mutate(resolution_order = row_number()) %>%
    select(-edge_sort_from, -edge_sort_to)

  vertices <- sort(unique(c(ordered_edges$from, ordered_edges$to)))
  vertex_ids <- stats::setNames(seq_along(vertices), vertices)
  edge_from_ids <- unname(vertex_ids[ordered_edges$from])
  edge_to_ids <- unname(vertex_ids[ordered_edges$to])

  vertex_years <- vertex_year(vertices)
  year_levels <- sort(unique(vertex_years))
  year_bits <- stats::setNames(
    bitwShiftL(1L, seq_along(year_levels) - 1L),
    as.character(year_levels)
  )

  parent <- seq_along(vertices)
  component_year_mask <- as.integer(year_bits[as.character(vertex_years)])

  find_root <- function(vertex_id) {
    root <- vertex_id
    while (parent[root] != root) {
      parent[root] <<- parent[parent[root]]
      root <- parent[root]
    }
    root
  }

  union_roots <- function(root_a, root_b) {
    if (vertices[root_a] <= vertices[root_b]) {
      new_root <- root_a
      old_root <- root_b
    } else {
      new_root <- root_b
      old_root <- root_a
    }

    parent[old_root] <<- new_root
    component_year_mask[new_root] <<- bitwOr(
      component_year_mask[root_a],
      component_year_mask[root_b]
    )
    new_root
  }

  kept_idx <- integer(nrow(ordered_edges))
  dropped_idx <- integer(nrow(ordered_edges))
  dropped_reasons <- character(nrow(ordered_edges))
  dropped_duplicate_years <- character(nrow(ordered_edges))
  kept_n <- 0L
  dropped_n <- 0L

  for (i in seq_len(nrow(ordered_edges))) {
    root_from <- find_root(edge_from_ids[i])
    root_to <- find_root(edge_to_ids[i])

    if (root_from == root_to) {
      dropped_n <- dropped_n + 1L
      dropped_idx[dropped_n] <- i
      dropped_reasons[dropped_n] <- "redundant_within_component"
      dropped_duplicate_years[dropped_n] <- NA_character_
      next
    }

    duplicate_year_mask <- bitwAnd(
      component_year_mask[root_from],
      component_year_mask[root_to]
    )

    if (duplicate_year_mask != 0L) {
      dropped_n <- dropped_n + 1L
      dropped_idx[dropped_n] <- i
      dropped_reasons[dropped_n] <- "duplicate_year_component"
      dropped_duplicate_years[dropped_n] <- paste(
        year_levels[bitwAnd(duplicate_year_mask, year_bits) != 0L],
        collapse = "; "
      )
      next
    }

    kept_n <- kept_n + 1L
    kept_idx[kept_n] <- i
    union_roots(root_from, root_to)
  }

  membership_tbl <- tibble(
    vertex = vertices,
    root_id = vapply(seq_along(vertices), find_root, integer(1))
  )

  component_map <- membership_tbl %>%
    group_by(root_id) %>%
    summarise(component_key = min(vertex), .groups = "drop") %>%
    arrange(component_key) %>%
    mutate(component = row_number()) %>%
    select(root_id, component)

  membership_tbl <- membership_tbl %>%
    left_join(component_map, by = "root_id") %>%
    transmute(
      vertex,
      component,
      year = vertex_year(vertex),
      site_id = vertex_site_id(vertex)
    )

  empty_kept_edges <- tibble(
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

  empty_dropped_edges <- tibble(
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

  kept_edges <- if (kept_n > 0) {
    ordered_edges[kept_idx[seq_len(kept_n)], , drop = FALSE] %>%
      attach_edge_components(membership_tbl) %>%
      select(
        component, year_from, site_id_from, year_to, site_id_to,
        from, to, match_method, match_type, match_level, join_keys,
        n_keys, evidence_field_count, field_priority_score,
        raw_score, edge_priority, weight, resolution_order
      )
  } else {
    empty_kept_edges
  }

  dropped_edges <- if (dropped_n > 0) {
    ordered_edges[dropped_idx[seq_len(dropped_n)], , drop = FALSE] %>%
      mutate(
        drop_reason = dropped_reasons[seq_len(dropped_n)],
        duplicate_years = dropped_duplicate_years[seq_len(dropped_n)]
      )
  } else {
    empty_dropped_edges
  }

  if (nrow(dropped_edges) > 0) {
    component_lookup <- membership_tbl %>%
      select(vertex, component)

    dropped_edges <- dropped_edges %>%
      left_join(component_lookup, by = c("from" = "vertex")) %>%
      rename(final_component_from = component) %>%
      left_join(component_lookup, by = c("to" = "vertex")) %>%
      rename(final_component_to = component) %>%
      mutate(
        year_from = vertex_year(from),
        site_id_from = vertex_site_id(from),
        year_to = vertex_year(to),
        site_id_to = vertex_site_id(to)
      ) %>%
      select(
        final_component_from, final_component_to,
        year_from, site_id_from, year_to, site_id_to,
        from, to, match_method, match_type, match_level, join_keys,
        n_keys, evidence_field_count, field_priority_score,
        raw_score, edge_priority, weight, resolution_order,
        drop_reason, duplicate_years
      )
  }

  list(
    kept_edges = kept_edges,
    dropped_edges = dropped_edges,
    membership_tbl = membership_tbl
  )
}

#' Attach pre-resolution MST component IDs to constrained resolution edges
#' @param edge_tbl Kept or dropped edge table from constrained resolution
#' @param pre_membership_tbl Membership table from the unconstrained MST
#' @return Edge table with pre_component diagnostics
attach_pre_resolution_components <- function(edge_tbl, pre_membership_tbl) {
  if (nrow(edge_tbl) == 0) {
    return(edge_tbl %>%
      mutate(
        pre_component = integer(),
        pre_component_from = integer(),
        pre_component_to = integer()
      ) %>%
      select(pre_component, pre_component_from, pre_component_to, everything()))
  }

  pre_lookup <- pre_membership_tbl %>%
    select(vertex, pre_component = component)

  edge_tbl %>%
    left_join(pre_lookup, by = c("from" = "vertex")) %>%
    rename(pre_component_from = pre_component) %>%
    left_join(pre_lookup, by = c("to" = "vertex")) %>%
    rename(pre_component_to = pre_component) %>%
    mutate(
      pre_component = dplyr::if_else(
        pre_component_from == pre_component_to,
        pre_component_from,
        NA_integer_
      )
    ) %>%
    select(pre_component, pre_component_from, pre_component_to, everything())
}
