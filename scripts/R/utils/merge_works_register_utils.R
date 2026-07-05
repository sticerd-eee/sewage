############################################################
# Merge Works Register Utilities
# Project: Sewage
############################################################

#' Utilities for constructing a year-invariant works register over canonical
#' annual-return lookup site IDs.
#'
#' Defines functions only. File reads, writes, and logging are owned by the
#' orchestrating script.

WORKS_REGISTER_MEMBERSHIP_PROTOTYPE <- tibble::tibble(
  site_id = integer(),
  member_site_id = integer(),
  water_company = character(),
  component_id = integer(),
  n_members = integer(),
  site_id_members = character()
)

WORKS_REGISTER_RESOLVER_PROTOTYPE <- tibble::tibble(
  annual_row_id = integer(),
  site_id = integer(),
  member_site_id = integer(),
  canonical_site_id = integer(),
  water_company = character(),
  year = integer(),
  annual_site_id = integer(),
  site_name_ea = character(),
  site_name_ea_norm = character(),
  site_name_wa_sc = character(),
  site_name_wa_sc_norm = character(),
  permit_reference_ea = character(),
  permit_reference_ea_norm = character(),
  permit_reference_wa_sc = character(),
  permit_reference_wa_sc_norm = character(),
  activity_reference = character(),
  activity_reference_norm = character(),
  unique_id = character(),
  unique_id_norm = character(),
  outlet_discharge_ngr = character(),
  ngr_clean = character(),
  easting = double(),
  northing = double(),
  spill_hrs_ea = double(),
  spill_count_ea = double()
)

WORKS_REGISTER_EDGE_PROTOTYPE <- tibble::tibble(
  water_company = character(),
  site_name_ea_norm = character(),
  site_id_from = integer(),
  site_id_to = integer(),
  justification = character(),
  shared_permit_reference_ea = character(),
  distance_m = double()
)

WORKS_REGISTER_NEAR_MISS_PROTOTYPE <- tibble::tibble(
  water_company = character(),
  site_name_ea_norm = character(),
  site_id_from = integer(),
  site_id_to = integer(),
  distance_m = double(),
  reason = character()
)

normalise_works_text <- function(x) {
  x <- as.character(x)
  x <- stringr::str_squish(stringr::str_to_upper(stringr::str_trim(x)))
  x[x %in% c("", "TBC", "N/A", "NA")] <- NA_character_
  x
}

normalise_register_permit <- function(x) {
  x <- as.character(x)
  x <- stringr::str_squish(stringr::str_to_upper(stringr::str_trim(x)))
  x[x %in% c("", "TBC", "N/A", "NA")] <- NA_character_
  x
}

annual_lookup_long <- function(lookup_tbl) {
  if (is.null(lookup_tbl) || nrow(lookup_tbl) == 0) {
    return(tibble::tibble(
      canonical_site_id = integer(),
      year = integer(),
      annual_site_id = integer()
    ))
  }

  lookup_tbl %>%
    tidyr::pivot_longer(
      cols = tidyselect::starts_with("site_id_"),
      names_to = "year_col",
      values_to = "annual_site_id"
    ) %>%
    dplyr::mutate(
      canonical_site_id = as.integer(.data$site_id),
      year = as.integer(stringr::str_remove(.data$year_col, "^site_id_")),
      annual_site_id = as.integer(.data$annual_site_id)
    ) %>%
    dplyr::filter(!is.na(.data$annual_site_id)) %>%
    dplyr::select("canonical_site_id", "year", "annual_site_id")
}

attach_annual_site_id <- function(annual_rows) {
  if ("annual_site_id" %in% names(annual_rows)) {
    return(dplyr::mutate(annual_rows, annual_site_id = as.integer(.data$annual_site_id)))
  }

  annual_rows %>%
    dplyr::mutate(
      annual_site_id = dplyr::case_when(
        .data$year == 2021L ~ as.integer(.data$site_id_2021),
        .data$year == 2022L ~ as.integer(.data$site_id_2022),
        .data$year == 2023L ~ as.integer(.data$site_id_2023),
        .data$year == 2024L ~ as.integer(.data$site_id_2024),
        TRUE ~ NA_integer_
      )
    )
}

prepare_works_register_annual_rows <- function(annual_rows, lookup_tbl = NULL) {
  if (nrow(annual_rows) == 0) {
    return(WORKS_REGISTER_RESOLVER_PROTOTYPE %>%
      dplyr::select(-"site_id", -"member_site_id"))
  }

  rows <- annual_rows %>%
    attach_annual_site_id() %>%
    dplyr::mutate(
      annual_row_id = dplyr::row_number(),
      year = as.integer(.data$year)
    )

  if (!"canonical_site_id" %in% names(rows)) {
    if (is.null(lookup_tbl)) {
      stop(
        "lookup_tbl is required when annual_rows lacks canonical_site_id.",
        call. = FALSE
      )
    }

    lookup_long <- annual_lookup_long(lookup_tbl)
    rows <- rows %>%
      dplyr::left_join(lookup_long, by = c("year", "annual_site_id"))

    missing_links <- rows %>%
      dplyr::filter(is.na(.data$canonical_site_id))
    if (nrow(missing_links) > 0) {
      stop(
        "Annual rows without lookup links: ", nrow(missing_links),
        call. = FALSE
      )
    }
  }

  if (!"site_name_wa_sc" %in% names(rows)) rows$site_name_wa_sc <- NA_character_
  if (!"permit_reference_wa_sc" %in% names(rows)) rows$permit_reference_wa_sc <- NA_character_
  if (!"activity_reference" %in% names(rows)) rows$activity_reference <- NA_character_
  if (!"unique_id" %in% names(rows)) rows$unique_id <- NA_character_
  if (!"outlet_discharge_ngr" %in% names(rows)) rows$outlet_discharge_ngr <- NA_character_
  if (!"permit_reference_ea" %in% names(rows)) rows$permit_reference_ea <- NA_character_
  if (!"spill_hrs_ea" %in% names(rows)) rows$spill_hrs_ea <- NA_real_
  if (!"spill_count_ea" %in% names(rows)) rows$spill_count_ea <- NA_real_

  coords <- parse_bng_coordinates(clean_ngr(rows$outlet_discharge_ngr))

  rows %>%
    dplyr::mutate(
      canonical_site_id = as.integer(.data$canonical_site_id),
      water_company = as.character(.data$water_company),
      site_name_ea = as.character(.data$site_name_ea),
      site_name_ea_norm = normalise_works_text(.data$site_name_ea),
      site_name_wa_sc = as.character(.data$site_name_wa_sc),
      site_name_wa_sc_norm = normalise_works_text(.data$site_name_wa_sc),
      permit_reference_ea = as.character(.data$permit_reference_ea),
      permit_reference_ea_norm =
        normalise_register_permit(.data$permit_reference_ea),
      permit_reference_wa_sc = as.character(.data$permit_reference_wa_sc),
      permit_reference_wa_sc_norm =
        normalise_register_permit(.data$permit_reference_wa_sc),
      activity_reference = as.character(.data$activity_reference),
      activity_reference_norm =
        normalise_register_permit(.data$activity_reference),
      unique_id = as.character(.data$unique_id),
      unique_id_norm = normalise_register_permit(.data$unique_id),
      outlet_discharge_ngr = as.character(.data$outlet_discharge_ngr),
      ngr_clean = clean_ngr(.data$outlet_discharge_ngr),
      easting = coords$easting,
      northing = coords$northing,
      spill_hrs_ea = as.numeric(.data$spill_hrs_ea),
      spill_count_ea = as.numeric(.data$spill_count_ea)
    ) %>%
    dplyr::select(
      "annual_row_id",
      "canonical_site_id",
      "water_company",
      "year",
      "annual_site_id",
      "site_name_ea",
      "site_name_ea_norm",
      "site_name_wa_sc",
      "site_name_wa_sc_norm",
      "permit_reference_ea",
      "permit_reference_ea_norm",
      "permit_reference_wa_sc",
      "permit_reference_wa_sc_norm",
      "activity_reference",
      "activity_reference_norm",
      "unique_id",
      "unique_id_norm",
      "outlet_discharge_ngr",
      "ngr_clean",
      "easting",
      "northing",
      "spill_hrs_ea",
      "spill_count_ea"
    )
}

site_pair_metrics <- function(left_rows, right_rows) {
  left_permits <- unique(stats::na.omit(left_rows$permit_reference_ea_norm))
  right_permits <- unique(stats::na.omit(right_rows$permit_reference_ea_norm))
  shared_permits <- sort(intersect(left_permits, right_permits))

  left_coords <- left_rows %>%
    dplyr::filter(!is.na(.data$easting), !is.na(.data$northing)) %>%
    dplyr::distinct(.data$easting, .data$northing)
  right_coords <- right_rows %>%
    dplyr::filter(!is.na(.data$easting), !is.na(.data$northing)) %>%
    dplyr::distinct(.data$easting, .data$northing)

  min_distance <- NA_real_
  if (nrow(left_coords) > 0 && nrow(right_coords) > 0) {
    distances <- as.matrix(stats::dist(rbind(left_coords, right_coords)))
    left_idx <- seq_len(nrow(left_coords))
    right_idx <- nrow(left_coords) + seq_len(nrow(right_coords))
    min_distance <- min(distances[left_idx, right_idx, drop = FALSE])
  }

  list(
    shared_permit_reference_ea = if (length(shared_permits) > 0) {
      paste(shared_permits, collapse = "; ")
    } else {
      NA_character_
    },
    has_permit_match = length(shared_permits) > 0,
    distance_m = min_distance
  )
}

build_register_pairs_for_name <- function(group_rows, config) {
  site_ids <- sort(unique(group_rows$canonical_site_id))
  if (length(site_ids) < 2L) {
    return(list(
      edges = WORKS_REGISTER_EDGE_PROTOTYPE,
      near_misses = WORKS_REGISTER_NEAR_MISS_PROTOTYPE
    ))
  }

  pair_grid <- utils::combn(site_ids, 2L, simplify = FALSE)
  edge_rows <- vector("list", length(pair_grid))
  near_rows <- vector("list", length(pair_grid))

  for (i in seq_along(pair_grid)) {
    pair <- pair_grid[[i]]
    left <- group_rows %>% dplyr::filter(.data$canonical_site_id == pair[[1L]])
    right <- group_rows %>% dplyr::filter(.data$canonical_site_id == pair[[2L]])
    metrics <- site_pair_metrics(left, right)

    has_ngr_match <- !is.na(metrics$distance_m) &&
      metrics$distance_m <= config$works_merge_ngr_m
    has_near_miss <- !is.na(metrics$distance_m) &&
      metrics$distance_m > config$works_merge_ngr_m &&
      metrics$distance_m <= config$works_near_miss_m

    if (metrics$has_permit_match || has_ngr_match) {
      justification <- dplyr::case_when(
        metrics$has_permit_match && has_ngr_match ~ "permit_reference_ea_and_ngr",
        metrics$has_permit_match ~ "permit_reference_ea",
        TRUE ~ "ngr_distance"
      )
      edge_rows[[i]] <- tibble::tibble(
        water_company = group_rows$water_company[[1L]],
        site_name_ea_norm = group_rows$site_name_ea_norm[[1L]],
        site_id_from = as.integer(pair[[1L]]),
        site_id_to = as.integer(pair[[2L]]),
        justification = justification,
        shared_permit_reference_ea = metrics$shared_permit_reference_ea,
        distance_m = metrics$distance_m
      )
    } else if (has_near_miss) {
      near_rows[[i]] <- tibble::tibble(
        water_company = group_rows$water_company[[1L]],
        site_name_ea_norm = group_rows$site_name_ea_norm[[1L]],
        site_id_from = as.integer(pair[[1L]]),
        site_id_to = as.integer(pair[[2L]]),
        distance_m = metrics$distance_m,
        reason = "same_name_distance_250m_to_1km"
      )
    }
  }

  list(
    edges = dplyr::bind_rows(edge_rows) %>%
      conform_works_table(WORKS_REGISTER_EDGE_PROTOTYPE),
    near_misses = dplyr::bind_rows(near_rows) %>%
      conform_works_table(WORKS_REGISTER_NEAR_MISS_PROTOTYPE)
  )
}

conform_works_table <- function(tbl, prototype) {
  if (is.null(tbl) || nrow(tbl) == 0) {
    return(prototype)
  }

  missing_cols <- setdiff(names(prototype), names(tbl))
  for (col in missing_cols) {
    tbl[[col]] <- prototype[[col]][NA_integer_][1]
  }

  tbl %>%
    dplyr::select(dplyr::all_of(names(prototype))) %>%
    dplyr::mutate(
      dplyr::across(
        names(prototype),
        ~ vctrs::vec_cast(.x, prototype[[dplyr::cur_column()]])
      )
    )
}

build_works_register_edges <- function(prepared_annual_rows, config) {
  if (nrow(prepared_annual_rows) == 0) {
    return(list(
      edges = WORKS_REGISTER_EDGE_PROTOTYPE,
      near_misses = WORKS_REGISTER_NEAR_MISS_PROTOTYPE
    ))
  }

  pair_results <- prepared_annual_rows %>%
    dplyr::filter(!is.na(.data$site_name_ea_norm)) %>%
    dplyr::group_by(.data$water_company, .data$site_name_ea_norm) %>%
    dplyr::group_split() %>%
    purrr::map(build_register_pairs_for_name, config = config)

  list(
    edges = purrr::map(pair_results, "edges") %>%
      dplyr::bind_rows() %>%
      dplyr::distinct() %>%
      dplyr::arrange(
        .data$water_company, .data$site_name_ea_norm,
        .data$site_id_from, .data$site_id_to, .data$justification
      ) %>%
      conform_works_table(WORKS_REGISTER_EDGE_PROTOTYPE),
    near_misses = purrr::map(pair_results, "near_misses") %>%
      dplyr::bind_rows() %>%
      dplyr::distinct() %>%
      dplyr::arrange(
        .data$water_company, .data$site_name_ea_norm,
        .data$site_id_from, .data$site_id_to
      ) %>%
      conform_works_table(WORKS_REGISTER_NEAR_MISS_PROTOTYPE)
  )
}

build_works_membership <- function(nodes, edges) {
  if (nrow(nodes) == 0) {
    return(WORKS_REGISTER_MEMBERSHIP_PROTOTYPE)
  }

  vertices <- sort(unique(nodes$canonical_site_id))
  parent <- stats::setNames(vertices, as.character(vertices))

  find_root <- function(x) {
    key <- as.character(x)
    while (parent[[key]] != as.integer(key)) {
      parent[[key]] <<- parent[[as.character(parent[[key]])]]
      key <- as.character(parent[[key]])
    }
    as.integer(parent[[key]])
  }

  union_pair <- function(a, b) {
    root_a <- find_root(a)
    root_b <- find_root(b)
    if (root_a == root_b) return(invisible(root_a))
    new_root <- min(root_a, root_b)
    old_root <- max(root_a, root_b)
    parent[[as.character(old_root)]] <<- new_root
    invisible(new_root)
  }

  if (nrow(edges) > 0) {
    for (i in seq_len(nrow(edges))) {
      union_pair(edges$site_id_from[[i]], edges$site_id_to[[i]])
    }
  }

  raw_membership <- nodes %>%
    dplyr::distinct(.data$canonical_site_id, .data$water_company) %>%
    dplyr::mutate(root_id = purrr::map_int(.data$canonical_site_id, find_root))

  component_lookup <- raw_membership %>%
    dplyr::group_by(.data$root_id) %>%
    dplyr::summarise(site_id = min(.data$canonical_site_id), .groups = "drop") %>%
    dplyr::arrange(.data$site_id) %>%
    dplyr::mutate(component_id = dplyr::row_number())

  raw_membership %>%
    dplyr::left_join(component_lookup, by = "root_id") %>%
    dplyr::group_by(.data$component_id) %>%
    dplyr::mutate(
      n_members = dplyr::n(),
      site_id_members = paste(sort(.data$canonical_site_id), collapse = ";")
    ) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      site_id = as.integer(.data$site_id),
      member_site_id = as.integer(.data$canonical_site_id),
      water_company = .data$water_company,
      component_id = as.integer(.data$component_id),
      n_members = as.integer(.data$n_members),
      site_id_members = .data$site_id_members
    ) %>%
    dplyr::arrange(.data$site_id, .data$member_site_id) %>%
    conform_works_table(WORKS_REGISTER_MEMBERSHIP_PROTOTYPE)
}

build_works_register <- function(
    annual_rows,
    lookup_tbl = NULL,
    config = list(works_merge_ngr_m = 250, works_near_miss_m = 1000)) {
  prepared <- prepare_works_register_annual_rows(annual_rows, lookup_tbl)

  if (nrow(prepared) == 0) {
    return(list(
      membership = WORKS_REGISTER_MEMBERSHIP_PROTOTYPE,
      resolver = WORKS_REGISTER_RESOLVER_PROTOTYPE,
      edges = WORKS_REGISTER_EDGE_PROTOTYPE,
      near_misses = WORKS_REGISTER_NEAR_MISS_PROTOTYPE,
      edge_summary = tibble::tibble(justification = character(), n_edges = integer())
    ))
  }

  pair_outputs <- build_works_register_edges(prepared, config)

  nodes <- prepared %>%
    dplyr::distinct(.data$canonical_site_id, .data$water_company)

  membership <- build_works_membership(nodes, pair_outputs$edges)

  resolver <- prepared %>%
    dplyr::left_join(
      membership %>%
        dplyr::select(
          site_id,
          member_site_id,
          water_company
        ) %>%
        dplyr::mutate(canonical_site_id = .data$member_site_id),
      by = c("canonical_site_id", "water_company")
    ) %>%
    dplyr::select(dplyr::all_of(names(WORKS_REGISTER_RESOLVER_PROTOTYPE))) %>%
    dplyr::arrange(.data$year, .data$water_company, .data$annual_site_id) %>%
    conform_works_table(WORKS_REGISTER_RESOLVER_PROTOTYPE)

  edge_summary <- pair_outputs$edges %>%
    dplyr::count(.data$justification, name = "n_edges") %>%
    dplyr::arrange(.data$justification)

  list(
    membership = membership,
    resolver = resolver,
    edges = pair_outputs$edges,
    near_misses = pair_outputs$near_misses,
    edge_summary = edge_summary
  )
}
