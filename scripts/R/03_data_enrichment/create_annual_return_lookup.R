############################################################
# Create Annual Return Lookup Table
# Project: Sewage
# Date: 17/04/2025
# Author: Jacopo Olivieri
############################################################

# This script builds a cross‑year lookup table that links the same spill
# sites across the annual EDM returns (2021‑2024).

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "tidyverse", "arrow", "here", "conflicted", "stringr", "stringdist",
  "reclin2", "igraph", "ranger", "logger", "glue", "data.table",
  "purrr", "rio"
)

LOG_FILE <- here::here("output", "log", "09_create_annual_return_lookup.log")

check_required_packages(REQUIRED_PACKAGES)

# Setup Functions
############################################################

#' Attach the packages used unqualified in this script
#' @return NULL
initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(pkg) {
    library(pkg, character.only = TRUE)
  }))

  conflicted::conflict_prefer("select", "dplyr")
  conflicted::conflict_prefer("filter", "dplyr")
  conflicted::conflict_prefer("first", "dplyr")
}

#' Initialise logging for this script
#' @return NULL
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "INFO")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# Configuration
############################################################

CONFIG <- list(
  years = 2021:2024,
  data_path = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  output_dir = here::here("data", "processed"),
  lookup_parquet = here::here(
    "data", "processed", "annual_return_lookup.parquet"
  ),
  edge_metadata_parquet = here::here(
    "data", "processed", "annual_return_lookup_edges.parquet"
  ),
  conflict_summary_parquet = here::here(
    "data", "processed", "annual_return_lookup_conflict_summary.parquet"
  ),
  conflict_records_parquet = here::here(
    "data", "processed", "annual_return_lookup_conflict_records.parquet"
  ),
  conflict_edges_parquet = here::here(
    "data", "processed", "annual_return_lookup_conflict_edges.parquet"
  ),
  resolution_kept_edges_parquet = here::here(
    "data", "processed", "annual_return_lookup_resolution_kept_edges.parquet"
  ),
  resolution_dropped_edges_parquet = here::here(
    "data", "processed", "annual_return_lookup_resolution_dropped_edges.parquet"
  ),
  conflict_excel_output = here::here(
    "data", "processed", "annual_return_lookup_conflicts.xlsx"
  ),
  post_resolution_conflict_summary_parquet = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_conflict_summary.parquet"
  ),
  post_resolution_conflict_records_parquet = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_conflict_records.parquet"
  ),
  post_resolution_conflict_edges_parquet = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_conflict_edges.parquet"
  ),
  post_resolution_kept_edges_parquet = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_kept_edges.parquet"
  ),
  post_resolution_dropped_edges_parquet = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_dropped_edges.parquet"
  ),
  post_resolution_conflict_excel_output = here::here(
    "data", "processed",
    "annual_return_lookup_post_resolution_conflicts.xlsx"
  ),
  excel_output = here::here(
    "data", "processed", "annual_return_lookup.xlsx"
  ),
  rf_model_file = here::here(
    "output", "code_testing", "rf_model_2023_2024.rds"
  ),
  evidence_field_priority = c(
    "activity_reference",
    "outlet_discharge_ngr",
    "permit_reference_ea",
    "permit_reference_wa_sc",
    "site_name_ea",
    "site_name_wa_sc"
  ),
  core_identifier_cols = c(
    "year", "water_company", "site_name_ea", "site_name_wa_sc",
    "permit_reference_ea", "permit_reference_wa_sc",
    "activity_reference", "outlet_discharge_ngr"
  )
)

# Convenience binding
YEARS <- CONFIG$years

# Sourced Utilities
############################################################

# Graph resolution (MST view, year-constrained forest, component bridging)
source(
  here::here("scripts", "R", "utils", "annual_return_lookup_graph_utils.R"),
  local = TRUE
)

# Conflict audits (identifier lookup, audit construction/export, safety stop)
source(
  here::here("scripts", "R", "utils", "annual_return_lookup_audit_utils.R"),
  local = TRUE
)

# Data Preparation
############################################################

#' Prepare a list of per‑year tibbles with consistent columns & site_id
#' @param years Vector of years to process
#' @param data_path Path to the consolidated parquet file
#' @return List of tibbles with one tibble per year
prepare_data_list <- function(years = CONFIG$years, data_path = CONFIG$data_path) {
  logger::log_info("Reading consolidated parquet from {data_path}")

  full_data <- arrow::read_parquet(data_path)

  res <- lapply(years, function(yr) {
    df <- dplyr::filter(full_data, year == yr)

    # Create primary_id based on unique_id where available (2024)
    if (yr == 2024 && "unique_id" %in% names(df)) {
      df <- mutate(df, primary_id = unique_id)
    }

    # Store 2023 unique_id for matching with 2024
    if (yr == 2023 && "unique_id" %in% names(df)) {
      df <- mutate(df, unique_id_2023 = unique_id)
    }

    # Add a deterministic site_id_<year> if not present already
    site_id_col <- paste0("site_id_", yr)
    if (!site_id_col %in% names(df)) {
      df <- mutate(df, !!site_id_col := row_number())
    }

    df <- mutate(df, across(
      any_of(CONFIG$evidence_field_priority),
      ~ if_else(
        stringr::str_to_upper(stringr::str_trim(as.character(.x))) %in% c("TBC", "N/A", ""),
        NA_character_,
        as.character(.x)
      )
    ))

    # Keep only relevant columns
    select(df, any_of(c(
      "primary_id", "unique_id", "unique_id_2023",
      CONFIG$core_identifier_cols, site_id_col
    )))
  })
  names(res) <- paste0("df", years)

  logger::log_info("Successfully prepared data for {length(years)} years")
  res
}

# Deterministic ("Windfall") Matching
############################################################

# --- Helper: outer‑join with diagnostics ---------------------------------- #

#' Perform a full join on a set of keys while tracking match multiplicity
#' @param df_left Left dataframe
#' @param df_right Right dataframe
#' @param join_keys Vector of column names to join on
#' @param suffix_left Suffix for columns from left dataframe
#' @param suffix_right Suffix for columns from right dataframe
#' @return List containing joined_data, non_1_to_1_matched_left, non_1_to_1_matched_right
f_outer_join <- function(df_left, df_right, join_keys,
                         suffix_left = "_left", suffix_right = "_right") {
  # Add row IDs to track matches
  df_left <- mutate(df_left, ..rowid_left = row_number())
  df_right <- mutate(df_right, ..rowid_right = row_number())

  # Perform full join
  joined_df <- suppressWarnings(
    full_join(df_left, df_right,
      by = join_keys,
      suffix = c(suffix_left, suffix_right), multiple = "all",
      na_matches = "never"
    )
  )

  # Categorize join results
  joined_df <- joined_df %>%
    mutate(join_origin = dplyr::case_when(
      !is.na(..rowid_left) & is.na(..rowid_right) ~ "left_only",
      is.na(..rowid_left) & !is.na(..rowid_right) ~ "right_only",
      !is.na(..rowid_left) & !is.na(..rowid_right) ~ "matched",
      TRUE ~ NA_character_
    ))

  matched_rows <- filter(joined_df, join_origin == "matched")

  # Analyze match types
  if (nrow(matched_rows) > 0) {
    match_results <- matched_rows %>%
      group_by(across(all_of(join_keys))) %>%
      summarise(
        n_left = n_distinct(..rowid_left),
        n_right = n_distinct(..rowid_right), .groups = "drop"
      ) %>%
      mutate(match_type = dplyr::case_when(
        n_left == 1 & n_right == 1 ~ "1_to_1",
        n_left == 1 ~ "1_to_many",
        n_right == 1 ~ "many_to_1",
        TRUE ~ "many_to_many"
      ))

    joined_df <- left_join(joined_df,
      select(match_results, all_of(join_keys), match_type),
      by = join_keys, na_matches = "never"
    )

    problem_keys <- filter(match_results, match_type != "1_to_1") %>%
      select(all_of(join_keys))
  } else {
    joined_df <- mutate(joined_df, match_type = NA_character_)
    problem_keys <- tibble()
  }

  # Helper to collect problematic row indices (unmatched or n:1)
  get_problem_ids <- function(side) {
    unmatched_ids <- joined_df %>%
      filter(join_origin == paste0(side, "_only")) %>%
      pull(!!sym(paste0("..rowid_", side)))

    matched_problem_ids <- if (nrow(problem_keys) > 0) {
      matched_rows %>%
        semi_join(problem_keys, by = join_keys) %>%
        pull(!!sym(paste0("..rowid_", side))) %>%
        unique()
    } else {
      integer(0)
    }

    base::union(unmatched_ids, matched_problem_ids)
  }

  # Get indices of problematic rows
  problem_left_ids <- get_problem_ids("left")
  problem_right_ids <- get_problem_ids("right")

  # Create dataframes with problematic rows
  problem_left_df <- slice(df_left, sort(problem_left_ids)) %>%
    select(-any_of("..rowid_left"))
  problem_right_df <- slice(df_right, sort(problem_right_ids)) %>%
    select(-any_of("..rowid_right"))

  # Finalize joined dataframe
  final_joined_df <- joined_df %>%
    mutate(join_keys = if_else(join_origin == "matched" &
      match_type %in% c("1_to_1", "1_to_many", "many_to_1"),
    paste0(join_keys, collapse = "|"), NA_character_
    )) %>%
    select(-c(..rowid_left, ..rowid_right))

  # Return result
  list(
    joined_data               = final_joined_df,
    non_1_to_1_matched_left   = problem_left_df,
    non_1_to_1_matched_right  = problem_right_df
  )
}

# --- Single pair windfall matching --------------------------------------- #

#' Execute multi‑level windfall matching for a pair of years
#' @param pair_key String indicating the year pair (e.g., "2021_2022")
#' @param matching_levels List of attribute combinations to use for matching
#' @param data_list List of tibbles with one tibble per year
#' @param verbose Boolean flag for verbose output
#' @return List containing matches, unmatched_left_ids, and unmatched_right_ids
perform_windfall_matching <- function(pair_key, matching_levels, data_list,
                                      verbose = FALSE) {
  tryCatch(
    {
      years <- as.numeric(strsplit(pair_key, "_")[[1]])
      left_year <- years[1]
      right_year <- years[2]
      year_suffix <- paste0(left_year, "_", right_year)

      df_left <- data_list[[paste0("df", left_year)]]
      df_right <- data_list[[paste0("df", right_year)]]

      left_remaining <- df_left
      right_remaining <- df_right
      all_matches <- list()

      for (level in seq_along(matching_levels)) {
        join_keys <- c("water_company", matching_levels[[level]])

        match_res <- f_outer_join(
          df_left        = left_remaining,
          df_right       = right_remaining,
          join_keys      = join_keys,
          suffix_left    = paste0("_", left_year),
          suffix_right   = paste0("_", right_year)
        )

        matches <- match_res$joined_data %>%
          filter(join_origin == "matched", match_type == "1_to_1") %>%
          mutate(!!paste0("match_level_", year_suffix) := level)

        all_matches[[level]] <- matches
        left_remaining <- match_res$non_1_to_1_matched_left
        right_remaining <- match_res$non_1_to_1_matched_right

        if (verbose) {
          logger::log_info("{pair_key} Level {level}: Matched {nrow(matches)} rows")
        }
      }

      combined_matches <- bind_rows(all_matches) %>%
        mutate(!!paste0("match_method_", year_suffix) := "windfall") %>%
        rename(
          !!paste0("match_type_", year_suffix) := match_type,
          !!paste0("join_keys_", year_suffix) := join_keys
        ) %>%
        select(
          !!sym(paste0("site_id_", left_year)),
          !!sym(paste0("site_id_", right_year)),
          starts_with(paste0("match_level_", year_suffix)),
          starts_with(paste0("match_method_", year_suffix)),
          starts_with(paste0("match_type_", year_suffix)),
          starts_with(paste0("join_keys_", year_suffix))
        )

      result <- list(
        matches             = combined_matches,
        unmatched_left_ids  = pull(left_remaining, !!sym(paste0("site_id_", left_year))),
        unmatched_right_ids = pull(right_remaining, !!sym(paste0("site_id_", right_year)))
      )

      if (verbose) {
        logger::log_info("{pair_key}: Found {nrow(combined_matches)} matches, {length(result$unmatched_left_ids)} unmatched left, {length(result$unmatched_right_ids)} unmatched right")
      }

      return(result)
    },
    error = function(e) {
      logger::log_error("Windfall matching failed for pair {pair_key}: {e$message}")
      stop(glue::glue("Matching failed for {pair_key}: {e$message}"))
    }
  )
}

#' Check whether a dataframe has a non-missing matching variable
#' @param df Dataframe to inspect
#' @param col Column name
#' @return TRUE if the column exists and has at least one non-missing value
has_non_missing_match_var <- function(df, col) {
  col %in% names(df) && any(!is.na(df[[col]]) & as.character(df[[col]]) != "")
}

#' Split pipe-separated join key strings into fields
#' @param join_keys Character vector of pipe-separated join keys
#' @return List of character vectors
split_join_keys <- function(join_keys) {
  strsplit(as.character(join_keys), "\\|")
}

#' Convert an ordered evidence-field set into a lexicographic priority score
#' @param fields Character vector of ordinary non-unique matching fields
#' @param priority_order Field names from strongest to weakest
#' @return Numeric scalar; larger means stronger under the ordinal ranking
score_evidence_fields <- function(
    fields,
    priority_order = CONFIG$evidence_field_priority) {
  fields <- unique(fields[fields %in% priority_order])
  if (length(fields) == 0) {
    return(0)
  }

  ranks <- sort(match(fields, priority_order))
  base <- length(priority_order) + 1L
  values <- base - ranks
  sum(values * (base ^ rev(seq_along(values) - 1L)))
}

#' Score join keys using ordinary evidence fields only
#' @param join_keys Character vector of pipe-separated join keys
#' @param priority_order Field names from strongest to weakest
#' @return Tibble with evidence counts, ordinal scores, and unique-ID flags
score_join_key_evidence <- function(
    join_keys,
    priority_order = CONFIG$evidence_field_priority) {
  purrr::map_dfr(split_join_keys(join_keys), function(keys) {
    evidence_fields <- unique(keys[keys %in% priority_order])

    tibble(
      evidence_field_count = length(evidence_fields),
      field_priority_score = score_evidence_fields(
        evidence_fields,
        priority_order = priority_order
      ),
      has_unique_id_2023_key = "unique_id_2023" %in% keys,
      has_unique_id_key = "unique_id" %in% keys
    )
  })
}

#' Sort ordinary matching levels by field count, then ordinal field priority
#' @param matching_levels List of ordinary non-unique matching field vectors
#' @param priority_order Field names from strongest to weakest
#' @return Matching levels sorted from strongest to weakest
sort_matching_levels_by_priority <- function(
    matching_levels,
    priority_order = CONFIG$evidence_field_priority) {
  level_scores <- purrr::map_dfr(seq_along(matching_levels), function(i) {
    level <- matching_levels[[i]]
    evidence_fields <- unique(level[level %in% priority_order])

    tibble(
      level_index = i,
      evidence_field_count = length(evidence_fields),
      field_priority_score = score_evidence_fields(
        evidence_fields,
        priority_order = priority_order
      ),
      level_text = paste(sort(level), collapse = "|")
    )
  })

  matching_levels[level_scores %>%
    arrange(
      desc(evidence_field_count),
      desc(field_priority_score),
      level_text
    ) %>%
    pull(level_index)]
}

#' Build deterministic matching levels with exact unique IDs first where valid
#' @param left_year Left year
#' @param right_year Right year
#' @param data_list List of yearly dataframes
#' @param general_levels Attribute-combination matching levels
#' @return List of matching levels for the year pair
build_windfall_matching_levels <- function(
    left_year, right_year, data_list, general_levels) {
  df_left <- data_list[[paste0("df", left_year)]]
  df_right <- data_list[[paste0("df", right_year)]]

  unique_levels <- list()

  if (left_year == 2023 && right_year == 2024 &&
      has_non_missing_match_var(df_left, "unique_id_2023") &&
      has_non_missing_match_var(df_right, "unique_id_2023")) {
    unique_levels <- append(unique_levels, list(c("unique_id_2023")))
  }

  if (has_non_missing_match_var(df_left, "unique_id") &&
      has_non_missing_match_var(df_right, "unique_id")) {
    unique_levels <- append(unique_levels, list(c("unique_id")))
  }

  unique_levels <- unique(unique_levels)
  c(unique_levels, general_levels)
}

#' Enumerate all left<right reporting-year pairs in matching order
#' @param years Vector of years to process
#' @return Dataframe with left_year and right_year columns
build_year_pairs <- function(years) {
  expand.grid(
    left_year = years, right_year = years,
    stringsAsFactors = FALSE
  ) %>%
    filter(left_year < right_year) %>%
    arrange(desc(right_year), desc(left_year))
}

#' Run deterministic matching over every left<right year pair
#' @param years Vector of years to process
#' @param data_list List of tibbles with one tibble per year
#' @param verbose Boolean flag for verbose output
#' @return List of match dataframes, one for each year pair
run_all_windfall_matches <- function(
    years = CONFIG$years,
    data_list, verbose = FALSE) {
  logger::log_info("Running deterministic matching across all year pairs")

  # Create all possible year pairs (left_year < right_year)
  year_pairs <- build_year_pairs(years)

  # Build variable combinations and rank them by evidence strength.
  vars <- CONFIG$evidence_field_priority
  var_list <- unlist(lapply(seq_along(vars), function(i) combn(vars, i, simplify = FALSE)),
    recursive = FALSE
  )
  config_general <- sort_matching_levels_by_priority(var_list)

  # Map pair -> config list
  pair_keys <- apply(year_pairs, 1, function(x) paste(x[1], x[2], sep = "_"))
  configs <- lapply(seq_along(pair_keys), function(i) {
    build_windfall_matching_levels(
      left_year = year_pairs$left_year[i],
      right_year = year_pairs$right_year[i],
      data_list = data_list,
      general_levels = config_general
    )
  })
  names(configs) <- pair_keys

  # Run matching for each year pair
  match_results <- lapply(pair_keys, function(pk) {
    if (verbose) logger::log_info("Processing year pair: {pk}")
    perform_windfall_matching(pk,
      matching_levels = configs[[pk]],
      data_list = data_list, verbose = verbose
    )
  })
  names(match_results) <- pair_keys

  # Extract just the matches dataframes
  result <- lapply(match_results, `[[`, "matches")
  logger::log_info("Completed deterministic matching across {length(pair_keys)} year pairs")
  result
}

# Graph & Lookup‑Table Construction
############################################################


#' Check whether a match dataframe can be converted to graph edges
#' @param df Match dataframe
#' @return TRUE when the dataframe has the required lookup columns
has_lookup_match_schema <- function(df) {
  is.data.frame(df) &&
    length(grep("^site_id_", names(df), value = TRUE)) == 2 &&
    length(grep("^match_method_", names(df), value = TRUE)) == 1 &&
    length(grep("^match_type_", names(df), value = TRUE)) == 1 &&
    length(grep("^match_level_", names(df), value = TRUE)) == 1 &&
    length(grep("^join_keys_", names(df), value = TRUE)) == 1
}

#' Convert pairwise match dataframes into weighted candidate graph edges
#' @param match_dfs Named list of pairwise match dataframes
#' @return Tibble of weighted, deduplicated candidate edges (possibly empty)
build_weighted_edges <- function(match_dfs) {
  nonempty_match_dfs <- purrr::keep(
    match_dfs,
    ~ is.data.frame(.x) && nrow(.x) > 0
  )
  invalid_match_dfs <- purrr::discard(
    nonempty_match_dfs,
    has_lookup_match_schema
  )

  if (length(invalid_match_dfs) > 0) {
    invalid_names <- names(invalid_match_dfs)
    if (is.null(invalid_names) || any(invalid_names == "")) {
      invalid_names <- paste0("match_df_", seq_along(invalid_match_dfs))
    }
    stop(glue::glue(
      "Nonempty match dataframes are missing lookup columns: ",
      "{paste(invalid_names, collapse = ', ')}"
    ))
  }

  if (length(nonempty_match_dfs) == 0) {
    return(tibble())
  }

  # Build edge list
  edges_df <- lapply(nonempty_match_dfs, function(df) {
    site_cols <- grep("^site_id_", names(df), value = TRUE)
    match_method_col <- grep("^match_method_", names(df), value = TRUE)
    match_type_col <- grep("^match_type_", names(df), value = TRUE)
    level_col <- grep("^match_level_", names(df), value = TRUE)
    join_keys_col <- grep("^join_keys_", names(df), value = TRUE)

    # Edges are undirected, but exports use chronological endpoint labels.
    site_years <- as.integer(stringr::str_extract(site_cols, "\\d{4}$"))
    endpoint_order <- order(site_years)
    site_cols <- site_cols[endpoint_order]
    site_years <- site_years[endpoint_order]
    yr_l <- site_years[1]
    yr_r <- site_years[2]

    df %>%
      filter(!is.na(.data[[join_keys_col]])) %>%
      transmute(
        from         = paste0(yr_l, "_", .data[[site_cols[1]]]),
        to           = paste0(yr_r, "_", .data[[site_cols[2]]]),
        match_method = .data[[match_method_col]],
        match_type   = .data[[match_type_col]],
        match_level  = as.integer(.data[[level_col]]),
        join_keys    = .data[[join_keys_col]]
      ) %>%
      filter(from != "", to != "")
  }) %>%
    bind_rows() %>%
    distinct()

  if (nrow(edges_df) == 0) {
    return(edges_df)
  }

  edge_evidence <- score_join_key_evidence(edges_df$join_keys)

  # Weight: ordinary field count plus exact-ID/RF priority diagnostics.
  bind_cols(edges_df, edge_evidence) %>%
    mutate(
      n_keys = stringr::str_count(join_keys, "\\|") + 1L,
      edge_priority = case_when(
        has_unique_id_2023_key ~ 300L,
        has_unique_id_key ~ 290L,
        stringr::str_detect(join_keys, "rf_probabilistic") ~ 10L,
        TRUE ~ 100L
      ),
      raw_score = case_when(
        has_unique_id_2023_key | has_unique_id_key ~ max(n_keys) + 2,
        stringr::str_detect(join_keys, "rf_probabilistic") ~ 1,
        TRUE ~ evidence_field_count + 1
      ),
      weight = raw_score * 100 - match_level
    ) %>%
    select(-has_unique_id_2023_key, -has_unique_id_key)
}

#' Given pairwise match dataframes, build a year-constrained lookup
#'
#' Pure on every successful return: yields the lookup, edge metadata, and
#' audit tables and writes nothing - main() owns all exports. Writes
#' happen only on the failure path, where the safety net trips: the
#' diagnostics are exported inside the same code path as the stop, so a
#' tripped run always leaves its evidence on disk.
#' @param match_dfs Named list of pairwise match dataframes
#' @param data_list Named list of yearly dataframes
#' @param conflict_resolution Same-year conflict policy: fail closed, or
#'   explicitly split ambiguous components with a year-constrained forest
#' @return A list containing lookup_table, edge_metadata, conflict_audit
#'   (pre-resolution), and post_resolution_state (post-resolution audit;
#'   all-empty on every successful return)
build_lookup_from_matches <- function(
    match_dfs,
    data_list,
    conflict_resolution = c("fail", "year_constrained_forest")) {
  logger::log_info("Building lookup table from match data")

  conflict_resolution <- match.arg(conflict_resolution)
  edges_df <- build_weighted_edges(match_dfs)

  if (nrow(edges_df) == 0) {
    logger::log_warn("No usable match edges supplied; returning empty lookup")
    return(list(
      lookup_table = empty_lookup_table(CONFIG$years),
      edge_metadata = empty_edge_metadata(),
      conflict_audit = empty_conflict_audit(),
      post_resolution_state = empty_conflict_audit()
    ))
  }

  # Keep the old MST only as a pre-resolution audit view, then build the
  # canonical lookup from the year-constrained forest.
  logger::log_info("Building unconstrained MST audit view from {nrow(edges_df)} edges")
  pre_resolution <- build_unconstrained_mst_components(edges_df)

  logger::log_info("Building year-constrained maximum spanning forest")
  constrained_forest <- build_year_constrained_spanning_forest(edges_df)

  kept_edges_for_audit <- attach_pre_resolution_components(
    constrained_forest$kept_edges,
    pre_resolution$membership_tbl
  )
  dropped_edges_for_audit <- attach_pre_resolution_components(
    constrained_forest$dropped_edges,
    pre_resolution$membership_tbl
  )

  pre_conflict_audit <- build_lookup_conflict_audit(
    membership_tbl = pre_resolution$membership_tbl,
    edge_metadata = pre_resolution$edge_metadata,
    data_list = data_list,
    kept_edges = kept_edges_for_audit,
    dropped_edges = dropped_edges_for_audit,
    resolution_component_scope = "pre"
  )
  if (conflict_resolution == "fail" &&
      nrow(pre_conflict_audit$summary) > 0) {
    export_conflict_audit(pre_conflict_audit, paths = CONFIG)
    stop_if_lookup_conflicts(
      pre_conflict_audit,
      audit_path = CONFIG$conflict_excel_output,
      written_files = conflict_audit_files(CONFIG)
    )
  }

  edge_metadata <- if (nrow(constrained_forest$kept_edges) > 0) {
    constrained_forest$kept_edges %>%
      select(all_of(names(EDGE_METADATA_EXPORT_PROTOTYPE)))
  } else {
    empty_edge_metadata()
  }

  post_conflict_audit <- build_lookup_conflict_audit(
    membership_tbl = constrained_forest$membership_tbl,
    edge_metadata = edge_metadata,
    data_list = data_list,
    kept_edges = kept_edges_for_audit,
    dropped_edges = dropped_edges_for_audit,
    resolution_component_scope = "final"
  )
  logger::log_info(
    "Post-resolution conflict audit found {nrow(post_conflict_audit$summary)} conflicted component-years"
  )
  if (nrow(post_conflict_audit$summary) > 0) {
    # Failure-gated diagnostics: written only at the moment the final
    # safety net trips, inside the same code path as the stop (KTD3).
    post_conflict_paths <- post_resolution_conflict_audit_paths(CONFIG)
    export_conflict_audit(post_conflict_audit, paths = post_conflict_paths)
    stop_if_lookup_conflicts(
      post_conflict_audit,
      audit_path = post_conflict_paths$conflict_excel_output,
      written_files = conflict_audit_files(post_conflict_paths)
    )
  }

  lookup_table <- constrained_forest$membership_tbl %>%
    group_by(component, year) %>%
    summarise(
      site_id = {
        if (dplyr::n() != 1L) {
          stop("Year-constrained forest invariant failed: component-year contains multiple site IDs.")
        }
        site_id[[1]]
      },
      .groups = "drop"
    ) %>%
    tidyr::pivot_wider(
      names_from = year, values_from = site_id,
      names_prefix = "site_id_", values_fill = NA_character_
    )

  site_cols <- paste0("site_id_", CONFIG$years)
  if (!"component" %in% names(lookup_table)) {
    lookup_table$component <- integer()
  }
  for (site_col in setdiff(site_cols, names(lookup_table))) {
    lookup_table[[site_col]] <- NA_character_
  }
  lookup_table <- lookup_table %>%
    select(component, all_of(site_cols)) %>%
    arrange(component)

  logger::log_info("Successfully built lookup table with {nrow(lookup_table)} rows")
  list(
    lookup_table = lookup_table,
    edge_metadata = edge_metadata,
    conflict_audit = pre_conflict_audit,
    post_resolution_state = post_conflict_audit
  )
}

#' Ensure numeric year-specific site_id columns, failing on bad coercions
#'
#' Adds any missing site_id_<year> columns as NA and converts existing ones
#' to numeric, stopping when coercion would silently introduce NAs.
#' @param lookup_tbl Lookup table tibble
#' @param site_cols Character vector of site_id_<year> column names
#' @return Lookup table with all site_cols present and numeric
ensure_site_id_columns <- function(lookup_tbl, site_cols) {
  for (site_col in setdiff(site_cols, names(lookup_tbl))) {
    lookup_tbl[[site_col]] <- NA_real_
  }

  for (site_col in site_cols) {
    original <- lookup_tbl[[site_col]]
    converted <- suppressWarnings(as.numeric(as.character(original)))
    coercion_failures <- is.na(converted) & !is.na(original)
    if (any(coercion_failures)) {
      bad_values <- unique(as.character(original)[coercion_failures])
      stop(glue::glue(
        "Non-numeric {site_col} values cannot be used as annual-return ",
        "site IDs: {paste(utils::head(bad_values, 5), collapse = ', ')}"
      ))
    }
    lookup_tbl[[site_col]] <- converted
  }

  lookup_tbl
}

# Helper to append singleton site_ids without matches
#' Append orphan sites (no matches) to the lookup table
#' @param lookup_tbl Lookup table tibble
#' @param data_list Named list of yearly dataframes
#' @return Extended lookup table including singletons
append_singleton_sites <- function(lookup_tbl, data_list) {
  years <- data_list_years(data_list)
  site_cols <- paste0("site_id_", years)

  lookup_tbl <- ensure_site_id_columns(lookup_tbl, site_cols)

  orphan_rows <- purrr::map_dfr(years, function(yr) {
    col <- paste0("site_id_", yr)
    all_ids <- suppressWarnings(as.numeric(as.character(
      data_list[[paste0("df", yr)]][[col]]
    )))
    missing <- setdiff(all_ids, lookup_tbl[[col]])
    if (length(missing) == 0) return(tibble())
    tibble::tibble(!!!setNames(
      purrr::map(years, function(y) if (y == yr) missing else rep(NA_real_, length(missing))),
      site_cols
    ))
  })

  if (nrow(orphan_rows) > 0) {
    lookup_tbl <- bind_rows(lookup_tbl, orphan_rows)
  }

  lookup_tbl
}

#' Assign stable canonical site IDs after matched and singleton rows are present
#' @param lookup_tbl Lookup table with one column per annual-return year
#' @param years Reporting years included in the lookup
#' @return Lookup table with deterministic canonical site_id values
assign_canonical_site_ids <- function(lookup_tbl, years = CONFIG$years) {
  site_cols <- paste0("site_id_", years)

  if (!"component" %in% names(lookup_tbl)) {
    lookup_tbl$component <- NA_integer_
  }

  lookup_tbl <- ensure_site_id_columns(lookup_tbl, site_cols)

  # One-pass vectorized scan for each row's earliest observed year; rows
  # with no observed year sort last via Inf.
  observed <- !is.na(as.matrix(lookup_tbl[site_cols]))
  first_observed_idx <- max.col(observed, ties.method = "first")
  first_observed_year <- ifelse(
    rowSums(observed) > 0,
    years[first_observed_idx],
    Inf
  )

  # Stable canonical order: earliest observed annual-return year, then the
  # year-specific annual-return IDs from oldest to newest.
  lookup_tbl %>%
    mutate(
      first_observed_year = first_observed_year,
      component_sort = tidyr::replace_na(as.integer(component), .Machine$integer.max)
    ) %>%
    arrange(
      first_observed_year,
      across(all_of(site_cols)),
      component_sort
    ) %>%
    mutate(site_id = row_number()) %>%
    select(site_id, component, all_of(site_cols), everything(),
      -first_observed_year, -component_sort
    )
}

#' Verify that the final lookup preserves one unique row per input annual site
#' @param lookup_tbl Final lookup table
#' @param data_list Named list of yearly dataframes
#' @return Invisibly TRUE when invariants hold
assert_lookup_year_integrity <- function(lookup_tbl, data_list) {
  years <- data_list_years(data_list)

  for (yr in years) {
    site_col <- paste0("site_id_", yr)
    if (!site_col %in% names(lookup_tbl)) {
      stop(glue::glue("Final lookup is missing {site_col}."))
    }

    observed_ids <- lookup_tbl[[site_col]][!is.na(lookup_tbl[[site_col]])]
    expected_n <- nrow(data_list[[paste0("df", yr)]])
    observed_n <- length(observed_ids)

    if (observed_n != expected_n) {
      stop(glue::glue(
        "Final lookup row-conservation failed for {yr}: ",
        "expected {expected_n} non-missing {site_col} values, found {observed_n}."
      ))
    }

    if (anyDuplicated(as.character(observed_ids)) > 0) {
      stop(glue::glue(
        "Final lookup uniqueness failed for {yr}: duplicate {site_col} values found."
      ))
    }
  }

  invisible(TRUE)
}


# Export 
############################################################

#' Export lookup data to Excel and Parquet formats
#' @param lookup_tbl The lookup table
#' @param edges_tbl The edge metadata table
#' @param output_dir Directory for output files
#' @param excel_output Path to Excel output file
#' @param lookup_parquet Path to lookup table parquet file
#' @param edge_metadata_parquet Path to edge metadata parquet file
#' @return NULL
export_data <- function(
    lookup_tbl,
    edges_tbl,
    output_dir = CONFIG$output_dir,
    excel_output = CONFIG$excel_output,
    lookup_parquet = CONFIG$lookup_parquet,
    edge_metadata_parquet = CONFIG$edge_metadata_parquet) {
  # Create output directory if it doesn't exist
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  # 1. Export to parquet files
  arrow::write_parquet(lookup_tbl, lookup_parquet)
  logger::log_info("Exported lookup table to parquet: {lookup_parquet}")

  arrow::write_parquet(edges_tbl, edge_metadata_parquet)
  logger::log_info("Exported edge metadata to parquet: {edge_metadata_parquet}")

  # 2. Export to Excel (both tables in one file, different sheets)
  rio::export(
    list("lookup_table" = lookup_tbl, "edge_metadata" = edges_tbl),
    excel_output
  )

  logger::log_info("Exported data to Excel file: {excel_output}")
}

# Main Execution
############################################################

#' Main execution function
#' @param compute_rf_matching Boolean probabilistic matching
#' @param same_year_conflict_resolution Same-year conflict policy for lookup
#'   construction. Defaults to explicit year-constrained splitting.
#' @return NULL
main <- function(
    compute_rf_matching = FALSE,
    same_year_conflict_resolution = c("year_constrained_forest", "fail")) {
  tryCatch({
    same_year_conflict_resolution <- match.arg(same_year_conflict_resolution)

    # Setup
    logger::log_info("===== Building Annual Return Lookup Table =====")
    initialise_environment()
    initialise_logging()

    # 1. Prepare per‑year data
    data_list <- prepare_data_list()
    logger::log_info(
      "Prepared per‑year datasets: {paste(names(data_list), collapse=', ')}"
    )

    # 2. Windfall matching
    windfall_match_dfs <- run_all_windfall_matches(
      data_list = data_list, verbose = FALSE
    )
    logger::log_info("Deterministic matching complete.")

    # 3. Probabilistic matching (if enabled)
    match_dfs <- windfall_match_dfs

    if (compute_rf_matching) {
      # RF matching is optional and heavy: source its utilities on demand only.
      source(
        here::here("scripts", "R", "utils", "annual_return_lookup_rf_matching.R"),
        local = TRUE
      )

      # Windfall-only forest pass (no exports) to find still-unmatched sites.
      unmatched_ids_by_year <- derive_windfall_unmatched_ids(
        windfall_match_dfs,
        data_list = data_list,
        years = CONFIG$years
      )

      m_rf <- load_or_train_rf_model(
        data_list,
        train_if_missing = TRUE
      )
      if (!is.null(m_rf)) {
        year_pairs_df <- build_year_pairs(CONFIG$years)
        rf_match_dfs <- run_rf_matching(
          year_pairs_df, data_list, m_rf, unmatched_ids_by_year
        )
        match_dfs <- c(match_dfs, rf_match_dfs)
        logger::log_info(
          "RF matching complete -
          {sum(vapply(rf_match_dfs, nrow, integer(1)))} matches added."
        )
      }
    } else {
      logger::log_info(
        "RF matching skipped because compute_rf_matching is FALSE"
      )
    }

    # 4. Build lookup table (pure - all file-writes happen below in main)
    lookup_res <- build_lookup_from_matches(
      match_dfs,
      data_list = data_list,
      conflict_resolution = same_year_conflict_resolution
    )
    edges_tbl <- lookup_res$edge_metadata %>%
      mutate(across(starts_with("site_id") | starts_with("year"),
                    ~ as.numeric(as.character(.))))

    # 5. Append singleton sites with no matches and assign canonical IDs
    lookup_tbl <- lookup_res$lookup_table %>%
      append_singleton_sites(data_list) %>%
      assign_canonical_site_ids()
    assert_lookup_year_integrity(lookup_tbl, data_list)

    # 6. Save outputs: pre-resolution audit (zero-row tables on zero-edge
    # runs, so stale audits never survive), then the lookup and edges.
    # Post-resolution diagnostics exist only when the safety net tripped;
    # a healthy run removes leftovers from earlier failed runs.
    export_conflict_audit(lookup_res$conflict_audit, paths = CONFIG)
    clear_post_resolution_conflict_audit(CONFIG)
    export_data(lookup_tbl, edges_tbl)

    logger::log_info("Lookup table saved (rows: {nrow(lookup_tbl)})")
    logger::log_info("Edge metadata saved (rows: {nrow(edges_tbl)})")

    logger::log_info(
      "===== Lookup Table Construction Completed Successfully ====="
    )
  }, error = function(e) {
    logger::log_error("Fatal error: {e$message}")
    stop(e)
  }, finally = {
    logger::log_info("Script finished at {Sys.time()}")
  })
}

# Execute when run directly -------------------------------------------------
if (sys.nframe() == 0) {
  main()
}
