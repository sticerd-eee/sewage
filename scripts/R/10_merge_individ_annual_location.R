############################################################
# Merge location data with individual spill data
# Project: Sewage
# Date: 18/05/2025
# Author: Jacopo Olivieri
############################################################

#' This script merges the location data from the annual sewage overflow datasets
#' with the individual spill data.

# Setup Functions
############################################################

#' Initialize the R environment with required packages
#' @return NULL
initialise_environment <- function() {
  # Package management with renv
  if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv")
    renv::init()
  }

  # Define required packages
  required_packages <- c(
    "rmarkdown", "rio", "tidyverse", "purrr", "here", "logger", "glue",
    "fs", "conflicted", "reclin2", "data.table", "assertthat"
  )

  # Install and load packages
  invisible(sapply(required_packages, function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }))
  conflict_prefer("flatten", "purrr")
  conflict_prefer("filter", "dplyr")
}

#' Configure logging for the script
#' @return NULL
setup_logging <- function() {
  log_dir <- here::here("output", "log")
  fs::dir_create(log_dir, recurse = TRUE)
  log_path <- file.path(log_dir, "10_merge_individ_annual_location.log")

  logger::log_appender(logger::appender_tee(log_path))
  logger::log_layout(logger::layout_glue_colors)
  logger::log_threshold(logger::INFO)
  logger::log_info("Script started at {Sys.time()}")
}


# Configuration
############################################################

CONFIG <- list(
  years = 2021:2023,
  data_path_annual = here::here(
    "data", "processed", "annual_return_edm.parquet"
  ),
  data_path_individual = here::here(
    "data", "processed", "combined_edm_data.parquet"
  ),
  data_path_lookup = here::here(
    "data", "processed", "annual_return_lookup.parquet"
  ),
  id_cols = c(
    "site_name_ea", "site_name_wa_sc",
    "permit_reference_ea", "permit_reference_wa_sc",
    "activity_reference", "asset_type", "unique_id",
    "site_id_2021", "site_id_2022", "site_id_2023",
    "site_id_2024"
  ),
  block_var = "blocking_var",
  threshold = 0.5,
  match_cols = c(
    "match_method", "match_type", "match_key", "key_length", "match_quality"
  ),
  output_dir = here::here("data", "processed", "matched_events_annual_data"),
  keep_cols = c(
    "site_id", # "site_id_2021", "site_id_2022", "site_id_2024", "site_id_2024",
    "match_method", "match_quality", "match_type", "match_key", "key_length", "tie",
    "water_company", "year",
    "site_name_ea", "site_name_wa_sc",
    "permit_reference_ea", "permit_reference_wa_sc",
    "activity_reference", "asset_type",
    "ngr", "ngr_og",
    "unique_id",
    "start_time", "end_time",
    "spill_hrs_ea", "spill_count_ea",
    "wfd_waterbody_id_cycle_2", "receiving_water_name",
    "shellfish_water", "bathing_water",
    "edm_commission_date", "edm_operation_percent",
    "data_source", "no_full_years_edm_data",
    "group_id_event", "group_id_annual"
  )
)

# Data Preparation
############################################################

#' Load the individual spill events data annual spill returns data
#' @return A list containing two data frames: spill_events and annual_returns.
load_data <- function() {
  logger::log_info("Loading EDM datasets: individual spill events and annual returns.")

  # Define file paths using configuration
  event_path <- fs::path(CONFIG$data_path_individual)
  annual_path <- fs::path(CONFIG$data_path_annual)

  # Check if both files exist before attempting to load
  files <- c(event_path, annual_path)
  missing_files <- files[!file.exists(files)]
  if (length(missing_files) > 0) {
    error_msg <- glue::glue(
      "Input files not found: {paste(missing_files, collapse = ', ')}"
    )
    logger::log_error(error_msg)
    stop(error_msg)
  }

  # Load the data and return
  tryCatch(
    {
      spill_event_df <- arrow::read_parquet(event_path) %>%
        filter(year %in% CONFIG$years)
      annual_return_df <- arrow::read_parquet(annual_path) %>%
        filter(year %in% CONFIG$years)
      logger::log_info("Data loaded successfully.")
    },
    error = function(e) {
      error_msg <- glue::glue("Failed to load data: {e$message}")
      logger::log_error(error_msg)
      stop(error_msg)
    }
  )

  return(list(
    spill_events = spill_event_df,
    annual_returns = annual_return_df
  ))
}

# Many-to-One Windfall Matching
############################################################

#' Generate All Non-Empty Combinations of Shared Column Names
#' @param shared_cols Column names common to both datasets.
#' @return A list where each element is a character vector representing a
#'   potential key combination. Returns an empty list if input is empty.
generate_key_combinations <- function(shared_cols) {
  if (length(shared_cols) == 0) {
    return(list())
  }

  # Create combinations for each size from length(cols) down to 1
  seq_along(shared_cols) %>%
    rev() %>%
    map(~ combn(shared_cols, .x, simplify = FALSE)) %>%
    flatten()
}

#' Find Keys Present in Individual DF and Unique in Year DF
#' Identifies key values existing in event_df and appearing exactly once
#' in annual_df for a specific key combination.
#' @param event_df The individual spill event data frame.
#' @param annual_df The annual return data frame (summarised by site/year).
#' @param key A character vector representing the column names to use as the key.
#' @return A tibble containing the key columns for rows matching the criteria.
#'   Returns an empty tibble with the correct structure if no such keys are found.
find_matchable_keys <- function(event_df, annual_df, key) {
  if (!all(key %in% names(event_df)) || !all(key %in% names(annual_df))) {
    stop("Key columns must exist in both data frames: ", paste(key, collapse = ", "))
  }

  if (nrow(event_df) == 0 || nrow(annual_df) == 0 || length(key) == 0) {
    return(event_df %>% slice(0) %>% select(all_of(key)))
  }

  # Count occurrences of key values in both dataframes
  event_counts <- event_df %>%
    count(across(all_of(key)), name = "n_event", .drop = FALSE)
  annual_counts <- annual_df %>%
    count(across(all_of(key)), name = "n_annual", .drop = FALSE)

  # Identify keys that are unique in the annual (reference) data
  annual_unique_keys <- annual_counts %>%
    filter(n_annual == 1) %>%
    select(all_of(key))

  # Find the final set of keys to use for matching in this iteration.
  matchable_keys <- inner_join(
    event_counts %>% select(all_of(key)),
    annual_unique_keys,
    by = key,
    na_matches = "never"
  )

  return(matchable_keys)
}

#' Extract Matching Subsets, Join, Add Metadata, and Coalesce Columns
#' Extracts rows corresponding to `matchable_keys`, joins them,
#' adds match metadata, then coalesces duplicated columns (prioritising annual data).
#' @param event_df The current individual spill event data subset.
#' @param annual_df The current annual return data subset.
#' @param matchable_keys A tibble of keys identified by `find_matchable_keys`.
#' @param key A character vector representing the column names used as the key.
#' @param event_suffix Suffix for overlapping non-key columns from event_df.
#' @param annual_suffix Suffix for overlapping non-key columns from annual_df.
#' @return A tibble containing the joined rows with added metadata and coalesced columns.
extract_join_tag <- function(
    event_df, annual_df,
    matchable_keys, key,
    event_suffix = "_event", annual_suffix = "_annual") {
  # Extract the matching rows
  event_sub <- dplyr::semi_join(event_df, matchable_keys, by = key, na_matches = "never")
  annual_sub <- dplyr::semi_join(annual_df, matchable_keys, by = key, na_matches = "never")

  # Join and tag
  matched_sub <- dplyr::inner_join(
    event_sub, annual_sub,
    by = key,
    na_matches = "never",
    suffix = c(event_suffix, annual_suffix)
  ) %>%
    dplyr::mutate(
      match_method = "windfall",
      match_quality = 1,
      match_type = "unique",
      match_key = paste(key, collapse = "|"),
      key_length = length(key),
      .before = 1
    )

  # Identify overlapping base names from suffixed columns
  overlap_bases <- names(matched_sub)[grepl(paste0(event_suffix, "$"), names(matched_sub))] %>%
    sub(paste0(event_suffix, "$"), "", .)

  # Coalesce each pair, preferring annual values
  for (base in overlap_bases) {
    event_col <- paste0(base, event_suffix)
    annual_col <- paste0(base, annual_suffix)
    if (annual_col %in% names(matched_sub) && event_col %in% names(matched_sub)) {
      matched_sub[[base]] <- dplyr::coalesce(
        matched_sub[[annual_col]],
        matched_sub[[event_col]]
      )
    }
  }

  # Drop the original suffixed columns
  matched_sub <- matched_sub %>%
    dplyr::select(
      -dplyr::ends_with(event_suffix),
      -dplyr::ends_with(annual_suffix)
    )

  return(matched_sub)
}

#' Find Keys Present in Individual DF and Unique in Year DF
#' Identifies key values existing in event_df and appearing exactly once
#' in annual_df for a specific key combination.
#' @param event_df The individual spill event data frame.
#' @param annual_df The annual return data frame (summarised by site/year).
#' @param key A character vector representing the column names to use as the key.
#' @return A tibble containing the key columns for rows matching the criteria.
#'   Returns an empty tibble with the correct structure if no such keys are found.
find_matchable_keys <- function(event_df, annual_df, key) {
  if (!all(key %in% names(event_df)) || !all(key %in% names(annual_df))) {
    stop("Key columns must exist in both data frames: ", paste(key, collapse = ", "))
  }

  if (nrow(event_df) == 0 || nrow(annual_df) == 0 || length(key) == 0) {
    return(event_df %>% slice(0) %>% select(all_of(key)))
  }

  # Count occurrences of key values in both dataframes
  event_counts <- event_df %>%
    count(across(all_of(key)), name = "n_event", .drop = FALSE)
  annual_counts <- annual_df %>%
    count(across(all_of(key)), name = "n_annual", .drop = FALSE)

  # Identify keys that are unique in the annual (reference) data
  annual_unique_keys <- annual_counts %>%
    filter(n_annual == 1) %>%
    select(all_of(key))

  # Find the final set of keys to use for matching in this iteration.
  matchable_keys <- inner_join(
    event_counts %>% select(all_of(key)),
    annual_unique_keys,
    by = key,
    na_matches = "never"
  )

  return(matchable_keys)
}

#' Iteratively Join Data Frames Prioritising Larger, Unique-Right Keys
#' Performs a prioritised join between two data frames based on shared columns.
#' Iterates through key combinations (largest first), matching rows where
#' the key is unique in the annual data frame. Matched rows are removed for
#' subsequent iterations.
#' @param event_df The individual spill event data frame for the current group.
#' @param annual_df The annual return data frame for the current group.
#' @param event_suffix Suffix for overlapping non-key
#' @param annual_suffix Suffix for overlapping non-key
#' @return A list containing matched_data, events_unmatched, & annual_unmatched
run_windfall_match <- function(
    event_df,
    annual_df,
    event_suffix = "_event",
    annual_suffix = "_annual") {
  shared_cols <- intersect(names(event_df), names(annual_df))
  key_list <- generate_key_combinations(shared_cols)

  # Initialise state variables
  remaining_events <- event_df
  remaining_annual <- annual_df
  all_matched_data <- tibble()

  # Loop through potential keys, largest combinations first
  for (key in key_list) {
    # Stop if either dataset has no remaining rows
    if (nrow(remaining_events) == 0 || nrow(remaining_annual) == 0) {
      break
    }

    # Keys unique in the remaining annual data and present in remaining event data
    matchable_keys <- find_matchable_keys(
      remaining_events, remaining_annual, key
    )

    # If suitable keys are found for this iteration...
    if (nrow(matchable_keys) > 0) {
      # Extract, join, and tag the subset corresponding to these keys
      matched_sub <- extract_join_tag(
        remaining_events, remaining_annual,
        matchable_keys, key,
        event_suffix, annual_suffix
      )

      # Accumulate the matches
      all_matched_data <- bind_rows(all_matched_data, matched_sub)

      # Remove the matched rows from the pools for the next iteration
      remaining_events <- anti_join(
        remaining_events, matchable_keys,
        by = key, na_matches = "never"
      )
      remaining_annual <- anti_join(
        remaining_annual, matchable_keys,
        by = key, na_matches = "never"
      )
    }
  }

  # Return all components
  list(
    matched_data      = all_matched_data,
    events_unmatched  = remaining_events,
    annual_unmatched  = remaining_annual
  )
}

#' Add Unmatched Zero-Spill Annual Records and Separate Non-Zero Unmatched
#' @param matched_data A data frame or tibble containing the successfully matched
#' records from `run_windfall_match`.
#' @param unmatched_annual_data A data frame or tibble containing the annual
#' records that were not matched during `run_windfall_match`
#' (i.e., `$annual_unmatched`). Must contain `spill_hrs_ea` and `spill_count_ea` columns.
#' @return A list containing two tibbles:
#'   \item{matched_data_with_zeros}{The input `matched_data` with zero-spill rows
#'     from `unmatched_annual_data` appended.}
#'   \item{annual_unmatched_nonzero}{A subset of `unmatched_annual_data` containing
#'     only rows where `spill_hrs_ea != 0` or `spill_count_ea != 0`.}
add_zero_spill_stats <- function(matched_data, unmatched_annual_data) {
  # Filter the *unmatched* annual data for zero-spill records
  annual_data_zero <- unmatched_annual_data %>%
    filter(spill_hrs_ea == 0 & spill_count_ea == 0) %>%
    mutate(
      match_method = "windfall",
      match_quality = 1,
      match_type = "unique",
      match_key = "",
      key_length = 0,
      .before = 1
    )

  # Filter the *unmatched* annual data for non-zero spill records
  annual_data_nonzero <- unmatched_annual_data %>%
    filter(spill_hrs_ea != 0 | spill_count_ea != 0)

  # Append the zero-spill records (if any) to the main matched dataset
  if (nrow(annual_data_zero) > 0) {
    matched_data_with_zeros <- bind_rows(matched_data, annual_data_zero)
  } else {
    matched_data_with_zeros <- matched_data
  }

  # Return the combined data and the separated non-zero unmatched annual records
  list(
    matched_data_with_zeros = matched_data_with_zeros,
    annual_unmatched_nonzero = annual_data_nonzero
  )
}

merge_events_with_annual_returns <- function(spill_event_df, annual_return_df) {
  # Create combinations of years and water companies
  company_year_combinations <- expand_grid(
    water_company = unique(annual_return_df$water_company),
    year = CONFIG$years
  ) %>%
    arrange(year, water_company)

  logger::log_info("Starting matching process for company-year combinations.")

  # Iterate over each company-year subset
  match_results_list <- pmap(
    list(
      company_year_combinations$year,
      company_year_combinations$water_company
    ),
    ~ {
      # Assign arguments to named variables for clarity
      current_year <- ..1
      current_company <- ..2
      logger::log_debug(
        "Processing: Year {current_year}, Company {current_company}"
      )

      # Filter data for current company and year
      event_df_sub <- spill_event_df %>%
        filter(water_company == current_company, year == current_year) %>%
        select(where(~ any(!is.na(.))))
      annual_return_df_sub <- annual_return_df %>%
        filter(water_company == current_company, year == current_year) %>%
        select(where(~ any(!is.na(.))), any_of(c("spill_hrs_ea", "spill_count_ea")))

      # Perform the windfall matching on the subsets
      windfall_res <- run_windfall_match(
        event_df = event_df_sub,
        annual_df = annual_return_df_sub,
        event_suffix = "_event",
        annual_suffix = "_annual"
      )

      # Add unmatched zero-spill annual records and separate non-zero unmatched
      final_group_results <- add_zero_spill_stats(
        matched_data = windfall_res$matched_data,
        unmatched_annual_data = windfall_res$annual_unmatched
      )

      # Return results
      list(
        # Matched event data
        matched_combined = final_group_results$matched_data_with_zeros,
        # Event records that found no match in the annual data
        events_unmatched = windfall_res$events_unmatched,
        # Annual records that found no match AND had non-zero spills
        annual_unmatched_nonzero = final_group_results$annual_unmatched_nonzero
      )
    }
  )

  logger::log_info("Finished matching process.")

  # Combine all matched dataframes into a single dataframe
  return(list(
    matched_combined = rbindlist(
      map(match_results_list, ~ .x$matched_combined),
      fill = TRUE
    ),
    events_unmatched = rbindlist(
      map(match_results_list, ~ .x$events_unmatched),
      fill = TRUE
    ),
    annual_unmatched_nonzero = rbindlist(
      map(match_results_list, ~ .x$annual_unmatched_nonzero),
      fill = TRUE
    )
  ))
}

# Many-to-Many Windfall Matching
############################################################

## Many-to-Many Matching by Company-Year
#' @param events_unmatched tibble of spill events remaining after unique windfall matching
#' @param annual_unmatched_nonzero tibble of annual records remaining after windfall
#' @return A list with three tibbles:
#'   - matches: retained event→annual pairs (one per event)
#'   - events_unmatched: events still without a match
#'   - annual_unmatched: annual records never selected
#' @param events_unmatched tibble of spill events remaining after unique windfall matching
#' @param annual_unmatched_nonzero tibble of annual records (spill_hrs_ea>0 or spill_count_ea>0) remaining after windfall
#' @param keep_joined_losses Logical; if TRUE, retains annual records that joined but lost the max selection.
#'                           If FALSE (default), only annual records never joined are kept in the remaining set.
#' @return A list with three tibbles:
#'   - matches: retained event→annual pairs (one per event)
#'   - events_unmatched: events still without a match
#'   - annual_unmatched: annual records never selected (subject to `keep_joined_losses`)
run_max_match <- function(
    events_unmatched,
    annual_unmatched_nonzero,
    keep_joined_losses = FALSE) {
  # prepare datasets and identify shared columns
  df_left <- events_unmatched %>% select(where(~ any(!is.na(.))))
  df_right <- annual_unmatched_nonzero %>% select(where(~ any(!is.na(.))))
  shared_cols <- intersect(names(df_left), names(df_right))

  # tag rows for later unmatched detection
  df_left <- df_left %>% tibble::rowid_to_column("event_row_id")
  df_right <- df_right %>% tibble::rowid_to_column("annual_row_id")

  # inner join on all shared columns (explicit many-to-many)
  joined_all <- inner_join(
    df_left, df_right,
    by = shared_cols,
    suffix = c("_event", "_annual"),
    na_matches = "never",
    relationship = "many-to-many"
  ) %>%
    mutate(
      match_method = "max",
      match_type   = "many-to-many",
      match_key    = paste(shared_cols, collapse = "|"),
      key_length   = length(shared_cols)
    )

  # compute tie indicator robustly: handle all-NA cases
  tie_info <- joined_all %>%
    group_by(event_row_id) %>%
    summarise(
      max_hrs = if (all(is.na(spill_hrs_ea))) NA_real_ else max(spill_hrs_ea, na.rm = TRUE),
      max_count = if (is.na(max_hrs)) {
        if (all(is.na(spill_count_ea))) NA_real_ else max(spill_count_ea, na.rm = TRUE)
      } else {
        max(spill_count_ea[spill_hrs_ea == max_hrs], na.rm = TRUE)
      },
      tie = sum(spill_hrs_ea == max_hrs & spill_count_ea == max_count, na.rm = TRUE) > 1,
      .groups = "drop"
    ) %>%
    select(event_row_id, tie)

  # select the single best record per event: highest spill_hrs_ea, then highest spill_count_ea
  best_matches <- joined_all %>%
    group_by(event_row_id) %>%
    arrange(desc(spill_hrs_ea), desc(spill_count_ea)) %>%
    slice(1) %>%
    ungroup() %>%
    left_join(tie_info, by = "event_row_id")

  # detect unmatched events
  events_remaining <- df_left %>%
    filter(!event_row_id %in% best_matches$event_row_id) %>%
    select(-event_row_id)

  # detect remaining annuals, conditional on keep_joined_losses
  if (keep_joined_losses) {
    # keep all non-selected
    annual_remaining <- df_right %>%
      filter(!annual_row_id %in% best_matches$annual_row_id)
  } else {
    # keep only those never joined at all
    annual_remaining <- df_right %>%
      filter(!annual_row_id %in% joined_all$annual_row_id)
  }
  annual_remaining <- annual_remaining %>% select(-annual_row_id)

  # drop row IDs from matches
  matches <- best_matches %>% select(-event_row_id, -annual_row_id)

  list(
    matches          = matches,
    events_unmatched = events_remaining,
    annual_unmatched = annual_remaining
  )
}

#' Many-to-Many Matching then Max(spill_hrs_ea, spill_count_ea) by Company-Year
#' @param events_unmatched tibble of spill events remaining
#' @param annual_unmatched_nonzero tibble of annual records remaining
#' @param keep_joined_losses Logical; passed to `run_max_match`
#' @return A list with three tibbles:
#'   - max_matches_all: retained event→annual pairs (one per event)
#'   - events_unmatched: events still without a match
#'   - annual_unmatched: annual records never selected
run_many_to_many_max_match <- function(
    events_unmatched,
    annual_unmatched_nonzero,
    keep_joined_losses = FALSE) {
  company_years <- expand_grid(
    water_company = unique(events_unmatched$water_company),
    year          = unique(events_unmatched$year)
  ) %>% arrange(year, water_company)

  results <- pmap(
    list(company_years$year, company_years$water_company),
    ~ {
      current_year <- ..1
      current_company <- ..2

      ev_sub <- events_unmatched %>%
        filter(water_company == current_company, year == current_year) %>%
        select(where(~ any(!is.na(.))))
      an_sub <- annual_unmatched_nonzero %>%
        filter(water_company == current_company, year == current_year) %>%
        select(where(~ any(!is.na(.))), any_of(c("spill_hrs_ea", "spill_count_ea")))

      if (nrow(ev_sub) == 0 || nrow(an_sub) == 0) {
        return(list(
          max_matches = ev_sub[0, ],
          events_unmatched = ev_sub,
          annual_unmatched = an_sub
        ))
      }

      mm <- run_max_match(
        events_unmatched         = ev_sub,
        annual_unmatched_nonzero = an_sub,
        keep_joined_losses       = keep_joined_losses
      )
      # bind all group-level results into master tibbles
      list(
        max_matches = mm$matches,
        events_unmatched = mm$events_unmatched,
        annual_unmatched = mm$annual_unmatched
      )
    }
  )

  list(
    max_matches_all  = rbindlist(map(results, "max_matches"), fill = TRUE),
    events_unmatched = rbindlist(map(results, "events_unmatched"), fill = TRUE),
    annual_unmatched = rbindlist(map(results, "annual_unmatched"), fill = TRUE)
  )
}

# Fuzzy Matching
############################################################

#' Prepare data for fuzzy matching by combining, grouping, and blocking.
#' @param events_df A data frame of individual spill events.
#' @param annual_df A data frame of annual spill returns.
#' @param vars Character vector of columns to include (e.g., identifiers and blocking variables).
#' @param id_cols Character vector of columns used to define unique records (defaults to `vars`).
#' @return A named list of four data.tables: `events`, `annual`, `events_full`, `annual_full`.
prepare_fuzzy_data <- function(events_df, annual_df, block_var, id_cols) {
  if (!all(c("water_company", "year") %in% names(events_df))) {
    stop("events_df must contain 'water_company' and 'year'")
  }
  if (!all(c("water_company", "year") %in% names(annual_df))) {
    stop("annual_df must contain 'water_company' and 'year'")
  }

  dfs <- list(events = events_df, annual = annual_df)
  prep <- lapply(dfs, function(df) {
    # assign group_id and blocking key
    full <- df %>%
      group_by(across(any_of(id_cols))) %>%
      mutate(group_id = cur_group_id()) %>%
      ungroup() %>%
      mutate(!!block_var := paste(water_company, year, sep = "_"), .before = 1)

    # minimal (deduped) version
    minimal <- full %>%
      select(any_of(id_cols), group_id, !!block_var) %>%
      distinct()

    list(
      full = as.data.table(full),
      minimal = as.data.table(minimal)
    )
  })

  # assemble into named list
  list(
    events        = prep$events$minimal,
    annual        = prep$annual$minimal,
    events_full   = prep$events$full,
    annual_full   = prep$annual$full
  )
}

#' Create candidate record pairs using blocking.
#' @param prepped A list returned by `prepare_fuzzy_data`.
#' @param block_var The blocking variable used to link x and y.
#' @return A data.table of candidate pairs.
create_candidate_pairs <- function(prepped, block_var) {
  logger::log_debug("Creating candidate pairs with block variable '{block_var}'")
  pairs <- pair_blocking(
    x      = prepped$events,
    y      = prepped$annual,
    on     = block_var,
    add_xy = TRUE
  )
  logger::log_info("Generated {nrow(pairs)} candidate pairs")
  pairs
}

#' Compare candidate pairs using fuzzy comparators.
#' @param pairs A data.table of candidate pairs.
#' @param fuzzy_vars Character vector of columns to compare.
#' @return The same data.table, updated in place with comparison columns.
compare_candidate_pairs <- function(pairs, fuzzy_vars) {
  logger::log_debug("Comparing candidate pairs on vars: {paste(fuzzy_vars, collapse = ', ')}")
  compare_pairs(
    pairs,
    on                  = fuzzy_vars,
    default_comparator  = reclin2::cmp_jarowinkler(threshold = 0.85),
    inplace             = TRUE
  )
  invisible(pairs)
}

#' Score candidate pairs using the EM algorithm for probabilistic linkage.
#' @param pairs A data.table of compared pairs.
#' @param fuzzy_vars Character vector used in EM formula.
#' @return The data.table with added posterior match probabilities (`mpost`).
score_pairs_em <- function(pairs, fuzzy_vars) {
  # Ensure there are pairs to score
  if (nrow(pairs) == 0) {
    logger::log_warn("No pairs to score with EM algorithm")
    return(pairs)
  }
  # Catch EM failures gracefully
  tryCatch(
    {
      fs_formula <- reformulate(fuzzy_vars)
      m_em <- problink_em(formula = fs_formula, data = pairs)
      predict(
        m_em,
        pairs   = pairs,
        type    = "mpost",
        add     = TRUE,
        binary  = TRUE,
        inplace = TRUE
      )
    },
    error = function(e) {
      logger::log_error("EM model failed: {e$message}")
      pairs[, mpost := 0]
      pairs
    }
  )
}

#' Select matching pairs based on posterior probabilities, n-to-m settings, and threshold.
#' @param pairs A data.table scored by `score_pairs_em`.
#' @param threshold Numeric cutoff for selecting matches (e.g., 0.2).
#' @param n Integer max matches per record in x (events).
#' @param m Integer max matches per record in y (annual).
#' @param include_ties Logical, whether to include ties at the threshold.
#' @return The data.table updated in place with a logical `selected_match` column.
select_matches <- function(
    pairs,
    threshold = CONFIG$threshold,
    n,
    m,
    include_ties = TRUE) {
  if (!is.numeric(threshold) || threshold <= 0 || threshold >= 1) {
    stop("'threshold' must be a numeric value between 0 and 1")
  }
  select_n_to_m(
    pairs,
    variable     = "selected_match",
    score        = "mpost",
    threshold    = threshold,
    n            = n,
    m            = m,
    include_ties = include_ties,
    inplace      = TRUE
  )
  n_sel <- sum(pairs$selected_match, na.rm = TRUE)
  logger::log_info(
    "Selected {n_sel} matches with threshold {threshold}, n={n}, m={m}, include_ties={include_ties}"
  )
  invisible(pairs)
}

#' Link the selected fuzzy matches, adding match metadata and suffixes.
#' @param pairs A data.table after `select_matches` with `selected_match` flags.
#' @return A tibble of linked records with match metadata columns.
link_selected_pairs <- function(pairs) {
  logger::log_debug("Linking selected pairs")
  linked <- link(
    pairs,
    selection       = "selected_match",
    suffixes        = c("_event", "_annual"),
    keep_from_pairs = "mpost",
    all_x           = FALSE,
    all_y           = FALSE
  ) %>%
    as_tibble() %>%
    rename(match_quality = mpost) %>%
    mutate(
      match_method = "fuzzy",
      match_type   = "unique",
      match_key    = "fuzzy",
      key_length   = NA_integer_,
      .before      = 1
    )
  logger::log_info("Linked {nrow(linked)} fuzzy match records")
  linked
}

#' Performs fuzzy linkage within that block and aggregates the results
#' @param final_res A list with elements `matched_combined`, `events_unmatched` and
#'   `annual_unmatched_nonzero`
#' @param threshold Cutoff (0–1) for posterior match probability
#' @param n Maximum matches per event record
#' @param m Maximum matches per annual record
#' @param include_ties If TRUE, include any ties at the threshold
#' @param block_var Column name defining each block (e.g. "company_year")
#' @return A list with updated `matched_combined`, `events_unmatched` and
#'   `annual_unmatched_nonzero`.
run_fuzzy_matching <- function(
    final_res,
    threshold = CONFIG$threshold,
    n,
    m,
    include_ties = TRUE,
    block_var = CONFIG$block_var) {
  # validate input structure
  assert_that(is.list(final_res), msg = "final_res must be a list")
  required <- c("matched_combined", "events_unmatched", "annual_unmatched_nonzero")
  if (!all(required %in% names(final_res))) {
    stop("final_res missing: ", paste(setdiff(required, names(final_res)), collapse = ", "))
  }

  # prepare blocking + group IDs
  id_cols <- c("water_company", "year", CONFIG$id_cols)
  prepped <- prepare_fuzzy_data(
    events_df = final_res$events_unmatched,
    annual_df = final_res$annual_unmatched_nonzero,
    block_var = block_var,
    id_cols   = id_cols
  )

  # perform fuzzy matching by block
  blocks <- unique(prepped$events[[block_var]])
  fuzzy_merges <- purrr::map(blocks, function(b) {
    # subset to current block and drop all‐NA columns
    ev <- filter(prepped$events, .data[[block_var]] == b) %>% select(where(~ any(!is.na(.))))
    an <- filter(prepped$annual, .data[[block_var]] == b) %>% select(where(~ any(!is.na(.))))
    if (nrow(ev) == 0 || nrow(an) == 0) {
      return(NULL)
    }

    # identify comparators available in both subsets
    shared <- intersect(names(ev), names(an))
    vars_block <- setdiff(shared, c(block_var, "source", "group_id", "water_company", "year"))
    if (length(vars_block) == 0) {
      return(NULL)
    }

    # generate and score candidate pairs
    pairs <- create_candidate_pairs(list(events = ev, annual = an), block_var)
    if (nrow(pairs) == 0) {
      return(NULL)
    }

    compare_candidate_pairs(pairs, vars_block)
    score_pairs_em(pairs, vars_block)
    select_matches(pairs, threshold, n, m, include_ties)

    # link and return minimal join keys
    link_selected_pairs(pairs) %>%
      select(all_of(CONFIG$match_cols), group_id_event, group_id_annual)
  }) %>%
    purrr::compact() %>%
    bind_rows()

  # reconstruct matched + unmatched sets
  matched_combined <- fuzzy_merges %>%
    left_join(prepped$annual_full, by = c("group_id_annual" = "group_id")) %>%
    left_join(prepped$events_full,
      by = c("group_id_event" = "group_id"),
      suffix = c("", "_dup")
    ) %>%
    select(-ends_with("_dup")) %>%
    bind_rows(final_res$matched_combined, .)

  events_unmatched <- anti_join(
    prepped$events_full, fuzzy_merges,
    by = c("group_id" = "group_id_event")
  )
  annual_unmatched <- anti_join(
    prepped$annual_full, fuzzy_merges,
    by = c("group_id" = "group_id_annual")
  )

  list(
    matched_combined = matched_combined,
    events_unmatched = events_unmatched,
    annual_unmatched_nonzero = annual_unmatched
  )
}

# Cleaning Results
############################################################

#' Finalise merged data: append discharge coordinates and select output columns
#' @param fuzzy_results A list containing:
#'   \describe{
#'     \item{matched_combined}{Matched event–annual pairs}
#'     \item{events_unmatched}{Unmatched spill events}
#'     \item{annual_unmatched_nonzero}{Unmatched annual records with spills}
#'   }
#' @return A list containing the same elements as input, plus:
#'   \itemize{
#'     \item{`matched_combined` has a new `ngr` column with coalesced per‑year discharge NGRs.}
#'     \item{`site_metadata` contains distinct site information derived from the enriched matched data, excluding time columns.}
#'     \item{All elements except `site_metadata` are limited to `CONFIG$keep_cols`. `site_metadata` retains all its derived columns.}
#'   }
finalise_merged_data <- function(fuzzy_results) {
  # Load lookup and annual returns
  lookup <- import(CONFIG$data_path_lookup)
  annual_returns <- import(CONFIG$data_path_annual)

  # Build lookup with per-year outlet discharge NGR
  years_lookup <- 2021:2024
  lookup <- purrr::reduce(
    years_lookup,
    .init = lookup,
    function(acc, yr) {
      year_id <- paste0("site_id_", yr)
      ngr_col <- paste0("outlet_discharge_ngr_", yr)
      coords <- annual_returns %>%
        filter(year == yr) %>%
        select({{ year_id }}, outlet_discharge_ngr) %>%
        rename({{ ngr_col }} := outlet_discharge_ngr)
      left_join(acc, coords, by = year_id, na_matches = "never")
    }
  )

  # Enrich matched_combined for each year, then combine
  enriched <- purrr::map(
    CONFIG$years,
    function(y) {
      id_col <- paste0("site_id_", y)
      other_ids <- setdiff(paste0("site_id_", years_lookup), id_col)
      fuzzy_results$matched_combined %>%
        filter(!is.na(.data[[id_col]])) %>%
        select(-any_of(other_ids)) %>%
        left_join(lookup, by = id_col)
    }
  ) %>%
    rbindlist(., use.names = TRUE, fill = TRUE) %>%
    rename(ngr_og = outlet_discharge_ngr) %>%
    mutate(
      ngr = coalesce(
        outlet_discharge_ngr_2024,
        outlet_discharge_ngr_2023,
        outlet_discharge_ngr_2022,
        outlet_discharge_ngr_2021
      )
    )

  fuzzy_results$matched_combined <- enriched
  fuzzy_results$annual_unmatched_nonzero <- fuzzy_results$annual_unmatched_nonzero %>%
    rename(ngr_og = outlet_discharge_ngr)

  # Trim all list elements to desired columns
  final_list <- purrr::map(
    fuzzy_results,
    ~ dplyr::select(.x, any_of(CONFIG$keep_cols))
  )

  # Add metadata
  final_list$site_metadata <- final_list$matched_combined %>%
    select(-start_time, -end_time) %>%
    distinct()

  return(final_list)
}


# Exporting Results
############################################################

#' Export function to save merge results
#' @param final_results List containing `matched_combined`, `events_unmatched`,
#'   `annual_unmatched_nonzero`, and `site_metadata` elements.
#' @return NULL
export_results <- function(final_results) {
  output_dir <- CONFIG$output_dir

  if (!dir.exists(output_dir)) {
    fs::dir_create(output_dir, recurse = TRUE)
    logger::log_info("Created output directory: {output_dir}")
  }

  # Define file paths
  matched_path <- file.path(output_dir, "matched_events_annual_data.parquet")
  events_unmatched_path <- file.path(output_dir, "events_unmatched.parquet")
  annual_unmatched_path <- file.path(output_dir, "annual_unmatched.parquet")
  site_metadata_path <- file.path(output_dir, "site_metadata.parquet")

  # Save the data files
  logger::log_info("Saving matched data to {matched_path}")
  arrow::write_parquet(final_results$matched_combined, matched_path)

  logger::log_info("Saving unmatched events to {events_unmatched_path}")
  arrow::write_parquet(final_results$events_unmatched, events_unmatched_path)

  logger::log_info("Saving unmatched annual data to {annual_unmatched_path}")
  arrow::write_parquet(final_results$annual_unmatched_nonzero, annual_unmatched_path)

  logger::log_info("Saving site metadata to {site_metadata_path}")
  arrow::write_parquet(final_results$site_metadata, site_metadata_path)

  logger::log_info("Export completed successfully")
}

# Main Execution
############################################################

#' Main execution function
main <- function() {
  # Initialize environment and set up logging
  initialise_environment()
  setup_logging()

  logger::log_info("Starting merge process for individual and annual location data")

  # Load data
  data_list <- load_data()
  spill_events <- data_list$spill_events
  annual_returns <- data_list$annual_returns

  # Run initial windfall matching (prioritized exact matching with uniqueness requirements)
  logger::log_info("Running windfall matching (1:1 exact matches)")
  windfall_res <- merge_events_with_annual_returns(spill_events, annual_returns)

  # Run many-to-many max matching for remaining observations
  logger::log_info("Running many-to-many max matching")
  max_match_res <- run_many_to_many_max_match(
    events_unmatched = windfall_res$events_unmatched,
    annual_unmatched_nonzero = windfall_res$annual_unmatched_nonzero
  )
  # Fuzzy matching for remaining observations
  logger::log_info("Running fuzzy matching for remaining records")
  fuzzy_results <- run_fuzzy_matching(
    final_res = list(
      matched_combined = bind_rows(
        windfall_res$matched_combined,
        max_match_res$max_matches_all
      ),
      events_unmatched = max_match_res$events_unmatched,
      annual_unmatched_nonzero = max_match_res$annual_unmatched
    ),
    n = 1, # One event can match at most one annual record
    m = 1 # One annual record can match at most one event
  )

  # Clean data
  logger::log_info("Finalising merged data")
  final_results <- finalise_merged_data(fuzzy_results)

  # Export data
  logger::log_info("Exporting results to {CONFIG$output_dir}")
  export_results(final_results)
  logger::log_info("Merge process completed successfully")
}

# Execute the main function when the script is sourced
if (sys.nframe() == 0) {
  main()
}
