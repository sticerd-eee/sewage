############################################################
# Annual Return Lookup: Random-Forest Matching Utilities
# Project: Sewage
# Date: 10/06/2026
# Author: Jacopo Olivieri
############################################################

#' Optional probabilistic (random forest) matching for the annual-return
#' lookup (scripts/R/03_data_enrichment/create_annual_return_lookup.R).
#'
#' This file is sourced lazily: main() loads it only when
#' compute_rf_matching = TRUE. RF matching stays OFF by default; enabling
#' it in production is a separate, documented decision gated on
#' scripts/R/testing/evaluate_rf_matching.R.
#'
#' Side effects: train_rf_linkage_model()/load_or_train_rf_model() write
#' the trained model to the model_file path the caller passes in
#' (CONFIG$rf_model_file). Nothing else writes to disk.
#'
#' Trainer design (downsampled, 2026-06-10 evaluation): materialising all
#' candidate pairs (25.7M) was SIGKILLed on 16 GB in ranger's OOB pass.
#' Training instead keeps, per company block: every known-match positive,
#' the hardest negatives (similarity above hard_negative_floor on any
#' evidence field, capped at ~hard_negative_ratio per positive), and
#' ~easy_negative_ratio sampled easy negatives per positive. This trains
#' in well under two minutes within 3 GB with equal holdout quality.
#' Undersampling inflates predicted
#' probabilities, so the default match threshold is 0.90, validated by
#' holdout (recovery precision ~1.0; false-link rate on partner-less
#' sites 0.3% vs 4.2% at the legacy 0.05 threshold).
#'
#' Labels are constructed explicitly from known matches: a pair is
#' "match" iff its (left, right) site IDs appear in known_matches, and
#' "non_match" otherwise - including pairs whose similarity is all-NA.
#' (The retired April 2025 model was trained with a poison-label bug and
#' must never be loaded; see load_or_train_rf_model().)
#'
#' Required packages (loaded by the calling script): dplyr, tibble,
#' stringr, purrr, glue, logger, data.table, reclin2, ranger.

# Evidence fields compared by the RF linkage model.
RF_MATCH_VARS <- c(
  "site_name_ea", "site_name_wa_sc", "permit_reference_ea",
  "permit_reference_wa_sc", "activity_reference", "outlet_discharge_ngr"
)

# Marker stored alongside saved models; load_or_train_rf_model() refuses
# to load model files that do not carry it.
RF_TRAINER_VERSION <- "downsampled_v2"

#' Compare candidate pairs on the evidence fields, in place
#'
#' Single comparator construction shared by training and scoring so the
#' model always predicts on features computed exactly as it was trained.
#' @param pairs reclin2 pairs object (modified in place)
#' @param match_vars Evidence fields to compare
#' @return The pairs object, invisibly
compare_rf_match_vars <- function(pairs, match_vars) {
  cmp_list <- lapply(match_vars, function(x) jaro_winkler_na())
  names(cmp_list) <- match_vars
  reclin2::compare_pairs(pairs,
    on = match_vars,
    default_comparator = jaro_winkler_na(),
    comparators = cmp_list, inplace = TRUE
  )
  invisible(pairs)
}

#' Create a Jaro-Winkler string comparator with NA handling
#' @param threshold Threshold for Jaro-Winkler distance
#' @param na_placeholder Placeholder for NA values
#' @return Comparator function that handles NA values
jaro_winkler_na <- function(threshold = 0.85, na_placeholder = "") {
  SCORE_BOTH_NA <- -1L
  SCORE_ONE_NA <- -2L
  base_comp <- reclin2::cmp_jarowinkler(threshold = threshold)

  function(x, y) {
    is_na_x <- is.na(x) | x == na_placeholder
    is_na_y <- is.na(y) | y == na_placeholder

    ifelse(is_na_x & is_na_y, SCORE_BOTH_NA,
      ifelse(is_na_x | is_na_y, SCORE_ONE_NA, base_comp(x, y))
    )
  }
}

#' Derive per-year unmatched site IDs from a windfall-only forest pass
#'
#' Runs the year-constrained spanning forest over the deterministic
#' matches WITHOUT building the full lookup or writing any audit files,
#' and returns the site IDs that no deterministic edge touches. Sites
#' whose every edge was dropped during resolution still count as matched
#' (they appear in the forest membership) - the same semantics as the
#' preliminary lookup pass this replaces.
#' @param match_dfs Named list of windfall match dataframes
#' @param data_list Named list of yearly dataframes
#' @param years Reporting years included in the lookup
#' @return Named list (by year) of character vectors of unmatched IDs
derive_windfall_unmatched_ids <- function(match_dfs, data_list, years) {
  edges_df <- build_weighted_edges(match_dfs)

  membership_tbl <- if (nrow(edges_df) == 0) {
    tibble(year = integer(), site_id = character())
  } else {
    build_year_constrained_spanning_forest(edges_df)$membership_tbl
  }

  out <- lapply(years, function(yr) {
    all_ids <- as.character(
      data_list[[paste0("df", yr)]][[paste0("site_id_", yr)]]
    )
    matched_ids <- membership_tbl$site_id[membership_tbl$year == yr]
    setdiff(all_ids, matched_ids)
  })
  names(out) <- as.character(years)
  out
}

#' Build a downsampled RF training set per company block
#'
#' Keeps, within each company block: every positive (known match), the
#' hardest negatives (non-matches at or above hard_negative_floor on at
#' least one evidence field, ranked by similarity and capped at
#' hard_negative_ratio per positive), and at most easy_negative_ratio
#' sampled easy negatives per positive. Comparison happens one company at
#' a time so the full candidate-pair matrix is never materialised.
#' @param df_left Left dataframe (one site_id_ column)
#' @param df_right Right dataframe (one site_id_ column)
#' @param known_matches Dataframe with both site_id_ columns of true links
#' @param match_vars Evidence fields to compare
#' @param blocking_var Variable to block on
#' @param hard_negative_floor Similarity floor for hard negatives
#' @param hard_negative_ratio Hard negatives kept per positive (hardest first)
#' @param easy_negative_ratio Easy negatives sampled per positive
#' @param seed RNG seed for the easy-negative sample
#' @return Dataframe of comparator features, training_role, and label
build_rf_training_pairs <- function(
    df_left,
    df_right,
    known_matches,
    match_vars = RF_MATCH_VARS,
    blocking_var = "water_company",
    hard_negative_floor = 0.85,
    hard_negative_ratio = 5,
    easy_negative_ratio = 10,
    seed = 20260610) {
  site_id_left <- grep("^site_id_", names(df_left), value = TRUE)
  site_id_right <- grep("^site_id_", names(df_right), value = TRUE)
  if (length(site_id_left) != 1 || length(site_id_right) != 1) {
    stop("df_left / df_right must carry exactly one site_id_ column each")
  }

  known_keys <- paste(
    as.character(known_matches[[site_id_left]]),
    as.character(known_matches[[site_id_right]]),
    sep = "::"
  )

  companies <- intersect(
    unique(df_left[[blocking_var]]),
    unique(df_right[[blocking_var]])
  )
  set.seed(seed)

  blocks <- lapply(companies, function(comp) {
    lb <- data.table::as.data.table(
      df_left[df_left[[blocking_var]] %in% comp, , drop = FALSE]
    )
    rb <- data.table::as.data.table(
      df_right[df_right[[blocking_var]] %in% comp, , drop = FALSE]
    )
    if (nrow(lb) == 0 || nrow(rb) == 0) {
      return(NULL)
    }

    pairs <- reclin2::pair_blocking(lb, rb, on = blocking_var)
    if (nrow(pairs) == 0) {
      return(NULL)
    }
    compare_rf_match_vars(pairs, match_vars)

    pair_tbl <- as.data.frame(pairs)
    pair_key <- paste(
      as.character(lb[[site_id_left]][pair_tbl$.x]),
      as.character(rb[[site_id_right]][pair_tbl$.y]),
      sep = "::"
    )
    is_positive <- pair_key %in% known_keys

    # NA sentinel codes (-1/-2) are not similarity evidence.
    sim <- as.data.frame(pair_tbl[match_vars])
    sim[sim < 0] <- NA
    max_sim <- do.call(pmax, c(sim, na.rm = TRUE))
    max_sim[is.na(max_sim)] <- 0

    training_role <- dplyr::case_when(
      is_positive ~ "positive",
      max_sim >= hard_negative_floor ~ "hard_negative",
      TRUE ~ "easy_negative"
    )

    block_budget <- max(1L, sum(is_positive))

    hard_idx <- which(training_role == "hard_negative")
    n_hard_keep <- min(
      length(hard_idx),
      ceiling(hard_negative_ratio * block_budget)
    )
    hard_keep <- hard_idx[order(max_sim[hard_idx], decreasing = TRUE)][
      seq_len(n_hard_keep)
    ]

    easy_idx <- which(training_role == "easy_negative")
    n_easy_keep <- min(
      length(easy_idx),
      ceiling(easy_negative_ratio * block_budget)
    )
    # sample.int avoids sample()'s scalar trap when only one easy negative exists
    easy_keep <- easy_idx[sample.int(length(easy_idx), n_easy_keep)]

    keep_idx <- sort(c(which(is_positive), hard_keep, easy_keep))

    out <- pair_tbl[keep_idx, match_vars, drop = FALSE]
    out$training_role <- training_role[keep_idx]
    out$training_block <- comp
    out
  })

  training_pairs <- dplyr::bind_rows(blocks)
  training_pairs$label <- factor(
    ifelse(training_pairs$training_role == "positive", "match", "non_match"),
    levels = c("non_match", "match")
  )
  training_pairs
}

#' Train a random-forest record-linkage model on the downsampled design
#' @param df_left Left dataframe
#' @param df_right Right dataframe
#' @param known_matches Dataframe of known matches between left and right
#' @param match_vars Evidence fields to compare
#' @param blocking_var Variable to block on
#' @param num_trees Number of trees in the random forest
#' @param max_depth Maximum depth of trees
#' @param num_threads Number of threads to use
#' @param oob_error Whether ranger computes out-of-bag error
#' @param hard_negative_floor Similarity floor for hard negatives
#' @param easy_negative_ratio Easy negatives sampled per positive
#' @param seed RNG seed for sampling and ranger
#' @param model_file Path to save the trained model (NULL skips saving)
#' @return Trained ranger probability model
train_rf_linkage_model <- function(
    df_left,
    df_right,
    known_matches,
    match_vars = RF_MATCH_VARS,
    blocking_var = "water_company",
    num_trees = 500,
    max_depth = 8,
    num_threads = 4,
    oob_error = TRUE,
    hard_negative_floor = 0.85,
    hard_negative_ratio = 5,
    easy_negative_ratio = 10,
    seed = 20260610,
    model_file = NULL) {
  logger::log_info("Building downsampled RF training pairs per company block")
  training_pairs <- build_rf_training_pairs(
    df_left = df_left,
    df_right = df_right,
    known_matches = known_matches,
    match_vars = match_vars,
    blocking_var = blocking_var,
    hard_negative_floor = hard_negative_floor,
    hard_negative_ratio = hard_negative_ratio,
    easy_negative_ratio = easy_negative_ratio,
    seed = seed
  )

  n_positive <- sum(training_pairs$label == "match")
  if (n_positive == 0) {
    stop("RF training requires at least one known-match positive pair")
  }
  logger::log_info(
    "Training RF on {nrow(training_pairs)} pairs ({n_positive} positives)"
  )

  formula_rf <- stats::as.formula(
    paste("label ~", paste(match_vars, collapse = " + "))
  )
  m_rf <- ranger::ranger(formula_rf,
    data = training_pairs,
    num.trees = num_trees,
    max.depth = max_depth,
    num.threads = num_threads,
    probability = TRUE,
    importance = "impurity",
    oob.error = oob_error,
    seed = seed
  )

  if (!is.null(model_file)) {
    dir.create(dirname(model_file), recursive = TRUE, showWarnings = FALSE)
    saveRDS(
      list(
        model = m_rf,
        importance = ranger::importance(m_rf),
        trainer = RF_TRAINER_VERSION
      ),
      model_file
    )
    logger::log_info("RF model saved to {model_file}")
  }

  if (isTRUE(oob_error)) {
    logger::log_info("RF training complete: OOB error = {m_rf$prediction.error}")
  }
  m_rf
}

#' Prepare data by sub-setting to unmatched IDs for RF matching
#' @param df Original dataframe
#' @param unmatched_ids Vector of unmatched IDs
#' @return Filtered and processed dataframe ready for RF matching
prepare_rf_data <- function(df, unmatched_ids) {
  site_id_col <- grep("^site_id_", names(df), value = TRUE)
  if (length(site_id_col) != 1) {
    stop("Unexpected site_id column configuration")
  }

  logger::log_info(
    "Preparing data for RF matching with {length(unmatched_ids)} unmatched IDs"
  )

  df %>%
    filter(.data[[site_id_col]] %in% unmatched_ids) %>%
    mutate(
      site_name_ea = if_else(
        is.na(site_name_ea) & !is.na(site_name_wa_sc),
        site_name_wa_sc, site_name_ea
      ),
      site_name_wa_sc = if_else(
        is.na(site_name_wa_sc) & !is.na(site_name_ea),
        site_name_ea, site_name_wa_sc
      )
    )
}

#' Split RF proposals into clean links and ambiguous monitor-multiples
#'
#' A proposal is ambiguous when the two rows agree exactly on every
#' evidence field AND that identifier combination covers multiple rows on
#' either side of the year pair: rows identical on every identifier are
#' distinct monitored discharge points of the same works, and any 1:1
#' pairing between them would be arbitrary.
#' @param matches RF match dataframe carrying both site_id_ columns
#' @param df_left Left candidate dataframe (RF-prepared)
#' @param df_right Right candidate dataframe (RF-prepared)
#' @param match_vars Evidence fields used by the model
#' @param site_id_left Name of the left site_id_ column
#' @param site_id_right Name of the right site_id_ column
#' @return List with matches (clean) and ambiguous (flagged) dataframes
flag_ambiguous_rf_proposals <- function(
    matches,
    df_left,
    df_right,
    match_vars,
    site_id_left,
    site_id_right) {
  if (nrow(matches) == 0) {
    return(list(matches = matches, ambiguous = matches))
  }

  annotate_side <- function(df, site_id_col, prefix, count_name) {
    df %>%
      add_count(across(all_of(match_vars)), name = count_name) %>%
      select(all_of(c(site_id_col, match_vars, count_name))) %>%
      rename_with(~ paste0(prefix, .x), all_of(match_vars))
  }

  annotated <- matches %>%
    left_join(
      annotate_side(df_left, site_id_left, "..lhs_", "..n_left_group"),
      by = site_id_left
    ) %>%
    left_join(
      annotate_side(df_right, site_id_right, "..rhs_", "..n_right_group"),
      by = site_id_right
    )

  same_evidence <- Reduce(`&`, lapply(match_vars, function(v) {
    l <- annotated[[paste0("..lhs_", v)]]
    r <- annotated[[paste0("..rhs_", v)]]
    (is.na(l) & is.na(r)) | (!is.na(l) & !is.na(r) & l == r)
  }))
  ambiguous_flag <- same_evidence &
    (annotated$..n_left_group > 1 | annotated$..n_right_group > 1)

  list(
    matches = matches[!ambiguous_flag, , drop = FALSE],
    ambiguous = matches[ambiguous_flag, , drop = FALSE]
  )
}

#' Use a trained RF model to find high-probability matches between two years
#' @param df_left Left dataframe
#' @param df_right Right dataframe
#' @param rf_model Trained random forest model
#' @param match_threshold Probability threshold for considering a match
#' @param blocking_var Variable to use for blocking
#' @param match_vars Vector of variables to use for matching
#' @return List with matches (clean) and ambiguous (flagged) dataframes
perform_rf_matching <- function(
    df_left,
    df_right,
    rf_model,
    match_threshold = 0.90,
    blocking_var = "water_company",
    match_vars = RF_MATCH_VARS) {
  # Identify site_id columns
  site_id_left <- grep("^site_id_", names(df_left), value = TRUE)
  site_id_right <- grep("^site_id_", names(df_right), value = TRUE)
  if (length(site_id_left) != 1 || length(site_id_right) != 1) {
    stop("df_left / df_right must carry exactly one site_id_ column each")
  }
  left_year <- sub("^site_id_", "", site_id_left)
  right_year <- sub("^site_id_", "", site_id_right)
  year_suffix <- paste0(left_year, "_", right_year)

  logger::log_info("Performing RF matching for years {left_year} and {right_year}")

  dt_left <- data.table::as.data.table(df_left)
  dt_right <- data.table::as.data.table(df_right)

  # Blocking & comparison
  logger::log_info("Creating candidate pairs using blocking on {blocking_var}")
  pairs <- reclin2::pair_blocking(dt_left, dt_right, on = blocking_var, add_xy = TRUE)
  if (nrow(pairs) == 0) {
    logger::log_info("No candidate pairs found after blocking")
    return(list(matches = tibble(), ambiguous = tibble()))
  }

  # Compare pairs
  logger::log_info("Comparing {nrow(pairs)} candidate pairs")
  compare_rf_match_vars(pairs, match_vars)

  # Predict probabilities
  logger::log_info("Predicting match probabilities")
  pred <- predict(rf_model, data = pairs)
  prob_col <- paste0("match_prob_", year_suffix)
  pairs[, (prob_col) := pred$predictions[, "match"]]

  # Select matches
  logger::log_info("Selecting matches with threshold {match_threshold}")
  sel_col <- paste0("selected_rf_", year_suffix)
  pairs <- reclin2::select_n_to_m(pairs,
    variable = sel_col, score = prob_col,
    threshold = match_threshold, n = 1, m = 1, inplace = TRUE
  )

  # Link records
  results <- reclin2::link(pairs,
    selection = sel_col, all_x = FALSE, all_y = FALSE,
    suffixes = c(paste0("_", left_year), paste0("_", right_year)),
    keep_from_pairs = prob_col
  )

  if (nrow(results) == 0) {
    logger::log_info("No matches found above threshold")
    return(list(matches = tibble(), ambiguous = tibble()))
  }

  # Format results
  results <- as_tibble(results) %>%
    rename(!!paste0("match_quality_rf_", year_suffix) := all_of(prob_col)) %>%
    mutate(
      !!paste0("match_method_", year_suffix) := "rf",
      !!paste0("match_type_", year_suffix) := "one_to_one",
      !!paste0("join_keys_", year_suffix) := "rf_probabilistic",
      !!paste0("match_level_", year_suffix) := (1 - !!sym(paste0("match_quality_rf_", year_suffix))) * 100
    ) %>%
    select(
      !!sym(site_id_left), !!sym(site_id_right),
      starts_with("match_method_"), starts_with("match_type_"),
      starts_with("join_keys_"), starts_with("match_level_"),
      starts_with("match_quality_rf_")
    )

  # Monitor-multiples guard: identifier-identical groups cannot be paired
  # 1:1 on evidence, so their proposals are flagged, not linked.
  split_results <- flag_ambiguous_rf_proposals(
    matches = results,
    df_left = df_left,
    df_right = df_right,
    match_vars = match_vars,
    site_id_left = site_id_left,
    site_id_right = site_id_right
  )

  if (nrow(split_results$ambiguous) > 0) {
    logger::log_warn(
      "{nrow(split_results$ambiguous)} RF proposals for {year_suffix} are ambiguous monitor-multiples and were not linked"
    )
  }
  logger::log_info("Found {nrow(split_results$matches)} probabilistic matches")
  split_results
}

#' Run RF matching over all year pairs
#' @param year_pairs Dataframe with left_year and right_year columns
#' @param data_list List of tibbles with one tibble per year
#' @param rf_model Trained random forest model
#' @param unmatched_ids_by_year List of unmatched IDs by year
#' @param match_threshold Probability threshold for considering a match
#' @return Named list of match dataframes, one for each year pair
run_rf_matching <- function(
    year_pairs,
    data_list,
    rf_model,
    unmatched_ids_by_year,
    match_threshold = 0.90) {
  logger::log_info("Running RF matching across all year pairs")

  out <- list()
  n_ambiguous_total <- 0L

  for (i in seq_len(nrow(year_pairs))) {
    ly <- year_pairs$left_year[i]
    ry <- year_pairs$right_year[i]
    pair_name <- paste(ly, ry, sep = "_")

    logger::log_info("Processing year pair: {pair_name}")

    df_left <- data_list[[paste0("df", ly)]]
    df_right <- data_list[[paste0("df", ry)]]

    um_left <- unmatched_ids_by_year[[as.character(ly)]]
    um_right <- unmatched_ids_by_year[[as.character(ry)]]

    # Skip if either side has no unmatched IDs
    if (length(um_left) == 0 || length(um_right) == 0) {
      logger::log_info("Skipping {pair_name}: no unmatched IDs on one or both sides")
      out[[pair_name]] <- tibble()
      next
    }

    df_left_filt <- prepare_rf_data(df_left, um_left)
    df_right_filt <- prepare_rf_data(df_right, um_right)

    # If filtering removed all rows on either side, skip
    if (nrow(df_left_filt) == 0 || nrow(df_right_filt) == 0) {
      logger::log_info("Skipping {pair_name}: filtered data is empty")
      out[[pair_name]] <- tibble()
      next
    }

    res <- perform_rf_matching(
      df_left_filt, df_right_filt, rf_model,
      match_threshold = match_threshold
    )
    out[[pair_name]] <- res$matches
    n_ambiguous_total <- n_ambiguous_total + nrow(res$ambiguous)

    logger::log_info(
      "Completed RF matching for {pair_name}: found {nrow(res$matches)} matches"
    )
  }

  total_matches <- sum(vapply(out, nrow, integer(1)))
  logger::log_info(
    "RF matching complete. Found {total_matches} matches across all year pairs ({n_ambiguous_total} ambiguous proposals withheld)"
  )
  out
}

#' Load an existing RF model or train a new one conditionally
#'
#' Only model files written by the downsampled trainer (carrying the
#' RF_TRAINER_VERSION marker) are loaded. The April 2025 model file was
#' trained with a poison-label bug and has been retired; if a markerless
#' file is found at model_file, loading stops rather than silently using it.
#' @param data_list List of tibbles with one tibble per year
#' @param model_file Path to the saved model file
#' @param train_if_missing Boolean indicating whether to train a new model if not found
#' @return RF model object or NULL if model not found and training disabled
load_or_train_rf_model <- function(
    data_list,
    model_file = CONFIG$rf_model_file,
    train_if_missing = TRUE) {
  if (file.exists(model_file)) {
    logger::log_info("Loading RF model from {model_file}")
    saved_model <- readRDS(model_file)

    if (!is.list(saved_model) ||
        !identical(saved_model$trainer, RF_TRAINER_VERSION) ||
        !inherits(saved_model$model, "ranger")) {
      stop(glue::glue(
        "RF model file {model_file} was not written by the ",
        "{RF_TRAINER_VERSION} trainer and cannot be trusted (the legacy ",
        "model was trained with a poison-label bug). Delete the file and ",
        "rerun to retrain."
      ))
    }

    return(saved_model$model)
  }

  if (!train_if_missing) {
    logger::log_warn("RF model file not found and training disabled")
    return(NULL)
  }

  logger::log_info("RF model file not found - training a new model...")

  # Train on 2023 <-> 2024 deterministic unique_id matches
  windfall_unique <- perform_windfall_matching("2023_2024",
    matching_levels = list(c("unique_id_2023")), data_list = data_list
  )$matches %>%
    filter(join_keys_2023_2024 == "water_company|unique_id_2023") %>%
    select(site_id_2024, site_id_2023)

  logger::log_info("Training RF model with {nrow(windfall_unique)} known matches")
  m <- train_rf_linkage_model(
    df_left       = data_list$df2023,
    df_right      = data_list$df2024,
    known_matches = windfall_unique,
    model_file    = model_file
  )

  logger::log_info("RF model training complete")
  m
}
