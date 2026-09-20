source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))
source(here::here("scripts", "R", "utils", "salience_group_utils.R"))
source(here::here("scripts", "R", "utils", "salience_publication_utils.R"))

refinement_variants <- function() c("headline", "london_excluded", "bathing_2021")

refinement_expected_cells <- function() {
  dplyr::bind_rows(lapply(refinement_variants(), function(variant) {
    tidyr::expand_grid(variant = variant, specification = salience_paper_prefixes(),
      market = c("sales", "rentals"), group = if (variant == "bathing_2021") "bathing" else
        c("bathing", "coastal", "inland"))
  }))
}

report_bathing_2021 <- function(data, sites, market, assignment, lookup_path) {
  if (assignment == "nearest") {
    if (!all(data$site_id %in% sites$site_id)) stop("Missing 2021 nearest-site evidence.")
    status <- sites$bath_status_21[match(data$site_id, sites$site_id)]
    positive <- status == "designated"
    unknown <- status == "unknown"
  } else {
    id <- if (market == "sales") "house_id" else "rental_id"
    pairs <- arrow::open_dataset(lookup_path) |>
      dplyr::filter(.data$distance_m <= 250) |>
      dplyr::select(dplyr::all_of(c(id, "site_id"))) |> dplyr::collect()
    if (dplyr::n_distinct(pairs[[id]], pairs$site_id) != nrow(pairs) || any(!pairs$site_id %in% sites$site_id))
      stop("Invalid 2021 radius evidence join.")
    pairs$status <- sites$bath_status_21[match(pairs$site_id, sites$site_id)]
    evidence <- pairs |> dplyr::summarise(positive = any(.data$status == "designated"),
      unknown = any(.data$status == "unknown"), .by = dplyr::all_of(id))
    if (any(!data[[id]] %in% evidence[[id]])) stop("Missing required 2021 radius evidence.")
    positive <- evidence$positive[match(data[[id]], evidence[[id]])]
    unknown <- evidence$unknown[match(data[[id]], evidence[[id]])]
  }
  if (any(positive & !data$bathing)) stop("2021-positive evidence is not a subset of ever-positive evidence.")
  dplyr::mutate(data, bathing = .env$positive, bathing_2021_unknown = .env$unknown)
}

estimate_report_variant <- function(env, data, market, variant) {
  groups <- if (variant == "bathing_2021") "bathing" else c("bathing", "coastal", "inland")
  exclude <- variant %in% c("london_excluded", "legacy_london_excluded")
  counts <- env$audit_salience_groups(data, market, exclude_london = exclude,
                                    groups = groups, allow_unavailable = TRUE)
  counts <- dplyr::mutate(counts, fit_status = NA_character_, reason = NA_character_,
                         nobs = NA_real_, n_removed_by_estimator = NA_real_)
  id <- env$MARKETS[[market]]$id
  extensive <- exists("fit_salience_extensive", env, inherits = FALSE)
  hedonic <- exists("fit_salience_hedonic", env, inherits = FALSE)
  exposure <- if (extensive) "near_bin" else "spill_count_weekly_avg"
  terms <- if (hedonic) exposure else c(exposure, paste(exposure, env$CONFIG$attention, sep = ":"))
  models <- observations <- list()
  results <- list()
  for (group in groups) {
    sample <- data[data[[group]] & (!exclude | !data$london), ]
    fitted <- tryCatch({
      if (!nrow(sample)) stop("Empty eligible group")
      model <- if (extensive) env$fit_salience_extensive(sample, market, env$CONFIG$attention) else if (hedonic)
        env$fit_salience_hedonic(sample, market) else env$fit_salience_intensive(sample, market, env$CONFIG$attention)
      focal <- env$salience_group_results(model)
      focal <- focal[match(terms, focal$term), ]
      if (anyNA(focal$term) || any(!is.finite(focal$estimate) | !is.finite(focal$std_error)))
        stop("Focal coefficients are not identified")
      model
    }, error = identity)
    row <- which(counts$group == group)
    if (inherits(fitted, "error")) {
      counts$fit_status[row] <- "unavailable"
      counts$reason[row] <- conditionMessage(fitted)
      counts$nobs[row] <- NA_real_
      counts$n_removed_by_estimator[row] <- NA_real_
      results[[group]] <- tibble::tibble(term = terms, estimate = NA_real_, std_error = NA_real_,
        conf_low = NA_real_, conf_high = NA_real_, p_value = NA_real_, nobs = NA_real_,
        market = market, group = group, fit_status = "unavailable", reason = conditionMessage(fitted))
      observations[[group]] <- list(selected = sample[[id]], fitted = character())
    } else {
      counts$fit_status[row] <- "available"
      counts$reason[row] <- NA_character_
      counts$nobs[row] <- fitted$nobs
      counts$n_removed_by_estimator[row] <- nrow(sample) - fitted$nobs
      models[[group]] <- fitted
      observations[[group]] <- salience_observation_ids(fitted, sample, id)
      results[[group]] <- env$salience_group_results(fitted) |>
        dplyr::filter(.data$term %in% terms) |>
        dplyr::mutate(market = market, group = group, fit_status = "available", reason = NA_character_)
    }
  }
  list(models = models, counts = counts, results = dplyr::bind_rows(results), observations = observations,
    settings = list(variant = variant, specification = env$CONFIG$output_prefix, market = market,
      profile = if (variant == "legacy_london_excluded") "legacy_tidal" else "open_coast",
      coast_rule_m = 2000, london = if (exclude) "excluded" else "included",
      bathing_policy = if (variant == "bathing_2021") "2021_only" else "ever_2124",
      assignment = if (hedonic || extensive) "nearest" else "radius", config = env$CONFIG))
}

compare_coast_models <- function(legacy, refined, specification, market) {
  # Bathing definitions and every input row are held fixed. This is an exact
  # internal invariance check, not a claim about historical upstream inputs.
  if (!identical(legacy$observations$bathing, refined$observations$bathing) ||
      !isTRUE(all.equal(legacy$results[legacy$results$group == "bathing", ],
                       refined$results[refined$results$group == "bathing", ])))
    stop("Coast-only change altered the overlapping Bathing analysis.")
  old <- legacy$results |> dplyr::rename_with(~paste0("legacy_", .x),
    -dplyr::all_of(c("market", "group", "term")))
  new <- refined$results |> dplyr::rename_with(~paste0("refined_", .x),
    -dplyr::all_of(c("market", "group", "term")))
  dplyr::full_join(old, new, by = c("market", "group", "term"), relationship = "one-to-one") |>
    dplyr::mutate(specification = specification, .before = 1L)
}

validate_refinement_cell <- function(cell, row, results) {
  settings <- cell$settings
  legacy <- row$variant == "legacy_london_excluded"
  if (!identical(settings$variant, row$variant) || !identical(settings$specification, row$specification) ||
      !identical(settings$config$output_prefix, row$specification) ||
      !identical(settings$market, row$market) ||
      !identical(settings$profile, if (legacy) "legacy_tidal" else "open_coast") ||
      !identical(settings$london, if (row$variant %in% c("london_excluded", "legacy_london_excluded")) "excluded" else "included") ||
      !identical(settings$bathing_policy, if (row$variant == "bathing_2021") "2021_only" else "ever_2124"))
    stop("Refinement variant definition mismatch.")
  exposure <- if (grepl("extensive", row$specification)) "near_bin" else "spill_count_weekly_avg"
  terms <- if (grepl("hedonic", row$specification)) exposure else
    c(exposure, paste(exposure, settings$config$attention, sep = ":"))
  if (anyDuplicated(results$term) || !setequal(results$term, terms) ||
      any(results$fit_status != row$fit_status)) stop("Incomplete or inconsistent refinement coefficients.")
  saved_counts <- cell$counts[cell$counts$group == row$group, ]
  if (!isTRUE(all.equal(as.data.frame(row[names(saved_counts)]), as.data.frame(saved_counts), check.attributes = FALSE)))
    stop("Refinement counts differ from saved cell.")
  ids <- cell$observations[[row$group]]
  if (length(ids$selected) != row$n_estimation || anyDuplicated(ids$selected) ||
      anyDuplicated(ids$fitted) || any(!ids$fitted %in% ids$selected))
    stop("Refinement observation identities do not reconcile.")
  model <- cell$models[[row$group]]
  if (row$fit_status == "unavailable") {
    if (!is.null(model) || is.na(row$reason) || !nzchar(row$reason) ||
        any(is.na(results$reason) | results$reason != row$reason) ||
        !is.na(row$nobs) || !is.na(row$n_removed_by_estimator) || length(ids$fitted) ||
        any(!is.na(as.matrix(results[c("estimate", "std_error", "conf_low", "conf_high", "p_value", "nobs")]))))
      stop("Unavailable fit has stale or unexplained values.")
  } else {
    if (!identical(row$fit_status, "available") || is.null(model) || model$nobs != row$nobs ||
        length(ids$fitted) != row$nobs || row$n_estimation != row$nobs + row$n_removed_by_estimator)
      stop("Refinement fit accounting mismatch.")
    actual <- salience_group_results(model)
    actual <- actual[match(results$term, actual$term), ]
    if (!isTRUE(all.equal(as.data.frame(results[names(actual)]), as.data.frame(actual), check.attributes = FALSE)))
      stop("Refinement coefficients differ from saved model.")
  }
  invisible(cell)
}

validate_refinement_collection <- function(bundle) {
  expected <- refinement_expected_cells()
  key <- function(x) do.call(paste, c(x[c("variant", "specification", "market", "group")], sep = ":"))
  counts <- bundle$counts
  if (!identical(bundle$profile, "open_coast") || !identical(bundle$family, "overlapping") ||
      anyDuplicated(key(counts)) || !setequal(key(counts), key(expected))) stop("Incomplete refinement model grid.")
  if (any(!counts$fit_status %in% c("available", "unavailable")) ||
      any(counts$fit_status == "unavailable" & (is.na(counts$reason) | !nzchar(counts$reason))))
    stop("Unexplained unavailable fit.")
  if (anyDuplicated(bundle$results[c("variant", "specification", "market", "group", "term")]))
    stop("Duplicate refinement coefficient key.")
  if (!setequal(key(bundle$results), key(expected))) stop("Missing refinement coefficient cells.")
  provenance <- bundle$provenance
  if (!length(provenance$inputs) || !length(provenance$code) ||
      !identical(provenance$generation,
        digest::digest(provenance[setdiff(names(provenance), "generation")], algo = "sha256")))
    stop("Refinement provenance identity mismatch.")
  for (i in seq_len(nrow(counts))) {
    row <- counts[i, ]
    cell <- bundle$cells[[row$variant]][[row$specification]][[row$market]]
    validate_refinement_cell(cell, row, bundle$results[key(bundle$results) == key(row), ])
  }
  comparisons <- list()
  for (specification in salience_paper_prefixes()) for (market in c("sales", "rentals")) {
    legacy <- bundle$legacy_cells[[specification]][[market]]
    if (is.null(legacy) || anyDuplicated(legacy$counts$group) ||
        !setequal(legacy$counts$group, c("bathing", "coastal", "inland")))
      stop("Missing compatible legacy comparison cells.")
    for (group in c("bathing", "coastal", "inland")) {
      row <- legacy$counts[legacy$counts$group == group, ]
      row$variant <- "legacy_london_excluded"
      row$specification <- specification
      validate_refinement_cell(legacy, row, legacy$results[legacy$results$group == group, ])
    }
    comparisons[[paste(specification, market)]] <- compare_coast_models(legacy,
      bundle$cells$london_excluded[[specification]][[market]], specification, market)
  }
  if (!isTRUE(all.equal(bundle$coast_comparison, dplyr::bind_rows(comparisons))))
    stop("Coast comparison differs from saved models.")
  invisible(bundle)
}

read_refinement_collection <- function(root = file.path(salience_report_root(), "refinement")) {
  path <- file.path(root, "manifest.json")
  if (!file.exists(path)) return(NULL)
  manifest <- jsonlite::read_json(path)
  if (!identical(manifest$status, "complete") || !identical(manifest$profile, "open_coast") ||
      !identical(unlist(manifest$variants), refinement_variants())) stop("Incompatible refinement manifest.")
  artifact_paths <- vapply(manifest$artifacts, `[[`, "", "path")
  if (anyDuplicated(artifact_paths) || !manifest$bundle_path %in% artifact_paths) stop("Unmanifested refinement bundle.")
  for (entry in manifest$artifacts) {
    if (grepl("(^/|(^|/)\\.\\.(/|$))", entry$path)) stop("Unsafe refinement artifact path.")
    target <- file.path(root, entry$path)
    if (!file.exists(target) || !startsWith(normalizePath(target), paste0(normalizePath(root), "/")) ||
        !identical(salience_file_hash(target), entry$sha256)) stop("Refinement artifact hash mismatch.")
  }
  bundle <- readRDS(file.path(root, manifest$bundle_path))
  if (!identical(bundle$provenance$generation, manifest$generation)) stop("Refinement generation mismatch.")
  validate_refinement_collection(bundle)
  bundle
}
