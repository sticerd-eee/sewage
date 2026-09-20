source(here::here("scripts", "R", "utils", "open_coast_contracts.R"), local = TRUE)

# ==============================================================================
# Salience Group Evidence, Sample Audits and Table Formatting
# Purpose: Classify published Site Group/property-radius evidence consistently
#          across sales and rental analyses. No spatial matches or data builds.
# ==============================================================================

#' Select the nearest Site Group and attach its published characteristics
#'
#' @param lookup Property-Site Group pairs, a data frame or Arrow dataset, with
#'   `site_id`, `distance_m`, and the property identifier.
#' @param characteristics Published Site Group characteristics (data frame).
#' @param id_col Property identifier: `house_id` or `rental_id`.
#' @return One row per property within 2 km, ordered by property identifier;
#'   ties use the lowest numeric Site Group ID. The `coverage` attribute audits
#'   every eligible pair, not just the nearest pair. Missing characteristics
#'   fail closed; a joined row with missing coast distance is retained.
nearest_salience_sites <- function(lookup, characteristics, id_col, profile = "legacy_tidal") {
  profile <- match.arg(profile, c("legacy_tidal", "open_coast"))
  if (profile == "open_coast") validate_open_coast_sites(characteristics)
  if (anyNA(characteristics$site_id) || anyDuplicated(characteristics$site_id)) {
    stop("Site Group characteristics must have unique, nonmissing site_id.", call. = FALSE)
  }
  pairs <- lookup |>
    dplyr::select(dplyr::all_of(c(id_col, "site_id", "distance_m"))) |>
    dplyr::filter(!is.na(.data$distance_m), .data$distance_m <= 2000) |>
    dplyr::collect()
  if (anyNA(pairs[[id_col]]) || anyNA(pairs$site_id) || any(pairs$distance_m < 0)) {
    stop("Eligible pairs require property/site IDs and nonnegative distances.", call. = FALSE)
  }
  if (dplyr::n_distinct(pairs[[id_col]], pairs$site_id) != nrow(pairs))
    stop("Duplicate property-Site Group pair.")
  matched <- pairs$site_id %in% characteristics$site_id
  if (!all(matched)) {
    stop(sum(!matched), " pairs within 2 km lack Site Group characteristics.", call. = FALSE)
  }
  nearest <- pairs |>
    dplyr::arrange(.data[[id_col]], .data$distance_m, .data$site_id) |>
    dplyr::distinct(.data[[id_col]], .keep_all = TRUE) |>
    dplyr::rename(min_distance = "distance_m") |>
    dplyr::left_join(
      dplyr::select(characteristics, "site_id", "distance_to_coast_m",
                    "bath_ever_2124", "bath_unknown_2124",
                    dplyr::any_of("bath_status_21"),
                    dplyr::all_of(if (profile == "open_coast") open_coast_site_columns() else character())),
      by = "site_id", relationship = "many-to-one"
    )
  attr(nearest, "coverage") <- tibble::tibble(
    n_pairs_within_2km = nrow(pairs),
    n_pairs_with_characteristics = sum(matched),
    pair_characteristic_share = if (nrow(pairs)) mean(matched) else NA_real_,
    n_properties_within_2km = nrow(nearest),
    n_properties_with_characteristics = nrow(nearest),
    property_characteristic_share = if (nrow(nearest)) 1 else NA_real_
  )
  nearest
}

# Independent group membership. Positive bathing evidence needs no coast evidence.
classify_salience_groups <- function(data, coast_rule_m = 2000,
                                     source = c("nearest", "radius"),
                                     profile = "legacy_tidal", bathing_policy = "ever_2124") {
  source <- match.arg(source)
  profile <- match.arg(profile, c("legacy_tidal", "open_coast"))
  bathing_policy <- match.arg(bathing_policy, c("ever_2124", "2021_only"))
  if (profile == "open_coast") {
    if (source == "nearest") validate_open_coast_sites(data) else validate_open_coast_radius(data)
  }
  coast_column <- if (profile == "open_coast") {
    if (source == "nearest") "distance_to_open_coast_m" else "min_open_coast_dist_m"
  } else if (source == "nearest") "distance_to_coast_m" else "min_coast_dist_m"
  bath_column <- if (bathing_policy == "2021_only") {
    if (source == "nearest") "bath_status_21" else "any_bath_2021"
  } else if (source == "nearest") "bath_ever_2124" else "any_bath_2124"
  if (!all(c(coast_column, bath_column, "region") %in% names(data))) stop("Required salience evidence is absent.")
  if (length(coast_rule_m) != 1L || !is.finite(coast_rule_m) || coast_rule_m < 0) {
    stop("coast_rule_m must be one nonnegative finite distance.", call. = FALSE)
  }
  complete <- if (profile == "open_coast" && source == "radius") {
    data$n_open_coast_missing == 0L & data$n_open_coast_known > 0L
  } else rep(TRUE, nrow(data))
  bath <- if (bathing_policy == "2021_only" && source == "nearest")
    data[[bath_column]] %in% "designated" else data[[bath_column]] %in% TRUE
  data |>
    dplyr::mutate(
      london = .data$region %in% "London",
      bathing = .env$bath,
      coastal = is.finite(.data[[coast_column]]) & .data[[coast_column]] <= .env$coast_rule_m,
      inland = .env$complete & is.finite(.data[[coast_column]]) & .data[[coast_column]] > .env$coast_rule_m
    )
}

salience_group_masks <- function(data) {
  list(bathing = data$bathing %in% TRUE,
       coastal = data$coastal %in% TRUE,
       inland = data$inland %in% TRUE)
}

# fixest retains its ordered observation selections in lean models, even though
# obs() refuses lean objects. Apply every selection to the actual fitted input
# IDs; never infer retained identities from fitted N or another data ordering.
salience_observation_ids <- function(model, sample, id_column) {
  ids <- sample[[id_column]]
  if (anyNA(ids) || anyDuplicated(ids) || length(ids) != model$nobs_origin) {
    stop("Fitted input identities must be complete, unique and match nobs_origin.")
  }
  selected <- seq_along(ids)
  for (selection in model$obs_selection) selected <- selected[selection]
  if (length(selected) != model$nobs || anyNA(selected)) stop("Estimator identity accounting failed.")
  list(selected = ids, fitted = ids[selected])
}

# Preserve the transaction grain when attaching the published 250m companion.
join_salience_group_companion <- function(data, companion, id_column, radius = 250L,
                                         coast_rule_m = 2000, profile = "legacy_tidal",
                                         expected_generation = NULL) {
  profile <- match.arg(profile, c("legacy_tidal", "open_coast"))
  evidence <- companion |>
    dplyr::filter(.data$radius == .env$radius) |>
    dplyr::select(dplyr::all_of(c(
      id_column, "min_coast_dist_m", "any_bath_2124", "bath_unknown_2124",
      if (profile == "open_coast") c("n_spill_sites", open_coast_radius_columns())
    ))) |>
    dplyr::collect()
  if (anyNA(evidence[[id_column]]) || anyDuplicated(evidence[[id_column]])) {
    stop("Radius companion must have unique, nonmissing property keys.", call. = FALSE)
  }
  if (profile == "open_coast") {
    if (is.null(expected_generation)) stop("Expected Site Group generation is required.")
    validate_open_coast_radius(evidence, expected_generation)
    if (any(!data[[id_column]] %in% evidence[[id_column]])) stop("Missing required radius companion row.")
    if ("n_spill_sites" %in% names(data)) {
      matched <- match(data[[id_column]], evidence[[id_column]])
      if (any(data$n_spill_sites != evidence$n_spill_sites[matched])) stop("Radius/exposure site counts differ.")
      evidence <- dplyr::select(evidence, -"n_spill_sites")
    }
  }
  data |>
    dplyr::left_join(evidence, by = id_column, relationship = "many-to-one") |>
    classify_salience_groups(coast_rule_m, source = "radius", profile = profile)
}

audit_salience_groups <- function(data, market, exclude_london = TRUE,
                                 groups = c("bathing", "coastal", "inland"), allow_unavailable = FALSE) {
  masks <- salience_group_masks(data)
  if (any(!groups %in% names(masks))) stop("Unknown salience group.")
  dplyr::bind_rows(lapply(groups, function(group) {
    selected <- masks[[group]]
    retained <- selected & (!exclude_london | !data$london)
    if (!any(retained) && !allow_unavailable) stop("Empty group: ", market, " / ", group, call. = FALSE)
    period_count <- function(near, post) {
      if (!all(c("near_bin", "post") %in% names(data))) return(NA_integer_)
      sum(retained & data$near_bin == near & data$post == post)
    }
    counts <- tibble::tibble(
      market = market, group = group, n_input = nrow(data),
      n_before_london_drop = sum(selected),
      n_after_london_drop = sum(selected & !data$london), n_estimation = sum(retained),
      n_outside_all_groups = sum(!Reduce(function(x, y) x | y, masks)),
      n_near = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 1L) else NA_integer_,
      n_far = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 0L) else NA_integer_,
      n_near_pre = period_count(1L, 0L), n_near_post = period_count(1L, 1L),
      n_far_pre = period_count(0L, 0L), n_far_post = period_count(0L, 1L)
    )
    support <- if ("post" %in% names(data)) {
      unlist(counts[c("n_near_pre", "n_near_post", "n_far_pre", "n_far_post")])
    } else unlist(counts[c("n_near", "n_far")])
    if (any(support == 0L, na.rm = TRUE) && !allow_unavailable) {
      stop("Insufficient near/far support: ", market, " / ", group, call. = FALSE)
    }
    counts
  }))
}

# Read the covariance already stored by fixest; never change inference on export.
salience_group_results <- function(model) {
  if (inherits(model, "salience_unavailable")) return(tibble::tibble(
    term = model$terms, estimate = NA_real_, std_error = NA_real_, conf_low = NA_real_,
    conf_high = NA_real_, p_value = NA_real_, nobs = NA_real_))
  table <- model$coeftable
  df <- attr(model$cov.scaled, "df.t")
  if (is.null(df) || length(df) != 1L || !is.finite(df) || df <= 0) {
    stop("Saved inference must include positive t degrees of freedom.", call. = FALSE)
  }
  critical_value <- stats::qt(0.975, df)
  tibble::tibble(
    term = rownames(table), estimate = table[, 1L], std_error = table[, 2L],
    conf_low = table[, 1L] - critical_value * table[, 2L],
    conf_high = table[, 1L] + critical_value * table[, 2L],
    p_value = table[, 4L], nobs = model$nobs
  )
}

# Only estimation failures become unavailable cells; preparation and join
# failures occur before this boundary and must still stop the run.
fit_salience_group <- function(sample, fit, terms, id) {
  model <- tryCatch({
    if (!nrow(sample)) stop("Empty eligible group")
    value <- fit(sample)
    estimates <- salience_group_results(value)
    focal <- estimates[match(terms, estimates$term), ]
    if (anyNA(focal$term) || any(!is.finite(focal$estimate) | !is.finite(focal$std_error)))
      stop("Focal coefficients are not identified")
    value
  }, error = identity)
  if (inherits(model, "error")) return(structure(list(reason = conditionMessage(model),
    terms = terms, nobs = NA_real_, salience_observation_ids = list(selected = sample[[id]], fitted = character())),
    class = "salience_unavailable"))
  model$salience_observation_ids <- salience_observation_ids(model, sample, id)
  model
}

salience_fit_counts <- function(counts, models) {
  unavailable <- unname(vapply(models, inherits, logical(1), "salience_unavailable"))
  counts$fit_status <- ifelse(unavailable, "unavailable", "available")
  counts$reason <- unname(vapply(models, function(model) if (inherits(model, "salience_unavailable")) model$reason else NA_character_, ""))
  counts$nobs <- unname(vapply(models, function(model) model$nobs, numeric(1)))
  counts$n_removed_by_estimator <- counts$n_estimation - counts$nobs
  counts
}

#' Paper-style sentences defining the Salience Groups for table notes
#'
#' @param source "nearest" when groups follow the property's nearest Site
#'   Group; "radius" when they follow every Site Group within 250 m.
#' @return One character string, written in the same register as the parent
#'   table notes, to be spliced into a script's note text.
salience_group_notes <- function(source = c("nearest", "radius"), coast_rule_m = 2000, profile = "legacy_tidal") {
  source <- match.arg(source)
  coast <- paste0(format(coast_rule_m, scientific = FALSE), "m")
  if (identical(profile, "open_coast")) return(paste0(
    if (source == "nearest") "Groups follow the property's nearest overflow. " else
      "Groups follow any qualifying overflow within 250m of the property. ",
    "Coastal membership requires a validated overflow distance of at most ", coast,
    " to physical high-water shoreline adjoining officially coastal waters, including coastal bays. ",
    "Estuarine and tidal-river banks are excluded. Inland membership requires complete coast evidence ",
    "with every relevant overflow beyond ", coast, ". Bathing membership requires positive reported ",
    "bathing-water association in any of 2021--2024, independently of coast evidence. ",
    "This fixed association does not imply direct discharge or designation at the transaction date. ",
    "The groups overlap; observations must not be summed across columns. "))
  if (source == "nearest") {
    paste0(
      "Salience groups are defined by the property's nearest overflow. ",
      "Coastal overflows lie within ", coast, " of the coastline (tidal Mean High Water line) ",
      "and Inland overflows lie beyond ", coast, "; Bathing water overflows discharge to a ",
      "designated bathing water in at least one year over 2021--2024. ",
      "Bathing water overflows with observed coast distance also enter Coastal or Inland, so the groups overlap ",
      "and observations should not be summed across columns. ",
      "Overflows with missing coast distance enter neither the Coastal nor the Inland group, ",
      "and overflows whose designation evidence is unresolved are not classified as Bathing water. "
    )
  } else {
    paste0(
      "Salience groups are defined by the overflows within 250m of the property. ",
      "Properties are Coastal if the nearest coastline distance among these overflows is at most ",
      coast, " (tidal Mean High Water line) and Inland if it exceeds ", coast, "; ",
      "properties are Bathing water if any overflow within 250m discharges to a designated ",
      "bathing water in at least one year over 2021--2024. ",
      "Bathing water properties with observed coast distance also enter Coastal or Inland, so the groups overlap ",
      "and observations should not be summed across columns. ",
      "Properties with missing coast distance enter neither the Coastal nor the Inland group, ",
      "and unresolved designation evidence does not establish Bathing water membership. "
    )
  }
}

# Formatting only: models, displayed coefficients and specification notes are
# supplied by each paper script.
export_salience_group_table <- function(models, coefficient_map, title, notes, path) {
  labels <- c(inland = "Inland", coastal = "Coastal", bathing = "Bathing water")
  ordered_models <- lapply(models[c("sales", "rentals")], function(market_models) {
    market_models[names(labels)]
  })
  panels <- lapply(ordered_models, function(market_models) {
    fitted <- lapply(market_models, function(model) {
      result <- salience_group_results(model)
      structure(list(
        tidy = data.frame(
          term = result$term, estimate = result$estimate, std.error = result$std_error,
          conf.low = result$conf_low, conf.high = result$conf_high,
          p.value = result$p_value
        ),
        glance = data.frame(nobs = model$nobs,
                            adj.r.squared = if (inherits(model, "salience_unavailable")) NA_real_ else fixest::fitstat(model, "ar2")$ar2)
      ), class = "modelsummary_list")
    })
    stats::setNames(fitted, unname(labels[names(market_models)]))
  })
  names(panels) <- c("House Sales", "House Rentals")
  add_rows <- tibble::tibble(term = c("Property controls", "Location FE", "Time FE"))
  for (market in names(ordered_models)) {
    controls <- if (market == "sales") c("property_type", "old_new", "duration") else
      c("property_type", "bedrooms", "bathrooms")
    for (group in names(labels)) {
      model <- ordered_models[[market]][[group]]
      if (inherits(model, "salience_unavailable")) {
        add_rows[[paste(market, group, sep = "_")]] <- rep("Unavailable", 3)
        notes <- paste0(notes, " The ", market, " / ", group,
          " fit is unavailable; its reason is recorded in the matching cell-count CSV.")
        next
      }
      add_rows[[paste(market, group, sep = "_")]] <- c(
        if (all(controls %in% all.vars(model$fml))) "Yes" else "No",
        if ("lsoa" %in% model$fixef_vars) "LSOA" else "No",
        if ("month_id" %in% model$fixef_vars) "Month" else "No"
      )
    }
  }
  attr(add_rows, "position") <- "coef_end"
  has_unavailable <- any(vapply(unlist(ordered_models, recursive = FALSE), inherits,
    logical(1), "salience_unavailable"))
  if (has_unavailable) {
    # modelsummary drops all-NA models. Build explicit unavailable columns
    # instead of substituting a different group's fitted model.
    display <- data.frame(Term = c(unname(coefficient_map), "Observations", "Adj. R-squared",
      "Property controls", "Location FE", "Time FE"))
    for (market in names(ordered_models)) for (group in names(labels)) {
      model <- ordered_models[[market]][[group]]
      column <- paste(market, labels[[group]])
      if (inherits(model, "salience_unavailable")) {
        display[[column]] <- rep("Unavailable", nrow(display))
      } else {
        focal <- salience_group_results(model)
        focal <- focal[match(names(coefficient_map), focal$term), ]
        stars <- ifelse(focal$p_value < .01, "***", ifelse(focal$p_value < .05, "**", ifelse(focal$p_value < .1, "*", "")))
        display[[column]] <- c(sprintf("%.3f%s (%.3f)", focal$estimate, stars, focal$std_error),
          format(model$nobs, big.mark = ","), sprintf("%.3f", fixest::fitstat(model, "ar2")$ar2),
          add_rows[[paste(market, group, sep = "_")]])
      }
    }
    latex <- tinytable::save_tt(tinytable::tt(display, caption = title, notes = " "), output = "latex")
  } else latex <- modelsummary::modelsummary(
    panels, shape = "cbind", output = "latex", escape = FALSE,
    estimate = "{estimate}{stars}", statistic = "({std.error})",
    stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
    fmt = fmt_table, coef_map = coefficient_map,
    gof_map = tibble::tribble(
      ~raw, ~clean, ~fmt,
      "nobs", "Observations", 0,
      "adj.r.squared", "Adj. R-squared", 3
    ),
    add_rows = add_rows, notes = " ", title = title
  )
  latex <- fit_tblr_latex(
    latex, label = paste0("tbl:", gsub("_", "-", tools::file_path_sans_ext(basename(path)))),
    colsep = "2pt", cell_font = "\\fontsize{8pt}{9pt}\\selectfont",
    notes = paste0("note{}={\\\\footnotesize{\\\\textbf{Notes:} ", notes, "}},")
  )
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(latex, path)
  invisible(path)
}
