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
nearest_salience_sites <- function(lookup, characteristics, id_col) {
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
                    "bath_ever_2124", "bath_unknown_2124"),
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
                                     source = c("nearest", "radius")) {
  source <- match.arg(source)
  coast_column <- if (source == "nearest") "distance_to_coast_m" else "min_coast_dist_m"
  bath_column <- if (source == "nearest") "bath_ever_2124" else "any_bath_2124"
  if (length(coast_rule_m) != 1L || !is.finite(coast_rule_m) || coast_rule_m < 0) {
    stop("coast_rule_m must be one nonnegative finite distance.", call. = FALSE)
  }
  data |>
    dplyr::mutate(
      london = .data$region %in% "London",
      bathing = .data[[bath_column]] %in% TRUE,
      coastal = is.finite(.data[[coast_column]]) &
        .data[[coast_column]] <= .env$coast_rule_m,
      inland = is.finite(.data[[coast_column]]) &
        .data[[coast_column]] > .env$coast_rule_m
    )
}

salience_group_masks <- function(data) {
  list(bathing = data$bathing %in% TRUE,
       coastal = data$coastal %in% TRUE,
       inland = data$inland %in% TRUE)
}

# Preserve the transaction grain when attaching the published 250m companion.
join_salience_group_companion <- function(data, companion, id_column, radius = 250L,
                                         coast_rule_m = 2000) {
  evidence <- companion |>
    dplyr::filter(.data$radius == .env$radius) |>
    dplyr::select(dplyr::all_of(c(
      id_column, "min_coast_dist_m", "any_bath_2124", "bath_unknown_2124"
    ))) |>
    dplyr::collect()
  if (anyNA(evidence[[id_column]]) || anyDuplicated(evidence[[id_column]])) {
    stop("Radius companion must have unique, nonmissing property keys.", call. = FALSE)
  }
  data |>
    dplyr::left_join(evidence, by = id_column, relationship = "many-to-one") |>
    classify_salience_groups(coast_rule_m, source = "radius")
}

audit_salience_groups <- function(data, market) {
  masks <- salience_group_masks(data)
  dplyr::bind_rows(lapply(names(masks), function(group) {
    selected <- masks[[group]]
    retained <- selected & !data$london
    if (!any(retained)) stop("Empty group: ", market, " / ", group, call. = FALSE)
    period_count <- function(near, post) {
      if (!all(c("near_bin", "post") %in% names(data))) return(NA_integer_)
      sum(retained & data$near_bin == near & data$post == post)
    }
    counts <- tibble::tibble(
      market = market, group = group, n_input = nrow(data),
      n_before_london_drop = sum(selected),
      n_after_london_drop = sum(retained), n_estimation = sum(retained),
      n_outside_all_groups = sum(!Reduce(function(x, y) x | y, masks)),
      n_near = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 1L) else NA_integer_,
      n_far = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 0L) else NA_integer_,
      n_near_pre = period_count(1L, 0L), n_near_post = period_count(1L, 1L),
      n_far_pre = period_count(0L, 0L), n_far_post = period_count(0L, 1L)
    )
    support <- if ("post" %in% names(data)) {
      unlist(counts[c("n_near_pre", "n_near_post", "n_far_pre", "n_far_post")])
    } else unlist(counts[c("n_near", "n_far")])
    if (any(support == 0L, na.rm = TRUE)) {
      stop("Insufficient near/far support: ", market, " / ", group, call. = FALSE)
    }
    counts
  }))
}

# Read the covariance already stored by fixest; never change inference on export.
salience_group_results <- function(model) {
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

#' Paper-style sentences defining the Salience Groups for table notes
#'
#' @param source "nearest" when groups follow the property's nearest Site
#'   Group; "radius" when they follow every Site Group within 250 m.
#' @return One character string, written in the same register as the parent
#'   table notes, to be spliced into a script's note text.
salience_group_notes <- function(source = c("nearest", "radius"), coast_rule_m = 2000) {
  source <- match.arg(source)
  coast <- paste0(format(coast_rule_m, scientific = FALSE), "m")
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
                            adj.r.squared = fixest::fitstat(model, "ar2")$ar2)
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
      add_rows[[paste(market, group, sep = "_")]] <- c(
        if (all(controls %in% all.vars(model$fml))) "Yes" else "No",
        if ("lsoa" %in% model$fixef_vars) "LSOA" else "No",
        if ("month_id" %in% model$fixef_vars) "Month" else "No"
      )
    }
  }
  attr(add_rows, "position") <- "coef_end"
  latex <- modelsummary::modelsummary(
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
