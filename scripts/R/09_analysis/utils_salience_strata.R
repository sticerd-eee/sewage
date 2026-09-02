# ==============================================================================
# Shared Salience Strata
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

#' Classify coast/bathing evidence without dropping transaction rows
#'
#' @param data Transactions with nearest-site or radius-companion evidence.
#' @param coast_rule_m Coast threshold in metres: 2000 (headline) or 10000.
#' @param unknown_policy `not_designated` (headline) retains uncertain evidence;
#'   `exclude` removes only unresolved designation: unknown evidence with no
#'   observed positive. Ever-designated sites and radius companions with any
#'   positively designated site remain eligible despite other unknown evidence.
#' @param source `nearest` uses `distance_to_coast_m` and `bath_ever_2124`;
#'   `radius` uses `min_coast_dist_m` and `any_bath_2124`.
#' @return Input rows with independent `coastal` and `bathing` flags, the four
#'   coast x bathing `salience_class` values, `coast_rule_m`, `bath_unknown`,
#'   `bath_unresolved` and `unknown_policy`. Source evidence is preserved;
#'   `bath_unknown` also flags missing evidence, while `bath_unresolved` requires
#'   no observed positive. Coastal depends only on distance; designation beyond
#'   the threshold is inland bathing. Missing coast distance gives an NA class.
classify_salience_coast <- function(
  data, coast_rule_m = 2000, unknown_policy = c("not_designated", "exclude"),
  source = c("nearest", "radius")
) {
  unknown_policy <- match.arg(unknown_policy)
  source <- match.arg(source)
  if (length(coast_rule_m) != 1L || !coast_rule_m %in% c(2000, 10000)) {
    stop("coast_rule_m must be 2000 or 10000.", call. = FALSE)
  }
  coast_col <- if (source == "nearest") "distance_to_coast_m" else "min_coast_dist_m"
  bath_col <- if (source == "nearest") "bath_ever_2124" else "any_bath_2124"
  data |>
    dplyr::mutate(
      coast_rule_m = .env$coast_rule_m,
      unknown_policy = .env$unknown_policy,
      coastal = .data[[coast_col]] <= .env$coast_rule_m,
      bathing = .data[[bath_col]] %in% TRUE,
      bath_unknown = dplyr::coalesce(.data$bath_unknown_2124, TRUE) | is.na(.data[[bath_col]]),
      bath_unresolved = .data$bath_unknown & !.data$bathing,
      salience_class = dplyr::case_when(
        is.na(.data[[coast_col]]) ~ NA_character_,
        .env$unknown_policy == "exclude" & .data$bath_unresolved ~ NA_character_,
        .data$coastal & .data$bathing ~ "coastal_bathing",
        .data$coastal ~ "coastal_not_bathing",
        .data$bathing ~ "inland_bathing",
        TRUE ~ "inland_not_bathing"
      )
    )
}

#' Flag Greater London in either market using the transaction region
#' @param transactions Transaction data with `region`; an absent region value
#'   is not evidence of London and is retained.
#' @return Input rows with a nonmissing logical `london` column.
flag_salience_london <- function(transactions) {
  transactions |>
    dplyr::mutate(london = .data$region %in% "London")
}

#' Join nearest-site evidence and classify transactions
#' @param transactions Transactions with property identifier and `region`.
#' @param nearest Output of `nearest_salience_sites()`.
#' @param id_col Property identifier column.
#' @param coast_rule_m Coast threshold in metres.
#' @param unknown_policy Unknown-evidence policy passed to the classifier.
#' @return All input transactions, in order, with London and coast/bathing flags.
#'   Unmatched properties have an NA class and are excluded by the enumerator.
join_nearest_salience <- function(
  transactions, nearest, id_col, coast_rule_m = 2000,
  unknown_policy = "not_designated"
) {
  transactions |>
    dplyr::left_join(dplyr::select(nearest, -"min_distance"), by = id_col,
                     relationship = "many-to-one") |>
    flag_salience_london() |>
    classify_salience_coast(coast_rule_m, unknown_policy)
}

#' Join a published property-radius companion and optionally classify its coast
#' @param transactions Transactions with property identifier and `region`.
#' @param companion Published property-radius data frame or Arrow dataset.
#' @param id_col Property identifier column.
#' @param radius Radius in metres (250 or 500); cutoffs are never recalculated.
#' @param coast_rule_m Coast threshold in metres.
#' @param unknown_policy Unknown-evidence policy passed to the classifier.
#' @param classify_coast FALSE attaches only the radius and spill-count band,
#'   preserving nearest-site classification for extensive-margin intensity.
#' @return Input transactions with radius-specific evidence. Missing companion
#'   rows retain an NA band; they are never silently labelled as `no_site`.
join_radius_salience <- function(
  transactions, companion, id_col, radius, coast_rule_m = 2000,
  unknown_policy = "not_designated", classify_coast = TRUE
) {
  fields <- c(id_col, "radius", "spill_count_band")
  if (classify_coast) {
    fields <- c(fields, "min_coast_dist_m", "any_bath_2124", "bath_unknown_2124")
  }
  evidence <- companion |>
    dplyr::filter(.data$radius == .env$radius) |>
    dplyr::select(dplyr::all_of(fields)) |>
    dplyr::collect()
  if (anyNA(evidence[[id_col]]) || anyDuplicated(evidence[[id_col]])) {
    stop("Radius companion must have unique, nonmissing property keys.", call. = FALSE)
  }
  out <- transactions |>
    dplyr::left_join(evidence, by = id_col, relationship = "many-to-one") |>
    flag_salience_london()
  if (classify_coast) {
    out <- classify_salience_coast(out, coast_rule_m, unknown_policy, "radius")
  }
  out
}

#' Enumerate ordered named Salience Stratum filters
#' @param family `coast_bathing` or `intensity`.
#' @param include_far For extensive-margin intensity only, include the entire
#'   far group in each comparison. A nonmissing far band other than `no_site`
#'   is an error. Unknown and zero near bands enter neither intensity stratum.
#' @return Named list of functions taking a classified data frame and returning
#'   logical row masks without NA. The four coast x bathing classes are disjoint
#'   and exhaustive among eligible rows. Intensity masks are disjoint
#'   among near rows, with shared far controls when requested.
salience_strata <- function(family = c("coast_bathing", "intensity"), include_far = FALSE) {
  family <- match.arg(family)
  if (family == "coast_bathing") {
    return(list(
      coastal_bathing = function(data) data$salience_class %in% "coastal_bathing",
      coastal_not_bathing = function(data) data$salience_class %in% "coastal_not_bathing",
      inland_bathing = function(data) data$salience_class %in% "inland_bathing",
      inland_not_bathing = function(data) data$salience_class %in% "inland_not_bathing"
    ))
  }
  levels <- c("spill_le_p50", "spill_gt_p50")
  stats::setNames(lapply(levels, function(level) {
    force(level)
    function(data) {
      band <- data$spill_count_band
      if (any(!is.na(band) & !band %in% c("no_site", "unknown", "zero", levels))) {
        stop("Unrecognised published spill_count_band.", call. = FALSE)
      }
      selected <- band %in% level
      if (include_far) {
        if (!"near_bin" %in% names(data) || anyNA(data$near_bin) ||
            any(!data$near_bin %in% c(0L, 1L))) {
          stop("Extensive intensity requires a complete binary near_bin.", call. = FALSE)
        }
        far <- data$near_bin == 0L
        if (any(far & !is.na(band) & band != "no_site")) {
          stop("Far-group properties must have no_site or an absent band.", call. = FALSE)
        }
        selected <- selected | far
      }
      selected
    }
  }), levels)
}

#' Log transaction counts and missing nearest-site coast evidence
#' @param data Classified transaction sample before the London drop, including
#'   `site_id`, `london`, `coast_rule_m`, and `unknown_policy`.
#' @param nearest Nearest-site lookup returned by `nearest_salience_sites()`.
#' @param market Market name for the audit and any empty-cell error.
#' @param family Family passed to `salience_strata()`.
#' @param include_far Include shared far controls for extensive intensity.
#' @param drop_london FALSE validates the London-retained robustness sample.
#' @return One row per stratum with transaction counts before/after the London
#'   drop, input/exclusion counts, and the missing-coast share over distinct
#'   nearest Site Groups represented in this sample (before the London drop).
#'   Prints cell counts; callers can bind and write the returned audit as CSV.
#'   Empty cells fail by market/stratum, including no treated rows for intensity.
#'   Both distance groups (and all four near/far x pre/post cells for Post)
#'   must survive the selected London policy. Estimation counts and counts after
#'   the London drop are reported separately, even when London is retained.
log_salience_cells <- function(
  data, nearest, market, family, include_far = FALSE, drop_london = TRUE
) {
  sites <- nearest |>
    dplyr::filter(.data$site_id %in% data$site_id) |>
    dplyr::distinct(.data$site_id, .data$distance_to_coast_m)
  settings <- dplyr::distinct(data, .data$coast_rule_m, .data$unknown_policy)
  if (nrow(settings) != 1L) {
    stop("Cell logging requires one coast rule and unknown policy.", call. = FALSE)
  }
  filters <- salience_strata(family, include_far)
  masks <- lapply(filters, function(select_stratum) select_stratum(data))
  n_unclassified <- sum(!Reduce(`|`, masks))
  counts <- dplyr::bind_rows(lapply(names(filters), function(stratum) {
    selected <- masks[[stratum]]
    retained <- selected & !data$london
    estimation <- if (drop_london) retained else selected
    london_policy <- if (drop_london) "after London drop" else "London retained"
    n_before <- sum(selected)
    n_after <- sum(retained)
    if (!any(estimation) ||
        (include_far && !any(estimation & data$near_bin == 1L))) {
      stop("Empty salience stratum: ", market, " / ", stratum,
           " (", london_policy, ").", call. = FALSE)
    }
    period_counts <- function(mask) {
      if (!all(c("near_bin", "post") %in% names(data))) {
        return(c(near_pre = NA_integer_, near_post = NA_integer_,
                 far_pre = NA_integer_, far_post = NA_integer_))
      }
      c(near_pre = sum(mask & data$near_bin == 1L & data$post == 0L),
        near_post = sum(mask & data$near_bin == 1L & data$post == 1L),
        far_pre = sum(mask & data$near_bin == 0L & data$post == 0L),
        far_post = sum(mask & data$near_bin == 0L & data$post == 1L))
    }
    after_periods <- period_counts(retained)
    estimation_periods <- period_counts(estimation)
    if ("near_bin" %in% names(data)) {
      support <- c(near = sum(estimation & data$near_bin == 1L),
                   far = sum(estimation & data$near_bin == 0L))
      if ("post" %in% names(data)) {
        support <- estimation_periods
      }
      if (any(support == 0L)) {
        stop("Insufficient near/far support: ", market, " / ", stratum,
             " (", paste(names(support)[support == 0L], collapse = ", "),
             "; ", london_policy, ").", call. = FALSE)
      }
    }
    tibble::tibble(
      market = market, family = family, stratum = stratum,
      coast_rule_m = settings$coast_rule_m, unknown_policy = settings$unknown_policy,
      n_before_london_drop = n_before, n_after_london_drop = n_after,
      drop_london = drop_london, n_estimation = sum(estimation),
      n_near_estimation = if ("near_bin" %in% names(data)) sum(estimation & data$near_bin == 1L) else NA_integer_,
      n_far_estimation = if ("near_bin" %in% names(data)) sum(estimation & data$near_bin == 0L) else NA_integer_,
      n_near_before = if ("near_bin" %in% names(data)) sum(selected & data$near_bin == 1L) else NA_integer_,
      n_near_after = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 1L) else NA_integer_,
      n_far_before = if ("near_bin" %in% names(data)) sum(selected & data$near_bin == 0L) else NA_integer_,
      n_far_after = if ("near_bin" %in% names(data)) sum(retained & data$near_bin == 0L) else NA_integer_,
      n_near_pre_after = unname(after_periods["near_pre"]),
      n_near_post_after = unname(after_periods["near_post"]),
      n_far_pre_after = unname(after_periods["far_pre"]),
      n_far_post_after = unname(after_periods["far_post"]),
      n_near_pre_estimation = unname(estimation_periods["near_pre"]),
      n_near_post_estimation = unname(estimation_periods["near_post"]),
      n_far_pre_estimation = unname(estimation_periods["far_pre"]),
      n_far_post_estimation = unname(estimation_periods["far_post"]),
      n_input = nrow(data), n_unclassified = n_unclassified,
      n_without_nearest = sum(is.na(data$site_id)),
      n_nearest_sites = nrow(sites),
      n_nearest_sites_missing_coast = sum(is.na(sites$distance_to_coast_m)),
      nearest_site_missing_coast_share = if (nrow(sites)) mean(is.na(sites$distance_to_coast_m)) else NA_real_
    )
  }))
  cat(sprintf("%s / %s / coast %sm / %s:\n", market, family,
              settings$coast_rule_m, settings$unknown_policy))
  for (i in seq_len(nrow(counts))) {
    cat(sprintf("  %s: %d before, %d after London drop; %d for estimation\n", counts$stratum[i],
                counts$n_before_london_drop[i], counts$n_after_london_drop[i],
                counts$n_estimation[i]))
  }
  counts
}
