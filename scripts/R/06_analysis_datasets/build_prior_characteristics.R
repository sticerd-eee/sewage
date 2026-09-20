# ==============================================================================
# Prior-to-Transaction Property Characteristics Builder
# ==============================================================================
#
# Reads only existing property-Site Group lookups, the Site Group companion,
# and the closed prior-exposure datasets. It does not reconstruct spill events
# or modify any existing exposure artifact.
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "open_coast_contracts.R"), local = TRUE)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)

REQUIRED_PACKAGES <- c("arrow", "dplyr", "here", "logger", "tibble", "tidyr")
check_required_packages(REQUIRED_PACKAGES)

RADII <- c(250L, 500L, 1000L)
MEASURES <- c("spill_count_weekly_avg", "spill_hrs_weekly_avg")
BAND_LEVELS <- c("no_site", "unknown", "zero", "spill_le_p50", "spill_gt_p50")
LOG_FILE <- here::here("output", "log", "build_prior_characteristics.log")

MARKET_SPECS <- list(
  sales = list(
    market = "sales",
    id = "house_id",
    lookup_path = here::here("data", "processed", "spill_house_lookup.parquet"),
    source_path = here::here(
      "data", "processed", "cross_section", "sales", "prior_to_sale"
    ),
    output_path = here::here(
      "data", "processed", "cross_section", "sales", "prior_characteristics"
    )
  ),
  rentals = list(
    market = "rentals",
    id = "rental_id",
    lookup_path = here::here(
      "data", "processed", "zoopla", "spill_rental_lookup.parquet"
    ),
    source_path = here::here(
      "data", "processed", "cross_section", "rentals", "prior_to_rental"
    ),
    output_path = here::here(
      "data", "processed", "cross_section", "rentals", "prior_characteristics"
    )
  )
)

SITE_CHARACTERISTICS_PATH <- here::here(
  "data", "processed", "site_characteristics", "site_group_characteristics.parquet"
)
CUTOFF_OUTPUT_PATH <- here::here(
  "data", "processed", "cross_section", "prior_intensity_cutoffs.parquet"
)

prior_characteristics_columns <- function(id_column, refined = FALSE) {
  c(
    id_column, "radius", "n_spill_sites", "min_coast_dist_m",
    "max_coast_dist_m", "any_bath_2124", "all_bath_2124",
    "mixed_bath_2124", "bath_unknown_2124", "n_bath_2124",
    "any_shell_2124", "all_shell_2124", "mixed_shell_2124",
    "shell_unknown_2124", "n_shell_2124", "spill_count_band",
    "spill_hrs_band", if (refined) open_coast_radius_columns()
  )
}

prior_characteristics_schema <- function(id_column, include_radius = TRUE, refined = FALSE) {
  fields <- list()
  fields[[id_column]] <- arrow::utf8()
  if (include_radius) fields$radius <- arrow::int32()
  fields$n_spill_sites <- arrow::int32()
  fields$min_coast_dist_m <- arrow::float64()
  fields$max_coast_dist_m <- arrow::float64()
  for (type in c("bath", "shell")) {
    fields[[paste0("any_", type, "_2124")]] <- arrow::boolean()
    fields[[paste0("all_", type, "_2124")]] <- arrow::boolean()
    fields[[paste0("mixed_", type, "_2124")]] <- arrow::boolean()
    fields[[paste0(type, "_unknown_2124")]] <- arrow::boolean()
    fields[[paste0("n_", type, "_2124")]] <- arrow::int32()
  }
  fields$spill_count_band <- arrow::utf8()
  fields$spill_hrs_band <- arrow::utf8()
  if (refined) {
    fields$min_open_coast_dist_m <- arrow::float64()
    fields$max_open_coast_dist_m <- arrow::float64()
    fields$n_open_coast_known <- arrow::int32()
    fields$n_open_coast_missing <- arrow::int32()
    fields$site_generation <- arrow::utf8()
  }
  do.call(arrow::schema, fields)
}

cutoff_audit_schema <- function() {
  arrow::schema(
    market = arrow::utf8(),
    radius = arrow::int32(),
    measure = arrow::utf8(),
    p50 = arrow::float64(),
    n_total = arrow::int64(),
    n_no_site = arrow::int64(),
    n_unknown = arrow::int64(),
    n_zero = arrow::int64(),
    n_positive = arrow::int64(),
    n_le_p50 = arrow::int64(),
    n_gt_p50 = arrow::int64()
  )
}

validate_source_universe <- function(data, id_column, radii = RADII) {
  data <- tibble::as_tibble(data)
  required <- c(
    id_column, "radius", "n_spill_sites", "spill_count_weekly_avg",
    "spill_hrs_weekly_avg"
  )
  missing_columns <- setdiff(required, names(data))
  if (length(missing_columns) > 0L) {
    stop("Prior exposure source is missing: ", paste(missing_columns, collapse = ", "), call. = FALSE)
  }
  if (anyNA(data[c(id_column, "radius", "n_spill_sites")]) ||
      anyDuplicated(data[c(id_column, "radius")])) {
    stop("Prior exposure source keys must be complete and unique.", call. = FALSE)
  }
  if (any(!data$radius %in% radii)) stop("Prior exposure source has an unsupported radius.", call. = FALSE)
  if (any(data$n_spill_sites < 0L)) stop("n_spill_sites must be non-negative.", call. = FALSE)
  for (measure in MEASURES) {
    values <- data[[measure]]
    if (any(!is.na(values) & (!is.finite(values) | values < 0))) {
      stop(measure, " must contain non-negative finite values or NA.", call. = FALSE)
    }
  }
  invisible(data)
}

summarise_property_pairs <- function(pairs, site_characteristics, id_column, refined = FALSE) {
  if (refined) validate_open_coast_sites(site_characteristics)
  if (anyDuplicated(pairs[c(id_column, "radius", "site_id")])) stop("Duplicate property-site pair.")
  required_site <- c(
    "site_id", "distance_to_coast_m", "bath_ever_2124",
    "bath_unknown_2124", "shell_ever_2124", "shell_unknown_2124"
  )
  missing_site <- setdiff(required_site, names(site_characteristics))
  if (length(missing_site) > 0L) {
    stop("Site characteristics are missing: ", paste(missing_site, collapse = ", "), call. = FALSE)
  }
  if (anyNA(site_characteristics$site_id) || anyDuplicated(site_characteristics$site_id)) {
    stop("Site characteristics must be complete and unique on site_id.", call. = FALSE)
  }
  before <- nrow(pairs)
  joined <- pairs |>
    dplyr::left_join(
      dplyr::mutate(site_characteristics, characteristic_matched = TRUE),
      by = "site_id"
    )
  if (nrow(joined) != before || anyNA(joined$characteristic_matched)) {
    stop("Every real property-Site Group pair must join one characteristic row.", call. = FALSE)
  }
  output <- joined |>
    dplyr::mutate(
      bath_designated = tidyr::replace_na(.data$bath_ever_2124, FALSE),
      bath_uncertain = .data$bath_unknown_2124 | is.na(.data$bath_ever_2124),
      shell_designated = tidyr::replace_na(.data$shell_ever_2124, FALSE),
      shell_uncertain = .data$shell_unknown_2124 | is.na(.data$shell_ever_2124)
    ) |>
    dplyr::summarise(
      n_pair_sites = dplyr::n(),
      min_coast_dist_m = if (all(is.na(.data$distance_to_coast_m))) NA_real_ else
        min(.data$distance_to_coast_m, na.rm = TRUE),
      max_coast_dist_m = if (all(is.na(.data$distance_to_coast_m))) NA_real_ else
        max(.data$distance_to_coast_m, na.rm = TRUE),
      any_bath_2124 = any(.data$bath_designated),
      all_bath_2124 = all(.data$bath_designated) && !any(.data$bath_uncertain),
      mixed_bath_2124 = any(.data$bath_designated) &&
        any(!.data$bath_designated & !.data$bath_uncertain),
      bath_unknown_2124 = any(.data$bath_uncertain),
      n_bath_2124 = sum(.data$bath_designated),
      any_shell_2124 = any(.data$shell_designated),
      all_shell_2124 = all(.data$shell_designated) && !any(.data$shell_uncertain),
      mixed_shell_2124 = any(.data$shell_designated) &&
        any(!.data$shell_designated & !.data$shell_uncertain),
      shell_unknown_2124 = any(.data$shell_uncertain),
      n_shell_2124 = sum(.data$shell_designated),
      .by = dplyr::all_of(c(id_column, "radius"))
    )
  if (refined) {
    refined_evidence <- joined |>
      dplyr::summarise(
        min_open_coast_dist_m = if (all(is.na(.data$distance_to_open_coast_m))) NA_real_ else
          min(.data$distance_to_open_coast_m, na.rm = TRUE),
        max_open_coast_dist_m = if (all(is.na(.data$distance_to_open_coast_m))) NA_real_ else
          max(.data$distance_to_open_coast_m, na.rm = TRUE),
        n_open_coast_known = sum(!is.na(.data$distance_to_open_coast_m)),
        n_open_coast_missing = sum(is.na(.data$distance_to_open_coast_m)),
        .by = dplyr::all_of(c(id_column, "radius")))
    output <- dplyr::left_join(output, refined_evidence, by = c(id_column, "radius"))
  }
  output
}

aggregate_property_characteristics <- function(
    source_data, lookup_data, site_characteristics, id_column, radii = RADII, refined = FALSE) {
  validate_source_universe(source_data, id_column, radii)
  required_lookup <- c(id_column, "site_id", "distance_m")
  if (!all(required_lookup %in% names(lookup_data))) {
    stop("Property-Site Group lookup is missing required columns.", call. = FALSE)
  }
  if (anyNA(lookup_data[[id_column]])) stop("Property lookup identifiers are missing.")
  lookup_data <- dplyr::filter(lookup_data, !(is.na(.data$site_id) & is.na(.data$distance_m)))
  if (anyNA(lookup_data[required_lookup]) ||
      anyDuplicated(lookup_data[c(id_column, "site_id")]) ||
      any(!is.finite(lookup_data$distance_m) | lookup_data$distance_m < 0)) {
    stop("Property-Site Group lookup keys and distances must be complete and unique.", call. = FALSE)
  }
  expanded <- dplyr::bind_rows(lapply(radii, function(radius_value) {
    lookup_data |>
      dplyr::filter(.data$distance_m <= .env$radius_value) |>
      dplyr::mutate(radius = as.integer(.env$radius_value))
  }))
  aggregated <- summarise_property_pairs(expanded, site_characteristics, id_column, refined)
  output <- source_data |>
    dplyr::select(dplyr::all_of(c(id_column, "radius", "n_spill_sites"))) |>
    dplyr::left_join(aggregated, by = c(id_column, "radius"))
  if (any(!is.na(output$n_pair_sites) & output$n_pair_sites != output$n_spill_sites) ||
      any(is.na(output$n_pair_sites) & output$n_spill_sites != 0L)) {
    stop("Lookup Site Group counts do not equal source n_spill_sites.", call. = FALSE)
  }
  output <- output |>
    dplyr::mutate(
      min_coast_dist_m = dplyr::if_else(.data$n_spill_sites == 0L, NA_real_, .data$min_coast_dist_m),
      max_coast_dist_m = dplyr::if_else(.data$n_spill_sites == 0L, NA_real_, .data$max_coast_dist_m),
      dplyr::across(dplyr::starts_with("any_"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::starts_with("mixed_"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::ends_with("_unknown_2124"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::starts_with("n_"), ~ tidyr::replace_na(as.integer(.x), 0L)),
      dplyr::across(dplyr::starts_with("all_"),
        ~ dplyr::if_else(.data$n_spill_sites == 0L, NA, tidyr::replace_na(.x, FALSE)))
    ) |>
    dplyr::select(-"n_pair_sites")
  if (refined) output$site_generation <- single_open_coast_generation(site_characteristics)
  output
}
measure_band <- function(values, n_spill_sites, radius, market, measure) {
  if (any(!is.na(values) & (!is.finite(values) | values < 0))) {
    stop(measure, " must contain non-negative finite values or NA.", call. = FALSE)
  }
  positive <- values[n_spill_sites > 0L & !is.na(values) & values > 0]
  if (length(positive) == 0L) {
    stop(market, " radius ", radius, " ", measure,
      " has no positive rows for a p50 cutoff.", call. = FALSE)
  }
  p50 <- as.numeric(stats::median(positive))
  band <- dplyr::case_when(
    n_spill_sites == 0L ~ "no_site",
    is.na(values) ~ "unknown",
    values == 0 ~ "zero",
    values <= p50 ~ "spill_le_p50",
    TRUE ~ "spill_gt_p50"
  )
  list(band = band, p50 = p50)
}

add_intensity_bands <- function(data, market) {
  data$.source_order <- seq_len(nrow(data))
  pieces <- lapply(sort(unique(data$radius)), function(radius_value) {
    piece <- data[data$radius == radius_value, , drop = FALSE]
    for (measure in MEASURES) {
      result <- measure_band(
        piece[[measure]], piece$n_spill_sites, radius_value, market, measure
      )
      band_column <- if (measure == "spill_count_weekly_avg") {
        "spill_count_band"
      } else {
        "spill_hrs_band"
      }
      piece[[band_column]] <- result$band
    }
    piece
  })
  dplyr::bind_rows(pieces) |>
    dplyr::arrange(.data$.source_order) |>
    dplyr::select(-".source_order")
}

build_intensity_cutoff_audit <- function(data) {
  required <- c("market", "radius", "n_spill_sites", MEASURES)
  if (!all(required %in% names(data))) stop("Cutoff input is missing required columns.", call. = FALSE)
  rows <- list()
  index <- 0L
  for (market_value in sort(unique(data$market))) {
    for (radius_value in sort(unique(data$radius[data$market == market_value]))) {
      piece <- data[data$market == market_value & data$radius == radius_value, , drop = FALSE]
      for (measure in MEASURES) {
        index <- index + 1L
        result <- measure_band(
          piece[[measure]], piece$n_spill_sites, radius_value, market_value, measure
        )
        values <- piece[[measure]]
        positive <- piece$n_spill_sites > 0L & !is.na(values) & values > 0
        rows[[index]] <- tibble::tibble(
          market = market_value,
          radius = as.integer(radius_value),
          measure = measure,
          p50 = result$p50,
          n_total = as.double(nrow(piece)),
          n_no_site = as.double(sum(piece$n_spill_sites == 0L)),
          n_unknown = as.double(sum(piece$n_spill_sites > 0L & is.na(values))),
          n_zero = as.double(sum(piece$n_spill_sites > 0L & !is.na(values) & values == 0)),
          n_positive = as.double(sum(positive)),
          n_le_p50 = as.double(sum(positive & values <= result$p50, na.rm = TRUE)),
          n_gt_p50 = as.double(sum(positive & values > result$p50, na.rm = TRUE))
        )
      }
    }
  }
  output <- dplyr::bind_rows(rows)
  validate_cutoff_audit(output)
  output
}

validate_cutoff_audit <- function(data) {
  data <- tibble::as_tibble(data)
  expected_columns <- c(
    "market", "radius", "measure", "p50", "n_total", "n_no_site",
    "n_unknown", "n_zero", "n_positive", "n_le_p50", "n_gt_p50"
  )
  if (!identical(names(data), expected_columns)) stop("Cutoff audit schema is not exact.", call. = FALSE)
  if (anyNA(data[c("market", "radius", "measure", "p50")]) ||
      anyDuplicated(data[c("market", "radius", "measure")])) {
    stop("Cutoff audit keys and p50 values must be complete and unique.", call. = FALSE)
  }
  if (any(data$n_total != data$n_no_site + data$n_unknown + data$n_zero + data$n_positive) ||
      any(data$n_positive != data$n_le_p50 + data$n_gt_p50)) {
    stop("Cutoff audit counts do not reconcile.", call. = FALSE)
  }
  invisible(data)
}

build_market_prior_characteristics <- function(
    source_data, lookup_data, site_characteristics, id_column, market,
    radii = RADII, refined = FALSE) {
  aggregated <- aggregate_property_characteristics(
    source_data, lookup_data, site_characteristics, id_column, radii, refined
  )
  band_input <- dplyr::left_join(
    aggregated,
    dplyr::select(source_data, dplyr::all_of(c(id_column, "radius", MEASURES))),
    by = c(id_column, "radius")
  )
  add_intensity_bands(band_input, market) |>
    dplyr::select(dplyr::all_of(prior_characteristics_columns(id_column, refined)))
}

validate_prior_characteristics <- function(data, source_data, id_column, refined = FALSE) {
  data <- tibble::as_tibble(data)
  source_data <- tibble::as_tibble(source_data)
  if (!identical(names(data), prior_characteristics_columns(id_column, refined))) {
    stop("Prior characteristics must have the exact hand-written schema.", call. = FALSE)
  }
  missing_key_values <- sum(is.na(data[[id_column]])) + sum(is.na(data$radius)) +
    sum(is.na(data$n_spill_sites))
  duplicate_keys <- anyDuplicated(data[c(id_column, "radius")])
  if (missing_key_values > 0L || duplicate_keys > 0L) {
    stop(
      "Prior characteristics keys must be complete and unique (missing=",
      missing_key_values, ", first_duplicate=", duplicate_keys, ").",
      call. = FALSE
    )
  }
  source_keys <- source_data |>
    dplyr::select(dplyr::all_of(c(id_column, "radius", "n_spill_sites"))) |>
    dplyr::arrange(.data[[id_column]], .data$radius)
  output_keys <- data |>
    dplyr::select(dplyr::all_of(c(id_column, "radius", "n_spill_sites"))) |>
    dplyr::arrange(.data[[id_column]], .data$radius)
  if (!identical(source_keys, output_keys)) {
    stop("Prior characteristics keys or n_spill_sites differ from the source.", call. = FALSE)
  }
  band_columns <- c("spill_count_band", "spill_hrs_band")
  band_values <- unlist(dplyr::select(data, dplyr::all_of(band_columns)), use.names = FALSE)
  invalid_bands <- unique(band_values[is.na(band_values) | !band_values %in% BAND_LEVELS])
  if (length(invalid_bands) > 0L) {
    stop(
      "Intensity bands contain values outside the allowed enum: ",
      paste(invalid_bands, collapse = ", "), ".",
      call. = FALSE
    )
  }
  if (any(!is.na(data$min_coast_dist_m) &
      (!is.finite(data$min_coast_dist_m) | data$min_coast_dist_m < 0)) ||
      any(!is.na(data$max_coast_dist_m) &
        (!is.finite(data$max_coast_dist_m) | data$max_coast_dist_m < 0))) {
    stop("Coast-distance summaries must be non-negative finite values or NA.", call. = FALSE)
  }
  if (refined) validate_open_coast_radius(data)
  invisible(data)
}

read_source_radius <- function(spec, radius_value) {
  arrow::open_dataset(spec$source_path) |>
    dplyr::filter(.data$radius == !!radius_value) |>
    dplyr::select(dplyr::all_of(c(spec$id, "radius", "n_spill_sites", MEASURES))) |>
    dplyr::collect()
}

aggregate_pairs_arrow <- function(spec, radius_value, site_path, refined = FALSE) {
  sites <- arrow::read_parquet(site_path)
  if (anyNA(sites$site_id) || anyDuplicated(sites$site_id)) stop("Site keys must be unique and complete.")
  if (refined) validate_open_coast_sites(sites)
  bad_distances <- arrow::open_dataset(spec$lookup_path) |>
    # The lookup retains no-match placeholders with missing site and distance.
    # They are outside every radius; real pairs still require valid distances.
    dplyr::filter((!is.na(.data$site_id) & (is.na(.data$distance_m) | !is.finite(.data$distance_m))) |
      .data$distance_m < 0) |>
    dplyr::select("distance_m") |> head(1L) |> dplyr::collect()
  if (nrow(bad_distances)) stop("Property-site distances must be finite and nonnegative.")
  pairs <- arrow::open_dataset(spec$lookup_path) |>
    dplyr::filter(.data$distance_m <= !!radius_value)
  invalid <- pairs |>
    dplyr::group_by(.data[[spec$id]], .data$site_id) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::filter(.data$n > 1L | is.na(.data[[spec$id]]) | is.na(.data$site_id)) |>
    dplyr::collect()
  if (nrow(invalid)) stop("Duplicate or missing property-site pair keys.")
  site_query <- arrow::open_dataset(site_path) |>
    dplyr::select(
      "site_id", "distance_to_coast_m", "bath_ever_2124",
      "bath_unknown_2124", "shell_ever_2124", "shell_unknown_2124",
      dplyr::all_of(if (refined) "distance_to_open_coast_m" else character())
    ) |>
    dplyr::mutate(characteristic_matched = 1L)
  if (!refined) site_query <- dplyr::mutate(site_query, distance_to_open_coast_m = NA_real_)
  output <- pairs |>
    dplyr::select(dplyr::all_of(c(spec$id, "site_id", "distance_m"))) |>
    dplyr::filter(.data$distance_m <= !!radius_value) |>
    dplyr::left_join(site_query, by = "site_id") |>
    dplyr::mutate(
      characteristic_missing = dplyr::if_else(is.na(.data$characteristic_matched), 1L, 0L),
      bath_designated = dplyr::if_else(.data$bath_ever_2124, 1L, 0L),
      bath_unknown = dplyr::if_else(.data$bath_unknown_2124, 1L, 0L),
      bath_negative = dplyr::if_else(!.data$bath_ever_2124 & !.data$bath_unknown_2124, 1L, 0L),
      shell_designated = dplyr::if_else(.data$shell_ever_2124, 1L, 0L),
      shell_unknown = dplyr::if_else(.data$shell_unknown_2124, 1L, 0L),
      shell_negative = dplyr::if_else(!.data$shell_ever_2124 & !.data$shell_unknown_2124, 1L, 0L)
    ) |>
    dplyr::group_by(.data[[spec$id]]) |>
    dplyr::summarise(
      n_pair_sites = dplyr::n(),
      min_open_coast_dist_m = min(.data$distance_to_open_coast_m, na.rm = TRUE),
      max_open_coast_dist_m = max(.data$distance_to_open_coast_m, na.rm = TRUE),
      n_open_coast_known = sum(dplyr::if_else(is.na(.data$distance_to_open_coast_m), 0L, 1L)),
      n_open_coast_missing = sum(dplyr::if_else(is.na(.data$distance_to_open_coast_m), 1L, 0L)),
      n_characteristic_missing = sum(.data$characteristic_missing),
      min_coast_dist_m = min(.data$distance_to_coast_m, na.rm = TRUE),
      max_coast_dist_m = max(.data$distance_to_coast_m, na.rm = TRUE),
      n_bath_2124 = sum(.data$bath_designated),
      n_bath_unknown = sum(.data$bath_unknown),
      n_bath_negative = sum(.data$bath_negative),
      n_shell_2124 = sum(.data$shell_designated),
      n_shell_unknown = sum(.data$shell_unknown),
      n_shell_negative = sum(.data$shell_negative)
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      radius = as.integer(radius_value),
      min_open_coast_dist_m = dplyr::if_else(.data$n_open_coast_known == 0L, NA_real_, .data$min_open_coast_dist_m),
      max_open_coast_dist_m = dplyr::if_else(.data$n_open_coast_known == 0L, NA_real_, .data$max_open_coast_dist_m),
      any_bath_2124 = .data$n_bath_2124 > 0L,
      all_bath_2124 = .data$n_bath_2124 == .data$n_pair_sites & .data$n_bath_unknown == 0L,
      mixed_bath_2124 = .data$n_bath_2124 > 0L & .data$n_bath_negative > 0L,
      bath_unknown_2124 = .data$n_bath_unknown > 0L,
      any_shell_2124 = .data$n_shell_2124 > 0L,
      all_shell_2124 = .data$n_shell_2124 == .data$n_pair_sites & .data$n_shell_unknown == 0L,
      mixed_shell_2124 = .data$n_shell_2124 > 0L & .data$n_shell_negative > 0L,
      shell_unknown_2124 = .data$n_shell_unknown > 0L
    ) |>
    dplyr::select(
      dplyr::all_of(spec$id), "radius", "n_pair_sites",
      "n_characteristic_missing", "min_coast_dist_m", "max_coast_dist_m",
      "any_bath_2124", "all_bath_2124", "mixed_bath_2124",
      "bath_unknown_2124", "n_bath_2124", "any_shell_2124",
      "all_shell_2124", "mixed_shell_2124", "shell_unknown_2124",
      "n_shell_2124",
      dplyr::all_of(if (refined) setdiff(open_coast_radius_columns(), "site_generation") else character())
    )
  if (refined) output$site_generation <- single_open_coast_generation(sites)
  output
}

build_production_radius <- function(spec, radius_value, site_path, refined = FALSE) {
  source <- read_source_radius(spec, radius_value)
  validate_source_universe(source, spec$id, radius_value)
  aggregated <- aggregate_pairs_arrow(spec, radius_value, site_path, refined)
  if (any(aggregated$n_characteristic_missing != 0L)) {
    stop(spec$market, " radius ", radius_value,
      " contains property-Site Group pairs without characteristics.", call. = FALSE)
  }
  output <- source |>
    dplyr::left_join(aggregated, by = c(spec$id, "radius"))
  if (any(!is.na(output$n_pair_sites) & output$n_pair_sites != output$n_spill_sites) ||
      any(is.na(output$n_pair_sites) & output$n_spill_sites != 0L)) {
    stop(spec$market, " radius ", radius_value,
      " lookup counts differ from source n_spill_sites.", call. = FALSE)
  }
  output <- output |>
    dplyr::mutate(
      min_coast_dist_m = dplyr::if_else(.data$n_spill_sites == 0L, NA_real_, .data$min_coast_dist_m),
      max_coast_dist_m = dplyr::if_else(.data$n_spill_sites == 0L, NA_real_, .data$max_coast_dist_m),
      dplyr::across(dplyr::starts_with("any_"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::starts_with("mixed_"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::ends_with("_unknown_2124"), ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::matches("^n_(bath|shell)_2124$|^n_open_coast_(known|missing)$"),
        ~ tidyr::replace_na(as.integer(.x), 0L)),
      dplyr::across(dplyr::starts_with("all_"),
        ~ dplyr::if_else(.data$n_spill_sites == 0L, NA, tidyr::replace_na(.x, FALSE)))
    )
  if (refined) output$site_generation <- single_open_coast_generation(arrow::read_parquet(site_path))
  audit <- build_intensity_cutoff_audit(
    dplyr::mutate(
      dplyr::select(output, "radius", "n_spill_sites", dplyr::all_of(MEASURES)),
      market = spec$market,
      .before = 1L
    )
  )
  output <- add_intensity_bands(output, spec$market) |>
    dplyr::select(dplyr::all_of(prior_characteristics_columns(spec$id, refined)))
  validate_prior_characteristics(output, source, spec$id, refined)
  list(data = output, audit = audit)
}

validate_published_market <- function(path, spec, refined = FALSE, expected_generation = NULL) {
  if (refined) validate_open_coast_companion_manifest(path, expected_generation)
  dataset <- arrow::open_dataset(path)
  expected_columns <- prior_characteristics_columns(spec$id, refined)
  if (!setequal(dataset$schema$names, expected_columns) ||
      length(dataset$schema$names) != length(expected_columns)) {
    stop(spec$market, " published schema is not exact.", call. = FALSE)
  }
  expected_signature <- arrow_schema_signature(
    prior_characteristics_schema(spec$id, include_radius = TRUE, refined = refined)
  )
  observed_signature <- arrow_schema_signature(dataset$schema)
  if (!identical(
      observed_signature[names(expected_signature)], expected_signature)) {
    stop(spec$market, " published physical types are not exact.", call. = FALSE)
  }
  observed_radii <- sort(unique(
    dataset |> dplyr::select("radius") |> dplyr::distinct() |> dplyr::collect() |>
      dplyr::pull("radius")
  ))
  if (!identical(as.integer(observed_radii), RADII)) {
    stop(spec$market, " published radius partitions are incomplete.", call. = FALSE)
  }
  for (radius_value in RADII) {
    output <- dataset |>
      dplyr::filter(.data$radius == !!radius_value) |>
      dplyr::collect() |>
      dplyr::select(dplyr::all_of(expected_columns))
    source <- read_source_radius(spec, radius_value)
    validate_prior_characteristics(output, source, spec$id, refined)
    if (refined) validate_open_coast_radius(output, expected_generation)
  }
  invisible(path)
}

write_market_stage <- function(stage_path, spec, site_path, refined = FALSE) {
  dir.create(stage_path, recursive = TRUE, showWarnings = FALSE)
  audits <- list()
  for (index in seq_along(RADII)) {
    radius_value <- RADII[[index]]
    logger::log_info("Building {spec$market} radius {radius_value} m")
    result <- build_production_radius(spec, radius_value, site_path, refined)
    partition_path <- file.path(stage_path, paste0("radius=", radius_value))
    dir.create(partition_path, recursive = TRUE, showWarnings = FALSE)
    partition_data <- dplyr::select(result$data, -"radius")
    table <- arrow::Table$create(
      partition_data,
      schema = prior_characteristics_schema(spec$id, include_radius = FALSE, refined = refined)
    )
    arrow::write_parquet(table, file.path(partition_path, "part-0.parquet"))
    audits[[index]] <- result$audit
    logger::log_info(
      "{spec$market} radius {radius_value}: source_rows={nrow(result$data)}, lookup_matched_rows={sum(result$data$n_spill_sites > 0L)}, no_site={sum(result$data$n_spill_sites == 0L)}, bath_unknown={sum(result$data$bath_unknown_2124)}, shell_unknown={sum(result$data$shell_unknown_2124)}"
    )
    for (measure in MEASURES) {
      row <- result$audit[result$audit$measure == measure, ]
      logger::log_info(
        "{spec$market} radius {radius_value} {measure}: p50={row$p50}, no_site={row$n_no_site}, unknown={row$n_unknown}, zero={row$n_zero}, positive={row$n_positive}, le={row$n_le_p50}, gt={row$n_gt_p50}"
      )
    }
  }
  list(audit = dplyr::bind_rows(audits))
}

publish_market <- function(spec, site_path = SITE_CHARACTERISTICS_PATH, refined = TRUE) {
  dir.create(dirname(spec$output_path), recursive = TRUE, showWarnings = FALSE)
  stage_path <- file.path(
    dirname(spec$output_path),
    paste0(".", basename(spec$output_path), ".stage-", Sys.getpid())
  )
  if (dir.exists(stage_path)) unlink(stage_path, recursive = TRUE)
  on.exit({
    if (dir.exists(stage_path)) unlink(stage_path, recursive = TRUE)
  }, add = TRUE)
  result <- write_market_stage(stage_path, spec, site_path, refined)
  if (refined) {
    input_paths <- c(site_path, spec$lookup_path,
      if (dir.exists(spec$source_path)) list.files(spec$source_path, pattern = "[.]parquet$", recursive = TRUE,
        full.names = TRUE) else spec$source_path)
    entry <- function(path, relative) list(path = relative, sha256 = digest::digest(file = path, algo = "sha256"))
    inputs <- lapply(sort(unique(input_paths)), function(path) entry(path,
      if (startsWith(path, paste0(here::here(), "/"))) substring(path, nchar(here::here()) + 2L) else path))
    outputs <- lapply(paste0("radius=", RADII, "/part-0.parquet"), function(relative)
      entry(file.path(stage_path, relative), relative))
    jsonlite::write_json(list(status = "complete", site_generation = single_open_coast_generation(arrow::read_parquet(site_path)),
      inputs = inputs, outputs = outputs,
      builder_sha256 = digest::digest(file = here::here("scripts", "R", "06_analysis_datasets", "build_prior_characteristics.R"), algo = "sha256")),
      file.path(stage_path, "_open_coast_manifest.json"), auto_unbox = TRUE, pretty = TRUE)
  }
  if (dir.exists(spec$output_path)) {
    legacy_columns <- prior_characteristics_columns(spec$id)
    for (radius_value in RADII) {
      read_legacy <- function(path) arrow::open_dataset(path) |>
        dplyr::filter(.data$radius == !!radius_value) |>
        dplyr::select(dplyr::all_of(legacy_columns)) |> dplyr::collect() |>
        dplyr::arrange(.data[[spec$id]])
      if (!isTRUE(all.equal(as.data.frame(read_legacy(stage_path)),
                           as.data.frame(read_legacy(spec$output_path)), tolerance = 0, check.attributes = FALSE)))
        stop("Legacy property characteristics changed for ", spec$market, " / ", radius_value, ".")
    }
  }
  publish_validated_dataset(
    stage_path, spec$output_path,
    validate = function(path) validate_published_market(path, spec, refined,
      if (refined) single_open_coast_generation(arrow::read_parquet(site_path)) else NULL)
  )
  result$audit
}

publish_cutoff_audit <- function(data, output_path = CUTOFF_OUTPUT_PATH) {
  validate_cutoff_audit(data)
  if (file.exists(output_path)) {
    ordered <- function(x) dplyr::arrange(x, .data$market, .data$radius, .data$measure)
    if (!isTRUE(all.equal(as.data.frame(ordered(data)), as.data.frame(ordered(arrow::read_parquet(output_path))),
                         tolerance = 0, check.attributes = FALSE))) stop("Existing intensity cutoffs changed.")
  }
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  candidate <- file.path(dirname(output_path), paste0(".", basename(output_path), ".candidate"))
  if (file.exists(candidate)) unlink(candidate)
  arrow::write_parquet(
    arrow::Table$create(data, schema = cutoff_audit_schema()), candidate
  )
  validate_file <- function(path) {
    if (!identical(
        arrow_schema_signature(arrow::open_dataset(path)$schema),
        arrow_schema_signature(cutoff_audit_schema()))) {
      stop("Cutoff audit physical types do not match the hand-written schema.",
        call. = FALSE)
    }
    validate_cutoff_audit(arrow::read_parquet(path))
  }
  publish_validated_file(candidate, output_path, validate_file)
}

main <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "INFO")
  if (!file.exists(SITE_CHARACTERISTICS_PATH)) {
    stop("Missing Site Group characteristics: ", SITE_CHARACTERISTICS_PATH, call. = FALSE)
  }
  audits <- lapply(MARKET_SPECS, publish_market)
  cutoff_audit <- dplyr::bind_rows(audits) |>
    dplyr::arrange(.data$market, .data$radius, .data$measure)
  publish_cutoff_audit(cutoff_audit)
  logger::log_info("Published cutoff audit: {CUTOFF_OUTPUT_PATH}")
}

if (sys.nframe() == 0L) main()
