# ==============================================================================
# Site Group Environmental Characteristics Builder
# ==============================================================================
#
# Inputs:
#   - data/processed/annual_return_edm.parquet
#   - data/processed/annual_return_lookup.parquet
#   - data/processed/unique_spill_sites.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#   - data/raw/shapefiles/UK_Nations/CTRY_DEC_2024_UK_BGC.shp
#
# Output:
#   - data/processed/site_characteristics/site_group_characteristics.parquet
#
# Site Coast Distance is distance to the nearest Mean High Water tidal line.
# It is a near-coast location measure, not a receiving-water classification or
# a measure of distance to open sea.
# Boundary source: Office for National Statistics, December 2024 Countries
# (BGC), licensed under the Open Government Licence v3.0. Contains OS data
# © Crown copyright and database right 2024.
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop("Package `here` is required to run this script.", call. = FALSE)
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "site_group_utils.R"), local = TRUE)
source(here::here("scripts", "R", "utils", "open_coast_contracts.R"), local = TRUE)
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"), local = TRUE)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)

REQUIRED_PACKAGES <- c(
  "arrow", "dplyr", "here", "logger", "sf", "tibble", "tidyr"
)
check_required_packages(REQUIRED_PACKAGES)

YEARS <- 2021:2024
LOG_FILE <- here::here("output", "log", "build_site_group_characteristics.log")

CONFIG <- list(
  open_coast_path = here::here("data", "processed", "geography", "open_coast", "reference.rds"),
  annual_path = here::here("data", "processed", "annual_return_edm.parquet"),
  lookup_path = here::here("data", "processed", "annual_return_lookup.parquet"),
  membership_path = here::here("data", "processed", "unique_spill_sites.parquet"),
  crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  boundary_path = here::here(
    "data", "raw", "shapefiles", "UK_Nations",
    "CTRY_DEC_2024_UK_BGC.shp"
  ),
  output_path = here::here(
    "data", "processed", "site_characteristics",
    "site_group_characteristics.parquet"
  )
)

site_group_characteristics_columns <- function(refined = FALSE) {
  annual <- unlist(lapply(c("bath", "shell"), function(type) {
    unlist(lapply(c("status", "mixed", "unknown"), function(field) {
      paste0(type, "_", field, "_", 21:24)
    }), use.names = FALSE)
  }), use.names = FALSE)
  summaries <- unlist(lapply(c("bath", "shell"), function(type) {
    paste0(type, c("_ever_2124", "_changed_2124", "_unknown_2124", "_24"))
  }), use.names = FALSE)
  c("site_id", "distance_to_coast_m", annual, summaries,
    if (refined) open_coast_site_columns())
}

site_group_characteristics_schema <- function(refined = FALSE) {
  fields <- list(
    site_id = arrow::int32(),
    distance_to_coast_m = arrow::float64()
  )
  for (type in c("bath", "shell")) {
    for (year in 21:24) fields[[paste0(type, "_status_", year)]] <- arrow::utf8()
    for (year in 21:24) fields[[paste0(type, "_mixed_", year)]] <- arrow::boolean()
    for (year in 21:24) fields[[paste0(type, "_unknown_", year)]] <- arrow::boolean()
  }
  for (type in c("bath", "shell")) {
    fields[[paste0(type, "_ever_2124")]] <- arrow::boolean()
    fields[[paste0(type, "_changed_2124")]] <- arrow::boolean()
    fields[[paste0(type, "_unknown_2124")]] <- arrow::boolean()
    fields[[paste0(type, "_24")]] <- arrow::boolean()
  }
  if (refined) {
    fields$distance_to_open_coast_m <- arrow::float64()
    fields$open_coast_status <- arrow::utf8()
    fields$geometry_generation <- arrow::utf8()
    fields$site_generation <- arrow::utf8()
  }
  do.call(arrow::schema, fields)
}

classify_designation_value <- function(value, row_present = TRUE) {
  if (length(row_present) == 1L) row_present <- rep(row_present, length(value))
  if (length(value) != length(row_present) || anyNA(row_present)) {
    stop("value and row_present must have compatible, non-missing lengths.", call. = FALSE)
  }
  normalized <- tolower(trimws(as.character(value)))
  normalized[is.na(normalized)] <- ""
  negatives <- c("", "0", "no", "not applicable")
  unknowns <- c("tbc", "to be confirmed", "unknown")
  dplyr::case_when(
    !row_present ~ "unknown",
    normalized %in% negatives ~ "not_designated",
    normalized %in% unknowns ~ "unknown",
    TRUE ~ "designated"
  )
}

validate_unique_mapping <- function(data, key, context) {
  if (!key %in% names(data) || anyNA(data[[key]]) || anyDuplicated(data[[key]])) {
    stop(context, " must be complete and unique on ", key, ".", call. = FALSE)
  }
  invisible(data)
}

reduce_designation_rows <- function(data, group_columns) {
  summarise_type <- function(type) {
    status_column <- paste0(type, "_status")
    mixed_column <- paste0(type, "_mixed")
    unknown_column <- paste0(type, "_unknown")
    data |>
      dplyr::group_by(dplyr::across(dplyr::all_of(group_columns))) |>
      dplyr::summarise(
        has_designated = any(.data[[status_column]] == "designated"),
        has_negative = any(.data[[status_column]] == "not_designated"),
        has_unknown = any(.data[[status_column]] == "unknown") |
          any(.data[[unknown_column]]),
        input_mixed = any(.data[[mixed_column]]),
        .groups = "drop"
      ) |>
      dplyr::transmute(
        dplyr::across(dplyr::all_of(group_columns)),
        "{type}_status" := dplyr::case_when(
          .data$has_designated ~ "designated",
          .data$has_unknown ~ "unknown",
          TRUE ~ "not_designated"
        ),
        "{type}_mixed" := .data$input_mixed |
          (.data$has_designated & .data$has_negative),
        "{type}_unknown" := .data$has_unknown
      )
  }
  dplyr::left_join(
    summarise_type("bath"),
    summarise_type("shell"),
    by = group_columns
  )
}

build_lookup_long <- function(lookup_data, years) {
  lookup_data <- tibble::as_tibble(lookup_data)
  validate_unique_mapping(lookup_data, "site_id", "Annual Return lookup")
  pieces <- lapply(years, function(year) {
    source_column <- paste0("site_id_", year)
    if (!source_column %in% names(lookup_data)) {
      stop("Annual Return lookup is missing ", source_column, ".", call. = FALSE)
    }
    values <- lookup_data[[source_column]]
    valid <- is.na(values) | (is.finite(values) & values == floor(values))
    if (!all(valid)) stop(source_column, " must be integer-like.", call. = FALSE)
    tibble::tibble(
      year = as.integer(year),
      annual_site_id = as.integer(values),
      site_id_canonical = as.integer(lookup_data$site_id)
    ) |>
      dplyr::filter(!is.na(.data$annual_site_id))
  })
  output <- dplyr::bind_rows(pieces)
  if (anyDuplicated(output[c("year", "annual_site_id")])) {
    stop("Annual Return year-site mappings must be unique.", call. = FALSE)
  }
  output
}

build_designation_histories <- function(
    annual_data, lookup_data, membership_data, site_ids, years = YEARS) {
  years <- as.integer(years)
  if (!identical(sort(unique(years)), years) || anyNA(years)) {
    stop("years must be sorted, unique, and non-missing.", call. = FALSE)
  }
  annual_data <- tibble::as_tibble(annual_data) |>
    dplyr::filter(.data$year %in% years)
  required_annual <- c(
    "year", "bathing_water", "shellfish_water", paste0("site_id_", years)
  )
  missing_annual <- setdiff(required_annual, names(annual_data))
  if (length(missing_annual) > 0L) {
    stop(
      "Annual Return data are missing: ", paste(missing_annual, collapse = ", "),
      call. = FALSE
    )
  }
  membership_data <- tibble::as_tibble(membership_data) |>
    dplyr::select("site_id_canonical", "site_id")
  validate_unique_mapping(
    membership_data, "site_id_canonical", "Canonical Site Group membership"
  )
  lookup_long <- build_lookup_long(lookup_data, years)

  annual_site_id <- vapply(seq_len(nrow(annual_data)), function(index) {
    value <- annual_data[[paste0("site_id_", annual_data$year[[index]])]][[index]]
    if (is.na(value)) NA_integer_ else as.integer(value)
  }, integer(1))
  if (anyNA(annual_site_id)) {
    stop("Active Annual Return rows must have their year-specific site ID.", call. = FALSE)
  }
  evidence <- annual_data |>
    dplyr::transmute(
      year = as.integer(.data$year),
      annual_site_id = annual_site_id,
      bath_status = classify_designation_value(.data$bathing_water, TRUE),
      bath_mixed = FALSE,
      bath_unknown = .data$bath_status == "unknown",
      shell_status = classify_designation_value(.data$shellfish_water, TRUE),
      shell_mixed = FALSE,
      shell_unknown = .data$shell_status == "unknown"
    ) |>
    dplyr::left_join(lookup_long, by = c("year", "annual_site_id"))
  if (anyNA(evidence$site_id_canonical)) {
    misses <- unique(evidence$annual_site_id[is.na(evidence$site_id_canonical)])
    stop(
      "Unmapped Annual Return Site IDs: ",
      paste(utils::head(misses, 10L), collapse = ", "),
      call. = FALSE
    )
  }

  annual_site <- reduce_designation_rows(evidence, c("annual_site_id", "year")) |>
    dplyr::left_join(
      dplyr::distinct(evidence, .data$annual_site_id, .data$year,
        .data$site_id_canonical),
      by = c("annual_site_id", "year")
    )
  canonical <- reduce_designation_rows(
    annual_site, c("site_id_canonical", "year")
  ) |>
    dplyr::left_join(membership_data, by = "site_id_canonical")
  if (anyNA(canonical$site_id)) {
    misses <- unique(canonical$site_id_canonical[is.na(canonical$site_id)])
    stop(
      "Unmapped Canonical Spill Site IDs: ",
      paste(utils::head(misses, 10L), collapse = ", "),
      call. = FALSE
    )
  }
  group_year <- reduce_designation_rows(canonical, c("site_id", "year"))
  group_year <- tidyr::expand_grid(
    site_id = sort(unique(as.integer(site_ids))), year = years
  ) |>
    dplyr::left_join(group_year, by = c("site_id", "year")) |>
    dplyr::mutate(
      dplyr::across(dplyr::ends_with("_status"),
        ~ tidyr::replace_na(.x, "unknown")),
      dplyr::across(dplyr::ends_with("_mixed"),
        ~ tidyr::replace_na(.x, FALSE)),
      dplyr::across(dplyr::ends_with("_unknown"),
        ~ tidyr::replace_na(.x, TRUE))
    )

  output <- tibble::tibble(site_id = sort(unique(as.integer(site_ids))))
  for (type in c("bath", "shell")) {
    for (field in c("status", "mixed", "unknown")) {
      for (year in years) {
        column <- paste0(type, "_", field)
        value_column <- paste0(type, "_", field, "_", year %% 100L)
        piece <- group_year |>
          dplyr::filter(.data$year == .env$year) |>
          dplyr::select("site_id", dplyr::all_of(column))
        names(piece)[[2L]] <- value_column
        output <- dplyr::left_join(output, piece, by = "site_id")
      }
    }
  }
  for (type in c("bath", "shell")) {
    status_columns <- paste0(type, "_status_", years %% 100L)
    unknown_columns <- paste0(type, "_unknown_", years %% 100L)
    status_matrix <- as.matrix(output[status_columns])
    unknown_matrix <- as.matrix(output[unknown_columns])
    output[[paste0(type, "_ever_2124")]] <- rowSums(status_matrix == "designated") > 0L
    output[[paste0(type, "_changed_2124")]] <-
      rowSums(status_matrix == "designated") > 0L &
      rowSums(status_matrix == "not_designated") > 0L
    output[[paste0(type, "_unknown_2124")]] <-
      rowSums(status_matrix == "unknown") > 0L | rowSums(unknown_matrix) > 0L
    status_24 <- output[[paste0(type, "_status_24")]]
    output[[paste0(type, "_24")]] <- dplyr::case_when(
      status_24 == "designated" ~ TRUE,
      status_24 == "not_designated" ~ FALSE,
      TRUE ~ NA
    )
  }
  output
}

geometry_has_interior_rings <- function(geometry) {
  any(vapply(sf::st_geometry(geometry), function(feature) {
    if (inherits(feature, "POLYGON")) return(length(feature) > 1L)
    if (inherits(feature, "MULTIPOLYGON")) {
      return(any(vapply(feature, length, integer(1)) > 1L))
    }
    TRUE
  }, logical(1)))
}

calculate_site_coast_distance <- function(projection, country_boundaries) {
  projection <- tibble::as_tibble(projection)
  required <- c("site_id", "easting", "northing")
  if (!all(required %in% names(projection))) {
    stop("Site Group projection must contain site_id, easting, and northing.", call. = FALSE)
  }
  validate_unique_mapping(projection, "site_id", "Site Group projection")
  if (!inherits(country_boundaries, "sf")) {
    stop("country_boundaries must be an sf object.", call. = FALSE)
  }
  name_column <- intersect(c("CTRY24NM", "country", "name"), names(country_boundaries))
  if (length(name_column) == 0L) {
    stop("Country boundary data must contain a country-name field.", call. = FALSE)
  }
  gb <- country_boundaries |>
    dplyr::filter(.data[[name_column[[1L]]]] %in% c("England", "Wales", "Scotland")) |>
    sf::st_transform(27700)
  if (nrow(gb) != 3L && !identical(name_column[[1L]], "country")) {
    stop("ONS boundary must contain England, Wales, and Scotland exactly once.", call. = FALSE)
  }
  dissolved <- sf::st_sf(geometry = sf::st_union(sf::st_geometry(gb)))
  if (geometry_has_interior_rings(dissolved)) {
    stop("Dissolved Great Britain geometry contains interior rings.", call. = FALSE)
  }
  coast <- sf::st_boundary(dissolved)
  output <- tibble::tibble(
    site_id = as.integer(projection$site_id),
    distance_to_coast_m = NA_real_
  )
  valid <- is.finite(projection$easting) & is.finite(projection$northing)
  if (any(valid)) {
    points <- sf::st_as_sf(
      projection[valid, ], coords = c("easting", "northing"), crs = 27700,
      remove = FALSE
    )
    output$distance_to_coast_m[valid] <- as.numeric(sf::st_distance(points, coast))
  }
  output
}

validate_site_group_characteristics <- function(data, expected_site_ids, refined = FALSE) {
  data <- tibble::as_tibble(data)
  expected_columns <- site_group_characteristics_columns(refined)
  if (!identical(names(data), expected_columns)) {
    stop("Site Group characteristics must have the exact hand-written schema.", call. = FALSE)
  }
  validate_unique_mapping(data, "site_id", "Site Group characteristics")
  if (!identical(sort(data$site_id), sort(as.integer(expected_site_ids)))) {
    stop("Site Group characteristics row universe does not match the projection.", call. = FALSE)
  }
  if (any(!is.na(data$distance_to_coast_m) &
      (!is.finite(data$distance_to_coast_m) | data$distance_to_coast_m < 0))) {
    stop("distance_to_coast_m must contain non-negative finite values or NA.", call. = FALSE)
  }
  status_columns <- grep("_(status)_[0-9]{2}$", names(data), value = TRUE)
  if (anyNA(data[status_columns]) ||
      any(!unlist(data[status_columns], use.names = FALSE) %in%
        c("designated", "not_designated", "unknown"))) {
    stop("Designation status columns contain values outside the allowed enum.", call. = FALSE)
  }
  logical_columns <- setdiff(
    names(data)[vapply(data, is.logical, logical(1))], c("bath_24", "shell_24")
  )
  if (anyNA(data[logical_columns])) {
    stop("Non-current designation flags must not be missing.", call. = FALSE)
  }
  if (refined) validate_open_coast_sites(data)
  invisible(data)
}

add_open_coast_characteristics <- function(data, projection, reference) {
  validate_published_open_coast_reference(reference)
  validate_unique_mapping(projection, "site_id", "Representative locations")
  if (!setequal(data$site_id, projection$site_id)) stop("Representative location keys differ.")
  if (!identical(reference$review$location_hash, open_coast_location_hash(projection)))
    stop("Representative locations differ from the geography coverage review.")
  usable <- is.finite(projection$easting) & is.finite(projection$northing)
  evidence <- tibble::tibble(site_id = projection$site_id,
    distance_to_open_coast_m = NA_real_, open_coast_status = "missing_location")
  if (any(usable)) {
    points <- sf::st_as_sf(projection[usable, ], coords = c("easting", "northing"), crs = 27700)
    measured <- measure_open_coast_evidence(points, reference, reference$coverage)
    evidence$distance_to_open_coast_m[usable] <- measured$distance_to_open_coast_m
    evidence$open_coast_status[usable] <- ifelse(measured$open_coast_status == "supported_candidate",
      "validated", measured$open_coast_status)
  }
  output <- dplyr::left_join(data, evidence, by = "site_id", relationship = "one-to-one")
  output$geometry_generation <- reference$generation
  # Include representative coordinates in the generation without changing the
  # legacy site schema or selecting a different representative location.
  output$site_generation <- open_coast_site_generation(output, open_coast_location_hash(projection))
  validate_site_group_characteristics(output, projection$site_id, refined = TRUE)
  output
}

read_site_characteristic_inputs <- function(config = CONFIG) {
  required_paths <- unlist(config[c(
    "annual_path", "lookup_path", "membership_path", "crosswalk_path",
    "boundary_path"
  )], use.names = FALSE)
  missing_paths <- required_paths[!file.exists(required_paths)]
  if (length(missing_paths) > 0L) {
    stop("Missing required input(s): ", paste(missing_paths, collapse = ", "), call. = FALSE)
  }
  list(
    annual = arrow::read_parquet(config$annual_path),
    lookup = arrow::read_parquet(config$lookup_path),
    membership = arrow::read_parquet(config$membership_path),
    projection = read_site_group_projection(config$crosswalk_path, years = YEARS),
    boundaries = sf::st_read(config$boundary_path, quiet = TRUE)
  )
}

build_site_group_characteristics <- function(inputs) {
  histories <- build_designation_histories(
    inputs$annual, inputs$lookup, inputs$membership,
    inputs$projection$site_id, YEARS
  )
  coast <- calculate_site_coast_distance(inputs$projection, inputs$boundaries)
  output <- inputs$projection |>
    dplyr::select("site_id") |>
    dplyr::left_join(coast, by = "site_id") |>
    dplyr::left_join(histories, by = "site_id") |>
    dplyr::select(dplyr::all_of(site_group_characteristics_columns()))
  validate_site_group_characteristics(output, inputs$projection$site_id)
  output
}

log_site_diagnostics <- function(data) {
  logger::log_info("Site Groups: {nrow(data)}")
  logger::log_info("Missing coast distance: {sum(is.na(data$distance_to_coast_m))}")
  quantiles <- stats::quantile(
    data$distance_to_coast_m, c(0, .25, .5, .75, 1), na.rm = TRUE
  )
  logger::log_info("Site Coast Distance quantiles (m): {paste(round(quantiles, 1), collapse = ', ')}")
  for (type in c("bath", "shell")) {
    for (year in 21:24) {
      counts <- table(data[[paste0(type, "_status_", year)]], useNA = "ifany")
      logger::log_info("{type} status {year}: {paste(names(counts), counts, sep = '=', collapse = ', ')}")
      logger::log_info(
        "{type} diagnostics {year}: mixed={sum(data[[paste0(type, '_mixed_', year)]])}, unknown_evidence={sum(data[[paste0(type, '_unknown_', year)]])}"
      )
    }
    logger::log_info(
      "{type} summary ever={sum(data[[paste0(type, '_ever_2124')]])}, changed={sum(data[[paste0(type, '_changed_2124')]])}, unknown={sum(data[[paste0(type, '_unknown_2124')]])}"
    )
  }
  logger::log_info("Annual Return and Canonical membership mapping misses: 0")
}

main <- function() {
  setup_logging(LOG_FILE, console = interactive(), threshold = "INFO")
  logger::log_info("Building Site Group characteristics")
  inputs <- read_site_characteristic_inputs()
  output <- build_site_group_characteristics(inputs)
  if (file.exists(CONFIG$output_path)) {
    prior <- arrow::read_parquet(CONFIG$output_path)
    if (nrow(prior) != nrow(output) || anyDuplicated(prior$site_id) ||
        !setequal(prior$site_id, output$site_id)) stop("Legacy Site Group identities changed.")
    legacy_columns <- site_group_characteristics_columns()
    prior <- prior[match(output$site_id, prior$site_id), legacy_columns]
    if (!isTRUE(all.equal(as.data.frame(output), as.data.frame(prior), tolerance = 0, check.attributes = FALSE)))
      stop("Legacy Site Group fields changed; refusing additive publication.")
  }
  if (!file.exists(CONFIG$open_coast_path)) stop("Validated open-coast reference is required before publication.")
  output <- add_open_coast_characteristics(output, inputs$projection, readRDS(CONFIG$open_coast_path))
  log_site_diagnostics(output)

  dir.create(dirname(CONFIG$output_path), recursive = TRUE, showWarnings = FALSE)
  candidate <- file.path(
    dirname(CONFIG$output_path), paste0(".", basename(CONFIG$output_path), ".candidate")
  )
  if (file.exists(candidate)) unlink(candidate)
  table <- arrow::Table$create(output, schema = site_group_characteristics_schema(refined = TRUE))
  arrow::write_parquet(table, candidate)
  validate_file <- function(path) {
    observed_signature <- arrow_schema_signature(
      arrow::open_dataset(path)$schema
    )
    expected_signature <- arrow_schema_signature(
      site_group_characteristics_schema(refined = TRUE)
    )
    if (!identical(observed_signature, expected_signature)) {
      stop("Site Group parquet physical types do not match the hand-written schema.",
        call. = FALSE)
    }
    candidate_data <- arrow::read_parquet(path)
    validate_site_group_characteristics(candidate_data, inputs$projection$site_id, refined = TRUE)
    if (!identical(single_open_coast_generation(candidate_data),
        open_coast_site_generation(candidate_data, open_coast_location_hash(inputs$projection))))
      stop("Site artifact values do not match their generation.")
  }
  publish_validated_file(candidate, CONFIG$output_path, validate_file)
  logger::log_info("Published {CONFIG$output_path}")
}

if (sys.nframe() == 0L) main()
