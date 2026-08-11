# ==============================================================================
# Rental to Site Group Lookup Producer
# ==============================================================================
#
# Purpose: Match geocoded rental listings to Site Groups within the configured
#          radius and write the canonical single-file Parquet lookup.
#
# Author: Jacopo Olivieri
# Date Modified: 2026-08-11
#
# Inputs:
#   - data/processed/zoopla/zoopla_rentals.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/zoopla/spill_rental_lookup.parquet
#   - output/log/site_rental_match.log
#   - output/log/site_rental_match_dropped_spill_sites.csv
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first with `rv sync`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "arrow",
  "dplyr",
  "glue",
  "logger",
  "sf",
  "tibble",
  "tidyr"
)

LOG_FILE <- here::here("output", "log", "site_rental_match.log")

check_required_packages(REQUIRED_PACKAGES)

CONFIG <- list(
  input_path = here::here(
    "data", "processed", "zoopla", "zoopla_rentals.parquet"
  ),
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  site_group_years = 2021:2024,
  output_path = here::here(
    "data", "processed", "zoopla", "spill_rental_lookup.parquet"
  ),
  dropped_site_path = here::here(
    "output", "log", "site_rental_match_dropped_spill_sites.csv"
  ),
  radius_km = 10,
  chunk_size = 2000L
)

initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(package) {
    library(package, character.only = TRUE)
  }))
  source(
    here::here("scripts", "R", "utils", "site_group_utils.R"),
    local = TRUE
  )
}

initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

load_data <- function() {
  logger::log_info("Loading rentals and Site Group projection")
  rental_data <- arrow::read_parquet(
    CONFIG$input_path,
    col_select = c("rental_id", "easting", "northing")
  ) |>
    tibble::as_tibble()
  spill_data <- read_site_group_projection(
    CONFIG$site_group_crosswalk_path,
    years = CONFIG$site_group_years
  )
  list(rentals = rental_data, spill = spill_data)
}

prepare_spill_sites <- function(spill_data) {
  logger::log_info("Preparing Site Group spatial data")
  assert_unique_site_groups(spill_data, "Rental-match Site Group projection")

  coordinate_eligible <- !is.na(spill_data$easting) &
    !is.na(spill_data$northing) &
    is.finite(spill_data$easting) &
    is.finite(spill_data$northing)
  dropped_sites <- spill_data |>
    dplyr::filter(!coordinate_eligible) |>
    dplyr::select(dplyr::any_of(c(
      "site_id", "water_company", "ngr", "easting", "northing"
    ))) |>
    dplyr::mutate(
      missing_easting = is.na(.data$easting),
      missing_northing = is.na(.data$northing)
    )

  if (nrow(dropped_sites) > 0L) {
    dir.create(dirname(CONFIG$dropped_site_path), recursive = TRUE, showWarnings = FALSE)
    utils::write.csv(dropped_sites, CONFIG$dropped_site_path, row.names = FALSE, na = "")
    logger::log_warn(
      "Dropping {nrow(dropped_sites)} Site Groups with invalid coordinates: ",
      "{paste(dropped_sites$site_id, collapse = ', ')}"
    )
  }

  spill_sites_sf <- spill_data |>
    dplyr::filter(coordinate_eligible) |>
    sf::st_as_sf(coords = c("easting", "northing"), crs = 27700) |>
    dplyr::rename(spill_geom = "geometry") |>
    dplyr::select("site_id", "spill_geom")

  spill_lookup <- spill_sites_sf |>
    sf::st_set_geometry(NULL) |>
    dplyr::mutate(spill_geom = spill_sites_sf$spill_geom) |>
    dplyr::select("site_id", "spill_geom")

  list(spill_sf = spill_sites_sf, lookup = spill_lookup)
}

prepare_rental_data <- function(rental_data) {
  required_columns <- c("rental_id", "easting", "northing")
  missing_columns <- setdiff(required_columns, names(rental_data))
  if (length(missing_columns) > 0L) {
    stop(
      "Rental input is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      ".",
      call. = FALSE
    )
  }
  if (anyNA(rental_data$rental_id)) {
    stop("rental_id must not contain missing values.", call. = FALSE)
  }
  if (anyDuplicated(rental_data$rental_id)) {
    stop("Rental input must be unique on rental_id.", call. = FALSE)
  }
  if (!is.numeric(rental_data$easting) || !is.numeric(rental_data$northing)) {
    stop("Rental easting and northing must be numeric.", call. = FALSE)
  }

  total_rows <- nrow(rental_data)
  coordinate_eligible <- !is.na(rental_data$easting) &
    !is.na(rental_data$northing) &
    is.finite(rental_data$easting) &
    is.finite(rental_data$northing)
  eligible_rows <- sum(coordinate_eligible)
  excluded_rows <- total_rows - eligible_rows
  excluded_percentage <- if (total_rows == 0L) 0 else 100 * excluded_rows / total_rows
  coverage_message <- glue::glue(
    "Rental coordinate coverage: total={total_rows}, eligible={eligible_rows}, ",
    "excluded={excluded_rows} ({sprintf('%.2f', excluded_percentage)}%)."
  )
  logger::log_info(coverage_message)
  if (excluded_rows > 0L) {
    logger::log_warn(coverage_message)
    warning(coverage_message, call. = FALSE)
  }
  if (eligible_rows == 0L) {
    stop("No coordinate-eligible rental rows remain after validation.", call. = FALSE)
  }

  rental_data |>
    dplyr::filter(coordinate_eligible) |>
    sf::st_as_sf(coords = c("easting", "northing"), crs = 27700) |>
    dplyr::select("rental_id", "geometry")
}

match_rental_chunk <- function(rentals_sf, spill_sites_sf, spill_lookup, radius_km) {
  spatial_matches <- sf::st_join(
    rentals_sf,
    spill_sites_sf,
    join = sf::st_is_within_distance,
    dist = radius_km * 1000,
    left = TRUE
  )
  chunk_result <- left_join_site_group_projection(
    spatial_matches,
    spill_lookup,
    context = "Rental-match Site Group geometry join"
  ) |>
    dplyr::mutate(
      distance_m = dplyr::if_else(
        is.na(.data$spill_geom),
        NA_real_,
        as.numeric(sf::st_distance(.data$geometry, .data$spill_geom, by_element = TRUE))
      ),
      distance_km = .data$distance_m / 1000
    ) |>
    dplyr::group_by(.data$rental_id) |>
    dplyr::mutate(n_site_groups = sum(!is.na(.data$site_id))) |>
    dplyr::ungroup() |>
    sf::st_drop_geometry() |>
    dplyr::select(
      "rental_id", "site_id", "distance_m", "distance_km", "n_site_groups"
    )

  if (anyDuplicated(chunk_result[c("rental_id", "site_id")])) {
    stop("Rental Site Group lookup must be unique on rental_id, site_id.", call. = FALSE)
  }
  chunk_result
}

rental_lookup_schema <- function() {
  arrow::schema(
    rental_id = arrow::int32(),
    site_id = arrow::int32(),
    distance_m = arrow::float64(),
    distance_km = arrow::float64(),
    n_site_groups = arrow::int32()
  )
}

normalise_rental_lookup <- function(data) {
  tibble::tibble(
    rental_id = as.integer(data$rental_id),
    site_id = as.integer(data$site_id),
    distance_m = as.double(data$distance_m),
    distance_km = as.double(data$distance_km),
    n_site_groups = as.integer(data$n_site_groups)
  )
}

create_stage_path <- function(output_path) {
  tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = dirname(output_path),
    fileext = ".parquet"
  )
}

sort_rental_lookup <- function(data) {
  data <- as.data.frame(data)
  data <- data[order(data$rental_id, data$site_id, na.last = TRUE), , drop = FALSE]
  row.names(data) <- NULL
  data
}

validate_rental_stage <- function(stage_path, expected_schema,
                                  expected_input_rows, expected_output_rows,
                                  expected_row_groups, radius_km,
                                  sample_expected) {
  reader <- arrow::ParquetFileReader$create(stage_path)
  if (reader$GetSchema()$ToString() != expected_schema$ToString()) {
    stop("Staged rental lookup schema does not match the explicit contract.", call. = FALSE)
  }
  if (reader$num_rows != expected_output_rows) {
    stop(
      "Staged rental lookup row count changed during publication.",
      call. = FALSE
    )
  }
  if (reader$num_row_groups != expected_row_groups) {
    stop("Staged rental lookup row-group count is incomplete.", call. = FALSE)
  }

  sample_ids <- unique(sample_expected$rental_id)
  sample_parts <- vector("list", reader$num_row_groups)
  covered_properties <- 0L
  for (row_group_index in seq_len(reader$num_row_groups)) {
    row_group <- reader$ReadRowGroup(row_group_index - 1L)$to_data_frame()
    if (nrow(row_group) == 0L) {
      stop("Staged rental lookup contains an empty row group.", call. = FALSE)
    }
    if (anyDuplicated(row_group[c("rental_id", "site_id")])) {
      stop("Staged rental lookup contains duplicate rental-site keys.", call. = FALSE)
    }
    if (any(
      !is.na(row_group$distance_m) &
        (row_group$distance_m < 0 | row_group$distance_m > radius_km * 1000 + 1e-6)
    )) {
      stop("Staged rental lookup contains a distance outside the configured radius.", call. = FALSE)
    }
    group_counts <- row_group |>
      dplyr::summarise(
        expected_count = sum(!is.na(.data$site_id)),
        observed_count = dplyr::first(.data$n_site_groups),
        .by = "rental_id"
      )
    if (any(group_counts$expected_count != group_counts$observed_count)) {
      stop("Staged rental lookup contains inconsistent Site Group counts.", call. = FALSE)
    }
    covered_properties <- covered_properties + dplyr::n_distinct(row_group$rental_id)
    sample_parts[[row_group_index]] <- row_group[
      row_group$rental_id %in% sample_ids,
      ,
      drop = FALSE
    ]
  }
  if (covered_properties != expected_input_rows) {
    stop("Staged rental lookup does not cover every eligible rental.", call. = FALSE)
  }

  sample_actual <- do.call(rbind, sample_parts)
  if (!identical(sort_rental_lookup(sample_actual), sort_rental_lookup(sample_expected))) {
    stop("Staged rental lookup disagrees with the direct sample recomputation.", call. = FALSE)
  }
  invisible(TRUE)
}

write_rental_lookup <- function(rentals_sf, spill_sites_sf, spill_lookup,
                                output_path, radius_km, chunk_size,
                                fail_at = NULL) {
  if (!is.numeric(radius_km) || length(radius_km) != 1L ||
      is.na(radius_km) || radius_km <= 0) {
    stop("radius_km must be one positive number.", call. = FALSE)
  }
  if (!is.numeric(chunk_size) || length(chunk_size) != 1L ||
      is.na(chunk_size) || chunk_size < 1 || chunk_size %% 1 != 0) {
    stop("chunk_size must be one positive integer.", call. = FALSE)
  }

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  stage_path <- create_stage_path(output_path)
  writer <- NULL
  output_stream <- NULL
  writer_open <- FALSE
  stream_open <- FALSE
  on.exit(
    {
      if (writer_open) try(writer$Close(), silent = TRUE)
      if (stream_open) try(output_stream$close(), silent = TRUE)
      if (file.exists(stage_path)) unlink(stage_path)
    },
    add = TRUE
  )

  schema <- rental_lookup_schema()
  output_stream <- arrow::FileOutputStream$create(stage_path)
  stream_open <- TRUE
  writer_properties <- arrow::ParquetWriterProperties$create(names(schema))
  writer <- arrow::ParquetFileWriter$create(
    schema,
    output_stream,
    properties = writer_properties
  )
  writer_open <- TRUE

  starts <- seq.int(1L, nrow(rentals_sf), by = as.integer(chunk_size))
  output_rows <- 0L
  for (row_group_index in seq_along(starts)) {
    start <- starts[[row_group_index]]
    end <- min(start + chunk_size - 1L, nrow(rentals_sf))
    logger::log_info("Writing rental rows {start}:{end} of {nrow(rentals_sf)}")
    chunk <- match_rental_chunk(
      rentals_sf[start:end, ],
      spill_sites_sf,
      spill_lookup,
      radius_km = radius_km
    ) |>
      normalise_rental_lookup()
    table <- arrow::Table$create(chunk, schema = schema)
    writer$WriteTable(table, chunk_size = table$num_rows)
    output_rows <- output_rows + nrow(chunk)
    rm(chunk, table)

    if (identical(fail_at, "after_first_row_group") && row_group_index == 1L) {
      stop("Injected failure after the first rental row group.", call. = FALSE)
    }
  }

  writer$Close()
  writer_open <- FALSE
  if (identical(fail_at, "close")) {
    stop("Injected rental writer close failure.", call. = FALSE)
  }
  output_stream$close()
  stream_open <- FALSE

  if (identical(fail_at, "validation")) {
    stop("Injected rental staged validation failure.", call. = FALSE)
  }
  sample_indices <- unique(as.integer(round(seq(
    1,
    nrow(rentals_sf),
    length.out = min(10L, nrow(rentals_sf))
  ))))
  sample_expected <- match_rental_chunk(
    rentals_sf[sample_indices, ],
    spill_sites_sf,
    spill_lookup,
    radius_km = radius_km
  ) |>
    normalise_rental_lookup()
  if (identical(fail_at, "sample_oracle")) {
    sample_expected$n_site_groups[[1]] <- sample_expected$n_site_groups[[1]] + 1L
  }
  validate_rental_stage(
    stage_path,
    expected_schema = schema,
    expected_input_rows = nrow(rentals_sf),
    expected_output_rows = output_rows,
    expected_row_groups = length(starts),
    radius_km = radius_km,
    sample_expected = sample_expected
  )

  if (identical(fail_at, "promotion")) {
    stop("Injected rental promotion failure.", call. = FALSE)
  }
  if (!file.rename(stage_path, output_path)) {
    stop("Failed to promote the staged rental lookup.", call. = FALSE)
  }
  logger::log_info(
    "Published {output_rows} rental lookup rows across {length(starts)} row groups to {output_path}"
  )
  list(
    input_rows = nrow(rentals_sf),
    output_rows = output_rows,
    row_groups = length(starts),
    output_path = output_path
  )
}

process_spatial_data <- function(data, output_path, radius_km, chunk_size,
                                 fail_at = NULL) {
  rentals_sf <- prepare_rental_data(data$rentals)
  spill_data <- prepare_spill_sites(data$spill)
  write_rental_lookup(
    rentals_sf,
    spill_data$spill_sf,
    spill_data$lookup,
    output_path = output_path,
    radius_km = radius_km,
    chunk_size = chunk_size,
    fail_at = fail_at
  )
}

main <- function() {
  initialise_environment()
  initialise_logging()
  process_spatial_data(
    load_data(),
    output_path = CONFIG$output_path,
    radius_km = CONFIG$radius_km,
    chunk_size = CONFIG$chunk_size
  )
  invisible(NULL)
}

if (sys.nframe() == 0) {
  main()
}
