# ==============================================================================
# House-Sale to Site Group Lookup Producer
# ==============================================================================
#
# Purpose: Match geocoded house sales to Site Groups within the configured
#          radius and write the canonical single-file Parquet lookup.
#
# Author: Jacopo Olivieri
# Date Modified: 2026-08-11
#
# Inputs:
#   - data/processed/house_price.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/spill_house_lookup.parquet
#   - output/log/site_house_sale_match.log
#   - output/log/site_house_sale_match_dropped_spill_sites.csv
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

LOG_FILE <- here::here("output", "log", "site_house_sale_match.log")

check_required_packages(REQUIRED_PACKAGES)

CONFIG <- list(
  input_path = here::here("data", "processed", "house_price.parquet"),
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  site_group_years = 2021:2024,
  output_path = here::here("data", "processed", "spill_house_lookup.parquet"),
  dropped_site_path = here::here(
    "output", "log", "site_house_sale_match_dropped_spill_sites.csv"
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
  logger::log_info("Loading house sales and Site Group projection")
  house_data <- arrow::read_parquet(
    CONFIG$input_path,
    col_select = c("house_id", "easting", "northing")
  ) |>
    tibble::as_tibble()
  spill_data <- read_site_group_projection(
    CONFIG$site_group_crosswalk_path,
    years = CONFIG$site_group_years
  )
  list(house = house_data, spill = spill_data)
}

prepare_spill_sites <- function(spill_data) {
  logger::log_info("Preparing Site Group spatial data")
  assert_unique_site_groups(spill_data, "House-match Site Group projection")

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

prepare_house_data <- function(house_data) {
  required_columns <- c("house_id", "easting", "northing")
  missing_columns <- setdiff(required_columns, names(house_data))
  if (length(missing_columns) > 0L) {
    stop(
      "House input is missing required column(s): ",
      paste(missing_columns, collapse = ", "),
      ".",
      call. = FALSE
    )
  }
  if (anyNA(house_data$house_id)) {
    stop("house_id must not contain missing values.", call. = FALSE)
  }
  if (anyDuplicated(house_data$house_id)) {
    stop("House input must be unique on house_id.", call. = FALSE)
  }
  if (!is.numeric(house_data$easting) || !is.numeric(house_data$northing)) {
    stop("House easting and northing must be numeric.", call. = FALSE)
  }

  total_rows <- nrow(house_data)
  coordinate_eligible <- !is.na(house_data$easting) &
    !is.na(house_data$northing) &
    is.finite(house_data$easting) &
    is.finite(house_data$northing)
  eligible_rows <- sum(coordinate_eligible)
  excluded_rows <- total_rows - eligible_rows
  excluded_percentage <- if (total_rows == 0L) 0 else 100 * excluded_rows / total_rows
  coverage_message <- glue::glue(
    "House coordinate coverage: total={total_rows}, eligible={eligible_rows}, ",
    "excluded={excluded_rows} ({sprintf('%.2f', excluded_percentage)}%)."
  )
  logger::log_info(coverage_message)
  if (excluded_rows > 0L) {
    logger::log_warn(coverage_message)
    warning(coverage_message, call. = FALSE)
  }
  if (eligible_rows == 0L) {
    stop("No coordinate-eligible house rows remain after validation.", call. = FALSE)
  }

  house_data |>
    dplyr::filter(coordinate_eligible) |>
    sf::st_as_sf(coords = c("easting", "northing"), crs = 27700) |>
    dplyr::select("house_id", "geometry")
}

match_house_chunk <- function(houses_sf, spill_sites_sf, spill_lookup, radius_km) {
  spatial_matches <- sf::st_join(
    houses_sf,
    spill_sites_sf,
    join = sf::st_is_within_distance,
    dist = radius_km * 1000,
    left = TRUE
  )
  chunk_result <- left_join_site_group_projection(
    spatial_matches,
    spill_lookup,
    context = "House-match Site Group geometry join"
  ) |>
    dplyr::mutate(
      distance_m = dplyr::if_else(
        is.na(.data$spill_geom),
        NA_real_,
        as.numeric(sf::st_distance(.data$geometry, .data$spill_geom, by_element = TRUE))
      ),
      distance_km = .data$distance_m / 1000
    ) |>
    dplyr::group_by(.data$house_id) |>
    dplyr::mutate(n_site_groups = sum(!is.na(.data$site_id))) |>
    dplyr::ungroup() |>
    sf::st_drop_geometry() |>
    dplyr::select(
      "house_id", "site_id", "distance_m", "distance_km", "n_site_groups"
    )

  if (anyDuplicated(chunk_result[c("house_id", "site_id")])) {
    stop("House Site Group lookup must be unique on house_id, site_id.", call. = FALSE)
  }
  chunk_result
}

perform_spatial_join <- function(houses_sf, spill_sites_sf, spill_lookup,
                                 radius_km, chunk_size) {
  if (anyDuplicated(houses_sf$house_id)) {
    stop("House input must be unique on house_id.", call. = FALSE)
  }
  if (!is.numeric(radius_km) || length(radius_km) != 1L ||
      is.na(radius_km) || radius_km <= 0) {
    stop("radius_km must be one positive number.", call. = FALSE)
  }
  if (!is.numeric(chunk_size) || length(chunk_size) != 1L ||
      is.na(chunk_size) || chunk_size < 1 || chunk_size %% 1 != 0) {
    stop("chunk_size must be one positive integer.", call. = FALSE)
  }

  starts <- seq.int(1L, nrow(houses_sf), by = as.integer(chunk_size))
  chunks <- vector("list", length(starts))
  for (index in seq_along(starts)) {
    start <- starts[[index]]
    end <- min(start + chunk_size - 1L, nrow(houses_sf))
    logger::log_info(
      "Processing house rows {start}:{end} of {nrow(houses_sf)}"
    )
    chunks[[index]] <- match_house_chunk(
      houses_sf[start:end, ],
      spill_sites_sf,
      spill_lookup,
      radius_km = radius_km
    )
  }
  dplyr::bind_rows(chunks)
}

process_spatial_data <- function(data, radius_km, chunk_size) {
  houses_sf <- prepare_house_data(data$house)
  spill_data <- prepare_spill_sites(data$spill)
  perform_spatial_join(
    houses_sf,
    spill_data$spill_sf,
    spill_data$lookup,
    radius_km = radius_km,
    chunk_size = chunk_size
  )
}

export_data <- function(data, output_path = CONFIG$output_path) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(data, output_path)
  logger::log_info("Data exported successfully to {output_path}")
  invisible(output_path)
}

main <- function() {
  initialise_environment()
  initialise_logging()
  data <- load_data()
  matched_data <- process_spatial_data(
    data,
    radius_km = CONFIG$radius_km,
    chunk_size = CONFIG$chunk_size
  )
  export_data(matched_data)
  invisible(NULL)
}

if (sys.nframe() == 0) {
  main()
}
