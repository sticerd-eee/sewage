# ==============================================================================
# Long-Run Coordinate to Nearest Site Group Lookup Producer
# ==============================================================================
#
# Purpose: Match every distinct property coordinate in the long-run sales and
#          rental histories to its nearest usable Site Group and write one
#          lookup keyed on the coordinate pair.
#
# Author: Jacopo Olivieri
# Date: 2026-09-24
# Date Modified: 2026-09-25
#
# Inputs:
#   - data/processed/house_price_long_run.parquet
#   - data/processed/zoopla/zoopla_rentals_long_run.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/site_nearest_long_run.parquet
#   - output/log/site_nearest_long_run_match.log
#
# Notes:
#   - The matching reproduces project_sites() and nearest_site_distance() in
#     docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd.
#   - Manual prerequisite of
#     scripts/R/09_analysis/05_news/did_attention_repeat_extensive.R: stage 04
#     has no runner, so run this script by hand before that analysis script.
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
  "here",
  "logger",
  "sf"
)

LOG_FILE <- here::here("output", "log", "site_nearest_long_run_match.log")

check_required_packages(REQUIRED_PACKAGES)

source(here::here("scripts", "R", "utils", "site_group_utils.R"), local = TRUE)
source(
  here::here("scripts", "R", "utils", "dataset_publication_utils.R"),
  local = TRUE
)

# Configuration
############################################################

CONFIG <- list(
  # Input file paths
  history_paths = c(
    sales = here::here("data", "processed", "house_price_long_run.parquet"),
    rentals = here::here(
      "data", "processed", "zoopla", "zoopla_rentals_long_run.parquet"
    )
  ),
  site_group_crosswalk_path = here::here(
    "data", "processed", "matched_events_annual_data",
    "site_group_crosswalk.parquet"
  ),
  # Site Group projection window
  site_group_years = 2021:2024,
  # Output file path
  output_path = here::here(
    "data", "processed", "site_nearest_long_run.parquet"
  )
)

# Functions
############################################################

# Attach dplyr so its verbs resolve inside Arrow queries; other calls are qualified.
initialise_environment <- function() {
  invisible(lapply("dplyr", function(pkg) {
    library(pkg, character.only = TRUE)
  }))
}

# Initialise logging for this script.
initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# Read the distinct finite coordinate pairs of one history, computed inside
# Arrow, and log how many history rows lack coordinates.
read_history_coordinates <- function(path, label) {
  if (!file.exists(path)) {
    stop(label, " history is missing: ", path, call. = FALSE)
  }
  dataset <- arrow::open_dataset(path)
  missing_columns <- setdiff(c("easting", "northing"), names(dataset$schema))
  if (length(missing_columns) > 0L) {
    stop(
      label, " history is missing column(s): ",
      paste(missing_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }

  total_rows <- dataset$num_rows
  missing_rows <- dataset |>
    dplyr::filter(is.na(.data$easting) | is.na(.data$northing)) |>
    dplyr::count() |>
    dplyr::collect() |>
    dplyr::pull("n")
  missing_percentage <- if (total_rows == 0) 0 else 100 * missing_rows / total_rows
  logger::log_info(
    "{label} history: total rows={total_rows}, rows without finite ",
    "coordinates={missing_rows} ({sprintf('%.2f', missing_percentage)}%)."
  )

  coordinates <- dataset |>
    dplyr::select("easting", "northing") |>
    dplyr::filter(!is.na(.data$easting), !is.na(.data$northing)) |>
    dplyr::distinct() |>
    dplyr::collect() |>
    as.data.frame() |>
    dplyr::mutate(
      easting = as.integer(.data$easting),
      northing = as.integer(.data$northing)
    )
  logger::log_info(
    "{label} history: {nrow(coordinates)} distinct finite coordinate pairs."
  )
  coordinates
}

# Union the distinct coordinate pairs of both histories.
load_coordinates <- function(history_paths) {
  coordinates <- lapply(names(history_paths), function(label) {
    read_history_coordinates(history_paths[[label]], label)
  }) |>
    dplyr::bind_rows() |>
    dplyr::distinct() |>
    dplyr::arrange(.data$easting, .data$northing)
  if (nrow(coordinates) == 0L) {
    stop("No finite coordinate pairs in either history.", call. = FALSE)
  }
  logger::log_info(
    "Union of both histories: {nrow(coordinates)} distinct coordinate pairs."
  )
  coordinates
}

# Project Site Groups to their latest valid location and keep the usable ones,
# with the report's usability rule. Unusable sites are named in the log.
prepare_sites <- function(crosswalk_path, years) {
  logger::log_info("Projecting Site Groups over {min(years)}-{max(years)}")
  projection <- read_site_group_projection(crosswalk_path, years = years)
  assert_unique_site_groups(projection, "Long-run nearest-site projection")
  projection <- projection |>
    dplyr::mutate(
      site_id = as.integer(.data$site_id),
      easting = as.numeric(.data$easting),
      northing = as.numeric(.data$northing),
      usable = is.finite(.data$easting) & is.finite(.data$northing)
    )
  n_usable <- sum(projection$usable)
  if (n_usable == 0L) {
    stop("No usable Site Group locations; nearest-site matching cannot run.", call. = FALSE)
  }

  unusable <- projection |>
    dplyr::filter(!.data$usable)
  logger::log_info(
    "Site Groups: total={nrow(projection)}, usable={n_usable}, ",
    "unusable={nrow(unusable)}."
  )
  if (nrow(unusable) > 0L) {
    logger::log_warn(
      "Excluding {nrow(unusable)} Site Groups without a valid location: ",
      "{paste(unusable$site_id, collapse = ', ')}"
    )
  }

  as.data.frame(projection[projection$usable, c("site_id", "easting", "northing")])
}

# Match each unique coordinate to its nearest usable Site Group, as the
# report's nearest_site_distance() does, in EPSG:27700.
match_nearest_sites <- function(coords, sites) {
  sites_sf <- sf::st_as_sf(sites, coords = c("easting", "northing"), crs = 27700)
  pts_sf <- sf::st_as_sf(
    data.frame(
      easting = as.numeric(coords$easting),
      northing = as.numeric(coords$northing)
    ),
    coords = c("easting", "northing"),
    crs = 27700
  )
  idx <- sf::st_nearest_feature(pts_sf, sites_sf)
  dist <- sf::st_distance(pts_sf, sites_sf[idx, ], by_element = TRUE)
  result <- data.frame(
    easting = coords$easting,
    northing = coords$northing,
    site_id = sites$site_id[idx],
    distance_m = as.numeric(dist)
  )
  if (anyNA(result$site_id) || any(!is.finite(result$distance_m))) {
    stop("Nearest-site matching left unmatched coordinates.", call. = FALSE)
  }
  result
}

# Explicit Arrow schema of the published lookup. Coordinates are written back
# as int32 so consumers join on the histories' native integer columns.
lookup_schema <- function() {
  arrow::schema(
    easting = arrow::int32(),
    northing = arrow::int32(),
    site_id = arrow::int32(),
    distance_m = arrow::float64()
  )
}

# Validate a staged lookup: schema, complete coordinates, uniqueness on the
# coordinate pair, and finite non-negative distances.
validate_lookup <- function(path) {
  observed_signature <- arrow_schema_signature(arrow::open_dataset(path)$schema)
  expected_signature <- arrow_schema_signature(lookup_schema())
  if (!identical(observed_signature, expected_signature)) {
    stop("Long-run lookup schema does not match the explicit contract.", call. = FALSE)
  }
  lookup <- as.data.frame(arrow::read_parquet(path))
  if (anyNA(lookup$easting) || anyNA(lookup$northing)) {
    stop("Long-run lookup has missing coordinates.", call. = FALSE)
  }
  if (anyDuplicated(lookup[c("easting", "northing")]) > 0L) {
    stop("Long-run lookup is not unique on (easting, northing).", call. = FALSE)
  }
  if (any(!is.finite(lookup$distance_m)) || any(lookup$distance_m < 0)) {
    stop("Long-run lookup has a non-finite or negative distance.", call. = FALSE)
  }
  invisible(TRUE)
}

# Stage the lookup beside the output, validate it, and publish it atomically.
write_lookup <- function(lookup, output_path) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  candidate <- file.path(
    dirname(output_path), paste0(".", basename(output_path), ".candidate")
  )
  if (file.exists(candidate)) unlink(candidate)
  arrow::write_parquet(arrow::Table$create(lookup, schema = lookup_schema()), candidate)
  publish_validated_file(candidate, output_path, validate_lookup)
  logger::log_info(
    "Published {nrow(lookup)} coordinate lookup rows to {output_path}"
  )
}

# Main Workflow
############################################################

main <- function() {
  initialise_environment()
  initialise_logging()
  started <- Sys.time()

  coordinates <- load_coordinates(CONFIG$history_paths)
  sites <- prepare_sites(
    CONFIG$site_group_crosswalk_path,
    years = CONFIG$site_group_years
  )
  lookup <- match_nearest_sites(coordinates, sites)
  logger::log_info(
    "Distance to nearest usable Site Group (m): ",
    "min={sprintf('%.1f', min(lookup$distance_m))}, ",
    "median={sprintf('%.1f', stats::median(lookup$distance_m))}, ",
    "max={sprintf('%.1f', max(lookup$distance_m))}."
  )
  write_lookup(lookup, CONFIG$output_path)

  elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))
  logger::log_info(
    "Script finished at {Sys.time()} ({sprintf('%.1f', elapsed)} s elapsed)"
  )
  invisible(NULL)
}

if (sys.nframe() == 0) {
  main()
}
