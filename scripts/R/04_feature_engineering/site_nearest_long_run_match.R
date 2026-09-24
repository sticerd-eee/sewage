# ==============================================================================
# Long-Run Coordinate to Nearest Site Group Lookup Producer
# ==============================================================================
#
# Purpose: Match every distinct property coordinate in the long-run sales and
#          rental histories to its nearest usable Site Group and write one
#          lookup keyed on the coordinate pair.
#
# Author: Jacopo Olivieri
# Date Modified: 2026-09-24
#
# Inputs:
#   - data/processed/house_price_long_run.parquet
#   - data/processed/zoopla/zoopla_rentals_long_run.parquet
#   - data/processed/matched_events_annual_data/site_group_crosswalk.parquet
#
# Outputs:
#   - data/processed/site_nearest_long_run.parquet
#   - output/log/site_nearest_long_run_match.log
#   - output/log/site_nearest_long_run_match_dropped_sites.csv
#
# Notes:
#   - The matching reproduces project_sites() and nearest_site_distance() in
#     docs/reports/2026-09-19-001-long-run-public-attention-repeat-properties.qmd.
#   - The lookup is at unique-coordinate grain: one row per distinct
#     (easting, northing) pair across both histories. Coordinates are cast to
#     double only for sf and written back as int32 so consumers join on the
#     histories' native integer columns.
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
  "tibble"
)

LOG_FILE <- here::here("output", "log", "site_nearest_long_run_match.log")

check_required_packages(REQUIRED_PACKAGES)

CONFIG <- list(
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
  site_group_years = 2021:2024,
  output_path = here::here(
    "data", "processed", "site_nearest_long_run.parquet"
  ),
  dropped_site_path = here::here(
    "output", "log", "site_nearest_long_run_match_dropped_sites.csv"
  ),
  chunk_size = 250000L,
  n_oracle_sample = 20L
)

initialise_environment <- function() {
  invisible(lapply(REQUIRED_PACKAGES, function(package) {
    library(package, character.only = TRUE)
  }))
  sys.source(
    here::here("scripts", "R", "utils", "site_group_utils.R"),
    envir = environment(initialise_environment)
  )
}

initialise_logging <- function() {
  setup_logging(log_file = LOG_FILE, console = interactive(), threshold = "DEBUG")
  logger::log_info("Logging to {LOG_FILE}")
  logger::log_info("Script started at {Sys.time()}")
}

# ---- Coordinates --------------------------------------------------------------

#' Distinct finite coordinate pairs of one history, computed inside Arrow.
read_history_coordinates <- function(path, label) {
  if (!file.exists(path)) {
    stop(glue::glue("{label} history is missing: {path}"), call. = FALSE)
  }
  dataset <- arrow::open_dataset(path)
  schema <- dataset$schema
  missing_columns <- setdiff(c("easting", "northing"), names(schema))
  if (length(missing_columns) > 0L) {
    stop(
      glue::glue(
        "{label} history is missing column(s): ",
        "{paste(missing_columns, collapse = ', ')}."
      ),
      call. = FALSE
    )
  }
  for (column in c("easting", "northing")) {
    if (!schema[[column]]$type$Equals(arrow::int32())) {
      stop(
        glue::glue(
          "{label} history column `{column}` must be int32, ",
          "found {schema[[column]]$type$ToString()}."
        ),
        call. = FALSE
      )
    }
  }

  total_rows <- dataset$num_rows
  # int32 columns carry no NaN or Inf, so nonmissing is finite.
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
    as.data.frame()
  logger::log_info(
    "{label} history: {nrow(coordinates)} distinct finite coordinate pairs."
  )
  list(
    coordinates = coordinates,
    total_rows = total_rows,
    missing_rows = missing_rows
  )
}

load_coordinates <- function(history_paths) {
  per_history <- lapply(names(history_paths), function(label) {
    read_history_coordinates(history_paths[[label]], label)
  })
  names(per_history) <- names(history_paths)
  union <- do.call(rbind, lapply(per_history, `[[`, "coordinates"))
  union <- union[!duplicated(union), , drop = FALSE]
  union <- union[order(union$easting, union$northing), , drop = FALSE]
  row.names(union) <- NULL
  if (nrow(union) == 0L) {
    stop("No finite coordinate pairs in either history.", call. = FALSE)
  }
  logger::log_info(
    "Union of both histories: {nrow(union)} distinct coordinate pairs."
  )
  list(coordinates = union, per_history = per_history)
}

# ---- Site Groups ----------------------------------------------------------------

#' Latest-valid Site Group projection with the report's usable rule.
prepare_sites <- function(crosswalk_path, years, dropped_site_path) {
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
    stop(
      "Site projection has zero usable locations; nearest-site matching cannot run.",
      call. = FALSE
    )
  }

  unusable <- projection |>
    dplyr::filter(!.data$usable) |>
    dplyr::select(dplyr::any_of(c(
      "site_id", "water_company", "ngr", "easting", "northing"
    ))) |>
    dplyr::mutate(
      missing_easting = !is.finite(.data$easting),
      missing_northing = !is.finite(.data$northing)
    )
  dir.create(dirname(dropped_site_path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(unusable, dropped_site_path, row.names = FALSE, na = "")
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

  sites <- as.data.frame(projection[projection$usable, c("site_id", "easting", "northing")])
  sites_sf <- sf::st_as_sf(sites, coords = c("easting", "northing"), crs = 27700)
  list(
    sites = sites,
    sites_sf = sites_sf,
    n_sites = nrow(projection),
    n_usable = n_usable,
    unusable_site_ids = unusable$site_id
  )
}

# ---- Matching -------------------------------------------------------------------

#' Nearest usable Site Group for unique coordinates, as nearest_site_distance().
match_nearest_sites <- function(coords, sites) {
  pts_sf <- sf::st_as_sf(
    data.frame(
      easting = as.numeric(coords$easting),
      northing = as.numeric(coords$northing)
    ),
    coords = c("easting", "northing"),
    crs = 27700
  )
  idx <- sf::st_nearest_feature(pts_sf, sites$sites_sf)
  dist <- sf::st_distance(pts_sf, sites$sites_sf[idx, ], by_element = TRUE)
  result <- data.frame(
    easting = as.integer(coords$easting),
    northing = as.integer(coords$northing),
    site_id = as.integer(sites$sites$site_id[idx]),
    distance_m = as.numeric(dist)
  )
  if (anyNA(result$site_id) || any(!is.finite(result$distance_m))) {
    stop("Nearest-site matching left unmatched coordinates.", call. = FALSE)
  }
  result
}

#' Brute-force minimum distance over all usable sites, for the sample oracle.
brute_force_nearest <- function(coords, sites) {
  out <- lapply(seq_len(nrow(coords)), function(i) {
    d <- sqrt(
      (sites$sites$easting - as.numeric(coords$easting[[i]]))^2 +
        (sites$sites$northing - as.numeric(coords$northing[[i]]))^2
    )
    data.frame(distance_m = min(d))
  })
  do.call(rbind, out)
}

match_all_coordinates <- function(coords, sites, chunk_size) {
  starts <- seq.int(1L, nrow(coords), by = as.integer(chunk_size))
  parts <- vector("list", length(starts))
  for (i in seq_along(starts)) {
    start <- starts[[i]]
    end <- min(start + chunk_size - 1L, nrow(coords))
    logger::log_info("Matching coordinate rows {start}:{end} of {nrow(coords)}")
    parts[[i]] <- match_nearest_sites(coords[start:end, , drop = FALSE], sites)
    gc(verbose = FALSE)
  }
  result <- do.call(rbind, parts)
  row.names(result) <- NULL
  result
}

# ---- Publication ----------------------------------------------------------------

lookup_schema <- function() {
  arrow::schema(
    easting = arrow::int32(),
    northing = arrow::int32(),
    site_id = arrow::int32(),
    distance_m = arrow::float64()
  )
}

create_stage_path <- function(output_path) {
  tempfile(
    pattern = paste0(".", basename(output_path), ".stage-"),
    tmpdir = dirname(output_path),
    fileext = ".parquet"
  )
}

validate_lookup_stage <- function(stage_path, expected_schema, expected_rows,
                                  usable_site_ids, sample_expected) {
  reader <- arrow::ParquetFileReader$create(stage_path)
  if (!reader$GetSchema()$Equals(expected_schema, check_metadata = FALSE)) {
    stop("Staged long-run lookup schema does not match the explicit contract.", call. = FALSE)
  }
  if (reader$num_rows != expected_rows) {
    stop("Staged long-run lookup row count changed during publication.", call. = FALSE)
  }
  staged <- as.data.frame(arrow::read_parquet(stage_path))
  if (anyNA(staged$easting) || anyNA(staged$northing)) {
    stop("Staged long-run lookup has missing coordinates.", call. = FALSE)
  }
  if (anyDuplicated(staged[c("easting", "northing")]) > 0L) {
    stop("Staged long-run lookup is not unique on (easting, northing).", call. = FALSE)
  }
  if (any(!is.finite(staged$distance_m)) || any(staged$distance_m < 0)) {
    stop("Staged long-run lookup has a non-finite or negative distance.", call. = FALSE)
  }
  if (anyNA(staged$site_id) || !all(staged$site_id %in% usable_site_ids)) {
    stop("Staged long-run lookup matched a site outside the usable set.", call. = FALSE)
  }

  sample_actual <- merge(
    sample_expected[c("easting", "northing", "oracle_distance_m")],
    staged,
    by = c("easting", "northing"),
    all.x = TRUE
  )
  if (nrow(sample_actual) != nrow(sample_expected) ||
      anyNA(sample_actual$distance_m) ||
      any(abs(sample_actual$distance_m - sample_actual$oracle_distance_m) > 1e-6)) {
    stop("Staged long-run lookup disagrees with the brute-force sample oracle.", call. = FALSE)
  }
  rm(staged)
  gc(verbose = FALSE)
  invisible(TRUE)
}

validate_lookup_publication <- function(output_path, stage_path,
                                        expected_schema, expected_rows) {
  if (!file.exists(output_path) || file.exists(stage_path)) {
    stop(
      "Long-run lookup promotion did not materialize at the canonical path.",
      call. = FALSE
    )
  }
  reader <- arrow::ParquetFileReader$create(output_path)
  if (!reader$GetSchema()$Equals(expected_schema, check_metadata = FALSE) ||
      reader$num_rows != expected_rows) {
    stop("Published long-run lookup failed canonical verification.", call. = FALSE)
  }
  invisible(TRUE)
}

write_lookup <- function(lookup, sites, output_path, n_oracle_sample,
                         promote_stage = function(from, to) file.rename(from, to)) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  stage_path <- create_stage_path(output_path)
  on.exit(unlink(stage_path), add = TRUE)

  schema <- lookup_schema()
  table <- arrow::Table$create(lookup, schema = schema)
  arrow::write_parquet(table, stage_path)
  rm(table)
  gc(verbose = FALSE)

  sample_indices <- unique(as.integer(round(seq(
    1,
    nrow(lookup),
    length.out = min(n_oracle_sample, nrow(lookup))
  ))))
  sample_expected <- lookup[sample_indices, c("easting", "northing"), drop = FALSE]
  sample_expected$oracle_distance_m <- brute_force_nearest(sample_expected, sites)$distance_m

  validate_lookup_stage(
    stage_path,
    expected_schema = schema,
    expected_rows = nrow(lookup),
    usable_site_ids = sites$sites$site_id,
    sample_expected = sample_expected
  )

  if (!promote_stage(stage_path, output_path)) {
    stop("Failed to promote the staged long-run lookup.", call. = FALSE)
  }
  validate_lookup_publication(
    output_path,
    stage_path,
    expected_schema = schema,
    expected_rows = nrow(lookup)
  )
  logger::log_info(
    "Published {nrow(lookup)} coordinate lookup rows to {output_path}"
  )
  invisible(list(output_rows = nrow(lookup), output_path = output_path))
}

# ---- Execution ------------------------------------------------------------------

main <- function() {
  initialise_environment()
  initialise_logging()
  started <- Sys.time()

  coordinates <- load_coordinates(CONFIG$history_paths)
  sites <- prepare_sites(
    CONFIG$site_group_crosswalk_path,
    years = CONFIG$site_group_years,
    dropped_site_path = CONFIG$dropped_site_path
  )
  lookup <- match_all_coordinates(
    coordinates$coordinates,
    sites,
    chunk_size = CONFIG$chunk_size
  )
  logger::log_info(
    "Distance to nearest usable Site Group (m): ",
    "min={sprintf('%.1f', min(lookup$distance_m))}, ",
    "median={sprintf('%.1f', stats::median(lookup$distance_m))}, ",
    "max={sprintf('%.1f', max(lookup$distance_m))}."
  )
  write_lookup(
    lookup,
    sites,
    output_path = CONFIG$output_path,
    n_oracle_sample = CONFIG$n_oracle_sample
  )
  logger::log_info(
    "Script finished in {sprintf('%.1f', as.numeric(difftime(Sys.time(), started, units = 'secs')))} s"
  )
  invisible(NULL)
}

if (sys.nframe() == 0L) {
  main()
}
