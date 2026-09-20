source(here::here("scripts", "R", "utils", "open_coast_contracts.R"))
source(here::here("scripts", "R", "utils", "salience_group_utils.R"))
producer <- new.env(parent = globalenv())
sys.source(here::here("scripts", "R", "06_analysis_datasets", "build_prior_characteristics.R"), producer)
generation <- paste(rep("a", 64), collapse = "")
sites <- tibble::tibble(site_id = 1:5,
  distance_to_coast_m = c(100, 3000, NA, 6000, 4000),
  bath_ever_2124 = c(TRUE, FALSE, FALSE, FALSE, TRUE), bath_unknown_2124 = FALSE,
  shell_ever_2124 = FALSE, shell_unknown_2124 = FALSE,
  distance_to_open_coast_m = c(1999.999, 2000, 2000.001, NA, 5000),
  open_coast_status = c(rep("validated", 3), "missing_location", "validated"),
  geometry_generation = generation, site_generation = generation)
validate_open_coast_sites(sites)
expect_error <- function(expr) stopifnot(inherits(tryCatch(force(expr), error = identity), "error"))
expect_error(validate_open_coast_sites(dplyr::mutate(sites, distance_to_open_coast_m = -1)))
expect_error(validate_open_coast_sites(dplyr::mutate(sites, open_coast_status = "validated")))
root <- tempfile("open-coast-data-")
dir.create(root)
site_path <- file.path(root, "sites.parquet")
arrow::write_parquet(sites, site_path)
stopifnot(identical(arrow::read_parquet(site_path)$distance_to_open_coast_m,
                    sites$distance_to_open_coast_m))
classified <- classify_salience_groups(dplyr::mutate(arrow::read_parquet(site_path), region = NA_character_),
  profile = "open_coast")
stopifnot(identical(classified$coastal, c(TRUE, TRUE, FALSE, FALSE, FALSE)),
          identical(classified$inland, c(FALSE, FALSE, TRUE, FALSE, TRUE)))
pairs <- tibble::tibble(property = c("coast_unknown", "coast_unknown", "inland_unknown",
  "inland_unknown", "inland", "unknown", "boundary"),
  site_id = c(1L, 4L, 5L, 4L, 3L, 4L, 2L), distance_m = 100)
for (market in c("sales", "rentals")) {
  id <- if (market == "sales") "house_id" else "rental_id"
  lookup <- dplyr::rename(pairs, !!id := "property")
  lookup <- dplyr::bind_rows(lookup, tibble::tibble(!!id := "no_site", site_id = NA_integer_, distance_m = NA_real_))
  # Counts are exact per property and identical across radii in this fixture.
  source <- tidyr::expand_grid(!!id := c(unique(pairs$property), "no_site"), radius = c(250L, 500L, 1000L))
  source$n_spill_sites <- vapply(source[[id]], function(key) sum(pairs$property == key), 1L)
  source$spill_count_weekly_avg <- seq_len(nrow(source)) / 10
  source$spill_hrs_weekly_avg <- seq_len(nrow(source))
  spec <- list(market = market, id = id, source_path = file.path(root, paste0(market, "-source.parquet")),
               lookup_path = file.path(root, paste0(market, "-lookup.parquet")))
  arrow::write_parquet(source, spec$source_path)
  arrow::write_parquet(lookup, spec$lookup_path)
  memory <- producer$build_market_prior_characteristics(source, lookup, sites, id, market, refined = TRUE)
  for (radius in c(250L, 500L, 1000L)) {
    actual <- producer$build_production_radius(spec, radius, site_path, refined = TRUE)$data |>
      dplyr::arrange(.data[[id]])
    expected <- memory |> dplyr::filter(.data$radius == .env$radius) |> dplyr::arrange(.data[[id]])
    stopifnot(isTRUE(all.equal(actual, expected, check.attributes = FALSE)))
    validate_open_coast_radius(actual, generation)
    expect_error(validate_open_coast_radius(actual, paste(rep("b", 64), collapse = "")))
    classified <- classify_salience_groups(dplyr::mutate(actual, region = "London"),
      source = "radius", profile = "open_coast")
    stopifnot(classified$coastal[classified[[id]] == "coast_unknown"],
      !classified$inland[classified[[id]] == "inland_unknown"],
      classified$inland[classified[[id]] == "inland"],
      !classified$coastal[classified[[id]] == "unknown"],
      !classified$inland[classified[[id]] == "no_site"])
    stopifnot(actual$n_open_coast_missing[actual[[id]] == "coast_unknown"] == 1L,
      actual$n_open_coast_known[actual[[id]] == "no_site"] == 0L,
      is.na(actual$min_open_coast_dist_m[actual[[id]] == "unknown"]))
  }
  spec$output_path <- file.path(root, market, "prior_characteristics")
  producer$publish_market(spec, site_path, refined = TRUE)
  validate_open_coast_companion_manifest(spec$output_path, generation)
  manifest_hash <- digest::digest(file = file.path(spec$output_path, "_open_coast_manifest.json"), algo = "sha256")
  arrow::write_parquet(dplyr::mutate(source, spill_count_weekly_avg = 999), spec$source_path)
  expect_error(validate_open_coast_companion_manifest(spec$output_path, generation))
  expect_error(producer$publish_market(spec, site_path, refined = TRUE))
  stopifnot(identical(manifest_hash,
    digest::digest(file = file.path(spec$output_path, "_open_coast_manifest.json"), algo = "sha256")))
  arrow::write_parquet(source, spec$source_path)
  arrow::write_parquet(dplyr::bind_rows(lookup, lookup[1, ]), spec$lookup_path)
  expect_error(producer$build_production_radius(spec, 250L, site_path, refined = TRUE))
  arrow::write_parquet(dplyr::mutate(lookup, distance_m = -1), spec$lookup_path)
  expect_error(producer$build_production_radius(spec, 250L, site_path, refined = TRUE))
}
site_builder <- new.env(parent = globalenv())
sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), site_builder)
legacy <- tibble::tibble(site_id = 1:4, distance_to_coast_m = c(1, 2, 3, NA_real_))
for (type in c("bath", "shell")) {
  for (field in c("status", "mixed", "unknown")) {
    for (year in 21:24) legacy[[paste(type, field, year, sep = "_")]] <-
      if (field == "status") rep("not_designated", 4) else rep(FALSE, 4)
  }
}
for (type in c("bath", "shell")) {
  for (field in c("ever_2124", "changed_2124", "unknown_2124", "24"))
    legacy[[paste(type, field, sep = "_")]] <- FALSE
}
projection <- tibble::tibble(site_id = 4:1, easting = c(NA, 2000.001, 2000, 1999.999), northing = 500)
reference <- list(retained = sf::st_sf(source_id = "fixture", geometry = sf::st_sfc(
  sf::st_linestring(matrix(c(0,0,0,1000), ncol = 2, byrow = TRUE)), crs = 27700)))
reference$unresolved <- reference$retained[FALSE, ]
reference$coverage <- sf::st_sf(geometry = sf::st_as_sfc(sf::st_bbox(c(
  xmin = -10000, ymin = -10000, xmax = 10000, ymax = 10000), crs = sf::st_crs(27700))))
reference$status <- "validated"
reference$review <- list(unresolved_relevant_segments = 0L,
                        location_hash = site_builder$open_coast_location_hash(projection))
reference$source_hashes <- generation
reference$generation <- digest::digest(reference, algo = "sha256")
added <- site_builder$add_open_coast_characteristics(legacy, projection, reference)
stopifnot(identical(added[names(legacy)], legacy),
          isTRUE(all.equal(added$distance_to_open_coast_m, c(1999.999, 2000, 2000.001, NA_real_), tolerance = 1e-12)))
arrow::write_parquet(arrow::Table$create(added, schema = site_builder$site_group_characteristics_schema(TRUE)), site_path)
site_builder$validate_site_group_characteristics(arrow::read_parquet(site_path), 1:4, TRUE)
reference$status <- "candidate_not_published"
expect_error(site_builder$add_open_coast_characteristics(legacy, projection, reference))
unlink(root, recursive = TRUE)
cat("Open-coast Arrow and in-memory data contracts passed for both markets and all radii.\n")
