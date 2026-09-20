# Propose traceable exclusions for physical tidal banks extending landward of
# the saline WFD polygons. These diagnostics do not publish a reference.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"))

review_inland_shore_extensions <- function() {
  root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
  reference <- readRDS(file.path(root, "private", "candidate_reference.rds"))
  raw <- here::here("data", "raw", "geography", "open_coast")
  river_manifest <- jsonlite::read_json(file.path(raw, "rivers", "source_manifest.json"))
  river_path <- file.path(raw, "rivers", river_manifest$path)
  stopifnot(identical(salience_file_hash(river_path), river_manifest$sha256))
  rivers <- sf::st_transform(sf::st_read(river_path, quiet = TRUE), 27700)
  ea <- sf::st_transform(sf::st_read(file.path(raw, "classifications", "ea.geojson"), quiet = TRUE), 27700)
  read_class <- function(path) sf::st_geometry(sf::st_transform(sf::st_read(file.path(raw, path), quiet = TRUE), 27700))
  coastal <- c(sf::st_geometry(ea[ea$water_body_type == "Coastal", ]),
    read_class("classifications/nrw-coastal.geojson"), read_class("sepa/coastal.geojson"))
  transitional <- c(sf::st_geometry(ea[ea$water_body_type == "Transitional", ]),
    read_class("classifications/nrw-transitional.geojson"), read_class("sepa/estuaries.geojson"))
  shore <- reference$unresolved
  nearest <- sf::st_nearest_feature(shore, coastal)
  coast_gap <- as.numeric(sf::st_distance(shore, coastal[nearest], by_element = TRUE))
  # The guard keeps mouth/coastal alignment cases out of this inland review.
  # It is not an overflow classification threshold or a coast-buffer extension.
  inland <- which(coast_gap > 1000)
  sampled <- sf::st_segmentize(shore[inland, ], dfMaxLength = 50)
  xy <- sf::st_coordinates(sampled)
  points <- sf::st_as_sf(data.frame(x = xy[,1], y = xy[,2]), coords = c("x", "y"), crs = 27700)
  river_nearest <- sf::st_nearest_feature(points, rivers)
  river_distance <- as.numeric(sf::st_distance(points, rivers[river_nearest, ], by_element = TRUE))
  transition_nearest <- sf::st_nearest_feature(points, transitional)
  transition_distance <- as.numeric(sf::st_distance(points, transitional[transition_nearest], by_element = TRUE))
  sampled_evidence <- tibble::tibble(segment_row = inland[xy[, "L1"]],
    river_distance = river_distance, transitional_distance = transition_distance,
    water_id = rivers$water_body_id[river_nearest], water_name = rivers$water_body_name[river_nearest])
  review <- sampled_evidence |>
    dplyr::summarise(max_river_gap_m = max(.data$river_distance),
      max_transitional_gap_m = max(.data$transitional_distance),
      max_noncoastal_gap_m = max(pmin(.data$river_distance, .data$transitional_distance)),
      water_ids = paste(sort(unique(.data$water_id)), collapse = ";"),
      water_names = paste(sort(unique(.data$water_name)), collapse = ";"), .by = "segment_row") |>
    dplyr::mutate(segment_id = shore$segment_id[.data$segment_row], source_id = shore$source_id[.data$segment_row],
      min_coastal_gap_m = coast_gap[.data$segment_row],
      proposed_exclusion = .data$max_noncoastal_gap_m + 25 + 1000 < .data$min_coastal_gap_m,
      evidence_url = river_manifest$metadata_url)
  # 50m sampling plus a 25m distance bound checks the intervening shore too.
  # The positive river/transitional evidence must remain at least 1km closer
  # than every coastal body along the entire segment. This proposes an inland
  # extension for map review; it never adds coast or classifies an overflow.
  utils::write.csv(review, file.path(root, "inland_extension_review.csv"), row.names = FALSE)
  excluded <- review$segment_row[review$proposed_exclusion]
  reference$excluded <- dplyr::bind_rows(reference$excluded,
    dplyr::mutate(shore[excluded, ], evidence = "inland_noncoastal_candidate"))
  reference$unresolved <- shore[-excluded, ]
  saveRDS(reference, file.path(root, "private", "inland_review_reference.rds"))
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  usable <- is.finite(locations$easting) & is.finite(locations$northing)
  points <- sf::st_as_sf(locations[usable, ], coords = c("easting", "northing"), crs = 27700)
  evidence <- measure_open_coast_evidence(points, reference, reference$coverage)
  saveRDS(evidence, file.path(root, "private", "inland_review_evidence.rds"))
  print(table(evidence$open_coast_status))
  print(review |> dplyr::count(.data$proposed_exclusion))
  invisible(review)
}

if (sys.nframe() == 0L) review_inland_shore_extensions()
