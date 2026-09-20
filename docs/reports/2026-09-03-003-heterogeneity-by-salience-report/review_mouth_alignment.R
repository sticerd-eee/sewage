# Diagnostic nearest-category ownership of short physical shoreline intervals.
# Original polygon edges select categories; no polygon edge becomes shoreline.
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"))
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

review_mouth_alignment <- function(alignment_m = 100, interval_m = 1) {
  raw <- here::here("data", "raw", "geography", "open_coast")
  root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
  ea <- sf::st_transform(sf::st_read(file.path(raw, "classifications", "ea.geojson"), quiet = TRUE), 27700)
  read_geometry <- function(path) sf::st_geometry(sf::st_transform(sf::st_read(file.path(raw, path), quiet = TRUE), 27700))
  coastal <- c(sf::st_geometry(ea[ea$water_body_type == "Coastal", ]),
    read_geometry("classifications/nrw-coastal.geojson"), read_geometry("sepa/coastal.geojson"))
  transitional <- c(sf::st_geometry(ea[ea$water_body_type == "Transitional", ]),
    read_geometry("classifications/nrw-transitional.geojson"), read_geometry("sepa/estuaries.geojson"))
  reference <- readRDS(file.path(root, "private", "inland_review_reference.rds"))
  sampled <- sf::st_segmentize(reference$unresolved, dfMaxLength = interval_m)
  coordinates <- lapply(sf::st_geometry(sampled), unclass)
  midpoints <- do.call(rbind, lapply(coordinates, function(x) (x[-1,,drop=FALSE] + x[-nrow(x),,drop=FALSE]) / 2))
  points <- sf::st_as_sf(data.frame(x = midpoints[,1], y = midpoints[,2]), coords = c("x", "y"), crs = 27700)
  distance_to <- function(polygons) {
    nearest <- sf::st_nearest_feature(points, polygons)
    distance <- numeric(nrow(points))
    # Serialize each large polygon once instead of repeating it for every
    # interval. Each group produces a points-by-one distance matrix.
    for (body in unique(nearest)) {
      index <- which(nearest == body)
      distance[index] <- as.numeric(sf::st_distance(points[index, ], polygons[body]))
    }
    distance
  }
  message("Classifying ", nrow(points), " physical intervals")
  coast <- distance_to(coastal)
  message("Coastal distances complete")
  transition <- distance_to(transitional)
  message("Transitional distances complete")
  category <- ifelse(coast < transition & coast <= alignment_m, "retained",
    ifelse(transition < coast & (transition <= alignment_m | transition + 1000 < coast), "excluded", "unresolved"))
  # Keep true conflicting polygon evidence unresolved; zero/zero is not a vote.
  groups <- split(category, rep(seq_along(coordinates), vapply(coordinates, nrow, 1L) - 1L))
  pieces <- lapply(seq_along(coordinates), function(i) {
    labels <- rle(groups[[i]])
    end <- cumsum(labels$lengths)
    begin <- c(1L, head(end, -1L) + 1L)
    lines <- lapply(seq_along(end), function(j) sf::st_linestring(coordinates[[i]][begin[j]:(end[j]+1L),,drop=FALSE]))
    output <- sf::st_sf(source_id = sampled$source_id[[i]], parent_segment_id = sampled$segment_id[[i]],
      decision = labels$values, geometry = sf::st_sfc(lines, crs = 27700))
    output$segment_id <- vapply(sf::st_as_binary(sf::st_geometry(output)), function(wkb)
      digest::digest(list(sampled$source_id[[i]], wkb), algo = "sha256"), "")
    output
  })
  reviewed <- dplyr::bind_rows(pieces)
  utils::write.csv(sf::st_drop_geometry(reviewed), file.path(root, "mouth_alignment_decisions.csv"), row.names = FALSE)
  reference$unresolved <- reference$unresolved[FALSE, ]
  for (kind in c("retained", "excluded", "unresolved")) {
    selected <- reviewed[reviewed$decision == kind, ]
    selected$evidence <- paste0(kind, "_alignment_candidate")
    reference[[kind]] <- dplyr::bind_rows(reference[[kind]], dplyr::select(selected, -"decision", -"parent_segment_id"))
  }
  reference$alignment_m <- alignment_m
  reference$eligibility_endpoint_resolution_m <- interval_m
  saveRDS(reference, file.path(root, "private", "mouth_review_reference.rds"))
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  usable <- is.finite(locations$easting) & is.finite(locations$northing)
  points <- sf::st_as_sf(locations[usable, ], coords = c("easting", "northing"), crs = 27700)
  evidence <- measure_open_coast_evidence(points, reference, reference$coverage)
  saveRDS(evidence, file.path(root, "private", "mouth_review_evidence.rds"))
  print(table(evidence$open_coast_status))
  print(reviewed |> sf::st_drop_geometry() |> dplyr::count(.data$decision))
  cat("Sites within interval resolution of 2km:", sum(abs(evidence$nearest_candidate_m - 2000) <= interval_m), "\n")
  invisible(reference)
}

if (sys.nframe() == 0L) review_mouth_alignment()
