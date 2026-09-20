# Diagnose the distance between relevant unresolved physical shores and the
# official classification polygons. These are alignment diagnostics, not an
# alternative coastal threshold or regression specification.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

audit_open_coast_alignment <- function() {
  root <- file.path(salience_report_root(), "geometry")
  reference <- readRDS(file.path(root, "private", "candidate_reference.rds"))
  evidence <- readRDS(file.path(root, "private", "site_candidate_evidence.rds"))
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  ids <- evidence$site_id[evidence$open_coast_status == "unresolved_shore"]
  locations <- locations[match(ids, locations$site_id), ]
  stopifnot(!anyNA(locations$site_id))
  points <- sf::st_as_sf(locations, coords = c("easting", "northing"), crs = 27700)
  nearest <- sf::st_nearest_feature(points, reference$unresolved)
  connectors <- sf::st_nearest_points(points, reference$unresolved[nearest, ], pairwise = TRUE)
  xy <- do.call(rbind, lapply(connectors, function(x) tail(x, 1L)))
  shore_points <- sf::st_as_sf(data.frame(easting = xy[,1], northing = xy[,2]),
                              coords = c("easting", "northing"), crs = 27700)
  raw <- here::here("data", "raw", "geography", "open_coast", "classifications")
  polygons <- do.call(c, lapply(c("ea", "nrw-coastal", "nrw-transitional"), function(name) {
    sf::st_geometry(sf::st_transform(sf::st_read(file.path(raw, paste0(name, ".geojson")), quiet = TRUE), 27700))
  }))
  closest <- sf::st_nearest_feature(shore_points, polygons)
  distance <- as.numeric(sf::st_distance(shore_points, polygons[closest], by_element = TRUE))
  diagnostics <- tibble::tibble(site_id = ids, source_id = reference$unresolved$source_id[nearest],
                                segment_id = reference$unresolved$segment_id[nearest],
                                alignment_m = distance, source_row = closest)
  saveRDS(diagnostics, file.path(root, "private", "nearest_unresolved_alignment.rds"))
  bins <- cut(distance, c(-Inf, 0, 1, 5, 10, 25, 50, 100, 500, Inf))
  summary <- as.data.frame(table(bins), stringsAsFactors = FALSE)
  names(summary) <- c("alignment_gap_m", "n_sites")
  utils::write.csv(summary, file.path(root, "alignment_summary.csv"), row.names = FALSE)
  by_company <- tibble::tibble(water_company = locations$water_company, gap_over_500m = distance > 500) |>
    dplyr::group_by(.data$water_company) |>
    dplyr::summarise(n_unresolved = dplyr::n(), n_gap_over_500m = sum(.data$gap_over_500m), .groups = "drop")
  utils::write.csv(by_company, file.path(root, "alignment_by_company.csv"), row.names = FALSE)
  print(summary)
  cat("Distinct unresolved source features beyond 500m:",
      dplyr::n_distinct(diagnostics$source_id[distance > 500]), "\n")
  # Supported geometry is still only a candidate until the source/coverage gate
  # passes. Normalize the label used by the initial exact-overlay diagnostic.
  evidence$open_coast_status[evidence$open_coast_status == "validated"] <- "supported_candidate"
  saveRDS(evidence, file.path(root, "private", "site_candidate_evidence.rds"))
  coverage <- utils::read.csv(file.path(root, "coverage_summary.csv"))
  coverage$status[coverage$status == "validated"] <- "supported_candidate"
  utils::write.csv(coverage, file.path(root, "coverage_summary.csv"), row.names = FALSE)
  files <- c("coverage_summary.csv", "alignment_summary.csv", "alignment_by_company.csv",
              paste0(c("thames", "severn", "morecambe", "poole", "dee", "solway"), ".png"),
              "source_manifest.json")
  manifest <- list(status = "candidate_not_published", alignment_m = reference$alignment_m,
    distance_engine = "sf/GEOS planar Euclidean, EPSG:27700, metres",
    physical_coordinate_displacement_m = 0,
    geometry_hash = salience_file_hash(file.path(root, "private", "candidate_reference.rds")),
    site_evidence_hash = salience_file_hash(file.path(root, "private", "site_candidate_evidence.rds")),
    artifacts = lapply(files, function(path) list(path = path, sha256 = salience_file_hash(file.path(root, path)))))
  jsonlite::write_json(manifest, file.path(root, "candidate_manifest.json"), auto_unbox = TRUE, pretty = TRUE)
  invisible(diagnostics)
}

if (sys.nframe() == 0L) audit_open_coast_alignment()
