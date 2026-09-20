source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

inspect_unresolved_shores <- function(directory, reference_name = "candidate_reference.rds",
                                      evidence_name = "site_candidate_evidence.rds", output_name = "unresolved_review") {
  root <- file.path(salience_report_root(), directory)
  reference <- readRDS(file.path(root, "private", reference_name))
  evidence <- readRDS(file.path(root, "private", evidence_name))
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  unresolved <- evidence[evidence$open_coast_status == "unresolved_shore", ]
  locations <- locations[match(unresolved$site_id, locations$site_id), ]
  points <- sf::st_as_sf(locations, coords = c("easting", "northing"), crs = 27700)
  nearest <- sf::st_nearest_feature(points, reference$unresolved)
  connectors <- sf::st_nearest_points(points, reference$unresolved[nearest, ], pairwise = TRUE)
  xy <- do.call(rbind, lapply(connectors, function(x) tail(x, 1L)))
  shore_points <- sf::st_as_sf(data.frame(x = xy[,1], y = xy[,2]), coords = c("x", "y"), crs = 27700)
  raw <- here::here("data", "raw", "geography", "open_coast")
  ea <- sf::st_transform(sf::st_read(file.path(raw, "classifications", "ea.geojson"), quiet = TRUE), 27700)
  result <- tibble::tibble(site_id = unresolved$site_id, source_id = reference$unresolved$source_id[nearest],
    segment_id = reference$unresolved$segment_id[nearest], x = xy[,1], y = xy[,2],
    segment_length_m = as.numeric(sf::st_length(reference$unresolved[nearest, ])),
    coast_candidate_m = unresolved$nearest_candidate_m)
  for (kind in c("Coastal", "Transitional")) {
    nrw_file <- if (kind == "Coastal") "nrw-coastal.geojson" else "nrw-transitional.geojson"
    sepa_file <- if (kind == "Coastal") "coastal.geojson" else "estuaries.geojson"
    polygons <- c(sf::st_geometry(ea[ea$water_body_type == kind, ]),
      sf::st_geometry(sf::st_read(file.path(raw, "classifications", nrw_file), quiet = TRUE)),
      sf::st_geometry(sf::st_read(file.path(raw, "sepa", sepa_file), quiet = TRUE)))
    closest <- sf::st_nearest_feature(shore_points, polygons)
    result[[paste0(tolower(kind), "_alignment_m")]] <- as.numeric(sf::st_distance(shore_points, polygons[closest], by_element = TRUE))
  }
  saveRDS(result, file.path(root, "private", paste0(output_name, ".rds")))
  summary <- result |>
    dplyr::mutate(reason = dplyr::case_when(
      .data$segment_length_m < 0.001 ~ "submillimetre overlay remnant",
      .data$coastal_alignment_m < 51 & .data$transitional_alignment_m < 51 ~ "near coastal/transitional seam",
      .data$transitional_alignment_m < 51 ~ "transitional alignment",
      .data$coastal_alignment_m < 51 ~ "coastal alignment",
      TRUE ~ "outside both selectors")) |>
    dplyr::count(.data$reason)
  print(summary)
  print(result |> dplyr::count(.data$source_id, sort = TRUE) |> head(20))
  utils::write.csv(summary, file.path(root, "unresolved_reasons.csv"), row.names = FALSE)
  invisible(result)
}

if (sys.nframe() == 0L) inspect_unresolved_shores(commandArgs(trailingOnly = TRUE)[[1L]])
