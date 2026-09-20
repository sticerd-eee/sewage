# Source alignment and coverage diagnostics only. Never publishes a reference.
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"))
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))

audit_open_coast_geometry <- function(alignment_m = 0, output_name = "geometry", include_sepa = FALSE) {
  raw <- here::here("data", "raw", "geography", "open_coast")
  output <- file.path(salience_report_root(), output_name)
  dir.create(file.path(output, "private"), recursive = TRUE, showWarnings = FALSE)
  os_root <- file.path(raw, "os_openmap_local_2026_04")
  os <- jsonlite::read_json(file.path(os_root, "source_manifest.json"))
  for (entry in os$components) {
    stopifnot(identical(salience_file_hash(file.path(os_root, entry$path)), entry$sha256))
  }
  classes <- jsonlite::read_json(file.path(raw, "classifications", "source_manifest.json"))
  for (entry in classes) {
    stopifnot(identical(salience_file_hash(file.path(raw, "classifications", entry$path)), entry$sha256))
  }
  shore <- dplyr::bind_rows(lapply(list.files(os_root, pattern = "[.]shp$", recursive = TRUE,
                                             full.names = TRUE), function(path) {
    sf::st_read(path, quiet = TRUE) |>
      dplyr::filter(.data$CLASSIFICA == "High Water Mark") |>
      dplyr::transmute(source_id = .data$ID)
  }))
  # OS supplies uncut features on overlapping tiles; duplicates must agree.
  geometries <- vapply(sf::st_as_binary(sf::st_geometry(shore)), digest::digest, "", algo = "sha256")
  duplicate_ids <- duplicated(shore$source_id)
  stopifnot(all(geometries[duplicate_ids] == geometries[match(shore$source_id[duplicate_ids], shore$source_id)]))
  shore <- shore[!duplicate_ids, ]
  read_class <- function(name) sf::st_transform(sf::st_read(file.path(raw, "classifications", name), quiet = TRUE), 27700)
  ea <- read_class("ea.geojson")
  nrw_coastal <- read_class("nrw-coastal.geojson")
  nrw_transitional <- read_class("nrw-transitional.geojson")
  coastal <- dplyr::bind_rows(
    dplyr::transmute(dplyr::filter(ea, .data$water_body_type == "Coastal"), water_id = .data$water_body_id),
    dplyr::transmute(nrw_coastal, water_id = .data$wbid))
  transitional <- dplyr::bind_rows(
    dplyr::transmute(dplyr::filter(ea, .data$water_body_type == "Transitional"), water_id = .data$water_body_id),
    dplyr::transmute(nrw_transitional, water_id = .data$wbid))
  if (include_sepa) {
    sepa <- jsonlite::read_json(file.path(raw, "sepa", "source_manifest.json"))
    for (entry in sepa$sources) {
      stopifnot(identical(salience_file_hash(file.path(raw, "sepa", entry$path)), entry$sha256))
    }
    read_sepa <- function(name) {
      sf::st_read(file.path(raw, "sepa", paste0(name, ".geojson")), quiet = TRUE) |>
        sf::st_transform(27700) |>
        dplyr::transmute(water_id = as.character(.data$water_body_id))
    }
    coastal <- dplyr::bind_rows(coastal, read_sepa("coastal"))
    transitional <- dplyr::bind_rows(transitional, read_sepa("estuaries"))
  }
  message("Physical high-water features: ", nrow(shore), "; coastal bodies: ", nrow(coastal),
          "; transitional bodies: ", nrow(transitional))
  # Retain all shores within an explicit search domain. Measurement below
  # rejects any site whose nearest-shore disk reaches this domain's boundary.
  coverage <- sf::st_as_sfc(sf::st_bbox(c(xmin=-100000, ymin=-100000, xmax=800000,
    ymax=800000), crs=27700)) |> sf::st_sf()
  shore <- suppressWarnings(sf::st_crop(shore, sf::st_bbox(coverage)))
  # Bound overlay complexity without simplifying or shifting physical lines.
  cells <- sf::st_make_grid(shore, cellsize = 50000)
  shore_hits <- sf::st_intersects(cells, shore)
  cells <- cells[lengths(shore_hits) > 0L]
  shore_hits <- shore_hits[lengths(shore_hits) > 0L]
  coast_hits <- sf::st_intersects(sf::st_buffer(cells, alignment_m + 1), coastal)
  transition_hits <- sf::st_intersects(sf::st_buffer(cells, alignment_m + 1), transitional)
  pieces <- lapply(seq_along(cells), function(i) {
    crop <- function(data, box) suppressWarnings(sf::st_crop(data, sf::st_bbox(box)))
    physical <- crop(shore[shore_hits[[i]], ], cells[i])
    physical <- physical[!sf::st_is_empty(physical) & sf::st_dimension(physical) == 1L, ]
    physical <- suppressWarnings(sf::st_collection_extract(physical, "LINESTRING"))
    if (!nrow(physical)) return(NULL)
    # The padded classification crop ensures tile edges cannot affect alignment.
    box <- sf::st_buffer(cells[i], alignment_m + 1)
    result <- split_open_coast_shoreline(physical,
      crop(coastal[coast_hits[[i]], ], box), crop(transitional[transition_hits[[i]], ], box),
      alignment_m, allow_empty_tile = TRUE)
    if (i %% 20L == 0L) message("Processed ", i, "/", length(cells), " shoreline tiles")
    result
  })
  reference <- stats::setNames(lapply(c("retained", "excluded", "unresolved"), function(kind) {
    dplyr::bind_rows(lapply(pieces, `[[`, kind))
  }), c("retained", "excluded", "unresolved"))
  reference$alignment_m <- alignment_m
  reference$coverage <- coverage
  if (!nrow(reference$retained)) stop("No physical coast retained across the complete source.")
  saveRDS(reference, file.path(output, "private", "candidate_reference.rds"))
  sites <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), sites)
  locations <- sites$read_site_group_projection(sites$CONFIG$crosswalk_path, years = sites$YEARS)
  usable <- is.finite(locations$easting) & is.finite(locations$northing)
  points <- sf::st_as_sf(locations[usable, ], coords = c("easting", "northing"), crs = 27700)
  evidence <- measure_open_coast_evidence(points, reference, coverage)
  saveRDS(evidence, file.path(output, "private", "site_candidate_evidence.rds"))
  summary <- as.data.frame(table(evidence$open_coast_status), stringsAsFactors = FALSE)
  names(summary) <- c("status", "n_sites")
  summary <- rbind(summary, data.frame(status = "missing_location", n_sites = sum(!usable)))
  utils::write.csv(summary, file.path(output, "coverage_summary.csv"), row.names = FALSE)
  print(summary)
  panel_bounds <- list(thames = c(-0.2,51.35,0.8,51.75), severn = c(-3.3,51.3,-2.3,52.0),
                       morecambe = c(-3.3,53.8,-2.7,54.3), poole = c(-2.2,50.55,-1.8,50.8),
                       dee = c(-3.4,53.1,-2.8,53.5), solway = c(-3.6,54.7,-2.8,55.1))
  for (name in names(panel_bounds)) {
    b <- panel_bounds[[name]]
    region <- sf::st_as_sfc(sf::st_bbox(stats::setNames(b, c("xmin","ymin","xmax","ymax")), crs=4326)) |>
      sf::st_transform(27700)
    png(file.path(output, paste0(name, ".png")), width=1400, height=1000, res=140)
    plot(region, col="white", border=NA, axes=TRUE, main=paste(name, "— candidate physical shoreline"))
    for (kind in c("retained", "excluded", "unresolved")) {
      part <- suppressWarnings(sf::st_crop(reference[[kind]], sf::st_bbox(region)))
      if (nrow(part)) plot(sf::st_geometry(part), add=TRUE,
                          col=c(retained="#0072B2", excluded="#999999", unresolved="#D55E00")[[kind]], lwd=1.2)
    }
    legend("bottomleft", legend=c("Coastal candidate", "Transitional", "Unresolved"),
            col=c("#0072B2", "#999999", "#D55E00"), lty=1, bg="white", cex=.85)
    dev.off()
  }
  jsonlite::write_json(list(status="candidate_not_published", alignment_m=alignment_m,
    source_hashes=c(salience_file_hash(file.path(os_root,"source_manifest.json")),
                    salience_file_hash(file.path(raw,"classifications","source_manifest.json")))),
    file.path(output,"candidate_manifest.json"), auto_unbox=TRUE, pretty=TRUE)
  invisible(summary)
}

if (sys.nframe() == 0L) audit_open_coast_geometry()
