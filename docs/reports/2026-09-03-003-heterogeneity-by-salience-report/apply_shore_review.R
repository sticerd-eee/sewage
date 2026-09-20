# Replay reviewed, segment-specific decisions without changing physical lines.
source(here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report", "report_storage.R"))
source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"))

apply_shore_review <- function() {
  # Load sf methods before subsetting serialized sf objects.
  requireNamespace("sf")
  root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
  reference <- readRDS(file.path(root, "private", "mouth_review_reference.rds"))
  decisions <- read.csv(file.path(root, "manual_shore_decisions.csv"))
  if (anyDuplicated(decisions$segment_id) || anyNA(decisions) ||
      !all(decisions$decision %in% c("retained", "excluded", "mouth_split")) ||
      !all(decisions$segment_id %in% reference$unresolved$segment_id))
    stop("Invalid shoreline review decisions.")
  selected <- reference$unresolved[match(decisions$segment_id, reference$unresolved$segment_id), ]
  if (!identical(selected$source_id, decisions$source_id)) stop("Reviewed physical source identity changed.")
  cuts <- read.csv(file.path(root, "mouth_separator_extensions.csv"))
  for (i in seq_len(nrow(cuts))) {
    cut <- cuts[i, ]
    shore <- selected[selected$source_id == cut$source_id & decisions$decision == "mouth_split", ]
    if (!nrow(shore)) stop("Mouth separator has no reviewed physical segment.")
    origin <- c(cut$x0, cut$y0)
    direction <- c(cut$x1, cut$y1) - origin
    direction <- direction / sqrt(sum(direction^2))
    normal <- c(-direction[2], direction[1])
    if (sum((c(cut$coast_x, cut$coast_y) - origin) * normal) < 0) normal <- -normal
    a <- origin - 100000 * direction
    b <- origin + 100000 * direction
    mask <- sf::st_sfc(sf::st_polygon(list(rbind(a, b, b + 100000 * normal,
      a + 100000 * normal, a))), crs = 27700)
    for (kind in c("retained", "excluded")) {
      pieces <- open_coast_line_parts(suppressWarnings(if (kind == "retained")
        sf::st_intersection(shore, mask) else sf::st_difference(shore, mask)))
      pieces$evidence <- rep("reviewed_mouth_separator_extension", nrow(pieces))
      pieces$segment_id <- vapply(seq_len(nrow(pieces)), function(j)
        digest::digest(list(pieces$source_id[j], sf::st_as_binary(sf::st_geometry(pieces[j, ]))), algo = "sha256"), "")
      reference[[kind]] <- dplyr::bind_rows(reference[[kind]], pieces)
    }
  }
  for (kind in c("retained", "excluded")) {
    pieces <- selected[decisions$decision == kind, ]
    pieces$evidence <- rep("reviewed_physical_shore", nrow(pieces))
    reference[[kind]] <- dplyr::bind_rows(reference[[kind]], pieces)
  }
  reference$unresolved <- reference$unresolved[!reference$unresolved$segment_id %in% decisions$segment_id, ]
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  usable <- is.finite(locations$easting) & is.finite(locations$northing)
  points <- sf::st_as_sf(locations[usable, ], coords = c("easting", "northing"), crs = 27700)
  evidence <- measure_open_coast_evidence(points, reference, reference$coverage)
  saveRDS(reference, file.path(root, "private", "reviewed_reference.rds"))
  saveRDS(evidence, file.path(root, "private", "reviewed_evidence.rds"))
  print(table(evidence$open_coast_status))
  cat("Sites within 1m of the 2km threshold:", sum(abs(evidence$nearest_candidate_m - 2000) <= 1), "\n")
  invisible(reference)
}

if (sys.nframe() == 0L) apply_shore_review()
