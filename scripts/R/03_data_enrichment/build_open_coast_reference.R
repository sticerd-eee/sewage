# Physical high-water lines are the only source of retained geometry. Official
# water polygons select portions; their offshore edges and mouth separators
# never enter the distance reference. Source review precedes publication.

validate_open_coast_layer <- function(data, types, name) {
  if (!inherits(data, "sf") || is.na(sf::st_crs(data)) || sf::st_crs(data)$epsg != 27700L) {
    stop(name, " requires EPSG:27700 geometry.", call. = FALSE)
  }
  if (any(!sf::st_is_valid(data)) || any(sf::st_is_empty(data)) ||
      any(!as.character(sf::st_geometry_type(data)) %in% types)) {
    stop(name, " contains invalid, empty or unexpected geometry.", call. = FALSE)
  }
  invisible(data)
}

open_coast_line_parts <- function(data) {
  if (!nrow(data)) return(data)
  data <- data[!sf::st_is_empty(data), ]
  if (!nrow(data)) return(data)
  data <- suppressWarnings(sf::st_collection_extract(data, "LINESTRING", warn = FALSE))
  data <- suppressWarnings(sf::st_cast(data, "LINESTRING"))
  data[as.numeric(sf::st_length(data)) > 0, ]
}

split_open_coast_shoreline <- function(shore, coastal, transitional, alignment_m,
                                       allow_empty_tile = FALSE) {
  validate_open_coast_layer(shore, c("LINESTRING", "MULTILINESTRING"), "Physical shoreline")
  validate_open_coast_layer(coastal, c("POLYGON", "MULTIPOLYGON"), "Coastal classification")
  validate_open_coast_layer(transitional, c("POLYGON", "MULTIPOLYGON"), "Transitional classification")
  if (!nrow(shore) || (!nrow(coastal) && !allow_empty_tile)) {
    stop("Empty physical shoreline or coastal classification.")
  }
  if (!"source_id" %in% names(shore) || anyNA(shore$source_id) || anyDuplicated(shore$source_id)) {
    stop("Physical shoreline requires unique, nonmissing source_id.")
  }
  if (length(alignment_m) != 1L || !is.finite(alignment_m) || alignment_m < 0) {
    stop("alignment_m must be an explicit nonnegative finite tolerance.")
  }
  union_mask <- function(data) {
    if (nrow(data)) sf::st_union(sf::st_geometry(data)) else
      sf::st_sfc(sf::st_polygon(), crs = 27700)
  }
  coast_mask <- union_mask(coastal)
  transition_mask <- union_mask(transitional)
  if (alignment_m > 0) {
    coast_mask <- sf::st_buffer(coast_mask, alignment_m)
    transition_mask <- sf::st_buffer(transition_mask, alignment_m)
  }
  # Both unsupported and conflicting evidence remain unresolved. Buffering the
  # selectors never shifts physical coordinates or expands the 2 km threshold.
  intersect_shore <- function(mask) {
    if (!length(mask) || all(sf::st_is_empty(mask))) return(shore[FALSE, ])
    open_coast_line_parts(suppressWarnings(sf::st_intersection(shore, mask)))
  }
  retained <- intersect_shore(sf::st_difference(coast_mask, transition_mask))
  excluded <- intersect_shore(sf::st_difference(transition_mask, coast_mask))
  known_mask <- sf::st_sym_difference(coast_mask, transition_mask)
  unresolved <- if (!length(known_mask) || all(sf::st_is_empty(known_mask))) shore else
    open_coast_line_parts(suppressWarnings(sf::st_difference(shore, known_mask)))
  if (!nrow(retained) && !allow_empty_tile) stop("No eligible physical shoreline remains.")
  add_ids <- function(data, status) {
    geometry <- sf::st_as_binary(sf::st_geometry(data))
    data$segment_id <- vapply(seq_len(nrow(data)), function(i) {
      digest::digest(list(data$source_id[[i]], geometry[[i]]), algo = "sha256")
    }, "")
    data$evidence <- rep(status, nrow(data))
    data
  }
  list(retained = add_ids(retained, "coastal"), excluded = add_ids(excluded, "transitional"),
       unresolved = add_ids(unresolved, "unresolved"), alignment_m = alignment_m)
}

measure_open_coast_evidence <- function(points, reference, coverage) {
  validate_open_coast_layer(points, "POINT", "Site locations")
  validate_open_coast_layer(coverage, c("POLYGON", "MULTIPOLYGON"), "Physical source coverage")
  if (!nrow(coverage)) stop("Physical source coverage is absent.")
  if (!"site_id" %in% names(points) || anyNA(points$site_id) || anyDuplicated(points$site_id)) {
    stop("Site locations require unique, nonmissing site_id.")
  }
  validate_open_coast_layer(reference$retained, c("LINESTRING", "MULTILINESTRING"), "Retained shore")
  if (!nrow(reference$retained)) stop("Retained physical shoreline is empty.")
  distance_to <- function(lines) {
    if (!nrow(lines)) return(rep(Inf, nrow(points)))
    nearest <- sf::st_nearest_feature(points, lines)
    as.numeric(sf::st_distance(points, lines[nearest, ], by_element = TRUE))
  }
  distance <- distance_to(reference$retained)
  unresolved <- distance_to(reference$unresolved)
  # A valid nearest distance needs all potentially closer physical shores covered.
  extent_boundary <- sf::st_boundary(sf::st_union(sf::st_geometry(coverage)))
  within <- lengths(sf::st_within(points, coverage)) > 0L
  boundary_distance <- as.numeric(sf::st_distance(points, extent_boundary))
  status <- ifelse(!within | boundary_distance <= distance, "outside_coverage",
                   ifelse(unresolved <= distance, "unresolved_shore", "supported_candidate"))
  tibble::tibble(site_id = points$site_id,
                 distance_to_open_coast_m = ifelse(status == "supported_candidate", distance, NA_real_),
                 open_coast_status = status,
                 nearest_candidate_m = distance, nearest_unresolved_m = unresolved)
}

validate_published_open_coast_reference <- function(reference) {
  if (!identical(reference$status, "validated") ||
      !identical(reference$review$unresolved_relevant_segments, 0L) ||
      is.null(reference$source_hashes) || !length(reference$source_hashes) ||
      !all(grepl("^[a-f0-9]{64}$", reference$source_hashes))) {
    stop("Open-coast reference lacks completed source/coverage review.", call. = FALSE)
  }
  payload <- reference[setdiff(names(reference), "generation")]
  if (!identical(reference$generation, digest::digest(payload, algo = "sha256"))) {
    stop("Open-coast reference generation does not match its contents.", call. = FALSE)
  }
  validate_open_coast_layer(reference$retained, c("LINESTRING", "MULTILINESTRING"), "Retained shore")
  validate_open_coast_layer(reference$coverage, c("POLYGON", "MULTIPOLYGON"), "Reviewed coverage")
  if (!nrow(reference$retained) || !nrow(reference$coverage)) stop("Empty open-coast reference.")
  invisible(reference)
}

open_coast_location_hash <- function(projection) {
  ordered <- projection[order(projection$site_id), ]
  digest::digest(list(site_id = as.integer(ordered$site_id),
    easting = as.numeric(ordered$easting), northing = as.numeric(ordered$northing)), algo = "sha256")
}

publish_open_coast_reference <- function(candidate_path, review_path,
    output_path = here::here("data", "processed", "geography", "open_coast", "reference.rds")) {
  review <- jsonlite::read_json(review_path)
  if (!identical(review$status, "approved") || !identical(review$unresolved_relevant_segments, 0L) ||
      !identical(review$candidate_sha256, digest::digest(file = candidate_path, algo = "sha256")))
    stop("A matching completed geography review is required.")
  for (entry in review$sources) {
    path <- here::here(entry$path)
    if (!file.exists(path) || !identical(digest::digest(file = path, algo = "sha256"), entry$sha256))
      stop("Reviewed geography source changed: ", entry$path)
  }
  reference <- readRDS(candidate_path)
  reference$status <- "validated"
  reference$review <- review
  reference$source_hashes <- vapply(review$sources, `[[`, "", "sha256")
  reference$generation <- digest::digest(reference, algo = "sha256")
  validate_published_open_coast_reference(reference)
  source(here::here("scripts", "R", "utils", "dataset_publication_utils.R"), local = TRUE)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  stage <- tempfile(".reference-", tmpdir = dirname(output_path))
  on.exit(unlink(stage), add = TRUE)
  saveRDS(reference, stage)
  publish_validated_file(stage, output_path, function(path) validate_published_open_coast_reference(readRDS(path)))
  invisible(reference)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 2L) stop("Supply the reviewed candidate RDS and matching geography-review JSON. Unreviewed candidates cannot be published.", call. = FALSE)
  publish_open_coast_reference(args[[1L]], args[[2L]])
}
