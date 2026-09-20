source(here::here("scripts", "R", "03_data_enrichment", "build_open_coast_reference.R"))
line <- function(x) sf::st_linestring(matrix(x, ncol = 2, byrow = TRUE))
box <- function(xmin, ymin, xmax, ymax) sf::st_polygon(list(matrix(
  c(xmin,ymin,xmax,ymin,xmax,ymax,xmin,ymax,xmin,ymin), ncol = 2, byrow = TRUE)))
shore <- sf::st_sf(source_id = c("bay_and_estuary", "opposite_shore"),
  geometry = sf::st_sfc(line(c(0,0,10,0)), line(c(0,10,10,10)), crs = 27700))
coastal <- sf::st_sf(water_id = "official_coastal_bay",
  geometry = sf::st_sfc(box(-1,-1,5,11), crs = 27700))
transitional <- sf::st_sf(water_id = "estuary",
  geometry = sf::st_sfc(box(5,-1,11,11), crs = 27700))
reference <- split_open_coast_shoreline(shore, coastal, transitional, alignment_m = 0)
stopifnot(abs(sum(as.numeric(sf::st_length(reference$retained))) - 10) < 1e-8,
          abs(sum(as.numeric(sf::st_length(reference$excluded))) - 10) < 1e-8,
          nrow(reference$unresolved) == 0L,
          all(reference$retained$source_id %in% shore$source_id))
# Nearest point is a physical segment interior; polygon offshore/mouth edges are absent.
points <- sf::st_sf(site_id = 1:2, geometry = sf::st_sfc(sf::st_point(c(2,2)),
                                                        sf::st_point(c(2,8)), crs = 27700))
coverage <- sf::st_sf(geometry = sf::st_sfc(box(-20,-20,20,20), crs = 27700))
distances <- measure_open_coast_evidence(points, reference, coverage)
stopifnot(identical(distances$distance_to_open_coast_m, c(2,2)),
          all(distances$open_coast_status == "supported_candidate"))
# Unknown classification can invalidate a seemingly finite coast distance.
partial <- split_open_coast_shoreline(shore, coastal, transitional[FALSE, ], 0)
point_unknown <- sf::st_sf(site_id = 3L, geometry = sf::st_sfc(sf::st_point(c(9,1)), crs = 27700))
unknown <- measure_open_coast_evidence(point_unknown, partial, coverage)
stopifnot(is.na(unknown$distance_to_open_coast_m), unknown$open_coast_status == "unresolved_shore")
# A classification overlap is unresolved, never silently assigned to coast.
overlap <- split_open_coast_shoreline(shore, coastal, transitional, alignment_m = 1)
stopifnot(sum(as.numeric(sf::st_length(overlap$unresolved))) > 0)
bad <- tryCatch(split_open_coast_shoreline(sf::st_set_crs(shore, NA), coastal, transitional, 0), error = identity)
stopifnot(inherits(bad, "error"))
empty <- tryCatch(split_open_coast_shoreline(shore, coastal[FALSE, ], transitional, 0), error = identity)
stopifnot(inherits(empty, "error"))
# Unclassified tiles remain unresolved, and missing physical coverage is fatal.
unclassified <- split_open_coast_shoreline(shore, coastal[FALSE, ], transitional[FALSE, ],
                                           0, allow_empty_tile = TRUE)
stopifnot(nrow(unclassified$retained) == 0L,
          abs(sum(as.numeric(sf::st_length(unclassified$unresolved))) - 20) < 1e-8)
absent <- tryCatch(measure_open_coast_evidence(points, reference, coverage[FALSE, ]), error = identity)
stopifnot(inherits(absent, "error"))
outside <- measure_open_coast_evidence(points, reference,
  sf::st_sf(geometry = sf::st_sfc(box(1,1,3,9), crs=27700)))
stopifnot(all(outside$open_coast_status == "outside_coverage"),
          all(is.na(outside$distance_to_open_coast_m)))
again <- split_open_coast_shoreline(shore, coastal, transitional, alignment_m = 0)
stopifnot(identical(sf::st_as_binary(sf::st_geometry(reference$retained)),
                    sf::st_as_binary(sf::st_geometry(again$retained))))
cat("Open-coast reference fixture contracts passed; production geography is a separate gate.\n")
