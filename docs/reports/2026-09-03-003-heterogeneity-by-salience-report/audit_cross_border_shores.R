# Country labels audit unrestricted nearest shores; they never filter the reference.
source('docs/reports/2026-09-03-003-heterogeneity-by-salience-report/report_storage.R')
env <- new.env(parent = globalenv())
sys.source(here::here('scripts','R','03_data_enrichment','build_site_group_characteristics.R'),env)
projection <- env$read_site_group_projection(env$CONFIG$crosswalk_path, years=env$YEARS)
projection <- projection[is.finite(projection$easting) & is.finite(projection$northing),]
points <- sf::st_as_sf(projection,coords=c('easting','northing'),crs=27700)
reference <- readRDS(env$CONFIG$open_coast_path)
nearest <- sf::st_nearest_feature(points,reference$retained)
xy <- do.call(rbind,lapply(sf::st_nearest_points(points,reference$retained[nearest,],pairwise=TRUE),function(x)tail(x,1)))
shore <- sf::st_as_sf(data.frame(x=xy[,1],y=xy[,2]),coords=c('x','y'),crs=27700)
nations <- sf::st_transform(sf::st_read(env$CONFIG$boundary_path,quiet=TRUE),27700)
country <- function(x) nations$CTRY24NM[sf::st_nearest_feature(x,nations)]
output <- data.frame(site_id=points$site_id,site_country=country(points),shore_country=country(shore),
 site_x=sf::st_coordinates(points)[,1],site_y=sf::st_coordinates(points)[,2],shore_x=xy[,1],shore_y=xy[,2],
 distance_m=as.numeric(sf::st_distance(points,shore,by_element=TRUE)))
output <- output[output$site_country!=output$shore_country,]
summary <- dplyr::count(output, .data$site_country, .data$shore_country, name = "n_sites")
examples <- output |> dplyr::group_by(.data$site_country, .data$shore_country) |>
  dplyr::slice_min(.data$distance_m, n = 3L, with_ties = FALSE) |> dplyr::ungroup()
root <- file.path(salience_report_root(), "geometry-alignment-50-sepa")
write.csv(summary, file.path(root, "cross_border_summary.csv"), row.names = FALSE)
write.csv(examples, file.path(root, "cross_border_examples.csv"), row.names = FALSE)
jsonlite::write_json(list(status = "complete", geometry_generation = reference$generation,
  reference_sha256 = salience_file_hash(env$CONFIG$open_coast_path),
  location_hash = env$open_coast_location_hash(env$read_site_group_projection(env$CONFIG$crosswalk_path, years=env$YEARS)),
  country_boundary_sha256 = salience_file_hash(env$CONFIG$boundary_path),
  method = "Country labels use the nearest UK Nations polygon to each point; they never constrain shoreline selection or distance measurement.",
  artifacts = lapply(c("cross_border_summary.csv", "cross_border_examples.csv"), function(name)
    list(path = name, sha256 = salience_file_hash(file.path(root, name))))),
  file.path(root, "cross_border_manifest.json"), auto_unbox = TRUE, pretty = TRUE)
print(summary)
print(examples)
