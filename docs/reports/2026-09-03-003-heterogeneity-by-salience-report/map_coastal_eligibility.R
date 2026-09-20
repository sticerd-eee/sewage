# Build report assets explicitly; rendering reads these files without live data.
map_coastal_eligibility <- function() {
  builder <- new.env(parent = globalenv())
  sys.source(here::here("scripts", "R", "03_data_enrichment", "build_site_group_characteristics.R"), builder)
  sites <- arrow::read_parquet(builder$CONFIG$output_path)
  locations <- builder$read_site_group_projection(builder$CONFIG$crosswalk_path, years = builder$YEARS)
  stopifnot(!anyDuplicated(locations$site_id), !anyDuplicated(sites$site_id),
    setequal(sites$site_id, locations$site_id))
  sites <- dplyr::left_join(sites, locations, by = "site_id", relationship = "one-to-one")
  located <- is.finite(sites$easting) & is.finite(sites$northing)
  stopifnot(all(is.finite(sites$distance_to_coast_m[located])))
  points <- sf::st_as_sf(sites[located, ], coords = c("easting", "northing"), crs = 27700)
  countries <- sf::st_read(builder$CONFIG$boundary_path, quiet = TRUE) |> sf::st_transform(27700)
  # Country labels set the map extent only; they never restrict coast distances.
  points$country <- countries$CTRY24NM[sf::st_nearest_feature(points, countries)]
  points$category <- ifelse(points$distance_to_coast_m <= 2000, "coastal", "inland")
  london_path <- here::here("data", "raw", "shapefiles", "local_authorities_uk_buc",
    "Local_Authority_Districts_(May_2025)_Boundaries_UK_BUC.shp")
  london_lads <- sf::st_read(london_path, quiet = TRUE) |>
    dplyr::filter(startsWith(.data$LAD25CD, "E090")) |>
    sf::st_transform(27700)
  stopifnot(nrow(london_lads) == 33L, !anyDuplicated(london_lads$LAD25CD))
  london <- sf::st_sf(geometry = sf::st_union(sf::st_make_valid(london_lads)))
  in_london <- lengths(sf::st_intersects(points, london)) > 0L
  points$category[points$category == "coastal" & in_london] <- "coastal_london"
  scope <- points |> dplyr::filter(.data$country %in% c("England", "Wales"))
  counts <- scope |> sf::st_drop_geometry() |> dplyr::count(.data$country, .data$category, name = "n_sites")
  n <- function(category) sum(counts$n_sites[counts$category == category])
  stopifnot(n("coastal") == 2917L, n("coastal_london") == 83L, n("inland") == 10968L,
    sum(!located) == 21L, nrow(points) - nrow(scope) == 1L)
  bounds <- sf::st_bbox(countries[countries$CTRY24NM %in% c("England", "Wales"), ])
  crop <- function(layer) suppressWarnings(sf::st_crop(layer, bounds))
  backdrop <- crop(countries) |> sf::st_simplify(dTolerance = 100)
  labels <- sf::st_as_sf(data.frame(name = c("London", "Bristol", "Liverpool", "Newcastle"),
    x = c(530000, 359000, 334000, 425000), y = c(180000, 173000, 390000, 564000)),
    coords = c("x", "y"), crs = 27700)
  base <- ggplot2::ggplot() +
    ggplot2::geom_sf(data = backdrop, fill = "#f4f2ed", colour = "#aaa79e", linewidth = .25) +
    ggplot2::coord_sf(xlim = bounds[c("xmin", "xmax")], ylim = bounds[c("ymin", "ymax")],
      expand = FALSE, datum = NA) +
    ggplot2::theme_void(base_size = 12) +
    ggplot2::theme(plot.title = ggplot2::element_text(face = "bold", size = 17),
      plot.subtitle = ggplot2::element_text(size = 11, margin = ggplot2::margin(b = 12)),
      plot.margin = ggplot2::margin(12, 18, 12, 18), legend.position = "bottom",
      legend.text = ggplot2::element_text(size = 10), legend.title = ggplot2::element_blank())
  city_layer <- list(ggplot2::geom_sf(data = labels, shape = 3, size = 1.4, colour = "#373b3e"),
    ggplot2::geom_sf_text(data = labels, ggplot2::aes(label = .data$name), nudge_x = 9500,
      nudge_y = 8500, hjust = 0, size = 3.2, colour = "#373b3e"))
  # Reproduce the original builder's actual reference, including tidal banks.
  # Dissolving GB first removes administrative country seams from the target.
  gb <- countries |> dplyr::filter(.data$CTRY24NM %in% c("England", "Wales", "Scotland"))
  dissolved <- sf::st_sf(geometry = sf::st_union(sf::st_geometry(gb)))
  stopifnot(!builder$geometry_has_interior_rings(dissolved))
  coast <- sf::st_boundary(dissolved)
  shore <- base + ggplot2::geom_sf(data = crop(coast),
    ggplot2::aes(colour = "coast"), linewidth = .45, key_glyph = "path") +
    ggplot2::scale_colour_manual(values = c(coast = "#007c91"),
      labels = "Included: original tidal coastline") + city_layer +
    ggplot2::labs(title = "1. Original shoreline used for distance",
      subtitle = "England and Wales | includes estuarine and tidal-river banks")
  map <- base +
    ggplot2::geom_sf(data = london, fill = "#f1e8f7", colour = "#773b9b",
      linewidth = .4, linetype = "dashed") +
    ggplot2::geom_sf(data = scope[scope$category == "inland", ],
      ggplot2::aes(colour = .data$category), size = .32, alpha = .3) +
    ggplot2::geom_sf(data = scope[scope$category == "coastal", ],
      ggplot2::aes(colour = .data$category), size = .75, alpha = .85) +
    ggplot2::geom_sf(data = scope[scope$category == "coastal_london", ],
      ggplot2::aes(colour = .data$category), size = 1.1, alpha = .95) +
    ggplot2::scale_colour_manual(values = c(coastal = "#007c91", coastal_london = "#773b9b", inland = "#b7babd"),
      breaks = c("coastal", "coastal_london", "inland"), labels = c(
        sprintf("Coastal outside Greater London (%s)", format(n("coastal"), big.mark = ",")),
        sprintf("Coastal inside Greater London (%s)", format(n("coastal_london"), big.mark = ",")),
        sprintf("Inland, above 2 km (%s)", format(n("inland"), big.mark = ",")))) +
    ggplot2::guides(colour = ggplot2::guide_legend(ncol = 1,
      override.aes = list(size = 3, alpha = 1))) + city_layer +
    ggplot2::labs(title = "2. Original Coastal site classification",
      subtitle = "Unchanged saved tidal-coast distance, inclusive 2 km threshold")
  shore_grob <- ggplot2::ggplotGrob(shore)
  map_grob <- ggplot2::ggplotGrob(map)
  aligned_heights <- grid::unit.pmax(shore_grob$heights, map_grob$heights)
  shore_grob$heights <- map_grob$heights <- aligned_heights
  output <- here::here("docs", "reports", "2026-09-03-003-heterogeneity-by-salience-report",
    "original-coastal-eligibility")
  dir.create(output, recursive = TRUE, showWarnings = FALSE)
  draw <- function() {
    grid::grid.newpage()
    grid::pushViewport(grid::viewport(x = .25, y = .53, width = .5, height = .94))
    grid::grid.draw(shore_grob)
    grid::popViewport()
    grid::pushViewport(grid::viewport(x = .75, y = .53, width = .5, height = .94))
    grid::grid.draw(map_grob)
    grid::popViewport()
    grid::grid.text(sprintf("Dots are Site Groups, not fitted observations. %s sites lack coordinates; %s located site lies outside England/Wales and is not shown.", sum(!located), nrow(points) - nrow(scope)),
      x = .025, y = .04, just = "left", gp = grid::gpar(fontsize = 10, col = "#454545"))
    source_note <- "ONS GB boundary (2024); Greater London outline from 33 local authorities (2025). London colour marks CSO location; regressions exclude London properties, not CSOs directly."
    grid::grid.text(source_note,
      x = .025, y = .02, just = "left", gp = grid::gpar(fontsize = 8, col = "#666666"))
  }
  grDevices::png(file.path(output, "england-wales-coastal-eligibility.png"), width = 2400, height = 1800, res = 160)
  draw()
  grDevices::dev.off()
  grDevices::pdf(file.path(output, "england-wales-coastal-eligibility.pdf"), width = 15, height = 11.25, useDingbats = FALSE)
  draw()
  grDevices::dev.off()
  readr::write_csv(counts, file.path(output, "site-counts.csv"))
  artifacts <- c("england-wales-coastal-eligibility.png", "england-wales-coastal-eligibility.pdf", "site-counts.csv")
  hash <- function(path) digest::digest(file = path, algo = "sha256")
  boundary_hashes <- function(path) {
    paths <- paste0(tools::file_path_sans_ext(path), c(".shp", ".shx", ".dbf", ".prj"))
    stats::setNames(lapply(paths, hash), basename(paths))
  }
  jsonlite::write_json(list(status = "complete", profile = "legacy_tidal",
    coast_rule_m = 2000, missing_location = sum(!located),
    outside_map = nrow(points) - nrow(scope),
    coastal_outside_london = n("coastal"), coastal_inside_london = n("coastal_london"),
    inland = n("inland"),
    source_hashes = list(sites = hash(builder$CONFIG$output_path),
      crosswalk = hash(builder$CONFIG$crosswalk_path),
      boundary = boundary_hashes(builder$CONFIG$boundary_path),
      london_boundary = boundary_hashes(london_path)),
    artifacts = lapply(artifacts, function(path) list(path = path, sha256 = hash(file.path(output, path))))),
    file.path(output, "manifest.json"), auto_unbox = TRUE, pretty = TRUE)
  print(counts)
}

if (sys.nframe() == 0L) map_coastal_eligibility()
