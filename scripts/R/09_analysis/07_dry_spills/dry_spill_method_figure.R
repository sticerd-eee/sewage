# ==============================================================================
# Dry-Spill Method Figure
# ==============================================================================
#
# Purpose: Visualise the preferred dry-spill methodology using one real
#          spill event. Produces two standalone figures: a local site map
#          and the 3 x 3 rainfall neighbourhood used in classification.
#
# Author: Jacopo Olivieri
# Date: 2026-03-09
#
# Inputs:
#   - data/processed/unique_spill_sites.parquet
#   - data/processed/rainfall/spill_site_grid_lookup.parquet
#   - data/processed/rainfall/rainfall_data_cleaned.parquet
#   - data/processed/rainfall/spill_blocks_rainfall_yr.parquet
#   - data/raw/haduk_rainfall_data/rainfall_YYYY_MM.nc
#   - data/raw/rivers/OSRivers_shapefile/WatercourseLink.shp
#
# Outputs:
#   - output/figures/dry_spill_method_figure_a.pdf
#   - output/figures/dry_spill_method_figure_b.pdf
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
TARGET_SITE_ID <- 8001L
TARGET_WATER_COMPANY <- "Thames Water"
TARGET_YEAR <- 2023L
TARGET_BLOCK_START <- "2023-02-09 05:46:00"
DRY_THRESHOLD_MM <- 0.25
MAP_PADDING_LEFT_M <- 1100
MAP_PADDING_RIGHT_M <- 1100
MAP_PADDING_TOP_M <- 1100
MAP_PADDING_BOTTOM_M <- 1700
PANEL_A_WIDTH <- 10.1
PANEL_B_WIDTH <- 9.9
PLOT_HEIGHT <- 10
PLOT_DPI <- 300
STREETMAP_PROVIDER <- "CartoDB.Positron"
STREETMAP_ZOOM <- 13L
MAP_ATTRIBUTION <- "Basemap: CARTO Positron, OpenStreetMap contributors."
RIVER_COLOR <- "#4C78A8"
SITE_COLOR <- "#B63679"
GRID_BORDER <- "#595959"


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "dplyr",
  "ggspatial",
  "ggplot2",
  "here",
  "maptiles",
  "ncdf4",
  "scales",
  "showtext",
  "sf",
  "sysfonts",
  "terra",
  "tidyr"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}

install_if_missing(required_packages)


# ==============================================================================
# 3. Setup
# ==============================================================================

# 3.1 Font Setup ---------------------------------------------------------------
showtext::showtext_auto()
showtext::showtext_opts(dpi = PLOT_DPI)
home_font_dir <- file.path(Sys.getenv("HOME"), "Library", "Fonts")
sysfonts::font_add(
  family = "libertinus",
  regular = file.path(home_font_dir, "LibertinusSerif-Regular.ttf"),
  bold = file.path(home_font_dir, "LibertinusSerif-Bold.ttf"),
  italic = file.path(home_font_dir, "LibertinusSerif-Italic.ttf"),
  bolditalic = file.path(home_font_dir, "LibertinusSerif-BoldItalic.ttf")
)
FONT_FAMILY <- "libertinus"

# 3.2 Output Directory ---------------------------------------------------------

output_dir <- here::here("output", "figures")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 3.3 Publication-Quality Themes ----------------------------------------------
theme_map_publication <- function() {
  theme_void(base_family = FONT_FAMILY) +
    theme(
      plot.caption = element_text(
        size = 7,
        color = "#4A4A4A",
        hjust = 0,
        margin = margin(t = 6)
      ),
      plot.caption.position = "plot",
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA),
      legend.position = "none",
      plot.margin = margin(t = 5, r = 8, b = 5, l = 5)
    )
}

theme_pref <- theme_minimal(base_family = FONT_FAMILY) +
  theme(
    text = element_text(size = 9, family = FONT_FAMILY),
    axis.title = element_blank(),
    axis.text = element_blank(),
    axis.ticks = element_blank(),
    panel.grid = element_blank(),
    plot.subtitle = element_text(
      size = 10,
      family = FONT_FAMILY,
      margin = margin(b = 8)
    ),
    strip.text = element_text(
      face = "bold",
      size = 9,
      family = FONT_FAMILY
    ),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.title.position = "top",
    legend.title = element_text(
      face = "bold",
      size = 10,
      family = FONT_FAMILY,
      margin = margin(b = 4)
    ),
    legend.text = element_text(
      size = 9,
      family = FONT_FAMILY
    ),
    legend.background = element_rect(fill = "white", color = NA),
    legend.key.width = grid::unit(1.5, "cm"),
    legend.key.height = grid::unit(0.35, "cm"),
    legend.margin = margin(t = 2),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    plot.margin = margin(t = 5, r = 5, b = 5, l = 8)
  )


# ==============================================================================
# 4. Helper Functions
# ==============================================================================
load_target_event <- function() {
  site_data <- arrow::read_parquet(
    here::here("data", "processed", "unique_spill_sites.parquet")
  ) |>
    dplyr::filter(
      .data$site_id == TARGET_SITE_ID,
      .data$water_company == TARGET_WATER_COMPANY
    )

  if (nrow(site_data) != 1) {
    stop("Expected exactly one matching site row.")
  }

  block_candidates <- arrow::read_parquet(
    here::here("data", "processed", "rainfall", "spill_blocks_rainfall_yr.parquet")
  ) |>
    dplyr::filter(
      .data$site_id == TARGET_SITE_ID,
      .data$year == TARGET_YEAR
    )

  block_data <- block_candidates |>
    dplyr::mutate(
      block_start_chr = format(.data$block_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")
    ) |>
    dplyr::filter(.data$block_start_chr == TARGET_BLOCK_START) |>
    dplyr::select(-"block_start_chr")

  if (nrow(block_data) != 1) {
    stop("Expected exactly one matching spill block.")
  }

  list(site = site_data, block = block_data)
}

load_site_lookup <- function(site_id, site_ngr) {
  lookup <- arrow::read_parquet(
    here::here("data", "processed", "rainfall", "spill_site_grid_lookup.parquet")
  ) |>
    dplyr::filter(
      .data$site_id == .env$site_id,
      .data$ngr == .env$site_ngr
    ) |>
    dplyr::arrange(.data$y_idx, .data$x_idx)

  if (nrow(lookup) != 9) {
    stop("Expected exactly nine lookup cells for the selected site and NGR.")
  }

  lookup
}

load_rainfall_for_event <- function(lookup, spill_date) {
  rainfall_dates <- c(spill_date - 1, spill_date)

  rainfall_data <- arrow::read_parquet(
    here::here("data", "processed", "rainfall", "rainfall_data_cleaned.parquet")
  ) |>
    dplyr::filter(
      .data$date >= rainfall_dates[1],
      .data$date <= rainfall_dates[2],
      .data$x_idx %in% lookup$x_idx,
      .data$y_idx %in% lookup$y_idx
    )

  lookup |>
    dplyr::select("x_idx", "y_idx", "is_center") |>
    dplyr::inner_join(rainfall_data, by = c("x_idx", "y_idx")) |>
    dplyr::mutate(
      day_label = dplyr::case_when(
        .data$date == spill_date - 1 ~ paste0(
          "Previous day\n",
          format(.data$date, "%d %b %Y")
        ),
        .data$date == spill_date ~ paste0(
          "Spill day\n",
          format(.data$date, "%d %b %Y")
        ),
        TRUE ~ as.character(.data$date)
      )
    ) |>
    dplyr::arrange(.data$date, .data$y_idx, .data$x_idx)
}

read_grid_bounds <- function(spill_date) {
  rainfall_file <- sprintf(
    "rainfall_%s.nc",
    format(spill_date, "%Y_%m")
  )
  rainfall_path <- here::here("data", "raw", "haduk_rainfall_data", rainfall_file)

  if (!file.exists(rainfall_path)) {
    stop("Missing rainfall NetCDF file for selected spill month: ", rainfall_file)
  }

  nc <- ncdf4::nc_open(
    rainfall_path
  )
  on.exit(ncdf4::nc_close(nc), add = TRUE)

  list(
    x_bounds = ncdf4::ncvar_get(nc, "projection_x_coordinate_bnds"),
    y_bounds = ncdf4::ncvar_get(nc, "projection_y_coordinate_bnds")
  )
}

build_grid_polygons <- function(lookup, bounds) {
  polygons <- lapply(seq_len(nrow(lookup)), function(i) {
    cell <- lookup[i, ]
    x_min <- bounds$x_bounds[1, cell$x_idx]
    x_max <- bounds$x_bounds[2, cell$x_idx]
    y_min <- bounds$y_bounds[1, cell$y_idx]
    y_max <- bounds$y_bounds[2, cell$y_idx]

    sf::st_polygon(list(matrix(
      c(
        x_min, y_min,
        x_max, y_min,
        x_max, y_max,
        x_min, y_max,
        x_min, y_min
      ),
      ncol = 2,
      byrow = TRUE
    )))
  })

  sf::st_sf(
    lookup,
    geometry = sf::st_sfc(polygons, crs = 27700)
  )
}

make_map_bbox <- function(grid_sf) {
  box <- sf::st_bbox(grid_sf)
  box["xmin"] <- unname(box["xmin"] - MAP_PADDING_LEFT_M)
  box["ymin"] <- unname(box["ymin"] - MAP_PADDING_BOTTOM_M)
  box["xmax"] <- unname(box["xmax"] + MAP_PADDING_RIGHT_M)
  box["ymax"] <- unname(box["ymax"] + MAP_PADDING_TOP_M)
  box
}

load_local_rivers <- function(map_bbox) {
  sf::st_read(
    here::here("data", "raw", "rivers", "OSRivers_shapefile", "WatercourseLink.shp"),
    quiet = TRUE
  ) |>
    sf::st_transform(27700) |>
    sf::st_crop(map_bbox)
}

load_street_basemap <- function(map_bbox) {
  bbox_sf <- sf::st_as_sfc(map_bbox)

  street_raster <- maptiles::get_tiles(
    bbox_sf,
    provider = STREETMAP_PROVIDER,
    zoom = STREETMAP_ZOOM,
    crop = TRUE,
    project = TRUE,
    verbose = FALSE
  )

  street_df <- terra::as.data.frame(street_raster, xy = TRUE, na.rm = TRUE)
  names(street_df)[3:5] <- c("red", "green", "blue")

  street_df |>
    dplyr::mutate(
      fill = grDevices::rgb(
        .data$red,
        .data$green,
        .data$blue,
        maxColorValue = 255
      )
    )
}

build_panel_a <- function(street_df, grid_sf, site_sf, rivers_sf, map_bbox) {
  center_cell <- dplyr::filter(grid_sf, .data$is_center)
  site_coords <- sf::st_coordinates(site_sf)
  label_x <- site_coords[1, "X"] + 475
  label_y <- site_coords[1, "Y"] + 700

  ggplot() +
    geom_raster(
      data = street_df,
      aes(x = .data$x, y = .data$y, fill = .data$fill),
      interpolate = TRUE
    ) +
    scale_fill_identity(guide = "none") +
    geom_sf(
      data = rivers_sf,
      color = RIVER_COLOR,
      linewidth = 0.65,
      alpha = 0.95,
      inherit.aes = FALSE
    ) +
    geom_sf(
      data = grid_sf,
      fill = scales::alpha("white", 0.20),
      color = GRID_BORDER,
      linewidth = 0.42,
      inherit.aes = FALSE
    ) +
    geom_sf(
      data = center_cell,
      fill = scales::alpha(SITE_COLOR, 0.10),
      color = SITE_COLOR,
      linewidth = 0.7,
      inherit.aes = FALSE
    ) +
    geom_segment(
      aes(
        x = site_coords[1, "X"] + 120,
        y = site_coords[1, "Y"] + 25,
        xend = label_x - 45,
        yend = label_y - 20
      ),
      color = SITE_COLOR,
      linewidth = 0.25
    ) +
    geom_sf(
      data = site_sf,
      color = SITE_COLOR,
      fill = "white",
      shape = 21,
      stroke = 0.55,
      size = 3,
      inherit.aes = FALSE
    ) +
    annotate(
      "label",
      x = label_x,
      y = label_y,
      label = "Overflow site",
      hjust = 0,
      label.padding = grid::unit(0.15, "lines"),
      linewidth = 0.15,
      fill = scales::alpha("white", 0.88),
      color = "#1F1F1F",
      family = FONT_FAMILY,
      size = 3.2
    ) +
    ggspatial::annotation_scale(
      location = "bl",
      text_family = FONT_FAMILY,
      text_cex = 0.8,
      line_width = 0.6,
      pad_x = grid::unit(0.3, "cm"),
      pad_y = grid::unit(0.3, "cm")
    ) +
    labs(
      caption = MAP_ATTRIBUTION
    ) +
    coord_sf(
      xlim = c(map_bbox["xmin"], map_bbox["xmax"]),
      ylim = c(map_bbox["ymin"], map_bbox["ymax"]),
      expand = FALSE,
      datum = NA
    ) +
    theme_map_publication()
}

build_panel_b <- function(grid_sf, rainfall_by_day) {
  rainfall_labels <- rainfall_by_day |>
    dplyr::mutate(
      rainfall_label = sprintf("%.2f", .data$rainfall)
    ) |>
    dplyr::group_by(.data$date) |>
    dplyr::mutate(
      is_event_max = .data$rainfall == max(rainfall_by_day$rainfall, na.rm = TRUE)
    ) |>
    dplyr::ungroup()

  panel_data <- grid_sf |>
    dplyr::select("x_idx", "y_idx", "is_center", geometry) |>
    dplyr::left_join(
      rainfall_labels,
      by = c("x_idx", "y_idx", "is_center")
    ) |>
    dplyr::mutate(
      day_label = factor(
        .data$day_label,
        levels = unique(rainfall_by_day$day_label)
      )
    )

  label_data <- sf::st_point_on_surface(panel_data)
  label_coords <- cbind(
    sf::st_drop_geometry(label_data),
    sf::st_coordinates(label_data)
  )

  ggplot(panel_data) +
    geom_sf(
      aes(fill = .data$rainfall),
      color = scales::alpha(GRID_BORDER, 0.85),
      linewidth = 0.32
    ) +
    geom_sf(
      data = dplyr::filter(panel_data, .data$is_event_max),
      fill = NA,
      color = SITE_COLOR,
      linewidth = 1.05
    ) +
    geom_text(
      data = label_coords,
      aes(x = .data$X, y = .data$Y, label = .data$rainfall_label),
      family = FONT_FAMILY,
      size = 3,
      fontface = "bold",
      inherit.aes = FALSE
    ) +
    facet_wrap(~ day_label, ncol = 2) +
    scale_fill_gradientn(
      colors = c("#F7FBFF", "#C6DBEF", "#6BAED6", "#2171B5"),
      limits = c(0, DRY_THRESHOLD_MM),
      breaks = c(0, 0.10, 0.20, 0.25),
      labels = function(x) sprintf("%.2f", x),
      name = "Rainfall (mm)"
    ) +
    labs() +
    coord_sf(expand = FALSE) +
    theme_pref +
    theme(
      panel.spacing = grid::unit(0.35, "cm"),
      panel.background = element_rect(fill = "white", color = NA)
    )
}


# ==============================================================================
# 5. Data Preparation
# ==============================================================================
cat("Loading target event...\n")

target <- load_target_event()
site_row <- target$site
block_row <- target$block
spill_date <- as.Date(block_row$block_start)

site_id <- site_row$site_id[[1]]
site_ngr <- site_row$ngr[[1]]
site_point <- sf::st_as_sf(
  site_row |>
    dplyr::select("site_id", "water_company", "ngr", "easting", "northing"),
  coords = c("easting", "northing"),
  crs = 27700
)

lookup <- load_site_lookup(site_id, site_ngr)
rainfall_by_day <- load_rainfall_for_event(lookup, spill_date)

event_max <- max(rainfall_by_day$rainfall, na.rm = TRUE)

if (abs(event_max - block_row$rainfall_max_9cell_d01_na_rm[[1]]) > 1e-8) {
  stop("Reconstructed rainfall maximum does not match stored block classification.")
}

grid_bounds <- read_grid_bounds(spill_date)
grid_sf <- build_grid_polygons(lookup, grid_bounds)
map_bbox <- make_map_bbox(grid_sf)
rivers_sf <- load_local_rivers(map_bbox)
street_df <- load_street_basemap(map_bbox)

cat(sprintf("Selected event max rainfall: %.5f mm\n", event_max))


# ==============================================================================
# 6. Plot Construction
# ==============================================================================
cat("Building figure panels...\n")

panel_a <- build_panel_a(
  street_df,
  grid_sf,
  site_point,
  rivers_sf,
  map_bbox
)
panel_b <- build_panel_b(grid_sf, rainfall_by_day)


# ==============================================================================
# 7. Save Figures
# ==============================================================================
cat("Saving figures...\n")

file_name_panel_a <- "dry_spill_method_figure_a.pdf"
ggsave(
  filename = file.path(output_dir, file_name_panel_a),
  plot = panel_a,
  width = PANEL_A_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm",
  device = cairo_pdf
)
cat("  Saved:", file_name_panel_a, "\n")

file_name_panel_b <- "dry_spill_method_figure_b.pdf"
ggsave(
  filename = file.path(output_dir, file_name_panel_b),
  plot = panel_b,
  width = PANEL_B_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm",
  device = cairo_pdf
)
cat("  Saved:", file_name_panel_b, "\n")

cat("\nAll figures generated successfully!\n")
