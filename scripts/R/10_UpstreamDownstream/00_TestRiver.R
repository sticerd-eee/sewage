#!/usr/bin/env Rscript

# ============================================================
# STATIC MAP: rivers + OS shapefiles + spill sites
# No leaflet, no htmlwidgets — just ggplot2
# ============================================================

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(sf)
  library(ggplot2)
})

sf::sf_use_s2(FALSE)
CRS_BNG <- 27700

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
ROOT <- "/Users/odran/Dropbox/sewage"

path_spill       <- file.path(ROOT, "data/processed/unique_spill_sites.parquet")
path_flow        <- file.path(ROOT, "data/processed/Rivers/River_Flow.parquet")
path_hydronode   <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/HydroNode.shp")
path_watercourse <- file.path(ROOT, "data/raw/rivers/oprvrs_essh_gb/data/WatercourseLink.shp")

stopifnot(
  "Spill sites parquet not found" = file.exists(path_spill),
  "River flow parquet not found"  = file.exists(path_flow),
  "HydroNode.shp not found"       = file.exists(path_hydronode),
  "WatercourseLink.shp not found" = file.exists(path_watercourse)
)

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
spills <- arrow::read_parquet(path_spill)

river_fl <- arrow::read_parquet(
  path_flow,
  col_select = c("easting", "northing", "flow_easting", "flow_northing")
)

hydro_nodes_bng <- sf::st_read(path_hydronode, quiet = TRUE)
watercourse_bng <- sf::st_read(path_watercourse, quiet = TRUE)

cat("Rows — spills:", nrow(spills),
    " river_fl:", nrow(river_fl),
    " HydroNode:", nrow(hydro_nodes_bng),
    " WatercourseLink:", nrow(watercourse_bng), "\n")

# ------------------------------------------------------------
# Build sf objects in BNG
# ------------------------------------------------------------

# River_Flow segments -> lines
river_segments <- river_fl %>%
  filter(
    is.finite(easting), is.finite(northing),
    is.finite(flow_easting), is.finite(flow_northing)
  )

river_line_geoms <- lapply(seq_len(nrow(river_segments)), function(i) {
  mat <- matrix(
    c(
      river_segments$easting[i],      river_segments$northing[i],
      river_segments$flow_easting[i], river_segments$flow_northing[i]
    ),
    ncol = 2, byrow = TRUE
  )
  sf::st_linestring(mat)
})

river_sf_bng <- sf::st_as_sf(
  river_segments,
  geometry = sf::st_sfc(river_line_geoms, crs = CRS_BNG)
)

# Spill sites as points
spill_pts_bng <- sf::st_as_sf(
  spills,
  coords = c("easting", "northing"),
  crs = CRS_BNG
)

# ------------------------------------------------------------
# Crop to spills’ bounding box (+ small buffer)
# ------------------------------------------------------------
bb_spill <- sf::st_bbox(spill_pts_bng)
buf <- 5000  # 5 km padding for context

bb_expanded <- bb_spill
bb_expanded["xmin"] <- bb_spill["xmin"] - buf
bb_expanded["xmax"] <- bb_spill["xmax"] + buf
bb_expanded["ymin"] <- bb_spill["ymin"] - buf
bb_expanded["ymax"] <- bb_spill["ymax"] + buf

bbox_poly <- sf::st_as_sfc(bb_expanded)

watercourse_crop <- suppressWarnings(sf::st_intersection(watercourse_bng, bbox_poly))
hydro_nodes_crop <- suppressWarnings(sf::st_intersection(hydro_nodes_bng, bbox_poly))
river_sf_crop    <- suppressWarnings(sf::st_intersection(river_sf_bng,    bbox_poly))

cat("After crop —",
    " river_sf:", nrow(river_sf_crop),
    " Watercourse:", nrow(watercourse_crop),
    " HydroNode:", nrow(hydro_nodes_crop), "\n")

# ------------------------------------------------------------
# Plot
# ------------------------------------------------------------

p <- ggplot() +
  # OS watercourses
  geom_sf(data = watercourse_crop, color = "cyan", linewidth = 0.2, alpha = 0.6) +
  # River_Flow segments
  geom_sf(data = river_sf_crop, color = "blue", linewidth = 0.3, alpha = 0.7) +
  # Hydro nodes
  geom_sf(data = hydro_nodes_crop, color = "black", size = 0.3, alpha = 0.7) +
  # Spills
  geom_sf(data = spill_pts_bng, color = "red", size = 0.6, alpha = 0.8) +
  coord_sf(crs = sf::st_crs(CRS_BNG), xlim = c(bb_expanded["xmin"], bb_expanded["xmax"]),
           ylim = c(bb_expanded["ymin"], bb_expanded["ymax"])) +
  theme_void() +
  ggtitle("Rivers (blue/cyan), HydroNodes (black), Spill sites (red)")

print(p)  # should show in VSCode plot pane

# ------------------------------------------------------------
# Save PNG as well
# ------------------------------------------------------------
out_dir  <- file.path(ROOT, "output", "figures")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_file <- file.path(out_dir, "rivers_spills_static.png")

ggsave(out_file, p, width = 10, height = 8, dpi = 300)
cat("Static map saved to:\n  ", out_file, "\n", sep = "")
