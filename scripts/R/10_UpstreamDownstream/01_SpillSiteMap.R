#!/usr/bin/env Rscript

# =============================================================================
# Export unique_spill_sites.parquet to GIS formats (GeoPackage + Shapefile)
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(sf)
  library(here)
  library(dplyr)
})

sf::sf_use_s2(FALSE)
CRS_BNG <- 27700

# -------------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------------
path_spill <- here("data", "processed", "unique_spill_sites.parquet")

out_gpkg_dir <- here("data", "processed")
out_gpkg     <- file.path(out_gpkg_dir, "unique_spill_sites.gpkg")

out_shp_dir  <- here("data", "processed", "shapefiles")
dir.create(out_shp_dir, recursive = TRUE, showWarnings = FALSE)
out_shp      <- file.path(out_shp_dir, "unique_spill_sites.shp")

cat("Reading spills from:\n  ", path_spill, "\n", sep = "")
spills <- arrow::read_parquet(path_spill)

cat("Rows in spills: ", nrow(spills), "\n", sep = "")

# -------------------------------------------------------------------------
# Build sf object (assumes British National Grid easting/northing)
# -------------------------------------------------------------------------
if (!all(c("easting", "northing") %in% names(spills))) {
  stop("Columns 'easting' and 'northing' not found in spills data.")
}

spills_sf <- st_as_sf(
  spills,
  coords = c("easting", "northing"),
  crs    = CRS_BNG,
  remove = FALSE  # keep original x/y columns
)

cat("CRS set to EPSG:", CRS_BNG, " (OSGB36 / British National Grid)\n")

# -------------------------------------------------------------------------
# Write GeoPackage (recommended for QGIS)
# -------------------------------------------------------------------------
cat("Writing GeoPackage to:\n  ", out_gpkg, "\n", sep = "")
st_write(spills_sf, out_gpkg,
         layer        = "unique_spill_sites",
         delete_layer = TRUE,
         quiet        = TRUE)

# -------------------------------------------------------------------------
# Also write ESRI Shapefile (if you really want .shp)
# -------------------------------------------------------------------------
cat("Writing Shapefile to:\n  ", out_shp, "\n", sep = "")
st_write(spills_sf, out_shp,
         delete_layer = TRUE,
         quiet        = TRUE)

cat("\nDone.\n")
cat("Load either of these in QGIS:\n")
cat("  - GeoPackage: ", out_gpkg, " (layer: unique_spill_sites)\n", sep = "")
cat("  - Shapefile : ", out_shp, "\n", sep = "")
