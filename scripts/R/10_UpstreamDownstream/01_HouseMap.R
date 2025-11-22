#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
  library(sf)
})

sf::sf_use_s2(FALSE)
CRS_BNG <- 27700

path_sales <- here("data", "processed", "house_price.parquet")
path_rent  <- here("data", "processed", "zoopla", "zoopla_rentals.parquet")

out_dir <- here("data", "processed", "shapefiles")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Sales → GeoPackage
sales  <- arrow::read_parquet(path_sales)

sales_sf <- sales |>
  filter(is.finite(easting), is.finite(northing)) |>
  st_as_sf(coords = c("easting", "northing"), crs = CRS_BNG)

out_sales <- file.path(out_dir, "sales_points.gpkg")
st_write(sales_sf, out_sales, layer = "sales_points",
         driver = "GPKG", delete_layer = TRUE)

# Rentals → GeoPackage
rentals <- arrow::read_parquet(path_rent)

rentals_sf <- rentals |>
  filter(is.finite(easting), is.finite(northing)) |>
  st_as_sf(coords = c("easting", "northing"), crs = CRS_BNG)

out_rent <- file.path(out_dir, "rentals_points.gpkg")
st_write(rentals_sf, out_rent, layer = "rentals_points",
         driver = "GPKG", delete_layer = TRUE)
