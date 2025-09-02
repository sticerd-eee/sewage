#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import rasterio
from pathlib import Path

# === Load shapefile as basemap (use Nations file that exists)
shapefile_path = Path("/Users/odran/Dropbox/sewage/data/raw/shapefiles/UK_Nations") / "CTRY_DEC_2024_UK_BGC.shp"
gdf_basemap = gpd.read_file(shapefile_path)

# === Load flow table
df = pd.read_parquet("/Users/odran/Dropbox/sewage/data/processed/Rivers/River_Flow.parquet")

# === Load transform (optional)
surf_path = Path("/Users/odran/Dropbox/sewage/data/raw/rivers/digimap_download_ceh_integrated_hydrological/ihdtm-2016_6024334/14jan16_SURF_0_000_000_700_1300.asc")
with rasterio.open(surf_path) as src:
    transform = src.transform

# === Drop rows without valid downstream pixel
df_valid = df.dropna(subset=["flow_easting", "flow_northing"]).copy()

# === Compute flow arrow deltas
dx = df_valid["flow_easting"].values - df_valid["easting"].values
dy = df_valid["flow_northing"].values - df_valid["northing"].values

# === Plot
fig, ax = plt.subplots(figsize=(14, 16))

# Plot basemap in light gray
gdf_basemap.plot(ax=ax, facecolor='none', edgecolor='lightgray', linewidth=0.5)

# Overlay river flow arrows in blue
ax.quiver(
    df_valid["easting"], df_valid["northing"],
    dx, dy,
    color="blue",  # Change arrows to blue
    angles="xy", scale_units="xy", scale=1,
    width=0.0008, headwidth=4, headlength=4, headaxislength=3
)

ax.set_title("River Flow Arrows over UK Basemap", fontsize=16)
ax.set_xlabel("Easting (m)")
ax.set_ylabel("Northing (m)")
ax.set_aspect("equal")
ax.grid(True)

plt.tight_layout()
plt.show()
