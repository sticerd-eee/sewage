#!/usr/bin/env python3
"""
RiverNetwork_NoMask.py
Build a UK-wide river flow table directly from IHDTM rasters by reconstructing
pixel-center -> pixel-center edges from SURF/OUTF. No country masking.

Outputs:
  1) /Users/odran/Dropbox/sewage/data/processed/Rivers/River_Flow.parquet
     Columns: easting, northing, flow_easting, flow_northing  (float64)
  2) /Users/odran/Dropbox/sewage/data/processed/Rivers/River_Lines.parquet  (GeoParquet)
     Geometry: LINESTRING (EPSG:27700)
"""

from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
from rasterio.crs import CRS

# NEW: for writing LineStrings
import geopandas as gpd
from shapely.geometry import LineString

# ----------------------------
# Config
# ----------------------------
CRS_EPSG = 27700  # Assume BNG if raster has no CRS
ROOT = Path("/Users/odran/Dropbox/sewage")

raw_river = ROOT / "data" / "raw" / "rivers" / "digimap_download_ceh_integrated_hydrological" / "ihdtm-2016_6024334"
surf_path = raw_river / "14jan16_SURF_0_000_000_700_1300.asc"
outf_path = raw_river / "14jan16_OUTF_0_000_000_700_1300.asc"
hght_path = raw_river / "14jan16_HGHT_0_000_000_700_1300.asc"  # optional; used to sanity-check alignment

OUT_DIR  = ROOT / "data" / "processed" / "Rivers"
OUT_PARQ = OUT_DIR / "River_Flow.parquet"
OUT_GPQ  = OUT_DIR / "River_Lines.parquet"  # GeoParquet (lines)

# D8 offsets (row, col) corresponding to OUTF codes
OFFSETS = {
    1: (1, -1), 2: (1, 0), 3: (1, 1),
    4: (0, -1),           6: (0, 1),
    7: (-1, -1), 8: (-1, 0), 9: (-1, 1)
}
VALID_CODES = np.array(sorted(OFFSETS.keys()), dtype=np.int32)

def ts(msg: str) -> None:
    print(msg, flush=True)

def main():
    ts("▶ Reading IHDTM rasters (SURF, OUTF, HGHT) …")
    with rasterio.open(surf_path) as surf_src, \
         rasterio.open(outf_path) as outf_src, \
         rasterio.open(hght_path) as hght_src:

        # Determine raster CRS (assume BNG if missing)
        raster_crs = surf_src.crs if surf_src.crs is not None else CRS.from_epsg(CRS_EPSG)

        # Read arrays
        surf = surf_src.read(1)                 # river mask (4 = river)
        outf = outf_src.read(1).astype("int32") # D8 codes
        transform = surf_src.transform
        height, width = surf.shape

        # Sanity: ensure same shape for OUTF
        if outf.shape != surf.shape:
            raise RuntimeError("SURF and OUTF rasters have different shapes.")
        if hght_src.width != surf_src.width or hght_src.height != surf_src.height:
            ts("⚠ HGHT shape differs; continuing since it isn't required for output.")

        # A) Candidate river pixels: SURF == 4
        river = (surf == 4)

        # B) Coordinates (row, col) for source pixels
        r, c = np.where(river)
        if r.size == 0:
            raise RuntimeError("No river pixels found (SURF == 4).")

        # C) Keep only valid D8 codes at those sources
        codes = outf[r, c]
        valid = np.isin(codes, VALID_CODES)
        r = r[valid]; c = c[valid]; codes = codes[valid]
        if r.size == 0:
            raise RuntimeError("No valid D8 codes at river pixels.")

        # D) Map codes -> neighbor (r2, c2)
        dy_lut = np.zeros(10, dtype=np.int32); dx_lut = np.zeros(10, dtype=np.int32)
        for k, (dy, dx) in OFFSETS.items():
            dy_lut[k] = dy; dx_lut[k] = dx

        r2 = r + dy_lut[codes]
        c2 = c + dx_lut[codes]

        # E) Clip to in-bounds neighbors
        in_bounds = (r2 >= 0) & (r2 < height) & (c2 >= 0) & (c2 < width)
        r, c, r2, c2 = r[in_bounds], c[in_bounds], r2[in_bounds], c2[in_bounds]

        # F) Vectorised pixel-centre coordinates from affine transform
        # Affine is: X = a*col + b*row + c0 ; Y = d*col + e*row + f0
        a, b, c0, d, e, f0 = transform.a, transform.b, transform.c, transform.d, transform.e, transform.f
        # use pixel centres: (col+0.5, row+0.5)
        colf  = c.astype(np.float64)  + 0.5
        rowf  = r.astype(np.float64)  + 0.5
        colf2 = c2.astype(np.float64) + 0.5
        rowf2 = r2.astype(np.float64) + 0.5

        x1 = a*colf  + b*rowf  + c0
        y1 = d*colf  + e*rowf  + f0
        x2 = a*colf2 + b*rowf2 + c0
        y2 = d*colf2 + e*rowf2 + f0

        # G) Assemble dataframe (float64), drop self-loops just in case
        River_Flow = pd.DataFrame({
            "easting": x1, "northing": y1,
            "flow_easting": x2, "flow_northing": y2
        }).astype("float64")
        River_Flow = River_Flow.loc[~((River_Flow.easting == River_Flow.flow_easting) &
                                      (River_Flow.northing == River_Flow.flow_northing))]

    # H) Write parquet (edges)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    River_Flow.to_parquet(OUT_PARQ, index=False)
    ts(f"💾 Wrote {OUT_PARQ}  (rows: {len(River_Flow):,})")

    # I) ALSO write a GeoParquet with LINESTRING geometry (for easy mapping)
    ts("▶ Building LineStrings (one per edge) …")
    # Create shapely LineStrings from start->end (vectorized-ish list comp)
    lines = [
        LineString([(x1, y1), (x2, y2)])
        for x1, y1, x2, y2 in zip(
            River_Flow["easting"].values,
            River_Flow["northing"].values,
            River_Flow["flow_easting"].values,
            River_Flow["flow_northing"].values
        )
    ]
    gdf = gpd.GeoDataFrame({"segments": np.ones(len(lines), dtype=np.int32)}, geometry=lines, crs=f"EPSG:{CRS_EPSG}")
    gdf.to_parquet(OUT_GPQ, index=False)
    ts(f"💾 Wrote {OUT_GPQ}  (features: {len(gdf):,})")

    # Quick peek
    ts("Head of River_Flow:")
    print(River_Flow.head(5))

if __name__ == "__main__":
    main()
