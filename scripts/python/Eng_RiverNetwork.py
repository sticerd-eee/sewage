#!/usr/bin/env python3
"""
Eng_RiverNetwork.py
Build an England-only river flow table directly from IHDTM rasters by masking to England
and rebuilding pixel-center -> pixel-center edges. This avoids slow geometry clipping.

Output:
  /Users/odran/Dropbox/sewage/data/processed/Rivers/eng_river_flow_table.parquet
Columns:
  easting, northing, flow_easting, flow_northing  (float64)
"""

from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from rasterio.crs import CRS
from shapely.validation import make_valid
from shapely.geometry import mapping

# ----------------------------
# Config
# ----------------------------
CRS_EPSG = 27700          # Assume rasters are BNG if no CRS is present
KEEP_RULE = "both"        # "both" = keep edges where BOTH pixels are inside England (default, topology-faithful)
                          # "either" = keep edges where EITHER pixel is inside England (looser)
ROOT = Path("/Users/odran/Dropbox/sewage")
UK_SHP = ROOT / "data" / "raw" / "shapefiles" / "UK_Nations" / "CTRY_DEC_2024_UK_BGC.shp"

raw_river = ROOT / "data" / "raw" / "rivers" / "digimap_download_ceh_integrated_hydrological" / "ihdtm-2016_6024334"
surf_path = raw_river / "14jan16_SURF_0_000_000_700_1300.asc"
outf_path = raw_river / "14jan16_OUTF_0_000_000_700_1300.asc"
hght_path = raw_river / "14jan16_HGHT_0_000_000_700_1300.asc"  # not required for output; read only to validate alignment

OUT_PARQ = ROOT / "data" / "processed" / "Rivers" / "eng_river_flow_table.parquet"

# D8 offsets (row, col). Codes must match your OUTF convention.
OFFSETS = {
    1: (1, -1), 2: (1, 0), 3: (1, 1),
    4: (0, -1),           6: (0, 1),
    7: (-1, -1), 8: (-1, 0), 9: (-1, 1)
}
VALID_CODES = np.array(sorted(OFFSETS.keys()), dtype=np.int32)

def ts(msg: str) -> None:
    print(msg, flush=True)

def select_england(uk: gpd.GeoDataFrame) -> gpd.GeoSeries:
    # Try common name fields first
    for col in ["CTRY24NM","CTRY22NM","CTRYNM","NAME","name","Country","country"]:
        if col in uk.columns:
            m = uk[col].astype(str).str.lower() == "england"
            if m.any():
                try:
                    # Shapely 2
                    return uk.loc[m, "geometry"].union_all()
                except Exception:
                    # Shapely 1 fallback
                    return uk.loc[m, "geometry"].unary_union
    # Fallback scan
    for col in uk.columns:
        if str(uk[col].dtype).startswith(("object","string","category")):
            m = uk[col].astype(str).str.lower() == "england"
            if m.any():
                try:
                    return uk.loc[m, "geometry"].union_all()
                except Exception:
                    return uk.loc[m, "geometry"].unary_union
    raise RuntimeError("Could not find an 'England' feature in the shapefile.")

def main():
    ts("▶ Reading UK nations shapefile …")
    uk = gpd.read_file(UK_SHP)
    if uk.crs is None:
        # We’ll project England to the raster CRS later; this is fine.
        pass

    eng = select_england(uk)
    eng = make_valid(eng)

    ts("▶ Reading IHDTM rasters (SURF, OUTF, HGHT) …")
    with rasterio.open(surf_path) as surf_src, \
         rasterio.open(outf_path) as outf_src, \
         rasterio.open(hght_path) as hght_src:

        # Determine raster CRS (assume BNG if missing)
        raster_crs = surf_src.crs if surf_src.crs is not None else CRS.from_epsg(CRS_EPSG)

        # Reproject England geometry to raster CRS
        uk_crs = uk.crs if uk.crs is not None else CRS.from_epsg(CRS_EPSG)
        eng_proj = gpd.GeoSeries([eng], crs=uk_crs).to_crs(raster_crs).iloc[0]

        # Read arrays
        surf = surf_src.read(1)                 # river mask (4 = river)
        outf = outf_src.read(1).astype("int32") # D8 codes
        transform = surf_src.transform
        height, width = surf.shape

        # Sanity: ensure same shape and transform for OUTF
        if outf.shape != surf.shape:
            raise RuntimeError("SURF and OUTF rasters have different shapes.")
        if hght_src.width != surf_src.width or hght_src.height != surf_src.height:
            ts("⚠ HGHT shape differs; continuing since it isn't required for output.")

        # A) England mask aligned to raster grid
        eng_mask = ~geometry_mask(
            [mapping(eng_proj)],
            out_shape=(height, width),
            transform=transform,
            invert=True
        )

        # B) Candidate river pixels: SURF == 4
        river = (surf == 4)

        # C) Source pixels inside England?
        src_in = river & eng_mask

        # D) Compute downstream neighbour indices for valid D8 codes
        r, c = np.where(src_in)
        if r.size == 0:
            raise RuntimeError("No river pixels found inside England.")

        codes = outf[r, c]
        valid = np.isin(codes, VALID_CODES)
        r = r[valid]; c = c[valid]; codes = codes[valid]

        # Lookups for dy/dx
        dy_lut = np.zeros(10, dtype=np.int32); dx_lut = np.zeros(10, dtype=np.int32)
        for k, (dy, dx) in OFFSETS.items():
            dy_lut[k] = dy; dx_lut[k] = dx

        r2 = r + dy_lut[codes]
        c2 = c + dx_lut[codes]

        in_bounds = (r2 >= 0) & (r2 < height) & (c2 >= 0) & (c2 < width)
        r, c, r2, c2 = r[in_bounds], c[in_bounds], r2[in_bounds], c2[in_bounds]

        # E) Apply KEEP_RULE
        if KEEP_RULE == "both":
            keep = eng_mask[r2, c2]  # downstream also in England
        elif KEEP_RULE == "either":
            keep = eng_mask[r, c] | eng_mask[r2, c2]  # at least one in England
        else:
            raise ValueError("KEEP_RULE must be 'both' or 'either'.")

        r, c, r2, c2 = r[keep], c[keep], r2[keep], c2[keep]

        ts(f"✅ England-only edges (rule={KEEP_RULE}): {r.size:,}")

        # F) Vectorised pixel-centre coordinates from affine transform
        a, b, c0, d, e, f0 = transform.a, transform.b, transform.c, transform.d, transform.e, transform.f
        # starts
        colf = c.astype(np.float64) + 0.5; rowf = r.astype(np.float64) + 0.5
        x1 = a*colf + b*rowf + c0
        y1 = d*colf + e*rowf + f0
        # ends
        colf2 = c2.astype(np.float64) + 0.5; rowf2 = r2.astype(np.float64) + 0.5
        x2 = a*colf2 + b*rowf2 + c0
        y2 = d*colf2 + e*rowf2 + f0

        out_df = pd.DataFrame({
            "easting": x1, "northing": y1,
            "flow_easting": x2, "flow_northing": y2
        }).astype("float64")

    OUT_PARQ.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(OUT_PARQ, index=False)
    ts(f"💾 Wrote {OUT_PARQ}  (rows: {len(out_df):,})")

if __name__ == "__main__":
    main()
