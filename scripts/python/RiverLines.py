#!/usr/bin/env python3
"""
make_river_lines.py
Merge pixel-to-pixel flow edges into longer LINESTRINGs for smooth river mapping.

Inputs
------
- River_Flow parquet with columns:
    easting, northing, flow_easting, flow_northing  (float64, EPSG:27700)

Outputs
-------
- GeoParquet of merged lines:
    /Users/odran/Dropbox/sewage/data/processed/Rivers/River_Lines.parquet
  (geometry=LINESTRING, crs=EPSG:27700)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

# ----------------------------
# Defaults
# ----------------------------
ROOT = Path("/Users/odran/Dropbox/sewage")
IN_PARQ_DEFAULT  = ROOT / "data" / "processed" / "Rivers" / "River_Flow.parquet"
OUT_GPQ_DEFAULT  = ROOT / "data" / "processed" / "Rivers" / "River_Lines.parquet"
CRS_EPSG_DEFAULT = 27700

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Merge river flow edges into longer LineStrings.")
    ap.add_argument("--in",  dest="in_parq",  default=str(IN_PARQ_DEFAULT),
                    help="Input edges parquet (River_Flow.parquet)")
    ap.add_argument("--out", dest="out_gpq",  default=str(OUT_GPQ_DEFAULT),
                    help="Output GeoParquet path for merged lines")
    ap.add_argument("--crs", dest="crs_epsg", default=CRS_EPSG_DEFAULT, type=int,
                    help="EPSG code of coordinates (default 27700 BNG)")
    ap.add_argument("--round", dest="round_to", default=0.0, type=float,
                    help="Optional rounding (meters) to snap coords before merging (e.g., 0, 0.1, 1)")
    ap.add_argument("--cap", dest="cap", default=0, type=int,
                    help="Optional cap on number of edges to process (0 = no cap)")
    ap.add_argument("--minlen", dest="min_vertices", default=2, type=int,
                    help="Minimum vertices per merged line to keep (default 2)")
    return ap.parse_args()

# ----------------------------
# Utilities
# ----------------------------
def load_edges(in_path: Path, cap: int = 0) -> pd.DataFrame:
    df = pd.read_parquet(in_path, columns=["easting","northing","flow_easting","flow_northing"])
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().copy()
    # drop self-loops
    df = df.loc[~((df.easting == df.flow_easting) & (df.northing == df.flow_northing))]
    if cap and len(df) > cap:
        df = df.sample(cap, random_state=1)
        print(f"⚠ Capped edges to {cap:,} for this run.")
    print(f"Loaded {len(df):,} edges.")
    return df

def round_if_needed(df: pd.DataFrame, meters: float) -> pd.DataFrame:
    if meters and meters > 0:
        q = float(meters)
        for c in ["easting","northing","flow_easting","flow_northing"]:
            df[c] = (np.round(df[c] / q) * q).astype("float64")
        # Remove duplicates created by rounding the same edge twice
        df = df.drop_duplicates(["easting","northing","flow_easting","flow_northing"])
        print(f"Rounded coordinates to nearest {meters} m and deduplicated edges → {len(df):,} edges.")
    return df

def merge_edges_to_lines(df: pd.DataFrame) -> list[LineString]:
    """
    Collapse degree-2 chains in the directed graph into longer LineStrings.
    Each unique coordinate pair is treated as a node key.
    """
    # Nodes as tuples for exact (or rounded) matching
    src = list(zip(df.easting.values, df.northing.values))
    dst = list(zip(df.flow_easting.values, df.flow_northing.values))

    out_edges: dict[tuple[float,float], list[tuple[float,float]]] = defaultdict(list)
    in_deg:   dict[tuple[float,float], int] = defaultdict(int)
    out_deg:  dict[tuple[float,float], int] = defaultdict(int)

    for a, b in zip(src, dst):
        out_edges[a].append(b)
        out_deg[a] += 1
        in_deg[b]  += 1
        # ensure keys exist
        _ = in_deg[a]; _ = out_deg[b]

    # Start at nodes where chains should begin: out>=1 and in!=1 (sources, confluences, splits)
    starts = [n for n in out_deg.keys() if out_deg[n] >= 1 and in_deg[n] != 1]

    visited = set()
    lines: list[LineString] = []

    def visit_edge(a: tuple[float,float], b: tuple[float,float]) -> None:
        """Walk forward collapsing degree-2 nodes into a single LineString."""
        if (a, b) in visited:
            return
        coords = [a, b]
        visited.add((a, b))
        cur = b
        # Continue while exactly one in, one out (linear chain)
        while out_deg[cur] == 1 and in_deg[cur] == 1:
            nxt = out_edges[cur][0]
            if (cur, nxt) in visited:
                break
            coords.append(nxt)
            visited.add((cur, nxt))
            cur = nxt
        if len(coords) >= 2:
            lines.append(LineString(coords))

    # Walk from all starts
    for s in starts:
        for nxt in out_edges[s]:
            visit_edge(s, nxt)

    # Catch remaining edges (e.g., within cycles or isolated small bits)
    for a, outs in out_edges.items():
        for b in outs:
            if (a, b) not in visited:
                visit_edge(a, b)

    return lines

def main():
    args = parse_args()
    in_path  = Path(args.in_parq)
    out_path = Path(args.out_gpq)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    assert in_path.exists(), f"Input parquet not found: {in_path}"

    df = load_edges(in_path, cap=args.cap)
    df = round_if_needed(df, meters=args.round_to)

    print("Merging edges into lines …")
    lines = merge_edges_to_lines(df)
    print(f"Built {len(lines):,} merged lines.")

    if args.min_vertices > 2:
        # keep lines with at least N vertices
        keep = [ln for ln in lines if len(ln.coords) >= args.min_vertices]
        print(f"Filtered to lines with ≥{args.min_vertices} vertices: {len(keep):,}")
        lines = keep

    gdf = gpd.GeoDataFrame({"segments": [len(ln.coords)-1 for ln in lines]},
                           geometry=lines, crs=f"EPSG:{args.crs_epsg}")

    gdf.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(gdf):,} merged lines → {out_path}")

if __name__ == "__main__":
    main()
