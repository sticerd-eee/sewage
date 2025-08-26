import numpy as np
import rasterio
import pandas as pd
from pathlib import Path

# === D8-style direction offsets
offsets = {
    1: (1, -1), 2: (1, 0), 3: (1, 1),
    4: (0, -1),           6: (0, 1),
    7: (-1, -1), 8: (-1, 0), 9: (-1, 1)
}

# === Helper to infer flow direction based on steepest descent
def infer_flow_by_slope(r, c, hght):
    min_elev = hght[r, c]
    best_code = None
    best_pos = None

    for code, (dy, dx) in offsets.items():
        r2, c2 = r + dy, c + dx
        if 0 <= r2 < hght.shape[0] and 0 <= c2 < hght.shape[1]:
            neighbor_elev = hght[r2, c2]
            if np.isnan(neighbor_elev):
                continue
            if neighbor_elev < min_elev:
                min_elev = neighbor_elev
                best_code = code
                best_pos = (r2, c2)

    return best_code, best_pos

# === File paths
folder = Path(__file__).parent

# Navigate up from script to root 'sewage' folder, then into the data folder
data_folder = folder.parent.parent / "data" / "raw" / "rivers" / "digimap_download_ceh_integrated_hydrological" / "ihdtm-2016_6024334"

surf_path = data_folder / "14jan16_SURF_0_000_000_700_1300.asc"
outf_path = data_folder / "14jan16_OUTF_0_000_000_700_1300.asc"
hght_path = data_folder / "14jan16_HGHT_0_000_000_700_1300.asc"

# === Load rasters
with rasterio.open(surf_path) as surf_src, \
     rasterio.open(outf_path) as outf_src, \
     rasterio.open(hght_path) as hght_src:

    surf = surf_src.read(1)
    outf = outf_src.read(1).astype(int)
    hght = hght_src.read(1)
    transform = surf_src.transform
    height, width = surf.shape

# === Build flow table from original OUTF
flow_table = {}
rows, cols = surf.shape
for row in range(rows):
    for col in range(cols):
        if surf[row, col] == 4 and outf[row, col] in offsets:
            dy, dx = offsets[outf[row, col]]
            r2, c2 = row + dy, col + dx
            if 0 <= r2 < rows and 0 <= c2 < cols:
                flow_table[(row, col)] = (r2, c2)

# === Output summary
print(f"\n✅ Flow table built: {len(flow_table)} river pixels with downstream connections\n")
print("🔍 Sample downstream mappings:")
for i, (src, dst) in enumerate(flow_table.items()):
    print(f"  From {src} → {dst}")
    if i >= 9:
        break

# === Find all river pixels
river_pixels = set(zip(*np.where(surf == 4)))
with_flow = set(flow_table.keys())
without_flow = river_pixels - with_flow

print(f"\n📌 Total river pixels: {len(river_pixels)}")
print(f"📌 With direction:      {len(with_flow)}")
print(f"📌 Without direction:   {len(without_flow)}")

# === Convert to DataFrame with coordinates, direction and height
records = []

# Pixels with flow from OUTF
for (r1, c1), (r2, c2) in flow_table.items():
    x1, y1 = rasterio.transform.xy(transform, r1, c1, offset="center")
    x2, y2 = rasterio.transform.xy(transform, r2, c2, offset="center")
    code = outf[r1, c1]
    elev = hght[r1, c1]
    records.append({
        "row": r1, "col": c1,
        "easting": x1, "northing": y1,
        "flow_row": r2, "flow_col": c2,
        "flow_easting": x2, "flow_northing": y2,
        "direction_code": code,
        "height": elev
    })

# Infer flow for pixels without original OUTF
inferred = 0
for r, c in without_flow:
    code, dst = infer_flow_by_slope(r, c, hght)
    x1, y1 = rasterio.transform.xy(transform, r, c, offset="center")
    elev = hght[r, c]

    if dst:
        r2, c2 = dst
        x2, y2 = rasterio.transform.xy(transform, r2, c2, offset="center")
        inferred += 1
    else:
        r2 = c2 = x2 = y2 = None

    records.append({
        "row": r, "col": c,
        "easting": x1, "northing": y1,
        "flow_row": r2, "flow_col": c2,
        "flow_easting": x2, "flow_northing": y2,
        "direction_code": code if code else -1,
        "height": elev
    })

# Output folder: navigate up to sewage/, then to data/processed/Rivers
output_folder = folder.parent.parent / "data" / "processed" / "Rivers"
output_folder.mkdir(parents=True, exist_ok=True)  # Ensure the folder exists

# Save full dataset
df = pd.DataFrame.from_records(records)
output_path = output_folder / "river_flow_table.parquet"
df.to_parquet(output_path, index=False)

print(f"✅ Saved {output_path.name} with elevation and inferred directions ({inferred} inferred)")