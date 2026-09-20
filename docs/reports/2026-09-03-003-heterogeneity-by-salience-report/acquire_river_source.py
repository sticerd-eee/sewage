"""Pin EA river-centreline evidence used to review inland tidal-bank exclusions."""
import hashlib
import json
from pathlib import Path
import urllib.request

URL = ("https://environment.data.gov.uk/spatialdata/wfd-river-canal-and-swt-water-bodies-cycle-3-classification-2022/"
       "ogc/features/v1/collections/WFD_River_Canal_and_SWT_Water_Bodies_Cycle_3_Classification_2022/items?limit=10000&f=json")
SHA256 = "1ad8217572fe517db47cc2775ce84a83e9ac3fff5600e4e993ce44500263c248"


def acquire():
    root = Path(__file__).resolve().parents[3] / "data/raw/geography/open_coast/rivers"
    path = root / "ea-rivers.geojson"
    if path.exists():
        data = path.read_bytes()
    elif Path("/tmp/ea-rivers.geojson").exists():
        data = Path("/tmp/ea-rivers.geojson").read_bytes()
    else:
        with urllib.request.urlopen(URL, timeout=120) as response:
            data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise RuntimeError("EA river source differs from the pinned retrieval")
    collection = json.loads(data)
    if len(collection["features"]) != collection["numberMatched"]:
        raise RuntimeError("Incomplete river source")
    root.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(data)
    manifest = dict(url=URL, sha256=SHA256, path=path.name, feature_count=3928,
        classification_year=2022, cartographic_source="EA Detailed River Network; exact vintage unspecified",
        retrieved="2026-09-20", license="Open Government Licence; Contains OS data",
        metadata_url="https://environment.data.gov.uk/dataset/b0dc3be8-3384-423d-af5d-a93934565bc7")
    value = json.dumps(manifest, indent=2)
    destination = root / "source_manifest.json"
    if destination.exists() and destination.read_text() != value:
        raise RuntimeError("Refusing to replace source manifest")
    destination.write_text(value)


if __name__ == "__main__":
    acquire()
