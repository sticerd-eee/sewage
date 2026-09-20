"""Pin complete SEPA hydrography layers; refuse replacement of saved inputs."""
import hashlib
import json
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3] / "data/raw/geography/open_coast/sepa"
BASE = "https://map.sepa.org.uk/server/rest/services/Open/Hydrography/MapServer"


def fetch(url):
    with urllib.request.urlopen(url, timeout=120) as response:
        result = json.load(response)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


def acquire():
    ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = ROOT / "source_manifest.json"
    if manifest_path.exists():
        for source in json.loads(manifest_path.read_text())["sources"]:
            assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source["sha256"]
        return
    sources = []
    for layer, name in [(1, "estuaries"), (2, "coastal")]:
        url = f"{BASE}/{layer}"
        metadata = fetch(url + "?f=json")
        ids = sorted(fetch(url + "/query?where=1%3D1&returnIdsOnly=true&f=json")["objectIds"])
        features = []
        for start in range(0, len(ids), 10):
            params = urllib.parse.urlencode({"objectIds": ",".join(map(str, ids[start:start+10])),
                                            "outFields": "*", "outSR": 27700, "f": "geojson"})
            batch = fetch(url + "/query?" + params)
            if batch.get("exceededTransferLimit"):
                raise RuntimeError("Incomplete SEPA batch")
            features.extend(batch["features"])
            print(name, len(features), "/", len(ids), flush=True)
        observed = sorted(f["properties"]["objectid"] for f in features)
        if observed != ids:
            raise RuntimeError("SEPA feature IDs do not match complete inventory")
        collection = {"type": "FeatureCollection", "crs": {"type": "name", "properties": {
            "name": "urn:ogc:def:crs:EPSG::27700"}}, "features": features}
        for filename, value in [(name + ".geojson", collection), (name + "-metadata.json", metadata)]:
            data = json.dumps(value).encode()
            path = ROOT / filename
            if path.exists() and path.read_bytes() != data:
                raise RuntimeError("Refusing to replace immutable source " + str(path))
            path.write_bytes(data)
            sources.append({"path": filename, "sha256": hashlib.sha256(data).hexdigest(),
                            "url": url, "feature_count": len(ids),
                            "retrieved": "2026-09-20", "cartographic_vintage": "not specified by service"})
    manifest_path.write_text(json.dumps({"sources": sources}, indent=2))


if __name__ == "__main__":
    acquire()
