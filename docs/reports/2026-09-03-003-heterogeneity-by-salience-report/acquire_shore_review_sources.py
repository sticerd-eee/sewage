"""Pin supplementary physical/river evidence used in the Saltfleet map review."""
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from acquire_open_coast_sources import (
    NoRedirect, OS_URL, OS_ARCHIVE_MD5, RAW_ROOT, RemoteZip, write_raw,
)


def acquire_review_sources():
    root = RAW_ROOT / "os_foreshore_review_2026_04"
    manifest = root / "source_manifest.json"
    if not manifest.exists():
        try:
            urllib.request.build_opener(NoRedirect).open(OS_URL, timeout=30)
            raise RuntimeError("Expected the pinned official archive redirect")
        except urllib.error.HTTPError as error:
            if error.code != 307:
                raise
            metadata = json.loads(error.read())
            url = error.headers["Location"]
        if metadata["md5"] != OS_ARCHIVE_MD5:
            raise RuntimeError("OS archive release changed")
        components = []
        with zipfile.ZipFile(RemoteZip(url, metadata["size"])) as archive:
            for entry in archive.infolist():
                if entry.filename.startswith("data/TF/TF_Foreshore."):
                    sha = write_raw(root / entry.filename, archive.read(entry))
                    components.append(dict(path=entry.filename, sha256=sha, archive_crc32=entry.CRC))
        if len(components) != 4:
            raise RuntimeError("Incomplete foreshore shapefile")
        write_raw(manifest, json.dumps(dict(source_url=OS_URL, archive=metadata, components=components), indent=2).encode())
    metadata = json.loads(manifest.read_text())
    for entry in metadata["components"]:
        if hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest() != entry["sha256"]:
            raise RuntimeError("Pinned foreshore component changed")
    url = "https://environment.data.gov.uk/KB6uNVj5ZcJr7jUP/ArcGIS/rest/services/StatutoryMainRiverMap/FeatureServer/0/query?" + urllib.parse.urlencode(dict(
        where="1=1", geometry="543000,392000,548000,397000", geometryType="esriGeometryEnvelope",
        inSR=27700, outSR=27700, outFields="*", f="geojson"))
    root = RAW_ROOT / "saltfleet_main_river_review"
    path = root / "main-rivers.geojson"
    data = path.read_bytes() if path.exists() else urllib.request.urlopen(url, timeout=60).read()
    collection = json.loads(data)
    collection["features"].sort(key=lambda feature: feature["id"])
    data = json.dumps(collection, sort_keys=True, separators=(",", ":")).encode()
    expected = "c2304faa7e9e840a59901d77836c7ebdc3d53bcdb4a04253d60c5ac30f20f854"
    if hashlib.sha256(data).hexdigest() != expected:
        raise RuntimeError("Supplementary river source changed; review the new version")
    if len(json.loads(data)["features"]) != 24:
        raise RuntimeError("Unexpected supplementary river inventory")
    write_raw(path, data)
    write_raw(root / "source_manifest.json", json.dumps(dict(url=url, path=path.name,
        sha256=expected, features=24, serialization="JSON keys and feature IDs sorted; coordinates unchanged",
        scope="Saltfleet review envelope in EPSG:27700",
        license="Open Government Licence", cartographic_vintage="not established",
        metadata_url="https://www.data.gov.uk/dataset/4ae8ba46-f9a4-47d0-8d93-0f93eb494540/statutory-main-river-map"), indent=2).encode())


if __name__ == "__main__":
    acquire_review_sources()
