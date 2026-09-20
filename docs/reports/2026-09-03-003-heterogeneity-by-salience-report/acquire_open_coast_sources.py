"""Acquire the pinned OS shoreline without downloading unrelated map layers.

Each ZIP component is checked by zipfile's CRC validation and recorded with a
SHA-256 hash. The published archive MD5 pins the release identity; this partial
acquisition does not claim to recompute the MD5 of the complete 2.46 GB archive.
EA/NRW retrieval URLs and hashes are recorded in geometry/source_manifest.json.
"""

import hashlib
import io
import json
from pathlib import Path
import urllib.error
import urllib.request
import zipfile

OS_URL = (
    "https://api.os.uk/downloads/v1/products/OpenMapLocal/downloads"
    "?area=GB&format=ESRI%C2%AE+Shapefile&redirect"
)
OS_ARCHIVE_MD5 = "d255239a55763ab13f99f31f5d64d495"
RAW_ROOT = Path(__file__).resolve().parents[3] / "data/raw/geography/open_coast"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


class RemoteZip(io.RawIOBase):
    """Seekable, bounded HTTP byte ranges for ZIP central-directory access."""

    def __init__(self, url: str, size: int):
        self.url = url
        self.size = size
        self.position = 0

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence not in (0, 1, 2):
            raise ValueError("Unsupported seek origin")
        base = (0, self.position, self.size)[whence]
        position = base + offset
        if position < 0:
            raise ValueError("Negative byte position")
        self.position = position
        return position

    def read(self, size=-1):
        size = self.size - self.position if size < 0 else min(size, self.size - self.position)
        if size <= 0:
            return b""
        request = urllib.request.Request(
            self.url, headers={"Range": f"bytes={self.position}-{self.position + size - 1}"}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 206:
                raise RuntimeError("Server ignored byte range")
            data = response.read()
        if len(data) != size:
            raise RuntimeError("Incomplete byte range")
        self.position += size
        return data


def write_raw(path: Path, data: bytes) -> str:
    if path.exists():
        if path.read_bytes() != data:
            raise RuntimeError(f"Refusing to replace immutable raw source: {path}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def acquire_os() -> None:
    root = RAW_ROOT / "os_openmap_local_2026_04"
    manifest_path = root / "source_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest["archive"]["md5"] != OS_ARCHIVE_MD5:
            raise RuntimeError("Stored OS archive is not the pinned release")
        for component in manifest["components"]:
            digest = hashlib.sha256((root / component["path"]).read_bytes()).hexdigest()
            if digest != component["sha256"]:
                raise RuntimeError(f"Stored OS source hash mismatch: {component['path']}")
        print("Pinned OS shoreline already present and verified.")
        return
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(OS_URL, timeout=30):
            raise RuntimeError("Expected an official archive redirect")
    except urllib.error.HTTPError as error:
        if error.code != 307:
            raise
        archive_url = error.headers["Location"]
        metadata = json.loads(error.read())
    if metadata["md5"] != OS_ARCHIVE_MD5:
        raise RuntimeError("OS archive differs from April 2026; review a new source generation")
    components = []
    with zipfile.ZipFile(RemoteZip(archive_url, metadata["size"])) as archive:
        files = [entry for entry in archive.infolist() if "tidalboundary" in entry.filename.lower()]
        if not files:
            raise RuntimeError("Archive contains no tidal boundary components")
        for entry in files:
            path = root / entry.filename
            if not path.resolve().is_relative_to(root.resolve()):
                raise RuntimeError("Unsafe archive path")
            digest = write_raw(path, archive.read(entry))
            components.append({"path": entry.filename, "sha256": digest, "archive_crc32": entry.CRC})
    manifest = {
        "source_url": OS_URL, "archive": metadata, "cartographic_vintage": "2026-04",
        "license": "Open Government Licence; OS OpenData", "components": components,
    }
    write_raw(manifest_path, json.dumps(manifest, indent=2).encode())
    print(f"Acquired {len(components)} OS shoreline components.")


def acquire_classifications() -> None:
    manifest_path = Path(__file__).with_name("geometry") / "source_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for source in manifest["classifications"]:
        path = RAW_ROOT / "classifications" / source["path"]
        if path.exists():
            data = path.read_bytes()
        else:
            with urllib.request.urlopen(source["url"], timeout=60) as response:
                data = response.read()
        if hashlib.sha256(data).hexdigest() != source["sha256"]:
            raise RuntimeError(f"Classification source differs from pinned retrieval: {source['path']}")
        collection = json.loads(data)
        if len(collection["features"]) != collection["numberMatched"]:
            raise RuntimeError("Classification download is incomplete")
        write_raw(path, data)
    write_raw(
        RAW_ROOT / "classifications/source_manifest.json",
        json.dumps(manifest["classifications"], indent=2).encode(),
    )
    print("Pinned EA/NRW classification sources verified.")


if __name__ == "__main__":
    acquire_os()
    acquire_classifications()
