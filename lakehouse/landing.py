import csv
import hashlib
import io
import json
import logging
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests

from lakehouse import config

log = logging.getLogger("landing")

MONTH_RE = re.compile(r"(20\d{2})(0[1-9]|1[0-2])")
CHUNK = 1024 * 1024
RESERVED_COLUMNS = {"src_month", "src_file", "ingested_at", "corrupt_record"}


@dataclass(frozen=True)
class Member:
    name: str            # path inside the outer zip
    month: str           # YYYYMM, parsed from the file name
    size: int            # uncompressed bytes (for an inner zip: the zip's own size)
    crc: int             # CRC32 stored in the zip directory
    kind: str = "csv"    # "csv", or "zip" for a monthly zip inside the yearly zip


def zip_name(year: int) -> str:
    return f"{year}-citibike-tripdata.zip"


def zip_path(year: int) -> Path:
    return config.LANDING_ZIPS / zip_name(year)


def _manifest_path() -> Path:
    return config.LANDING_DIR / "manifest.json"


def _read_manifest() -> dict:
    p = _manifest_path()
    return json.loads(p.read_text()) if p.exists() else {}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def download_zip(year: int) -> Path:
    """Download a yearly zip. Skips if unchanged, resumes if interrupted."""
    config.LANDING_ZIPS.mkdir(parents=True, exist_ok=True)
    name, dest = zip_name(year), zip_path(year)
    url = f"{config.SOURCE_BASE_URL}/{name}"
    part = dest.with_name(name + ".part")

    head = requests.head(url, timeout=60, allow_redirects=True)
    head.raise_for_status()
    remote_size = int(head.headers["Content-Length"])
    remote_etag = head.headers.get("ETag", "").strip('"')
    manifest = _read_manifest()
    known_etag = manifest.get(name, {}).get("etag")

    if dest.exists():
        if dest.stat().st_size == remote_size and known_etag in (None, remote_etag):
            log.info("Skip download (unchanged): %s", name)
            return dest
        log.warning("Local copy differs from the source (size or ETag): downloading again")
        dest.unlink()
        part.unlink(missing_ok=True)

    if part.exists() and part.stat().st_size > remote_size:
        part.unlink()
    start = part.stat().st_size if part.exists() else 0

    t0 = time.time()
    fetched = 0
    if start < remote_size:
        headers = {"Range": f"bytes={start}-"} if start else {}
        with requests.get(url, headers=headers, stream=True, timeout=(10, 120)) as r:
            r.raise_for_status()
            resumed = start > 0 and r.status_code == 206
            written = start if resumed else 0
            step = 200 * CHUNK
            next_log = (written // step + 1) * step
            with open(part, "ab" if resumed else "wb") as f:
                for chunk in r.iter_content(CHUNK):
                    f.write(chunk)
                    written += len(chunk)
                    fetched += len(chunk)
                    if written >= next_log:
                        log.info("  %s: %.0f of %.0f MB", name, written / 1e6, remote_size / 1e6)
                        next_log += step

    size = part.stat().st_size
    if size != remote_size:
        raise OSError(f"Incomplete download: {size} of {remote_size} bytes. Run again to resume.")
    part.replace(dest)

    seconds = max(time.time() - t0, 0.001)
    manifest[name] = {"etag": remote_etag, "size": remote_size, "sha256": _sha256(dest)}
    _manifest_path().write_text(json.dumps(manifest, indent=2))
    log.info("Downloaded %s: %.0f MB this session in %.0f s (%.1f MB/s)",
             name, fetched / 1e6, seconds, fetched / 1e6 / seconds)
    return dest


def list_members(zip_file: Path) -> list[Member]:
    """Month entries of a yearly zip: CSVs or monthly zips. Skips macOS junk."""
    out = []
    with zipfile.ZipFile(zip_file) as zf:
        for info in zf.infolist():
            base = Path(info.filename).name
            if info.is_dir() or info.filename.startswith("__MACOSX/") or base.startswith("._"):
                continue
            lower = info.filename.lower()
            if lower.endswith(".zip"):
                kind = "zip"
            elif lower.endswith(".csv"):
                kind = "csv"
            else:
                continue
            m = MONTH_RE.search(base)
            if not m:
                raise ValueError(f"Cannot find a YYYYMM month in file name: {info.filename}")
            out.append(Member(info.filename, m.group(1) + m.group(2),
                              info.file_size, info.CRC, kind))
    return sorted(out, key=lambda x: x.name)


def read_header(zip_file: Path, member_name: str) -> list[str]:
    with zipfile.ZipFile(zip_file) as zf, zf.open(member_name) as fh:
        line = io.TextIOWrapper(fh, encoding="utf-8-sig", newline="").readline()
    if not line.strip():
        raise ValueError(f"Empty header line in {member_name}")
    return next(csv.reader([line]))

def read_header_file(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        line = fh.readline()
    if not line.strip():
        raise ValueError(f"Empty header line in {path.name}")
    return next(csv.reader([line]))

def normalize(name: str) -> str:
    s = re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")
    return s or "col"


def normalize_columns(raw: list[str]) -> list[str]:
    cols = [normalize(c) for c in raw]
    if len(set(cols)) != len(cols):
        raise ValueError(f"Duplicate column names after normalizing: {raw}")
    clash = RESERVED_COLUMNS & set(cols)
    if clash:
        raise ValueError(f"Source columns collide with reserved names: {sorted(clash)}")
    return cols


def month_fingerprint(members: list[Member]) -> str:
    h = hashlib.sha256()
    for m in sorted(members, key=lambda x: x.name):
        h.update(f"{m.name}|{m.size}|{m.crc}\n".encode())
    return h.hexdigest()


def _extract_csvs(zf: zipfile.ZipFile, dest_dir: Path) -> list[Path]:
    out = []
    for info in zf.infolist():
        base = Path(info.filename).name
        if info.is_dir() or info.filename.startswith("__MACOSX/") or base.startswith("._"):
            continue
        lower = info.filename.lower()
        if lower.endswith(".zip"):
            raise NotImplementedError(f"Zip nested more than one level: {info.filename}")
        if not lower.endswith(".csv"):
            continue
        target = dest_dir / base                    # flatten: file name only
        if target.exists():
            raise ValueError(f"Two files share the name {base}")
        with zf.open(info) as src, open(target, "wb") as out_f:
            shutil.copyfileobj(src, out_f, CHUNK)
        out.append(target)
    return out


def extract_month(zip_file: Path, members: list[Member], dest_dir: Path) -> list[Path]:
    """Extract every CSV for one month into dest_dir; returns the CSV paths."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = []
    with zipfile.ZipFile(zip_file) as outer:
        for m in sorted(members, key=lambda x: x.name):
            if m.kind == "csv":
                files += _extract_csvs_single(outer, m, dest_dir)
            else:
                tmp = dest_dir / "_inner.zip"
                with outer.open(m.name) as src, open(tmp, "wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK)
                try:
                    with zipfile.ZipFile(tmp) as inner:
                        files += _extract_csvs(inner, dest_dir)
                finally:
                    tmp.unlink(missing_ok=True)
    if not files:
        raise ValueError(f"No CSV files found for {members[0].month}")
    return sorted(files)


def _extract_csvs_single(outer: zipfile.ZipFile, m: Member, dest_dir: Path) -> list[Path]:
    target = dest_dir / Path(m.name).name
    if target.exists():
        raise ValueError(f"Two files share the name {target.name}")
    with outer.open(m.name) as src, open(target, "wb") as dst:
        shutil.copyfileobj(src, dst, CHUNK)
    return [target]