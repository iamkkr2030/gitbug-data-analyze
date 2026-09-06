import csv
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from pipeline.config import HEADERS


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_source(path):
    """Reject structural errors before Spark: its CSV reader tolerates extra fields."""
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != HEADERS:
            raise ValueError(f"Expected CSV header {HEADERS}")
        for row in reader:
            if len(row) != 2:
                raise ValueError(f"CSV line {reader.line_num}: expected 2 fields, got {len(row)}")
            yield row


def inspect_source(path):
    path = Path(path).resolve()
    before = sha256_file(path)
    count = sum(1 for _ in iter_source(path))
    if not count:
        raise ValueError("Empty input cannot replace a published snapshot")
    if sha256_file(path) != before:
        raise ValueError("Source changed during inspection; retry with a stable file")
    return {"source_path": str(path), "source_sha256": before,
            "input_rows": count, "source_bytes": path.stat().st_size}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def archive_source(context):
    target = Path(context["raw_path"])
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if sha256_file(target) != context["source_sha256"]:
            raise ValueError("Archived source hash mismatch")
        return
    handle, name = tempfile.mkstemp(dir=target.parent, suffix=".csv")
    os.close(handle)
    try:
        shutil.copyfile(context["source_path"], name)
        if sha256_file(name) != context["source_sha256"]:
            raise ValueError("Source changed between inspection and archive")
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)

