"""
Parqcast HTTP Server — receives raw Parquet files, stores as-is
in a UUID-split tree structure with API key authentication.

Tree structure:
    {data_root}/{namespace}/{table}/{year}/{month}/{day}/{ab}/{cd}/{ef}/{gh}/{uuid}/data.parquet

Usage:
    uv run uvicorn parqcast.server.app:app --host 0.0.0.0 --port 8420

Config:
    Reads config.toml from CWD or path in PARQCAST_CONFIG env var.
    Env vars PARQCAST_DATA_ROOT and PARQCAST_API_KEY override config file.
"""

import io
import json
import os
import tomllib
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# -- Load Config --
CONFIG_PATH = Path(os.environ.get("PARQCAST_CONFIG", "config.toml"))

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "rb") as f:
        CONFIG = tomllib.load(f)
else:
    CONFIG = {}

DATA_ROOT = Path(
    os.environ.get("PARQCAST_DATA_ROOT")
    or CONFIG.get("server", {}).get("data_root", "/var/parqcast")
)
API_KEY = os.environ.get("PARQCAST_API_KEY") or CONFIG.get("auth", {}).get("api_key", "")

app = FastAPI(title="Parqcast Server", version="0.3.0")

# -- Auth Middleware --

OPEN_PATHS = {"/health", "/docs", "/openapi.json"}


@app.middleware("http")
async def check_api_key(request: Request, call_next):
    if API_KEY and request.url.path not in OPEN_PATHS:
        provided = request.headers.get("X-API-Key", "")
        if provided != API_KEY:
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
    return await call_next(request)


# -- Models --


class UploadResponse(BaseModel):
    upload_id: str
    path: str
    rows: int
    size_bytes: int


# -- Helpers --


def get_partition_path(namespace: str, table: str, upload_id: str) -> Path:
    """Build path with UUID split: {ns}/{table}/{Y}/{M}/{D}/{ab}/{cd}/{ef}/{gh}/{uuid}/"""
    now = datetime.now(UTC)
    hex_prefix = upload_id.replace("-", "")[:8]
    uuid_dirs = Path(hex_prefix[0:2]) / hex_prefix[2:4] / hex_prefix[4:6] / hex_prefix[6:8]
    return DATA_ROOT / namespace / table / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}" / uuid_dirs / upload_id


# -- Endpoints --


@app.on_event("startup")
async def startup():
    DATA_ROOT.mkdir(parents=True, exist_ok=True)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "data_root": str(DATA_ROOT),
        "data_root_exists": DATA_ROOT.exists(),
        "auth_enabled": bool(API_KEY),
    }


@app.post("/upload/{namespace}/{table}", response_model=UploadResponse)
async def upload_parquet(namespace: str, table: str, request: Request):
    """Accept raw Parquet bytes and store as-is."""
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty body")

    # Validate it's a real Parquet file and extract row count from metadata
    try:
        buf = io.BytesIO(data)
        metadata = pq.read_metadata(buf)
        rows = metadata.num_rows
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid Parquet file") from e

    upload_id = str(uuid.uuid4())
    partition_dir = get_partition_path(namespace, table, upload_id)
    partition_dir.mkdir(parents=True, exist_ok=True)
    filepath = partition_dir / "data.parquet"
    filepath.write_bytes(data)

    return UploadResponse(
        upload_id=upload_id,
        path=str(filepath.relative_to(DATA_ROOT)),
        rows=rows,
        size_bytes=len(data),
    )


@app.post("/upload/{namespace}/_manifest")
async def upload_manifest(namespace: str, request: Request):
    """Accept manifest JSON and store alongside data."""
    body = await request.body()
    try:
        json.loads(body)
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail="Invalid JSON") from e

    upload_id = str(uuid.uuid4())
    partition_dir = get_partition_path(namespace, "_manifests", upload_id)
    partition_dir.mkdir(parents=True, exist_ok=True)
    filepath = partition_dir / "manifest.json"
    filepath.write_bytes(body)

    return {"upload_id": upload_id, "path": str(filepath.relative_to(DATA_ROOT))}


@app.get("/download/{path:path}")
async def download(path: str):
    """Return raw file bytes."""
    target = DATA_ROOT / path
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return FileResponse(target, media_type="application/octet-stream")


@app.get("/browse/{path:path}")
async def browse(path: str = ""):
    target = DATA_ROOT / path
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {path}")

    if target.is_file():
        stat = target.stat()
        return {
            "type": "file",
            "path": path,
            "size_bytes": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
        }

    entries = [
        {
            "name": item.name,
            "path": str(item.relative_to(DATA_ROOT)),
            "type": "dir" if item.is_dir() else "file",
        }
        for item in sorted(target.iterdir())
    ]
    return {"type": "dir", "path": path, "entries": entries}


@app.get("/read/{namespace}/{table}")
async def read_recent(namespace: str, table: str, limit: int = 100):
    table_dir = DATA_ROOT / namespace / table
    if not table_dir.exists():
        raise HTTPException(status_code=404, detail=f"No data for {namespace}/{table}")

    parquet_files = sorted(table_dir.rglob("*.parquet"))
    if not parquet_files:
        raise HTTPException(status_code=404, detail="No parquet files found")

    latest = parquet_files[-1]
    arrow_table = pq.read_table(latest)
    records = arrow_table.to_pydict()

    keys = list(records.keys())
    num_rows = len(records[keys[0]]) if keys else 0
    rows = [{k: records[k][i] for k in keys} for i in range(min(num_rows, limit))]

    return {
        "file": str(latest.relative_to(DATA_ROOT)),
        "total_records": num_rows,
        "returned": len(rows),
        "records": rows,
    }
