"""
api.py - FastAPI REST service for LGD Fuzzy Matcher
Run: uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""
import io
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List, Optional

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from matcher import LGDMatcher
from utils import load_config, setup_logging

# ---------------------------------------------------------------------------
# Configuration & logging
# ---------------------------------------------------------------------------
cfg = load_config("config.json")
logger = setup_logging(
    cfg.get("logging", {}).get("log_file", "lgd_matcher.log"),
    cfg.get("logging", {}).get("level", "INFO"),
)

# API key loaded from env var (optional — if unset, auth is disabled)
_API_KEY: str = os.getenv("LGD_API_KEY", "").strip()

# Input size guards
MAX_RECORDS_PER_REQUEST = int(os.getenv("LGD_MAX_RECORDS", "50000"))
MAX_CSV_SIZE_BYTES = int(os.getenv("LGD_MAX_CSV_BYTES", str(50 * 1024 * 1024)))  # 50 MB

_matcher: Optional[LGDMatcher] = None


# ---------------------------------------------------------------------------
# Matcher singleton
# ---------------------------------------------------------------------------
def get_matcher() -> LGDMatcher:
    global _matcher
    if _matcher is None:
        _matcher = LGDMatcher(config_path="config.json")
        _matcher.load_master_from_sqlite("lgd_master.db")
        logger.info("Matcher initialized from SQLite database.")
    return _matcher


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    get_matcher()
    logger.info("API started — matcher ready.")
    yield
    logger.info("API shutting down.")


# ---------------------------------------------------------------------------
# App & middleware
# ---------------------------------------------------------------------------
app = FastAPI(
    title="LGD Fuzzy Matcher API",
    version="1.1.0",
    description="Maps raw Indian state/district names to official LGD codes",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request logging middleware
# ---------------------------------------------------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = round(time.perf_counter() - start, 4)
    logger.info(
        "%s %s → %s (%.4fs)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed,
    )
    return response


# ---------------------------------------------------------------------------
# API key dependency (optional)
# ---------------------------------------------------------------------------
async def verify_api_key(x_api_key: str = Header(default="")) -> None:
    """If LGD_API_KEY env var is set, every request must include a matching
    ``X-API-Key`` header.  If the env var is unset, auth is disabled."""
    if _API_KEY and x_api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class MatchRecord(BaseModel):
    id: Optional[str] = None
    state_name_raw: str = Field(..., examples=["delhii"])
    district_name_raw: str = Field(..., examples=["New Delhi"])
    subdistrict_name_raw: Optional[str] = Field(default=None, examples=["pindra"])
    village_name_raw: Optional[str] = Field(default=None, examples=["bhagwanpur"])


class MatchRequest(BaseModel):
    records: List[MatchRecord]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "service": "lgd-fuzzy-matcher"}


@app.get("/stats", dependencies=[Depends(verify_api_key)])
def stats():
    m = get_matcher()
    return {
        "states": len(m.state_df),
        "districts": len(m.district_df),
        "thresholds": m.thresholds,
    }

@app.get("/list/states", dependencies=[Depends(verify_api_key)])
def list_states():
    return get_matcher().list_states()

@app.get("/list/districts", dependencies=[Depends(verify_api_key)])
def list_districts(state_lgd: str):
    return get_matcher().list_districts(state_lgd)

@app.get("/list/subdistricts", dependencies=[Depends(verify_api_key)])
def list_subdistricts(district_lgd: str):
    return get_matcher().list_subdistricts(district_lgd)

@app.get("/list/villages", dependencies=[Depends(verify_api_key)])
def list_villages(subdistrict_lgd: str):
    return get_matcher().list_villages(subdistrict_lgd)

@app.get("/suggest/states", dependencies=[Depends(verify_api_key)])
def suggest_states(q: str, limit: int = 5):
    return get_matcher().suggest_states(q, limit)

@app.get("/suggest/districts", dependencies=[Depends(verify_api_key)])
def suggest_districts(q: str, state_lgd: Optional[str] = None, limit: int = 5):
    return get_matcher().suggest_districts(q, state_lgd, limit)


@app.post("/match", dependencies=[Depends(verify_api_key)])
def match_records(payload: MatchRequest):
    if not payload.records:
        raise HTTPException(400, "No records provided.")
    if len(payload.records) > MAX_RECORDS_PER_REQUEST:
        raise HTTPException(
            413,
            f"Too many records ({len(payload.records)}). "
            f"Max allowed: {MAX_RECORDS_PER_REQUEST}.",
        )
    df = pd.DataFrame([r.model_dump() for r in payload.records], dtype=str)
    t0 = time.perf_counter()
    results = get_matcher().match_dataframe(df)
    elapsed = round(time.perf_counter() - t0, 3)
    results = results.where(pd.notnull(results), None)
    return {
        "total": len(results),
        "elapsed_sec": elapsed,
        "status_summary": results["match_status"].value_counts().to_dict(),
        "results": results.to_dict(orient="records"),
    }


@app.post("/match-csv", dependencies=[Depends(verify_api_key)])
async def match_csv(file: UploadFile = File(...)):
    content = await file.read()
    if len(content) > MAX_CSV_SIZE_BYTES:
        raise HTTPException(
            413,
            f"File too large ({len(content):,} bytes). "
            f"Max allowed: {MAX_CSV_SIZE_BYTES:,} bytes.",
        )
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception as e:
        raise HTTPException(400, f"Invalid CSV: {e}")
    missing = {"state_name_raw", "district_name_raw"} - set(df.columns)
    if missing:
        raise HTTPException(422, f"Missing columns: {sorted(missing)}")
    if len(df) > MAX_RECORDS_PER_REQUEST:
        raise HTTPException(
            413,
            f"CSV has {len(df):,} rows. Max allowed: {MAX_RECORDS_PER_REQUEST:,}.",
        )
    t0 = time.perf_counter()
    results = get_matcher().match_dataframe(df)
    elapsed = round(time.perf_counter() - t0, 3)
    results = results.where(pd.notnull(results), None)
    return {
        "total": len(results),
        "elapsed_sec": elapsed,
        "status_summary": results["match_status"].value_counts().to_dict(),
        "results": results.to_dict(orient="records"),
    }
