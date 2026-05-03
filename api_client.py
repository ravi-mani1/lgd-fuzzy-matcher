import os
import requests
import pandas as pd
from typing import List, Dict, Optional

# Allow overriding via env var (for Docker vs local)
API_BASE_URL = os.getenv("LGD_API_URL", "http://localhost:8000")
API_KEY = os.getenv("LGD_API_KEY", "")

def _headers() -> dict:
    h = {}
    if API_KEY:
        h["X-API-Key"] = API_KEY
    return h

def get_stats() -> dict:
    resp = requests.get(f"{API_BASE_URL}/stats", headers=_headers())
    resp.raise_for_status()
    return resp.json()

def list_states() -> List[Dict]:
    resp = requests.get(f"{API_BASE_URL}/list/states", headers=_headers())
    resp.raise_for_status()
    return resp.json()

def list_districts(state_lgd: str) -> List[Dict]:
    if not str(state_lgd).strip(): return []
    resp = requests.get(f"{API_BASE_URL}/list/districts", params={"state_lgd": state_lgd}, headers=_headers())
    resp.raise_for_status()
    return resp.json()

def list_subdistricts(district_lgd: str) -> List[Dict]:
    if not str(district_lgd).strip(): return []
    resp = requests.get(f"{API_BASE_URL}/list/subdistricts", params={"district_lgd": district_lgd}, headers=_headers())
    resp.raise_for_status()
    return resp.json()

def list_villages(subdistrict_lgd: str) -> List[Dict]:
    if not str(subdistrict_lgd).strip(): return []
    resp = requests.get(f"{API_BASE_URL}/list/villages", params={"subdistrict_lgd": subdistrict_lgd}, headers=_headers())
    resp.raise_for_status()
    return resp.json()

def suggest_states(q: str, limit: int = 5) -> List[Dict]:
    if not str(q).strip(): return []
    resp = requests.get(f"{API_BASE_URL}/suggest/states", params={"q": q, "limit": limit}, headers=_headers())
    resp.raise_for_status()
    return resp.json()

def suggest_districts(q: str, state_lgd: Optional[str] = None, limit: int = 5) -> List[Dict]:
    if not str(q).strip(): return []
    params = {"q": q, "limit": limit}
    if state_lgd and str(state_lgd).strip():
        params["state_lgd"] = state_lgd
    resp = requests.get(f"{API_BASE_URL}/suggest/districts", params=params, headers=_headers())
    resp.raise_for_status()
    return resp.json()

def match_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # We use /match endpoint by sending JSON records
    records = df.to_dict(orient="records")
    resp = requests.post(f"{API_BASE_URL}/match", json={"records": records}, headers=_headers())
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data["results"])

def match_csv_file(file_bytes: bytes) -> pd.DataFrame:
    resp = requests.post(f"{API_BASE_URL}/match-csv", files={"file": ("upload.csv", file_bytes, "text/csv")}, headers=_headers())
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data["results"])
