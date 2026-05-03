import os
import requests
import pandas as pd
from typing import List, Dict, Optional

# Allow overriding via env var. Set USE_LOCAL_MATCHER=true for Streamlit Community Cloud
USE_LOCAL_MATCHER = os.getenv("USE_LOCAL_MATCHER", "true").lower() == "true"

if USE_LOCAL_MATCHER:
    import streamlit as st
    from matcher import LGDMatcher
    import io
    
    @st.cache_resource
    def get_matcher() -> LGDMatcher:
        if not os.path.exists("lgd_master.db"):
            import build_db
            build_db.build_db()
        m = LGDMatcher("config.json")
        m.load_master_from_sqlite("lgd_master.db")
        return m

    def get_stats() -> dict:
        m = get_matcher()
        return {"states": len(m.state_df), "districts": len(m.district_df), "thresholds": m.thresholds}

    def list_states() -> List[Dict]:
        return get_matcher().list_states()

    def list_districts(state_lgd: str) -> List[Dict]:
        if not str(state_lgd).strip(): return []
        return get_matcher().list_districts(state_lgd)

    def list_subdistricts(district_lgd: str) -> List[Dict]:
        if not str(district_lgd).strip(): return []
        return get_matcher().list_subdistricts(district_lgd)

    def list_villages(subdistrict_lgd: str) -> List[Dict]:
        if not str(subdistrict_lgd).strip(): return []
        return get_matcher().list_villages(subdistrict_lgd)

    def suggest_states(q: str, limit: int = 5) -> List[Dict]:
        if not str(q).strip(): return []
        return get_matcher().suggest_states(q, limit)

    def suggest_districts(q: str, state_lgd: Optional[str] = None, limit: int = 5) -> List[Dict]:
        if not str(q).strip(): return []
        return get_matcher().suggest_districts(q, state_lgd, limit)

    def match_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        results = get_matcher().match_dataframe(df)
        return results.where(pd.notnull(results), None)
        
    def match_csv_file(file_bytes: bytes) -> pd.DataFrame:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
        results = get_matcher().match_dataframe(df)
        return results.where(pd.notnull(results), None)

else:
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
