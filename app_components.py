"""app_components.py - Reusable UI components for the Streamlit app."""
from __future__ import annotations

import io
from typing import Any

import pandas as pd
import streamlit as st

from matcher import LGDMatcher
from utils import generate_sql_update, is_blank


# ---------------------------------------------------------------------------
# Styling helpers
# ---------------------------------------------------------------------------
_STATUS_COLORS: dict[str, str] = {
    "EXACT": "#dcfce7",
    "HIGH_CONFIDENCE": "#dbeafe",
    "MEDIUM_CONFIDENCE": "#fef3c7",
    "LOW_CONFIDENCE": "#fee2e2",
    "NOT_FOUND": "#f3f4f6",
}


def row_style(row: pd.Series) -> list[str]:
    c = _STATUS_COLORS.get(row.get("match_status", "") or row.get("Status", ""), "")
    return [f"background-color:{c}"] * len(row)


def suggestion_row_style(row: pd.Series) -> list[str]:
    t = str(row.get("type", ""))
    if "PREFIX" in t or "ALL" in t:
        c = "#dcfce7"
    elif "IN_STATE" in t:
        c = "#dbeafe"
    elif "ANY_STATE" in t:
        c = "#fef3c7"
    elif t == "STATE":
        c = "#e0e7ff"
    else:
        c = "#f9fafb"
    return [f"background-color:{c}"] * len(row)


# ---------------------------------------------------------------------------
# Data conversion helpers
# ---------------------------------------------------------------------------
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Export DataFrame to an in-memory Excel file with status-row colouring."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
    return buf.getvalue()


def to_sql_bytes(df: pd.DataFrame, table: str) -> bytes:
    tmp = "tmp_sql_export.sql"
    generate_sql_update(df, table_name=table, output_path=tmp)
    from pathlib import Path

    return Path(tmp).read_bytes()


# ---------------------------------------------------------------------------
# Matcher-aware helpers
# ---------------------------------------------------------------------------
def state_from_lgd(matcher: LGDMatcher, state_lgd_code: str) -> dict | None:
    if not state_lgd_code:
        return None
    df = matcher.state_df
    if df is None:
        return None
    code = str(state_lgd_code).strip()
    hit = df[df["state_lgd_code"].astype(str).str.strip() == code]
    if hit.empty:
        return None
    r = hit.iloc[0]
    return {"state_lgd_code": code, "state_name": str(r["state_name"]).strip()}


def district_from_lgd(
    matcher: LGDMatcher,
    district_lgd_code: str,
    state_lgd_code: str | None = None,
) -> dict | None:
    if not district_lgd_code:
        return None
    df = matcher.district_df
    if df is None:
        return None
    dc = str(district_lgd_code).strip()
    ddf = df.copy()
    ddf["district_lgd_code"] = ddf["district_lgd_code"].astype(str).str.strip()
    if state_lgd_code:
        sc = str(state_lgd_code).strip()
        ddf["state_lgd_code"] = ddf["state_lgd_code"].astype(str).str.strip()
        hit = ddf[(ddf["district_lgd_code"] == dc) & (ddf["state_lgd_code"] == sc)]
    else:
        hit = ddf[ddf["district_lgd_code"] == dc]
    if hit.empty:
        return None
    r = hit.iloc[0]
    return {
        "district_lgd_code": dc,
        "district_name": str(r["district_name"]).strip(),
        "state_lgd_code": str(r["state_lgd_code"]).strip(),
    }


import api_client

def district_prefix_list_in_state(
    state_lgd_code: str,
    prefix: str,
) -> list[dict]:
    districts = api_client.list_districts(state_lgd_code) if state_lgd_code else []
    p = (prefix or "").strip().lower()
    if not p:
        return districts
    return [d for d in districts if str(d.get("district_name", "")).strip().lower().startswith(p)]

# ---------------------------------------------------------------------------
# CSV value helpers
# ---------------------------------------------------------------------------
def split_csv_values(s: str | None) -> list[str]:
    if s is None:
        return []
    parts = [p.strip() for p in str(s).split(",")]
    return [p for p in parts if p]


def build_rows(
    state_name: str,
    state_lgd: str,
    dist_name: str,
    dist_lgd: str,
    subdist_name: str = "",
    village_name: str = "",
) -> list[dict]:
    a = split_csv_values(state_name)
    b = split_csv_values(state_lgd)
    c = split_csv_values(dist_name)
    d = split_csv_values(dist_lgd)
    e = split_csv_values(subdist_name)
    f = split_csv_values(village_name)
    n = max(len(a), len(b), len(c), len(d), len(e), len(f), 1)

    def expand(lst: list[str]) -> list[str]:
        if not lst:
            return [""] * n
        if len(lst) == 1 and n > 1:
            return lst * n
        if len(lst) < n:
            return lst + [""] * (n - len(lst))
        return lst[:n]

    a, b, c, d, e, f = expand(a), expand(b), expand(c), expand(d), expand(e), expand(f)
    rows: list[dict] = []
    for i in range(n):
        rows.append({
            "id": str(i + 1),
            "state_name_in": a[i],
            "state_lgd_in": b[i],
            "district_name_in": c[i],
            "district_lgd_in": d[i],
            "subdist_name_in": e[i],
            "village_name_in": f[i],
        })
    return rows
