"""matcher.py - Core LGD Fuzzy Matching Engine."""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import pandas as pd
from rapidfuzz import fuzz, process

from utils import is_blank, load_config, normalize_alias_map, normalize_text

logger = logging.getLogger("lgd_matcher")


class LGDMatcher:
    """
    Production-grade LGD fuzzy matcher.
    Pipeline: Exact -> Normalize -> Alias -> Fuzzy (rapidfuzz) -> State-first -> Score
    Optimized for 100K+ rows via pre-indexed dicts + per-instance cache on unique values.
    """

    def __init__(self, config_path: str = "config.json") -> None:
        self.config: dict = load_config(config_path)
        self.thresholds: dict = self.config["thresholds"]
        self.stop_words: list[str] = self.config.get("stop_words", [])
        self.state_aliases: dict[str, str] = normalize_alias_map(
            self.config.get("state_aliases", {}), self.stop_words
        )
        self.district_aliases: dict[str, str] = normalize_alias_map(
            self.config.get("district_aliases", {}), self.stop_words
        )
        self.state_df: Optional[pd.DataFrame] = None
        self.district_df: Optional[pd.DataFrame] = None
        self.subdistrict_df: Optional[pd.DataFrame] = None
        self.village_df: Optional[pd.DataFrame] = None
        # State indices
        self.state_exact_map: dict[str, dict] = {}
        self.state_norm_map: dict[str, dict] = {}
        self.state_choices: list[str] = []
        # District indices
        self.district_exact_by_state: dict[str, dict] = {}
        self.district_norm_by_state: dict[str, dict] = {}
        self.district_choices_by_state: dict[str, list[str]] = {}
        self.global_district_exact_map: dict[str, list[dict]] = {}
        self.global_district_norm_map: dict[str, list[dict]] = {}
        self.global_district_choices: list[str] = []
        # Sub-district indices (scoped by district)
        self.subdistrict_exact_by_district: dict[str, dict[str, dict]] = {}
        self.subdistrict_norm_by_district: dict[str, dict[str, dict]] = {}
        self.subdistrict_choices_by_district: dict[str, list[str]] = {}
        # Village indices (scoped by sub-district)
        self.village_exact_by_subdistrict: dict[str, dict[str, dict]] = {}
        self.village_norm_by_subdistrict: dict[str, dict[str, dict]] = {}
        self.village_choices_by_subdistrict: dict[str, list[str]] = {}
        # Instance-level caches (no lru_cache → no GC leak)
        self._state_cache: dict[str, dict] = {}
        self._district_cache: dict[tuple[str, str], dict] = {}
        self._subdistrict_cache: dict[tuple[str, str], dict] = {}
        self._village_cache: dict[tuple[str, str], dict] = {}

    def load_master_from_csv(
        self,
        state_csv: str,
        district_csv: str,
        subdistrict_csv: str | None = None,
        village_csv: str | None = None,
    ) -> None:
        self.state_df = pd.read_csv(state_csv, dtype=str).fillna("")
        self.district_df = pd.read_csv(district_csv, dtype=str).fillna("")
        self.state_df = self.state_df.rename(columns={"state_lgd": "state_lgd_code"})
        self.district_df = self.district_df.rename(
            columns={"state_lgd": "state_lgd_code", "district_lgd": "district_lgd_code"}
        )
        if subdistrict_csv:
            self.subdistrict_df = pd.read_csv(subdistrict_csv, dtype=str).fillna("")
            self.subdistrict_df = self.subdistrict_df.rename(columns={
                "subdistrict_lgd": "subdistrict_lgd_code", "district_lgd": "district_lgd_code",
                "state_lgd": "state_lgd_code",
            })
        if village_csv:
            self.village_df = pd.read_csv(village_csv, dtype=str).fillna("")
            self.village_df = self.village_df.rename(columns={
                "village_lgd": "village_lgd_code", "subdistrict_lgd": "subdistrict_lgd_code",
                "district_lgd": "district_lgd_code", "state_lgd": "state_lgd_code",
            })
        self._validate_master_columns()
        self._build_indices()

    def load_master_from_dataframes(
        self,
        state_df: pd.DataFrame,
        district_df: pd.DataFrame,
        subdistrict_df: pd.DataFrame | None = None,
        village_df: pd.DataFrame | None = None,
    ) -> None:
        self.state_df = state_df.copy().fillna("").rename(columns={"state_lgd": "state_lgd_code"})
        self.district_df = (
            district_df.copy()
            .fillna("")
            .rename(columns={"state_lgd": "state_lgd_code", "district_lgd": "district_lgd_code"})
        )
        if subdistrict_df is not None:
            self.subdistrict_df = subdistrict_df.copy().fillna("").rename(columns={
                "subdistrict_lgd": "subdistrict_lgd_code", "district_lgd": "district_lgd_code",
                "state_lgd": "state_lgd_code",
            })
        if village_df is not None:
            self.village_df = village_df.copy().fillna("").rename(columns={
                "village_lgd": "village_lgd_code", "subdistrict_lgd": "subdistrict_lgd_code",
                "district_lgd": "district_lgd_code", "state_lgd": "state_lgd_code",
            })
        self._validate_master_columns()
        self._build_indices()

    def load_master_from_sqlite(self, db_path: str = "lgd_master.db") -> None:
        import sqlite3
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise RuntimeError(f"Database {db_path} not found. Run build_db.py first.")
        
        self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.db_conn.row_factory = sqlite3.Row
        
        # Load States into memory (they are small)
        self.state_df = pd.read_sql("SELECT state_lgd_code, state_name FROM states", self.db_conn).fillna("")
        
        # Load Districts into memory (also small)
        self.district_df = pd.read_sql("SELECT district_lgd_code, district_name, state_lgd_code FROM districts", self.db_conn).fillna("")
        
        # Sub-districts and Villages are NOT loaded into DataFrames anymore
        # They will be fetched on demand to save memory
        self.subdistrict_df = None
        self.village_df = None
        
        # We set flags so batch processor knows they exist in DB
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='subdistricts'")
        self.has_subdistricts_db = cursor.fetchone()[0] > 0
        
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='villages'")
        self.has_villages_db = cursor.fetchone()[0] > 0
        
        self._validate_master_columns()
        self._build_indices()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _validate_master_columns(self) -> None:
        for df, required, label in [
            (self.state_df, {"state_lgd_code", "state_name"}, "State"),
            (self.district_df, {"district_lgd_code", "district_name", "state_lgd_code"}, "District"),
        ]:
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"{label} master missing columns: {sorted(missing)}")

    def _build_indices(self) -> None:
        """Build lookup dicts from master DataFrames.

        Uses ``itertuples()`` for speed (avoids the overhead of ``iterrows()``).
        Also validates that the ``pradesh`` stop-word (if present) does not
        cause any two state names to collide after normalization.
        """
        logger.info("Building indices...")

        # -- States -------------------------------------------------------
        for row in self.state_df.itertuples(index=False):
            rec = {
                "state_lgd_code": str(row.state_lgd_code).strip(),
                "state_name": str(row.state_name).strip(),
            }
            self.state_exact_map[rec["state_name"].lower()] = rec
            norm_key = normalize_text(rec["state_name"], self.stop_words)
            if norm_key in self.state_norm_map:
                existing = self.state_norm_map[norm_key]
                if existing["state_lgd_code"] != rec["state_lgd_code"]:
                    logger.warning(
                        "Stop-word collision: '%s' and '%s' both normalize to '%s'. "
                        "Review stop_words in config.json.",
                        existing["state_name"],
                        rec["state_name"],
                        norm_key,
                    )
            self.state_norm_map[norm_key] = rec
        self.state_choices = list(self.state_norm_map.keys())

        # -- Districts ----------------------------------------------------
        for row in self.district_df.itertuples(index=False):
            rec = {
                "district_lgd_code": str(row.district_lgd_code).strip(),
                "district_name": str(row.district_name).strip(),
                "state_lgd_code": str(row.state_lgd_code).strip(),
            }
            sc = rec["state_lgd_code"]
            raw_k = rec["district_name"].lower()
            norm_k = normalize_text(rec["district_name"], self.stop_words)
            self.district_exact_by_state.setdefault(sc, {})[raw_k] = rec
            self.district_norm_by_state.setdefault(sc, {})[norm_k] = rec
            self.global_district_exact_map.setdefault(raw_k, []).append(rec)
            self.global_district_norm_map.setdefault(norm_k, []).append(rec)

        self.district_choices_by_state = {
            sc: list(d.keys()) for sc, d in self.district_norm_by_state.items()
        }
        self.global_district_choices = list(self.global_district_norm_map.keys())

        # -- Sub-districts (scoped by district) ----------------------------
        if self.subdistrict_df is not None:
            for row in self.subdistrict_df.itertuples(index=False):
                rec = {
                    "subdistrict_lgd_code": str(row.subdistrict_lgd_code).strip(),
                    "subdistrict_name": str(row.subdistrict_name).strip(),
                    "district_lgd_code": str(row.district_lgd_code).strip(),
                }
                dc = rec["district_lgd_code"]
                raw_k = rec["subdistrict_name"].lower()
                norm_k = normalize_text(rec["subdistrict_name"], self.stop_words)
                self.subdistrict_exact_by_district.setdefault(dc, {})[raw_k] = rec
                self.subdistrict_norm_by_district.setdefault(dc, {})[norm_k] = rec
            self.subdistrict_choices_by_district = {
                dc: list(d.keys()) for dc, d in self.subdistrict_norm_by_district.items()
            }

        # -- Villages (scoped by sub-district) -----------------------------
        if self.village_df is not None:
            for row in self.village_df.itertuples(index=False):
                rec = {
                    "village_lgd_code": str(row.village_lgd_code).strip(),
                    "village_name": str(row.village_name).strip(),
                    "subdistrict_lgd_code": str(row.subdistrict_lgd_code).strip(),
                }
                sdc = rec["subdistrict_lgd_code"]
                raw_k = rec["village_name"].lower()
                norm_k = normalize_text(rec["village_name"], self.stop_words)
                self.village_exact_by_subdistrict.setdefault(sdc, {})[raw_k] = rec
                self.village_norm_by_subdistrict.setdefault(sdc, {})[norm_k] = rec
            self.village_choices_by_subdistrict = {
                sdc: list(d.keys()) for sdc, d in self.village_norm_by_subdistrict.items()
            }

        # Clear instance caches after rebuilding indices.
        self._state_cache.clear()
        self._district_cache.clear()
        self._subdistrict_cache.clear()
        self._village_cache.clear()

        sd_count = len(self.subdistrict_df) if self.subdistrict_df is not None else 0
        vl_count = len(self.village_df) if self.village_df is not None else 0
        logger.info(
            "Indices ready | states=%d | districts=%d | subdistricts=%d | villages=%d",
            len(self.state_df),
            len(self.district_df),
            sd_count,
            vl_count,
        )

    # ------------------------------------------------------------------
    # Public listing helpers
    # ------------------------------------------------------------------
    def list_states(self) -> list[dict]:
        if self.state_df is None:
            raise RuntimeError("Master data not loaded. Call load_master_from_csv/load_master_from_mysql first.")
        out = (
            self.state_df[["state_lgd_code", "state_name"]]
            .dropna()
            .astype(str)
            .assign(
                state_lgd_code=lambda d: d["state_lgd_code"].str.strip(),
                state_name=lambda d: d["state_name"].str.strip(),
            )
        )
        out = out[(out["state_lgd_code"] != "") & (out["state_name"] != "")]
        out = out.drop_duplicates().sort_values(["state_name", "state_lgd_code"])
        return out.to_dict(orient="records")

    def list_districts(self, state_lgd_code: str) -> list[dict]:
        if self.district_df is None:
            raise RuntimeError("Master data not loaded. Call load_master_from_csv/load_master_from_mysql first.")
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()
        if not sc:
            return []
        df = self.district_df.copy()
        df["state_lgd_code"] = df["state_lgd_code"].astype(str).str.strip()
        df["district_lgd_code"] = df["district_lgd_code"].astype(str).str.strip()
        df["district_name"] = df["district_name"].astype(str).str.strip()
        df = df[
            (df["state_lgd_code"] == sc) & (df["district_lgd_code"] != "") & (df["district_name"] != "")
        ]
        df = df[["district_lgd_code", "district_name"]].drop_duplicates().sort_values(
            ["district_name", "district_lgd_code"]
        )
        return df.to_dict(orient="records")

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------
    def _status(self, score: float, exact: bool = False) -> str:
        if exact:
            return "EXACT"
        t = self.thresholds
        if score >= t["high_confidence"]:
            return "HIGH_CONFIDENCE"
        if score >= t["medium_confidence"]:
            return "MEDIUM_CONFIDENCE"
        if score >= t["low_confidence"]:
            return "LOW_CONFIDENCE"
        return "NOT_FOUND"

    def _best_fuzzy(self, query: str, choices: list[str]) -> tuple[str | None, float]:
        if not query or not choices:
            return None, 0.0
        results: list[tuple[str, float]] = []
        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):
            r = process.extractOne(query, choices, scorer=scorer, processor=None)
            if r:
                results.append((r[0], float(r[1])))
        return max(results, key=lambda x: x[1]) if results else (None, 0.0)

    def _top_fuzzy(
        self, query: str, choices: list[str], limit: int = 5
    ) -> list[tuple[str, float]]:
        if not query or not choices or limit <= 0:
            return []
        scores: dict[str, float] = {}
        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):
            for c, s, _ in process.extract(query, choices, scorer=scorer, processor=None, limit=limit):
                s = float(s)
                if c not in scores or s > scores[c]:
                    scores[c] = s
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]

    # ------------------------------------------------------------------
    # Suggestion helpers
    # ------------------------------------------------------------------
    def suggest_states(self, raw_state: str, limit: int = 5) -> list[dict]:
        if is_blank(raw_state):
            return []
        norm = normalize_text(raw_state, self.stop_words)
        if not norm:
            return []
        query = normalize_text(self.state_aliases.get(norm, norm), self.stop_words)
        out: list[dict] = []
        for choice, score in self._top_fuzzy(query, self.state_choices, limit=limit):
            rec = self.state_norm_map.get(choice)
            if not rec:
                continue
            out.append({
                "state_lgd_code": rec["state_lgd_code"],
                "state_name": rec["state_name"],
                "score": round(score, 2),
                "status": self._status(score),
            })
        return out

    def suggest_districts(
        self,
        raw_district: str,
        state_lgd_code: str | None = None,
        limit: int = 5,
    ) -> list[dict]:
        if is_blank(raw_district):
            return []
        norm = normalize_text(raw_district, self.stop_words)
        if not norm:
            return []
        query = normalize_text(self.district_aliases.get(norm, norm), self.stop_words)
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()
        norm_map = self.district_norm_by_state.get(sc, {}) if sc else self.global_district_norm_map
        choices = self.district_choices_by_state.get(sc, []) if sc else self.global_district_choices
        global_mode = not bool(sc)

        out: list[dict] = []
        for choice, score in self._top_fuzzy(query, choices, limit=limit):
            cand = norm_map.get(choice)
            if cand is None:
                continue
            if global_mode:
                for rec in cand:
                    out.append({
                        "district_lgd_code": rec["district_lgd_code"],
                        "district_name": rec["district_name"],
                        "state_lgd_code": rec.get("state_lgd_code"),
                        "score": round(score, 2),
                        "status": self._status(score),
                    })
            else:
                out.append({
                    "district_lgd_code": cand["district_lgd_code"],
                    "district_name": cand["district_name"],
                    "state_lgd_code": cand.get("state_lgd_code"),
                    "score": round(score, 2),
                    "status": self._status(score),
                })

        seen: set[tuple] = set()
        deduped: list[dict] = []
        for r in sorted(out, key=lambda x: x["score"], reverse=True):
            k = (r.get("state_lgd_code"), r.get("district_lgd_code"))
            if k in seen:
                continue
            seen.add(k)
            deduped.append(r)
            if len(deduped) >= limit:
                break
        return deduped

    # ------------------------------------------------------------------
    # Core matching — dict-based cache (replaces @lru_cache)
    # ------------------------------------------------------------------
    def match_state(self, raw_state: str) -> dict:
        if raw_state in self._state_cache:
            return self._state_cache[raw_state]
        result = self._match_state_impl(raw_state)
        self._state_cache[raw_state] = result
        return result

    def _match_state_impl(self, raw_state: str) -> dict:
        empty: dict[str, Any] = {
            "state_lgd_code": None,
            "state_name_corrected": None,
            "state_score": 0.0,
            "state_status": "NOT_FOUND",
        }
        if is_blank(raw_state):
            return empty
        raw = str(raw_state).strip()
        if raw.lower() in self.state_exact_map:
            m = self.state_exact_map[raw.lower()]
            return {
                "state_lgd_code": m["state_lgd_code"],
                "state_name_corrected": m["state_name"],
                "state_score": 100.0,
                "state_status": "EXACT",
            }
        norm = normalize_text(raw, self.stop_words)
        if not norm:
            return empty
        if norm in self.state_norm_map:
            m = self.state_norm_map[norm]
            return {
                "state_lgd_code": m["state_lgd_code"],
                "state_name_corrected": m["state_name"],
                "state_score": 100.0,
                "state_status": "EXACT",
            }
        query = norm
        alias_val = self.state_aliases.get(norm)
        if alias_val:
            alias_norm = normalize_text(alias_val, self.stop_words)
            if alias_norm in self.state_norm_map:
                m = self.state_norm_map[alias_norm]
                return {
                    "state_lgd_code": m["state_lgd_code"],
                    "state_name_corrected": m["state_name"],
                    "state_score": 100.0,
                    "state_status": "EXACT",
                }
            query = alias_norm
        choice, score = self._best_fuzzy(query, self.state_choices)
        if choice is None or score < self.thresholds["low_confidence"]:
            return empty
        m = self.state_norm_map[choice]
        return {
            "state_lgd_code": m["state_lgd_code"],
            "state_name_corrected": m["state_name"],
            "state_score": round(score, 2),
            "state_status": self._status(score),
        }

    def match_district(self, raw_district: str, state_lgd_code: str) -> dict:
        cache_key = (raw_district, state_lgd_code)
        if cache_key in self._district_cache:
            return self._district_cache[cache_key]
        result = self._match_district_impl(raw_district, state_lgd_code)
        self._district_cache[cache_key] = result
        return result

    def _match_district_impl(self, raw_district: str, state_lgd_code: str) -> dict:
        empty: dict[str, Any] = {
            "district_lgd_code": None,
            "district_name_corrected": None,
            "district_score": 0.0,
            "district_status": "NOT_FOUND",
        }
        if is_blank(raw_district):
            return empty
        raw = str(raw_district).strip()
        raw_k = raw.lower()
        norm = normalize_text(raw, self.stop_words)
        if not norm:
            return empty
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()

        exact_map = self.district_exact_by_state.get(sc, {}) if sc else self.global_district_exact_map
        norm_map = self.district_norm_by_state.get(sc, {}) if sc else self.global_district_norm_map
        choices = self.district_choices_by_state.get(sc, []) if sc else self.global_district_choices
        global_mode = not bool(sc)

        def _get(mapping: dict, key: str) -> dict | None:
            v = mapping.get(key)
            if v is None:
                return None
            if global_mode:
                if len(v) == 1:
                    return v[0]
                logger.warning("Ambiguous district '%s' across states; skipping.", raw_district)
                return None
            return v

        if cand := _get(exact_map, raw_k):
            return {
                "district_lgd_code": cand["district_lgd_code"],
                "district_name_corrected": cand["district_name"],
                "district_score": 100.0,
                "district_status": "EXACT",
            }
        if cand := _get(norm_map, norm):
            return {
                "district_lgd_code": cand["district_lgd_code"],
                "district_name_corrected": cand["district_name"],
                "district_score": 100.0,
                "district_status": "EXACT",
            }

        query = norm
        alias_val = self.district_aliases.get(norm)
        if alias_val:
            alias_norm = normalize_text(alias_val, self.stop_words)
            if cand := _get(norm_map, alias_norm):
                return {
                    "district_lgd_code": cand["district_lgd_code"],
                    "district_name_corrected": cand["district_name"],
                    "district_score": 100.0,
                    "district_status": "EXACT",
                }
            query = alias_norm

        # If state is unknown, avoid "guessing" districts via fuzzy search.
        # Use suggest_districts() to guide the user instead.
        if global_mode:
            return empty

        choice, score = self._best_fuzzy(query, choices)
        if choice is None or score < self.thresholds["low_confidence"]:
            return empty
        if cand := _get(norm_map, choice):
            return {
                "district_lgd_code": cand["district_lgd_code"],
                "district_name_corrected": cand["district_name"],
                "district_score": round(score, 2),
                "district_status": self._status(score),
            }
        return empty

    # ------------------------------------------------------------------
    # Sub-district matching (scoped by district)
    # ------------------------------------------------------------------
    def list_subdistricts(self, district_lgd_code: str) -> list[dict]:
        dc = "" if is_blank(district_lgd_code) else str(district_lgd_code).strip()
        if not dc:
            return []
            
        if getattr(self, "has_subdistricts_db", False):
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT subdistrict_lgd_code, subdistrict_name FROM subdistricts WHERE district_lgd_code = ?", (dc,))
            return [{"subdistrict_lgd_code": str(r[0]), "subdistrict_name": r[1]} for r in cursor.fetchall()]
            
        if self.subdistrict_df is None:
            return []
        return [
            {"subdistrict_lgd_code": v["subdistrict_lgd_code"], "subdistrict_name": v["subdistrict_name"]}
            for v in self.subdistrict_norm_by_district.get(dc, {}).values()
        ]

    def match_subdistrict(self, raw_subdistrict: str, district_lgd_code: str) -> dict:
        cache_key = (raw_subdistrict, district_lgd_code)
        if cache_key in self._subdistrict_cache:
            return self._subdistrict_cache[cache_key]
        result = self._match_subdistrict_impl(raw_subdistrict, district_lgd_code)
        self._subdistrict_cache[cache_key] = result
        return result

    def _match_subdistrict_impl(self, raw_subdistrict: str, district_lgd_code: str) -> dict:
        empty: dict[str, Any] = {
            "subdistrict_lgd_code": None, "subdistrict_name_corrected": None,
            "subdistrict_score": 0.0, "subdistrict_status": "NOT_FOUND",
        }
        if is_blank(raw_subdistrict) or is_blank(district_lgd_code):
            return empty
        dc = str(district_lgd_code).strip()
        raw = str(raw_subdistrict).strip()
        
        # Lazy-load from DB into memory cache
        if getattr(self, "has_subdistricts_db", False) and dc not in self.subdistrict_exact_by_district:
            exact_map, norm_map, choices = {}, {}, []
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT subdistrict_lgd_code, subdistrict_name, subdistrict_name_norm, subdistrict_name_phonetic FROM subdistricts WHERE district_lgd_code = ?", (dc,))
            for r in cursor.fetchall():
                rec = {"subdistrict_lgd_code": str(r[0]), "subdistrict_name": r[1], "subdistrict_name_phonetic": r[3]}
                exact_map[r[1].lower()] = rec
                norm_map[r[2]] = rec
                choices.append(r[2])
            self.subdistrict_exact_by_district[dc] = exact_map
            self.subdistrict_norm_by_district[dc] = norm_map
            self.subdistrict_choices_by_district[dc] = choices

        exact_map = self.subdistrict_exact_by_district.get(dc, {})
        norm_map = self.subdistrict_norm_by_district.get(dc, {})
        choices = self.subdistrict_choices_by_district.get(dc, [])

        if cand := exact_map.get(raw.lower()):
            return {"subdistrict_lgd_code": cand["subdistrict_lgd_code"],
                    "subdistrict_name_corrected": cand["subdistrict_name"],
                    "subdistrict_score": 100.0, "subdistrict_status": "EXACT"}
        norm = normalize_text(raw, self.stop_words)
        if not norm:
            return empty
        if cand := norm_map.get(norm):
            return {"subdistrict_lgd_code": cand["subdistrict_lgd_code"],
                    "subdistrict_name_corrected": cand["subdistrict_name"],
                    "subdistrict_score": 100.0, "subdistrict_status": "EXACT"}

        choice, score = self._best_fuzzy(norm, choices)
        
        # Phonetic Fallback via Jellyfish Metaphone
        if (choice is None or score < self.thresholds["low_confidence"]) and getattr(self, "has_subdistricts_db", False):
            import jellyfish
            ph_hash = jellyfish.metaphone(norm)
            ph_matches = [k for k, v in norm_map.items() if v.get("subdistrict_name_phonetic") == ph_hash]
            if ph_matches:
                ph_choice, ph_score = self._best_fuzzy(norm, ph_matches)
                if ph_choice:
                    cand = norm_map[ph_choice]
                    return {"subdistrict_lgd_code": cand["subdistrict_lgd_code"],
                            "subdistrict_name_corrected": cand["subdistrict_name"],
                            "subdistrict_score": max(round(ph_score, 2), self.thresholds["low_confidence"]),
                            "subdistrict_status": self._status(max(ph_score, self.thresholds["low_confidence"]))}
        
        if choice is None or score < self.thresholds["low_confidence"]:
            return empty
            
        if cand := norm_map.get(choice):
            return {"subdistrict_lgd_code": cand["subdistrict_lgd_code"],
                    "subdistrict_name_corrected": cand["subdistrict_name"],
                    "subdistrict_score": round(score, 2),
                    "subdistrict_status": self._status(score)}
        return empty

    # ------------------------------------------------------------------
    # Village matching (scoped by sub-district)
    # ------------------------------------------------------------------
    def list_villages(self, subdistrict_lgd_code: str) -> list[dict]:
        sdc = "" if is_blank(subdistrict_lgd_code) else str(subdistrict_lgd_code).strip()
        if not sdc:
            return []
            
        if getattr(self, "has_villages_db", False):
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT village_lgd_code, village_name FROM villages WHERE subdistrict_lgd_code = ?", (sdc,))
            return [{"village_lgd_code": str(r[0]), "village_name": r[1]} for r in cursor.fetchall()]
            
        if self.village_df is None:
            return []
        return [
            {"village_lgd_code": v["village_lgd_code"], "village_name": v["village_name"]}
            for v in self.village_norm_by_subdistrict.get(sdc, {}).values()
        ]

    def match_village(self, raw_village: str, subdistrict_lgd_code: str) -> dict:
        cache_key = (raw_village, subdistrict_lgd_code)
        if cache_key in self._village_cache:
            return self._village_cache[cache_key]
        result = self._match_village_impl(raw_village, subdistrict_lgd_code)
        self._village_cache[cache_key] = result
        return result

    def _match_village_impl(self, raw_village: str, subdistrict_lgd_code: str) -> dict:
        empty: dict[str, Any] = {
            "village_lgd_code": None, "village_name_corrected": None,
            "village_score": 0.0, "village_status": "NOT_FOUND",
        }
        if is_blank(raw_village) or is_blank(subdistrict_lgd_code):
            return empty
        sdc = str(subdistrict_lgd_code).strip()
        raw = str(raw_village).strip()
        
        # Lazy-load from DB into memory cache
        if getattr(self, "has_villages_db", False) and sdc not in self.village_exact_by_subdistrict:
            exact_map, norm_map, choices = {}, {}, []
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT village_lgd_code, village_name, village_name_norm, village_name_phonetic FROM villages WHERE subdistrict_lgd_code = ?", (sdc,))
            for r in cursor.fetchall():
                rec = {"village_lgd_code": str(r[0]), "village_name": r[1], "village_name_phonetic": r[3]}
                exact_map[r[1].lower()] = rec
                norm_map[r[2]] = rec
                choices.append(r[2])
            self.village_exact_by_subdistrict[sdc] = exact_map
            self.village_norm_by_subdistrict[sdc] = norm_map
            self.village_choices_by_subdistrict[sdc] = choices

        exact_map = self.village_exact_by_subdistrict.get(sdc, {})
        norm_map = self.village_norm_by_subdistrict.get(sdc, {})
        choices = self.village_choices_by_subdistrict.get(sdc, [])

        if cand := exact_map.get(raw.lower()):
            return {"village_lgd_code": cand["village_lgd_code"],
                    "village_name_corrected": cand["village_name"],
                    "village_score": 100.0, "village_status": "EXACT"}
        norm = normalize_text(raw, self.stop_words)
        if not norm:
            return empty
        if cand := norm_map.get(norm):
            return {"village_lgd_code": cand["village_lgd_code"],
                    "village_name_corrected": cand["village_name"],
                    "village_score": 100.0, "village_status": "EXACT"}

        choice, score = self._best_fuzzy(norm, choices)
        
        # Phonetic Fallback via Jellyfish Metaphone
        if (choice is None or score < self.thresholds["low_confidence"]) and getattr(self, "has_villages_db", False):
            import jellyfish
            ph_hash = jellyfish.metaphone(norm)
            # Find any village in this sub-district with the same phonetic hash
            ph_matches = [k for k, v in norm_map.items() if v.get("village_name_phonetic") == ph_hash]
            if ph_matches:
                # If multiple match phonetically, pick the one with highest fuzzy score
                ph_choice, ph_score = self._best_fuzzy(norm, ph_matches)
                if ph_choice:
                    cand = norm_map[ph_choice]
                    return {"village_lgd_code": cand["village_lgd_code"],
                            "village_name_corrected": cand["village_name"],
                            # Boost score slightly to pass threshold if phonetic matches
                            "village_score": max(round(ph_score, 2), self.thresholds["low_confidence"]),
                            "village_status": self._status(max(ph_score, self.thresholds["low_confidence"]))}
        
        if choice is None or score < self.thresholds["low_confidence"]:
            return empty
            
        if cand := norm_map.get(choice):
            return {"village_lgd_code": cand["village_lgd_code"],
                    "village_name_corrected": cand["village_name"],
                    "village_score": round(score, 2),
                    "village_status": self._status(score)}
        return empty

    # ------------------------------------------------------------------
    # Batch matching
    # ------------------------------------------------------------------
    def match_dataframe(
        self,
        df: pd.DataFrame,
        progress_callback: Any | None = None,
    ) -> pd.DataFrame:
        """Match every row. Sub-district/village columns are processed if present."""
        data = df.copy()
        if "id" not in data.columns:
            data.insert(0, "id", range(1, len(data) + 1))
        for col in ["state_name_raw", "district_name_raw"]:
            if col not in data.columns:
                data[col] = ""
        data["state_name_raw"] = data["state_name_raw"].fillna("").astype(str)
        data["district_name_raw"] = data["district_name_raw"].fillna("").astype(str)

        has_subdistrict = "subdistrict_name_raw" in data.columns and (self.subdistrict_df is not None or getattr(self, "has_subdistricts_db", False))
        has_village = "village_name_raw" in data.columns and (self.village_df is not None or getattr(self, "has_villages_db", False))
        if has_subdistrict:
            data["subdistrict_name_raw"] = data["subdistrict_name_raw"].fillna("").astype(str)
        if has_village:
            data["village_name_raw"] = data["village_name_raw"].fillna("").astype(str)

        total = len(data)
        logger.info("Matching %d rows (subdistrict=%s, village=%s)...", total, has_subdistrict, has_village)

        # ── State matching ────────────────────────────────────────────
        unique_states = data["state_name_raw"].unique().tolist()
        state_cache = {s: self.match_state(s) for s in unique_states}
        data["_sm"] = data["state_name_raw"].map(state_cache)
        data["state_lgd_code"] = data["_sm"].map(lambda x: x["state_lgd_code"])
        data["state_name_corrected"] = data["_sm"].map(lambda x: x["state_name_corrected"])
        data["_ss"] = data["_sm"].map(lambda x: x["state_score"])
        data["_sst"] = data["_sm"].map(lambda x: x["state_status"])
        if progress_callback:
            progress_callback(total // 5, total)

        # ── District matching ─────────────────────────────────────────
        pairs = data[["district_name_raw", "state_lgd_code"]].drop_duplicates()
        dist_cache: dict[tuple, dict] = {}
        for _, r in pairs.iterrows():
            key = (str(r["district_name_raw"]), "" if is_blank(r["state_lgd_code"]) else str(r["state_lgd_code"]))
            dist_cache[key] = self.match_district(*key)
        data["_dk"] = list(zip(data["district_name_raw"], data["state_lgd_code"].fillna("").astype(str)))
        data["_dm"] = data["_dk"].map(dist_cache)
        data["district_lgd_code"] = data["_dm"].map(lambda x: x["district_lgd_code"])
        data["district_name_corrected"] = data["_dm"].map(lambda x: x["district_name_corrected"])
        data["_ds"] = data["_dm"].map(lambda x: x["district_score"])
        data["_dst"] = data["_dm"].map(lambda x: x["district_status"])
        if progress_callback:
            progress_callback(total * 2 // 5, total)

        # ── Sub-district matching ─────────────────────────────────────
        if has_subdistrict:
            sd_pairs = data[["subdistrict_name_raw", "district_lgd_code"]].drop_duplicates()
            sd_cache: dict[tuple, dict] = {}
            for _, r in sd_pairs.iterrows():
                key = (str(r["subdistrict_name_raw"]), "" if is_blank(r["district_lgd_code"]) else str(r["district_lgd_code"]))
                sd_cache[key] = self.match_subdistrict(*key)
            data["_sdk"] = list(zip(data["subdistrict_name_raw"], data["district_lgd_code"].fillna("").astype(str)))
            data["_sdm"] = data["_sdk"].map(sd_cache)
            data["subdistrict_lgd_code"] = data["_sdm"].map(lambda x: x["subdistrict_lgd_code"])
            data["subdistrict_name_corrected"] = data["_sdm"].map(lambda x: x["subdistrict_name_corrected"])
            data["_sds"] = data["_sdm"].map(lambda x: x["subdistrict_score"])
            data["_sdst"] = data["_sdm"].map(lambda x: x["subdistrict_status"])
        if progress_callback:
            progress_callback(total * 3 // 5, total)

        # ── Village matching ──────────────────────────────────────────
        if has_village and has_subdistrict:
            vl_pairs = data[["village_name_raw", "subdistrict_lgd_code"]].drop_duplicates()
            vl_cache: dict[tuple, dict] = {}
            for _, r in vl_pairs.iterrows():
                key = (str(r["village_name_raw"]), "" if is_blank(r["subdistrict_lgd_code"]) else str(r["subdistrict_lgd_code"]))
                vl_cache[key] = self.match_village(*key)
            data["_vlk"] = list(zip(data["village_name_raw"], data["subdistrict_lgd_code"].fillna("").astype(str)))
            data["_vlm"] = data["_vlk"].map(vl_cache)
            data["village_lgd_code"] = data["_vlm"].map(lambda x: x["village_lgd_code"])
            data["village_name_corrected"] = data["_vlm"].map(lambda x: x["village_name_corrected"])
            data["_vls"] = data["_vlm"].map(lambda x: x["village_score"])
            data["_vlst"] = data["_vlm"].map(lambda x: x["village_status"])
        if progress_callback:
            progress_callback(total * 4 // 5, total)

        # ── Scoring — dynamic weights based on matched levels ─────────
        def calc_score_and_status(row: pd.Series) -> pd.Series:
            has_sd_val = has_subdistrict and not is_blank(row.get("subdistrict_name_raw"))
            has_v_val = has_village and not is_blank(row.get("village_name_raw"))
            
            # Gather statuses
            statuses = [row["_sst"], row["_dst"]]
            if has_sd_val:
                statuses.append(row["_sdst"])
            if has_v_val and has_sd_val:
                statuses.append(row["_vlst"])
                
            # Calculate dynamic score
            if has_v_val and has_sd_val:
                score = (row["_ss"] * 0.15 + row["_ds"] * 0.25 + row.get("_sds", 0) * 0.25 + row.get("_vls", 0) * 0.35)
            elif has_sd_val:
                score = (row["_ss"] * 0.25 + row["_ds"] * 0.35 + row.get("_sds", 0) * 0.40)
            else:
                score = (row["_ss"] * 0.40 + row["_ds"] * 0.60)
            score = round(score, 2)
            
            # Determine final status
            if any(s == "NOT_FOUND" for s in statuses):
                status = "NOT_FOUND"
            elif all(s == "EXACT" for s in statuses):
                status = "EXACT"
            elif score >= self.thresholds["high_confidence"]:
                status = "HIGH_CONFIDENCE"
            elif score >= self.thresholds["medium_confidence"]:
                status = "MEDIUM_CONFIDENCE"
            elif score >= self.thresholds["low_confidence"]:
                status = "LOW_CONFIDENCE"
            else:
                status = "NOT_FOUND"
                
            return pd.Series({"match_confidence_score": score, "match_status": status})

        res_cols = data.apply(calc_score_and_status, axis=1)
        data["match_confidence_score"] = res_cols["match_confidence_score"]
        data["match_status"] = res_cols["match_status"]
        
        if progress_callback:
            progress_callback(total, total)

        out = ["id", "state_name_raw", "district_name_raw", "state_lgd_code",
               "state_name_corrected", "district_lgd_code", "district_name_corrected"]
        if has_subdistrict:
            out += ["subdistrict_name_raw", "subdistrict_lgd_code", "subdistrict_name_corrected"]
        if has_village and has_subdistrict:
            out += ["village_name_raw", "village_lgd_code", "village_name_corrected"]
        out += ["match_confidence_score", "match_status"]
        # Only include columns that exist
        out = [c for c in out if c in data.columns]
        return data[out].copy()

