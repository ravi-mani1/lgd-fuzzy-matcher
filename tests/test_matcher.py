"""Tests for matcher.py — core matching engine."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest
from matcher import LGDMatcher


class TestMatchState:
    def test_exact_match(self, matcher: LGDMatcher):
        r = matcher.match_state("Delhi")
        assert r["state_status"] == "EXACT"
        assert r["state_lgd_code"] == "7"

    def test_alias_match(self, matcher: LGDMatcher):
        r = matcher.match_state("UP")
        assert r["state_status"] == "EXACT"
        assert r["state_name_corrected"] == "Uttar Pradesh"

    def test_fuzzy_match(self, matcher: LGDMatcher):
        r = matcher.match_state("delhii")
        assert r["state_status"] in ("EXACT", "HIGH_CONFIDENCE")
        assert r["state_lgd_code"] == "7"

    def test_blank(self, matcher: LGDMatcher):
        r = matcher.match_state("")
        assert r["state_status"] == "NOT_FOUND"

    def test_unknown(self, matcher: LGDMatcher):
        r = matcher.match_state("XYZ Nonexistent")
        assert r["state_status"] == "NOT_FOUND"


class TestMatchDistrict:
    def test_exact_match(self, matcher: LGDMatcher):
        r = matcher.match_district("New Delhi", "7")
        assert r["district_status"] == "EXACT"
        assert r["district_lgd_code"] == "79"

    def test_alias_match(self, matcher: LGDMatcher):
        r = matcher.match_district("calcutta", "19")
        assert r["district_status"] == "EXACT"
        assert r["district_name_corrected"] == "Kolkata"

    def test_fuzzy_match(self, matcher: LGDMatcher):
        r = matcher.match_district("varansi", "9")
        assert r["district_status"] in ("EXACT", "HIGH_CONFIDENCE")
        assert "Varanasi" in (r["district_name_corrected"] or "")

    def test_blank(self, matcher: LGDMatcher):
        r = matcher.match_district("", "7")
        assert r["district_status"] == "NOT_FOUND"

    def test_global_mode_no_fuzzy(self, matcher: LGDMatcher):
        """Without a state, fuzzy matching should be skipped."""
        r = matcher.match_district("some random district", "")
        assert r["district_status"] == "NOT_FOUND"


class TestMatchDataframe:
    def test_batch(self, matcher: LGDMatcher, sample_df: pd.DataFrame):
        results = matcher.match_dataframe(sample_df)
        assert len(results) == 5
        assert "match_status" in results.columns
        # Delhi + New Delhi should be EXACT
        delhi_row = results[results["id"] == "1"].iloc[0]
        assert delhi_row["match_status"] in ("EXACT", "HIGH_CONFIDENCE")

    def test_adds_id_column(self, matcher: LGDMatcher):
        df = pd.DataFrame({"state_name_raw": ["Delhi"], "district_name_raw": ["New Delhi"]})
        results = matcher.match_dataframe(df)
        assert "id" in results.columns

    def test_progress_callback(self, matcher: LGDMatcher, sample_df: pd.DataFrame):
        calls = []
        matcher.match_dataframe(sample_df, progress_callback=lambda c, t: calls.append((c, t)))
        assert len(calls) >= 1


class TestSuggestions:
    def test_suggest_states(self, matcher: LGDMatcher):
        suggestions = matcher.suggest_states("delhii", limit=3)
        assert len(suggestions) >= 1
        assert suggestions[0]["state_name"] == "Delhi"

    def test_suggest_districts(self, matcher: LGDMatcher):
        suggestions = matcher.suggest_districts("varansi", state_lgd_code="9", limit=3)
        assert len(suggestions) >= 1

    def test_list_districts(self, matcher: LGDMatcher):
        districts = matcher.list_districts("7")  # Delhi
        assert len(districts) > 0
        names = [d["district_name"] for d in districts]
        assert "New Delhi" in names


class TestCacheDoesNotLeakMemory:
    def test_instance_cache_clears_on_rebuild(self):
        """Instance caches should be cleared when indices are rebuilt."""
        m = LGDMatcher(config_path=str(Path(__file__).resolve().parent.parent / "config.json"))
        m.load_master_from_csv(
            str(Path(__file__).resolve().parent.parent / "lgd_STATE.csv"),
            str(Path(__file__).resolve().parent.parent / "DISTRICT_STATE.csv"),
        )
        # Populate cache
        m.match_state("Delhi")
        assert len(m._state_cache) > 0
        # Reload should clear it
        m.load_master_from_csv(
            str(Path(__file__).resolve().parent.parent / "lgd_STATE.csv"),
            str(Path(__file__).resolve().parent.parent / "DISTRICT_STATE.csv"),
        )
        assert len(m._state_cache) == 0
