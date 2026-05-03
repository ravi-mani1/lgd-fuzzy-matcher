"""Tests for sub-district and village matching."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest
from matcher import LGDMatcher

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def village_matcher() -> LGDMatcher:
    """Matcher with full hierarchy: state → district → sub-district → village."""
    m = LGDMatcher(config_path=str(PROJECT_ROOT / "config.json"))
    db_path = PROJECT_ROOT / "lgd_master.db"
    if not db_path.exists():
        pytest.skip("lgd_master.db not found. Please run build_db.py first.")
    m.load_master_from_sqlite(str(db_path))
    return m


class TestSubDistrictMatching:
    def test_exact_match(self, village_matcher: LGDMatcher):
        r = village_matcher.match_subdistrict("Car Nicobar", "603")
        assert r["subdistrict_status"] == "EXACT"
        assert r["subdistrict_lgd_code"] == "5916"

    def test_fuzzy_match(self, village_matcher: LGDMatcher):
        r = village_matcher.match_subdistrict("car nikobar", "603")
        assert r["subdistrict_status"] in ("EXACT", "HIGH_CONFIDENCE")

    def test_blank_returns_not_found(self, village_matcher: LGDMatcher):
        r = village_matcher.match_subdistrict("", "603")
        assert r["subdistrict_status"] == "NOT_FOUND"

    def test_no_district_returns_not_found(self, village_matcher: LGDMatcher):
        r = village_matcher.match_subdistrict("Car Nicobar", "")
        assert r["subdistrict_status"] == "NOT_FOUND"

    def test_list_subdistricts(self, village_matcher: LGDMatcher):
        subs = village_matcher.list_subdistricts("603")  # Nicobars
        assert len(subs) > 0
        names = [s["subdistrict_name"] for s in subs]
        assert "Car Nicobar" in names


class TestVillageMatching:
    def test_exact_match(self, village_matcher: LGDMatcher):
        r = village_matcher.match_village("Mus", "5916")  # Car Nicobar sub-district
        assert r["village_status"] == "EXACT"
        assert r["village_lgd_code"] is not None

    def test_blank_returns_not_found(self, village_matcher: LGDMatcher):
        r = village_matcher.match_village("", "5916")
        assert r["village_status"] == "NOT_FOUND"

    def test_no_subdistrict_returns_not_found(self, village_matcher: LGDMatcher):
        r = village_matcher.match_village("Mus", "")
        assert r["village_status"] == "NOT_FOUND"

    def test_list_villages(self, village_matcher: LGDMatcher):
        villages = village_matcher.list_villages("5916")  # Car Nicobar
        assert len(villages) > 0

    def test_phonetic_fallback(self, village_matcher: LGDMatcher):
        # 'Moos' metaphone is 'MS', matching 'Mus'
        r = village_matcher.match_village("Moos", "5916")
        assert r["village_status"] != "NOT_FOUND"
        assert r["village_lgd_code"] is not None


class TestBatchWithVillage:
    def test_batch_includes_village_columns(self, village_matcher: LGDMatcher):
        df = pd.DataFrame({
            "state_name_raw": ["Andaman And Nicobar Islands"],
            "district_name_raw": ["Nicobars"],
            "subdistrict_name_raw": ["Car Nicobar"],
            "village_name_raw": ["Mus"],
        })
        results = village_matcher.match_dataframe(df)
        assert "subdistrict_lgd_code" in results.columns
        assert "village_lgd_code" in results.columns
        assert results.iloc[0]["subdistrict_lgd_code"] is not None
        assert results.iloc[0]["village_lgd_code"] is not None

    def test_batch_without_village_columns(self, village_matcher: LGDMatcher):
        """Backward-compatible: no village columns → only state+district output."""
        df = pd.DataFrame({
            "state_name_raw": ["Delhi"],
            "district_name_raw": ["New Delhi"],
        })
        results = village_matcher.match_dataframe(df)
        assert "village_lgd_code" not in results.columns
        assert results.iloc[0]["match_status"] in ("EXACT", "HIGH_CONFIDENCE")
