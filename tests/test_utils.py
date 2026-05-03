"""Tests for utils.py."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import (
    _sql_escape,
    generate_sql_update,
    hash_password,
    is_blank,
    normalize_text,
    verify_password,
)
import pandas as pd
import pytest


class TestIsBlank:
    def test_none(self):
        assert is_blank(None) is True

    def test_empty_string(self):
        assert is_blank("") is True
        assert is_blank("   ") is True

    def test_nan(self):
        assert is_blank(float("nan")) is True

    def test_non_blank(self):
        assert is_blank("hello") is False
        assert is_blank(0) is False


class TestNormalizeText:
    def test_basic(self):
        assert normalize_text("  Delhi  ") == "delhi"

    def test_ampersand(self):
        assert normalize_text("J&K") == "j and k"

    def test_special_chars(self):
        assert normalize_text("Dr. B.R. Ambedkar") == "dr b r ambedkar"

    def test_stop_words(self):
        result = normalize_text("District of Agra", stop_words=["district", "of"])
        assert result == "agra"

    def test_blank_input(self):
        assert normalize_text(None) == ""
        assert normalize_text("") == ""

    def test_pradesh_removal(self):
        # Demonstrates the "pradesh" stop word behaviour
        result = normalize_text("Uttar Pradesh", stop_words=["pradesh"])
        assert result == "uttar"


class TestPasswordHashing:
    def test_round_trip(self):
        pw = "my_secure_password"
        hashed = hash_password(pw)
        assert verify_password(pw, hashed) is True
        assert verify_password("wrong", hashed) is False

    def test_legacy_plaintext(self):
        # Backward-compatible: if stored value has no '$', treat as plaintext.
        assert verify_password("admin123", "admin123") is True
        assert verify_password("admin123", "wrong") is False

    def test_hash_uniqueness(self):
        h1 = hash_password("same")
        h2 = hash_password("same")
        assert h1 != h2  # Different salts


class TestSqlEscape:
    def test_single_quotes(self):
        assert _sql_escape("O'Brien") == "O''Brien"

    def test_backslashes(self):
        assert _sql_escape("path\\file") == "path\\\\file"


class TestGenerateSqlUpdate:
    def test_valid_output(self, tmp_path):
        df = pd.DataFrame({
            "id": ["1", "2"],
            "state_lgd_code": ["7", None],
            "district_lgd_code": ["79", None],
            "match_status": ["EXACT", "NOT_FOUND"],
        })
        out = tmp_path / "out.sql"
        generate_sql_update(df, table_name="my_table", output_path=str(out))
        content = out.read_text()
        assert "UPDATE my_table" in content
        assert "WHERE id = '1'" in content
        # NOT_FOUND rows should be excluded
        assert "WHERE id = '2'" not in content

    def test_invalid_table_name(self, tmp_path):
        df = pd.DataFrame({"id": ["1"], "state_lgd_code": ["7"], "district_lgd_code": ["79"], "match_status": ["EXACT"]})
        with pytest.raises(ValueError, match="Invalid table name"):
            generate_sql_update(df, table_name="DROP TABLE--", output_path=str(tmp_path / "bad.sql"))
