"""utils.py - Utility helpers for LGD Fuzzy Matcher."""
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Path resolution — all file paths relative to THIS file's directory
# ---------------------------------------------------------------------------
_BASE_DIR = Path(__file__).resolve().parent


def resolve_path(relative: str) -> Path:
    """Resolve *relative* against the project root (directory of this file)."""
    return _BASE_DIR / relative


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------
def load_config(config_path: str = "config.json") -> dict:
    p = resolve_path(config_path) if not os.path.isabs(config_path) else Path(config_path)
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(log_file: str = "lgd_matcher.log", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("lgd_matcher")
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for h in [logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")]:
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------
def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == ""


# Pre-compiled patterns for normalize_text — avoids recompilation per call.
_RE_NON_ALNUM = re.compile(r"[^a-z0-9\s]")
_RE_MULTI_SPACE = re.compile(r"\s+")

# Cache compiled stop-word patterns keyed by frozenset of words.
_STOP_WORD_PATTERN_CACHE: dict[frozenset, re.Pattern] = {}


def _get_stop_word_pattern(stop_words: list[str]) -> re.Pattern | None:
    """Return a compiled regex for removing stop words (cached)."""
    key = frozenset(str(w).strip().lower() for w in stop_words if str(w).strip())
    if not key:
        return None
    if key not in _STOP_WORD_PATTERN_CACHE:
        escaped = [re.escape(w) for w in sorted(key)]
        _STOP_WORD_PATTERN_CACHE[key] = re.compile(r"\b(" + "|".join(escaped) + r")\b")
    return _STOP_WORD_PATTERN_CACHE[key]


def normalize_text(text: Any, stop_words: list | None = None) -> str:
    if is_blank(text):
        return ""
    t = str(text).strip().lower()
    t = t.replace("&", " and ")
    t = _RE_NON_ALNUM.sub(" ", t)
    t = _RE_MULTI_SPACE.sub(" ", t).strip()
    if stop_words:
        pat = _get_stop_word_pattern(stop_words)
        if pat:
            t = pat.sub(" ", t)
    return _RE_MULTI_SPACE.sub(" ", t).strip()


def normalize_alias_map(alias_map: dict, stop_words: list) -> dict:
    return {normalize_text(k, stop_words): v for k, v in alias_map.items() if normalize_text(k, stop_words)}


# ---------------------------------------------------------------------------
# Password hashing helpers (pbkdf2 — no extra dependency)
# ---------------------------------------------------------------------------
_HASH_ITERATIONS = 260_000  # OWASP recommendation for PBKDF2-SHA256


def hash_password(password: str) -> str:
    """Return a hex-encoded salt$hash string using PBKDF2-SHA256."""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _HASH_ITERATIONS)
    return salt.hex() + "$" + dk.hex()


def verify_password(password: str, stored: str) -> bool:
    """Verify *password* against a hash produced by ``hash_password``.

    Also accepts **plain-text fallback** for backward compatibility:
    if *stored* does not contain a ``$`` separator it is treated as a
    raw password and compared with constant-time comparison.
    """
    import hmac as _hmac

    if "$" not in stored:
        # Legacy plain-text comparison (constant-time).
        return _hmac.compare_digest(password, stored)
    try:
        salt_hex, hash_hex = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except (ValueError, TypeError):
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _HASH_ITERATIONS)
    return _hmac.compare_digest(dk, expected)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
def save_matched_csv(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


def save_unmatched_csv(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df[df["match_status"] == "NOT_FOUND"].to_csv(output_path, index=False, encoding="utf-8-sig")


def _sql_escape(value: str) -> str:
    """Escape a string value for safe inclusion in a SQL literal."""
    return value.replace("\\", "\\\\").replace("'", "''")


def generate_sql_update(
    df: pd.DataFrame,
    table_name: str = "target_table",
    output_path: str = "lgd_updates.sql",
) -> None:
    """Generate parameterised-style SQL UPDATE statements.

    ``table_name`` is validated to contain only safe identifier characters.
    All values are properly escaped / quoted.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    # Validate table name — only allow alphanumeric, underscores, dots
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")

    matched = df[df["match_status"] != "NOT_FOUND"]
    lines: list[str] = [f"-- Auto-generated by LGD Fuzzy Matcher\n-- Total updates: {len(matched)}\n"]
    for _, row in matched.iterrows():
        sc = "NULL" if is_blank(row["state_lgd_code"]) else int(float(row["state_lgd_code"]))
        dc = "NULL" if is_blank(row["district_lgd_code"]) else int(float(row["district_lgd_code"]))
        safe_id = _sql_escape(str(row["id"]))
        lines.append(
            f"UPDATE {table_name}\n"
            f"SET state_lgd_code = {sc},\n"
            f"    district_lgd_code = {dc}\n"
            f"WHERE id = '{safe_id}';\n"
        )
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
