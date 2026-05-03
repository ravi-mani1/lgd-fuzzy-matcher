"""Shared test fixtures for LGD Fuzzy Matcher."""
import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from matcher import LGDMatcher  # noqa: E402


@pytest.fixture(scope="session")
def matcher() -> LGDMatcher:
    """Return a fully initialised LGDMatcher backed by the real CSV files."""
    m = LGDMatcher(config_path=str(PROJECT_ROOT / "config.json"))
    m.load_master_from_csv(
        str(PROJECT_ROOT / "lgd_STATE.csv"),
        str(PROJECT_ROOT / "DISTRICT_STATE.csv"),
    )
    return m


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "id": ["1", "2", "3", "4", "5"],
        "state_name_raw": ["delhii", "NCT Delhi", "UP", "Bengluru", "west bengall"],
        "district_name_raw": ["New Delhi", "District Agra", "varansi", "bangalore", "calcuta"],
    })
