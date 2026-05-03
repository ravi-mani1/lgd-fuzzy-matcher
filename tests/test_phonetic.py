import pytest
import sqlite3
import pandas as pd
import jellyfish
from matcher import LGDMatcher

def test_phonetic_fallback():
    # Setup in-memory SQLite DB
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE subdistricts (subdistrict_lgd_code TEXT, subdistrict_name TEXT, subdistrict_name_norm TEXT, subdistrict_name_phonetic TEXT, district_lgd_code TEXT)")
    conn.execute("CREATE TABLE villages (village_lgd_code TEXT, village_name TEXT, village_name_norm TEXT, village_name_phonetic TEXT, subdistrict_lgd_code TEXT)")
    
    # Insert subdistrict and village
    sd_norm = "pindra"
    sd_phon = jellyfish.metaphone(sd_norm)
    conn.execute("INSERT INTO subdistricts VALUES ('100', 'Pindra', ?, ?, '10')", (sd_norm, sd_phon))
    
    v_norm = "madhupur"
    v_phon = jellyfish.metaphone(v_norm)
    conn.execute("INSERT INTO villages VALUES ('1001', 'Madhupur', ?, ?, '100')", (v_norm, v_phon))
    conn.commit()

    # Initialize matcher and mock DB connection
    m = LGDMatcher()
    m.db_conn = conn
    m.has_subdistricts_db = True
    m.has_villages_db = True
    
    # 'madhoopoor' matches 'madhupur' phonetically (MTHPR) but has low fuzzy score
    res = m.match_village("madhoopoor", "100")
    
    # It should match Madhupur due to phonetic fallback
    assert res["village_lgd_code"] == "1001"
    assert res["village_name_corrected"] == "Madhupur"
    assert res["village_score"] >= m.thresholds["low_confidence"] # boosted
