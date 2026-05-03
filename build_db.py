import sqlite3
import pandas as pd
import jellyfish
from pathlib import Path

from utils import load_config, normalize_text

def build_db(db_path: str = "lgd_master.db"):
    config = load_config("config.json")
    stop_words = config.get("stop_words", [])
    
    print(f"Creating database {db_path}...")
    conn = sqlite3.connect(db_path)
    
    # Process States
    print("Loading States...")
    df_state = pd.read_csv("lgd_STATE.csv", dtype=str).fillna("")
    df_state = df_state.rename(columns={"state_lgd": "state_lgd_code"})
    df_state["state_name_norm"] = df_state["state_name"].apply(lambda x: normalize_text(x, stop_words))
    df_state["state_name_phonetic"] = df_state["state_name_norm"].apply(jellyfish.metaphone)
    df_state.to_sql("states", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_state_norm ON states(state_name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_state_lgd ON states(state_lgd_code)")
    
    # Process Districts
    print("Loading Districts...")
    df_dist = pd.read_csv("DISTRICT_STATE.csv", dtype=str).fillna("")
    df_dist = df_dist.rename(columns={"state_lgd": "state_lgd_code", "district_lgd": "district_lgd_code"})
    df_dist["district_name_norm"] = df_dist["district_name"].apply(lambda x: normalize_text(x, stop_words))
    df_dist["district_name_phonetic"] = df_dist["district_name_norm"].apply(jellyfish.metaphone)
    df_dist.to_sql("districts", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dist_norm ON districts(district_name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dist_state ON districts(state_lgd_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dist_lgd ON districts(district_lgd_code)")
    
    # Process Sub-districts
    print("Loading Sub-districts...")
    subdist_file = "SUBDISTRICT_DISTRICT.zip" if Path("SUBDISTRICT_DISTRICT.zip").exists() else "SUBDISTRICT_DISTRICT.csv"
    if Path(subdist_file).exists():
        df_subdist = pd.read_csv(subdist_file, dtype=str).fillna("")
        df_subdist = df_subdist.rename(columns={
            "subdistrict_lgd": "subdistrict_lgd_code", "district_lgd": "district_lgd_code",
            "state_lgd": "state_lgd_code",
        })
        df_subdist["subdistrict_name_norm"] = df_subdist["subdistrict_name"].apply(lambda x: normalize_text(x, stop_words))
        df_subdist["subdistrict_name_phonetic"] = df_subdist["subdistrict_name_norm"].apply(jellyfish.metaphone)
        df_subdist.to_sql("subdistricts", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subdist_norm ON subdistricts(subdistrict_name_norm)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subdist_dist ON subdistricts(district_lgd_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_subdist_lgd ON subdistricts(subdistrict_lgd_code)")
    else:
        print("Skipping sub-districts (file not found).")
        
    # Process Villages
    print("Loading Villages (this may take a minute)...")
    village_file = "VILLAGE_SUBDISTRICT.zip" if Path("VILLAGE_SUBDISTRICT.zip").exists() else "VILLAGE_SUBDISTRICT.csv"
    if Path(village_file).exists():
        # Read in chunks to save memory during build, though pandas can probably handle 50MB
        df_village = pd.read_csv(village_file, dtype=str).fillna("")
        df_village = df_village.rename(columns={
            "village_lgd": "village_lgd_code", "subdistrict_lgd": "subdistrict_lgd_code",
            "district_lgd": "district_lgd_code", "state_lgd": "state_lgd_code",
        })
        df_village["village_name_norm"] = df_village["village_name"].apply(lambda x: normalize_text(x, stop_words))
        df_village["village_name_phonetic"] = df_village["village_name_norm"].apply(jellyfish.metaphone)
        df_village.to_sql("villages", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_village_norm ON villages(village_name_norm)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_village_subdist ON villages(subdistrict_lgd_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_village_lgd ON villages(village_lgd_code)")
    else:
        print("Skipping villages (file not found).")
        
    conn.close()
    print("Database build complete.")

if __name__ == "__main__":
    build_db()
