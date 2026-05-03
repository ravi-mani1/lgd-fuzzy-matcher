import os
import time
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lgd_sync")

# Example placeholder URL - replace with actual govt endpoint or S3 bucket
LGD_API_BASE = os.getenv("LGD_API_SOURCE_URL", "https://example-gov-api.in/lgd-data")

FILES_TO_SYNC = [
    "lgd_STATE.csv",
    "DISTRICT_STATE.csv",
    "SUBDISTRICT_DISTRICT.csv",
    "VILLAGE_SUBDISTRICT.csv"
]

def download_file(filename: str):
    url = f"{LGD_API_BASE}/{filename}"
    logger.info(f"Downloading {filename} from {url}...")
    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        
        tmp_path = f"{filename}.tmp"
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Atomic replace
        os.replace(tmp_path, filename)
        logger.info(f"Successfully synced {filename}.")
        return True
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")
        return False

def run_sync():
    logger.info("Starting LGD data sync...")
    all_success = True
    for f in FILES_TO_SYNC:
        if not download_file(f):
            all_success = False
            
    if all_success:
        logger.info("Sync complete. Rebuilding local SQLite database...")
        # Automatically trigger the DB rebuild
        import subprocess
        subprocess.run(["python", "build_db.py"], check=True)
        logger.info("Database rebuilt. Please restart the FastAPI/Streamlit services to load new data.")
    else:
        logger.error("Sync completed with errors. Database was NOT rebuilt.")

if __name__ == "__main__":
    run_sync()
