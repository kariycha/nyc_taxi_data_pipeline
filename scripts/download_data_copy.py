"""
download_data.py

This script downloads a Parquet file of NYC Yellow and Green Taxi trip data from a remote URL
(e.g., GitHub or public S3), loads it into a Pandas DataFrame, and saves it
locally in the 'data/' directory for further processing. Logs messages with rotation.

As GitHub doesn't serve raw folder listings via HTTP used the GitHub API to list the contents of 
the folder and download all .parquet files.

Intended as part of a data engineering pipeline that includes transformation,
upload to S3, and querying with Athena.

Includes error handling for:
- Invalid or unreachable URL
- Directory creation issues
- File read/write errors

Author: CK
Created: 2025-07-15
"""

import os
import sys
from io import BytesIO
import requests
import logging
from logging.handlers import RotatingFileHandler
import urllib.error
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime

# === Configurations ===
PARQUET_URL = "http://raw.githubusercontent.com/kariycha/nyc_taxi_data_pipeline/data/source/main/"
# http://github.com/kariycha/nyc_taxi_data_pipeline/blob/main/data/source/green_tripdata_2025-05.parquet
# https://raw.githubusercontent.com/smaanan/sev.en_commodities/main/random_deals.parq
PARQUET_URL = "http://raw.githubusercontent.com/kariycha/nyc_taxi_data_pipeline/main/data/source/green_tripdata_2025-05.parquet" #green_tripdata_2025-05.parquet"
# PARQUET_URL = "https://github.com/kariycha/nyc_taxi_data_pipeline/blob/12c2f675f5eb16c00721544069f0d58d4fd988fa/data/source/green_tripdata_2025-05.parquet"
DATA_DIR = "data"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "download.log")

# === LOGGING SETUP ===
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("downloader")
    logger.setLevel(logging.INFO)

    if not logger.handlers: 
        handler = RotatingFileHandler(
            LOG_FILE, maxBytes=1_000_000, backupCount=2  # 1MB max, keep 2 logs
        )
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def main():

    # Create data folder if it doesn't exist
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError as e:
        logger.error(f"[ERROR] Failed to create/access directory '{DATA_DIR}': {e}")
        sys.exit(1)
    
    try:
        logger.info(f"Downloading Parquet file from URL: {PARQUET_URL}")
        taxi_df = pd.read_parquet(PARQUET_URL, engine="pyarrow")
    except (urllib.error.URLError, FileNotFoundError, OSError, ValueError) as e:
        logger.error(f"Failed to download or read Parquet file: {e}")
        sys.exit(1)

    try:
        # Save locally
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        output_filename = os.path.join(DATA_DIR, f"taxi_tripdata_{timestamp}.parquet")
        taxi_df.to_parquet(f"{output_filename}", engine="pyarrow", compression="snappy")
        logger.info(f"[SUCCESS] Parquet file downloaded and saved locally: {output_filename}")
    except Exception as e:
        logger.error(f"Failed to save file locally '{DATA_DIR}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_logger()
    main()