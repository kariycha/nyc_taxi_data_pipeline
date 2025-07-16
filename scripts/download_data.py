#!/usr/bin/env python3.11
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
Created: 2025-Jul
"""

import os
import sys
import logger_setup
import urllib.error
from io import BytesIO
import requests
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime

# === Configurations ===
GITHUB_USER = "kariycha"
GITHUB_REPO = "nyc_taxi_data_pipeline"
FOLDER_PATH = "data/source"
BRANCH = "main"
PARQUET_FOLDER_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{BRANCH}/{FOLDER_PATH}/"
DATA_DIR = "data"
LOG_DIR = "logs"
LOG_FILE = "download.log"

# === LOGGING SETUP ===
'''
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("downloader")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        LOG_FILE, maxBytes=1_000_000, backupCount=2  # 1MB max, keep 2 logs
    )
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
'''
 # === Main Function ===
def main():
    # Create or Get the logfile
    logger = logger_setup.setup_logger("downloader",LOG_FILE, LOG_DIR )

    # Create data folder if it doesn't exist
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError as e:
        logger.error(f"[ERROR] Failed to create/access directory '{DATA_DIR}': {e}")
        logger.info("Download terminated.")
        logger.info("-" * 120)
        sys.exit(1)

    # Get a list of all parquet files as GitHub only serves individual files 
    # via raw.githubusercontent.com
    api_url = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{FOLDER_PATH}?ref={BRANCH}"
    try:
        logger.info(f"[INFO] Querying GitHub API: {api_url}")
        response = requests.get(api_url)
        response.raise_for_status()
        files_list = response.json()
    except Exception as e:
        logger.error(f"Failed to fetch file list from GitHub, {api_url}: {e}")
        logger.info("Download terminated.")
        logger.info("-" * 120)
        sys.exit(1)

    parquet_files = [f for f in files_list if f["name"].endswith(".parquet")]
    logger.info(f"Found {len(parquet_files)} Parquet files.")

    if not parquet_files:
        logger.info("No Parquet files found in folder {api_url}.")
        logger.info("Download completed.")
        logger.info("-" * 120)
        return
    
    # Read all Parquet files into DataFrames
    dfs = []
    for file in parquet_files:
        parquet_url = file["download_url"]
        try:
            logger.info(f"Reading {file['name']} from {parquet_url}")
            response = requests.get(parquet_url)
            response.raise_for_status()
            dfs.append(pd.read_parquet(BytesIO(response.content), engine="pyarrow"))
        except (urllib.error.URLError, FileNotFoundError, OSError, ValueError) as e:
            logger.info(f"Failed to read Parquet file {file['name']} : {e}")
            logger.info("Download terminated.")
            logger.info("-" * 120)
            sys.exit(1)

    # Combine and Save locally
    if dfs:
        try:
            timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
            output_filename = os.path.join(DATA_DIR, f"taxi_tripdata_{timestamp}.parquet")
            taxi_df = pd.concat(dfs, ignore_index=True)
            taxi_df.to_parquet(output_filename, engine="pyarrow", compression="snappy")
            logger.info(f"[SUCCESS] Combined Parquet saved to: {output_filename}")
        except Exception as e:
            logger.error(f"Failed to save file locally '{DATA_DIR}': {e}")
            logger.info("Download terminated.")
            logger.info("-" * 120)
            sys.exit(1)
    else:
        logger.warning("No valid dataframes found to combine.")

    logger.info("Download completed.")
    logger.info("-" * 120)

if __name__ == "__main__":
    main()