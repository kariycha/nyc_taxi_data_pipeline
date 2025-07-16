#!/usr/bin/env python3.11
"""
transform_data.py

Reads all Parquet files from the specified directory (default: data/),
applies data transformations, and saves the processed versions to data/processed/
and errored fils to data/error

Includes error handling for:
- Invalid or unreachable file path
- File read/write errors

Author: CK
Created: 2025-Jul
"""

import os
import sys
import glob
import logger_setup
import argparse
import pandas as pd

# === Configurations ===
DATA_DIR = "data"
LOG_DIR = "logs"
LOG_FILE = "transform.log"

'''
# === Logging Setup ===
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("transform")
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

# === Transformation Logic ===
def transform(df: pd.DataFrame, logger) -> pd.DataFrame:
    logger.info("Starting data transformation")

    # EXAMPLE transformations (adjust as needed)
    df.columns = [col.lower() for col in df.columns]

    # Drop rows with missing pickup/dropoff times if present
    if "lpep_pickup_datetime" in df.columns and "lpep_dropoff_datetime" in df.columns:
        df = df.dropna(subset=["lpep_pickup_datetime", "lpep_dropoff_datetime"])

    # Convert datetime strings to datetime objects (if needed)
    for col in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    logger.info("Transformation complete")
    return df

 # === Main Function ===
def main(input_dir: str):
    # Create or Get the logfile
    logger = logger_setup.setup_logger("transform",LOG_FILE, LOG_DIR )

    input_dir = os.path.abspath(input_dir)
    output_dir = os.path.join(input_dir, "processed")
    error_dir = os.path.join(input_dir, "error")
    # Create processed folder if it doesn't exist
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create/access directory '{output_dir}': {e}")
        logger.info("Transformation terminated.")
        logger.info("-" * 120)
        sys.exit(1)

    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))
    if not parquet_files:
        logger.warning(f"No Parquet files found in {input_dir}")
        logger.info("Transformation Completed.")
        logger.info("-" * 120)
        sys.exit(0)

    logger.info(f"Found {len(parquet_files)} files in {input_dir}")

    for file_path in parquet_files:
        try:
            logger.info(f"Reading file: {file_path}")
            df = pd.read_parquet(file_path, engine="pyarrow")

            df_transformed = transform(df, logger)

            filename = os.path.basename(file_path).replace(".parquet", "_transformed.parquet")
            output_path = os.path.join(output_dir, filename)

            df_transformed.to_parquet(output_path, engine="pyarrow", compression="snappy")
            logger.info(f"[SUCCESS] Transformed file saved to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            # Create error folder if it doesn't exist
            try:
                os.makedirs(error_dir, exist_ok=True)
                dest_path = os.path.join(error_dir, os.path.basename(file_path))
                os.rename(file_path, dest_path)
                logger.info(f"Moved failed file to: {dest_path}")
            except OSError as e:
                logger.error(f"Failed to create/access directory '{error_dir}': {e}")
                logger.info("Transformation terminated.")
                logger.info("-" * 120)
                sys.exit(1)

    logger.info("All transformations complete")
    logger.info("-" * 120)


# === Entry Point ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Parquet taxi data")
    parser.add_argument(
        "--input-dir",
        type=str,
        default="data",
        help="Path to directory containing Parquet files (default: data/)",
    )
    args = parser.parse_args()
    main(args.input_dir)   