#!/usr/bin/env python3.11
"""
taxi_data_pipeline.py

Orchestrates the NYC Taxi Data Engineering pipeline:
1. Downloads source data from GitHub (or S3)
2. Applies transformations to raw data
3. Uploads processed data to S3 bucket

Logs each step and terminates cleanly on failure.

Author: CK
Created: 2025-Jul
"""

import sys
import logger_setup
from download_data import download_parquet_files
from transform_data import transform_parquet_files
from upload_to_s3 import upload_files_to_s3

# === Pipeline Config ===
LOG_DIR = "logs"
PIPELINE_LOG_FILE = "tax_data_pipeline.log"
DATA_DIR = "data"
PROCESSED_SUBDIR = "processed"
S3_BUCKET = "XXXXX-nyc-taxi-pipeline"  # update with actually bucket name
S3_PREFIX = "nyc_taxi/"


def main():
    logger = logger_setup.setup_logger("pipeline", PIPELINE_LOG_FILE, LOG_DIR)
    logger.info("Starting full data pipeline...")

    # Step 1: Download
    try:
        combined_file = download_parquet_files(output_dir=DATA_DIR, logger=logger)
        if not combined_file:
            logger.error("No files downloaded. Pipeline exiting.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Download step failed: {e}")
        sys.exit(1)

    # Step 2: Transform
    try:
        transformed_files = transform_parquet_files(input_dir=DATA_DIR, logger=logger)
        if not transformed_files:
            logger.error("No files transformed. Pipeline exiting.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Transformation step failed: {e}")
        sys.exit(1)

    # Step 3: Upload
    try:
        uploaded_files = upload_files_to_s3(
            bucket_name=S3_BUCKET,
            input_dir=f"{DATA_DIR}/{PROCESSED_SUBDIR}",
            logger=logger,
            s3_prefix=S3_PREFIX
        )
        if not uploaded_files:
            logger.warning("No files uploaded.")
    except Exception as e:
        logger.error(f"Upload step failed: {e}")
        sys.exit(1)

    logger.info("Pipeline completed successfully.")
    logger.info("=" * 120)


if __name__ == "__main__":
    main()
