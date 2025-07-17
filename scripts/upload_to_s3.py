#!/usr/bin/env python3.11
"""
upload_to_s3.py

Uploads all Parquet files from a local directory to an S3 bucket path.

Includes error handling for:
- Missing AWS credentials
- S3 upload failures
- File read errors

Author: CK
Created: 2025-Jul
"""

import os
import sys
import glob
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError
import argparse
import logger_setup

# === Configurations ===
DATA_DIR = "data/processed"
LOG_DIR = "logs"
LOG_FILE = "upload.log"
S3_BUCKET = "kariycha_nyc-taxi-pipeline"
S3_PREFIX = "nyc_taxi/"  # Optional prefix in the bucket

def upload_files_to_s3(bucket_name, input_dir=DATA_DIR, logger=None, s3_prefix=S3_PREFIX):
    if not logger:
        logger = logger_setup.setup_logger("uploader", LOG_FILE, LOG_DIR)

    input_dir = os.path.abspath(input_dir)
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    if not parquet_files:
        logger.warning(f"No Parquet files found in {input_dir} to upload.")
        logger.info("Upload completed.")
        logger.info("-" * 120)
        return []

    s3 = boto3.client("s3")
    uploaded_files = []

    for file_path in parquet_files:
        try:
            file_name = os.path.basename(file_path)
            s3_key = os.path.join(s3_prefix, file_name) if s3_prefix else file_name

            logger.info(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
            s3.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"[SUCCESS] Uploaded: {file_name}")
            uploaded_files.append(s3_key)

        except (BotoCoreError, NoCredentialsError, ClientError, OSError) as e:
            logger.error(f"Failed to upload {file_path} to S3: {e}")

    logger.info("Upload process completed.")
    logger.info("-" * 120)
    return uploaded_files


# === Entry Point ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Parquet files to S3")
    parser.add_argument(
        "--input-dir",
        type=str,
        default=DATA_DIR,
        help="Directory containing processed Parquet files",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="Target S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=S3_PREFIX,
        help="S3 key prefix (default: nyc_taxi/)",
    )
    args = parser.parse_args()
    upload_files_to_s3(args.bucket, args.input_dir, s3_prefix=args.prefix)
