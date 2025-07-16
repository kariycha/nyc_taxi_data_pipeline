#!/usr/bin/env python3.11
"""
logger_setup.py

This script setup logging handler

Author: CK
Created: 2025-07-15
"""
import os
import sys
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(name="downloader", log_fn="download.log", log_dir="logs"):

    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, log_fn)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # Prevent duplicate handlers
        # 1MB max, keep 2 logs
        handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=2) 
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger