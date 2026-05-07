"""
Configuration settings for ETL pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# PostgreSQL Source Database
POSTGRES_CONFIG = {
    "host": "10.0.10.238",
    "port": 5432,
    "database": "upside",
    "user": "upside_readonly",
    "password": "upside_readonly",
}

# S3 staging settings used for Redshift COPY
S3_CONFIG = {
    "bucket_name": "upside-robotics-redshift-staging-aarav",
    "region_name": "ca-central-1",
    "prefix": "robot_executive_state",
    "file_format": "csv",  # Supported: csv, jsonl
}

# AWS configuration for S3 access
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
    "profile_name": os.getenv("AWS_PROFILE") or os.getenv("AWS_PROFILE_NAME"),
}

# Redshift Warehouse configuration
REDSHIFT_CONFIG = {
    "host": "upside-robotics-analytics-wg.905418281504.ca-central-1.redshift-serverless.amazonaws.com",
    "port": 5439,
    "database": "dev",
    "user": "aarav",
    "password": "Password1",
    "schema": "warehouse_raw",
    "table": "robot_executive_state",
    "sslmode": "require",
    "copy_options": {
        "iam_role_arn": None,
        "file_format": "csv",
        "delimiter": ",",
        "ignore_header": 1,
    },
}

# ETL Pipeline Settings
ETL_CONFIG = {
    "batch_size": 10000,  # Number of rows to process at a time
    "timeout": 300,  # Connection timeout in seconds
    "source_table": "robot_executive_state",
    "target_file_format": "csv",
}
