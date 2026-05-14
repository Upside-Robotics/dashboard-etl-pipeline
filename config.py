"""
Configuration settings for ETL pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# PostgreSQL Source Database
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DATABASE"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# S3 staging settings used for Redshift COPY
S3_CONFIG = {
    "bucket_name": "upside-robotics-redshift-staging-aarav",
    "region_name": "ca-central-1",
    "prefix": "etl-staging",
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
    "host": os.getenv("REDSHIFT_HOST"),
    "port": int(os.getenv("REDSHIFT_PORT", 5439)),
    "database": os.getenv("REDSHIFT_DATABASE"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "schema": os.getenv("REDSHIFT_SCHEMA"),
    "sslmode": "require",
    "copy_options": {
        "iam_role_arn": os.getenv("REDSHIFT_IAM_ROLE_ARN"),
        "file_format": "csv",
        "delimiter": ",",
        "ignore_header": 1,
    },
}

# ETL Pipeline Settings
ETL_CONFIG = {
    "batch_size": 10000,
    "timeout": 300,
    "target_file_format": "csv",
    "tables": [
        {"source": "robot_executive_state", "watermark_column": "write_time"},
        {"source": "field_application_zones", "watermark_column": None},  # full replace each run
    ],
}
