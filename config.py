"""
Configuration settings for ETL pipeline
"""

# PostgreSQL Source Database
POSTGRES_CONFIG = {
    "host": "10.0.10.238",
    "port": 5432,
    "database": "upside",
    "user": "upside_readonly",
    "password": "upside_readonly",
}

# ETL Pipeline Settings
ETL_CONFIG = {
    "batch_size": 10000,  # Number of rows to process at a time
    "timeout": 300,  # Connection timeout in seconds
    "source_table": "robot_executive_state",
}

# Data Warehouse Config (to be configured)
WAREHOUSE_CONFIG = {
    "type": None,  # "snowflake", "redshift", "bigquery", etc.
    "connection_params": {},
}
