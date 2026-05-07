# Robot ETL Pipeline

**Status**: ✅ Production ETL pipeline successfully connecting robot_executive_state data from PostgreSQL → S3 → Redshift → Power BI

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             DATA PIPELINE FLOW                               │
└─────────────────────────────────────────────────────────────────────────────┘

EXTRACT                          STAGE                          LOAD              VISUALIZE
(PostgreSQL)                      (Amazon S3)                    (Redshift)        (Power BI)

┌──────────────────┐         ┌──────────────┐         ┌──────────────────┐   ┌─────────┐
│   PostgreSQL     │         │              │         │   Redshift       │   │ Power   │
│   Database       │         │  S3 Bucket   │         │   Warehouse      │   │   BI    │
│                  │         │              │         │                  │   │ Reports │
│ ┌──────────────┐ │  BATCH  │ (Staging)    │  COPY   │ ┌─────────────┐  │   │         │
│ │ robot_       │ │    ──→  │              │   ──→   │ │ warehouse_  │  │   │   ──→   │
│ │ executive_   │ │ (CSV)   │ robot_       │         │ │ raw schema  │  │   │         │
│ │ state table  │ │ EXTRACT │ executive_   │         │ │             │  │   │ Dashboards
│ │              │ │ 10K     │ state_date   │ LOAD    │ │ robot_      │  │   │ Analytics
│ │ 10.0.10.238  │ │ rows    │ .csv         │ ROLE    │ │ executive_  │  │   │ Reporting
│ │ port 5432    │ │ chunks  │              │ IAM     │ │ state       │  │   │
│ │              │ │         │ ca-central-1 │         │ │             │  │   │
│ └──────────────┘ │         │              │         │ │ dev database│  │   │
│                  │         │              │         │ │ serverless  │  │   │
└──────────────────┘         └──────────────┘         └──────────────────┘   └─────────┘
        ↓
   upside database
   readonly user
```

## Pipeline Stages

### 1. **EXTRACT** - PostgreSQL Data Source
- **Source**: PostgreSQL database (`upside` database)
- **Table**: `robot_executive_state`
- **Method**: Batch processing (10,000 rows per batch for memory efficiency)
- **Connection**: psycopg2 with SSL/TLS encryption
- **Data Flow**:
  - Retrieves table metadata (column names, types, row count)
  - Iterates through rows in configurable batch sizes
  - Serializes datetime and binary data to JSON-compatible formats
  - No data transformation at this stage (source format preserved)

### 2. **STAGE** - Amazon S3 Intermediate Storage
- **Bucket**: `upside-robotics-redshift-staging-aarav` (ca-central-1)
- **Format**: CSV with headers
- **Filename Pattern**: `robot_executive_state/robot_executive_state_YYYYMMDD_HHMMSS.csv`
- **Delimiter**: Comma (`,`)
- **Header**: Skip first row during Redshift LOAD
- **Purpose**: 
  - Staging area for Redshift COPY command
  - Temporary storage between extraction and warehouse load
  - Fault tolerance (file persists if downstream load fails)

### 3. **LOAD** - Amazon Redshift Data Warehouse
- **Destination**: `warehouse_raw.robot_executive_state`
- **Schema**: `warehouse_raw` (analytics/business schema, not public)
- **Database**: `dev` (Redshift serverless)
- **Load Method**: COPY command with IAM role authentication
- **Authentication**: 
  - IAM role-based (preferred for security)
  - Credentials passed via Redshift service role
  - Supports fallback to AWS access key/secret token
- **Load Behavior**:
  - Full table load (not incremental)
  - COPY ignores header row
  - CSV format detection with automatic delimiter
  - Compression and update statistics disabled for speed
  - Timeformat auto-detection

### 4. **VISUALIZE** - Power BI Business Intelligence
- **Connection**: Redshift `warehouse_raw` schema
- **Dataset**: `robot_executive_state` table
- **Use**: Interactive dashboards, reports, analytics
- **Access**: Power BI Desktop or Web

## Execution Flow

```
1. Initialize ETLPipeline()
   ├── PostgreSQLConnector → connects to source
   ├── S3Uploader → initializes AWS session
   └── RedshiftConnector → ready for warehouse load

2. run_full_redshift_load(table_name)
   ├── extract_and_stage_to_s3(table_name)
   │   ├── Connect to PostgreSQL
   │   ├── Get table metadata
   │   ├── For each batch (10K rows):
   │   │   ├── Fetch rows from PostgreSQL
   │   │   ├── Serialize special data types
   │   │   └── Write to CSV file
   │   ├── Upload CSV to S3
   │   └── Return S3 URI
   │
   └── load_from_s3_to_redshift(s3_uri)
       ├── Connect to Redshift
       ├── Build COPY command with:
       │   ├── S3 path
       │   ├── IAM role for authentication
       │   ├── CSV format options
       │   ├── Delimiter and header settings
       │   └── Region for endpoint resolution
       ├── Execute COPY statement
       ├── Monitor execution
       └── Log results and statistics
```

## Configuration

Configuration is managed via **`config.py`** with environment variables for sensitive data:

| Component | Environment Variable | Example Value |
|-----------|---------------------|---------------|
| **PostgreSQL** | `POSTGRES_HOST` | `10.0.10.238` |
| | `POSTGRES_PORT` | `5432` |
| | `POSTGRES_DATABASE` | `upside` |
| | `POSTGRES_USER` | `upside_readonly` |
| | `POSTGRES_PASSWORD` | `upside_readonly` |
| **S3** | `AWS_ACCESS_KEY_ID` | `AKIAIOSFODNN7EXAMPLE` |
| | `AWS_SECRET_ACCESS_KEY` | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| | `AWS_SESSION_TOKEN` | (optional) |
| | `AWS_PROFILE` | `aarav` |
| **Redshift** | `REDSHIFT_HOST` | `upside-robotics-analytics-wg.905418281504.ca-central-1.redshift-serverless.amazonaws.com` |
| | `REDSHIFT_PORT` | `5439` |
| | `REDSHIFT_DATABASE` | `dev` |
| | `REDSHIFT_USER` | `aarav` |
| | `REDSHIFT_PASSWORD` | `Password1` |
| | `REDSHIFT_SCHEMA` | `warehouse_raw` |
| | `REDSHIFT_TABLE` | `robot_executive_state` |
| | `REDSHIFT_IAM_ROLE_ARN` | `arn:aws:iam::905418281504:role/service-role/AmazonRedshift-CommandsAccessRole-20260507T114313` |

**All values stored in `.env` file (not committed to git)**:
- Database passwords and credentials
- AWS access keys and tokens
- IAM role ARNs
- Hostnames and connection details

## Technical Stack

| Layer | Technology |
|-------|------------|
| **Orchestration** | Python 3.9+ |
| **Source DB** | PostgreSQL 12+ |
| **Data Transport** | AWS S3, Boto3 |
| **Warehouse** | Amazon Redshift (Serverless) |
| **BI Tool** | Power BI |
| **Python Libraries** | psycopg2 (PostgreSQL), boto3 (AWS), python-dotenv (env config) |

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up environment variables (.env file)
# AWS credentials, database passwords, IAM role ARN

# 3. Run the pipeline
python etl_pipeline.py
```

**Output**:
- CSV file saved to `extracted_data/robot_executive_state_YYYYMMDD_HHMMSS.csv`
- Data loaded to Redshift `warehouse_raw.robot_executive_state`
- Logs written to `etl_pipeline.log`
- Execution statistics printed to console

---

**Key Features**: Batch processing, memory efficient, comprehensive logging, error handling & recovery, modular design for extensibility
- Ensure firewall allows connections to 10.0.10.238:5432

### Memory Issues with Large Tables
- Reduce `batch_size` in `config.py`
- Process tables in smaller chunks using WHERE clauses

### Performance Tuning
- Increase `batch_size` for faster processing (if memory allows)
- Ensure PostgreSQL server resources are available
- Consider running extraction during off-peak hours

## License

Internal use only
