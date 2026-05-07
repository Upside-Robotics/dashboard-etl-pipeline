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
│ │              │ │ rows    │ .csv         │ ROLE    │ │ executive_  │  │   │ Reporting
│ │              │ │ chunks  │              │ IAM     │ │ state       │  │   │
│ │              │ │         │ ca-central-1 │         │ │             │  │   │
│ └──────────────┘ │         │              │         │ │ dev database│  │   │
│                  │         │              │         │ │ serverless  │  │   │
└──────────────────┘         └──────────────┘         └──────────────────┘   └─────────┘
        ↓
   upside database
   readonly user
```

