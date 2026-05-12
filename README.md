# Dashboard ETL Pipeline

Production ETL pipeline that extracts robot state data from a PostgreSQL replica, stages it to S3, and loads it into Redshift for Power BI reporting.

## Architecture

```
PostgreSQL Replica        S3 (Staging)               Redshift Serverless     Power BI
(VPC: 10.0.10.238)        (ca-central-1)              (Serverless)
        │                       │                           │                    │
        │   EXTRACT (CSV)        │   COPY (IAM Role)         │                    │
        │   10K rows/batch       │                           │                    │
        └──────────────────────►│──────────────────────────►│───────────────────►│
        robot_executive_state   robot_executive_state_       warehouse_raw.       Dashboards
                                {timestamp}.csv              robot_executive_state
```

## Infrastructure

| Component | Details |
|---|---|
| Source DB | PostgreSQL 16 replica (private VPC, no public IP) |
| ETL Host | Dedicated EC2 instance in AWS |
| S3 Bucket | `upside-robotics-redshift-staging-aarav` |
| Redshift | `upside-robotics-analytics-wg` (Serverless, `dev` database) |
| Schema | `warehouse_raw.robot_executive_state` |
| Networking | VPC peering between ETL EC2 and Postgres VPCs |
| Region | `ca-central-1` |

## Project Structure

```
dashboard-etl-pipeline/
├── main.py                  # Entrypoint
├── etl_pipeline.py          # Pipeline orchestration (ETLPipeline class)
├── postgres_connector.py    # PostgreSQL connection and data extraction
├── warehouse_loader.py      # S3 upload and Redshift COPY
├── config.py                # Configuration loaded from .env
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container image definition
├── Makefile                 # Local dev commands
├── test_connection.py       # Manual connectivity test script
└── .github/
    └── workflows/
        └── deploy.yml       # CI/CD: build → ECR → EC2 deploy
```

## Local Development

### Prerequisites
- Python 3.12+
- Docker
- AWS CLI configured
- VPN access (to reach the PostgreSQL replica at `10.0.10.238`)

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
```

### Running

```bash
# Run directly
python main.py

# Run via Docker (matches production exactly)
make build
make run
```

### Test connectivity

```bash
python test_connection.py
```

Fetches the 5 latest rows from `robot_executive_state` to verify the Postgres connection is working.

## Environment Variables

Create a `.env` file in the project root (ask a team member for values):

```env
# PostgreSQL
POSTGRES_HOST=
POSTGRES_PORT=5432
POSTGRES_DATABASE=
POSTGRES_USER=
POSTGRES_PASSWORD=

# AWS
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=

# Redshift
REDSHIFT_HOST=
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=
REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_SCHEMA=warehouse_raw
REDSHIFT_TABLE=robot_executive_state
REDSHIFT_IAM_ROLE_ARN=
```

## Deployment

Deployment is fully automated via GitHub Actions on every push to `main`:

1. **Build** — Docker image built and pushed to ECR (`905418281504.dkr.ecr.ca-central-1.amazonaws.com/dashboard-etl-pipeline`)
2. **Deploy** — EC2 pulls the new image, stops the old container, starts the new one

```bash
# Trigger a deploy
git push origin main
```

### Monitor on EC2

```bash
ssh -i <key.pem> ec2-user@<ec2-host>

docker ps                                          # check container is running
docker logs dashboard-etl-pipeline -f              # live logs
docker logs dashboard-etl-pipeline --tail 50       # last 50 lines
docker stats dashboard-etl-pipeline               # CPU / memory
```

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM user key for ECR push |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret |
| `EC2_HOST` | ETL EC2 public IP |
| `EC2_SSH_PRIVATE_KEY` | SSH private key for EC2 access |

## Makefile Commands

| Command | Description |
|---|---|
| `make build` | Build Docker image locally |
| `make run` | Run pipeline in Docker using `.env` |
| `make lint` | Run ruff linter |
| `make clean` | Remove local Docker image |
