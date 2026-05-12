# Contributing

## Getting Started

### 1. Clone and set up

```bash
git clone git@github.com:Upside-Robotics/dashboard-etl-pipeline.git
cd dashboard-etl-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Fill in the values. You need:
- VPN access to reach the PostgreSQL replica (`10.0.10.238`)
- AWS credentials with access to S3 and Redshift
- Redshift credentials

### 3. Verify connectivity

```bash
python test_connection.py
```

Should print the 5 latest rows from `robot_executive_state`. If it fails, check VPN is connected.

---

## Development Workflow

### Branching

- Branch off `main` for all changes
- Use descriptive branch names: `fix/postgres-timeout`, `feat/incremental-load`, `chore/update-deps`
- Open a PR to merge back into `main`

### Running locally

```bash
# Quickest — runs directly with your venv
python main.py

# Closest to production — runs inside Docker
make build
make run
```

Always test with `make build && make run` before opening a PR — this catches dependency or environment issues that only appear in the container.

### Linting

```bash
make lint
```

We use `ruff` for linting. Fix any issues before pushing.

---

## Project Structure

| File | Responsibility |
|---|---|
| `main.py` | Entrypoint — calls `etl_pipeline.main()` |
| `etl_pipeline.py` | Orchestrates the full ETL flow |
| `postgres_connector.py` | Connects to PostgreSQL, extracts data in batches |
| `warehouse_loader.py` | Uploads CSV to S3, runs Redshift COPY |
| `config.py` | Loads all config from `.env` |
| `test_connection.py` | Manual script to test Postgres connectivity |

### Adding a new source table

1. Add the table name to `ETL_CONFIG` in `config.py`
2. Call `pipeline.run_full_redshift_load("new_table_name")` in `etl_pipeline.py`
3. Create the corresponding table in Redshift under `warehouse_raw`
4. Test locally with `python main.py`

---

## Pull Requests

- Keep PRs focused — one change per PR
- Include a clear description of what changed and why
- If you change infrastructure (security groups, VPC, IAM), document it in the PR body
- PRs to `main` trigger an automatic deploy to the EC2 — make sure it's ready

### PR checklist

- [ ] `python test_connection.py` passes
- [ ] `make build && make run` completes successfully
- [ ] `make lint` passes
- [ ] PR description explains the change

---

## Infrastructure

Key resources — handle with care:

| Resource | Notes |
|---|---|
| PostgreSQL replica | Read-only replica in private VPC — do not write to it |
| ETL EC2 | Runs the Docker container — ask a team member for access |
| VPC Peering | Links ETL VPC to Postgres VPC |
| Postgres SG | Port 5432 open to ETL EC2 only |
| Redshift SG | Port 5439 publicly accessible (credentials required) |
| ECR Repo | `dashboard-etl-pipeline` — images tagged by git SHA |

### Postgres access

The PostgreSQL replica is in a private VPC with no public IP. To access it locally you need to be on VPN. The ETL EC2 reaches it directly via VPC peering.

### Deploying manually to EC2

If you need to deploy without pushing to `main`:

```bash
ssh -i <key.pem> ec2-user@<ec2-host>

# Pull image and retag as latest
IMAGE=<ecr-registry>/dashboard-etl-pipeline:<sha>
aws ecr get-login-password --region ca-central-1 | docker login --username AWS --password-stdin <ecr-registry>
docker pull $IMAGE
docker tag $IMAGE dashboard-etl-pipeline:latest

# Install (or update) the daily cron — runs at 9 AM EDT (13:00 UTC)
make cron-install
```

The pipeline runs as a one-shot container triggered by cron, not a persistent daemon. Verify the cron is set with `crontab -l`.

---

## Environment Variables

Never commit `.env`, `*.pem`, or `*.key` files — they are in `.gitignore`. Share credentials securely (1Password, AWS Secrets Manager, etc.).

GitHub Actions secrets are managed in the repo settings under **Settings → Secrets and variables → Actions**.
