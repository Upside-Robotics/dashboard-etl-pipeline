#!/bin/bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin

exec 9>/tmp/etl.lock
flock -n 9 || { echo "$(date): ETL already running, skipping"; exit 0; }

docker run --rm \
  --log-driver awslogs \
  --log-opt awslogs-region=ca-central-1 \
  --log-opt awslogs-group=/etl/dashboard-etl-pipeline \
  --log-opt awslogs-create-group=true \
  --log-opt "awslogs-stream=$(date +%Y-%m-%d)" \
  --env-file /home/ec2-user/.env \
  dashboard-etl-pipeline:latest
