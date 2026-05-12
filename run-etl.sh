#!/bin/bash
set -euo pipefail

docker run --rm \
  --log-driver awslogs \
  --log-opt awslogs-region=ca-central-1 \
  --log-opt awslogs-group=/etl/dashboard-etl-pipeline \
  --log-opt awslogs-create-group=true \
  --log-opt "awslogs-stream=$(date +%Y-%m-%d)" \
  --env-file /home/ec2-user/.env \
  dashboard-etl-pipeline:latest
