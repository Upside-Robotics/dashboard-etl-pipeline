IMAGE_NAME := dashboard-etl-pipeline

.PHONY: lint build run clean

lint:
	pip install --quiet ruff && ruff check .

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm --env-file .env $(IMAGE_NAME)

clean:
	docker rmi $(IMAGE_NAME) 2>/dev/null || true
