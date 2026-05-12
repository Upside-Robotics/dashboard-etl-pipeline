IMAGE_NAME := dashboard-etl-pipeline

.PHONY: lint build run clean cron-install

lint:
	pip install --quiet ruff && ruff check .

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm --env-file .env $(IMAGE_NAME)

clean:
	docker rmi $(IMAGE_NAME) 2>/dev/null || true

cron-install:
	(crontab -l 2>/dev/null | grep -v '$(IMAGE_NAME)'; \
	 echo "0 13 * * * docker run --rm --env-file /home/ec2-user/.env $(IMAGE_NAME):latest >> /home/ec2-user/etl.log 2>&1") | crontab -
	@echo "Cron installed:"
	@crontab -l | grep $(IMAGE_NAME)
