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
	chmod +x /home/ec2-user/run-etl.sh
	(crontab -l 2>/dev/null | grep -v 'run-etl.sh'; \
	 echo "0 * * * * /home/ec2-user/run-etl.sh >> /home/ec2-user/etl.log 2>&1") | crontab -
	@echo "Cron installed:"
	@crontab -l | grep run-etl.sh
