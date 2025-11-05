.PHONY: up down schemas topics ksql spark clear clean-state webapp webapp-local

up:
	mkdir -p data/redpanda data/redis
	docker compose up -d --build
	bash scripts/register_schemas.sh
	bash scripts/create_topics.sh
	bash scripts/apply_ksql.sh
	docker compose ps

down:
	docker compose down

schemas:
	bash scripts/register_schemas.sh

topics:
	bash scripts/create_topics.sh

ksql: topics
	bash scripts/apply_ksql.sh

spark:
	docker compose up --build spark

clear:
	docker compose down -v
	$(MAKE) clean-state

clean-state:
	rm -rf .checkpoints
	rm -rf delta
	rm -rf /tmp/delta
	rm -rf data

webapp:
	docker compose up --build webapp

webapp-local:
	python3 webapp/app.py
