.PHONY: up down schemas topics seed seed-random ksql spark clear clean-state webapp webapp-local

up:
	docker compose up -d --build
	bash scripts/register_schemas.sh
	bash scripts/create_topics.sh
	bash scripts/apply_ksql.sh
	docker compose ps

down:
	docker compose down

schemas:
	bash scripts/register_schemas.sh

seed:
	bash scripts/seed_customer.sh
	bash scripts/seed_order.sh
	bash scripts/seed_activation.sh

seed-random:
	bash scripts/seed_random_order.sh

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
	rm -rf ~/.img-data/redpanda

webapp:
	docker compose up --build webapp

webapp-local:
	python3 webapp/app.py
