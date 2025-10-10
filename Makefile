.PHONY: up down schemas seed topics ksql spark webapp webapp-local

up:
	docker compose up -d
	docker compose ps

down:
	docker compose down -v
	rm -rf .checkpoints
	rm -rf delta
	rm -rf /tmp/delta

schemas:
	bash scripts/register_schemas.sh

seed:
	bash scripts/seed_customer.sh
	bash scripts/seed_order.sh
	bash scripts/seed_activation.sh

topics:
	bash scripts/create_topics.sh

ksql: topics
	bash scripts/apply_ksql.sh

spark:
	docker compose up --build spark

