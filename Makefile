.PHONY: up down schemas seed ksql spark

up:
	docker compose up -d
	docker compose ps

down:
	docker compose down -v

schemas:
	bash scripts/register_schemas.sh

seed:
	bash scripts/seed_customer.sh
	bash scripts/seed_order.sh
	bash scripts/seed_activation.sh

ksql:
	docker compose run --rm ksql-cli ksql http://ksqldb:8088 -f /scripts/streams.sql

spark:
	bash spark/run_all.sh
