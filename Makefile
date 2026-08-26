.PHONY: up down topic producer stream smoke test

up:
	docker compose up -d

down:
	docker compose down

topic:
	./scripts/create_topic.sh

producer:
	python -m src.producer.generate_payments --count 1000 --rate 25

stream:
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.13 \
	  src/streaming/payment_stream.py

smoke:
	./scripts/smoke_test.sh

test:
	pytest -q
