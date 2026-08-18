# Architecture Notes

## Flow

1. `generate_payments.py` creates synthetic payment events, including a small configurable percentage of bad records and duplicates.
2. Kafka stores the raw events in `payments.raw` with three partitions.
3. Spark Structured Streaming reads Kafka micro-batches, preserves Kafka source coordinates, parses JSON, and validates required fields and business rules.
4. Valid events are written to PostgreSQL `payments`; invalid events are retained in `rejected_payments` with the rejection reason.
5. Airflow runs scheduled reconciliation and quality aggregation into `daily_payment_metrics` and `data_quality_daily`.

## Design choices

- **Kafka** decouples ingestion from processing and allows the producer and stream processor to scale independently.
- **Structured Streaming** keeps stream transformations in the DataFrame API and uses checkpointing/watermarking for streaming state.
- **PostgreSQL** provides a simple analytical/operational sink that is easy to query during development.
- **Airflow** owns scheduled batch work rather than mixing orchestration into the streaming job.
- **Rejected records are retained** instead of silently discarded so failed data can be inspected and reprocessed.
- **Kafka partitions are the source parallelism.** `maxOffsetsPerTrigger` bounds each micro-batch; it does not change the Kafka partition count.
- **JDBC write partitions are bounded explicitly.** Each valid/rejected batch uses a round-robin `repartition`, which intentionally creates a shuffle but avoids coupling database concurrency to Kafka's partition count.
- **One `foreachBatch` query routes both outputs.** This avoids running two independent Kafka streaming queries for valid and rejected data.

## Spark Milestone 1

Milestone 1 covers ingestion and stateless transformations: Kafka source metadata, explicit schema parsing, validation, valid/rejected routing, and deliberate partition control. Event-time watermarking and cross-batch duplicate removal are deferred to the stateful-processing milestone.

## MVP limitations

This repository is intentionally a local-development project, not a production deployment. Authentication, secrets management, schema registry, multi-broker Kafka, HA Postgres, centralized metrics, and Kubernetes deployment are future work.
