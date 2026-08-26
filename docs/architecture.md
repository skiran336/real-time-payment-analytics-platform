# Architecture Notes

## Flow

1. `generate_payments.py` creates synthetic payment events, including a small configurable percentage of bad records and duplicates.
2. Kafka stores the raw events in `payments.raw` with three partitions.
3. Spark Structured Streaming reads Kafka micro-batches, preserves Kafka source coordinates, parses JSON, and validates required fields and business rules.
4. Valid events are written to PostgreSQL `payments`; invalid events are retained in `rejected_payments` with the rejection reason.
5. Each non-empty micro-batch upserts processed, valid, rejected, and rejection-rate counters into `streaming_batch_quality` and emits the same fields as a structured JSON log.
6. Airflow runs scheduled reconciliation and quality aggregation into `daily_payment_metrics` and `data_quality_daily`.

## Design choices

- **Kafka** decouples ingestion from processing and allows the producer and stream processor to scale independently.
- **Structured Streaming** keeps stream transformations in the DataFrame API and uses checkpointing/watermarking for streaming state.
- **PostgreSQL** provides a simple analytical/operational sink that is easy to query during development.
- **Airflow** owns scheduled batch work rather than mixing orchestration into the streaming job.
- **Rejected records are retained** instead of silently discarded so failed data can be inspected and reprocessed.
- **Kafka partitions are the source parallelism.** `maxOffsetsPerTrigger` bounds each micro-batch; it does not change the Kafka partition count.
- **JDBC write partitions are bounded explicitly.** Each valid/rejected batch uses a round-robin `repartition`, which intentionally creates a shuffle but avoids coupling database concurrency to Kafka's partition count.
- **One `foreachBatch` query routes both outputs.** This avoids running two independent Kafka streaming queries for valid and rejected data.
- **Batch metrics are retry-safe.** `(stream_name, batch_id)` identifies one micro-batch metric and PostgreSQL `ON CONFLICT` updates that row if Spark retries it.

## Spark Milestone 1

Milestone 1 covers ingestion and stateless transformations: Kafka source metadata, explicit schema parsing, validation, valid/rejected routing, and deliberate partition control. Event-time watermarking and cross-batch duplicate removal are deferred to the stateful-processing milestone.

## Spark Chapter 5: memory and performance

Each `foreachBatch` micro-batch produces one validated DataFrame that is reused by the valid and rejected branches. The job persists that validated result with `MEMORY_AND_DISK`, rather than caching the raw parsed input, so validation expressions are materialized once and partitions that do not fit in storage memory may spill to disk. The `finally` block always calls `unpersist()` so completed micro-batches do not accumulate in executor storage memory.

This cache is justified because the validated DataFrame feeds two actions. Do not extend caching to one-use DataFrames without evidence from the Spark UI. When diagnosing performance, inspect the micro-batch stage for task-duration skew, peak execution memory, spill, GC time, shuffle read/write, failed tasks, and executor loss before changing executor sizes or partition counts.

## Data-quality observability milestone

The streaming job performs one aggregate over the cached validated batch to calculate its quality counters. The invariant `processed_count = valid_count + rejected_count` is enforced both in code and by a PostgreSQL check constraint. The metric upsert occurs only after both record sinks return successfully, and the smoke test verifies that the metric total advances along with both sink tables.

The three sink operations are not one distributed transaction. Metric upsert idempotency prevents duplicate metric rows, while full retry-safe handling for record sinks is intentionally left as a separate reliability milestone.

## MVP limitations

This repository is intentionally a local-development project, not a production deployment. Authentication, secrets management, schema registry, multi-broker Kafka, HA Postgres, centralized metrics, and Kubernetes deployment are future work.
