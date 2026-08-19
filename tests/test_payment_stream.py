import json

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from src.streaming import payment_stream
from src.streaming.payment_stream import (
    parse_kafka_events,
    prepare_checked_batch,
    prepare_for_jdbc,
    split_valid_and_rejected,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("payment-stream-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.use.daemon", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def kafka_row(payload: dict, partition: int = 0, offset: int = 0) -> tuple:
    return (json.dumps(payload).encode(), "payments.raw", partition, offset, None)


def valid_payment() -> dict:
    return {
        "transaction_id": "tx-1",
        "customer_id": "cust-1",
        "merchant_id": "merchant-1",
        "amount": 42.50,
        "currency": "USD",
        "event_time": "2026-08-17T12:00:00+00:00",
        "payment_method": "CARD",
        "status": "CAPTURED",
        "country": "US",
    }


def test_parse_validate_and_route_payments(spark):
    invalid = valid_payment() | {"transaction_id": "tx-2", "amount": -5.0}
    kafka_df = spark.createDataFrame(
        [kafka_row(valid_payment()), kafka_row(invalid, partition=1, offset=4)],
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    parsed = parse_kafka_events(kafka_df)
    valid, rejected = split_valid_and_rejected(parsed)

    assert [row.transaction_id for row in valid.collect()] == ["tx-1"]
    rejected_row = rejected.select("transaction_id", "validation_errors", "kafka_partition", "kafka_offset").first()
    assert rejected_row.transaction_id == "tx-2"
    assert "invalid amount" in rejected_row.validation_errors
    assert (rejected_row.kafka_partition, rejected_row.kafka_offset) == (1, 4)


def test_null_status_and_payment_method_are_rejected(spark):
    invalid = valid_payment() | {"status": None, "payment_method": None}
    kafka_df = spark.createDataFrame(
        [kafka_row(invalid)],
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    _, rejected = split_valid_and_rejected(parse_kafka_events(kafka_df))
    errors = rejected.select("validation_errors").first().validation_errors

    assert "invalid status" in errors
    assert "invalid payment_method" in errors


def test_prepare_for_jdbc_controls_output_partitions(spark):
    df = spark.range(20, numPartitions=2)

    repartitioned = prepare_for_jdbc(df, num_partitions=3)

    assert repartitioned.rdd.getNumPartitions() == 3


def test_prepare_for_jdbc_rejects_invalid_partition_count(spark):
    with pytest.raises(ValueError, match="at least 1"):
        prepare_for_jdbc(spark.range(1), num_partitions=0)


def test_checked_batch_uses_memory_and_disk_storage(spark):
    kafka_df = spark.createDataFrame(
        [kafka_row(valid_payment())],
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    )
    parsed = parse_kafka_events(kafka_df)

    checked = prepare_checked_batch(parsed)
    try:
        assert checked.storageLevel == StorageLevel.MEMORY_AND_DISK
        assert checked.select("validation_errors").first().validation_errors == ""
    finally:
        checked.unpersist()


def test_process_batch_releases_cached_validation_result(spark, monkeypatch):
    kafka_df = spark.createDataFrame(
        [kafka_row(valid_payment())],
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    )
    parsed = parse_kafka_events(kafka_df)
    captured = {}
    original_route = payment_stream.route_checked_payments

    def capture_route(checked):
        captured["checked"] = checked
        assert checked.storageLevel == StorageLevel.MEMORY_AND_DISK
        return original_route(checked)

    monkeypatch.setattr(payment_stream, "route_checked_payments", capture_route)
    monkeypatch.setattr(payment_stream, "write_valid_batch", lambda _df, _batch_id: None)
    monkeypatch.setattr(payment_stream, "write_rejected_batch", lambda _df, _batch_id: None)

    payment_stream.process_batch(parsed, batch_id=7)

    assert captured["checked"].storageLevel == StorageLevel.NONE
