import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments.raw")
JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/payments")
POSTGRES_USER = os.getenv("POSTGRES_USER", "payments")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "payments")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "spark-checkpoints")

PAYMENT_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("payment_method", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("country", T.StringType()),
    ]
)


def add_validation_columns(df: DataFrame) -> DataFrame:
    errors = F.concat_ws(
        "; ",
        F.when(F.col("transaction_id").isNull() | (F.length("transaction_id") == 0), "missing transaction_id"),
        F.when(F.col("customer_id").isNull() | (F.length("customer_id") == 0), "missing customer_id"),
        F.when(F.col("merchant_id").isNull() | (F.length("merchant_id") == 0), "missing merchant_id"),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), "invalid amount"),
        F.when(F.col("currency").isNull() | (F.length("currency") != 3), "invalid currency"),
        F.when(F.col("event_time").isNull(), "missing event_time"),
        F.when(~F.col("status").isin("AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED"), "invalid status"),
        F.when(~F.col("payment_method").isin("CARD", "WALLET", "BANK_TRANSFER"), "invalid payment_method"),
        F.when(F.col("country").isNull() | (F.length("country") != 2), "invalid country"),
    )
    return df.withColumn("validation_errors", errors)


def jdbc_options(table: str) -> dict:
    return {
        "url": JDBC_URL,
        "dbtable": table,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def write_valid_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    (
        batch_df.select(
            "transaction_id",
            "customer_id",
            "merchant_id",
            F.col("amount").cast(T.DecimalType(12, 2)).alias("amount"),
            "currency",
            "event_time",
            "payment_method",
            "status",
            "country",
        )
        .write.format("jdbc")
        .options(**jdbc_options("payments"))
        .mode("append")
        .save()
    )
    print(f"batch={batch_id}: wrote valid payments")


def write_rejected_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    (
        batch_df.select(
            F.col("raw_json").cast("string").alias("raw_event"),
            F.col("validation_errors").alias("reason"),
            "event_time",
        )
        .write.format("jdbc")
        .options(**jdbc_options("rejected_payments"))
        .mode("append")
        .save()
    )
    print(f"batch={batch_id}: wrote rejected payments")


def main() -> None:
    spark = SparkSession.builder.appName("payment-data-quality-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        kafka_df.select(F.col("value").cast("string").alias("raw_json"), F.col("timestamp").alias("kafka_timestamp"))
        .withColumn("payload", F.from_json("raw_json", PAYMENT_SCHEMA))
        .select("raw_json", "kafka_timestamp", "payload.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    checked = add_validation_columns(parsed)

    valid = (
        checked.where(F.length("validation_errors") == 0)
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["transaction_id"])
    )
    rejected = checked.where(F.length("validation_errors") > 0)

    valid_query = (
        valid.writeStream.foreachBatch(write_valid_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/valid")
        .outputMode("append")
        .start()
    )

    rejected_query = (
        rejected.writeStream.foreachBatch(write_rejected_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/rejected")
        .outputMode("append")
        .start()
    )

    print(f"streaming from {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP_SERVERS}")
    spark.streams.awaitAnyTermination()
    valid_query.stop()
    rejected_query.stop()


if __name__ == "__main__":
    main()
