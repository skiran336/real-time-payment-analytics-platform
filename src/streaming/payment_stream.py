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
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
SHUFFLE_PARTITIONS = int(os.getenv("SHUFFLE_PARTITIONS", "6"))
JDBC_WRITE_PARTITIONS = int(os.getenv("JDBC_WRITE_PARTITIONS", "3"))

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
        F.when(
            F.col("status").isNull()
            | ~F.col("status").isin("AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED"),
            "invalid status",
        ),
        F.when(
            F.col("payment_method").isNull()
            | ~F.col("payment_method").isin("CARD", "WALLET", "BANK_TRANSFER"),
            "invalid payment_method",
        ),
        F.when(F.col("country").isNull() | (F.length("country") != 2), "invalid country"),
    )
    return df.withColumn("validation_errors", errors)


def parse_kafka_events(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka values while retaining source coordinates for traceability."""
    return (
        kafka_df.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("payload", F.from_json("raw_json", PAYMENT_SCHEMA))
        .select("raw_json", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "payload.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )


def split_valid_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    checked = add_validation_columns(df)
    return (
        checked.where(F.length("validation_errors") == 0),
        checked.where(F.length("validation_errors") > 0),
    )


def prepare_for_jdbc(df: DataFrame, num_partitions: int = JDBC_WRITE_PARTITIONS) -> DataFrame:
    """Bound JDBC parallelism with an intentional round-robin shuffle."""
    if num_partitions < 1:
        raise ValueError("num_partitions must be at least 1")
    return df.repartition(num_partitions)


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
        prepare_for_jdbc(batch_df).select(
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
        prepare_for_jdbc(batch_df).select(
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


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Validate one micro-batch once, then route its two outputs."""
    batch_df.persist()
    try:
        if batch_df.isEmpty():
            return
        valid, rejected = split_valid_and_rejected(batch_df)
        write_valid_batch(valid, batch_id)
        write_rejected_batch(rejected, batch_id)
    finally:
        batch_df.unpersist()


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("payment-data-quality-stream")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    query = (
        parse_kafka_events(kafka_df)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/payment-quality")
        .outputMode("append")
        .start()
    )

    print(
        f"streaming from {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP_SERVERS}; "
        f"input cap={MAX_OFFSETS_PER_TRIGGER}, shuffle partitions={SHUFFLE_PARTITIONS}, "
        f"JDBC write partitions={JDBC_WRITE_PARTITIONS}"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
