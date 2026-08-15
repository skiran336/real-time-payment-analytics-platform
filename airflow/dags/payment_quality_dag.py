import os
from datetime import datetime, timezone

import psycopg2
from airflow.sdk import dag, task


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "host.docker.internal"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "payments"),
        user=os.getenv("POSTGRES_USER", "payments"),
        password=os.getenv("POSTGRES_PASSWORD", "payments"),
    )


@dag(
    dag_id="payment_quality_daily",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["payments", "data-quality", "reconciliation"],
)
def payment_quality_daily():
    @task(retries=2)
    def aggregate_payments() -> None:
        sql = """
        INSERT INTO daily_payment_metrics (metric_date, status, transaction_count, total_amount, updated_at)
        SELECT
            event_time::date,
            status,
            COUNT(*),
            COALESCE(SUM(amount), 0),
            NOW()
        FROM payments
        WHERE event_time >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY event_time::date, status
        ON CONFLICT (metric_date, status)
        DO UPDATE SET
            transaction_count = EXCLUDED.transaction_count,
            total_amount = EXCLUDED.total_amount,
            updated_at = NOW();
        """
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(sql)

    @task(retries=2)
    def calculate_data_quality() -> None:
        sql = """
        WITH valid AS (
            SELECT event_time::date AS metric_date, COUNT(*) AS valid_count
            FROM payments
            WHERE event_time >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY event_time::date
        ), rejected AS (
            SELECT COALESCE(event_time::date, rejected_at::date) AS metric_date, COUNT(*) AS rejected_count
            FROM rejected_payments
            WHERE rejected_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY COALESCE(event_time::date, rejected_at::date)
        ), dates AS (
            SELECT metric_date FROM valid
            UNION
            SELECT metric_date FROM rejected
        )
        INSERT INTO data_quality_daily (metric_date, valid_count, rejected_count, rejection_rate, updated_at)
        SELECT
            d.metric_date,
            COALESCE(v.valid_count, 0),
            COALESCE(r.rejected_count, 0),
            CASE
                WHEN COALESCE(v.valid_count, 0) + COALESCE(r.rejected_count, 0) = 0 THEN 0
                ELSE COALESCE(r.rejected_count, 0)::numeric /
                     (COALESCE(v.valid_count, 0) + COALESCE(r.rejected_count, 0))
            END,
            NOW()
        FROM dates d
        LEFT JOIN valid v USING (metric_date)
        LEFT JOIN rejected r USING (metric_date)
        ON CONFLICT (metric_date)
        DO UPDATE SET
            valid_count = EXCLUDED.valid_count,
            rejected_count = EXCLUDED.rejected_count,
            rejection_rate = EXCLUDED.rejection_rate,
            updated_at = NOW();
        """
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(sql)

    aggregate_payments() >> calculate_data_quality()


payment_quality_daily()
