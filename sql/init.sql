CREATE TABLE IF NOT EXISTS payments (
    transaction_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    status VARCHAR(24) NOT NULL,
    country CHAR(2) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_event_time ON payments(event_time);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);

CREATE TABLE IF NOT EXISTS rejected_payments (
    id BIGSERIAL PRIMARY KEY,
    raw_event TEXT,
    reason TEXT NOT NULL,
    event_time TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_payment_metrics (
    metric_date DATE NOT NULL,
    status VARCHAR(24) NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, status)
);

CREATE TABLE IF NOT EXISTS data_quality_daily (
    metric_date DATE PRIMARY KEY,
    valid_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    rejection_rate NUMERIC(8, 5) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS streaming_batch_quality (
    stream_name VARCHAR(128) NOT NULL,
    batch_id BIGINT NOT NULL,
    processed_count BIGINT NOT NULL CHECK (processed_count >= 0),
    valid_count BIGINT NOT NULL CHECK (valid_count >= 0),
    rejected_count BIGINT NOT NULL CHECK (rejected_count >= 0),
    rejection_rate NUMERIC(8, 5) NOT NULL CHECK (rejection_rate BETWEEN 0 AND 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_name, batch_id),
    CHECK (processed_count = valid_count + rejected_count)
);
