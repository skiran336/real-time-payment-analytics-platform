import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

STATUSES = ["AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED"]
PAYMENT_METHODS = ["CARD", "WALLET", "BANK_TRANSFER"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
COUNTRIES = ["US", "GB", "DE", "IN", "CA"]


def build_event() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"cust-{random.randint(1, 5000):05d}",
        "merchant_id": f"merchant-{random.randint(1, 250):04d}",
        "amount": round(random.uniform(1.0, 1500.0), 2),
        "currency": random.choice(CURRENCIES),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
        "country": random.choice(COUNTRIES),
    }


def inject_quality_issue(event: dict, invalid_rate: float) -> dict:
    if random.random() >= invalid_rate:
        return event

    issue = random.choice(["missing_customer", "negative_amount", "bad_status"])
    broken = event.copy()
    if issue == "missing_customer":
        broken["customer_id"] = None
    elif issue == "negative_amount":
        broken["amount"] = -abs(float(broken["amount"]))
    else:
        broken["status"] = "UNKNOWN"
    return broken


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic payment events.")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=25.0, help="events per second")
    parser.add_argument("--invalid-rate", type=float, default=0.03)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "payments.raw")
    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "payment-event-generator"})

    previous_events: list[dict] = []
    sleep_seconds = 1.0 / max(args.rate, 0.1)

    for index in range(args.count):
        if previous_events and random.random() < args.duplicate_rate:
            event = random.choice(previous_events).copy()
        else:
            event = build_event()
            previous_events.append(event)
            previous_events = previous_events[-250:]

        event = inject_quality_issue(event, args.invalid_rate)
        payload = json.dumps(event).encode("utf-8")
        producer.produce(topic, key=str(event.get("transaction_id", "missing")), value=payload, callback=delivery_report)
        producer.poll(0)

        if (index + 1) % 100 == 0:
            print(f"published {index + 1} events")
        time.sleep(sleep_seconds)

    producer.flush()
    print(f"done: published {args.count} events to {topic}")


if __name__ == "__main__":
    main()
