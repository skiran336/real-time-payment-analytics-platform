from src.producer.generate_payments import build_event, inject_quality_issue


def test_build_event_contains_expected_fields():
    event = build_event()
    expected = {
        "transaction_id",
        "customer_id",
        "merchant_id",
        "amount",
        "currency",
        "event_time",
        "payment_method",
        "status",
        "country",
    }
    assert set(event) == expected
    assert event["amount"] > 0


def test_invalid_event_is_injected_when_rate_is_one():
    event = build_event()
    broken = inject_quality_issue(event, invalid_rate=1.0)
    assert broken != event
