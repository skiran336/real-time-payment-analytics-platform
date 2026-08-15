VALID_STATUSES = {"AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED"}
VALID_PAYMENT_METHODS = {"CARD", "WALLET", "BANK_TRANSFER"}
REQUIRED_FIELDS = (
    "transaction_id",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "event_time",
    "payment_method",
    "status",
    "country",
)
