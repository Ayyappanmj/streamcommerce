"""Dataset specifications shared by ingestion, processing and quality code."""
from __future__ import annotations

from dataclasses import dataclass

DATASET_ORDER = ("customers", "products", "orders", "order_items", "payments", "refunds")
ORDER_STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled")
PAYMENT_STATUSES = ("captured", "failed")
EVENT_TYPES = ("order_created", "order_paid", "order_shipped", "order_cancelled", "order_refunded")
CURRENCIES = ("USD",)


@dataclass(frozen=True)
class DatasetSpec:
    """Column types are one of: str, int, float, datetime."""

    name: str
    columns: dict[str, str]
    key: str
    required: tuple[str, ...]

    @property
    def column_names(self) -> list[str]:
        return list(self.columns)


DATASETS: dict[str, DatasetSpec] = {
    "customers": DatasetSpec(
        "customers",
        {"customer_id": "str", "email": "str", "country": "str", "segment": "str", "signup_ts": "datetime"},
        "customer_id", ("customer_id", "email", "signup_ts"),
    ),
    "products": DatasetSpec(
        "products",
        {"product_id": "str", "product_name": "str", "category": "str", "unit_price": "float",
         "created_ts": "datetime"},
        "product_id", ("product_id", "product_name", "unit_price"),
    ),
    "orders": DatasetSpec(
        "orders",
        {"order_id": "str", "customer_id": "str", "order_ts": "datetime", "status": "str",
         "currency": "str", "order_total": "float"},
        "order_id", ("order_id", "customer_id", "order_ts", "status", "order_total"),
    ),
    "order_items": DatasetSpec(
        "order_items",
        {"order_item_id": "str", "order_id": "str", "product_id": "str", "quantity": "int",
         "unit_price": "float", "line_total": "float"},
        "order_item_id", ("order_item_id", "order_id", "product_id", "quantity", "unit_price"),
    ),
    "payments": DatasetSpec(
        "payments",
        {"payment_id": "str", "order_id": "str", "amount": "float", "method": "str",
         "payment_ts": "datetime", "payment_status": "str"},
        "payment_id", ("payment_id", "order_id", "amount", "payment_ts"),
    ),
    "refunds": DatasetSpec(
        "refunds",
        {"refund_id": "str", "order_id": "str", "amount": "float", "refund_ts": "datetime", "reason": "str"},
        "refund_id", ("refund_id", "order_id", "amount", "refund_ts"),
    ),
}
