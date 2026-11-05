"""Configurable quality rules.

A rule is a dict: ``{name, type, severity, stage, ...params}``.
* severity ``error``   -> failing rows are quarantined;
* severity ``warn``    -> failures are recorded but rows are kept;
* stage ``ingest``     -> evaluated per batch file; stage ``final`` -> evaluated over the processed layer.

Override with a JSON file (QUALITY_RULES_PATH): ``{"disable": ["rule_name"], "rules": {...replace...}}``.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from src.common.exceptions import DataQualityError
from src.common.schemas import ORDER_STATUSES, PAYMENT_STATUSES


def _r(name: str, rule_type: str, severity: str = "error", stage: str = "ingest", **params: object) -> dict:
    return {"name": name, "type": rule_type, "severity": severity, "stage": stage, **params}


DEFAULT_RULES: dict[str, list[dict]] = {
    "customers": [
        _r("customers_required_fields", "required_fields", columns=["customer_id", "email", "signup_ts"]),
        _r("customers_parseable_types", "parseable"),
        _r("customers_id_format", "identifier_format", column="customer_id", pattern=r"^C\d{6,}$"),
    ],
    "products": [
        _r("products_required_fields", "required_fields", columns=["product_id", "product_name", "unit_price"]),
        _r("products_parseable_types", "parseable"),
        _r("products_id_format", "identifier_format", column="product_id", pattern=r"^P\d{5,}$"),
        _r("products_price_non_negative", "non_negative", column="unit_price"),
    ],
    "orders": [
        _r("orders_required_fields", "required_fields",
           columns=["order_id", "customer_id", "order_ts", "status", "order_total"]),
        _r("orders_parseable_types", "parseable"),
        _r("orders_id_format", "identifier_format", column="order_id", pattern=r"^ORD-\d{7,}$"),
        _r("orders_total_non_negative", "non_negative", column="order_total"),
        _r("orders_status_allowed", "allowed_values", column="status", values=list(ORDER_STATUSES)),
        _r("orders_customer_exists", "reference", column="customer_id", ref_dataset="customers",
           ref_column="customer_id"),
        _r("orders_total_matches_items", "amount_matches", severity="warn", stage="final", column="order_total",
           key="order_id", ref_dataset="order_items", ref_key="order_id", ref_column="line_total"),
    ],
    "order_items": [
        _r("items_required_fields", "required_fields",
           columns=["order_item_id", "order_id", "product_id", "quantity", "unit_price"]),
        _r("items_parseable_types", "parseable"),
        _r("items_quantity_positive", "positive", column="quantity"),
        _r("items_price_non_negative", "non_negative", column="unit_price"),
        _r("items_order_exists", "reference", column="order_id", ref_dataset="orders", ref_column="order_id"),
        _r("items_product_exists", "reference", column="product_id", ref_dataset="products", ref_column="product_id"),
    ],
    "payments": [
        _r("payments_required_fields", "required_fields", columns=["payment_id", "order_id", "amount", "payment_ts"]),
        _r("payments_parseable_types", "parseable"),
        _r("payments_amount_non_negative", "non_negative", column="amount"),
        _r("payments_status_allowed", "allowed_values", column="payment_status", values=list(PAYMENT_STATUSES)),
        _r("payments_order_exists", "reference", column="order_id", ref_dataset="orders", ref_column="order_id"),
        _r("payments_match_order_total", "amount_matches", severity="warn", stage="final", column="amount",
           key="order_id", ref_dataset="orders", ref_key="order_id", ref_column="order_total", agg="first",
           group_self=True),
    ],
    "refunds": [
        _r("refunds_required_fields", "required_fields", columns=["refund_id", "order_id", "amount", "refund_ts"]),
        _r("refunds_parseable_types", "parseable"),
        _r("refunds_amount_non_negative", "non_negative", column="amount"),
        _r("refunds_order_exists", "reference", column="order_id", ref_dataset="orders", ref_column="order_id"),
    ],
}


def load_rules(path: str | Path | None = None) -> dict[str, list[dict]]:
    """Return the active rules (defaults, optionally modified by a JSON override file)."""
    rules = copy.deepcopy(DEFAULT_RULES)
    if not path:
        return rules
    p = Path(path)
    if not p.is_file():
        raise DataQualityError(f"Quality rules file not found: {p}")
    try:
        override = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DataQualityError(f"Quality rules file is not valid JSON: {exc}") from exc
    if "rules" in override:
        rules = override["rules"]
    disabled = set(override.get("disable", []))
    return {ds: [r for r in rs if r["name"] not in disabled] for ds, rs in rules.items()}


if __name__ == "__main__":  # python -m src.quality.quality_rules  -> dump defaults as JSON
    print(json.dumps({"rules": DEFAULT_RULES}, indent=2))
