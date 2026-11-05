"""Pure pandas transformations: type coercion and dimensional-model construction."""
from __future__ import annotations

import hashlib

import pandas as pd

from src.common.exceptions import DataQualityError
from src.common.schemas import DatasetSpec


def coerce_types(df: pd.DataFrame, spec: DatasetSpec) -> pd.DataFrame:
    """Convert columns to the spec types. Values that cannot be converted become null and the
    offending column names are listed in ``_parse_errors`` so quality rules can quarantine the row."""
    out = pd.DataFrame(index=df.index)
    errors = pd.Series("", index=df.index, dtype=object)
    for col, typ in spec.columns.items():
        raw = df[col].astype(object) if col in df else pd.Series(None, index=df.index, dtype=object)
        raw = raw.map(lambda v: (v.strip() or None) if isinstance(v, str) else v)
        raw = raw.where(raw.notna(), None)
        if typ == "str":
            typed = raw
        elif typ == "float":
            typed = pd.to_numeric(raw, errors="coerce").astype("float64")
        elif typ == "int":
            num = pd.to_numeric(raw, errors="coerce")
            num = num.where(num.isna() | (num == num.round()))
            typed = num.astype("Int64")
        elif typ == "datetime":
            typed = pd.to_datetime(raw, utc=True, errors="coerce", format="ISO8601")
        else:
            raise DataQualityError(f"Unsupported column type {typ!r} for {col}")
        bad = raw.notna() & typed.isna()
        errors = errors.where(~bad, errors + col + ",")
        out[col] = typed
    out["_parse_errors"] = errors.str.rstrip(",")
    return out


def stable_key(value: str) -> int:
    """Deterministic 60-bit surrogate key derived from a business key (stable across reruns)."""
    return int(hashlib.md5(value.encode("utf-8")).hexdigest()[:15], 16)


def date_key(series: pd.Series) -> pd.Series:
    """YYYYMMDD integer key (UTC) for a datetime Series."""
    return series.dt.strftime("%Y%m%d").astype("int64")


def to_naive_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Convert tz-aware datetime columns to naive UTC (portable TIMESTAMP in DuckDB)."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out


def _assert_unique(df: pd.DataFrame, col: str, table: str) -> None:
    if df[col].duplicated().any():
        raise DataQualityError(f"Surrogate key collision or duplicate business key in {table}.{col}")


def build_dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    dim = customers.copy()
    dim.insert(0, "customer_key", dim["customer_id"].map(stable_key))
    _assert_unique(dim, "customer_key", "dim_customers")
    return dim[["customer_key", "customer_id", "email", "country", "segment", "signup_ts"]]


def build_dim_products(products: pd.DataFrame) -> pd.DataFrame:
    dim = products.copy()
    dim.insert(0, "product_key", dim["product_id"].map(stable_key))
    _assert_unique(dim, "product_key", "dim_products")
    return dim[["product_key", "product_id", "product_name", "category", "unit_price", "created_ts"]]


def build_dim_date(*series: pd.Series) -> pd.DataFrame:
    """Calendar dimension covering the min..max date across the given datetime Series."""
    values = pd.concat([s.dropna() for s in series]) if series else pd.Series(dtype="datetime64[ns, UTC]")
    cols = ["date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "is_weekend"]
    if values.empty:
        return pd.DataFrame(columns=cols)
    days = pd.date_range(values.min().tz_convert("UTC").normalize().tz_localize(None),
                         values.max().tz_convert("UTC").normalize().tz_localize(None), freq="D")
    dim = pd.DataFrame({"full_date": days})
    dim["date_key"] = dim["full_date"].dt.strftime("%Y%m%d").astype("int64")
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["day"] = dim["full_date"].dt.day
    dim["day_of_week"] = dim["full_date"].dt.dayofweek + 1  # 1 = Monday
    dim["is_weekend"] = dim["day_of_week"] >= 6
    dim["full_date"] = dim["full_date"].dt.date
    return dim[cols]


def build_fact_orders(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Grain: one row per order. Item counts are pre-aggregated so order revenue is never multiplied."""
    agg = items.groupby("order_id").agg(item_count=("order_item_id", "count"), units=("quantity", "sum"))
    fact = orders.merge(agg, how="left", left_on="order_id", right_index=True)
    fact["item_count"] = fact["item_count"].fillna(0).astype("int64")
    fact["units"] = fact["units"].fillna(0).astype("int64")
    fact.insert(0, "order_key", fact["order_id"].map(stable_key))
    fact["customer_key"] = fact["customer_id"].map(stable_key)
    fact["order_date_key"] = date_key(fact["order_ts"])
    _assert_unique(fact, "order_key", "fact_orders")
    return fact[["order_key", "order_id", "customer_key", "order_date_key", "order_ts", "status", "currency",
                 "order_total", "item_count", "units"]]


def build_fact_order_items(items: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """Grain: one row per order item."""
    fact = items.merge(orders[["order_id", "order_ts"]], on="order_id", how="inner")
    fact.insert(0, "order_item_key", fact["order_item_id"].map(stable_key))
    fact["order_key"] = fact["order_id"].map(stable_key)
    fact["product_key"] = fact["product_id"].map(stable_key)
    fact["order_date_key"] = date_key(fact["order_ts"])
    fact["line_total"] = fact["line_total"].fillna((fact["quantity"] * fact["unit_price"]).round(2))
    _assert_unique(fact, "order_item_key", "fact_order_items")
    return fact[["order_item_key", "order_item_id", "order_key", "product_key", "order_date_key", "quantity",
                 "unit_price", "line_total"]]


def build_fact_payments(payments: pd.DataFrame) -> pd.DataFrame:
    """Grain: one row per payment transaction."""
    fact = payments.copy()
    fact.insert(0, "payment_key", fact["payment_id"].map(stable_key))
    fact["order_key"] = fact["order_id"].map(stable_key)
    fact["payment_date_key"] = date_key(fact["payment_ts"])
    _assert_unique(fact, "payment_key", "fact_payments")
    return fact[["payment_key", "payment_id", "order_key", "payment_date_key", "amount", "method",
                 "payment_status", "payment_ts"]]


def build_fact_refunds(refunds: pd.DataFrame) -> pd.DataFrame:
    """Grain: one row per refund transaction."""
    fact = refunds.copy()
    fact.insert(0, "refund_key", fact["refund_id"].map(stable_key))
    fact["order_key"] = fact["order_id"].map(stable_key)
    fact["refund_date_key"] = date_key(fact["refund_ts"])
    _assert_unique(fact, "refund_key", "fact_refunds")
    return fact[["refund_key", "refund_id", "order_key", "refund_date_key", "amount", "reason", "refund_ts"]]


def build_warehouse_tables(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Build every dimensional-model table from the cleaned datasets."""
    orders, items = frames["orders"], frames["order_items"]
    return {
        "dim_customers": build_dim_customers(frames["customers"]),
        "dim_products": build_dim_products(frames["products"]),
        "dim_date": build_dim_date(orders["order_ts"], frames["payments"]["payment_ts"],
                                   frames["refunds"]["refund_ts"]),
        "fact_orders": build_fact_orders(orders, items),
        "fact_order_items": build_fact_order_items(items, orders),
        "fact_payments": build_fact_payments(frames["payments"]),
        "fact_refunds": build_fact_refunds(frames["refunds"]),
    }


def total_sales(fact_orders: pd.DataFrame) -> float:
    """Gross sales: sum of order totals for non-cancelled orders. Uses the order grain only,
    so joining to items or payments elsewhere can never inflate it."""
    return round(float(fact_orders.loc[fact_orders["status"] != "cancelled", "order_total"].sum()), 2)
