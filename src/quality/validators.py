"""Reusable vectorised validators. Each returns a boolean Series: True = the row FAILS the rule."""
from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from src.common.exceptions import DataQualityError

Context = dict[str, "pd.DataFrame | None"]


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def required_fields(df: pd.DataFrame, columns: list[str], **_: object) -> pd.Series:
    """Fail rows where any listed column is null or blank."""
    mask = pd.Series(False, index=df.index)
    for col in columns:
        mask |= _blank(df[col]) if col in df else True
    return mask


def parseable(df: pd.DataFrame, **_: object) -> pd.Series:
    """Fail rows whose values could not be converted to the declared type (see ``_parse_errors``)."""
    if "_parse_errors" not in df:
        return pd.Series(False, index=df.index)
    return df["_parse_errors"].fillna("").astype(str) != ""


def identifier_format(df: pd.DataFrame, column: str, pattern: str, **_: object) -> pd.Series:
    """Fail non-blank values that do not match the regex ``pattern`` (blank handled by required_fields)."""
    ok = df[column].astype(str).str.match(pattern)
    return ~_blank(df[column]) & ~ok


def non_negative(df: pd.DataFrame, column: str, **_: object) -> pd.Series:
    """Fail values below zero (nulls are handled by required/parseable rules)."""
    return pd.to_numeric(df[column], errors="coerce") < 0


def positive(df: pd.DataFrame, column: str, **_: object) -> pd.Series:
    """Fail values that are not strictly greater than zero."""
    return pd.to_numeric(df[column], errors="coerce") <= 0


def allowed_values(df: pd.DataFrame, column: str, values: list[str], **_: object) -> pd.Series:
    """Fail non-blank values outside the allowed set."""
    return ~_blank(df[column]) & ~df[column].isin(values)


def reference(df: pd.DataFrame, column: str, ref_dataset: str, ref_column: str, context: Context | None = None,
              **_: object) -> pd.Series | None:
    """Fail non-blank values missing from the reference dataset. Returns None when the reference is unavailable."""
    ref = (context or {}).get(ref_dataset)
    if ref is None:
        return None
    return ~_blank(df[column]) & ~df[column].isin(set(ref[ref_column]))


def amount_matches(df: pd.DataFrame, column: str, key: str, ref_dataset: str, ref_key: str, ref_column: str,
                   agg: str = "sum", group_self: bool = False, tolerance: float = 0.01,
                   context: Context | None = None, **_: object) -> pd.Series | None:
    """Fail rows whose (aggregated) amount differs from the reference amount by more than ``tolerance``.

    Rows without a counterpart in the reference are not flagged (use ``reference`` for that).
    """
    ref = (context or {}).get(ref_dataset)
    if ref is None:
        return None
    own = df.groupby(key)[column].sum() if group_self else df.drop_duplicates(key).set_index(key)[column]
    ref_vals = ref.groupby(ref_key)[ref_column].sum() if agg == "sum" else ref.drop_duplicates(ref_key).set_index(
        ref_key)[ref_column]
    own_val = df[key].map(own)
    ref_val = df[key].map(ref_vals)
    return ref_val.notna() & ((own_val - ref_val).abs() > tolerance)


VALIDATORS: dict[str, Callable[..., pd.Series | None]] = {
    "required_fields": required_fields, "parseable": parseable, "identifier_format": identifier_format,
    "non_negative": non_negative, "positive": positive, "allowed_values": allowed_values,
    "reference": reference, "amount_matches": amount_matches,
}


def get_validator(rule_type: str) -> Callable[..., pd.Series | None]:
    try:
        return VALIDATORS[rule_type]
    except KeyError as exc:
        raise DataQualityError(f"Unknown quality rule type: {rule_type!r}") from exc


def detect_schema_drift(columns: list[str], expected: list[str]) -> dict[str, list[str]]:
    """Return missing and unexpected columns compared with the expected schema."""
    return {"missing": [c for c in expected if c not in columns],
            "unexpected": [c for c in columns if c not in expected]}

