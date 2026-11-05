"""Run configured rules against a DataFrame, producing valid rows, rejected rows and rule results."""
from __future__ import annotations

import logging

import pandas as pd

from src.metadata.models import QualityResult, utc_now
from src.quality.validators import Context, get_validator

log = logging.getLogger(__name__)
INTERNAL_COLUMNS = ("_parse_errors", "_dup_rank")


def strip_internal(df: pd.DataFrame) -> pd.DataFrame:
    """Remove helper columns used during processing."""
    return df.drop(columns=[c for c in INTERNAL_COLUMNS if c in df.columns])


def run_rules(dataset: str, df: pd.DataFrame, rules: list[dict], context: Context | None = None,
              stage: str = "ingest") -> tuple[pd.DataFrame, pd.DataFrame, list[QualityResult]]:
    """Evaluate ``rules`` of the given ``stage``.

    Returns ``(valid, rejected, results)``. ``rejected`` has a ``reject_reason`` column listing every failed
    error-severity rule; warn-severity rules are only recorded. Nothing is dropped without a reason.
    """
    reasons = pd.Series("", index=df.index, dtype=object)
    results: list[QualityResult] = []
    for rule in rules:
        if rule.get("stage", "ingest") != stage:
            continue
        params = {k: v for k, v in rule.items() if k not in ("name", "type", "severity", "stage")}
        mask = get_validator(rule["type"])(df, context=context, **params)
        now = utc_now()
        if mask is None:
            results.append(QualityResult(dataset, rule["name"], rule["severity"], 0, 0, "skipped",
                                         "reference dataset unavailable; rule not evaluated", now))
            continue
        failed = int(mask.sum())
        results.append(QualityResult(
            dataset, rule["name"], rule["severity"], len(df), failed, "failed" if failed else "passed",
            f"{failed} of {len(df)} records failed {rule['type']}" if failed else "", now))
        if rule["severity"] == "error" and failed:
            reasons = reasons.where(~mask, reasons + rule["name"] + ";")
    bad = reasons != ""
    rejected = strip_internal(df[bad]).copy()
    rejected["reject_reason"] = reasons[bad].str.rstrip(";")
    valid = strip_internal(df[~bad]).copy()
    return valid, rejected, results
