"""Duplicate detection for batch DataFrames."""
from __future__ import annotations

import pandas as pd


def split_duplicates(df: pd.DataFrame, key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split ``df`` into (first occurrence per key, later duplicates).

    The first row in file order wins. Rows with a null key are never treated as duplicates
    (they are rejected later by the required-field rule). Duplicates carry ``reject_reason='duplicate_key'``
    so they are quarantined with an explanation rather than silently dropped.
    """
    if "_dup_rank" in df:  # already computed by the Spark engine
        rank = df["_dup_rank"].astype(int)
    else:
        rank = pd.Series(1, index=df.index)
        has_key = df[key].notna()
        if has_key.any():
            rank[has_key] = df[has_key].groupby(key).cumcount() + 1
    unique = df[rank == 1].drop(columns=["_dup_rank"], errors="ignore")
    dups = df[rank > 1].drop(columns=["_dup_rank"], errors="ignore").copy()
    dups["reject_reason"] = "duplicate_key"
    return unique, dups
