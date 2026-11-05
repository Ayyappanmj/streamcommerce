"""Small UI helpers shared by all dashboard pages."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from app.components import data


def setup(title: str) -> None:
    st.set_page_config(page_title=f"StreamCommerce - {title}", layout="wide")
    st.title(title)


def empty_state(message: str) -> None:
    st.info(message)


def load(sql: str, empty_message: str = "No data yet. Run the batch pipeline first.") -> pd.DataFrame | None:
    """Run a warehouse query; show an explanatory message and return None when data is unavailable or empty."""
    try:
        df = data.query(sql)
    except data.DataUnavailable as exc:
        empty_state(str(exc))
        return None
    if df.empty:
        empty_state(empty_message)
        return None
    return df


def load_meta(kind: str) -> pd.DataFrame | None:
    try:
        df = data.metadata(kind)
    except data.DataUnavailable as exc:
        empty_state(str(exc))
        return None
    if df.empty:
        empty_state(f"No {kind} recorded yet.")
        return None
    return df


def load_stream() -> dict[str, pd.DataFrame] | None:
    try:
        return data.stream_tables()
    except data.DataUnavailable as exc:
        empty_state(str(exc))
        return None


def fmt_money(value: float | None) -> str:
    return "n/a" if value is None or pd.isna(value) else f"${value:,.2f}"


def fmt_pct(value: float | None) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{value * 100:.1f}%"
