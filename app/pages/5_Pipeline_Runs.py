import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Pipeline Runs")
runs = ui.load_meta("runs")
if runs is not None:
    runs["started_at"] = pd.to_datetime(runs["started_at"], utc=True, format="ISO8601")
    runs["finished_at"] = pd.to_datetime(runs["finished_at"], utc=True, format="ISO8601", errors="coerce")
    runs["duration_s"] = (runs["finished_at"] - runs["started_at"]).dt.total_seconds()
    finished = runs[runs["status"].isin(["success", "failed"])]
    st.metric("Success rate (finished runs)", ui.fmt_pct((finished["status"] == "success").mean() if len(finished) else None))
    if finished["duration_s"].notna().any():
        st.plotly_chart(px.bar(finished, x="started_at", y="duration_s", color="status",
                               title="Processing duration (seconds)"), use_container_width=True)
    st.dataframe(runs.drop(columns=["details"], errors="ignore"), use_container_width=True)
    st.subheader("Ingestion manifests")
    manifests = ui.load_meta("manifests")
    if manifests is not None:
        st.dataframe(manifests, use_container_width=True)
