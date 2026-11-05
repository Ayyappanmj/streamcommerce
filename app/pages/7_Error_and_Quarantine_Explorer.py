import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import data, ui  # noqa: E402

ui.setup("Error and Quarantine Explorer")
settings = data.get_settings()
st.subheader("Batch quarantine files")
files = sorted(settings.quarantine_dir.glob("*/*.csv")) if settings.quarantine_dir.exists() else []
if not files:
    ui.empty_state("No quarantined batch records.")
else:
    choice = st.selectbox("File", files, format_func=lambda p: f"{p.parent.name}/{p.name}")
    frame = pd.read_csv(choice, dtype=str)
    st.metric("Rows in file", len(frame))
    st.dataframe(frame, use_container_width=True)
st.subheader("Quarantined streaming events")
tables = ui.load_stream()
if tables is not None:
    bad = tables["quarantined_events"]
    if bad.empty:
        ui.empty_state("No quarantined streaming events.")
    else:
        st.dataframe(bad.sort_values("id", ascending=False), use_container_width=True)
st.subheader("Processing errors")
errors = ui.load_meta("errors")
if errors is not None:
    st.dataframe(errors, use_container_width=True)
