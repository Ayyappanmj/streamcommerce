import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st  # noqa: E402

from app.components import data, ui  # noqa: E402

ui.setup("Warehouse Information")
settings = data.get_settings()
if settings.warehouse_path.exists():
    st.metric("Warehouse file size (MB)", f"{settings.warehouse_path.stat().st_size / 1e6:.2f}")
tables = ui.load("SELECT schema_name AS table_schema, table_name, estimated_size AS approx_rows FROM duckdb_tables() "
                 "ORDER BY schema_name, table_name", "The warehouse contains no tables yet.")
if tables is not None:
    st.dataframe(tables, use_container_width=True)
    cols = ui.load("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
                   "WHERE table_schema IN ('dw', 'analytics') ORDER BY 1, 2, ordinal_position")
    if cols is not None:
        st.subheader("Columns (dw and analytics schemas)")
        st.dataframe(cols, use_container_width=True)
st.subheader("Dataset metadata")
meta = ui.load_meta("datasets")
if meta is not None:
    st.dataframe(meta, use_container_width=True)
