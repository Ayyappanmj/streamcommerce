import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Data Quality")
q = ui.load_meta("quality")
if q is not None:
    failed = q[q["status"] == "failed"]
    a, b, c = st.columns(3)
    a.metric("Rule evaluations", f"{len(q):,}")
    b.metric("Failed evaluations", f"{len(failed):,}")
    c.metric("Records failing rules", f"{int(failed['failed_records'].sum()):,}")
    if len(failed):
        agg = failed.groupby(["dataset_name", "rule_name"], as_index=False)["failed_records"].sum()
        st.plotly_chart(px.bar(agg, x="rule_name", y="failed_records", color="dataset_name",
                               title="Failed records by rule"), use_container_width=True)
    else:
        st.success("No rule failures recorded.")
    st.dataframe(q.sort_values("id", ascending=False), use_container_width=True)
