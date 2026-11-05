import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Streaming Monitor")
tables = ui.load_stream()
if tables is not None:
    ev, bad, dup = tables["processed_events"], tables["quarantined_events"], tables["duplicate_events"]
    a, b, c, d = st.columns(4)
    a.metric("Processed events", f"{len(ev):,}")
    b.metric("Failed (quarantined) events", f"{len(bad):,}")
    c.metric("Duplicate deliveries ignored", f"{len(dup):,}")
    d.metric("Late events", f"{int(ev['is_late'].sum()) if len(ev) else 0:,}")
    if len(ev):
        st.plotly_chart(px.histogram(ev, x="event_type", title="Events by type"), use_container_width=True)
        ev = ev.assign(ingested_at=pd.to_datetime(ev["ingested_at"], utc=True, format="ISO8601"))
        lag = (ev["ingested_at"] - pd.to_datetime(ev["event_ts"], utc=True)).dt.total_seconds() / 3600
        st.caption("Event time (business time) vs ingestion time (pipeline time) lag, in hours:")
        st.plotly_chart(px.histogram(lag.rename("lag_hours"), title="Ingestion lag (hours)"),
                        use_container_width=True)
        st.caption(f"Latest ingestion: {ev['ingested_at'].max()}")
    else:
        ui.empty_state("The streaming store exists but holds no valid events yet.")
