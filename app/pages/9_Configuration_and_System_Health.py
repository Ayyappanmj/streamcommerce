import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import importlib.util  # noqa: E402

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import data, ui  # noqa: E402

ui.setup("Configuration and System Health")
s = data.get_settings()
st.subheader("Configuration (secrets are never displayed)")
st.json({"data_dir": str(s.data_dir), "processed_format": s.processed_format, "metadata_backend": s.metadata_backend,
         "postgres": f"{s.postgres_host}:{s.postgres_port}/{s.postgres_db}" if s.metadata_backend == "postgres" else "disabled",
         "kafka_bootstrap": s.kafka_bootstrap, "kafka_topic": s.kafka_topic,
         "late_event_seconds": s.late_event_seconds})
st.subheader("Health checks")


def _metadata_ok() -> bool:
    try:
        data.metadata("runs")
        return True
    except data.DataUnavailable:
        return False


checks = [
    ("Warehouse file exists", s.warehouse_path.exists()),
    ("Streaming store exists", s.events_db_path.exists()),
    ("Metadata store reachable", _metadata_ok()),
    ("Kafka broker reachable (optional)", data.tcp_reachable(s.kafka_bootstrap)),
]
st.table(pd.DataFrame(checks, columns=["check", "ok"]))
st.subheader("Installed optional components")
mods = ["duckdb", "pyarrow", "pyspark", "confluent_kafka", "psycopg", "dbt", "snowflake.connector"]
st.table(pd.DataFrame([(m, importlib.util.find_spec(m.split(".")[0]) is not None) for m in mods],
                      columns=["module", "installed"]))
