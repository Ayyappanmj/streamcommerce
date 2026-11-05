"""Executive Overview (entry page). Run with: streamlit run app/dashboard.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import data, ui  # noqa: E402

ui.setup("Executive Overview")

kpi = ui.load(
    "SELECT count(*) AS orders, coalesce(sum(order_total), 0) AS sales FROM dw.fact_orders WHERE status <> 'cancelled'",
    "The warehouse has no orders yet. Run the batch pipeline (README: Lightweight mode).")
if kpi is not None:
    orders, sales = int(kpi.loc[0, "orders"]), float(kpi.loc[0, "sales"])
    ret = ui.load("WITH c AS (SELECT customer_key, count(*) n FROM dw.fact_orders WHERE status <> 'cancelled' "
                  "GROUP BY 1) SELECT avg(CASE WHEN n >= 2 THEN 1.0 ELSE 0.0 END) AS v FROM c")
    ref = ui.load("SELECT (SELECT count(DISTINCT order_key) FROM dw.fact_refunds) * 1.0 / "
                  "nullif((SELECT count(*) FROM dw.fact_orders WHERE status <> 'cancelled'), 0) AS v")
    fresh = ui.load("SELECT max(order_ts) AS ts FROM dw.fact_orders")
    c = st.columns(5)
    c[0].metric("Total orders", f"{orders:,}")
    c[1].metric("Total sales", ui.fmt_money(sales))
    c[2].metric("Average order value", ui.fmt_money(sales / orders if orders else None))
    c[3].metric("Returning customers", ui.fmt_pct(None if ret is None else ret.loc[0, "v"]))
    c[4].metric("Refund rate (orders)", ui.fmt_pct(None if ref is None else ref.loc[0, "v"]))
    if fresh is not None:
        st.caption(f"Latest order timestamp in warehouse: {fresh.loc[0, 'ts']} (business time, UTC)")
    daily = ui.load("SELECT cast(order_ts AS DATE) AS day, sum(order_total) AS sales FROM dw.fact_orders "
                    "WHERE status <> 'cancelled' GROUP BY 1 ORDER BY 1")
    if daily is not None:
        st.plotly_chart(px.line(daily, x="day", y="sales", title="Daily sales"), use_container_width=True)

st.subheader("Pipeline")
runs = ui.load_meta("runs")
if runs is not None:
    done = runs[runs["status"].isin(["success", "failed"])]
    rate = (done["status"] == "success").mean() if len(done) else None
    latest = runs.iloc[0]
    a, b, c3 = st.columns(3)
    a.metric("Pipeline success rate", ui.fmt_pct(rate))
    b.metric("Latest run", f"{latest['status']}", help=str(latest["run_id"]))
    hrs = data.hours_since(latest["finished_at"] or latest["started_at"])
    c3.metric("Hours since latest run", "n/a" if hrs is None else f"{hrs:,.1f}")
