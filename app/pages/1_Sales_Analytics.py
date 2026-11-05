import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Sales Analytics")
weekly = ui.load("SELECT d.day_of_week, d.date_key, o.order_total FROM dw.fact_orders o "
                 "JOIN dw.dim_date d ON d.date_key = o.order_date_key WHERE o.status <> 'cancelled'")
if weekly is not None:
    by_day = weekly.groupby("day_of_week", as_index=False)["order_total"].sum()
    st.plotly_chart(px.bar(by_day, x="day_of_week", y="order_total", title="Sales by weekday (1 = Monday)"),
                    use_container_width=True)
monthly = ui.load("SELECT date_trunc('month', order_ts) AS month, count(*) AS orders, sum(order_total) AS sales "
                  "FROM dw.fact_orders WHERE status <> 'cancelled' GROUP BY 1 ORDER BY 1")
if monthly is not None:
    st.plotly_chart(px.bar(monthly, x="month", y="sales", title="Monthly sales"), use_container_width=True)
    st.dataframe(monthly, use_container_width=True)
status = ui.load("SELECT status, count(*) AS orders, sum(order_total) AS order_value FROM dw.fact_orders GROUP BY 1")
if status is not None:
    st.plotly_chart(px.pie(status, names="status", values="orders", title="Orders by status"),
                    use_container_width=True)
