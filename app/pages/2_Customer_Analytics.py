import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Customer Analytics")
per_customer = ui.load("SELECT c.customer_id, c.country, c.segment, count(*) AS orders, sum(o.order_total) AS spent "
                       "FROM dw.fact_orders o JOIN dw.dim_customers c USING (customer_key) "
                       "WHERE o.status <> 'cancelled' GROUP BY 1, 2, 3")
if per_customer is not None:
    st.plotly_chart(px.histogram(per_customer, x="orders", title="Orders per customer"), use_container_width=True)
    country = per_customer.groupby("country", as_index=False)["spent"].sum()
    st.plotly_chart(px.bar(country, x="country", y="spent", title="Spend by country"), use_container_width=True)
    st.subheader("Top customers by spend")
    st.dataframe(per_customer.sort_values("spent", ascending=False).head(20), use_container_width=True)
    repeat = (per_customer["orders"] >= 2).mean()
    st.metric("Repeat purchase rate", ui.fmt_pct(repeat))
