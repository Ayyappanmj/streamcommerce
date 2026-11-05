import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components import ui  # noqa: E402

ui.setup("Product Analytics")
# Revenue here is summed at line-item grain, so it is never multiplied by joins to orders.
products = ui.load("SELECT p.product_id, p.product_name, p.category, sum(i.quantity) AS units, "
                   "sum(i.line_total) AS revenue FROM dw.fact_order_items i "
                   "JOIN dw.dim_products p USING (product_key) "
                   "JOIN dw.fact_orders o USING (order_key) WHERE o.status <> 'cancelled' GROUP BY 1, 2, 3")
if products is not None:
    cat = products.groupby("category", as_index=False)["revenue"].sum()
    st.plotly_chart(px.bar(cat, x="category", y="revenue", title="Revenue by category"), use_container_width=True)
    top = products.sort_values("revenue", ascending=False).head(15)
    st.plotly_chart(px.bar(top, x="product_name", y="revenue", title="Top 15 products by revenue"),
                    use_container_width=True)
    st.dataframe(products.sort_values("revenue", ascending=False), use_container_width=True)
