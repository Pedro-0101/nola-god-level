# pages/Stores.py
import os
from datetime import datetime, date, time
from typing import List, Optional

import pandas as pd
import sqlalchemy
import altair as alt
import streamlit as st
from dotenv import load_dotenv

from app_state import init_state_defaults, reset_filters_and_rerun, STATE_KEYS

st.set_page_config(page_title="Stores", layout="wide")

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg://challenge:challenge_2024@localhost:5432/challenge_db"
)

@st.cache_resource(ttl=3600)
def get_engine(url: str = DB_URL):
    return sqlalchemy.create_engine(url, pool_size=5, max_overflow=10)


engine = get_engine()

def load_store_list() -> List[str]:
    q = "SELECT id, name FROM stores ORDER BY name"
    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn)
    if df.empty:
        return []
    return [f"{row['id']} - {row['name']}" for _, row in df.iterrows()]


def load_channels_list() -> List[str]:
    q = "SELECT id, name FROM channels ORDER BY name"
    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn)
    if df.empty:
        return []
    return [f"{row['id']} - {row['name']}" for _, row in df.iterrows()]

init_state_defaults()
st.title("Stores")
st.header("Stores overview & details")

stores_options = load_store_list()
channels_options = load_channels_list()

st.sidebar.header("Filters")
st.sidebar.button("Clear filters", on_click=reset_filters_and_rerun)

today = date.today()
start_date = st.sidebar.date_input(
    "Start date",
    value=st.session_state.get(STATE_KEYS["start_date"], today),
    key=STATE_KEYS["start_date"],
)
end_date = st.sidebar.date_input(
    "End date",
    value=st.session_state.get(STATE_KEYS["end_date"], today),
    key=STATE_KEYS["end_date"],
)

store_choices = st.sidebar.multiselect(
    "Select the stores (multiple):",
    options=stores_options,
    default=st.session_state.get(STATE_KEYS["stores"], []),
    key=STATE_KEYS["stores"],
)

channels_choices = st.sidebar.multiselect(
    "Select the sales channels (multiple):",
    options=channels_options,
    default=st.session_state.get(STATE_KEYS["channels"], []),
    key=STATE_KEYS["channels"],
)


def choices_to_ids(choices: List[str]) -> Optional[List[int]]:
    if not choices:
        return None
    return [int(s.split(" - ")[0]) for s in choices]


stores_ids = choices_to_ids(store_choices)
channels_ids = choices_to_ids(channels_choices)

st.markdown("**Active filters:**")
st.write(f"- Date: **{start_date}** → **{end_date}**")
st.write(f"- Stores: **{', '.join(store_choices) if store_choices else 'All'}**")
st.write(f"- Channels: **{', '.join(channels_choices) if channels_choices else 'All'}**")


@st.cache_data(ttl=300)
def stores_overview(start: date, end: date, stores_ids: Optional[List[int]] = None, channels_ids: Optional[List[int]] = None):
    params = {
        "start": datetime.combine(start, time(0, 0, 0)),
        "end": datetime.combine(end, time(23, 59, 59)),
    }
    left_join_channel_filter = ""
    where_clause = ""
    if channels_ids:
        left_join_channel_filter = " AND s.channel_id = ANY(:channels_ids)"
        params["channels_ids"] = channels_ids
    if stores_ids:
        where_clause = "WHERE st.id = ANY(:stores_ids)"
        params["stores_ids"] = stores_ids

    q = f"""
        SELECT
            st.id AS store_id,
            st.name AS store_name,
            COALESCE(SUM(s.total_amount), 0) AS total_sales,
            COUNT(s.*) AS total_orders,
            COALESCE(AVG(NULLIF(s.total_amount,0)), 0) AS avg_ticket
        FROM stores st
        LEFT JOIN sales s
            ON s.store_id = st.id
            AND s.created_at BETWEEN :start AND :end
            {left_join_channel_filter}
        {where_clause}
        GROUP BY st.id, st.name
        ORDER BY total_sales DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=["store_id", "store_name", "total_sales", "total_orders", "avg_ticket"])

    df["total_sales"] = df["total_sales"].astype(float)
    df["total_orders"] = df["total_orders"].astype(int)
    df["avg_ticket"] = df["avg_ticket"].astype(float)
    return df


@st.cache_data(ttl=300)
def store_daily_sales(start: date, end: date, store_ids: Optional[List[int]] = None, channels_ids: Optional[List[int]] = None):
    params = {
        "start": datetime.combine(start, time(0, 0, 0)),
        "end": datetime.combine(end, time(23, 59, 59)),
    }
    filters = []
    if store_ids:
        params["store_ids"] = store_ids
        filters.append("s.store_id = ANY(:store_ids)")
    if channels_ids:
        params["channels_ids"] = channels_ids
        filters.append("s.channel_id = ANY(:channels_ids)")

    filters_sql = (" AND " + " AND ".join(filters)) if filters else ""

    q = f"""
        SELECT
            DATE(s.created_at) AS date,
            st.id AS store_id,
            st.name AS store_name,
            SUM(s.total_amount) AS total_sales,
            COUNT(*) AS orders
        FROM sales s
        JOIN stores st ON st.id = s.store_id
        WHERE s.created_at BETWEEN :start AND :end
        {filters_sql}
        GROUP BY DATE(s.created_at), st.id, st.name
        ORDER BY DATE(s.created_at)
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=["date", "store_id", "store_name", "total_sales", "orders"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["total_sales"] = df["total_sales"].astype(float)
    df["orders"] = df["orders"].astype(int)
    return df


@st.cache_data(ttl=300)
def top_products_by_store(start: date, end: date, store_ids: Optional[List[int]] = None, channels_ids: Optional[List[int]] = None, limit: int = 10):
    params = {
        "start": datetime.combine(start, time(0, 0, 0)),
        "end": datetime.combine(end, time(23, 59, 59)),
        "limit": limit,
    }
    filters = []
    if store_ids:
        filters.append("s.store_id = ANY(:store_ids)")
        params["store_ids"] = store_ids
    if channels_ids:
        filters.append("s.channel_id = ANY(:channels_ids)")
        params["channels_ids"] = channels_ids

    filters_sql = (" AND " + " AND ".join(filters)) if filters else ""

    q = f"""
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            SUM(ps.quantity) AS total_qty,
            SUM(ps.total_price) AS revenue
        FROM product_sales ps
        JOIN products p ON p.id = ps.product_id
        JOIN sales s ON s.id = ps.sale_id
        WHERE s.created_at BETWEEN :start AND :end
        {filters_sql}
        GROUP BY p.id, p.name
        ORDER BY total_qty DESC
        LIMIT :limit
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=["product_id", "product_name", "total_qty", "revenue"])
    df["total_qty"] = df["total_qty"].astype(int)
    df["revenue"] = df["revenue"].astype(float)
    return df


df_stores = stores_overview(start_date, end_date, stores_ids=stores_ids, channels_ids=channels_ids)

st.subheader("Stores overview")
if df_stores.empty:
    st.info("No stores / sales found for selected filters.")
else:
    df_display = df_stores.rename(columns={
        "store_id": "Store id",
        "store_name": "Store",
        "total_sales": "Total sales ($)",
        "total_orders": "Orders",
        "avg_ticket": "Avg ticket ($)"
    })
    df_display["Total sales ($)"] = pd.to_numeric(df_display["Total sales ($)"], errors="coerce").fillna(0.0)
    df_display["Orders"] = pd.to_numeric(df_display["Orders"], errors="coerce").fillna(0).astype(int)
    df_display = df_display.sort_values("Total sales ($)", ascending=False)

    st.dataframe(df_display)

    top_n_stores = st.slider("Top N stores to show in chart", min_value=1, max_value=50, value=10, step=1)
    top_stores = df_display.head(top_n_stores)

    chart = (
        alt.Chart(top_stores)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X("Total sales ($):Q", title="Total sales ($)"),
            y=alt.Y("Store:N", sort="-x", title="Store"),
            tooltip=[
                alt.Tooltip("Store:N", title="Store"),
                alt.Tooltip("Total sales ($):Q", title="Total sales", format=",.2f"),
                alt.Tooltip("Orders:Q", title="Orders"),
                alt.Tooltip("Avg ticket ($):Q", title="Avg ticket", format=",.2f"),
            ],
        )
        .properties(height=450)
    )
    st.altair_chart(chart)

st.divider()
st.subheader("Store details")

store_names = df_stores["store_name"].tolist() if not df_stores.empty else []
selected_for_detail = st.multiselect("Select store(s) to inspect", options=store_names, default=None)

if not selected_for_detail:
    st.info("Select one or more stores above to view daily trends and top products.")
else:
    selected_ids = df_stores[df_stores["store_name"].isin(selected_for_detail)]["store_id"].tolist()

    df_daily = store_daily_sales(start_date, end_date, store_ids=selected_ids, channels_ids=channels_ids)

    if df_daily.empty:
        st.warning("No daily sales for selected stores / filters.")
    else:
        st.subheader("Daily sales (selected stores)")
        df_pivot = df_daily.pivot_table(index="date", columns="store_name", values="total_sales", aggfunc="sum", fill_value=0).reset_index().melt(id_vars=["date"], var_name="Store", value_name="Total sales ($)")
        chart_daily = (
            alt.Chart(df_pivot)
            .mark_line(point=True)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("Total sales ($):Q", title="Total sales ($)"),
                color=alt.Color("Store:N", title="Store"),
                tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("Store:N", title="Store"), alt.Tooltip("Total sales ($):Q", title="Total sales", format=",.2f")],
            )
            .properties(height=450)
            .interactive()
        )
        st.altair_chart(chart_daily)

    st.subheader("Top products in selected store(s)")
    top_products_df = top_products_by_store(start_date, end_date, store_ids=selected_ids, channels_ids=channels_ids, limit=20)
    if top_products_df.empty:
        st.info("No products sold in the selected stores/filters.")
    else:
        top_products_df = top_products_df.rename(columns={"product_id": "Product id", "product_name": "Product", "total_qty": "Quantity", "revenue": "Revenue ($)"})
        st.dataframe(top_products_df.sort_values("Quantity", ascending=False))

        chart_prod = (
            alt.Chart(top_products_df.head(10))
            .mark_bar()
            .encode(
                x=alt.X("Quantity:Q", title="Quantity sold"),
                y=alt.Y("Product:N", sort="-x", title="Product"),
                tooltip=[alt.Tooltip("Product:N", title="Product"), alt.Tooltip("Quantity:Q", title="Quantity"), alt.Tooltip("Revenue ($):Q", title="Revenue", format=",.2f")],
            )
            .properties(height=420)
        )
        st.altair_chart(chart_prod)
