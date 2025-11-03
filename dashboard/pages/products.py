# pages/Products.py
import os
from datetime import datetime, date, time
from typing import List, Optional

import pandas as pd
import sqlalchemy
import streamlit as st
import altair as alt
from dotenv import load_dotenv

from app_state import init_state_defaults, reset_filters_and_rerun, STATE_KEYS

st.set_page_config(page_title="Products", layout="wide")

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg://neondb_owner:npg_U3HIg8fSyNYG@ep-round-credit-acfujbd5-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
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
st.title("Products")
st.header("Catalog & Top selling products")

stores = load_store_list()
channels = load_channels_list()

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
    options=stores,
    default=st.session_state.get(STATE_KEYS["stores"], []),
    key=STATE_KEYS["stores"],
)

channels_choices = st.sidebar.multiselect(
    "Select the sales channels (multiple):",
    options=channels,
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
st.write(
    f"- Stores: **{', '.join(store_choices) if store_choices else 'All'}**")
st.write(
    f"- Channels: **{', '.join(channels_choices) if channels_choices else 'All'}**")


@st.cache_data(ttl=300)
def top_products(start: date, end: date, stores_ids=None, channels_ids=None, limit=50):
    params = {
        "start": datetime.combine(start, time(0, 0, 0)),
        "end": datetime.combine(end, time(23, 59, 59)),
        "limit": limit,
    }
    filters = []
    if stores_ids:
        filters.append("s.store_id = ANY(:stores_ids)")
        params["stores_ids"] = stores_ids
    if channels_ids:
        filters.append("s.channel_id = ANY(:channels_ids)")
        params["channels_ids"] = channels_ids

    filters_sql = ""
    if filters:
        filters_sql = " AND " + " AND ".join(filters)

    q = f"""
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            COUNT(ps.*) AS sold_qty,
            SUM(ps.total_price) AS revenue
        FROM product_sales ps
        JOIN products p ON p.id = ps.product_id
        JOIN sales s ON s.id = ps.sale_id
        WHERE s.created_at BETWEEN :start AND :end
        {filters_sql}
        GROUP BY p.id, p.name
        ORDER BY sold_qty DESC
        LIMIT :limit
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)
    if df.empty:
        return pd.DataFrame(columns=["product_id", "product_name", "sold_qty", "revenue"])
    df["sold_qty"] = df["sold_qty"].astype(int)
    df["revenue"] = df["revenue"].astype(float)
    return df


df_top = top_products(start_date, end_date, stores_ids=stores_ids,
                      channels_ids=channels_ids, limit=100)

st.subheader("Top products")
if df_top.empty:
    st.info("No products sold in the selected period/filters.")
else:
    df_top = df_top.rename(
        columns={
            "product_id": "Product id",
            "product_name": "Product",
            "sold_qty": "Quantity",
            "revenue": "Revenue ($)",
        }
    )
    st.dataframe(df_top)

st.divider()
st.subheader("Product details")


@st.cache_data(ttl=300)
def load_all_products():
    q = "SELECT id, name FROM products ORDER BY name"
    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn)
    return df


df_products_all = load_all_products()

if df_products_all.empty:
    st.info("No products found in database.")
else:
    product_options = df_products_all["name"].tolist()
    selected_products = st.multiselect(
        "Select one or more products to view detailed metrics",
        options=product_options,
        default=None,
        help="Pick products (multiple) to show daily sales and revenue for each"
    )

    if not selected_products:
        st.info("Select one or more products to see details.")
    else:
        selected_ids = [
            int(df_products_all.loc[df_products_all["name"]
                == name, "id"].values[0])
            for name in selected_products
        ]

        @st.cache_data(ttl=300)
        def products_details_multi(start: date, end: date, product_ids: List[int], stores_ids=None, channels_ids=None):
            params = {
                "start": datetime.combine(start, time(0, 0, 0)),
                "end": datetime.combine(end, time(23, 59, 59)),
                "product_ids": [int(x) for x in product_ids],
            }
            filters = ["ps.product_id = ANY(:product_ids)"]
            if stores_ids:
                filters.append("s.store_id = ANY(:stores_ids)")
                params["stores_ids"] = stores_ids
            if channels_ids:
                filters.append("s.channel_id = ANY(:channels_ids)")
                params["channels_ids"] = channels_ids

            filters_sql = " AND " + " AND ".join(filters)

            q = f"""
                SELECT
                    DATE(s.created_at) AS date,
                    p.id AS product_id,
                    p.name AS product_name,
                    SUM(ps.quantity) AS total_qty,
                    SUM(ps.total_price) AS revenue
                FROM product_sales ps
                JOIN products p ON p.id = ps.product_id
                JOIN sales s ON s.id = ps.sale_id
                WHERE s.created_at BETWEEN :start AND :end
                {filters_sql}
                GROUP BY DATE(s.created_at), p.id, p.name
                ORDER BY DATE(s.created_at), p.name
            """

            with engine.connect() as conn:
                df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)

            if df.empty:
                return pd.DataFrame(columns=["date", "product_id", "product_name", "total_qty", "revenue"])

            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["total_qty"] = df["total_qty"].astype(int)
            df["revenue"] = df["revenue"].astype(float)
            return df

        df_product_multi = products_details_multi(
            start_date, end_date, product_ids=selected_ids,
            stores_ids=stores_ids, channels_ids=channels_ids
        )

        if df_product_multi.empty:
            st.warning(
                "No sales found for the selected products in the chosen period/filters.")
        else:
            agg_by_product = df_product_multi.groupby("product_name", as_index=False).agg(
                total_qty=pd.NamedAgg(column="total_qty", aggfunc="sum"),
                total_revenue=pd.NamedAgg(column="revenue", aggfunc="sum"),
            ).sort_values("total_qty", ascending=False)

            total_qty_all = int(agg_by_product["total_qty"].sum())
            total_revenue_all = float(agg_by_product["total_revenue"].sum())

            c1, c2 = st.columns(2)
            c1.metric("Total quantity (selected products)",
                      f"{total_qty_all:,}")
            c2.metric("Total revenue (selected products)",
                      f"${total_revenue_all:,.2f}")

            st.markdown("**Breakdown by product**")
            st.dataframe(agg_by_product)

            st.subheader("Daily trend (selected products)")
            chart = (
                alt.Chart(df_product_multi)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("total_qty:Q", title="Quantity sold"),
                    color=alt.Color("product_name:N", title="Product"),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("product_name:N", title="Product"),
                        alt.Tooltip("total_qty:Q", title="Quantity sold"),
                        alt.Tooltip("revenue:Q", title="Revenue",
                                    format=",.2f"),
                    ],
                )
                .properties(height=450, width="container")
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)

            st.markdown("**Detailed daily table**")
            df_product_multi = df_product_multi.rename(
                columns={
                    "date": "Date",
                    "product_id": "Product id",
                    "product_name": "Product",
                    "total_qty": "Quantity",
                    "revenue": "Revenue ($)",
                }
            )
            st.dataframe(df_product_multi.sort_values(
                "Revenue ($)", ascending=False))
