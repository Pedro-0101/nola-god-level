import os
from datetime import datetime, date, time, timedelta
from typing import Tuple

import pandas as pd
import sqlalchemy
import altair as alt
import streamlit as st
from stqdm import stqdm
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://challenge:challenge_2024@localhost:5432/challenge_db"
)


# ---------- Helpers ----------
def day_range_for_date(d: date) -> tuple[str, str]:
    start = datetime.combine(d, time(0, 0, 0))
    end = datetime.combine(d, time(23, 59, 59))
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


@st.cache_resource(ttl=3600)  # tempo em segundos que o recurso fica válido no cache.
def get_engine(url: str = DB_URL):
    """
    Cria (e cacheia) o SQLAlchemy Engine:
            - pool_size: conexões persistentes no pool
            - max_overflow: conexões temporárias além do pool
    Retorna: sqlalchemy.Engine
    """
    return sqlalchemy.create_engine(url, pool_size=5, max_overflow=10)


engine = get_engine()


def run_query_to_df(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Executa uma query em modo somente leitura e retorna um DataFrame.
    Usa context manager para garantir fechamento da conexão/cursores.
    """
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(sql), params or {})
        df = pd.DataFrame(result.mappings().all())
        return df


def run_scalar(sql: str, params: dict = None):
    with engine.connect() as conn:
        res = conn.execute(sqlalchemy.text(sql), params or {})
        row = res.mappings().first()
    return row


# ---------- UI ----------

# Sidebar filters (defaults para hoje)
today = date.today()
start_ts_default, end_ts_default = day_range_for_date(today)

st.sidebar.header("Filters")
# Data pickers defaultam para hoje
start_date = st.sidebar.date_input("Start date", value=today)
end_date = st.sidebar.date_input("End date", value=today)

st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.title(f"📊 Sales Dashboard: From {start_date} to {end_date}")


# Carrega lojas
def load_store_list():
    q = "SELECT id, name FROM stores ORDER BY name"
    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn)
    if df.empty:
        return []
    # transforma em lista de strings "id - nome"
    return [f"{row['id']} - {row['name']}" for _, row in df.iterrows()]


# Carrega os canais
def load_channels_list():
    q = "SELECT id, name FROM channels ORDER BY name"
    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn)
    if df.empty:
        return []
    return [f"{row['id']} - {row['name']}" for _, row in df.iterrows()]


# Popular filtros
stores = load_store_list()
channels = load_channels_list()

# ---------- Checkbox de lojas ----------
store_choices = st.sidebar.multiselect(
    "Select the stores (multiple):",
    options=stores,
    default=None,  # por padrão, não seleciona nenhuma
)
store_ids = [int(s.split(" - ")[0]) for s in store_choices]

# ---------- Checkbox de channels ----------
channels_choices = st.sidebar.multiselect(
    "Select the sales channels (multiple):", options=channels, default=None
)
channels_ids = [int(c.split(" - ")[0]) for c in channels_choices]


@st.cache_data(ttl=300)
def sales_dashboard_data(
    start: datetime,
    end: datetime,
    stores_ids: list = None,
    channels_ids: list = None,
):
    """
    Retorna:
      - df_daily: linhas por date, channel_name, store_name, total (R$) e qtde (count por grupo)
      - total_sales: soma de value_paid no nível de sales
      - total_orders: contagem de pedidos (COUNT(*) em sales)
      - avg_ticket: total_sales / total_orders
    Observações:
      - total_orders é calculado diretamente na tabela sales, evitando duplicação por joins.
      - df_daily é útil para gráficos e drill-down.
    """
    params = {"start": start, "end": end}
    filters = []
    scalar_filters = []  # filtros para query scalar (sem joins)

    # montar filtros condicionais (para ambas queries)
    if stores_ids:
        filters.append("s.store_id = ANY(:store_ids)")
        scalar_filters.append("store_id = ANY(:store_ids)")
        params["store_ids"] = stores_ids
    if channels_ids:
        filters.append("s.channel_id = ANY(:channels_ids)")
        scalar_filters.append("channel_id = ANY(:channels_ids)")
        params["channels_ids"] = channels_ids

    filters_sql = ""
    if filters:
        filters_sql = " AND " + " AND ".join(filters)

    scalar_filters_sql = ""
    if scalar_filters:
        scalar_filters_sql = " AND " + " AND ".join(scalar_filters)

    # Query detalhada (por dia / canal / loja)
    q_daily = f"""
        SELECT
            DATE(s.created_at) AS date,
            c.name AS channel_name,
            st.name AS store_name,
            SUM(s.value_paid) AS total,
            COUNT(*) AS qtde  -- qtde por agrupamento (pode ser >1)
        FROM sales s
        INNER JOIN channels c ON c.id = s.channel_id
        INNER JOIN stores st ON st.id = s.store_id
        WHERE s.created_at BETWEEN :start AND :end
        {filters_sql}
        GROUP BY DATE(s.created_at), c.name, st.name
        ORDER BY date
    """

    # Query scalar no nível de pedidos (sem joins) — garante contagem correta
    q_scalar = f"""
        SELECT
            COALESCE(SUM(value_paid), 0) AS total_sales,
            COUNT(*) AS total_orders
        FROM sales
        WHERE created_at BETWEEN :start AND :end
        {scalar_filters_sql}
    """

    with engine.connect() as conn:
        df_daily = pd.read_sql_query(sqlalchemy.text(q_daily), conn, params=params)
        scalar = (
            conn.execute(
                sqlalchemy.text(q_scalar),
                {
                    "start": params["start"],
                    "end": params["end"],
                    **(
                        {
                            k: v
                            for k, v in params.items()
                            if k in ("store_ids", "channels_ids")
                        }
                    ),
                },
            )
            .mappings()
            .first()
        )

    total_sales = float(scalar["total_sales"] or 0)
    total_orders = int(scalar["total_orders"] or 0)
    avg_ticket = (total_sales / total_orders) if total_orders > 0 else 0.0

    # normalizações rápidas
    if not df_daily.empty:
        df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.date
        df_daily["channel_name"] = df_daily["channel_name"].astype(str)
        df_daily["store_name"] = df_daily["store_name"].astype(str)
        df_daily["total"] = df_daily["total"].astype(float)
        df_daily["qtde"] = df_daily["qtde"].astype(int)

    return df_daily, total_sales, total_orders, avg_ticket


df_daily, total_sales, total_orders, avg_ticket = sales_dashboard_data(
    start=start_date, end=end_date, stores_ids=store_ids, channels_ids=channels_ids
)

# --- Monta o DataFrame pivoteado ---
if not df_daily.empty:
    df_pivot = df_daily.pivot_table(
        index="date",
        columns="channel_name",
        values="total",
        aggfunc="sum",
        fill_value=0,
    )

    # Garante que as colunas são strings
    df_pivot.columns = df_pivot.columns.map(str)

    # Cria a coluna "Total" (soma horizontal)
    df_pivot["Total"] = df_pivot.sum(axis=1)

    # Garante que "Total" apareça por último
    cols = [c for c in df_pivot.columns if c != "Total"] + ["Total"]
    df_pivot = df_pivot[cols]

    # Converte para formato longo (necessário para o Altair)
    df_long = df_pivot.reset_index().melt(
        id_vars=["date"], var_name="channel", value_name="total"
    )

    # Define a ordem da legenda (mantendo "Total" por último)
    df_long["channel"] = pd.Categorical(
        df_long["channel"], categories=cols, ordered=True
    )

    # Cria o gráfico
    chart = (
        alt.Chart(df_long, title="Total sales per channel")
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total:Q", title="Total (currency)"),
            color=alt.Color("channel:N", title="Channel", sort=cols),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("channel:N", title="Channel"),
                alt.Tooltip("total:Q", title="Total"),
            ],
        )
        .properties(width="container", height=420)
        .interactive()
    )

    # Exibe o gráfico
    st.subheader("Total sales per channel")
    st.altair_chart(chart, width="stretch")

else:
    st.info("No sales in the selected period.")

# KPIS utilizando metrics do streamlit
c1, c2, c3 = st.columns(3)
# Total de vendas em $
c1.metric(label="Total sales", value=f"${total_sales:,.2f}", border=True)
# Contagem total de vendas
c2.metric(label="Number of Orders", value=total_orders, border=True)
# Ticket medio
c3.metric(label="Average Ticket", value=f"${avg_ticket:,.2f}", border=True)
