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

st.sidebar.header("Filtros")
# Data pickers defaultam para hoje
start_date = st.sidebar.date_input("Data início", value=today)
end_date = st.sidebar.date_input("Data fim", value=today)

st.set_page_config(
    page_title="Sales Dashboard", layout="wide"
)

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


stores = load_store_list()

# ---------- Checkbox de lojas ----------
store_choices = st.sidebar.multiselect(
    "Selecione as lojas (múltiplas):",
    options=stores,
    default=None,  # por padrão, não seleciona nenhuma
)
store_ids = [int(s.split(" - ")[0]) for s in store_choices]

# ---------- Checkbox de channels ----------



@st.cache_data(ttl=500)
def salesByDate(
    start: datetime = start_date, end: datetime = end_date, stores_ids: list = store_ids
):
    params = {"start": start, "end": end, "store_ids": stores_ids}
    q = """
        SELECT 
            DATE(created_at) AS date, 
            SUM(value_paid) AS total
        FROM sales
        WHERE created_at BETWEEN :start AND :end
        AND store_id = ANY(:store_ids)
        GROUP BY DATE(created_at)
        ORDER BY date
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(sqlalchemy.text(q), conn, params=params)
    return df


sales_line = salesByDate(start_date, end_date, store_ids)

# Mostra o gráfico de linha (datas no eixo X, valores no Y)
if not sales_line.empty:
    st.line_chart(data=sales_line, x="date", y="total", use_container_width=True)
else:
    st.info("No sales in the selected period.")
