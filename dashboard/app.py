import os
from datetime import timedelta
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


@st.cache_resource(ttl=3600)
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


st.title("Dashboard simples")
start_date = st.date_input("Data início")
end_date = st.date_input("Data fim")

if st.button("Carregar"):
    sql = """
    SELECT date_trunc('day', created_at) as dia, SUM(total_amount) as faturamento
    FROM sales
    WHERE created_at BETWEEN :start AND :end
    GROUP BY 1
    ORDER BY 1
    """
    df = run_query_to_df(
        sql, {"start": f"{start_date} 00:00:00", "end": f"{end_date} 23:59:59"}
    )
    st.dataframe(df)
