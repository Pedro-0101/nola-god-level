from datetime import date
import streamlit as st

STATE_KEYS = {
    "start_date": "start_date",
    "end_date": "end_date",
    "stores": "stores_selected",
    "channels": "channels_selected",
}

def init_state_defaults():
    """Inicializa chaves no session_state apenas se não existirem."""
    today = date.today()
    if STATE_KEYS["start_date"] not in st.session_state:
        st.session_state[STATE_KEYS["start_date"]] = today
    if STATE_KEYS["end_date"] not in st.session_state:
        st.session_state[STATE_KEYS["end_date"]] = today
    if STATE_KEYS["stores"] not in st.session_state:
        st.session_state[STATE_KEYS["stores"]] = []
    if STATE_KEYS["channels"] not in st.session_state:
        st.session_state[STATE_KEYS["channels"]] = []

def reset_filters_and_rerun():
    """Callback para limpar filtros e recarregar a página."""
    today = date.today()
    st.session_state[STATE_KEYS["start_date"]] = today
    st.session_state[STATE_KEYS["end_date"]] = today
    st.session_state[STATE_KEYS["stores"]] = []
    st.session_state[STATE_KEYS["channels"]] = []