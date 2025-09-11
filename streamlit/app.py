import os
import requests
import streamlit as st

API = os.getenv("API_URL", "http://api:8000")  # use service name 'api' from compose

st.set_page_config(page_title="MyApp Streamlit Client", layout="centered")
st.title("MyApp — Streamlit Client")
st.header("Welcome to MyApp!")

st.subheader("Db params")
if st.button("Get DB Params"):
    try:
        db_params = requests.get(f"{API}/dbparams", timeout=10).json()
        st.info(db_params)
    except Exception as e:
        st.error(f"Error: {e}")

# Make mock trades file
st.subheader("Trades Parquet")
try:
    path = requests.get(f"{API}/make_trades", timeout=120).json()
    st.success(path)
except Exception as e:
    st.error(f"Cannot reach API at {API}: {e}")

st.subheader("Recent Values")
if st.button("Refresh values"):
    try:
        values = requests.get(f"{API}/values?limit=20", timeout=10).json()
        st.table(values)
    except Exception as e:
        st.error(f"Error: {e}")

# Health check
st.subheader("API Health")
try:
    health = requests.get(f"{API}/health", timeout=5).json()
    st.success(health)
except Exception as e:
    st.error(f"Cannot reach API at {API}: {e}")

# Compute average
st.subheader("Compute Average")
if st.button("Get average"):
    try:
        avg = requests.get(f"{API}/compute", timeout=10).json()
        st.info(avg)
    except Exception as e:
        st.error(f"Error: {e}")

st.caption(f"API URL: {API}")