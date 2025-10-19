import os
import requests
import streamlit as st
import pandas as pd
import time
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# use service name 'api' from compose
API = os.getenv("API_URL", "http://api:8000/v1")

st.set_page_config(page_title="Trade Builder Dashboard", layout="wide")

def main():
    """Main function to run the Streamlit app."""
    st.title("Trade Builder Dashboard")

    if "load_in_progress" not in st.session_state:
        st.session_state.status_progress = "Click to start loading."
        st.session_state.load_in_progress = False
        st.session_state.loaded = False
        st.session_state.run_id = None
        st.session_state.run_type = "trades"

    # Create two columns for left and right side
    left_col, right_col = st.columns([1, 3])

    # Left column: input lists for Date, Server, and Risk Site
    with left_col:
        # Date picker for selecting the date
        date = st.date_input("Select Date", value=pd.to_datetime("2025-10-17"))

        st.subheader("Trade Builder Parameters")
        rows = st.number_input("Number of Trades", min_value=1000, max_value=50_000_000, value=100_000, step=10_000)
        chunksize = st.number_input("Chunk Size", min_value=10_000, max_value=1_000_000, value=100_000, step=10_000)

        # Create a placeholder for status updates
        status_label = st.text(st.session_state.status_progress)

        if st.button("Make Trades"):
            if not st.session_state.load_in_progress:
                st.session_state.loaded = False
                st.session_state.load_in_progress = True
                st.session_state.status_progress = ""
                status_label.text(st.session_state.status_progress)
                st.session_state.run_type = "trades"

                payload = {
                    "cob_dt": str(date),
                    "rows": int(rows), 
                    "chunk_size": int(chunksize)
                }

                start_time = time.strftime('%X')
                with st.spinner(f'Triggering trade build at {start_time} ...'):
                    # Make trades file
                    try:
                        res = requests.post(f"{API}/maketrades", json=payload)
                        if res.status_code in (200, 202):
                            st.session_state.run_id = res.json()["run_id"]
                            st.success(f"Started run {st.session_state.run_id}")
                        else:
                            st.error(f"Error: {res.status_code} - {res.text}")
                    except Exception as e:
                        st.error(f"Request failed: {e}")
        
        st.subheader("Cpty Builder Parameters")
        cptyrows = st.number_input("Number of Cpties", min_value=1, max_value=100_000, value=100_000, step=10_000)
        cptychunksize = st.number_input("Chunk Size", min_value=10_000, max_value=100_000, value=10_000, step=10_000)

        if st.button("Make Cpties"):
            if not st.session_state.load_in_progress:
                st.session_state.loaded = False
                st.session_state.load_in_progress = True
                st.session_state.status_progress = ""
                status_label.text(st.session_state.status_progress)
                st.session_state.run_type = "cpty"

                cptypayload = {
                    "cob_dt": str(date),
                    "rows": int(cptyrows), 
                    "chunk_size": int(cptychunksize)
                }

                start_time = time.strftime('%X')
                with st.spinner(f'Triggering cpty build at {start_time} ...'):
                    # Make cpty file
                    try:
                        res = requests.post(f"{API}/makecpties", json=cptypayload)
                        if res.status_code in (200, 202):
                            st.session_state.run_id = res.json()["run_id"]
                            st.success(f"Started run {st.session_state.run_id}")
                        else:
                            st.error(f"Error: {res.status_code} - {res.text}")
                    except Exception as e:
                        st.error(f"Request failed: {e}")


    with right_col:
        run_id = st.session_state.run_id
        if run_id:
            st.subheader(f"Run Status: {run_id}")

            # Auto-refresh every 2 seconds
            st_autorefresh(interval=2000, key="refresh")

            try:
                if st.session_state.run_type == "trades":
                    endpoint = f"{API}/runs/trades/{str(date)}/{run_id}"
                else:
                    endpoint = f"{API}/runs/cpties/{str(date)}/{run_id}"
                
                resp = requests.get(endpoint, timeout=10)
                if resp.status_code == 404:
                    st.warning(f"Run not found yet... ({endpoint})")
                else:
                    data = resp.json()
                    st.json(data)

                    written = data.get("written", 0)
                    rows_total = data.get("rows", 1)
                    pct = min(int(100 * written / rows_total), 100)
                    st.progress(pct)

                    status = data.get("status", "?")
                    if status == "succeeded":
                        st.success(f"Run completed in {data.get('duration_s', '?')}s")
                        st.session_state.status = "succeeded"
                        st.session_state.status_progress = "Click to start loading."
                        st.session_state.load_in_progress = False
                        st.session_state.loaded = False
                    elif status == "failed":
                        st.error("Run failed")
                        st.session_state.status = "failed"
            except Exception as e:
                st.error(f"Error fetching status: {e}")

st.caption(f"API URL: {API}")

if __name__ == '__main__':
    main()