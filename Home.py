import streamlit as st
from app_auth import render_auth_gate
import api_client

st.set_page_config(page_title="LGD Fuzzy Matcher", page_icon="🗺️", layout="wide")

st.markdown("""
<style>
/* Same CSS as before */
.stButton button { width: 100%; border-radius: 6px; }
.section-card { background: #f8f9fa; border-radius: 8px; padding: 1.5rem; margin-bottom: 1rem; border: 1px solid #e9ecef; }
.small-muted { font-size: 0.85em; color: #6c757d; }
</style>
""", unsafe_allow_html=True)

# Auth gate
render_auth_gate()

st.title("🗺️ LGD Fuzzy Matcher")
st.markdown("Welcome to the Local Government Directory (LGD) Matcher. Please select a tool from the sidebar.")

try:
    stats = api_client.get_stats()
    st.success(f"Connected to API. Loaded {stats['states']} states and {stats['districts']} districts.")
except Exception as e:
    st.error(f"Cannot connect to API Backend at {api_client.API_BASE_URL}. Ensure it is running: `uvicorn api:app`")
    st.code(str(e))

with st.sidebar:
    st.caption(f"Signed in as: **{st.session_state.get('auth_user', 'unknown')}**")
    if st.button("Sign out"):
        st.session_state["auth_ok"] = False
        st.session_state["auth_user"] = ""
        st.query_params.clear()
        st.rerun()
