import streamlit as st
import pandas as pd
from app_auth import render_auth_gate
import api_client

render_auth_gate()

st.title("🗂️ Browse LGD Directory")
st.caption("Traverse the official LGD hierarchy: State ➔ District ➔ Sub-district ➔ Village")

st.markdown("""
<style>
.metric-box { border: 1px solid #e0e0e0; padding: 15px; border-radius: 8px; text-align: center; background-color: #f8f9fa; }
.metric-value { font-size: 24px; font-weight: bold; color: #1f77b4; }
.metric-label { font-size: 12px; color: #666; text-transform: uppercase; }
</style>
""", unsafe_allow_html=True)

try:
    states = api_client.list_states()
    state_names = [f"{s['state_name']} ({s['state_lgd_code']})" for s in states]
except Exception as e:
    st.error("Could not fetch states from API.")
    st.stop()

selected_state = st.selectbox("Select State", ["-- Select a State --"] + state_names)

if selected_state != "-- Select a State --":
    state_code = selected_state.split("(")[-1].strip(")")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### Districts")
        try:
            districts = api_client.list_districts(state_code)
            dist_names = [f"{d['district_name']} ({d['district_lgd_code']})" for d in districts]
            selected_dist = st.selectbox("Select District", ["-- Select a District --"] + dist_names)
            st.markdown(f'<div class="metric-box"><div class="metric-value">{len(districts)}</div><div class="metric-label">Total Districts</div></div>', unsafe_allow_html=True)
        except Exception:
            st.warning("Could not fetch districts.")
            selected_dist = "-- Select a District --"
            
    with col2:
        st.markdown("### Sub-districts")
        if selected_dist != "-- Select a District --":
            dist_code = selected_dist.split("(")[-1].strip(")")
            try:
                subdists = api_client.list_subdistricts(dist_code)
                sub_names = [f"{s['subdistrict_name']} ({s['subdistrict_lgd_code']})" for s in subdists]
                selected_sub = st.selectbox("Select Sub-district", ["-- Select a Sub-district --"] + sub_names)
                st.markdown(f'<div class="metric-box"><div class="metric-value">{len(subdists)}</div><div class="metric-label">Total Sub-districts</div></div>', unsafe_allow_html=True)
            except Exception:
                st.warning("Could not fetch sub-districts.")
                selected_sub = "-- Select a Sub-district --"
        else:
            st.info("Select a district to view sub-districts.")
            selected_sub = "-- Select a Sub-district --"
            
    with col3:
        st.markdown("### Villages")
        if selected_sub != "-- Select a Sub-district --":
            sub_code = selected_sub.split("(")[-1].strip(")")
            try:
                villages = api_client.list_villages(sub_code)
                if villages:
                    st.markdown(f'<div class="metric-box"><div class="metric-value">{len(villages)}</div><div class="metric-label">Total Villages</div></div>', unsafe_allow_html=True)
                    st.dataframe(pd.DataFrame(villages).rename(columns={"village_name": "Village Name", "village_lgd_code": "LGD Code"}), use_container_width=True, height=400)
                else:
                    st.info("No villages found for this sub-district.")
            except Exception:
                st.warning("Could not fetch villages.")
        else:
            st.info("Select a sub-district to view villages.")
