import streamlit as st
import pandas as pd
from app_auth import render_auth_gate
import api_client
from app_components import to_csv_bytes

st.set_page_config(page_title="LGD Directory", page_icon="DIR", layout="wide")

render_auth_gate()

st.title("🗂️ Browse LGD Directory")
st.caption("Traverse the official LGD hierarchy: State ➔ District ➔ Sub-district ➔ Village")

st.markdown("""
<style>
.block-container { max-width: 100%; padding-left: 2rem; padding-right: 2rem; }
.metric-box {
    border: 1px solid #e0e0e0;
    padding: 18px 12px;
    border-radius: 8px;
    text-align: center;
    background-color: #f8f9fa;
    margin: 8px 0 10px 0;
}
.metric-value { font-size: 28px; font-weight: bold; color: #1f77b4; line-height: 1.1; }
.metric-label { font-size: 12px; color: #666; text-transform: uppercase; margin-top: 8px; }
</style>
""", unsafe_allow_html=True)


def code_from_label(label: str) -> str:
    return label.split("(")[-1].strip(")")


def metric_box(value: int, label: str) -> None:
    st.markdown(
        f'<div class="metric-box"><div class="metric-value">{value}</div>'
        f'<div class="metric-label">{label}</div></div>',
        unsafe_allow_html=True,
    )


def disabled_download(label: str) -> None:
    st.button(label, disabled=True, width="stretch")


def empty_table_message(message: str) -> None:
    st.info(message)

try:
    states = api_client.list_states()
    state_names = [f"{s['state_name']} ({s['state_lgd_code']})" for s in states]
except Exception as e:
    st.error("Could not fetch states from API.")
    st.stop()

selected_state = st.selectbox("Select State", ["-- Select a State --"] + state_names)

if selected_state != "-- Select a State --":
    state_code = code_from_label(selected_state)
    
    col1, col2, col3 = st.columns(3, gap="large")
    
    with col1:
        st.markdown("### Districts")
        try:
            districts = api_client.list_districts(state_code)
            dist_names = [f"{d['district_name']} ({d['district_lgd_code']})" for d in districts]
            selected_dist = st.selectbox("Select District", ["-- Select a District --"] + dist_names, width="stretch")
            metric_box(len(districts), "Total Districts")
            if districts:
                district_df = pd.DataFrame(districts).rename(columns={
                    "district_lgd_code": "LGD Code",
                    "district_name": "District Name",
                })
                st.download_button(
                    "Download Districts CSV",
                    to_csv_bytes(district_df),
                    f"districts_{state_code}.csv",
                    "text/csv",
                    width="stretch",
                )
                st.dataframe(
                    district_df,
                    width="stretch",
                    height=520,
                    hide_index=True,
                    column_config={
                        "LGD Code": st.column_config.TextColumn(width="small"),
                        "District Name": st.column_config.TextColumn(width="large"),
                    },
                )
        except Exception:
            st.warning("Could not fetch districts.")
            selected_dist = "-- Select a District --"
            
    with col2:
        st.markdown("### Sub-districts")
        if selected_dist != "-- Select a District --":
            dist_code = code_from_label(selected_dist)
            try:
                subdists = api_client.list_subdistricts(dist_code)
                sub_names = [f"{s['subdistrict_name']} ({s['subdistrict_lgd_code']})" for s in subdists]
                selected_sub = st.selectbox("Select Sub-district", ["-- Select a Sub-district --"] + sub_names, width="stretch")
                metric_box(len(subdists), "Total Sub-districts")
                if subdists:
                    subdistrict_df = pd.DataFrame(subdists).rename(columns={
                        "subdistrict_lgd_code": "LGD Code",
                        "subdistrict_name": "Sub-district Name",
                    })
                    st.download_button(
                        "Download Sub-districts CSV",
                        to_csv_bytes(subdistrict_df),
                        f"subdistricts_{dist_code}.csv",
                        "text/csv",
                        width="stretch",
                    )
                    st.dataframe(
                        subdistrict_df,
                        width="stretch",
                        height=520,
                        hide_index=True,
                        column_config={
                            "LGD Code": st.column_config.TextColumn(width="small"),
                            "Sub-district Name": st.column_config.TextColumn(width="large"),
                        },
                    )
                else:
                    st.info("No sub-districts found for this district.")
            except Exception:
                st.warning("Could not fetch sub-districts.")
                selected_sub = "-- Select a Sub-district --"
        else:
            st.selectbox(
                "Select Sub-district",
                ["-- Select a District First --"],
                disabled=True,
                width="stretch",
            )
            metric_box(0, "Total Sub-districts")
            disabled_download("Download Sub-districts CSV")
            empty_table_message("Select a district to view sub-districts.")
            selected_sub = "-- Select a Sub-district --"
            
    with col3:
        st.markdown("### Villages")
        st.text_input(
            "Selected Sub-district",
            selected_sub if selected_sub != "-- Select a Sub-district --" else "",
            disabled=True,
            width="stretch",
        )
        if selected_sub != "-- Select a Sub-district --":
            sub_code = code_from_label(selected_sub)
            try:
                villages = api_client.list_villages(sub_code)
                if villages:
                    metric_box(len(villages), "Total Villages")
                    village_df = pd.DataFrame(villages).rename(columns={
                        "village_lgd_code": "LGD Code",
                        "village_name": "Village Name",
                    })
                    st.download_button(
                        "Download Villages CSV",
                        to_csv_bytes(village_df),
                        f"villages_{sub_code}.csv",
                        "text/csv",
                        width="stretch",
                    )
                    st.dataframe(
                        village_df,
                        width="stretch",
                        height=520,
                        hide_index=True,
                        column_config={
                            "LGD Code": st.column_config.TextColumn(width="small"),
                            "Village Name": st.column_config.TextColumn(width="large"),
                        },
                    )
                else:
                    st.info("No villages found for this sub-district.")
            except Exception:
                st.warning("Could not fetch villages.")
        else:
            metric_box(0, "Total Villages")
            disabled_download("Download Villages CSV")
            empty_table_message("Select a sub-district to view villages.")
