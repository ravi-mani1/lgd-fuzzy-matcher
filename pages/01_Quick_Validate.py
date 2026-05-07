import streamlit as st
import pandas as pd
from app_auth import render_auth_gate
import api_client
from app_components import build_rows, row_style, suggestion_row_style, district_prefix_list_in_state

render_auth_gate()

st.title("⚡ Quick Validate")
st.caption("Fill any 1–4 fields. You can enter multiple values separated by commas.")
st.info("Supported combinations: state/district name, state/district LGD code, or any mix.")

st.markdown(
    "<span style='background:#dcfce7;padding:2px 8px;border-radius:6px;'>EXACT</span> "
    "<span style='background:#dbeafe;padding:2px 8px;border-radius:6px;'>HIGH</span> "
    "<span style='background:#fef3c7;padding:2px 8px;border-radius:6px;'>MEDIUM</span> "
    "<span style='background:#fee2e2;padding:2px 8px;border-radius:6px;'>LOW</span> "
    "<span style='background:#f3f4f6;padding:2px 8px;border-radius:6px;'>NOT_FOUND</span>",
    unsafe_allow_html=True,
)

c1, c2 = st.columns(2)
with c1:
    q_state_name = st.text_input("State name (optional)", placeholder="e.g. UP, Delhi")
    q_dist_name = st.text_input("District name (optional)", placeholder="e.g. varansi")
    q_subdist_name = st.text_input("Sub-district/Block name (optional)", placeholder="e.g. pindra")
with c2:
    q_state_lgd = st.text_input("State LGD code (optional)", placeholder="e.g. 9")
    q_dist_lgd = st.text_input("District LGD code (optional)", placeholder="e.g. 187")
    
    is_village_disabled = not bool(q_subdist_name.strip())
    q_village_name = st.text_input(
        "Village name (optional)", 
        placeholder="e.g. bhagwanpur", 
        disabled=is_village_disabled,
        help="Enter a Sub-district name first to search for a village." if is_village_disabled else ""
    )

run_quick = st.button("Validate", type="primary")
if run_quick:
    rows = build_rows(q_state_name, q_state_lgd, q_dist_name, q_dist_lgd, q_subdist_name, q_village_name)
    
    # We will build a DataFrame and send to API
    df_rows = []
    lookup_warnings = []
    for r in rows:
        state_name = r["state_name_in"] or ""
        state_lgd = r["state_lgd_in"] or ""
        district_name = r["district_name_in"] or ""
        district_lgd = r["district_lgd_in"] or ""

        if state_lgd:
            state_hit = api_client.lookup_state(state_lgd)
            if state_hit:
                state_name = state_name or state_hit["state_name"]
                state_lgd = state_hit["state_lgd_code"]
            else:
                lookup_warnings.append(f"State LGD code {state_lgd} was not found.")

        if district_lgd:
            district_hit = api_client.lookup_district(district_lgd, state_lgd)
            if district_hit:
                district_name = district_name or district_hit["district_name"]
                state_lgd = state_lgd or district_hit["state_lgd_code"]
                if not state_name and state_lgd:
                    state_hit = api_client.lookup_state(state_lgd)
                    if state_hit:
                        state_name = state_hit["state_name"]
            else:
                lookup_warnings.append(f"District LGD code {district_lgd} was not found.")

        row_data = {
            "id": r["id"],
            "state_name_raw": state_name,
            "district_name_raw": district_name,
            "subdistrict_name_raw": r["subdist_name_in"] or "",
            "village_name_raw": r["village_name_in"] or ""
        }
            
        df_rows.append(row_data)

    if not df_rows:
        st.warning("No input provided.")
    else:
        for msg in lookup_warnings:
            st.warning(msg)
        df = pd.DataFrame(df_rows)
        with st.spinner("Matching via API..."):
            try:
                results_df = api_client.match_dataframe(df)
                outputs = results_df.to_dict(orient="records")
                
                if len(outputs) == 1:
                    res = outputs[0]
                    st.divider()
                    st.markdown("### Match Result")
                    c3, c4, c5 = st.columns(3)
                    c3.metric("Match Score", f"{res.get('match_confidence_score', 0.0)}%")
                    c4.metric("Status", res.get("match_status", "NOT_FOUND"))
                    with c5:
                        pass

                    cols = st.columns(2)
                    with cols[0]:
                        st.markdown("**Input**")
                        st.write(f"- State: `{res.get('state_name_raw') or '-'}`")
                        st.write(f"- District: `{res.get('district_name_raw') or '-'}`")
                        st.write(f"- Sub-dist: `{res.get('subdistrict_name_raw') or '-'}`")
                        st.write(f"- Village: `{res.get('village_name_raw') or '-'}`")
                    with cols[1]:
                        st.markdown("**Matched to LGD**")
                        st.write(f"- State: `{res.get('state_name_corrected') or '-'}` (LGD: {res.get('state_lgd_code') or '-'})")
                        st.write(f"- District: `{res.get('district_name_corrected') or '-'}` (LGD: {res.get('district_lgd_code') or '-'})")
                        st.write(f"- Sub-dist: `{res.get('subdistrict_name_corrected') or '-'}` (LGD: {res.get('subdistrict_lgd_code') or '-'})")
                        st.write(f"- Village: `{res.get('village_name_corrected') or '-'}` (LGD: {res.get('village_lgd_code') or '-'})")
                else:
                    # Table view for multiple
                    table_rows = []
                    for res in outputs:
                        row = {
                            "Input State": res.get("state_name_raw") or "-",
                            "Matched State": res.get("state_name_corrected") or "-",
                            "State LGD": res.get("state_lgd_code") or "-",
                            "Input District": res.get("district_name_raw") or "-",
                            "Matched District": res.get("district_name_corrected") or "-",
                            "District LGD": res.get("district_lgd_code") or "-",
                            "Score": f"{res.get('match_confidence_score', 0.0)}%",
                            "Status": res.get("match_status", "NOT_FOUND"),
                        }
                        if "subdistrict_name_corrected" in res or "subdistrict_name_raw" in res:
                            row["Input Sub-District"] = res.get("subdistrict_name_raw") or "-"
                            row["Matched Sub-District"] = res.get("subdistrict_name_corrected") or "-"
                            row["SubDist LGD"] = res.get("subdistrict_lgd_code") or "-"
                        if "village_name_corrected" in res or "village_name_raw" in res:
                            row["Input Village"] = res.get("village_name_raw") or "-"
                            row["Matched Village"] = res.get("village_name_corrected") or "-"
                            row["Village LGD"] = res.get("village_lgd_code") or "-"
                        table_rows.append(row)
                    
                    st.dataframe(pd.DataFrame(table_rows).style.apply(row_style, axis=1), use_container_width=True)
            except Exception as e:
                st.error(f"API Error: {e}")
