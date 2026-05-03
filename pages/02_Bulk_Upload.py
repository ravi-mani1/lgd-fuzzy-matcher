import streamlit as st
import pandas as pd
from app_auth import render_auth_gate
import api_client
from app_components import to_csv_bytes, to_excel_bytes, to_sql_bytes, generate_sql_update

render_auth_gate()

st.title("📂 Bulk Upload & Match")
st.markdown('<div class="section-card"><strong>Bulk Upload Workflow</strong><br/><span class="small-muted">1) Upload file  2) Map columns  3) Run LGD matching  4) Download output</span></div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Upload Input File")
    inp = st.file_uploader("CSV or Excel", type=["csv", "xlsx", "xls"])
    if inp:
        raw_df = pd.read_excel(inp, dtype=str) if inp.name.endswith((".xlsx", ".xls")) else pd.read_csv(inp, dtype=str)
        st.success(f"Loaded {len(raw_df):,} rows")
        st.dataframe(raw_df.head(5), use_container_width=True)
with col2:
    st.subheader("Sample Format")
    sample = pd.DataFrame({"id": [1, 2, 3, 4, 5],
        "state_name_raw": ["delhii", "NCT Delhi", "UP", "Bengluru", "west bengall"],
        "district_name_raw": ["New Delhi", "District Agra", "varansi", "bangalore", "calcuta"],
        "subdistrict_name_raw": ["", "", "pindra", "", ""],
        "village_name_raw": ["", "", "bhagwanpur", "", ""]})
    st.dataframe(sample, use_container_width=True)
    st.download_button("Download Sample CSV", to_csv_bytes(sample), "sample_input.csv", "text/csv")

if inp and "raw_df" in dir():
    st.divider()
    st.subheader("📌 Map Your Columns")
    all_cols = list(raw_df.columns)
    none_opt = ["-- Not in file --"]
    col_options = none_opt + all_cols
    c1, c2, c3 = st.columns(3)
    with c1:
        auto_state = next((c for c in all_cols if any(k in c.lower() for k in ["state_name_raw", "state_name", "state"])), all_cols[0] if all_cols else None)
        state_col = st.selectbox("State Name Column", all_cols, index=all_cols.index(auto_state) if auto_state in all_cols else 0, key="state_col")
    with c2:
        auto_dist = next((c for c in all_cols if any(k in c.lower() for k in ["district_name_raw", "district_name", "district"])), all_cols[1] if len(all_cols) > 1 else all_cols[0])
        dist_col = st.selectbox("District Name Column", all_cols, index=all_cols.index(auto_dist) if auto_dist in all_cols else 0, key="dist_col")
    with c3:
        auto_id_gen = (c for c in all_cols if c.lower() in ["id", "sr", "sno", "s_no", "serial"])
        auto_id = next(auto_id_gen, None)
        id_col = st.selectbox("ID Column (optional)", col_options, index=col_options.index(auto_id) if auto_id in col_options else 0, key="id_col")

    c4, c5 = st.columns(2)
    with c4:
        auto_subdist = next((c for c in all_cols if any(k in c.lower() for k in ["subdistrict", "sub_district", "block", "tehsil", "taluk"])), None)
        subdist_col = st.selectbox("Sub-District/Block Column (optional)", col_options, index=col_options.index(auto_subdist) if auto_subdist in col_options else 0, key="subdist_col")
    with c5:
        auto_village = next((c for c in all_cols if any(k in c.lower() for k in ["village_name", "village"])), None)
        village_col = st.selectbox("Village Column (optional)", col_options, index=col_options.index(auto_village) if auto_village in col_options else 0, key="village_col")

    mapped_df = raw_df.copy()
    mapped_df["state_name_raw"] = raw_df[state_col].fillna("").astype(str)
    mapped_df["district_name_raw"] = raw_df[dist_col].fillna("").astype(str)
    if id_col != "-- Not in file --":
        mapped_df["id"] = raw_df[id_col].astype(str)
    else:
        mapped_df["id"] = range(1, len(mapped_df) + 1)
        
    if subdist_col != "-- Not in file --":
        mapped_df["subdistrict_name_raw"] = raw_df[subdist_col].fillna("").astype(str)
    if village_col != "-- Not in file --":
        mapped_df["village_name_raw"] = raw_df[village_col].fillna("").astype(str)

    with st.expander("Preview mapped columns"):
        preview_cols = ["id", "state_name_raw", "district_name_raw"]
        if "subdistrict_name_raw" in mapped_df.columns:
            preview_cols.append("subdistrict_name_raw")
        if "village_name_raw" in mapped_df.columns:
            preview_cols.append("village_name_raw")
        st.dataframe(mapped_df[preview_cols].head(10), use_container_width=True)

    st.divider()
    run = st.button("Run LGD Matching", type="primary")

    if run:
        with st.spinner("Matching via API..."):
            try:
                # We can use the match_dataframe API which accepts JSON, or we can export to CSV and use the match_csv API
                # For large files, match_csv is much faster and uses less memory in transit.
                csv_bytes = to_csv_bytes(mapped_df[preview_cols])
                res_df = api_client.match_csv_file(csv_bytes)
                
                st.success(f"Matched {len(res_df)} rows successfully!")
                
                # ── Match Summary Dashboard ──
                st.subheader("📊 Match Summary")
                
                if "match_status" in res_df.columns:
                    status_counts = res_df["match_status"].value_counts().to_dict()
                    
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Exact Matches", status_counts.get("EXACT", 0))
                    fuzzy_count = status_counts.get("HIGH_CONFIDENCE", 0) + status_counts.get("MEDIUM_CONFIDENCE", 0) + status_counts.get("LOW_CONFIDENCE", 0)
                    m2.metric("Fuzzy Matches", fuzzy_count)
                    m3.metric("Not Found", status_counts.get("NOT_FOUND", 0))
                    
                    # Convert to dataframe for bar_chart
                    chart_data = pd.DataFrame.from_dict(status_counts, orient='index', columns=['count'])
                    st.bar_chart(chart_data)
                
                # Show results
                st.dataframe(res_df.head(50))
                
                # Prepare downloads
                final_df = pd.merge(raw_df, res_df.drop(columns=["state_name_raw", "district_name_raw"], errors="ignore"), on="id", how="left") if id_col != "-- Not in file --" else res_df
                
                d1, d2, d3 = st.columns(3)
                d1.download_button("⬇️ Download CSV", to_csv_bytes(final_df), "lgd_matched.csv", "text/csv", use_container_width=True)
                d2.download_button("⬇️ Download Excel", to_excel_bytes(final_df), "lgd_matched.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                sql_table = st.text_input("SQL Table Name for Updates", "target_table")
                generate_sql_update(final_df, sql_table, "tmp_sql_export.sql")
                with open("tmp_sql_export.sql", "rb") as f:
                    d3.download_button("⬇️ Download SQL", f, "lgd_updates.sql", "text/plain", use_container_width=True)
                    
            except Exception as e:
                st.error(f"API Error: {e}")
