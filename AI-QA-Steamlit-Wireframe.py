import streamlit as st
import pandas as pd
import requests
import time
import json

# -------------------------------------------------------------------
# CONFIGURATION (replace with st.secrets in production)
# -------------------------------------------------------------------
DATABRICKS_INSTANCE = "https://dbc-e124ec40-fa61.cloud.databricks.com"
TOKEN = "dapi5dacec5611a5238aec10d5f69b150d09"
JOB_ID = "408448156916986"


# -------------------------------
# Page Configuration
# -------------------------------
st.set_page_config(
    page_title="End-to-End AI QA",
    layout="wide"
)

# -------------------------------
# Header
# -------------------------------
st.title("End‑to‑End AI QA for Ingestion Pipelines")

# -------------------------------
# Main Layout
# -------------------------------
left_col, right_col = st.columns([3.5, 1.5], gap="medium")

# ============================================================================
# LEFT PANEL
# ============================================================================
with left_col:

    # ===============================
    # INGESTION CONFIGURATION
    # ===============================
    with st.container(border=True):
        st.subheader("Ingestion Configuration")

        st.selectbox(
            "Select STM Location",
            ["Select", "STM_Location_1", "STM_Location_2"],
            key="stm_location"
        )

        stm_file_path = st.text_input(
            "Files Location (parquet, csv, etc)",
            placeholder="Enter STM file path",
            key="files_location"
        )

        # ✅ Summary trigger (unique key already present)
        summary_clicked = st.button("Summary", key="summary_btn")

    # ===============================
    # QUALITY ASSURANCE
    # ===============================
    with st.container(border=True):
        st.subheader("Quality Assurance")

        st.button(
            "Run All Validations",
            key="run_all_validations_btn",
            use_container_width=True
        )

        st.markdown("**Structure**")
        col1, col2 = st.columns(2)

        with col1:
            st.button(
                "Test Case Generator",
                key="structure_test_case_generator",
                use_container_width=True
            )

        with col2:
            st.button(
                "Test Case Validation",
                key="structure_test_case_validation",
                use_container_width=True
            )

        st.markdown("**SCD**")
        col3, col4 = st.columns(2)

        with col3:
            st.button(
                "Test Case Generator",
                key="scd_test_case_generator",
                use_container_width=True
            )

        with col4:
            st.button(
                "Test Case Validation",
                key="scd_test_case_validation",
                use_container_width=True
            )

# ============================================================================
# RIGHT PANEL – SUMMARY VIEWER
# ============================================================================
with right_col:

    with st.container(border=True):
        st.subheader("Summary Viewer")

        # Initialize summary storage
        if "df_summary" not in st.session_state:
            st.session_state.df_summary = pd.DataFrame(
                columns=["Category", "Details"]
            )

        
        


        # --------------------------------------------------
        # RUN NOTEBOOK WHEN SUMMARY BUTTON IS CLICKED
        # --------------------------------------------------

        if summary_clicked:
        
            if not stm_file_path:
                st.error("Please enter STM file path before clicking Summary.")
                st.stop()
        
            headers = {"Authorization": f"Bearer {TOKEN}"}
        
            payload = {
                "job_id": JOB_ID,
                "notebook_params": {
                    "stm_file_path": stm_file_path
                }
            }
        
            try:
                # 1️⃣ Trigger job
                run_resp = requests.post(
                    f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
        
                run_id = run_resp.json().get("run_id")
                if not run_id:
                    st.error("No run_id returned")
                    st.stop()
        
                # 2️⃣ Wait until TASK is finished
                while True:
                    run_info = requests.get(
                        f"{DATABRICKS_INSTANCE}/api/2.2/jobs/runs/get",
                        headers=headers,
                        params={"run_id": run_id},
                        timeout=30
                    ).json()
        
                    task = run_info["tasks"][0]
                    task_run_id = task["run_id"]
                    task_state = task["state"]["life_cycle_state"]
        
                    if task_state == "TERMINATED":
                        break
        
                    time.sleep(5)
        
                # 3️⃣ Fetch notebook output using TASK run_id
                output_resp = requests.get(
                    f"{DATABRICKS_INSTANCE}/api/2.2/jobs/runs/get-output",
                    headers=headers,
                    params={"run_id": task_run_id},
                    timeout=30
                )
        
                # Full Databricks response
                output_json = output_resp.json()
                
                # ✅ Extract ONLY notebook results
                notebook_result = output_json.get("notebook_output", {}).get("result")
                
                
                if not notebook_result:
                    st.error("Notebook finished but returned no result")
                else:
                    # Convert JSON string → Python list
                    records = json.loads(notebook_result)
                
                    # Create DataFrame
                    df_summary = pd.DataFrame(records)
                
                    # (Optional) store in session state
                    st.session_state.df_summary = df_summary

                    st.dataframe(
                    st.session_state.df_summary,
                    use_container_width=True,
                    height=260,   # 👈 enables scrolling
                    hide_index=True
                )
        
            except Exception as e:
                st.error(str(e))
