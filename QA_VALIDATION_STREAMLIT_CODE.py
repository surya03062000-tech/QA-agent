
import streamlit as st
import requests
import time

# Databricks credentials
DATABRICKS_INSTANCE = "https://dbc-7c82be33-847c.cloud.databricks.com"
TOKEN = "dapib0936ff745e3a10c23c4b0264876090e"
JOB_ID = "813058467048815"

st.title("START VALIDATION")

# User inputs
param1 = st.text_input("STM file path:")
param2 = st.text_input("Source file path:")
param3 = st.text_input("Output Path:")
param4 = st.text_input("SOURCE_TABLE:")
param5 = st.text_input("TARGET_TABLE:")
param6 = st.text_input("PRIMARY_KEYS:")  
param7 = st.text_input("SCD_TYPE:")

if st.button("Run Notebook"):
    st.write("Triggering Databricks job...")
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {
        "job_id": JOB_ID,
        "notebook_params": {
            "STM_FILE_PATH": param1,
            "SOURCE_FILE_PATH": param2,
            "OUTPUT_FILE_PATH": param3,
            "SOURCE_TABLE": param4,
            "TARGET_TABLE": param5,
            "PRIMARY_KEYS": param6,
            "SCD_TYPE": param7
        }
    }

    try:
        response = requests.post(f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now",
                                 json=payload, headers=headers)

        # Check if response is JSON
        if response.status_code != 200:
            st.error(f"API Error: {response.status_code}")
            st.write("Response:", response.text)
        else:
            try:
                data = response.json()
                run_id = data.get("run_id")
                if not run_id:
                    st.error("No run_id returned. Response:")
                    st.write(data)
                else:
                    st.success(f"Job started with run_id: {run_id}")

                    # Poll job status
                    while True:
                        status_resp = requests.get(
                            f"{DATABRICKS_INSTANCE}/api/2.2/jobs/runs/get?run_id={run_id}",
                            headers=headers
                        )

                        if status_resp.status_code != 200:
                            st.error(f"Status API Error: {status_resp.status_code}")
                            st.write(status_resp.text)
                            break

                        status_data = status_resp.json()
                        state = status_data.get("state", {}).get("life_cycle_state", "UNKNOWN")
                        st.write(f"Current status: {state}")

                        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                            result_state = status_data.get("state", {}).get("result_state", "UNKNOWN")
                            st.write(f"Result state: {result_state}")
                            if result_state == "SUCCESS":
                                st.success("Job completed successfully!")
                            else:
                                st.error(f"Job ended with state: {result_state}")
                            break

                        time.sleep(60)

            except ValueError:
                st.error("Invalid JSON response from API")
                st.write(response.text)

    except Exception as e:
        st.error(f"Unexpected error: {e}")






