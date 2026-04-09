import streamlit as st
import requests
import time
import base64

# =========================================================
# PAGE CONFIG (ADVANCED LOOK)
# =========================================================
st.set_page_config(
    page_title="Databricks Validation Portal",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# CONSTANTS (MOVE TO SECRETS IN PROD)
# =========================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN = "dapi180370eb25ac521baee3f96924db98e9"  # ❗ use st.secrets in prod

VALIDATION_JOB_ID = 566631342323223
FILE_JOB_ID = 1095682687953224

WORKSPACE_DIR = "/Shared/uploads"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# =========================================================
# UI HEADER
# =========================================================
st.markdown("""
# ✅ Databricks Validation Portal
A unified interface to trigger **Validation Jobs** and **File Ingestion Jobs**
""")

# =========================================================
# SIDEBAR – FILE UPLOAD (AUTO JOB TRIGGER)
# =========================================================
with st.sidebar:
    st.header("📂 Upload Files (Auto Trigger)")

    uploaded_files = st.file_uploader(
        "Upload files (.pdf, .txt, .docx, .xlsx)",
        type=["pdf", "txt", "docx", "xlsx"],
        accept_multiple_files=True
    )

    if uploaded_files:
        if st.button("🚀 Upload & Trigger Job"):
            for file in uploaded_files:
                try:
                    encoded = base64.b64encode(file.getvalue()).decode("utf-8")

                    payload = {
                        "path": f"{WORKSPACE_DIR}/{file.name}",
                        "format": "AUTO",
                        "overwrite": True,
                        "content": encoded
                    }

                    upload_resp = requests.post(
                        f"{DATABRICKS_HOST}/api/2.0/workspace/import",
                        headers=HEADERS,
                        json=payload
                    )

                    if upload_resp.status_code != 200:
                        raise RuntimeError(upload_resp.text)

                    # Trigger job with uploaded file path
                    trigger_payload = {
                        "job_id": FILE_JOB_ID,
                        "notebook_params": {
                            "workspace_file_path": payload["path"]
                        }
                    }

                    job_resp = requests.post(
                        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
                        headers=HEADERS,
                        json=trigger_payload
                    )

                    if job_resp.status_code != 200:
                        raise RuntimeError(job_resp.text)

                    st.success(f"✅ {file.name} uploaded & job triggered")

                except Exception as e:
                    st.error(f"❌ {file.name}: {e}")

# =========================================================
# MAIN – MANUAL VALIDATION JOB
# =========================================================
st.divider()
st.subheader("🛠 Manual Validation Job Trigger")

with st.form("validation_form"):
    col1, col2 = st.columns(2)

    with col1:
        STM_FILE = st.text_input("STM File Path")
        SHEET_NAME = st.text_input("Sheet Name")
        SOURCE_FILE_PATH = st.text_input("Source File Path")
        OUTPUT_FILE_PATH = st.text_input("Output Path")

    with col2:
        SOURCE_TABLE = st.text_input("Source Table")
        TARGET_TABLE = st.text_input("Target Table")
        PRIMARY_KEYS = st.text_input("Primary Keys")
        SCD_TYPE = st.selectbox("SCD Type", ["1", "2"])

    submit = st.form_submit_button("🚀 Run Validation Job")

# =========================================================
# JOB EXECUTION & STATUS TRACKING
# =========================================================
if submit:
    with st.spinner("Triggering Databricks job..."):
        payload = {
            "job_id": VALIDATION_JOB_ID,
            "notebook_params": {
                "STM_FILE": STM_FILE,
                "SHEET_NAME": SHEET_NAME,
                "SOURCE_FILE_PATH": SOURCE_FILE_PATH,
                "OUTPUT_FILE_PATH": OUTPUT_FILE_PATH,
                "SOURCE_TABLE": SOURCE_TABLE,
                "TARGET_TABLE": TARGET_TABLE,
                "PRIMARY_KEYS": PRIMARY_KEYS,
                "SCD_TYPE": SCD_TYPE
            }
        }

        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.2/jobs/run-now",
            headers=HEADERS,
            json=payload
        )

        if resp.status_code != 200:
            st.error(resp.text)
        else:
            run_id = resp.json().get("run_id")
            st.success(f"✅ Job started | Run ID: {run_id}")

            # Polling
            while True:
                status_resp = requests.get(
                    f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get?run_id={run_id}",
                    headers=HEADERS
                )

                state = status_resp.json()["state"]["life_cycle_state"]
                st.info(f"Current Status: {state}")

                if state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]:
                    result = status_resp.json()["state"].get("result_state")
                    if result == "SUCCESS":
                        st.success("🎉 Validation completed successfully")
                    else:
                        st.error(f"❌ Job failed: {result}")
                    break

                time.sleep(30)
