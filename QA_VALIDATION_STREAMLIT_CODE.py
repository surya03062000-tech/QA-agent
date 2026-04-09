import streamlit as st
import requests
import time
import base64

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Databricks Validation Portal",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# CONSTANTS (USE st.secrets IN PROD)
# =========================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN = "dapiXXXXXXXXXXXX"

VALIDATION_JOB_ID = 566631342323223
FILE_JOB_ID = 1095682687953224

WORKSPACE_DIR = "/Shared/uploads"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# =========================================================
# REUSABLE: JOB STATUS TRACKER
# =========================================================
def track_job_status(run_id, label):
    status_container = st.container()

    while True:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get?run_id={run_id}",
            headers=HEADERS
        )

        if resp.status_code != 200:
            status_container.error(f"❌ {label}: Unable to fetch job status")
            break

        state = resp.json()["state"]["life_cycle_state"]
        result = resp.json()["state"].get("result_state")

        if state in ["PENDING", "RUNNING"]:
            status_container.info(f"⏳ {label} status: {state}")
        else:
            if result == "SUCCESS":
                status_container.success(f"✅ {label} completed successfully")
            else:
                status_container.error(f"❌ {label} failed: {result}")
            break

        time.sleep(15)

# =========================================================
# HEADER
# =========================================================
st.markdown("""
# ✅ Databricks Validation Portal
Modern UI to trigger **File Ingestion Jobs** and **Validation Jobs**
""")

# =========================================================
# SIDEBAR – FILE UPLOAD (AUTO JOB + STATUS)
# =========================================================
with st.sidebar:
    st.header("📂 Upload Files (Auto Job Trigger)")

    uploaded_files = st.file_uploader(
        "Upload files",
        type=["pdf", "txt", "docx", "xlsx"],
        accept_multiple_files=True
    )

    if uploaded_files and st.button("🚀 Upload & Trigger Job"):
        for file in uploaded_files:
            try:
                # Encode file
                encoded = base64.b64encode(file.getvalue()).decode("utf-8")

                # Upload to workspace
                upload_payload = {
                    "path": f"{WORKSPACE_DIR}/{file.name}",
                    "format": "AUTO",
                    "overwrite": True,
                    "content": encoded
                }

                upload_resp = requests.post(
                    f"{DATABRICKS_HOST}/api/2.0/workspace/import",
                    headers=HEADERS,
                    json=upload_payload
                )

                if upload_resp.status_code != 200:
                    raise RuntimeError(upload_resp.text)

                # Trigger file job
                trigger_payload = {
                    "job_id": FILE_JOB_ID,
                    "notebook_params": {
                        "workspace_file_path": upload_payload["path"]
                    }
                }

                job_resp = requests.post(
                    f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
                    headers=HEADERS,
                    json=trigger_payload
                )

                if job_resp.status_code != 200:
                    raise RuntimeError(job_resp.text)

                run_id = job_resp.json().get("run_id")

                st.success(f"📤 {file.name} uploaded. Job started (Run ID: {run_id})")

                # Track file job status
                track_job_status(run_id, label=f"File Job ({file.name})")

            except Exception as e:
                st.error(f"❌ {file.name}: {e}")

# =========================================================
# MAIN – MANUAL VALIDATION JOB
# =========================================================
st.divider()
st.subheader("🛠 Manual Validation Job Trigger")

with st.form("validation_form"):
    c1, c2 = st.columns(2)

    with c1:
        STM_FILE = st.text_input("STM File Path")
        SHEET_NAME = st.text_input("Sheet Name")
        SOURCE_FILE_PATH = st.text_input("Source File Path")
        OUTPUT_FILE_PATH = st.text_input("Output Path")

    with c2:
        SOURCE_TABLE = st.text_input("Source Table")
        TARGET_TABLE = st.text_input("Target Table")
        PRIMARY_KEYS = st.text_input("Primary Keys")
        SCD_TYPE = st.selectbox("SCD Type", ["1", "2"])

    submit = st.form_submit_button("🚀 Run Validation Job")

# =========================================================
# VALIDATION JOB EXECUTION + STATUS
# =========================================================
if submit:
    with st.spinner("Triggering validation job..."):
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
            st.success(f"✅ Validation job started (Run ID: {run_id})")

            track_job_status(run_id, label="Validation Job")
