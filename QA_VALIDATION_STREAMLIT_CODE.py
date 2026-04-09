import streamlit as st
import requests
import time
import base64
import pandas as pd
from datetime import datetime

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="QA Validation Portal",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# DATABRICKS CONFIG
# =========================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN = "dapiXXXXXXXXXXXX"  # move to st.secrets in prod
WORKSPACE_DIR = "/Shared/uploads"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# =========================================================
# SESSION STATE
# =========================================================
if "selected" not in st.session_state:
    st.session_state.selected = "All Validation"

if "job_history" not in st.session_state:
    st.session_state.job_history = []

if "last_job_payload" not in st.session_state:
    st.session_state.last_job_payload = None

# =========================================================
# JOB CONFIGURATION
# =========================================================
JOB_CONFIG = {
    "All Validation": {
        "job_id": 566631342323223,
        "params": [
            "STM_FILE", "SHEET_NAME", "SOURCE_FILE",
            "SOURCE_TABLE", "TARGET_TABLE",
            "PRIMARY_KEYS", "SCD_TYPE"
        ]
    },
    "STM Test Case Generation": {
        "job_id": 236718714761927,
        "params": ["STM_FILE", "SHEET_NAME"]
    },
    "SCD Validation": {
        "job_id": 909635921592434,
        "params": ["STM_FILE", "SOURCE_FILE", "SHEET_NAME", "SCD_TYPE"]
    },
    "STM Validation": {
        "job_id": 190540510295693,
        "params": ["STM_FILE", "SOURCE_FILE", "SHEET_NAME"]
    },
    "SCD Testcases Generation": {
        "job_id": 160480032307967,
        "params": ["SOURCE_TABLE", "TARGET_TABLE", "PRIMARY_KEYS", "SCD_TYPE"]
    }
}

# =========================================================
# OUTPUT FILE PATH MAPPING 🔥 NEW
# =========================================================
def get_output_paths(category, params):
    base = "/Volumes/edl_qa/qa_agent/qa_validation"

    if category == "All Validation":
        return [
            f"{base}/{params['STM_FILE']}_STM_Testcases.xlsx",
            f"{base}/{params['TARGET_TABLE']}_SCD_Validation.xlsx",
            f"{base}/{params['TARGET_TABLE']}_SCD_Testcases.xlsx",
            f"{base}/{params['TARGET_TABLE']}_STM_vs_Target_final_output.xlsx"
        ]
    if category == "STM Test Case Generation":
        return [f"{base}/{params['STM_FILE']}_STM_Testcases.xlsx"]
    if category == "SCD Validation":
        return [f"{base}/{params['TARGET_TABLE']}_SCD_Validation.xlsx"]
    if category == "SCD Testcases Generation":
        return [f"{base}/{params['TARGET_TABLE']}_SCD_Testcases.xlsx"]
    if category == "STM Validation":
        return [f"{base}/{params['TARGET_TABLE']}_STM_vs_Target_final_output.xlsx"]

    return []

# =========================================================
# DOWNLOAD + INLINE LOG VIEWER 🔥 NEW
# =========================================================
def get_job_logs(run_id):
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}",
        headers=HEADERS
    )
    if r.status_code != 200:
        return "Unable to fetch logs"
    return r.json().get("logs", "No logs")

# =========================================================
# JOB STATUS TRACKER 🔥 UPGRADED
# =========================================================
def track_job(run_id, category, job_id, params):
    status_box = st.empty()

    while True:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get?run_id={run_id}",
            headers=HEADERS
        )

        state = r.json()["state"]["life_cycle_state"]
        result = r.json()["state"].get("result_state")

        status_box.info(f"⏳ {category} Status: {state}")

        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            final_status = result or state

            st.session_state.job_history.append({
                "Category": category,
                "Job ID": job_id,
                "Run ID": run_id,
                "Status": final_status,
                "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

            job_url = f"{DATABRICKS_HOST}/#job/{job_id}/run/{run_id}"

            if final_status == "SUCCESS":
                status_box.success("✅ Job completed successfully")

                st.markdown("### 📂 Output Files Generated")
                for p in get_output_paths(category, params):
                    st.code(p)

            else:
                status_box.error(f"❌ Job failed: {final_status}")

                # 🔁 Retry Button
                if st.button("🔁 Retry Failed Job"):
                    st.session_state.retry = True

            st.markdown(f"🔗 **Databricks Job Run:** [{job_url}]({job_url})")

            logs = get_job_logs(run_id)

            with st.expander("📜 View Execution Logs"):
                st.text(logs)

            st.download_button(
                "📥 Download Logs",
                logs,
                file_name=f"{run_id}_logs.txt"
            )
            break

        time.sleep(10)

# =========================================================
# HEADER
# =========================================================
st.markdown("# ✅ QA Validation Portal")

# =========================================================
# CATEGORY BUTTONS (UNCHANGED UI LOGIC)
# =========================================================
st.subheader("🔹 Select QA Validation Category")

if st.button("✅ All Validation", use_container_width=True):
    st.session_state.selected = "All Validation"

c1, c2, c3, c4 = st.columns(4)

with c1:
    if st.button("📄 STM TC Gen", use_container_width=True):
        st.session_state.selected = "STM Test Case Generation"
with c2:
    if st.button("🔁 SCD Validation", use_container_width=True):
        st.session_state.selected = "SCD Validation"
with c3:
    if st.button("✅ STM Validation", use_container_width=True):
        st.session_state.selected = "STM Validation"
with c4:
    if st.button("🧪 SCD TC Gen", use_container_width=True):
        st.session_state.selected = "SCD Testcases Generation"

# =========================================================
# DYNAMIC FORM
# =========================================================
category = st.session_state.selected
job = JOB_CONFIG[category]

st.subheader(f"🛠 {category}")

with st.form("job_form"):
    inputs = {}
    cols = st.columns(2)

    for i, p in enumerate(job["params"]):
        with cols[i % 2]:
            inputs[p] = (
                st.selectbox("SCD Type", ["1", "2"])
                if p == "SCD_TYPE"
                else st.text_input(p)
            )

    submit = st.form_submit_button("🚀 Run Job")

# =========================================================
# JOB TRIGGER
# =========================================================
if submit or st.session_state.get("retry"):
    st.session_state.retry = False

    payload = {
        "job_id": job["job_id"],
        "notebook_params": inputs
    }

    st.session_state.last_job_payload = payload

    with st.spinner("Triggering job..."):
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.2/jobs/run-now",
            headers=HEADERS,
            json=payload
        )

        if r.status_code != 200:
            st.error(r.text)
        else:
            run_id = r.json()["run_id"]
            track_job(run_id, category, job["job_id"], inputs)

# =========================================================
# JOB HISTORY TABLE
# =========================================================
st.divider()
st.subheader("📊 Job Execution History")

if st.session_state.job_history:
    df = pd.DataFrame(st.session_state.job_history)
    st.dataframe(df, use_container_width=True)
else:
    st.info("No jobs executed yet")
