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
TOKEN = "dapi180370eb25ac521baee3f96924db98e9"   # ✅ move to st.secrets in prod
WORKSPACE_DIR = "/Shared/uploads"
FILE_JOB_ID = 1095682687953224

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

# =========================================================
# JOB CONFIGURATION
# =========================================================
JOB_CONFIG = {
    "All Validation": {
        "job_id": 566631342323223,
        "params": [
            "STM_FILE", "SHEET_NAME", "SOURCE_FILE",
            "SOURCE_TABLE", "TARGET_TABLE", "PRIMARY_KEYS", "SCD_TYPE"
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
# CLEAN FILE NAME
# =========================================================
def clean_name(name):
    if not name:
        return ""
    for ext in [".xlsx", ".xls", ".csv"]:
        name = name.replace(ext, "")
    return name.strip()

# =========================================================
# OUTPUT FILE PATHS
# =========================================================
def get_output_files(category, params):
    base = "/Volumes/edl_qa/qa_agent/qa_validation"

    stm = clean_name(params.get("STM_FILE"))
    tgt = clean_name(params.get("TARGET_TABLE"))

    if category == "All Validation":
        return [
            f"{base}/{stm}_STM_Testcases.xlsx",
            f"{base}/{tgt}_SCD_Validation.xlsx",
            f"{base}/{tgt}_SCD_Testcases.xlsx",
            f"{base}/{tgt}_STM_vs_Target_final_output.xlsx",
        ]
    if category == "STM Test Case Generation":
        return [f"{base}/{stm}_STM_Testcases.xlsx"]
    if category == "SCD Validation":
        return [f"{base}/{tgt}_SCD_Validation.xlsx"]
    if category == "SCD Testcases Generation":
        return [f"{base}/{tgt}_SCD_Testcases.xlsx"]
    if category == "STM Validation":
        return [f"{base}/{tgt}_STM_vs_Target_final_output.xlsx"]
    return []
# =========================================================
# CUSTOM CSS (BUTTON COLOR + SIZE)
# =========================================================
st.markdown("""
<style>
.big-btn button {
    background-color: #2563EB;
    color: white;
    height: 72px;
    font-size: 22px;
    border-radius: 12px;
    font-weight: 600;
}

.med-btn button {
    height: 56px;
    font-size: 17px;
    border-radius: 10px;
    font-weight: 500;
}

.stm button { background-color: #0EA5E9; color: white; }
.scd button { background-color: #16A34A; color: white; }
.stmval button { background-color: #9333EA; color: white; }
.scdtc button { background-color: #F97316; color: white; }
</style>
""", unsafe_allow_html=True)

# =========================================================
# SAFE LOG FETCH
# =========================================================
def download_job_logs(run_id):
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}",
        headers=HEADERS
    )
    if r.status_code != 200:
        return None
    return str(r.json().get("logs", ""))
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
# JOB STATUS TRACKER
# =========================================================
def track_job(run_id, category, job_id, params, payload):
    status_box = st.empty()

    while True:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get?run_id={run_id}",
            headers=HEADERS
        )

        state = r.json()["state"]["life_cycle_state"]
        result = r.json()["state"].get("result_state")

        status_box.info(f"⏳ {category} Status : {state}")

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
                st.markdown("### 📂 Output files generated at:")
                for f in get_output_files(category, params):
                    st.code(f)
            else:
                status_box.error(f"❌ Job failed : {final_status}")
                if st.button("🔁 Retry Failed Job"):
                    requests.post(
                        f"{DATABRICKS_HOST}/api/2.2/jobs/run-now",
                        headers=HEADERS,
                        json=payload
                    )
                    st.info("🔄 Job retriggered")

            st.markdown(f"🔗 **Databricks Job Run:** {job_url}")

            logs = download_job_logs(run_id)
            with st.expander("📜 View Execution Logs"):
                st.text(logs if logs else "No logs available")

            if logs:
                st.download_button(
                    "📥 Download Logs",
                    data=logs,
                    file_name=f"{run_id}_logs.txt",
                    mime="text/plain"
                )
            break

        time.sleep(10)

# =========================================================
# HEADER
# =========================================================
st.markdown("""
# ✅ QA Validation Portal
Enterprise‑ready UI for QA validations & testcase generation
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
# CATEGORY SELECTION
# =========================================================
st.divider()
st.subheader("🔹 Select QA Validation Category")

st.markdown("<div class='big-btn'>", unsafe_allow_html=True)
if st.button("✅ All Validation", use_container_width=True):
    st.session_state.selected = "All Validation"
st.markdown("</div>", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown("<div class='med-btn stm'>", unsafe_allow_html=True)
    if st.button("📄 STM TC Gen", use_container_width=True):
        st.session_state.selected = "STM Test Case Generation"
    st.markdown("</div>", unsafe_allow_html=True)

with c2:
    st.markdown("<div class='med-btn scd'>", unsafe_allow_html=True)
    if st.button("🔁 SCD Validation", use_container_width=True):
        st.session_state.selected = "SCD Validation"
    st.markdown("</div>", unsafe_allow_html=True)

with c3:
    st.markdown("<div class='med-btn stmval'>", unsafe_allow_html=True)
    if st.button("✅ STM Validation", use_container_width=True):
        st.session_state.selected = "STM Validation"
    st.markdown("</div>", unsafe_allow_html=True)

with c4:
    st.markdown("<div class='med-btn scdtc'>", unsafe_allow_html=True)
    if st.button("🧪 SCD TC Gen", use_container_width=True):
        st.session_state.selected = "SCD Testcases Generation"
    st.markdown("</div>", unsafe_allow_html=True)
# =========================================================
# DYNAMIC FORM
# =========================================================
st.divider()
category = st.session_state.selected
job = JOB_CONFIG[category]

st.subheader(f"🛠 {category}")

with st.form("job_form"):
    inputs = {}
    cols = st.columns(2)
    for i, p in enumerate(job["params"]):
        with cols[i % 2]:
            if p == "SCD_TYPE":
                inputs[p] = st.selectbox("SCD Type", ["1", "2"])
            else:
                inputs[p] = st.text_input(p)

    submit = st.form_submit_button("🚀 Run Job")

# =========================================================
# JOB TRIGGER
# =========================================================
if submit:
    payload = {
        "job_id": job["job_id"],
        "notebook_params": inputs
    }

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
            st.success(f"✅ Job Started | Run ID: {run_id}")
            track_job(run_id, category, job["job_id"], inputs, payload)

# =========================================================
# JOB HISTORY
# =========================================================
st.divider()
st.subheader("📊 Job Execution History")

if st.session_state.job_history:
    df = pd.DataFrame(st.session_state.job_history)
    st.dataframe(df, use_container_width=True)
else:
    st.info("No jobs executed yet.")
