import streamlit as st
import requests
import time
import base64

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
TOKEN = "dapiXXXXXXXXXXXX"   # use st.secrets in prod

WORKSPACE_DIR = "/Shared/uploads"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# =========================================================
# JOB CONFIGURATION
# =========================================================
JOB_CONFIG = {
    "All Validation": {
        "job_id": 566631342323223,
        "params": [
            "STM_FILE", "SHEET_NAME", "SOURCE_FILE_PATH", "OUTPUT_FILE_PATH",
            "SOURCE_TABLE", "TARGET_TABLE", "PRIMARY_KEYS", "SCD_TYPE"
        ]
    },
    "STM Test Case Generation": {
        "job_id": 4567,
        "params": ["STM_FILE", "SHEET_NAME"]
    },
    "SCD Validation": {
        "job_id": 6754,
        "params": ["STM_FILE", "SOURCE_FILE", "SHEET_NAME", "SCD_TYPE"]
    },
    "STM Validation": {
        "job_id": 4532,
        "params": ["STM_FILE", "SOURCE_FILE", "SHEET_NAME"]
    },
    "SCD Testcases Generation": {
        "job_id": 54738,
        "params": ["SOURCE_TABLE", "TARGET_TABLE", "PRIMARY_KEYS", "SCD_TYPE"]
    }
}

# =========================================================
# JOB STATUS TRACKER
# =========================================================
def track_job(run_id, label):
    box = st.empty()
    while True:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get?run_id={run_id}",
            headers=HEADERS
        )

        if r.status_code != 200:
            box.error("❌ Unable to fetch job status")
            break

        state = r.json()["state"]["life_cycle_state"]
        result = r.json()["state"].get("result_state")

        box.info(f"⏳ {label} : {state}")

        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            if result == "SUCCESS":
                box.success(f"✅ {label} completed successfully")
            else:
                box.error(f"❌ {label} failed : {result}")
            break

        time.sleep(10)

# =========================================================
# HEADER
# =========================================================
st.markdown("""
# ✅ QA Validation Portal
Enterprise‑grade UI for validations & testcase generation
""")

# =========================================================
# SIDEBAR – FILE UPLOAD (UNCHANGED)
# =========================================================
with st.sidebar:
    st.header("📂 Upload Files (Auto Job)")

    uploaded_files = st.file_uploader(
        "Upload files",
        type=["pdf", "txt", "docx", "xlsx"],
        accept_multiple_files=True
    )

    if uploaded_files and st.button("🚀 Upload & Trigger File Job"):
        for file in uploaded_files:
            try:
                encoded = base64.b64encode(file.getvalue()).decode("utf-8")
                payload = {
                    "path": f"{WORKSPACE_DIR}/{file.name}",
                    "format": "AUTO",
                    "overwrite": True,
                    "content": encoded
                }

                r = requests.post(
                    f"{DATABRICKS_HOST}/api/2.0/workspace/import",
                    headers=HEADERS,
                    json=payload
                )

                if r.status_code != 200:
                    raise RuntimeError(r.text)

                st.success(f"📤 {file.name} uploaded")

            except Exception as e:
                st.error(str(e))

# =========================================================
# CATEGORY BUTTONS (UI)
# =========================================================
st.divider()
st.subheader("🔹 Select QA Validation Category")

if "selected" not in st.session_state:
    st.session_state.selected = "All Validation"

# BIG BUTTON
if st.button("✅ All Validation", use_container_width=True):
    st.session_state.selected = "All Validation"

c1, c2, c3, c4 = st.columns(4)

with c1:
    if st.button("📄 STM TC Gen"):
        st.session_state.selected = "STM Test Case Generation"
with c2:
    if st.button("🔁 SCD Validation"):
        st.session_state.selected = "SCD Validation"
with c3:
    if st.button("✅ STM Validation"):
        st.session_state.selected = "STM Validation"
with c4:
    if st.button("🧪 SCD TC Gen"):
        st.session_state.selected = "SCD Testcases Generation"

st.divider()

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
            if p == "SCD_TYPE":
                inputs[p] = st.selectbox("SCD Type", ["1", "2"])
            else:
                inputs[p] = st.text_input(p)

    submit = st.form_submit_button("🚀 Run Job")

# =========================================================
# JOB TRIGGER + STATUS
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
            track_job(run_id, category)
