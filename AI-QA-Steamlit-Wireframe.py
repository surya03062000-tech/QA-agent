"""
========================================================================
 End-to-End AI QA Portal   (v6 — compact single-page, inline status,
                              light-blue theme, no STM labels)
========================================================================
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import time
import json
import base64
from datetime import datetime

# Try to import databricks.sql, show error if not available
try:
    from databricks import sql as databricks_sql
    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False
    databricks_sql = None

# =========================================================================
# 1. CONFIG
# =========================================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN           = "dapi180370eb25ac521baee3f96924db98e9"

# Databricks SQL Warehouse connection for DG Creation

WORKSPACE_UPLOAD_DIR = "/Shared/qa_uploads"
VOLUME_PATH          = "/Volumes/edl_qa/qa_agent/qa_validation_input"

FILE_COPY_JOB_ID = 1095682687953224
SUMMARY_JOB_ID   = 29471425720129
DG_CREATION_JOB_ID = 878559626474883

JOB_IDS = {
    "Run All Validation"  : 566631342323223,
    "Structure Validation": 190540510295693,
    "SCD Validation"      : 909635921592434,
    "Test Case Generator" : 160480032307967,
}

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type" : "application/json",
}

# ============================================================
# Credentials (from Streamlit secrets or env vars)
# ============================================================
def _get_secret(key: str) -> str:
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, "")

DBX_HOST = _get_secret("DBX_HOST")
DBX_HTTP_PATH = _get_secret("DBX_HTTP_PATH")
DBX_TOKEN = _get_secret("DBX_TOKEN")

if not all([DBX_HOST, DBX_HTTP_PATH, DBX_TOKEN]):
    st.error(
        "❌ Missing Databricks credentials.\n\n"
        "Set **DBX_HOST**, **DBX_HTTP_PATH**, **DBX_TOKEN** in Streamlit Cloud → "
        "App Settings → Secrets."
    )
    st.stop()

def get_databricks_connection():
    """Get Databricks SQL connection for catalog/schema/table queries"""
    if not DATABRICKS_SQL_AVAILABLE:
        raise ImportError(
            "databricks-sql-connector is not installed. "
            "Please add 'databricks-sql-connector' to your requirements.txt file."
        )
    return databricks_sql.connect(
        server_hostname=DBX_HOST,
        http_path=DBX_HTTP_PATH,
        access_token=DBX_TOKEN,
    )

# =========================================================================
# 2. PAGE CONFIG
# =========================================================================
st.set_page_config(
    page_title="IngestIQ™ AI QA",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================================================================
# 3. CSS — light-blue theme, zero gaps, single-page
# =========================================================================
st.markdown("""
<style>
/* ── Google Font ───────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');

/* ── Global reset / font ───────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif !important;
}

/* ── Shrink Streamlit's default top padding to near-zero ───────────── */
.block-container {
    padding-top: 0.6rem !important;
    padding-bottom: 0.6rem !important;
    max-width: 100% !important;
}
header[data-testid="stHeader"] { display: none !important; }
.stApp > header { display: none !important; }

/* Remove blank space Streamlit adds after st.title / st.caption */
.stMarkdown h1 { margin-bottom: 0 !important; }
.element-container:has(h1) { margin-bottom: 0 !important; }
div[data-testid="stVerticalBlock"] > div:first-child { padding-top: 0 !important; }

/* ── Light-blue palette for ALL bordered containers ────────────────── */
[data-testid="stContainerWithBorder"],
[data-testid="stVerticalBlockBorderWrapper"] {
    background: linear-gradient(145deg, #EFF6FF 0%, #DBEAFE 100%) !important;
    border: 1.5px solid #BFDBFE !important;
    border-radius: 14px !important;
    box-shadow: 0 2px 10px rgba(37,99,235,0.07) !important;
}

/* nested containers — slightly lighter */
[data-testid="stContainerWithBorder"] [data-testid="stContainerWithBorder"] {
    background: rgba(255,255,255,0.72) !important;
    border: 1px dashed #93C5FD !important;
    border-radius: 10px !important;
    box-shadow: none !important;
}

/* ── Subheader / caption ───────────────────────────────────────────── */
h3 { color: #1e40af !important; font-size: 1rem !important;
     letter-spacing: -.01em; margin-bottom: 6px !important; }
.stCaption p { color: #64748b !important; font-size: 0.78rem !important; }

/* ── File uploader ──────────────────────────────────────────────────── */
[data-testid="stFileUploader"] {
    background: rgba(255,255,255,0.8) !important;
    border: 2px dashed #93C5FD !important;
    border-radius: 10px !important;
}

/* ── Status badge colours (button wrappers) ─────────────────────────── */
.btn-idle    button { background:#2563EB !important; color:#fff !important; }
.btn-running button { background:#0EA5E9 !important; color:#fff !important;
                      animation: pulse 1.2s infinite; }
.btn-success button { background:#16A34A !important; color:#fff !important; }
.btn-failed  button { background:#DC2626 !important; color:#fff !important; }
@keyframes pulse { 0%{opacity:1;} 50%{opacity:.5;} 100%{opacity:1;} }

/* ── Compact QA buttons ─────────────────────────────────────────────── */
.btn-compact button {
    padding: 5px 14px !important;
    font-size: 11.5px !important;
    font-weight: 700 !important;
    min-height: 36px !important;
    height: 36px !important;
    border-radius: 8px !important;
    letter-spacing: 0.04em !important;
    border: none !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.18) !important;
    width: 100% !important;
    transition: opacity 0.15s ease, transform 0.1s ease !important;
}
.btn-compact button:hover { opacity:.88 !important; transform:translateY(-1px) !important; }
.btn-compact-idle    button { background:#2563EB !important; color:#fff !important; }
.btn-compact-running button { background:#0EA5E9 !important; color:#fff !important;
                               animation: pulse 1.2s infinite; }
.btn-compact-success button { background:#16A34A !important; color:#fff !important; }
.btn-compact-failed  button { background:#DC2626 !important; color:#fff !important; }
.btn-compact { margin-bottom:0 !important; }
.btn-compact > div { margin-bottom:0 !important; }

/* ── Inline log / status card ───────────────────────────────────────── */
.status-card {
    background: rgba(255,255,255,0.88);
    border: 1.5px solid #BFDBFE;
    border-radius: 10px;
    padding: 12px 16px;
    margin-top: 10px;
    font-family: 'DM Sans', sans-serif;
}
.status-card .sc-title {
    font-size: 12px;
    font-weight: 700;
    color: #1e40af;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 6px;
}
.status-card .sc-phase {
    font-size: 11.5px;
    color: #374151;
    margin-bottom: 6px;
}
.status-card .sc-bar-wrap {
    background: #DBEAFE;
    border-radius: 999px;
    overflow: hidden;
    height: 8px;
    margin-bottom: 5px;
}
.status-card .sc-bar-fill {
    background: linear-gradient(90deg, #60A5FA, #2563EB);
    height: 100%;
    border-radius: 999px;
    transition: width 0.5s ease;
}
.status-card .sc-pct {
    font-size: 10.5px;
    color: #6b7280;
    text-align: right;
    font-weight: 600;
    margin-bottom: 8px;
}
.status-card ul.sc-steps {
    list-style: none;
    padding: 0; margin: 0;
    font-size: 11px;
}
.status-card ul.sc-steps li { padding: 2px 0; color: #6b7280; }
.status-card ul.sc-steps li.done   { color: #16A34A; }
.status-card ul.sc-steps li.active { color: #1d4ed8; font-weight: 700; }

/* ── Progress tracker card (QA validation) ──────────────────────────── */
.progress-card {
    background: rgba(255,255,255,0.9);
    border: 1.5px solid #BFDBFE;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 2px 8px rgba(37,99,235,0.08);
    font-family: 'DM Sans', sans-serif;
    color: #1f2937;
    margin: 10px 0;
}
.progress-card .pc-title {
    font-size: 14px; font-weight: 700; color: #1e40af;
    margin-bottom: 8px;
    display: flex; align-items: center; justify-content: space-between;
}
.progress-card .pc-phase { font-size: 12px; color: #374151; margin-bottom: 7px; }
.progress-card .pc-phase b { color: #1e40af; }
.progress-card .pc-bar-wrap {
    background: #DBEAFE; border-radius: 999px;
    overflow: hidden; height: 12px; margin-bottom: 5px;
}
.progress-card .pc-bar-fill {
    background: linear-gradient(90deg, #60A5FA, #2563EB);
    height: 100%; border-radius: 999px; transition: width 0.6s ease;
}
.progress-card .pc-pct {
    font-size: 11px; color: #64748b; text-align: right;
    font-weight: 600; margin-bottom: 8px;
}
.progress-card ul.pc-steps {
    list-style: none; padding: 0; margin: 8px 0 0; font-size: 11.5px;
}
.progress-card ul.pc-steps li { padding: 2.5px 0; color: #6b7280; }
.progress-card ul.pc-steps li.done   { color: #16A34A; }
.progress-card ul.pc-steps li.active { color: #1d4ed8; font-weight: 700; }

/* ── Header strip ───────────────────────────────────────────────────── */
.iq-header {
    background: linear-gradient(135deg, #1e40af 0%, #2563EB 60%, #38BDF8 100%);
    border-radius: 14px;
    padding: 14px 24px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 4px 18px rgba(37,99,235,0.25);
}
.iq-header .iq-title {
    font-size: 1.35rem;
    font-weight: 800;
    color: #fff;
    letter-spacing: -0.02em;
    line-height: 1.2;
}
.iq-header .iq-sub {
    font-size: 0.77rem;
    color: rgba(255,255,255,0.78);
    margin-top: 3px;
    font-weight: 400;
}
.iq-badge {
    background: rgba(255,255,255,0.18);
    border: 1px solid rgba(255,255,255,0.35);
    border-radius: 8px;
    padding: 4px 12px;
    font-size: 11px;
    font-weight: 700;
    color: #fff;
    letter-spacing: 0.06em;
}

/* ── Section title bars ─────────────────────────────────────────────── */
.sec-bar {
    background: linear-gradient(90deg, #1e40af, #3B82F6);
    color: #fff;
    font-size: 12.5px;
    font-weight: 700;
    padding: 7px 14px;
    border-radius: 7px 7px 0 0;
    margin-bottom: 0;
    letter-spacing: 0.03em;
}

/* ── Dataframe tweaks ───────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border-radius: 8px !important;
    overflow: hidden !important;
}

/* ── Divider ────────────────────────────────────────────────────────── */
hr { border-color: #BFDBFE !important; margin: 8px 0 !important; }

/* ── Info/warning boxes ─────────────────────────────────────────────── */
.stAlert { border-radius: 10px !important; font-size: 12px !important; }

/* ── Expander ───────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: rgba(255,255,255,0.6) !important;
    border: 1px solid #BFDBFE !important;
    border-radius: 8px !important;
}

/* ── Radio ──────────────────────────────────────────────────────────── */
[data-testid="stRadio"] label { font-size: 12.5px !important; }

/* hide Streamlit top decoration */
#MainMenu, footer, [data-testid="stDecoration"] { display:none !important; }
</style>
""", unsafe_allow_html=True)

# MutationObserver — override emotion inline styles reliably
import streamlit.components.v1 as _components
_components.html("""
<script>
(function(){
  var BG  = 'linear-gradient(145deg,#EFF6FF 0%,#DBEAFE 100%)';
  var BDR = '1.5px solid #BFDBFE';
  function applyBg(root){
    var els=(root||document).querySelectorAll(
      '[data-testid="stContainerWithBorder"],[data-testid="stVerticalBlockBorderWrapper"]');
    els.forEach(function(el){
      var par=el.parentElement&&el.parentElement.closest('[data-testid="stContainerWithBorder"]');
      if(par){
        el.style.setProperty('background','rgba(255,255,255,0.68)','important');
        el.style.setProperty('border','1px dashed #93C5FD','important');
      } else {
        el.style.setProperty('background',BG,'important');
        el.style.setProperty('border',BDR,'important');
        el.style.setProperty('border-radius','14px','important');
        el.style.setProperty('box-shadow','0 2px 10px rgba(37,99,235,0.07)','important');
      }
    });
  }
  applyBg(document);
  var obs=new MutationObserver(function(m){m.forEach(function(x){if(x.addedNodes.length)applyBg(document);});});
  obs.observe(document.body,{childList:true,subtree:true});
})();
</script>
""", height=0, scrolling=False)

# =========================================================================
# 4. SESSION-STATE BOOTSTRAP
# =========================================================================
defaults = {
    "df_summary"           : pd.DataFrame(),
    "job_history"          : [],
    "uploaded_file_names"  : [],   # renamed from uploaded_stm_names
    "uploaded_file_paths"  : [],
    "pending_action"       : None,
    "pending_validation"   : None,
    "inline_status_html"   : "",   # shown inside upload box
    "btn_status"           : {
        "upload_summary": "idle",
        "run_all"       : "idle",
        "struct_val"    : "idle",
        "scd_val"       : "idle",
        "tc_gen"        : "idle",
        "dg_creation"   : "idle",
    },
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# =========================================================================
# 5. DATABRICKS API HELPERS
# =========================================================================

# ── Databricks SQL Catalog/Schema/Table Fetch Functions ──
@st.cache_data(ttl=30, show_spinner=False)
def fetch_catalogs():
    """Fetch all catalogs from Databricks"""
    if not DATABRICKS_SQL_AVAILABLE:
        st.error("❌ databricks-sql-connector is not installed. Add it to requirements.txt")
        return []
    try:
        with get_databricks_connection() as conn:
            cur = conn.cursor()
            cur.execute("SHOW CATALOGS")
            rows = cur.fetchall()
        return sorted([r[0] for r in rows])
    except Exception as e:
        st.error(f"Error fetching catalogs: {e}")
        return []


@st.cache_data(ttl=30, show_spinner=False)
def fetch_schemas(catalog: str):
    """Fetch all schemas for a given catalog"""
    if not DATABRICKS_SQL_AVAILABLE:
        st.error("❌ databricks-sql-connector is not installed. Add it to requirements.txt")
        return []
    try:
        with get_databricks_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SHOW SCHEMAS IN `{catalog}`")
            rows = cur.fetchall()
        return sorted([r[0] for r in rows])
    except Exception as e:
        st.error(f"Error fetching schemas for `{catalog}`: {e}")
        return []


@st.cache_data(ttl=30, show_spinner=False)
def fetch_tables(catalog: str, schema: str):
    """Fetch all tables for a given catalog.schema"""
    if not DATABRICKS_SQL_AVAILABLE:
        st.error("❌ databricks-sql-connector is not installed. Add it to requirements.txt")
        return pd.DataFrame()
    try:
        with get_databricks_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        if not df.empty:
            df.insert(0, "catalog", catalog)
        return df
    except Exception as e:
        st.error(f"Error fetching tables for `{catalog}`.`{schema}`: {e}")
        return pd.DataFrame()


# ── Original API Helpers ──
def _clean_file_name(fname: str) -> str:
    return fname.rsplit("/", 1)[-1].rsplit(".", 1)[0]


def upload_to_workspace(fname: str, data: bytes) -> tuple[bool, str]:
    target = f"{WORKSPACE_UPLOAD_DIR}/{fname}"
    body = {
        "path": target, "format": "AUTO", "overwrite": True,
        "content": base64.b64encode(data).decode(),
    }
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/workspace/import",
            headers=HEADERS, json=body, timeout=120,
        )
        return (True, target) if r.status_code == 200 else (False, f"{r.status_code}: {r.text[:400]}")
    except Exception as e:
        return False, str(e)


def trigger_job(job_id: int, params: dict) -> tuple[bool, int | str]:
    body = {"job_id": job_id, "notebook_params": params}
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers=HEADERS, json=body, timeout=30,
        )
        return (True, r.json()["run_id"]) if r.status_code == 200 else (False, f"{r.status_code}: {r.text[:400]}")
    except Exception as e:
        return False, str(e)


def get_run_details(run_id: int) -> dict:
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
        headers=HEADERS, params={"run_id": run_id}, timeout=30,
    )
    return r.json() if r.status_code == 200 else {}


def get_notebook_output(task_run_id: int) -> dict:
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
        headers=HEADERS, params={"run_id": task_run_id}, timeout=30,
    )
    return r.json() if r.status_code == 200 else {}


# =========================================================================
# 6. PHASE PLANS
# =========================================================================
PHASE_PLANS = {
    "Uploading & Generating Summary": [
        ("Uploading source files",             1.0),
        ("Staging source files to governed storage",      1.5),
        ("Parsing structure and metadata",             2.0),
        ("Extracting column inventory by layer",      1.5),
        ("Generating summary report",                   1.0),
    ],
    "Structure Validation": [
        ("Parsing mapping document metadata",       1.0),
        ("Resolving source and target artifacts",   1.0),
        ("Validating RAW source against CSV file",     2.0),
        ("Validating RAW target against Parquet file", 2.0),
        ("Validating STD_RAW source (Parquet+audit)",  2.0),
        ("Validating STD_RAW target (Databricks)",     2.0),
        ("Validating CURATED source",                  1.5),
        ("Validating CURATED target",                  1.5),
        ("Generating formatted validation report",          1.0),
        ("Rendering dashboard and delivering email",   1.0),
    ],
    "SCD Validation": [
        ("Parsing and resolving target tables",        1.0),
        ("Validating record counts",    1.5),
        ("Validating null constraints on key columns",             1.5),
        ("Performing aggregate validations",       2.0),
        ("Validating primary‑key uniqueness",          1.5),
        ("Performing column-level data validation",  2.5),
        ("Validating control fields",                        1.5),
        ("Validating SCD",                    2.0),
        ("Generating formatted validation output",           1.0),
    ],
    "Test Case Generator": [
        ("Parsing and extracting business rules",      1.0),
        ("Deriving test scenarios",          2.0),
        ("Generating test cases by category",  2.5),
        ("Formatting test-case workbook",              1.5),
        ("Finalizing and delivering artifacts",        1.0),
    ],
    "Run All Validation": [
        ("Preparing data for validation",         1.0),
        ("Executing structure validations",    3.0),
        ("Executing SCD validations",3.0),
        ("Executing data and control validations",  2.5),
        ("Generating test cases",                      2.0),
        ("Consolidating and distributing reports", 1.5),
    ],
}


# =========================================================================
# 7. PROGRESS TRACKER — renders inside a given slot
# =========================================================================
class ProgressTracker:
    def __init__(self, slot, kind: str, compact: bool = False):
        self.slot     = slot
        self.kind     = kind
        self.compact  = compact          # True → inline status-card style
        self.phases   = PHASE_PLANS.get(kind, [(kind, 1.0)])
        total_w       = sum(w for _, w in self.phases) or 1.0
        cum = 0.0
        self.thresholds = []
        for (_, w) in self.phases:
            cum += w / total_w * 100.0
            self.thresholds.append(cum)
        self.start_ts    = None
        self.expected_sec = 45.0

    def start(self):
        self.start_ts = time.time()
        self._render(pct=0, phase_idx=0)

    def _phase_from_pct(self, pct):
        for i, t in enumerate(self.thresholds):
            if pct <= t:
                return i
        return len(self.phases) - 1

    def tick(self):
        if self.start_ts is None:
            return
        pct = min(95.0, (time.time() - self.start_ts) / self.expected_sec * 95.0)
        self._render(pct=pct, phase_idx=self._phase_from_pct(pct))

    def done(self):
        self._render(pct=100.0, phase_idx=len(self.phases)-1, terminal="success")

    def fail(self, message=""):
        elapsed = time.time() - (self.start_ts or time.time())
        pct = min(95.0, elapsed / self.expected_sec * 95.0)
        self._render(pct=pct, phase_idx=self._phase_from_pct(pct),
                     terminal="failed", message=message)

    def clear(self):
        self.slot.empty()

    def _render(self, pct, phase_idx, terminal="", message=""):
        steps_html = ""
        for i, (label, _) in enumerate(self.phases):
            if terminal == "success" or i < phase_idx:
                cls  = "done";   icon = "✓"
            elif i == phase_idx:
                cls  = "active"; icon = "▶"
            else:
                cls  = "";       icon = "○"
            steps_html += f'<li class="{cls}">{icon}&nbsp;{label}</li>'

        if terminal == "success":
            phase_text = "✅ Completed successfully."
        elif terminal == "failed":
            phase_text = f"❌ Failed: {message or 'see history'}"
        elif phase_idx < len(self.phases):
            phase_text = f"<b>Now:</b> {self.phases[phase_idx][0]}"
        else:
            phase_text = ""

        pct_int = int(round(pct))
        h_icon  = "✅" if terminal == "success" else ("⚠️" if terminal == "failed" else "⏳")
        h_color = ("#16A34A" if terminal == "success"
                   else "#DC2626" if terminal == "failed" else "#1e40af")

        if self.compact:
            # Inline inside upload box
            html = f"""
<div class="status-card">
  <div class="sc-title">
    <span>{h_icon}</span>
    <span style="color:{h_color};">{self.kind}</span>
    <span style="margin-left:auto;font-size:10px;color:#64748b;">{pct_int}%</span>
  </div>
  <div class="sc-phase" style="font-size:11.5px;">{phase_text}</div>
  <div class="sc-bar-wrap">
    <div class="sc-bar-fill" style="width:{pct_int}%;"></div>
  </div>
  <ul class="sc-steps">{steps_html}</ul>
</div>"""
        else:
            html = f"""
<div class="progress-card">
  <div class="pc-title">
    <span style="color:{h_color};">{h_icon}&nbsp;{self.kind}</span>
    <span style="font-size:11px;color:#64748b;font-weight:500;">{pct_int}% complete</span>
  </div>
  <div class="pc-phase">{phase_text}</div>
  <div class="pc-bar-wrap">
    <div class="pc-bar-fill" style="width:{pct_int}%;"></div>
  </div>
  <ul class="pc-steps">{steps_html}</ul>
</div>"""

        self.slot.markdown(html, unsafe_allow_html=True)


def poll_until_done(run_id, tracker=None, label=""):
    while True:
        info  = get_run_details(run_id)
        state = info.get("state", {})
        lc    = state.get("life_cycle_state", "UNKNOWN")
        rs    = state.get("result_state")
        tasks = info.get("tasks", [])
        last_task_run_id = tasks[-1]["run_id"] if tasks else run_id
        if tracker is not None:
            tracker.tick()
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return (rs or lc), last_task_run_id
        time.sleep(3)


def extract_summary_list(run_id):
    info = get_run_details(run_id)
    tasks = info.get("tasks", []) or [{"run_id": run_id}]
    for t in reversed(tasks):
        try:
            out = get_notebook_output(t["run_id"])
            raw = out.get("notebook_output", {}).get("result")
            if not raw:
                continue
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return parsed
        except Exception:
            continue
    return None


def log_history(category, job_id, run_id, status, start_ts=None, end_ts=None):
    _end   = end_ts   if end_ts   is not None else time.time()
    _start = start_ts if start_ts is not None else _end
    _dur   = max(0.0, _end - _start)
    if _dur < 60:
        dur_txt = f"{_dur:.1f}s"
    elif _dur < 3600:
        m, s = divmod(int(_dur), 60); dur_txt = f"{m}m {s}s"
    else:
        h, r = divmod(int(_dur), 3600); m, s = divmod(r, 60); dur_txt = f"{h}h {m}m {s}s"
    st.session_state.job_history.append({
        "Category"  : category,
        "Job ID"    : job_id,
        "Run ID"    : run_id,
        "Status"    : status,
        "Start Time": datetime.fromtimestamp(_start).strftime("%Y-%m-%d %H:%M:%S"),
        "End Time"  : datetime.fromtimestamp(_end).strftime("%Y-%m-%d %H:%M:%S"),
        "Duration"  : dur_txt,
    })


# =========================================================================
# 8. SUMMARY FILTERING
# =========================================================================
IGNORED_SHEETS     = {"Version.History", "version.history"}
IGNORED_CATEGORIES = {"PII Present", "Temporal Columns", "Nullability", "Extraction Mode"}


def _apply_summary_filters(rows):
    if not rows:
        return rows
    sheet_keys = [k for k in rows[0] if k.lower().replace("_"," ").strip() in ("sheet","sheet name")]
    cat_keys   = [k for k in rows[0] if k.lower().strip() == "category"]
    filtered = []
    for r in rows:
        sv = next((str(r.get(k,"")).strip() for k in sheet_keys), "")
        cv = next((str(r.get(k,"")).strip() for k in cat_keys),   "")
        if sv and sv.lower() in {s.lower() for s in IGNORED_SHEETS}: continue
        if cv and cv in IGNORED_CATEGORIES: continue
        filtered.append(r)
    return filtered


# =========================================================================
# 9. BUTTON HELPERS
# =========================================================================
def styled_button(label, key, status_key, disabled=False, use_container_width=True):
    status = st.session_state.btn_status.get(status_key, "idle")
    cls = {"idle":"btn-idle","running":"btn-running","success":"btn-success","failed":"btn-failed"}.get(status,"btn-idle")
    st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
    clicked = st.button(label, key=key, disabled=disabled, use_container_width=use_container_width)
    st.markdown("</div>", unsafe_allow_html=True)
    return clicked


def compact_button(label, key, status_key, disabled=False, use_container_width=True):
    status = st.session_state.btn_status.get(status_key, "idle")
    cls = {
        "idle"   : "btn-compact btn-compact-idle",
        "running": "btn-compact btn-compact-running",
        "success": "btn-compact btn-compact-success",
        "failed" : "btn-compact btn-compact-failed",
    }.get(status, "btn-compact btn-compact-idle")
    st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
    clicked = st.button(label, key=key, disabled=disabled, use_container_width=use_container_width)
    st.markdown("</div>", unsafe_allow_html=True)
    return clicked


# =========================================================================
# 10. HEADER STRIP (replaces st.title + gap)
# =========================================================================
st.markdown("""
<div class="iq-header">
  <div>
    <div class="iq-title">⚡ IngestIQ™ &nbsp;·&nbsp; AI-Powered QA for Data Ingestion Pipeline</div>
    <div class="iq-sub">Upload files &nbsp;→&nbsp; Generate summary &nbsp;→&nbsp; Run validations</div>
  </div>
  <div class="iq-badge">TCS · IngestIQ™</div>
</div>
""", unsafe_allow_html=True)
PANEL_HEIGHT = 400
left, right = st.columns([1, 1], gap="medium")

# =====================================================================
# 11. LEFT PANEL — Upload + inline status
# =====================================================================
with left:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("📁 Upload Files")

        uploaded = st.file_uploader(
            "Select file(s) (.xlsx)",
            accept_multiple_files=True, type=["xlsx"],
            label_visibility="collapsed",
        )

        busy      = st.session_state.pending_action is not None
        can_upload = bool(uploaded) and not busy

        if uploaded:
            names = [f.name for f in uploaded]
            st.caption(f"📎 {', '.join(names)}")

        up_clicked = styled_button(
            "⬆  Upload & Generate Summary",
            key="upload_summary_btn",
            status_key="upload_summary",
            disabled=not can_upload,
        )

        # ── Inline status lives here ──────────────────────────────────
        inline_status_slot = st.empty()
        if st.session_state.inline_status_html:
            inline_status_slot.markdown(
                st.session_state.inline_status_html, unsafe_allow_html=True
            )

        with st.expander("📋 Uploaded files", expanded=False):
            if st.session_state.uploaded_file_names:
                for fn in st.session_state.uploaded_file_names:
                    st.markdown(f"• `{fn}`")
                if st.button("🗑 Clear list", key="clear_upl"):
                    st.session_state.uploaded_file_names  = []
                    st.session_state.uploaded_file_paths  = []
                    st.session_state.inline_status_html   = ""
                    st.rerun()
            else:
                st.caption("No files uploaded yet.")

# =====================================================================
# 12. RIGHT PANEL — Summary Viewer
# =====================================================================
with right:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("📊 Summary Viewer")

        if st.session_state.df_summary.empty:
            st.caption("Upload file(s) and click **Upload & Generate Summary** to populate this panel.")
        else:
            df_sv = st.session_state.df_summary

            def _find_col(df, *candidates):
                wanted = {c.lower().replace("_"," ").strip() for c in candidates}
                for col in df.columns:
                    if col.lower().replace("_"," ").strip() in wanted:
                        return col
                return None

            stm_col   = _find_col(df_sv, "STM File","STM","STM Name","File")
            sheet_col = _find_col(df_sv, "Sheet","Sheet Name")
            display_cols = [c for c in df_sv.columns if c not in (stm_col, sheet_col)]

            SHEET_FLOW = {
                "raw"    : "Source → Raw",
                "std_raw": "Raw → Std.Raw",
                "curated": "Std.Raw → Curated",
            }

            def _sheet_label(s):
                sl = s.lower()
                if "std_raw" in sl: return SHEET_FLOW["std_raw"]
                if "raw" in sl and "std" not in sl: return SHEET_FLOW["raw"]
                if "curated" in sl: return SHEET_FLOW["curated"]
                return s

            if not sheet_col and not stm_col:
                st.dataframe(df_sv, use_container_width=True, hide_index=True)
            else:
                group_keys, groups = [], {}
                for _, row in df_sv.iterrows():
                    stm_v   = str(row[stm_col]).strip()   if stm_col   else ""
                    sheet_v = str(row[sheet_col]).strip() if sheet_col else ""
                    key = (stm_v, sheet_v)
                    if key not in groups:
                        groups[key] = []; group_keys.append(key)
                    groups[key].append(row)

                for i, (stm_v, sheet_v) in enumerate(group_keys):
                    flow = _sheet_label(sheet_v) if sheet_v else ""
                    title = (f"📄 {stm_v}  ·  {flow}" if stm_v and flow
                             else f"📄 {flow or stm_v or 'Summary'}")
                    st.markdown(
                        f"<div class='sec-bar' style='margin-top:{'10px' if i>0 else '0'};'>"
                        f"{title}</div>",
                        unsafe_allow_html=True,
                    )
                    sub_df = pd.DataFrame(groups[(stm_v, sheet_v)])[display_cols]
                    st.dataframe(sub_df, use_container_width=True, hide_index=True)


# =====================================================================
# 13. QUALITY ASSURANCE PANEL
# =====================================================================
with st.container(border=True):
    st.subheader("🔬 Quality Assurance")

    has_files     = bool(st.session_state.uploaded_file_names)
    busy          = st.session_state.pending_action is not None
    common_disable = (not has_files) or busy

    if not has_files:
        st.info("ℹ️ Upload files first to enable validation buttons.")

    qa1, qa2, qa3, qa4 = st.columns(4)
    with qa1:
        run_all_clicked = compact_button(
            "▶ Run All Validation", key="run_all_btn",
            status_key="run_all", disabled=common_disable,
        )
    with qa2:
        struct_clicked = compact_button(
            "🔍 Structure Validation", key="struct_val_btn",
            status_key="struct_val", disabled=common_disable,
        )
    with qa3:
        scd_clicked = compact_button(
            "🔁 SCD Validation", key="scd_val_btn",
            status_key="scd_val", disabled=common_disable,
        )
    with qa4:
        tc_clicked = compact_button(
            "🧬 Test Case Generator", key="tc_gen_btn",
            status_key="tc_gen", disabled=common_disable,
        )

    # ── INITIAL / DELTA picker ──────────────────────────────────────
    if run_all_clicked: st.session_state.pending_validation = "run_all"
    if scd_clicked:     st.session_state.pending_validation = "scd_val"

    if st.session_state.pending_validation in ("run_all", "scd_val") and not busy:
        with st.container(border=True):
            cat = ("Run All Validation"
                   if st.session_state.pending_validation == "run_all"
                   else "SCD Validation")
            st.markdown(f"**Choose Validation Type for {cat}**")
            v_type = st.radio(
                "Type", ["INITIAL","DELTA"],
                horizontal=True, key="v_type_radio",
                label_visibility="collapsed",
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Confirm & Run", key="confirm_vtype", use_container_width=True):
                    file_csv = ",".join(st.session_state.uploaded_file_names)
                    params   = {"STM_FILE_NAMES": file_csv, "VALIDATION_TYPE": v_type}
                    if st.session_state.pending_validation == "run_all":
                        st.session_state.btn_status["run_all"] = "running"
                        st.session_state.pending_action = {
                            "kind":"validation","category":"Run All Validation",
                            "job_id": JOB_IDS["Run All Validation"],
                            "params": params, "btn_key":"run_all",
                        }
                    else:
                        st.session_state.btn_status["scd_val"] = "running"
                        st.session_state.pending_action = {
                            "kind":"validation","category":"SCD Validation",
                            "job_id": JOB_IDS["SCD Validation"],
                            "params": params, "btn_key":"scd_val",
                        }
                    st.session_state.pending_validation = None
                    st.rerun()
            with c2:
                if st.button("✖ Cancel", key="cancel_vtype", use_container_width=True):
                    st.session_state.pending_validation = None
                    st.rerun()

    # ── Structure Validation ────────────────────────────────────────
    if struct_clicked and not busy:
        file_csv = ",".join(st.session_state.uploaded_file_names)
        st.session_state.btn_status["struct_val"] = "running"
        st.session_state.pending_action = {
            "kind":"validation","category":"Structure Validation",
            "job_id": JOB_IDS["Structure Validation"],
            "params": {"STM_FILE_NAMES": file_csv}, "btn_key":"struct_val",
        }
        st.rerun()

    # ── Test Case Generator ─────────────────────────────────────────
    if tc_clicked and not busy:
        file_csv = ",".join(st.session_state.uploaded_file_names)
        st.session_state.btn_status["tc_gen"] = "running"
        st.session_state.pending_action = {
            "kind":"validation","category":"Test Case Generator",
            "job_id": JOB_IDS["Test Case Generator"],
            "params": {"STM_FILE_NAMES": file_csv}, "btn_key":"tc_gen",
        }
        st.rerun()

    # ── QA progress slot (shown inside QA panel) ────────────────────
    qa_tracker_slot = st.empty()


# =====================================================================
# 13B. DG DOCUMENT CREATION PANEL
# =====================================================================
with st.container(border=True):
    # Header with Refresh button in top-right corner
    dg_header_col1, dg_header_col2 = st.columns([5, 1])
    with dg_header_col1:
        st.subheader("📝 DG Document Creation")
        st.caption("Generate Data Governance documentation for your tables")
    with dg_header_col2:
        if st.button("🔄", key="dg_refresh_btn", use_container_width=True, help="Refresh catalog/schema/table lists"):
            st.cache_data.clear()
            st.rerun()
    
    dg_busy = st.session_state.pending_action is not None
    
    # ── 1. Select Catalog (single select) ──
    st.markdown("##### 1️⃣ Select Catalog")
    catalogs = fetch_catalogs()
    
    if not catalogs:
        st.warning("⚠️ No catalogs found. Click refresh or check your Databricks connection.")
        st.stop()
    
    selected_catalog = st.selectbox(
        "Catalog",
        options=catalogs,
        index=0,
        key="dg_catalog_select",
        disabled=dg_busy,
        label_visibility="collapsed"
    )
    
    # ── 2. Select Schemas (multi-select) ──
    st.markdown("##### 2️⃣ Select Schema(s)")
    schemas = fetch_schemas(selected_catalog) if selected_catalog else []
    
    selected_schemas = st.multiselect(
        "Schemas (one or more)",
        options=schemas,
        default=[],
        key="dg_schema_select",
        placeholder="Choose one or more schemas",
        disabled=dg_busy,
        label_visibility="collapsed"
    )
    
    # ── 3. Select Tables (multi-select) ──
    st.markdown("##### 3️⃣ Select Table(s)")
    
    all_tables_df = pd.DataFrame()
    table_options = []
    
    if selected_schemas:
        frames = []
        for sch in selected_schemas:
            df = fetch_tables(selected_catalog, sch)
            if not df.empty:
                frames.append(df)
        if frames:
            all_tables_df = pd.concat(frames, ignore_index=True)
            name_col = "tableName" if "tableName" in all_tables_df.columns else all_tables_df.columns[2]
            db_col = "database" if "database" in all_tables_df.columns else all_tables_df.columns[1]
            table_options = sorted({
                f"{row[db_col]}.{row[name_col]}"
                for _, row in all_tables_df.iterrows()
            })
    
    selected_tables = st.multiselect(
        "Tables (one or more)",
        options=table_options,
        key="dg_table_select",
        placeholder="Choose one or more tables",
        disabled=dg_busy,
        label_visibility="collapsed"
    )
    
    # ── 4. Additional Parameters ──
    st.markdown("##### 4️⃣ Additional Parameters")
    table_description = st.text_area(
     "TABLE DESCRIPTION",
     key="dg_table_desc",
     height=200,                 
     placeholder="Enter detailed table description here...",
     disabled=dg_busy,
    )

 
    database_storage_name = st.text_input(
     "DATABASE STORAGE NAME",
     key="dg_database",
     placeholder="Enter database/storage name",
     disabled=dg_busy,
    )
    
    # ── Validation and submission ──
    dg_all_filled = all([
        selected_catalog,
        selected_schemas,
        selected_tables,
        table_description.strip(),
        database_storage_name.strip()
    ])
    
    # Button
    dg_clicked = compact_button(
        "🚀 Create DG Document", key="dg_creation_btn",
        status_key="dg_creation", disabled=(not dg_all_filled) or dg_busy,
    )
    
    if not dg_all_filled and not dg_busy:
        st.info("ℹ️ Please select catalog, schema(s), table(s) and fill in all additional parameters.")
    
    # ── DG Creation trigger ─────────────────────────────────────────
    if dg_clicked and not dg_busy:
        # Process multiple schemas and tables
        schema_csv = ",".join(selected_schemas)
        table_csv = ",".join(selected_tables)
        
        st.session_state.btn_status["dg_creation"] = "running"
        st.session_state.pending_action = {
            "kind": "dg_creation",
            "category": "DG Document Creation",
            "job_id": DG_CREATION_JOB_ID,
            "params": {
                "CATALOG": selected_catalog,
                "SCHEMA": schema_csv,
                "TABLE_NAME": table_csv,
                "TABLE_DESCRIPTION": table_description.strip(),
                "PLATFORM_SYSTEM_NAME": platform_system_name.strip(),
                "DATABASE_STORAGE_NAME": database_storage_name.strip(),
            },
            "btn_key": "dg_creation",
        }
        st.rerun()
    
    # ── DG progress slot (shown inside DG panel) ────────────────────
    dg_tracker_slot = st.empty()


# =====================================================================
# 14. UPLOAD — trigger
# =====================================================================
if up_clicked and can_upload:
    files_snapshot = [{"name": f.name, "data": f.getvalue()} for f in uploaded]
    st.session_state.btn_status["upload_summary"] = "running"
    st.session_state.inline_status_html = ""
    st.session_state.pending_action = {
        "kind"    : "upload_summary",
        "btn_key" : "upload_summary",
        "files"   : files_snapshot,
    }
    st.rerun()


# =====================================================================
# 15. PENDING-ACTION EXECUTOR
# =====================================================================
if st.session_state.pending_action is not None:
    action  = st.session_state.pending_action
    btn_key = action["btn_key"]

    if action["kind"] == "upload_summary":
        # Progress shown inline inside the upload box
        tracker = ProgressTracker(inline_status_slot, "Uploading & Generating Summary", compact=True)
    elif action["kind"] == "dg_creation":
        # Progress shown in the DG tracker slot
        tracker = ProgressTracker(dg_tracker_slot, action["category"], compact=False)
    else:
        # Progress shown in the QA tracker slot
        tracker = ProgressTracker(qa_tracker_slot, action["category"], compact=False)

    tracker.start()

    try:
        if action["kind"] == "upload_summary":
            files = action["files"]
            ws_paths, file_names, errs = [], [], []
            for f in files:
                ok, detail = upload_to_workspace(f["name"], f["data"])
                if ok:
                    ws_paths.append(detail)
                    file_names.append(_clean_file_name(f["name"]))
                else:
                    errs.append(f"{f['name']} → {detail}")
                tracker.tick()

            if not ws_paths:
                raise Exception(f"Upload failed: {errs}")

            _copy_start = time.time()
            ok, copy_run = trigger_job(
                FILE_COPY_JOB_ID,
                {"workspace_file_paths": ",".join(ws_paths)},
            )
            if not ok:
                raise Exception(f"File-Copy trigger failed: {copy_run}")

            copy_state, _ = poll_until_done(copy_run, tracker, "File-Copy")
            log_history("File Copy", FILE_COPY_JOB_ID, copy_run, copy_state,
                        start_ts=_copy_start, end_ts=time.time())
            if copy_state != "SUCCESS":
                raise Exception(f"File-Copy ended with {copy_state}")

            st.session_state.uploaded_file_names = sorted(set(
                st.session_state.uploaded_file_names + file_names))
            st.session_state.uploaded_file_paths = sorted(set(
                st.session_state.uploaded_file_paths + ws_paths))

            _sum_start = time.time()
            ok, sum_run = trigger_job(
                SUMMARY_JOB_ID,
                {"stm_file_names": ",".join(file_names)},
            )
            if not ok:
                raise Exception(f"Summary trigger failed: {sum_run}")

            sum_state, _ = poll_until_done(sum_run, tracker, "Summary")
            log_history("Summary", SUMMARY_JOB_ID, sum_run, sum_state,
                        start_ts=_sum_start, end_ts=time.time())
            if sum_state != "SUCCESS":
                raise Exception(f"Summary ended with {sum_state}")

            data = extract_summary_list(sum_run)
            if data:
                data = _apply_summary_filters(data)
                if data:
                    st.session_state.df_summary = pd.DataFrame(data)

            st.session_state.btn_status[btn_key] = "success"
            tracker.done()
            # Persist the final "done" HTML in session so it survives rerun
            # (we re-render it manually on next load via inline_status_slot)
            st.session_state.inline_status_html = ""   # clear after success

        elif action["kind"] == "validation":
            category = action["category"]
            job_id   = action["job_id"]
            params   = action["params"]

            _val_start = time.time()
            ok, run_id = trigger_job(job_id, params)
            if not ok:
                raise Exception(f"Trigger failed: {run_id}")

            state, _ = poll_until_done(run_id, tracker, category)
            log_history(category, job_id, run_id, state,
                        start_ts=_val_start, end_ts=time.time())
            if state != "SUCCESS":
                raise Exception(f"{category} ended with {state}")

            st.session_state.btn_status[btn_key] = "success"
            tracker.done()

        elif action["kind"] == "dg_creation":
            category = action["category"]
            job_id   = action["job_id"]
            params   = action["params"]

            _dg_start = time.time()
            ok, run_id = trigger_job(job_id, params)
            if not ok:
                raise Exception(f"Trigger failed: {run_id}")

            state, _ = poll_until_done(run_id, tracker, category)
            log_history(category, job_id, run_id, state,
                        start_ts=_dg_start, end_ts=time.time())
            if state != "SUCCESS":
                raise Exception(f"{category} ended with {state}")

            st.session_state.btn_status[btn_key] = "success"
            tracker.done()

    except Exception as e:
        st.session_state.btn_status[btn_key] = "failed"
        tracker.fail(str(e)[:120])
        log_history(action.get("category", action["kind"]), 0, "-", f"ERROR: {e}",
                    start_ts=tracker.start_ts, end_ts=time.time())

    finally:
        st.session_state.pending_action = None
        time.sleep(1.5)
        tracker.clear()
        st.rerun()


# =====================================================================
# 16. JOB EXECUTION HISTORY
# =====================================================================
st.divider()
with st.container(border=True):
    st.subheader("📋 Job Execution History")
    if st.session_state.job_history:
        hist = pd.DataFrame(st.session_state.job_history)

        def _color_status(val):
            colors = {
                "SUCCESS": "color:#16A34A;font-weight:700",
                "FAILED" : "color:#DC2626;font-weight:700",
            }
            return colors.get(val.upper() if isinstance(val, str) else "", "")

        st.dataframe(hist, use_container_width=True, hide_index=True)
    else:
        st.caption("No jobs run yet.")
