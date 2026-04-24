"""
========================================================================
 IngestIQ™ AI QA Portal  (v7 — premium enterprise UI, TCS logo,
                            glassmorphism, advanced animations)
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

# =========================================================================
# 1. CONFIG
# =========================================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN           = "dapi180370eb25ac521baee3f96924db98e9"
WORKSPACE_UPLOAD_DIR = "/Shared/qa_uploads"
FILE_COPY_JOB_ID = 1095682687953224
SUMMARY_JOB_ID   = 29471425720129
JOB_IDS = {
    "Run All Validation"  : 566631342323223,
    "Structure Validation": 190540510295693,
    "SCD Validation"      : 909635921592434,
    "Test Case Generator" : 160480032307967,
}
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# =========================================================================
# 2. PAGE CONFIG
# =========================================================================
st.set_page_config(page_title="IngestIQ™ AI QA", layout="wide",
                   initial_sidebar_state="collapsed")

# =========================================================================
# 3. TCS LOGO (inline SVG — no external file needed)
# =========================================================================
TCS_LOGO_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 44" width="110" height="40">
  <rect width="120" height="44" rx="5" fill="none"/>
  <!-- TCS wordmark -->
  <text x="4" y="28" font-family="Arial Black,Arial,sans-serif" font-weight="900"
        font-size="26" fill="#ffffff" letter-spacing="-1">tcs</text>
  <!-- TATA CONSULTANCY SERVICES -->
  <text x="46" y="19" font-family="Arial,sans-serif" font-weight="700"
        font-size="6.5" fill="rgba(255,255,255,0.9)" letter-spacing="0.5">TATA</text>
  <text x="46" y="28" font-family="Arial,sans-serif" font-weight="700"
        font-size="6.5" fill="rgba(255,255,255,0.9)" letter-spacing="0.3">CONSULTANCY</text>
  <text x="46" y="37" font-family="Arial,sans-serif" font-weight="700"
        font-size="6.5" fill="rgba(255,255,255,0.9)" letter-spacing="0.5">SERVICES</text>
</svg>
"""
TCS_LOGO_B64 = base64.b64encode(TCS_LOGO_SVG.encode()).decode()

# =========================================================================
# 4. CSS — Premium Enterprise Design
# =========================================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── RESET ─────────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* ── Kill all Streamlit chrome & gaps ──────────────────────────────── */
.block-container {
    padding-top: 0 !important;
    padding-bottom: 0.5rem !important;
    max-width: 100% !important;
    padding-left: 1.2rem !important;
    padding-right: 1.2rem !important;
}
header[data-testid="stHeader"],
[data-testid="stDecoration"],
#MainMenu, footer { display: none !important; }
.stApp { background: #060D1F !important; }

/* ── Page background ────────────────────────────────────────────────── */
[data-testid="stAppViewContainer"] {
    background: linear-gradient(160deg, #060D1F 0%, #0A1628 40%, #0D1F3C 100%);
    min-height: 100vh;
}
[data-testid="stMain"] { background: transparent !important; }

/* ── HEADER STRIP ───────────────────────────────────────────────────── */
.iq-header {
    background: linear-gradient(135deg,
        #0A1628 0%, #0F2040 30%, #1a3560 60%, #0E2848 100%);
    border: 1px solid rgba(99,179,237,0.18);
    border-radius: 16px;
    padding: 0;
    margin-bottom: 10px;
    overflow: hidden;
    position: relative;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5), 0 1px 0 rgba(255,255,255,0.05) inset;
}
.iq-header::before {
    content: '';
    position: absolute; inset: 0;
    background: repeating-linear-gradient(
        90deg,
        transparent, transparent 60px,
        rgba(59,130,246,0.03) 60px, rgba(59,130,246,0.03) 61px
    );
    pointer-events: none;
}
.iq-header-inner {
    display: flex; align-items: center;
    justify-content: space-between;
    padding: 16px 26px;
    position: relative; z-index: 1;
}
.iq-title-group { display: flex; flex-direction: column; gap: 3px; }
.iq-eyebrow {
    font-size: 9px; font-weight: 700; letter-spacing: 0.2em;
    color: rgba(96,165,250,0.8); text-transform: uppercase;
}
.iq-title {
    font-size: 1.45rem; font-weight: 800; color: #fff;
    letter-spacing: -0.03em; line-height: 1.1;
}
.iq-title span { color: #60A5FA; }
.iq-subtitle {
    font-size: 0.72rem; color: rgba(255,255,255,0.45);
    font-weight: 400; margin-top: 1px;
}
.iq-header-right {
    display: flex; align-items: center; gap: 18px;
}
.iq-status-pill {
    background: rgba(16,185,129,0.12);
    border: 1px solid rgba(16,185,129,0.3);
    border-radius: 999px; padding: 5px 14px;
    display: flex; align-items: center; gap: 6px;
    font-size: 10.5px; font-weight: 600; color: #34D399;
    letter-spacing: 0.05em;
}
.iq-status-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: #34D399;
    box-shadow: 0 0 8px #34D399;
    animation: blink 2s ease-in-out infinite;
}
@keyframes blink {
    0%,100% { opacity:1; box-shadow: 0 0 8px #34D399; }
    50%      { opacity:.4; box-shadow: 0 0 3px #34D399; }
}
.iq-tcs-logo {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 10px;
    padding: 7px 14px;
    display: flex; align-items: center;
}
.iq-divider {
    width: 1px; height: 36px;
    background: rgba(255,255,255,0.1);
}

/* ── GLASS CARD — base ──────────────────────────────────────────────── */
[data-testid="stContainerWithBorder"],
[data-testid="stVerticalBlockBorderWrapper"] {
    background: rgba(15,32,64,0.65) !important;
    border: 1px solid rgba(99,179,237,0.14) !important;
    border-radius: 16px !important;
    box-shadow: 0 4px 24px rgba(0,0,0,0.35),
                0 1px 0 rgba(255,255,255,0.04) inset !important;
    backdrop-filter: blur(12px) !important;
}
/* nested */
[data-testid="stContainerWithBorder"] [data-testid="stContainerWithBorder"] {
    background: rgba(30,58,100,0.4) !important;
    border: 1px solid rgba(99,179,237,0.12) !important;
    border-radius: 10px !important;
    box-shadow: none !important;
}

/* ── TYPOGRAPHY ─────────────────────────────────────────────────────── */
h3 {
    color: #93C5FD !important;
    font-size: 0.88rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    margin-bottom: 10px !important;
}
.stCaption p { color: rgba(148,163,184,0.7) !important; font-size: 0.75rem !important; }
p, li, label { color: #CBD5E1 !important; }

/* ── FILE UPLOADER ──────────────────────────────────────────────────── */
[data-testid="stFileUploader"] {
    background: rgba(30,58,100,0.3) !important;
    border: 2px dashed rgba(96,165,250,0.3) !important;
    border-radius: 12px !important;
    transition: border-color 0.2s ease !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: rgba(96,165,250,0.6) !important;
}
[data-testid="stFileUploader"] p,
[data-testid="stFileUploader"] small,
[data-testid="stFileUploader"] span { color: rgba(148,163,184,0.8) !important; }

/* ── MAIN ACTION BUTTON (Upload & Generate) ─────────────────────────── */
.btn-idle button {
    background: linear-gradient(135deg, #1D4ED8, #2563EB) !important;
    color: #fff !important;
    border: 1px solid rgba(96,165,250,0.3) !important;
    box-shadow: 0 4px 15px rgba(37,99,235,0.35), 0 1px 0 rgba(255,255,255,0.1) inset !important;
    border-radius: 10px !important; font-weight: 700 !important; letter-spacing: 0.03em !important;
    transition: all 0.2s ease !important;
}
.btn-idle button:hover { transform: translateY(-1px) !important;
    box-shadow: 0 6px 20px rgba(37,99,235,0.5) !important; }
.btn-running button {
    background: linear-gradient(135deg, #0369A1, #0EA5E9) !important;
    color: #fff !important; border-radius: 10px !important; font-weight: 700 !important;
    animation: pulse-glow 1.4s ease-in-out infinite !important;
}
.btn-success button {
    background: linear-gradient(135deg, #065F46, #059669) !important;
    color: #fff !important; border-radius: 10px !important; font-weight: 700 !important;
    border: 1px solid rgba(52,211,153,0.25) !important;
    box-shadow: 0 4px 15px rgba(5,150,105,0.3) !important;
}
.btn-failed button {
    background: linear-gradient(135deg, #7F1D1D, #DC2626) !important;
    color: #fff !important; border-radius: 10px !important; font-weight: 700 !important;
}
@keyframes pulse-glow {
    0%,100% { opacity:1; box-shadow: 0 4px 20px rgba(14,165,233,0.4); }
    50%      { opacity:.85; box-shadow: 0 4px 30px rgba(14,165,233,0.7); }
}

/* ── COMPACT QA BUTTONS ─────────────────────────────────────────────── */
.btn-compact button {
    padding: 6px 14px !important; font-size: 11px !important; font-weight: 700 !important;
    min-height: 38px !important; height: 38px !important; border-radius: 9px !important;
    letter-spacing: 0.05em !important; border: none !important; width: 100% !important;
    transition: all 0.18s ease !important; position: relative !important; overflow: hidden !important;
}
.btn-compact button::after {
    content: ''; position: absolute; inset: 0;
    background: rgba(255,255,255,0);
    transition: background 0.15s ease;
}
.btn-compact button:hover::after { background: rgba(255,255,255,0.06) !important; }
.btn-compact button:hover { transform: translateY(-1px) !important; }
.btn-compact-idle button {
    background: linear-gradient(135deg,#1e3a8a,#2563EB) !important;
    color:#fff !important;
    box-shadow: 0 3px 12px rgba(37,99,235,0.35) !important;
}
.btn-compact-running button {
    background: linear-gradient(135deg,#075985,#0EA5E9) !important;
    color:#fff !important; animation: pulse-glow 1.4s ease-in-out infinite !important;
}
.btn-compact-success button {
    background: linear-gradient(135deg,#064E3B,#059669) !important;
    color:#fff !important; box-shadow: 0 3px 12px rgba(5,150,105,0.35) !important;
}
.btn-compact-failed button {
    background: linear-gradient(135deg,#7F1D1D,#DC2626) !important;
    color:#fff !important; box-shadow: 0 3px 12px rgba(220,38,38,0.35) !important;
}
.btn-compact { margin-bottom:0 !important; }
.btn-compact > div { margin-bottom:0 !important; }

/* ── STATUS CARD (inline in upload box) ─────────────────────────────── */
.status-card {
    background: rgba(15,32,64,0.9);
    border: 1px solid rgba(99,179,237,0.2);
    border-radius: 12px;
    padding: 14px 16px;
    margin-top: 10px;
    font-family: 'Inter', sans-serif;
    position: relative; overflow: hidden;
}
.status-card::before {
    content: ''; position: absolute; top:0; left:0; right:0; height:2px;
    background: linear-gradient(90deg, #2563EB, #60A5FA, #2563EB);
    background-size: 200% 100%;
    animation: shimmer 2s linear infinite;
}
@keyframes shimmer { 0%{background-position:200% 0;} 100%{background-position:-200% 0;} }
.status-card .sc-title {
    font-size: 11.5px; font-weight: 700; color: #93C5FD;
    margin-bottom: 8px; display: flex; align-items: center; gap: 7px;
    letter-spacing: 0.04em; text-transform: uppercase;
}
.status-card .sc-phase { font-size: 11px; color: #94A3B8; margin-bottom: 8px; }
.status-card .sc-phase b { color: #60A5FA; }
.status-card .sc-bar-wrap {
    background: rgba(30,58,100,0.8); border-radius: 999px; overflow:hidden; height:6px; margin-bottom: 8px;
}
.status-card .sc-bar-fill {
    background: linear-gradient(90deg,#3B82F6,#60A5FA);
    height:100%; border-radius:999px; transition: width 0.5s ease;
}
.status-card ul.sc-steps {
    list-style: none; padding:0; margin:0; font-size: 10.5px; display: flex; flex-direction: column; gap: 2px;
}
.status-card ul.sc-steps li { color: rgba(148,163,184,0.5); display:flex; align-items:center; gap:6px; }
.status-card ul.sc-steps li.done   { color: #34D399; }
.status-card ul.sc-steps li.active { color: #60A5FA; font-weight: 700; }
.sc-step-dot {
    width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0;
    background: currentColor;
}

/* ── PROGRESS CARD (QA validation) ─────────────────────────────────── */
.progress-card {
    background: rgba(15,32,64,0.92);
    border: 1px solid rgba(99,179,237,0.2);
    border-radius: 14px; padding: 18px 22px;
    font-family: 'Inter', sans-serif; color: #CBD5E1;
    position: relative; overflow: hidden;
    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
}
.progress-card::before {
    content: ''; position: absolute; top:0; left:0; right:0; height:3px;
    background: linear-gradient(90deg, #1D4ED8, #3B82F6, #60A5FA, #3B82F6, #1D4ED8);
    background-size: 200% 100%;
    animation: shimmer 2s linear infinite;
}
.progress-card .pc-title {
    font-size: 13px; font-weight: 700; color: #93C5FD;
    margin-bottom: 10px; display:flex; align-items:center; justify-content:space-between;
    text-transform: uppercase; letter-spacing: 0.06em;
}
.progress-card .pc-phase { font-size: 12px; color: #94A3B8; margin-bottom: 8px; }
.progress-card .pc-phase b { color: #60A5FA; }
.progress-card .pc-bar-wrap {
    background: rgba(30,58,100,0.8); border-radius:999px; overflow:hidden; height:8px; margin-bottom:5px;
}
.progress-card .pc-bar-fill {
    background: linear-gradient(90deg,#1D4ED8,#3B82F6,#60A5FA);
    height:100%; border-radius:999px; transition: width 0.6s ease;
}
.progress-card .pc-pct {
    font-size: 11px; color: #64748B; text-align:right; font-weight:700; margin-bottom:10px;
}
.progress-card ul.pc-steps {
    list-style:none; padding:0; margin:0; display:flex; flex-direction:column; gap:5px; font-size:11.5px;
}
.progress-card ul.pc-steps li {
    display:flex; align-items:center; gap:8px; color:rgba(148,163,184,0.45);
    padding: 4px 10px; border-radius: 6px;
}
.progress-card ul.pc-steps li.done   {
    color:#34D399;
    background: rgba(52,211,153,0.05); border: 1px solid rgba(52,211,153,0.1);
}
.progress-card ul.pc-steps li.active {
    color:#60A5FA; font-weight:700;
    background: rgba(96,165,250,0.08); border: 1px solid rgba(96,165,250,0.2);
}
.pc-step-icon { font-size: 10px; flex-shrink:0; }

/* ── SUMMARY SECTION TITLE BARS ─────────────────────────────────────── */
.sec-bar {
    background: linear-gradient(90deg, rgba(29,78,216,0.6), rgba(37,99,235,0.2));
    border-left: 3px solid #3B82F6;
    color: #93C5FD;
    font-size: 11.5px; font-weight: 700; padding: 8px 14px;
    border-radius: 0 8px 0 0; margin-bottom: 0;
    letter-spacing: 0.06em; text-transform: uppercase;
    display: flex; align-items: center; gap: 8px;
}

/* ── DATAFRAME ───────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] { border-radius: 10px !important; overflow: hidden !important; }
.stDataFrame { color: #CBD5E1 !important; }

/* ── EXPANDER ────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: rgba(15,32,64,0.5) !important;
    border: 1px solid rgba(99,179,237,0.1) !important;
    border-radius: 10px !important;
}
[data-testid="stExpander"] summary { color: #93C5FD !important; font-weight: 600 !important; }

/* ── RADIO ───────────────────────────────────────────────────────────── */
[data-testid="stRadio"] label { color: #CBD5E1 !important; font-size: 12.5px !important; }
[data-testid="stRadio"] > div { gap: 12px !important; }

/* ── INFO ALERT ─────────────────────────────────────────────────────── */
.stAlert {
    background: rgba(30,58,100,0.5) !important;
    border: 1px solid rgba(96,165,250,0.2) !important;
    border-radius: 10px !important;
    color: #93C5FD !important;
    font-size: 12px !important;
}
.stAlert p { color: #93C5FD !important; }

/* ── DIVIDER ─────────────────────────────────────────────────────────── */
hr { border-color: rgba(99,179,237,0.1) !important; margin: 10px 0 !important; }

/* ── SCROLLBAR ───────────────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: rgba(15,32,64,0.4); border-radius:3px; }
::-webkit-scrollbar-thumb { background: rgba(96,165,250,0.3); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(96,165,250,0.5); }

/* ── INPUT / SELECT ──────────────────────────────────────────────────── */
input, select, textarea {
    background: rgba(15,32,64,0.7) !important;
    border: 1px solid rgba(99,179,237,0.2) !important;
    color: #E2E8F0 !important; border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# ── MutationObserver — override emotion inline bg ──────────────────────
components.html("""
<script>
(function(){
  function applyTheme(root){
    var els=(root||document).querySelectorAll(
      '[data-testid="stContainerWithBorder"],[data-testid="stVerticalBlockBorderWrapper"]');
    els.forEach(function(el){
      var par=el.parentElement&&el.parentElement.closest('[data-testid="stContainerWithBorder"]');
      if(par){
        el.style.setProperty('background','rgba(30,58,100,0.38)','important');
        el.style.setProperty('border','1px solid rgba(99,179,237,0.12)','important');
      } else {
        el.style.setProperty('background','rgba(15,32,64,0.65)','important');
        el.style.setProperty('border','1px solid rgba(99,179,237,0.14)','important');
        el.style.setProperty('border-radius','16px','important');
        el.style.setProperty('box-shadow','0 4px 24px rgba(0,0,0,0.35)','important');
      }
    });
  }
  applyTheme();
  new MutationObserver(function(m){
    m.forEach(function(x){if(x.addedNodes.length)applyTheme();});
  }).observe(document.body,{childList:true,subtree:true});
})();
</script>
""", height=0, scrolling=False)

# =========================================================================
# 5. SESSION STATE
# =========================================================================
_DEFAULTS = {
    "df_summary"          : pd.DataFrame(),
    "job_history"         : [],
    "uploaded_file_names" : [],
    "uploaded_file_paths" : [],
    "pending_action"      : None,
    "pending_validation"  : None,
    "inline_status_html"  : "",
    "btn_status"          : {
        "upload_summary": "idle",
        "run_all"       : "idle",
        "struct_val"    : "idle",
        "scd_val"       : "idle",
        "tc_gen"        : "idle",
    },
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# =========================================================================
# 6. DATABRICKS HELPERS
# =========================================================================
def _clean_name(fname): return fname.rsplit("/",1)[-1].rsplit(".",1)[0]

def upload_to_workspace(fname, data):
    target = f"{WORKSPACE_UPLOAD_DIR}/{fname}"
    body = {"path":target,"format":"AUTO","overwrite":True,
            "content":base64.b64encode(data).decode()}
    try:
        r = requests.post(f"{DATABRICKS_HOST}/api/2.0/workspace/import",
                          headers=HEADERS, json=body, timeout=120)
        return (True,target) if r.status_code==200 else (False,f"{r.status_code}:{r.text[:300]}")
    except Exception as e: return False, str(e)

def trigger_job(job_id, params):
    body = {"job_id":job_id,"notebook_params":params}
    try:
        r = requests.post(f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
                          headers=HEADERS, json=body, timeout=30)
        return (True,r.json()["run_id"]) if r.status_code==200 else (False,f"{r.status_code}:{r.text[:300]}")
    except Exception as e: return False, str(e)

def get_run_details(run_id):
    r = requests.get(f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
                     headers=HEADERS, params={"run_id":run_id}, timeout=30)
    return r.json() if r.status_code==200 else {}

def get_notebook_output(task_run_id):
    r = requests.get(f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
                     headers=HEADERS, params={"run_id":task_run_id}, timeout=30)
    return r.json() if r.status_code==200 else {}

def poll_until_done(run_id, tracker=None, label=""):
    while True:
        info  = get_run_details(run_id)
        state = info.get("state",{})
        lc    = state.get("life_cycle_state","UNKNOWN")
        rs    = state.get("result_state")
        tasks = info.get("tasks",[])
        last  = tasks[-1]["run_id"] if tasks else run_id
        if tracker: tracker.tick()
        if lc in ("TERMINATED","SKIPPED","INTERNAL_ERROR"):
            return (rs or lc), last
        time.sleep(3)

def extract_summary_list(run_id):
    info = get_run_details(run_id)
    tasks = info.get("tasks",[]) or [{"run_id":run_id}]
    for t in reversed(tasks):
        try:
            out = get_notebook_output(t["run_id"])
            raw = out.get("notebook_output",{}).get("result")
            if not raw: continue
            parsed = json.loads(raw)
            if isinstance(parsed,list) and parsed and isinstance(parsed[0],dict):
                return parsed
        except: continue
    return None

def log_history(category, job_id, run_id, status, start_ts=None, end_ts=None):
    _end = end_ts if end_ts else time.time()
    _start = start_ts if start_ts else _end
    dur = max(0.0, _end - _start)
    if dur < 60: dt = f"{dur:.1f}s"
    elif dur < 3600: m,s=divmod(int(dur),60); dt=f"{m}m {s}s"
    else: h,r=divmod(int(dur),3600); m,s=divmod(r,60); dt=f"{h}h {m}m {s}s"
    st.session_state.job_history.append({
        "Category":category, "Job ID":job_id, "Run ID":run_id,
        "Status":status,
        "Start":datetime.fromtimestamp(_start).strftime("%Y-%m-%d %H:%M:%S"),
        "End"  :datetime.fromtimestamp(_end).strftime("%Y-%m-%d %H:%M:%S"),
        "Duration":dt,
    })

# =========================================================================
# 7. PHASE PLANS
# =========================================================================
PHASE_PLANS = {
    "Uploading & Generating Summary": [
        ("Uploading file(s) to workspace",             1.0),
        ("Staging files to Unity Catalog volume",      1.5),
        ("Parsing structure and metadata",             2.0),
        ("Extracting column inventory per layer",      1.5),
        ("Compiling summary report",                   1.0),
    ],
    "Structure Validation": [
        ("Parsing workbook and metadata blocks",       1.0),
        ("Resolving source / target artifacts",        1.0),
        ("Validating RAW source against CSV",          2.0),
        ("Validating RAW target against Parquet",      2.0),
        ("Validating STD_RAW source",                  2.0),
        ("Validating STD_RAW target",                  2.0),
        ("Validating CURATED source",                  1.5),
        ("Validating CURATED target",                  1.5),
        ("Generating Excel report",                    1.0),
        ("Delivering dashboard & email",               1.0),
    ],
    "SCD Validation": [
        ("Parsing and resolving target tables",        1.0),
        ("Row count validation",                       1.5),
        ("Null validation on key columns",             1.5),
        ("Aggregate validation (sum/min/max)",         2.0),
        ("Primary-key uniqueness check",               1.5),
        ("Column-level data comparison",               2.5),
        ("Audit-column checks",                        1.5),
        ("SCD2 history validation",                    2.0),
        ("Compiling Excel report",                     1.0),
    ],
    "Test Case Generator": [
        ("Parsing and extracting business rules",      1.0),
        ("Deriving candidate test scenarios",          2.0),
        ("Generating test cases per type",             2.5),
        ("Formatting test-case workbook",              1.5),
        ("Finalising and delivering artifacts",        1.0),
    ],
    "Run All Validation": [
        ("Parsing and staging data artifacts",         1.0),
        ("Layer validations (RAW / STD / CUR)",        3.0),
        ("SCD validations (counts, nulls, PK)",        3.0),
        ("Data and audit-column validations",          2.5),
        ("Generating test cases",                      2.0),
        ("Consolidating reports & email delivery",     1.5),
    ],
}

# =========================================================================
# 8. PROGRESS TRACKER
# =========================================================================
class ProgressTracker:
    def __init__(self, slot, kind, compact=False):
        self.slot, self.kind, self.compact = slot, kind, compact
        self.phases = PHASE_PLANS.get(kind, [(kind,1.0)])
        total = sum(w for _,w in self.phases) or 1.0
        cum=0.0; self.thresholds=[]
        for (_,w) in self.phases:
            cum += w/total*100; self.thresholds.append(cum)
        self.start_ts=None; self.expected_sec=45.0

    def start(self):
        self.start_ts=time.time(); self._render(0,0)

    def _pfi(self, pct):
        for i,t in enumerate(self.thresholds):
            if pct<=t: return i
        return len(self.phases)-1

    def tick(self):
        if not self.start_ts: return
        pct=min(95.0,(time.time()-self.start_ts)/self.expected_sec*95.0)
        self._render(pct,self._pfi(pct))

    def done(self):  self._render(100.0,len(self.phases)-1,terminal="success")
    def fail(self,msg=""):
        e=time.time()-(self.start_ts or time.time())
        pct=min(95.0,e/self.expected_sec*95.0)
        self._render(pct,self._pfi(pct),terminal="failed",message=msg)
    def clear(self): self.slot.empty()

    def _render(self, pct, phase_idx, terminal="", message=""):
        steps_html=""
        for i,(label,_) in enumerate(self.phases):
            if terminal=="success" or i<phase_idx:
                cls="done"; icon="✓"
            elif i==phase_idx:
                cls="active"; icon="▶"
            else:
                cls=""; icon="○"
            if self.compact:
                steps_html+=(f'<li class="{cls}"><span class="sc-step-dot"></span>{label}</li>')
            else:
                steps_html+=(f'<li class="{cls}"><span class="pc-step-icon">{icon}</span>{label}</li>')

        if terminal=="success": phase_text="Completed successfully"
        elif terminal=="failed": phase_text=f"Failed — {message or 'see history'}"
        elif phase_idx<len(self.phases): phase_text=f"<b>Now:</b> {self.phases[phase_idx][0]}"
        else: phase_text=""

        pct_i=int(round(pct))
        h_icon=("✅" if terminal=="success" else "⚠" if terminal=="failed" else "⏳")
        h_color=("#34D399" if terminal=="success" else "#F87171" if terminal=="failed" else "#93C5FD")
        bar_color=("linear-gradient(90deg,#059669,#34D399)" if terminal=="success"
                   else "linear-gradient(90deg,#991B1B,#EF4444)" if terminal=="failed"
                   else "linear-gradient(90deg,#1D4ED8,#3B82F6,#60A5FA)")

        if self.compact:
            html=f"""
<div class="status-card">
  <div class="sc-title">
    <span>{h_icon}</span>
    <span style="color:{h_color};">{self.kind}</span>
    <span style="margin-left:auto;font-size:10px;color:#475569;font-family:'JetBrains Mono',monospace;">{pct_i}%</span>
  </div>
  <div class="sc-phase">{phase_text}</div>
  <div class="sc-bar-wrap"><div class="sc-bar-fill" style="width:{pct_i}%;background:{bar_color};"></div></div>
  <ul class="sc-steps">{steps_html}</ul>
</div>"""
        else:
            html=f"""
<div class="progress-card">
  <div class="pc-title">
    <span style="color:{h_color};">{h_icon}&nbsp;&nbsp;{self.kind}</span>
    <span style="font-size:11px;font-family:'JetBrains Mono',monospace;color:#475569;">{pct_i}%</span>
  </div>
  <div class="pc-phase">{phase_text}</div>
  <div class="pc-bar-wrap"><div class="pc-bar-fill" style="width:{pct_i}%;background:{bar_color};"></div></div>
  <div class="pc-pct">{pct_i}% complete</div>
  <ul class="pc-steps">{steps_html}</ul>
</div>"""
        self.slot.markdown(html, unsafe_allow_html=True)

# =========================================================================
# 9. SUMMARY FILTERS
# =========================================================================
IGNORED_SHEETS     = {"version.history"}
IGNORED_CATEGORIES = {"PII Present","Temporal Columns","Nullability","Extraction Mode"}

def _apply_filters(rows):
    if not rows: return rows
    sk=[k for k in rows[0] if k.lower().replace("_"," ").strip() in ("sheet","sheet name")]
    ck=[k for k in rows[0] if k.lower().strip()=="category"]
    out=[]
    for r in rows:
        sv=next((str(r.get(k,"")).strip() for k in sk),"")
        cv=next((str(r.get(k,"")).strip() for k in ck),"")
        if sv and sv.lower() in IGNORED_SHEETS: continue
        if cv and cv in IGNORED_CATEGORIES: continue
        out.append(r)
    return out

# =========================================================================
# 10. BUTTON HELPERS
# =========================================================================
def styled_button(label, key, status_key, disabled=False, use_container_width=True):
    s=st.session_state.btn_status.get(status_key,"idle")
    cls={"idle":"btn-idle","running":"btn-running","success":"btn-success","failed":"btn-failed"}.get(s,"btn-idle")
    st.markdown(f'<div class="{cls}">',unsafe_allow_html=True)
    c=st.button(label,key=key,disabled=disabled,use_container_width=use_container_width)
    st.markdown("</div>",unsafe_allow_html=True)
    return c

def compact_button(label, key, status_key, disabled=False, use_container_width=True):
    s=st.session_state.btn_status.get(status_key,"idle")
    cls={"idle":"btn-compact btn-compact-idle","running":"btn-compact btn-compact-running",
         "success":"btn-compact btn-compact-success","failed":"btn-compact btn-compact-failed"}.get(s,"btn-compact btn-compact-idle")
    st.markdown(f'<div class="{cls}">',unsafe_allow_html=True)
    c=st.button(label,key=key,disabled=disabled,use_container_width=use_container_width)
    st.markdown("</div>",unsafe_allow_html=True)
    return c

# =========================================================================
# 11. HEADER
# =========================================================================
now_str = datetime.now().strftime("%d %b %Y  %H:%M")
st.markdown(f"""
<div class="iq-header">
  <div class="iq-header-inner">
    <div class="iq-title-group">
      <div class="iq-eyebrow">AI-Powered Quality Assurance Platform</div>
      <div class="iq-title">Ingest<span>IQ</span>™ &mdash; Data Ingestion QA</div>
      <div class="iq-subtitle">Upload files &nbsp;›&nbsp; Generate summary &nbsp;›&nbsp; Run validations</div>
    </div>
    <div class="iq-header-right">
      <div class="iq-status-pill">
        <div class="iq-status-dot"></div>
        LIVE &nbsp;·&nbsp; {now_str}
      </div>
      <div class="iq-divider"></div>
      <div class="iq-tcs-logo">
        <img src="data:image/svg+xml;base64,{TCS_LOGO_B64}"
             style="height:38px;width:auto;display:block;" alt="TCS"/>
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# =========================================================================
# 12. TOP ROW — Upload + Summary
# =========================================================================
PANEL_H = 390
left, right = st.columns([1,1], gap="medium")

# ── LEFT: Upload ────────────────────────────────────────────────────────
with left:
    with st.container(border=True, height=PANEL_H):
        st.subheader("⬆  File Upload")

        uploaded = st.file_uploader(
            "Drop Excel file(s) here (.xlsx)",
            accept_multiple_files=True, type=["xlsx"],
            label_visibility="collapsed",
        )

        busy = st.session_state.pending_action is not None
        can_upload = bool(uploaded) and not busy

        if uploaded:
            names = [f.name for f in uploaded]
            st.caption(f"📎  {' · '.join(names)}")

        up_clicked = styled_button(
            "⬆  Upload & Generate Summary",
            key="upload_summary_btn", status_key="upload_summary",
            disabled=not can_upload,
        )

        # ── Inline status slot ──────────────────────────────────────────
        inline_slot = st.empty()
        if st.session_state.inline_status_html:
            inline_slot.markdown(st.session_state.inline_status_html, unsafe_allow_html=True)

        with st.expander("📋  Uploaded files", expanded=False):
            if st.session_state.uploaded_file_names:
                for fn in st.session_state.uploaded_file_names:
                    st.markdown(f"<span style='font-family:JetBrains Mono,monospace;"
                                f"font-size:11px;color:#60A5FA;'>📄 {fn}</span>",
                                unsafe_allow_html=True)
                if st.button("🗑  Clear list", key="clear_upl"):
                    st.session_state.uploaded_file_names = []
                    st.session_state.uploaded_file_paths = []
                    st.session_state.inline_status_html  = ""
                    st.rerun()
            else:
                st.caption("No files uploaded yet.")

# ── RIGHT: Summary Viewer ────────────────────────────────────────────────
with right:
    with st.container(border=True, height=PANEL_H):
        st.subheader("📊  Summary Viewer")

        if st.session_state.df_summary.empty:
            st.markdown("""
<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            height:260px;gap:12px;opacity:.5;">
  <div style="font-size:40px;">📂</div>
  <div style="font-size:12px;color:#64748B;text-align:center;line-height:1.5;">
    Upload file(s) and click<br>
    <b style="color:#60A5FA;">Upload &amp; Generate Summary</b><br>
    to populate this panel.
  </div>
</div>""", unsafe_allow_html=True)
        else:
            df_sv = st.session_state.df_summary

            def _fc(df, *cands):
                want={c.lower().replace("_"," ").strip() for c in cands}
                for col in df.columns:
                    if col.lower().replace("_"," ").strip() in want: return col
                return None

            stm_col   = _fc(df_sv,"STM File","STM","STM Name","File")
            sheet_col = _fc(df_sv,"Sheet","Sheet Name")
            disp_cols = [c for c in df_sv.columns if c not in (stm_col,sheet_col)]

            FLOW = {"raw":"Source → Raw","std_raw":"Raw → Std.Raw","curated":"Std.Raw → Curated"}
            def _label(s):
                sl=s.lower()
                if "std_raw" in sl: return FLOW["std_raw"]
                if "raw" in sl and "std" not in sl: return FLOW["raw"]
                if "curated" in sl: return FLOW["curated"]
                return s

            ICONS = {"raw":"🗂","std_raw":"🔄","curated":"✨"}
            def _icon(s):
                sl=s.lower()
                if "std_raw" in sl: return ICONS["std_raw"]
                if "raw" in sl and "std" not in sl: return ICONS["raw"]
                if "curated" in sl: return ICONS["curated"]
                return "📄"

            if not sheet_col and not stm_col:
                st.dataframe(df_sv, use_container_width=True, hide_index=True)
            else:
                gkeys,groups={},{}
                gkeys=[]
                for _,row in df_sv.iterrows():
                    sv=str(row[stm_col]).strip() if stm_col else ""
                    shv=str(row[sheet_col]).strip() if sheet_col else ""
                    key=(sv,shv)
                    if key not in groups: groups[key]=[]; gkeys.append(key)
                    groups[key].append(row)

                for i,(sv,shv) in enumerate(gkeys):
                    flow=_label(shv) if shv else ""
                    ico =_icon(shv)  if shv else "📄"
                    title=(f"{ico}  {sv}  ·  {flow}" if sv and flow
                           else f"{ico}  {flow or sv or 'Summary'}")
                    st.markdown(
                        f"<div class='sec-bar' style='margin-top:{'10px' if i>0 else '0'};'>"
                        f"{title}</div>",
                        unsafe_allow_html=True)
                    sub=pd.DataFrame(groups[(sv,shv)])[disp_cols]
                    st.dataframe(sub, use_container_width=True, hide_index=True)

# =========================================================================
# 13. QUALITY ASSURANCE PANEL
# =========================================================================
with st.container(border=True):
    st.subheader("🔬  Quality Assurance")

    has_files     = bool(st.session_state.uploaded_file_names)
    busy          = st.session_state.pending_action is not None
    common_dis    = (not has_files) or busy

    if not has_files:
        st.markdown("""
<div style="background:rgba(30,58,100,0.35);border:1px solid rgba(96,165,250,0.15);
            border-radius:10px;padding:10px 16px;font-size:11.5px;color:#64748B;
            display:flex;align-items:center;gap:10px;">
  <span style="font-size:16px;">ℹ️</span>
  Upload and process file(s) above to enable the validation suite below.
</div>""", unsafe_allow_html=True)
        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    qa1, qa2, qa3, qa4 = st.columns(4)
    with qa1:
        run_all_clicked = compact_button(
            "▶  Run All Validation", key="run_all_btn",
            status_key="run_all", disabled=common_dis)
    with qa2:
        struct_clicked = compact_button(
            "🔍  Structure Validation", key="struct_val_btn",
            status_key="struct_val", disabled=common_dis)
    with qa3:
        scd_clicked = compact_button(
            "🔁  SCD Validation", key="scd_val_btn",
            status_key="scd_val", disabled=common_dis)
    with qa4:
        tc_clicked = compact_button(
            "🧬  Test Case Generator", key="tc_gen_btn",
            status_key="tc_gen", disabled=common_dis)

    # ── INITIAL / DELTA picker ──────────────────────────────────────────
    if run_all_clicked: st.session_state.pending_validation="run_all"
    if scd_clicked:     st.session_state.pending_validation="scd_val"

    if st.session_state.pending_validation in ("run_all","scd_val") and not busy:
        with st.container(border=True):
            cat=("Run All Validation"
                 if st.session_state.pending_validation=="run_all"
                 else "SCD Validation")
            st.markdown(f"<div style='font-size:12px;font-weight:700;color:#93C5FD;"
                        f"text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>"
                        f"Validation Type — {cat}</div>",
                        unsafe_allow_html=True)
            v_type = st.radio("Type",["INITIAL","DELTA"],horizontal=True,
                              key="v_type_radio",label_visibility="collapsed")
            c1,c2=st.columns(2)
            with c1:
                if st.button("✅  Confirm & Run", key="confirm_vtype",
                             use_container_width=True):
                    fc=",".join(st.session_state.uploaded_file_names)
                    params={"STM_FILE_NAMES":fc,"VALIDATION_TYPE":v_type}
                    bk="run_all" if st.session_state.pending_validation=="run_all" else "scd_val"
                    cat2="Run All Validation" if bk=="run_all" else "SCD Validation"
                    jid=JOB_IDS[cat2]
                    st.session_state.btn_status[bk]="running"
                    st.session_state.pending_action={
                        "kind":"validation","category":cat2,
                        "job_id":jid,"params":params,"btn_key":bk}
                    st.session_state.pending_validation=None
                    st.rerun()
            with c2:
                if st.button("✖  Cancel", key="cancel_vtype", use_container_width=True):
                    st.session_state.pending_validation=None; st.rerun()

    # Structure
    if struct_clicked and not busy:
        fc=",".join(st.session_state.uploaded_file_names)
        st.session_state.btn_status["struct_val"]="running"
        st.session_state.pending_action={
            "kind":"validation","category":"Structure Validation",
            "job_id":JOB_IDS["Structure Validation"],
            "params":{"STM_FILE_NAMES":fc},"btn_key":"struct_val"}
        st.rerun()

    # Test Case
    if tc_clicked and not busy:
        fc=",".join(st.session_state.uploaded_file_names)
        st.session_state.btn_status["tc_gen"]="running"
        st.session_state.pending_action={
            "kind":"validation","category":"Test Case Generator",
            "job_id":JOB_IDS["Test Case Generator"],
            "params":{"STM_FILE_NAMES":fc},"btn_key":"tc_gen"}
        st.rerun()

    qa_tracker_slot = st.empty()

# =========================================================================
# 14. UPLOAD TRIGGER
# =========================================================================
if up_clicked and can_upload:
    snap=[{"name":f.name,"data":f.getvalue()} for f in uploaded]
    st.session_state.btn_status["upload_summary"]="running"
    st.session_state.inline_status_html=""
    st.session_state.pending_action={
        "kind":"upload_summary","btn_key":"upload_summary","files":snap}
    st.rerun()

# =========================================================================
# 15. PENDING ACTION EXECUTOR
# =========================================================================
if st.session_state.pending_action is not None:
    action=st.session_state.pending_action
    bk=action["btn_key"]

    if action["kind"]=="upload_summary":
        tracker=ProgressTracker(inline_slot,"Uploading & Generating Summary",compact=True)
    else:
        tracker=ProgressTracker(qa_tracker_slot,action["category"],compact=False)

    tracker.start()

    try:
        if action["kind"]=="upload_summary":
            files=action["files"]
            ws_paths,fnames,errs=[],[],[]
            for f in files:
                ok,det=upload_to_workspace(f["name"],f["data"])
                if ok: ws_paths.append(det); fnames.append(_clean_name(f["name"]))
                else: errs.append(f"{f['name']}→{det}")
                tracker.tick()
            if not ws_paths: raise Exception(f"Upload failed: {errs}")

            _cs=time.time()
            ok,cr=trigger_job(FILE_COPY_JOB_ID,{"workspace_file_paths":",".join(ws_paths)})
            if not ok: raise Exception(f"File-Copy trigger failed: {cr}")
            cstate,_=poll_until_done(cr,tracker,"File-Copy")
            log_history("File Copy",FILE_COPY_JOB_ID,cr,cstate,start_ts=_cs,end_ts=time.time())
            if cstate!="SUCCESS": raise Exception(f"File-Copy ended with {cstate}")

            st.session_state.uploaded_file_names=sorted(set(
                st.session_state.uploaded_file_names+fnames))
            st.session_state.uploaded_file_paths=sorted(set(
                st.session_state.uploaded_file_paths+ws_paths))

            _ss=time.time()
            ok,sr=trigger_job(SUMMARY_JOB_ID,{"stm_file_names":",".join(fnames)})
            if not ok: raise Exception(f"Summary trigger failed: {sr}")
            sstate,_=poll_until_done(sr,tracker,"Summary")
            log_history("Summary",SUMMARY_JOB_ID,sr,sstate,start_ts=_ss,end_ts=time.time())
            if sstate!="SUCCESS": raise Exception(f"Summary ended with {sstate}")

            data=extract_summary_list(sr)
            if data:
                data=_apply_filters(data)
                if data: st.session_state.df_summary=pd.DataFrame(data)

            st.session_state.btn_status[bk]="success"
            tracker.done()
            st.session_state.inline_status_html=""

        elif action["kind"]=="validation":
            cat=action["category"]; jid=action["job_id"]; params=action["params"]
            _vs=time.time()
            ok,rid=trigger_job(jid,params)
            if not ok: raise Exception(f"Trigger failed: {rid}")
            state,_=poll_until_done(rid,tracker,cat)
            log_history(cat,jid,rid,state,start_ts=_vs,end_ts=time.time())
            if state!="SUCCESS": raise Exception(f"{cat} ended with {state}")
            st.session_state.btn_status[bk]="success"
            tracker.done()

    except Exception as e:
        st.session_state.btn_status[bk]="failed"
        tracker.fail(str(e)[:120])
        log_history(action.get("category",action["kind"]),0,"-",f"ERROR: {e}",
                    start_ts=tracker.start_ts,end_ts=time.time())
    finally:
        st.session_state.pending_action=None
        time.sleep(1.5)
        tracker.clear()
        st.rerun()

# =========================================================================
# 16. JOB EXECUTION HISTORY
# =========================================================================
st.divider()
with st.container(border=True):
    col_h, col_b = st.columns([5,1])
    with col_h:
        st.subheader("📋  Job Execution History")
    with col_b:
        if st.session_state.job_history:
            if st.button("🗑 Clear", key="clear_hist", use_container_width=True):
                st.session_state.job_history=[]
                st.rerun()

    if st.session_state.job_history:
        hist=pd.DataFrame(st.session_state.job_history)

        def _status_emoji(val):
            if not isinstance(val,str): return val
            if "SUCCESS" in val.upper(): return f"✅ {val}"
            if "ERROR"   in val.upper() or "FAILED" in val.upper(): return f"❌ {val}"
            return f"⏳ {val}"

        hist["Status"]=hist["Status"].apply(_status_emoji)
        st.dataframe(hist, use_container_width=True, hide_index=True)
    else:
        st.markdown("""
<div style="text-align:center;padding:20px;opacity:.4;">
  <div style="font-size:28px;margin-bottom:8px;">🕐</div>
  <div style="font-size:12px;color:#64748B;">No jobs executed yet.</div>
</div>""", unsafe_allow_html=True)
