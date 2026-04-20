"""
========================================================================
 End-to-End AI QA Portal   (Community-Edition Compatible)
------------------------------------------------------------------------
 ▸ Works on Databricks Community Edition:
     - Uploads to DBFS /FileStore (no Unity Catalog volumes needed)
     - Uses DBFS streaming API (handles files bigger than 1 MB)
     - Only one job is triggered: the Summarizer
 ▸ Clean layout: upload (left) · summary viewer (right)
 ▸ Four QA buttons (INITIAL/DELTA for Run-All & SCD)
 ▸ Dino runner while a job is running
 ▸ Button colour automatically tracks status:
      idle = blue · running = yellow · success = green · fail = red
 ▸ Job execution history
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
# 1. CONFIG   (put in st.secrets for production)
# =========================================================================
# Community Edition URL pattern:
#   https://community.cloud.databricks.com
DATABRICKS_HOST = "https://community.cloud.databricks.com"
TOKEN           = "PUT_YOUR_CE_TOKEN_HERE"

# CE uses DBFS (/FileStore) instead of Unity-Catalog volumes
DBFS_UPLOAD_DIR = "/FileStore/qa_uploads"

# Job IDs (create these jobs in CE first) ---------------------------------
SUMMARY_JOB_ID = 0         # <<< put your Summary job id here

JOB_IDS = {
    "Run All Validation" : 0,
    "STM Validation"     : 0,
    "SCD Validation"     : 0,
    "Test Case Generator": 2564838,
}

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type" : "application/json",
}

# =========================================================================
# 2. PAGE CONFIG
# =========================================================================
st.set_page_config(
    page_title="End-to-End AI QA",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================================================================
# 3. GLOBAL CSS
# =========================================================================
st.markdown("""
<style>
.block-container { padding-top: 1.1rem; padding-bottom: 2rem; }
div[data-testid="stVerticalBlock"] > div { gap: 0.6rem; }

.btn-idle    button { background:#2563EB !important; color:#fff !important; }
.btn-running button { background:#FACC15 !important; color:#111 !important;
                       animation: pulse 1.1s infinite;}
.btn-success button { background:#16A34A !important; color:#fff !important; }
.btn-failed  button { background:#DC2626 !important; color:#fff !important; }

@keyframes pulse {
    0%   { box-shadow: 0 0 0 0   rgba(250,204, 21,.8); }
    70%  { box-shadow: 0 0 0 10px rgba(250,204, 21,0); }
    100% { box-shadow: 0 0 0 0   rgba(250,204, 21,0); }
}

.stButton > button {
    height: 52px;
    border-radius: 10px;
    font-weight: 600;
    font-size: 15px;
    transition: transform .1s ease;
    border: none !important;
}
.stButton > button:hover  { transform: translateY(-1px); }
.stButton > button:active { transform: translateY(0); }

[data-testid="stVerticalBlockBorderWrapper"] {
    border-radius: 14px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,.04);
}
.stDataFrame { border-radius: 10px; }
h1 { margin-bottom: 0.4rem !important; }
hr { margin: 1rem 0 !important; }
</style>
""", unsafe_allow_html=True)

# =========================================================================
# 4. SESSION STATE
# =========================================================================
def _init_state():
    defaults = {
        "uploaded_stm_names"  : [],
        "uploaded_file_paths" : [],
        "btn_status": {
            "upload_summary": "idle",
            "run_all"       : "idle",
            "stm_val"       : "idle",
            "scd_val"       : "idle",
            "tc_gen"        : "idle",
        },
        "df_summary"         : pd.DataFrame(columns=["Category", "Details"]),
        "job_history"        : [],
        "pending_validation" : None,
        "pending_action"     : None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
_init_state()


# =========================================================================
# 5. DBFS UPLOAD  (streaming — works for any size)
# =========================================================================
def dbfs_upload_streaming(file_bytes: bytes, dbfs_path: str) -> tuple[bool, str]:
    """Upload any-size file to DBFS using create / add-block / close.
       Works on Databricks Community Edition."""
    try:
        # Step 1 — open a handle (overwrite = True)
        r1 = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/create",
            headers=HEADERS,
            json={"path": dbfs_path, "overwrite": True},
            timeout=60,
        )
        if r1.status_code != 200:
            return False, f"create failed: {r1.status_code} {r1.text[:160]}"

        handle = r1.json().get("handle")
        if handle is None:
            return False, f"no handle returned: {r1.text[:160]}"

        # Step 2 — stream bytes in base-64 chunks (< 1 MB each)
        CHUNK = 900_000        # bytes before base64 encoding
        for i in range(0, len(file_bytes), CHUNK):
            piece = file_bytes[i : i + CHUNK]
            b64   = base64.b64encode(piece).decode("utf-8")
            rb = requests.post(
                f"{DATABRICKS_HOST}/api/2.0/dbfs/add-block",
                headers=HEADERS,
                json={"handle": handle, "data": b64},
                timeout=120,
            )
            if rb.status_code != 200:
                # try to close the handle cleanly
                requests.post(
                    f"{DATABRICKS_HOST}/api/2.0/dbfs/close",
                    headers=HEADERS, json={"handle": handle}, timeout=30,
                )
                return False, f"add-block failed: {rb.status_code} {rb.text[:160]}"

        # Step 3 — close
        rc = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/dbfs/close",
            headers=HEADERS,
            json={"handle": handle},
            timeout=30,
        )
        if rc.status_code != 200:
            return False, f"close failed: {rc.status_code} {rc.text[:160]}"

        return True, dbfs_path
    except Exception as e:
        return False, str(e)


# =========================================================================
# 6. UTILITIES
# =========================================================================
def _clean_stm_name(filename: str) -> str:
    for ext in (".xlsx", ".xls", ".xlsm", ".csv", ".parquet",
                ".txt", ".json", ".tsv"):
        if filename.lower().endswith(ext):
            return filename[: -len(ext)]
    return filename


def trigger_job(job_id: int, params: dict) -> tuple[bool, str | int]:
    if not job_id:
        return False, "Job id not configured (still 0)."
    payload = {"job_id": job_id, "notebook_params": params}
    try:
        # CE supports both Jobs API v2.1 and v2.2 — stick to 2.1 for safety
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers=HEADERS, json=payload, timeout=30,
        )
        if r.status_code != 200:
            return False, f"{r.status_code} – {r.text[:200]}"
        return True, r.json().get("run_id")
    except Exception as e:
        return False, str(e)


def get_run_details(run_id: int) -> dict:
    return requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
        headers=HEADERS, params={"run_id": run_id}, timeout=30,
    ).json()


def get_notebook_output(task_run_id: int) -> dict:
    return requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
        headers=HEADERS, params={"run_id": task_run_id}, timeout=30,
    ).json()


def poll_until_done(run_id: int, status_slot=None, label: str = "") -> tuple[str, int]:
    while True:
        info  = get_run_details(run_id)
        state = info.get("state", {})
        lc    = state.get("life_cycle_state", "UNKNOWN")
        rs    = state.get("result_state")

        tasks = info.get("tasks", [])
        last_task_run_id = tasks[-1]["run_id"] if tasks else run_id

        if status_slot is not None:
            status_slot.info(f"⏳ {label} · `{lc}`")

        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return (rs or lc), last_task_run_id
        time.sleep(5)


def extract_summary_list(run_id: int):
    """Find the first task whose notebook_output parses into the expected
       list-of-dicts format."""
    info  = get_run_details(run_id)
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


def log_history(category: str, job_id: int, run_id, status: str):
    st.session_state.job_history.append({
        "Category": category, "Job ID": job_id,
        "Run ID"  : run_id,   "Status": status,
        "Time"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


# =========================================================================
# 7. DINO RUNNER GAME
# =========================================================================
DINO_GAME_HTML = """
<!doctype html>
<html><head>
<style>
  body { margin:0; background:transparent; font-family:'Courier New',monospace; }
  .wrap { width:100%; padding:8px 4px 4px;
          background:linear-gradient(180deg,#f8fafc 0%,#e2e8f0 100%);
          border-radius:12px; text-align:center; }
  canvas { background:#fff; border:2px solid #94a3b8; border-radius:8px; max-width:100%; }
  .hint  { color:#475569; font-size:12px; margin-top:4px; }
  .score { font-weight:700; color:#111; margin-bottom:4px; font-size:13px; }
</style></head>
<body>
<div class="wrap">
  <div class="score">🦖 Dino Runner — Score : <span id="score">0</span>
       &nbsp;|&nbsp; High : <span id="high">0</span></div>
  <canvas id="game" width="720" height="170"></canvas>
  <div class="hint">Press <b>SPACE</b>/tap to jump · <b>↓</b> to duck</div>
</div>
<script>
const cvs = document.getElementById('game');
const ctx = cvs.getContext('2d');
const W = cvs.width, H = cvs.height, GROUND = H - 26;
let dino    = { x:60, y:GROUND-40, w:40, h:40, vy:0, ducking:false };
let gravity = 0.9;
let obstacles = [], clouds = [];
let frame=0, score=0, speed=6, gameOver=false, high=0;
try { high = parseInt(localStorage.getItem('dino_high')||'0'); } catch(e){}
document.getElementById('high').textContent = high;

function jump(){ if(gameOver){ reset(); return; }
    if(dino.y >= GROUND-dino.h-1){ dino.vy = -13; } }
function duck(on){ dino.ducking=on; dino.h=on?22:40; }

document.addEventListener('keydown', e=>{
    if(e.code==='Space' || e.code==='ArrowUp'){ e.preventDefault(); jump(); }
    if(e.code==='ArrowDown'){ duck(true); }
});
document.addEventListener('keyup', e=>{ if(e.code==='ArrowDown'){ duck(false); } });
cvs.addEventListener('click', jump);
cvs.addEventListener('touchstart', e=>{ e.preventDefault(); jump(); });

function spawn(){
    if(frame % Math.max(55-Math.floor(score/80), 30) === 0){
        const big = Math.random() > 0.5;
        obstacles.push({ x:W+20, y:GROUND-(big?38:24), w:big?18:14, h:big?38:24 });
    }
    if(frame % 110 === 0) clouds.push({ x:W, y:20+Math.random()*38 });
}
function reset(){ obstacles=[]; clouds=[]; frame=0; score=0; speed=6; gameOver=false;
    dino.y=GROUND-dino.h; dino.vy=0; }
function drawDino(){
    ctx.fillStyle='#111';
    ctx.fillRect(dino.x, dino.y, dino.w, dino.h);
    ctx.fillStyle='#fff';
    ctx.fillRect(dino.x+dino.w-10, dino.y+6, 4, 4);
    ctx.fillStyle='#111';
    const off=(frame%10<5)?0:4;
    if(!dino.ducking){
        ctx.fillRect(dino.x+6,  dino.y+dino.h, 8, 6-off);
        ctx.fillRect(dino.x+24, dino.y+dino.h, 8, 6+off-4);
    }
}
function loop(){
    ctx.clearRect(0,0,W,H);
    ctx.strokeStyle='#555'; ctx.lineWidth=2;
    ctx.beginPath(); ctx.moveTo(0,GROUND); ctx.lineTo(W,GROUND); ctx.stroke();
    ctx.fillStyle='#cbd5e1';
    clouds.forEach(c=>{
        ctx.beginPath();
        ctx.arc(c.x,c.y,9,0,Math.PI*2);
        ctx.arc(c.x+11,c.y-4,11,0,Math.PI*2);
        ctx.arc(c.x+22,c.y,9,0,Math.PI*2);
        ctx.fill(); c.x -= speed/3;
    });
    clouds = clouds.filter(c=>c.x>-40);
    if(!gameOver){
        dino.vy += gravity; dino.y += dino.vy;
        if(dino.y > GROUND-dino.h){ dino.y=GROUND-dino.h; dino.vy=0; }
        spawn();
        obstacles.forEach(o=>o.x -= speed);
        obstacles = obstacles.filter(o=>o.x+o.w>0);
        for(const o of obstacles){
            if(dino.x < o.x+o.w && dino.x+dino.w > o.x &&
               dino.y < o.y+o.h && dino.y+dino.h > o.y){
                gameOver = true;
                if(score>high){
                    high = score;
                    try{ localStorage.setItem('dino_high', high); }catch(e){}
                    document.getElementById('high').textContent = high;
                }
            }
        }
        frame++; score++; if(frame%400===0) speed += 0.3;
        document.getElementById('score').textContent = score;
    }
    ctx.fillStyle='#15803d';
    obstacles.forEach(o=>{
        ctx.fillRect(o.x, o.y, o.w, o.h);
        ctx.fillRect(o.x-3, o.y+6, 3, 6);
        ctx.fillRect(o.x+o.w, o.y+12, 3, 6);
    });
    drawDino();
    if(gameOver){
        ctx.fillStyle='rgba(0,0,0,.7)'; ctx.fillRect(0,0,W,H);
        ctx.fillStyle='#fff'; ctx.font='bold 20px Courier New'; ctx.textAlign='center';
        ctx.fillText('GAME OVER — SPACE / tap to restart', W/2, H/2);
    }
    requestAnimationFrame(loop);
}
loop();
</script>
</body></html>
"""


# =========================================================================
# 8. STYLED BUTTON
# =========================================================================
def styled_button(label: str, key: str, status_key: str, disabled=False):
    cls = {
        "idle"    : "btn-idle",
        "running" : "btn-running",
        "success" : "btn-success",
        "failed"  : "btn-failed",
    }[st.session_state.btn_status.get(status_key, "idle")]
    st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
    clicked = st.button(label, key=key, use_container_width=True, disabled=disabled)
    st.markdown("</div>", unsafe_allow_html=True)
    return clicked


# =========================================================================
# 9. HEADER
# =========================================================================
st.title("🤖 End-to-End AI QA for Ingestion Pipelines")


# =========================================================================
# 10. UPLOAD (LEFT) + SUMMARY VIEWER (RIGHT)
# =========================================================================
PANEL_HEIGHT = 380

left, right = st.columns([3.2, 2], gap="medium")

with left:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("📂 Upload Source Files")

        uploaded = st.file_uploader(
            "parquet · csv · xlsx · xls · txt · json · tsv",
            type=["parquet", "csv", "xlsx", "xls", "xlsm",
                  "txt", "json", "tsv"],
            accept_multiple_files=True,
            key="file_uploader_widget",
            label_visibility="collapsed",
        )

        upload_summary_clicked = styled_button(
            "⬆️  Upload Files & Generate Summary",
            key="upload_summary_btn",
            status_key="upload_summary",
            disabled=(not uploaded) or (st.session_state.pending_action is not None),
        )

        if st.session_state.uploaded_stm_names:
            with st.expander(f"📑 {len(st.session_state.uploaded_stm_names)} "
                             f"STM file(s) available", expanded=False):
                st.dataframe(
                    pd.DataFrame({
                        "STM Name"  : st.session_state.uploaded_stm_names,
                        "DBFS Path" : st.session_state.uploaded_file_paths,
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
                if st.button("Clear list", key="clear_uploads"):
                    st.session_state.uploaded_stm_names  = []
                    st.session_state.uploaded_file_paths = []
                    st.rerun()

with right:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("📊 Summary Viewer")
        if st.session_state.df_summary.empty:
            st.caption("Upload file(s) and click **Generate Summary** to populate this panel.")
        else:
            st.dataframe(
                st.session_state.df_summary,
                use_container_width=True,
                hide_index=True,
            )


# =========================================================================
# 11. QUALITY ASSURANCE
# =========================================================================
with st.container(border=True):
    st.subheader("🧪 Quality Assurance")

    has_files = bool(st.session_state.uploaded_stm_names)
    busy      = st.session_state.pending_action is not None
    dis       = (not has_files) or busy

    if not has_files:
        st.info("Upload files first to enable the validation buttons.")

    run_all_clicked = styled_button(
        "▶  Run All Validation",
        key="run_all_btn", status_key="run_all", disabled=dis,
    )
    b1, b2, b3 = st.columns(3)
    with b1:
        stm_clicked = styled_button(
            "🔍 STM Validation",
            key="stm_val_btn", status_key="stm_val", disabled=dis,
        )
    with b2:
        scd_clicked = styled_button(
            "🔁 SCD Validation",
            key="scd_val_btn", status_key="scd_val", disabled=dis,
        )
    with b3:
        tc_clicked = styled_button(
            "🧬 Test Case Generator",
            key="tc_gen_btn", status_key="tc_gen", disabled=dis,
        )

    if run_all_clicked: st.session_state.pending_validation = "run_all"
    if scd_clicked:     st.session_state.pending_validation = "scd_val"

    if st.session_state.pending_validation in ("run_all", "scd_val") and not busy:
        with st.container(border=True):
            cat = ("Run All Validation"
                   if st.session_state.pending_validation == "run_all"
                   else "SCD Validation")
            st.markdown(f"#### Choose Validation Type for **{cat}**")
            v_type = st.radio(
                "Validation Type", ["INITIAL", "DELTA"],
                horizontal=True, key="v_type_radio",
                label_visibility="collapsed",
            )
            cc1, cc2, _ = st.columns([1, 1, 3])
            confirm = cc1.button("✅ Confirm & Run", key="v_type_confirm")
            cancel  = cc2.button("Cancel", key="v_type_cancel")

            if cancel:
                st.session_state.pending_validation = None
                st.rerun()
            if confirm:
                which = st.session_state.pending_validation
                st.session_state.pending_validation = None
                stm_csv = ",".join(st.session_state.uploaded_stm_names)
                params  = {"STM_FILE_NAMES": stm_csv, "VALIDATION_TYPE": v_type}
                if which == "run_all":
                    st.session_state.btn_status["run_all"] = "running"
                    st.session_state.pending_action = {
                        "kind"    : "validation",
                        "btn_key" : "run_all",
                        "category": f"Run All Validation ({v_type})",
                        "job_id"  : JOB_IDS["Run All Validation"],
                        "params"  : params,
                    }
                else:
                    st.session_state.btn_status["scd_val"] = "running"
                    st.session_state.pending_action = {
                        "kind"    : "validation",
                        "btn_key" : "scd_val",
                        "category": f"SCD Validation ({v_type})",
                        "job_id"  : JOB_IDS["SCD Validation"],
                        "params"  : params,
                    }
                st.rerun()

    if stm_clicked and has_files and not busy:
        st.session_state.btn_status["stm_val"] = "running"
        st.session_state.pending_action = {
            "kind"    : "validation",
            "btn_key" : "stm_val",
            "category": "STM Validation",
            "job_id"  : JOB_IDS["STM Validation"],
            "params"  : {"STM_FILE_NAMES": ",".join(st.session_state.uploaded_stm_names)},
        }
        st.rerun()

    if tc_clicked and has_files and not busy:
        st.session_state.btn_status["tc_gen"] = "running"
        st.session_state.pending_action = {
            "kind"    : "validation",
            "btn_key" : "tc_gen",
            "category": "Test Case Generator",
            "job_id"  : JOB_IDS["Test Case Generator"],
            "params"  : {"STM_FILE_NAMES": ",".join(st.session_state.uploaded_stm_names)},
        }
        st.rerun()


# =========================================================================
# 12. UPLOAD + SUMMARY CLICK  →  schedule pending action
# =========================================================================
if (upload_summary_clicked
        and uploaded
        and st.session_state.pending_action is None):

    files_snapshot = [{"name": f.name, "data": f.getvalue()} for f in uploaded]
    st.session_state.btn_status["upload_summary"] = "running"
    st.session_state.pending_action = {
        "kind"   : "upload_summary",
        "btn_key": "upload_summary",
        "files"  : files_snapshot,
    }
    st.rerun()


# =========================================================================
# 13. EXECUTE PENDING ACTION (upload → summary job)
# =========================================================================
if st.session_state.pending_action is not None:

    action    = st.session_state.pending_action
    btn_key   = action["btn_key"]
    dino_slot = st.empty()

    with dino_slot.container():
        components.html(DINO_GAME_HTML, height=230, scrolling=False)
        status_slot = st.empty()

    try:
        if action["kind"] == "upload_summary":
            # --- 1. upload each file straight to DBFS FileStore -----
            status_slot.info("📤 Uploading files to DBFS…")
            ok_paths, stm_names, errs = [], [], []
            for f in action["files"]:
                dbfs_path = f"{DBFS_UPLOAD_DIR}/{f['name']}"
                ok, detail = dbfs_upload_streaming(f["data"], dbfs_path)
                if ok:
                    ok_paths.append(detail)
                    stm_names.append(_clean_stm_name(f["name"]))
                else:
                    errs.append(f"{f['name']} → {detail}")

            if not ok_paths:
                raise Exception(f"Upload failed. {errs}")

            # remember what was uploaded
            st.session_state.uploaded_stm_names = sorted(set(
                st.session_state.uploaded_stm_names + stm_names))
            st.session_state.uploaded_file_paths = sorted(set(
                st.session_state.uploaded_file_paths + ok_paths))

            # --- 2. trigger summary job ---------------------------------
            ok, run_id = trigger_job(
                SUMMARY_JOB_ID,
                {"stm_file_names": ",".join(stm_names)},
            )
            if not ok:
                raise Exception(f"Summary trigger failed: {run_id}")

            state, _ = poll_until_done(run_id, status_slot, "Summary")
            log_history("Summary", SUMMARY_JOB_ID, run_id, state)
            if state != "SUCCESS":
                raise Exception(f"Summary ended with {state}")

            # --- 3. pull result JSON ------------------------------------
            data = extract_summary_list(run_id)
            if data:
                st.session_state.df_summary = pd.DataFrame(data)

            st.session_state.btn_status[btn_key] = "success"

        elif action["kind"] == "validation":
            category, job_id, params = action["category"], action["job_id"], action["params"]
            ok, run_id = trigger_job(job_id, params)
            if not ok:
                raise Exception(f"Trigger failed: {run_id}")
            state, _ = poll_until_done(run_id, status_slot, category)
            log_history(category, job_id, run_id, state)
            if state != "SUCCESS":
                raise Exception(f"{category} ended with {state}")
            st.session_state.btn_status[btn_key] = "success"

    except Exception as e:
        st.session_state.btn_status[btn_key] = "failed"
        log_history(action.get("category", action["kind"]), 0, "-", f"ERROR: {e}")

    finally:
        st.session_state.pending_action = None
        dino_slot.empty()
        st.rerun()


# =========================================================================
# 14. JOB EXECUTION HISTORY
# =========================================================================
st.divider()
st.subheader("📜 Job Execution History")

if st.session_state.job_history:
    hist = pd.DataFrame(st.session_state.job_history)

    def _row_style(row):
        s = str(row["Status"])
        if s == "SUCCESS":
            return ["background-color:#dcfce7;color:#065f46;"] * len(row)
        if s.startswith("ERROR") or s in ("FAILED", "INTERNAL_ERROR"):
            return ["background-color:#fee2e2;color:#7f1d1d;"] * len(row)
        if s in ("CANCELED", "SKIPPED"):
            return ["background-color:#fef9c3;color:#713f12;"] * len(row)
        return [""] * len(row)

    st.dataframe(
        hist.style.apply(_row_style, axis=1),
        use_container_width=True,
        hide_index=True,
    )
    if st.button("🧹 Clear history", key="clear_history"):
        st.session_state.job_history = []
        st.rerun()
else:
    st.info("No jobs executed yet.")


# =========================================================================
# 15. FOOTER
# =========================================================================
st.markdown("""
---
<center><sub>Community Edition · DBFS FileStore · Streamlit + Databricks Jobs API</sub></center>
""", unsafe_allow_html=True)
