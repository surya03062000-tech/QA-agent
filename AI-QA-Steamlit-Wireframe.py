"""
========================================================================
 End-to-End AI QA Portal   (v2)
------------------------------------------------------------------------
 - Left  : Upload Source Files   (multi-file to UC Volume)
           Single button =  Upload + Trigger Summary  (backend chains next jobs)
 - Right : Summary Viewer
 - Below : Quality Assurance  — 4 buttons
           (Run-All & SCD show INITIAL/DELTA picker)
 - Dino runner game while the job runs
 - Live button colours  : yellow running | green success | red fail
 - Job Execution History table
========================================================================
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import time
import json
from datetime import datetime

# =========================================================================
# 1. CONFIG   (move to st.secrets in production)
# =========================================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN           = "dapi180370eb25ac521baee3f96924db98e9"

VOLUME_PATH     = "/Volumes/edl_qa/qa_agent/qa_validation_input"

# Job IDs ------------------------------------------------------------------
SUMMARY_JOB_ID  = 1095682687953224          # summary job (chains next jobs on backend)

JOB_IDS = {
    "Run All Validation" : 566631342323223,
    "STM Validation"     : 190540510295693,
    "SCD Validation"     : 909635921592434,
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
st.markdown(
    """
    <style>
    .block-container { padding-top: 1.3rem; }

    /* button status colours */
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
        height: 54px;
        border-radius: 10px;
        font-weight: 600;
        font-size: 16px;
    }

    .stDataFrame { border-radius: 8px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================================
# 4. SESSION-STATE
# =========================================================================
def _init_state():
    defaults = {
        "uploaded_stm_names"  : [],
        "uploaded_file_paths" : [],
        "btn_status" : {
            "upload_summary": "idle",
            "run_all"       : "idle",
            "stm_val"       : "idle",
            "scd_val"       : "idle",
            "tc_gen"        : "idle",
        },
        "df_summary"          : pd.DataFrame(columns=["Category", "Details"]),
        "job_history"         : [],
        "pending_validation"  : None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
_init_state()


# =========================================================================
# 5. UTILITIES
# =========================================================================
def _clean_stm_name(filename: str) -> str:
    for ext in (".xlsx", ".xls", ".xlsm", ".csv", ".parquet",
                ".txt", ".json", ".tsv"):
        if filename.lower().endswith(ext):
            return filename[: -len(ext)]
    return filename


def upload_to_volume(file_obj) -> tuple[bool, str]:
    full_path = f"{VOLUME_PATH}/{file_obj.name}"
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{full_path}?overwrite=true"
    hdr = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type" : "application/octet-stream",
    }
    try:
        r = requests.put(url, headers=hdr, data=file_obj.getvalue(), timeout=120)
        if r.status_code in (200, 204):
            return True, full_path
        return False, f"{r.status_code} – {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def trigger_job(job_id: int, params: dict) -> tuple[bool, str | int]:
    payload = {"job_id": job_id, "notebook_params": params}
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.2/jobs/run-now",
            headers=HEADERS, json=payload, timeout=30,
        )
        if r.status_code != 200:
            return False, f"{r.status_code} – {r.text[:200]}"
        return True, r.json().get("run_id")
    except Exception as e:
        return False, str(e)


def get_run_details(run_id: int) -> dict:
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get",
        headers=HEADERS, params={"run_id": run_id}, timeout=30,
    )
    return r.json()


def get_notebook_output(task_run_id: int) -> dict:
    r = requests.get(
        f"{DATABRICKS_HOST}/api/2.2/jobs/runs/get-output",
        headers=HEADERS, params={"run_id": task_run_id}, timeout=30,
    )
    return r.json()


def log_history(category: str, job_id: int, run_id, status: str):
    st.session_state.job_history.append(
        {
            "Category": category, "Job ID": job_id,
            "Run ID": run_id, "Status": status,
            "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


# =========================================================================
# 6. DINO RUNNER GAME  (client-side HTML canvas)
# =========================================================================
DINO_GAME_HTML = """
<!doctype html>
<html><head>
<style>
  body { margin:0; background:#fff; font-family:'Courier New',monospace; }
  .wrap { width:100%; padding:10px 4px 4px;
          background:linear-gradient(180deg,#f8fafc 0%,#e2e8f0 100%);
          border-radius:12px; text-align:center; }
  canvas { background:#fff; border:2px solid #94a3b8; border-radius:8px; max-width:100%; }
  .hint  { color:#475569; font-size:13px; margin-top:6px; }
  .score { font-weight:700; color:#111; margin-bottom:4px; }
</style></head>
<body>
<div class="wrap">
  <div class="score">🦖 Dino Runner — Score : <span id="score">0</span>
       &nbsp;|&nbsp; High : <span id="high">0</span></div>
  <canvas id="game" width="720" height="180"></canvas>
  <div class="hint">Press <b>SPACE</b> / tap to jump · <b>↓</b> to duck · keeps you company while the Databricks job runs</div>
</div>
<script>
const cvs = document.getElementById('game');
const ctx = cvs.getContext('2d');
const W = cvs.width, H = cvs.height, GROUND = H - 30;
let dino    = { x:60, y:GROUND-44, w:44, h:44, vy:0, ducking:false };
let gravity = 0.9;
let obstacles = [], clouds = [];
let frame = 0, score = 0, speed = 6, gameOver=false, high = 0;
try { high = parseInt(localStorage.getItem('dino_high')||'0'); } catch(e){}
document.getElementById('high').textContent = high;

function jump(){ if(gameOver){ reset(); return; }
    if(dino.y >= GROUND-dino.h-1){ dino.vy = -14; } }
function duck(on){ dino.ducking = on; dino.h = on?24:44; }

document.addEventListener('keydown', e=>{
    if(e.code==='Space' || e.code==='ArrowUp'){ e.preventDefault(); jump(); }
    if(e.code==='ArrowDown'){ duck(true); }
});
document.addEventListener('keyup', e=>{ if(e.code==='ArrowDown'){ duck(false); } });
cvs.addEventListener('click', jump);
cvs.addEventListener('touchstart', e=>{ e.preventDefault(); jump(); });

function spawn(){
    if(frame % Math.max(55-Math.floor(score/80), 32) === 0){
        const big = Math.random() > 0.5;
        obstacles.push({ x: W+20, y: GROUND-(big?40:26),
                         w: big?18:14, h: big?40:26 });
    }
    if(frame % 110 === 0){ clouds.push({ x:W, y: 20+Math.random()*40 }); }
}
function reset(){ obstacles=[]; clouds=[]; frame=0; score=0; speed=6; gameOver=false;
    dino.y = GROUND-dino.h; dino.vy=0; }
function drawDino(){
    ctx.fillStyle='#111';
    ctx.fillRect(dino.x, dino.y, dino.w, dino.h);
    ctx.fillStyle='#fff';
    ctx.fillRect(dino.x+dino.w-10, dino.y+6, 4, 4);
    ctx.fillStyle='#111';
    const off = (frame%10<5)?0:4;
    if(!dino.ducking){
        ctx.fillRect(dino.x+6,  dino.y+dino.h, 8, 6-off);
        ctx.fillRect(dino.x+26, dino.y+dino.h, 8, 6+off-4);
    }
}
function loop(){
    ctx.clearRect(0,0,W,H);
    ctx.strokeStyle='#555'; ctx.lineWidth=2;
    ctx.beginPath(); ctx.moveTo(0,GROUND); ctx.lineTo(W,GROUND); ctx.stroke();
    ctx.fillStyle='#cbd5e1';
    clouds.forEach(c=>{
        ctx.beginPath();
        ctx.arc(c.x,c.y,10,0,Math.PI*2);
        ctx.arc(c.x+12,c.y-4,12,0,Math.PI*2);
        ctx.arc(c.x+24,c.y,10,0,Math.PI*2);
        ctx.fill(); c.x -= speed/3;
    });
    clouds = clouds.filter(c=>c.x>-40);
    if(!gameOver){
        dino.vy += gravity; dino.y  += dino.vy;
        if(dino.y > GROUND-dino.h){ dino.y = GROUND-dino.h; dino.vy=0; }
        spawn();
        obstacles.forEach(o=>o.x -= speed);
        obstacles = obstacles.filter(o=>o.x+o.w>0);
        for(const o of obstacles){
            if(dino.x < o.x+o.w && dino.x+dino.w > o.x &&
               dino.y < o.y+o.h && dino.y+dino.h > o.y){
                gameOver = true;
                if(score > high){
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
        ctx.fillRect(o.x-3, o.y+6,  3, 6);
        ctx.fillRect(o.x+o.w, o.y+14, 3, 6);
    });
    drawDino();
    if(gameOver){
        ctx.fillStyle='rgba(0,0,0,.7)'; ctx.fillRect(0,0,W,H);
        ctx.fillStyle='#fff'; ctx.font='bold 22px Courier New'; ctx.textAlign='center';
        ctx.fillText('GAME OVER — press SPACE / tap to restart', W/2, H/2);
    }
    requestAnimationFrame(loop);
}
loop();
</script>
</body></html>
"""


# =========================================================================
# 7. JOB RUNNER  (poll + dino + colour)
# =========================================================================
def run_job_with_game(job_id: int, params: dict, btn_key: str,
                     category: str, on_success=None):

    ok, run_id = trigger_job(job_id, params)
    if not ok:
        st.session_state.btn_status[btn_key] = "failed"
        log_history(category, job_id, "-", f"TRIGGER_FAIL: {run_id}")
        st.error(f"❌ Could not start job: {run_id}")
        return

    st.session_state.btn_status[btn_key] = "running"

    game_slot   = st.empty()
    status_slot = st.empty()

    with game_slot.container():
        st.markdown(f"#### 🎮 **{category}** is running — play the Dino while you wait!")
        components.html(DINO_GAME_HTML, height=260, scrolling=False)

    final_state, notebook_result = None, None
    job_url = f"{DATABRICKS_HOST}/#job/{job_id}/run/{run_id}"

    while True:
        info = get_run_details(run_id)
        try:
            task = info["tasks"][0]
            task_run_id = task["run_id"]
            life_cycle  = task["state"]["life_cycle_state"]
            result      = task["state"].get("result_state")
        except Exception:
            life_cycle  = info.get("state", {}).get("life_cycle_state", "UNKNOWN")
            result      = info.get("state", {}).get("result_state")
            task_run_id = run_id

        status_slot.info(f"⏳ **{category}** — `{life_cycle}`  ·  "
                         f"[open in Databricks]({job_url})")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            final_state = result or life_cycle
            if result == "SUCCESS":
                try:
                    out = get_notebook_output(task_run_id)
                    notebook_result = out.get("notebook_output", {}).get("result")
                except Exception:
                    notebook_result = None
            break

        time.sleep(6)

    game_slot.empty()
    status_slot.empty()

    if final_state == "SUCCESS":
        st.session_state.btn_status[btn_key] = "success"
        st.success(f"✅ **{category}** completed successfully "
                   f"(run {run_id}) — [open in Databricks]({job_url})")
        if on_success:
            on_success(notebook_result)
    else:
        st.session_state.btn_status[btn_key] = "failed"
        st.error(f"❌ **{category}** ended with `{final_state}` — "
                 f"[open in Databricks]({job_url})")

    log_history(category, job_id, run_id, final_state)


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
# 10. TOP ROW — Upload (left)  +  Summary Viewer (right)
# =========================================================================
left, right = st.columns([3.2, 2], gap="medium")

# ------------------------------------------- LEFT : UPLOAD
with left:
    with st.container(border=True):
        st.subheader("📂 Upload Source Files")

        uploaded = st.file_uploader(
            "Drop or browse files (parquet · csv · xlsx · xls · txt · json · tsv)",
            type=["parquet", "csv", "xlsx", "xls", "xlsm",
                  "txt", "json", "tsv"],
            accept_multiple_files=True,
            key="file_uploader_widget",
        )

        upload_summary_clicked = styled_button(
            "⬆️  Upload Files & Generate Summary",
            key="upload_summary_btn",
            status_key="upload_summary",
            disabled=not uploaded,
        )

        if not uploaded:
            st.caption("🔒 Select file(s) above to enable the button.")

        # list of already-uploaded files
        if st.session_state.uploaded_stm_names:
            with st.expander(f"📑 {len(st.session_state.uploaded_stm_names)} "
                             f"file(s) currently available", expanded=False):
                st.dataframe(
                    pd.DataFrame({
                        "STM Name (auto)" : st.session_state.uploaded_stm_names,
                        "Volume Path"     : st.session_state.uploaded_file_paths,
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
                if st.button("🗑️ Clear uploaded list", key="clear_uploads"):
                    st.session_state.uploaded_stm_names  = []
                    st.session_state.uploaded_file_paths = []
                    st.rerun()


# ------------------------------------------- RIGHT : SUMMARY VIEWER
with right:
    with st.container(border=True):
        st.subheader("📊 Summary Viewer")

        if st.session_state.df_summary.empty:
            st.caption("Click **Upload Files & Generate Summary** to populate this panel.")
            st.dataframe(
                pd.DataFrame(columns=["Category", "Details"]),
                use_container_width=True,
                height=320,
                hide_index=True,
            )
        else:
            st.dataframe(
                st.session_state.df_summary,
                use_container_width=True,
                height=440,
                hide_index=True,
            )
            st.download_button(
                "⬇️  Download Summary CSV",
                data=st.session_state.df_summary.to_csv(index=False),
                file_name="stm_summary.csv",
                mime="text/csv",
                use_container_width=True,
            )


# =========================================================================
# 11. UPLOAD + SUMMARY  (single combined action)
# =========================================================================
def _summary_success_handler(notebook_result):
    if not notebook_result:
        st.warning("Summary job finished but returned no result.")
        return
    try:
        records = json.loads(notebook_result)
        st.session_state.df_summary = pd.DataFrame(records)
        st.success(f"📊 Summary loaded — {len(records)} row(s).")
    except Exception as e:
        st.error(f"Could not parse notebook_output: {e}")
        st.code(str(notebook_result)[:500])


if upload_summary_clicked and uploaded:
    # --- 1. upload every file to the volume -----------------------
    prog = st.progress(0.0, text="Uploading…")
    good_names, good_paths, errors = [], [], []

    for i, f in enumerate(uploaded, start=1):
        ok, detail = upload_to_volume(f)
        if ok:
            good_names.append(_clean_stm_name(f.name))
            good_paths.append(detail)
        else:
            errors.append(f"{f.name} → {detail}")
        prog.progress(i / len(uploaded),
                      text=f"Uploaded {i}/{len(uploaded)}")
    prog.empty()

    if errors:
        with st.expander("⚠️ Some uploads failed"):
            for e in errors:
                st.code(e)

    if not good_names:
        st.session_state.btn_status["upload_summary"] = "failed"
        st.error("No files uploaded — cannot start Summary.")
    else:
        # merge with anything previously uploaded
        st.session_state.uploaded_stm_names = sorted(set(
            st.session_state.uploaded_stm_names + good_names))
        st.session_state.uploaded_file_paths = sorted(set(
            st.session_state.uploaded_file_paths + good_paths))

        st.success(f"✅ Uploaded {len(good_names)} file(s) to volume. "
                   "Triggering Summary job…")

        # --- 2. trigger summary job (backend will chain next jobs) -
        stm_csv = ",".join(st.session_state.uploaded_stm_names)
        run_job_with_game(
            SUMMARY_JOB_ID,
            {"stm_file_names": stm_csv},
            "upload_summary",
            "Upload + Summary",
            on_success=_summary_success_handler,
        )
        st.rerun()


# =========================================================================
# 12. QUALITY ASSURANCE  (4 buttons)
# =========================================================================
with st.container(border=True):
    st.subheader("🧪 Quality Assurance")

    has_files = bool(st.session_state.uploaded_stm_names)
    if not has_files:
        st.info("Upload files first to enable the validation buttons.")

    # Row-1 : Run All Validation
    run_all_clicked = styled_button(
        "▶  Run All Validation",
        key="run_all_btn",
        status_key="run_all",
        disabled=not has_files,
    )

    # Row-2 : STM / SCD / TC-Gen
    b1, b2, b3 = st.columns(3)
    with b1:
        stm_clicked = styled_button(
            "🔍 STM Validation",
            key="stm_val_btn",
            status_key="stm_val",
            disabled=not has_files,
        )
    with b2:
        scd_clicked = styled_button(
            "🔁 SCD Validation",
            key="scd_val_btn",
            status_key="scd_val",
            disabled=not has_files,
        )
    with b3:
        tc_clicked = styled_button(
            "🧬 Test Case Generator",
            key="tc_gen_btn",
            status_key="tc_gen",
            disabled=not has_files,
        )

    # -- INITIAL / DELTA pop-up -----------------------------------
    if run_all_clicked:
        st.session_state.pending_validation = "run_all"
    if scd_clicked:
        st.session_state.pending_validation = "scd_val"

    if st.session_state.pending_validation in ("run_all", "scd_val"):
        with st.container(border=True):
            cat = ("Run All Validation"
                   if st.session_state.pending_validation == "run_all"
                   else "SCD Validation")
            st.markdown(f"#### Choose Validation Type for **{cat}**")
            v_type = st.radio(
                "Validation Type",
                ["INITIAL", "DELTA"],
                horizontal=True,
                key="v_type_radio",
            )
            cc1, cc2, _ = st.columns([1, 1, 3])
            confirm = cc1.button("✅ Confirm & Run", key="v_type_confirm")
            cancel  = cc2.button("Cancel",         key="v_type_cancel")

            if cancel:
                st.session_state.pending_validation = None
                st.rerun()

            if confirm:
                which = st.session_state.pending_validation
                st.session_state.pending_validation = None
                stm_csv = ",".join(st.session_state.uploaded_stm_names)
                params  = {
                    "STM_FILE_NAMES" : stm_csv,
                    "VALIDATION_TYPE": v_type,
                }
                if which == "run_all":
                    run_job_with_game(
                        JOB_IDS["Run All Validation"], params,
                        "run_all", f"Run All Validation ({v_type})",
                    )
                else:
                    run_job_with_game(
                        JOB_IDS["SCD Validation"], params,
                        "scd_val", f"SCD Validation ({v_type})",
                    )
                st.rerun()

    # -- direct buttons -------------------------------------------
    if stm_clicked:
        params = {"STM_FILE_NAMES": ",".join(st.session_state.uploaded_stm_names)}
        run_job_with_game(JOB_IDS["STM Validation"], params,
                          "stm_val", "STM Validation")
        st.rerun()

    if tc_clicked:
        params = {"STM_FILE_NAMES": ",".join(st.session_state.uploaded_stm_names)}
        run_job_with_game(JOB_IDS["Test Case Generator"], params,
                          "tc_gen", "Test Case Generator")
        st.rerun()


# =========================================================================
# 13. JOB EXECUTION HISTORY
# =========================================================================
st.divider()
st.subheader("📜 Job Execution History")

if st.session_state.job_history:
    hist = pd.DataFrame(st.session_state.job_history)

    def _row_style(row):
        col = ""
        if row["Status"] == "SUCCESS":
            col = "background-color:#dcfce7;color:#065f46;"
        elif row["Status"] in ("FAILED", "INTERNAL_ERROR") \
                or str(row["Status"]).startswith("TRIGGER_FAIL"):
            col = "background-color:#fee2e2;color:#7f1d1d;"
        elif row["Status"] in ("CANCELED", "SKIPPED"):
            col = "background-color:#fef9c3;color:#713f12;"
        return [col] * len(row)

    st.dataframe(
        hist.style.apply(_row_style, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    cdl1, cdl2 = st.columns([1, 5])
    with cdl1:
        st.download_button(
            "⬇️  Download",
            data=hist.to_csv(index=False),
            file_name="job_execution_history.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with cdl2:
        if st.button("🧹 Clear history", key="clear_history"):
            st.session_state.job_history = []
            st.rerun()
else:
    st.info("No jobs executed yet. Trigger any button above to see history here.")


# =========================================================================
# 14. FOOTER
# =========================================================================
st.markdown("""
---
<center><sub>Built with ❤️ on Streamlit + Databricks Jobs API · Unity-Catalog Volumes</sub></center>
""", unsafe_allow_html=True)
