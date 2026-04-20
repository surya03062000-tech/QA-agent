"""
========================================================================
 End-to-End AI QA Portal   (v5 — progress-phases, dino-free)
------------------------------------------------------------------------
 CHANGES vs v4:
   * Removed the dinosaur runner entirely
   * Added a phase-tracker card: shows the current phase in professional
     wording + a percentage, animated as the job progresses
   * Each job has its own phase list (STM / SCD / Test-Case / Run-All)
   * Summary Viewer now:
        - hides the 'Version.History' sheet
        - hides Category rows 'PII Present', 'Temporal Columns',
          'Nullability', and 'Extraction Mode'
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
# 1. CONFIG   (move to st.secrets in production!)
# =========================================================================
DATABRICKS_HOST = "https://dbc-927300a1-adc8.cloud.databricks.com"
TOKEN           = "dapi180370eb25ac521baee3f96924db98e9"

WORKSPACE_UPLOAD_DIR = "/Shared/qa_uploads"
VOLUME_PATH          = "/Volumes/edl_qa/qa_agent/qa_validation_input"

# Job IDs -----------------------------------------------------------------
FILE_COPY_JOB_ID = 1095682687953224        # copies workspace → volume
SUMMARY_JOB_ID   = 29471425720129          # STM summarizer

JOB_IDS = {
    "Run All Validation" : 566631342323223,
    "STM Validation"     : 190540510295693,
    "SCD Validation"     : 909635921592434,
    "Test Case Generator": 160480032307967,
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
    layout="wide",
)

# =========================================================================
# 3. CSS
# =========================================================================
st.markdown("""
<style>
/* ---- status-aware button colours (unchanged from v4) -------------- */
.btn-idle    button { background:#2563EB !important; color:white !important; }
.btn-running button { background:#FACC15 !important; color:#111 !important;
                      animation: pulse 1.2s infinite; }
.btn-success button { background:#16A34A !important; color:white !important; }
.btn-failed  button { background:#DC2626 !important; color:white !important; }
@keyframes pulse { 0%{opacity:1;} 50%{opacity:.55;} 100%{opacity:1;} }

/* ---- progress tracker card --------------------------------------- */
.progress-card {
    background: linear-gradient(135deg, #f8fafc 0%, #eff6ff 100%);
    border: 1px solid #cbd5e1;
    border-radius: 12px;
    padding: 18px 22px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.04);
    font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
    color: #1f2937;
    margin: 12px 0;
}
.progress-card .pc-title {
    font-size: 15px;
    font-weight: 700;
    color: #1F4E79;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.progress-card .pc-phase {
    font-size: 13px;
    color: #374151;
    margin-bottom: 8px;
}
.progress-card .pc-phase b { color: #1F4E79; }
.progress-card .pc-bar-wrap {
    background: #e5e7eb;
    border-radius: 999px;
    overflow: hidden;
    height: 14px;
    margin-bottom: 6px;
    position: relative;
}
.progress-card .pc-bar-fill {
    background: linear-gradient(90deg, #3b82f6, #2563eb);
    height: 100%;
    transition: width 0.6s ease;
    border-radius: 999px;
}
.progress-card .pc-pct {
    font-size: 12px;
    color: #64748b;
    text-align: right;
    font-weight: 600;
}
.progress-card .pc-done-icon  { color: #16A34A; font-weight: 700; }
.progress-card .pc-active-icon { color: #EA580C; font-weight: 700; }
.progress-card .pc-pending-icon { color: #9ca3af; }

.progress-card ul.pc-steps {
    list-style: none;
    padding: 0;
    margin: 10px 0 0 0;
    font-size: 12px;
}
.progress-card ul.pc-steps li {
    padding: 3px 0;
    color: #4b5563;
}
.progress-card ul.pc-steps li.done   { color: #16A34A; }
.progress-card ul.pc-steps li.active {
    color: #0f172a;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

# =========================================================================
# 4. SESSION-STATE BOOTSTRAP
# =========================================================================
if "df_summary" not in st.session_state:
    st.session_state.df_summary = pd.DataFrame()
if "job_history" not in st.session_state:
    st.session_state.job_history = []
if "uploaded_stm_names" not in st.session_state:
    st.session_state.uploaded_stm_names = []
if "uploaded_file_paths" not in st.session_state:
    st.session_state.uploaded_file_paths = []
if "pending_action" not in st.session_state:
    st.session_state.pending_action = None
if "pending_validation" not in st.session_state:
    st.session_state.pending_validation = None
if "btn_status" not in st.session_state:
    st.session_state.btn_status = {
        "upload_summary": "idle",
        "run_all"       : "idle",
        "stm_val"       : "idle",
        "scd_val"       : "idle",
        "tc_gen"        : "idle",
    }

# =========================================================================
# 5. DATABRICKS API HELPERS
# =========================================================================
def _clean_stm_name(fname: str) -> str:
    return fname.rsplit("/", 1)[-1].rsplit(".", 1)[0]


def upload_to_workspace(fname: str, data: bytes) -> tuple[bool, str]:
    target = f"{WORKSPACE_UPLOAD_DIR}/{fname}"
    body = {
        "path":      target,
        "format":    "AUTO",
        "overwrite": True,
        "content":   base64.b64encode(data).decode(),
    }
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/workspace/import",
            headers=HEADERS, json=body, timeout=120,
        )
        if r.status_code == 200:
            return True, target
        return False, f"{r.status_code}: {r.text[:400]}"
    except Exception as e:
        return False, str(e)


def trigger_job(job_id: int, params: dict) -> tuple[bool, int | str]:
    body = {"job_id": job_id, "notebook_params": params}
    try:
        r = requests.post(
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
            headers=HEADERS, json=body, timeout=30,
        )
        if r.status_code == 200:
            return True, r.json()["run_id"]
        return False, f"{r.status_code}: {r.text[:400]}"
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
# 6. PROGRESS TRACKER — phase list + percentage renderer
# =========================================================================
# Each kind maps to a list of (label, weight) phases. Weights are relative —
# the renderer normalises them so the full bar = 100%.
PHASE_PLANS = {
    "File Copy + Summary": [
        ("Uploading STM file(s) to workspace",          1.0),
        ("Staging files to Unity Catalog volume",       1.5),
        ("Parsing STM structure and metadata",          2.0),
        ("Extracting column inventory per layer",       1.5),
        ("Compiling summary report",                    1.0),
    ],
    "STM Validation": [
        ("Parsing STM workbook and metadata blocks",    1.0),
        ("Resolving source / target data artifacts",    1.0),
        ("Validating RAW source against CSV file",      2.0),
        ("Validating RAW target against Parquet file",  2.0),
        ("Validating STD_RAW source (Parquet + audit)", 2.0),
        ("Validating STD_RAW target (Databricks table)",2.0),
        ("Validating CURATED source (Databricks table)",1.5),
        ("Validating CURATED target (Databricks table)",1.5),
        ("Generating formatted Excel report",           1.0),
        ("Rendering dashboard and delivering email",    1.0),
    ],
    "SCD Validation": [
        ("Parsing STM and resolving target tables",     1.0),
        ("Row count validation (source vs target)",     1.5),
        ("Null validation on key columns",              1.5),
        ("Sum / min / max aggregate validation",        2.0),
        ("Primary-key uniqueness validation",           1.5),
        ("Data validation (column-level comparison)",   2.5),
        ("Audit-column checks (insert / update / run)", 1.5),
        ("SCD2 history validation",                     2.0),
        ("Compiling formatted Excel report",            1.0),
    ],
    "Test Case Generator": [
        ("Parsing STM and extracting business rules",   1.0),
        ("Deriving candidate test scenarios",           2.0),
        ("Generating test cases per validation type",   2.5),
        ("Formatting test-case workbook",               1.5),
        ("Finalising output and delivering artifacts",  1.0),
    ],
    "Run All Validation": [
        ("Parsing STM and staging data artifacts",      1.0),
        ("Running STM layer validations (RAW/STD/CUR)", 3.0),
        ("Running SCD validations (counts, nulls, PK)", 3.0),
        ("Running data and audit-column validations",   2.5),
        ("Generating test cases",                       2.0),
        ("Consolidating reports and delivering email",  1.5),
    ],
}


class ProgressTracker:
    """Renders a progress card with current phase + percentage.

    Usage:
        tracker = ProgressTracker(slot, "STM Validation")
        tracker.start()
        ...  # tracker auto-advances phases based on elapsed time
        tracker.done()    # or tracker.fail("reason")
    """
    def __init__(self, slot, kind: str):
        self.slot   = slot
        self.kind   = kind
        self.phases = PHASE_PLANS.get(kind, [(kind, 1.0)])
        total_w     = sum(w for _, w in self.phases) or 1.0
        # cumulative % after each phase completes
        cum = 0.0
        self.thresholds = []
        for (_, w) in self.phases:
            cum += w / total_w * 100.0
            self.thresholds.append(cum)
        self.start_ts   = None
        # expected wall-clock duration (seconds) — drives the phase advance.
        # Real jobs may run longer; poll_until_done still gates the real completion.
        self.expected_sec = 45.0

    def start(self):
        self.start_ts = time.time()
        self._render(pct=0, phase_idx=0)

    def _phase_from_pct(self, pct: float) -> int:
        for i, t in enumerate(self.thresholds):
            if pct <= t:
                return i
        return len(self.phases) - 1

    def tick(self):
        """Auto-advance based on elapsed time. Caps at 95% until done() is called
        so the final 5% shows only after the real job finishes."""
        if self.start_ts is None:
            return
        elapsed = time.time() - self.start_ts
        pct = min(95.0, elapsed / self.expected_sec * 95.0)
        self._render(pct=pct, phase_idx=self._phase_from_pct(pct))

    def done(self):
        self._render(pct=100.0, phase_idx=len(self.phases) - 1, terminal="success")

    def fail(self, message: str = ""):
        # freeze at current pct
        elapsed = time.time() - (self.start_ts or time.time())
        pct = min(95.0, elapsed / self.expected_sec * 95.0)
        self._render(pct=pct, phase_idx=self._phase_from_pct(pct),
                     terminal="failed", message=message)

    def clear(self):
        self.slot.empty()

    def _render(self, pct: float, phase_idx: int,
                terminal: str = "", message: str = ""):
        """Repaint the card with current phase + pct + step list."""
        # Build step list
        steps_html = ""
        for i, (label, _) in enumerate(self.phases):
            if terminal == "success" or i < phase_idx:
                cls = "done"; icon = '<span class="pc-done-icon">✓</span>'
            elif i == phase_idx:
                cls = "active"; icon = '<span class="pc-active-icon">▶</span>'
            else:
                cls = "pending"; icon = '<span class="pc-pending-icon">○</span>'
            steps_html += f'<li class="{cls}">{icon}&nbsp;&nbsp;{label}</li>'

        # Current phase text
        if terminal == "success":
            phase_text = "Completed successfully."
        elif terminal == "failed":
            phase_text = f"Failed: {message or 'see history for details'}"
        elif phase_idx < len(self.phases):
            phase_text = f"<b>Now:</b> {self.phases[phase_idx][0]}"
        else:
            phase_text = ""

        # Pct label
        pct_int = int(round(pct))

        header_icon = "✓" if terminal == "success" else ("⚠" if terminal == "failed" else "⏳")
        header_color = ("#16A34A" if terminal == "success"
                        else "#DC2626" if terminal == "failed" else "#1F4E79")

        html = f"""
<div class="progress-card">
  <div class="pc-title">
    <span style="color:{header_color};">{header_icon}&nbsp;&nbsp;{self.kind}</span>
    <span style="font-size:12px;color:#64748b;font-weight:500;">
      {pct_int}% complete
    </span>
  </div>
  <div class="pc-phase">{phase_text}</div>
  <div class="pc-bar-wrap">
    <div class="pc-bar-fill" style="width:{pct_int}%;"></div>
  </div>
  <ul class="pc-steps">
    {steps_html}
  </ul>
</div>
"""
        self.slot.markdown(html, unsafe_allow_html=True)


def poll_until_done(run_id: int, tracker: ProgressTracker | None = None,
                    label: str = "") -> tuple[str, int]:
    """Poll a run until it reaches a terminal state. Refreshes tracker every 3s."""
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


def extract_summary_list(run_id: int):
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


def log_history(category: str, job_id: int, run_id, status: str):
    st.session_state.job_history.append({
        "Category": category, "Job ID": job_id,
        "Run ID"  : run_id,   "Status": status,
        "Time"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


# =========================================================================
# 7. SUMMARY FILTERING — drop ignored sheets and categories
# =========================================================================
IGNORED_SHEETS     = {"Version.History", "version.history"}
IGNORED_CATEGORIES = {"PII Present", "Temporal Columns",
                      "Nullability", "Extraction Mode"}


def _apply_summary_filters(rows: list[dict]) -> list[dict]:
    """Remove rows belonging to ignored sheets or ignored categories.
    Matching is case-insensitive and resilient to small column-name variants
    (Sheet / Sheet Name / sheet_name ; Category / category)."""
    if not rows:
        return rows

    # Discover actual column keys on first row
    sheet_keys = [k for k in rows[0].keys()
                  if k.lower().replace("_", " ").strip() in ("sheet", "sheet name")]
    cat_keys   = [k for k in rows[0].keys()
                  if k.lower().strip() == "category"]

    filtered = []
    for r in rows:
        sheet_val = next((str(r.get(k, "")).strip() for k in sheet_keys), "")
        cat_val   = next((str(r.get(k, "")).strip() for k in cat_keys),   "")
        if sheet_val and sheet_val.lower() in (s.lower() for s in IGNORED_SHEETS):
            continue
        if cat_val and cat_val in IGNORED_CATEGORIES:
            continue
        filtered.append(r)
    return filtered


# =========================================================================
# 8. STYLED BUTTON HELPER
# =========================================================================
def styled_button(label, key, status_key, disabled=False, use_container_width=True):
    status = st.session_state.btn_status.get(status_key, "idle")
    cls = {
        "idle"    : "btn-idle",
        "running" : "btn-running",
        "success" : "btn-success",
        "failed"  : "btn-failed",
    }.get(status, "btn-idle")

    st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
    clicked = st.button(label, key=key, disabled=disabled,
                        use_container_width=use_container_width)
    st.markdown("</div>", unsafe_allow_html=True)
    return clicked


# =========================================================================
# 9. TOP-OF-PAGE LAYOUT
# =========================================================================
st.title("End-to-End AI Quality-Assurance Portal")
st.caption("Upload STM file(s) → generate summary → run validations.")

PANEL_HEIGHT = 420
left, right = st.columns([1, 1], gap="medium")

# ------------------------ LEFT : UPLOAD + SUMMARY GENERATOR ----------------
with left:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("📤 Upload Files")

        uploaded = st.file_uploader(
            "STM file(s) (.xlsx)",
            accept_multiple_files=True, type=["xlsx"],
        )

        busy = st.session_state.pending_action is not None
        can_upload = bool(uploaded) and not busy

        if uploaded:
            names = [f.name for f in uploaded]
            st.caption(f"Selected: {', '.join(names)}")

        up_clicked = styled_button(
            "⬆  Upload Files & Generate Summary",
            key="upload_summary_btn",
            status_key="upload_summary",
            disabled=not can_upload,
        )

        with st.expander("Uploaded so far", expanded=False):
            if st.session_state.uploaded_stm_names:
                st.write(st.session_state.uploaded_stm_names)
                if st.button("Clear uploaded list", key="clear_upl"):
                    st.session_state.uploaded_stm_names  = []
                    st.session_state.uploaded_file_paths = []
                    st.rerun()
            else:
                st.caption("No files yet.")

# ------------------------ RIGHT : SUMMARY VIEWER (grouped by STM + sheet) ----
with right:
    with st.container(border=True, height=PANEL_HEIGHT):
        st.subheader("Summary Viewer")

        if st.session_state.df_summary.empty:
            st.caption("Upload file(s) and click **Generate Summary** to populate this panel.")
        else:
            df_sv = st.session_state.df_summary

            # ----- identify the grouping columns (STM file + sheet) -------
            def _find_col(df, *candidates):
                """Return the first df column whose normalised name matches
                any of the candidate names (case/underscore/space-insensitive)."""
                wanted = {c.lower().replace("_", " ").strip() for c in candidates}
                for col in df.columns:
                    if col.lower().replace("_", " ").strip() in wanted:
                        return col
                return None

            stm_col   = _find_col(df_sv, "STM File", "STM", "STM Name",
                                  "STM File Name", "File")
            sheet_col = _find_col(df_sv, "Sheet", "Sheet Name")

            # Columns to show in each per-sheet table — everything except
            # the two grouping columns.
            display_cols = [c for c in df_sv.columns
                            if c not in (stm_col, sheet_col)]

            # ----- fallback: if we can't find sheet/stm cols, show flat -----
            if not sheet_col and not stm_col:
                st.dataframe(df_sv, use_container_width=True, hide_index=True)
            else:
                # Group preserving original row order
                group_keys = []
                groups = {}
                for _, row in df_sv.iterrows():
                    stm_val   = str(row[stm_col]).strip()   if stm_col   else ""
                    sheet_val = str(row[sheet_col]).strip() if sheet_col else ""
                    key = (stm_val, sheet_val)
                    if key not in groups:
                        groups[key] = []
                        group_keys.append(key)
                    groups[key].append(row)

                # Render each group as: title bar + table of content columns
                for i, (stm_val, sheet_val) in enumerate(group_keys):
                    if stm_val and sheet_val:
                        title = f"📄 {stm_val}  ·  Sheet: {sheet_val}"
                    elif sheet_val:
                        title = f"📄 Sheet: {sheet_val}"
                    else:
                        title = f"📄 {stm_val or 'Summary'}"

                    st.markdown(
                        f"<div style='background:#1F4E79;color:#fff;"
                        f"padding:8px 14px;border-radius:6px 6px 0 0;"
                        f"font:600 13px -apple-system,Segoe UI,Roboto,Arial;"
                        f"margin-top:{'14px' if i > 0 else '0'};'>"
                        f"{title}</div>",
                        unsafe_allow_html=True,
                    )

                    sub_df = pd.DataFrame(groups[(stm_val, sheet_val)])[display_cols]
                    st.dataframe(
                        sub_df,
                        use_container_width=True,
                        hide_index=True,
                    )


# =========================================================================
# 10. QUALITY ASSURANCE
# =========================================================================
with st.container(border=True):
    st.subheader("Quality Assurance")

    has_files     = bool(st.session_state.uploaded_stm_names)
    busy          = st.session_state.pending_action is not None
    common_disable = (not has_files) or busy

    if not has_files:
        st.info("Upload files first to enable the validation buttons.")

    run_all_clicked = styled_button(
        "▶  Run All Validation",
        key="run_all_btn", status_key="run_all",
        disabled=common_disable,
    )

    b1, b2, b3 = st.columns(3)
    with b1:
        stm_clicked = styled_button(
            "STM Validation",
            key="stm_val_btn", status_key="stm_val",
            disabled=common_disable,
        )
    with b2:
        scd_clicked = styled_button(
            "SCD Validation",
            key="scd_val_btn", status_key="scd_val",
            disabled=common_disable,
        )
    with b3:
        tc_clicked = styled_button(
            "Test Case Generator",
            key="tc_gen_btn", status_key="tc_gen",
            disabled=common_disable,
        )

    # ---- INITIAL / DELTA picker ------------------------------------
    if run_all_clicked:
        st.session_state.pending_validation = "run_all"
    if scd_clicked:
        st.session_state.pending_validation = "scd_val"

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

            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm and Run", key="confirm_vtype",
                             use_container_width=True):
                    stm_csv = ",".join(st.session_state.uploaded_stm_names)
                    params  = {
                        "STM_FILE_NAMES": stm_csv,
                        "VALIDATION_TYPE": v_type,
                    }
                    if st.session_state.pending_validation == "run_all":
                        st.session_state.btn_status["run_all"] = "running"
                        st.session_state.pending_action = {
                            "kind"    : "validation",
                            "category": "Run All Validation",
                            "job_id"  : JOB_IDS["Run All Validation"],
                            "params"  : params,
                            "btn_key" : "run_all",
                        }
                    else:
                        st.session_state.btn_status["scd_val"] = "running"
                        st.session_state.pending_action = {
                            "kind"    : "validation",
                            "category": "SCD Validation",
                            "job_id"  : JOB_IDS["SCD Validation"],
                            "params"  : params,
                            "btn_key" : "scd_val",
                        }
                    st.session_state.pending_validation = None
                    st.rerun()
            with c2:
                if st.button("Cancel", key="cancel_vtype",
                             use_container_width=True):
                    st.session_state.pending_validation = None
                    st.rerun()

    # ---- STM Validation (no INITIAL/DELTA choice needed) ------------
    if stm_clicked and not busy:
        stm_csv = ",".join(st.session_state.uploaded_stm_names)
        st.session_state.btn_status["stm_val"] = "running"
        st.session_state.pending_action = {
            "kind"    : "validation",
            "category": "STM Validation",
            "job_id"  : JOB_IDS["STM Validation"],
            "params"  : {"STM_FILE_NAMES": stm_csv},
            "btn_key" : "stm_val",
        }
        st.rerun()

    # ---- Test Case Generator ----------------------------------------
    if tc_clicked and not busy:
        stm_csv = ",".join(st.session_state.uploaded_stm_names)
        st.session_state.btn_status["tc_gen"] = "running"
        st.session_state.pending_action = {
            "kind"    : "validation",
            "category": "Test Case Generator",
            "job_id"  : JOB_IDS["Test Case Generator"],
            "params"  : {"STM_FILE_NAMES": stm_csv},
            "btn_key" : "tc_gen",
        }
        st.rerun()


# =========================================================================
# 11. UPLOAD & GENERATE SUMMARY — handler
# =========================================================================
if up_clicked and can_upload:
    files_snapshot = [
        {"name": f.name, "data": f.getvalue()} for f in uploaded
    ]
    st.session_state.btn_status["upload_summary"] = "running"
    st.session_state.pending_action = {
        "kind"    : "upload_summary",
        "btn_key" : "upload_summary",
        "files"   : files_snapshot,
    }
    st.rerun()


# =========================================================================
# 12. PENDING-ACTION EXECUTOR (with progress tracker)
# =========================================================================
if st.session_state.pending_action is not None:

    action    = st.session_state.pending_action
    btn_key   = action["btn_key"]
    tracker_slot = st.empty()

    # Pick the phase plan based on the action kind
    if action["kind"] == "upload_summary":
        tracker = ProgressTracker(tracker_slot, "File Copy + Summary")
    else:
        tracker = ProgressTracker(tracker_slot, action["category"])
    tracker.start()

    try:
        if action["kind"] == "upload_summary":
            files = action["files"]

            # Phase 1 — upload each file to the workspace
            ws_paths, stm_names, errs = [], [], []
            for f in files:
                ok, detail = upload_to_workspace(f["name"], f["data"])
                if ok:
                    ws_paths.append(detail)
                    stm_names.append(_clean_stm_name(f["name"]))
                else:
                    errs.append(f"{f['name']} → {detail}")
                tracker.tick()

            if not ws_paths:
                raise Exception(f"Upload failed: {errs}")

            # Phase 2-3 — File-Copy job
            ok, copy_run = trigger_job(
                FILE_COPY_JOB_ID,
                {"workspace_file_paths": ",".join(ws_paths)},
            )
            if not ok:
                raise Exception(f"File-Copy trigger failed: {copy_run}")

            copy_state, _ = poll_until_done(copy_run, tracker, "File-Copy")
            log_history("File Copy", FILE_COPY_JOB_ID, copy_run, copy_state)
            if copy_state != "SUCCESS":
                raise Exception(f"File-Copy ended with {copy_state}")

            # remember uploaded files in session
            st.session_state.uploaded_stm_names = sorted(set(
                st.session_state.uploaded_stm_names + stm_names))
            st.session_state.uploaded_file_paths = sorted(set(
                st.session_state.uploaded_file_paths + ws_paths))

            # Phase 4-5 — Summary job
            ok, sum_run = trigger_job(
                SUMMARY_JOB_ID,
                {"stm_file_names": ",".join(stm_names)},
            )
            if not ok:
                raise Exception(f"Summary trigger failed: {sum_run}")

            sum_state, _ = poll_until_done(sum_run, tracker, "Summary")
            log_history("Summary", SUMMARY_JOB_ID, sum_run, sum_state)
            if sum_state != "SUCCESS":
                raise Exception(f"Summary ended with {sum_state}")

            # Pull summary JSON and apply filters
            data = extract_summary_list(sum_run)
            if data:
                data = _apply_summary_filters(data)
                if data:
                    st.session_state.df_summary = pd.DataFrame(data)

            st.session_state.btn_status[btn_key] = "success"
            tracker.done()

        elif action["kind"] == "validation":
            category = action["category"]
            job_id   = action["job_id"]
            params   = action["params"]

            ok, run_id = trigger_job(job_id, params)
            if not ok:
                raise Exception(f"Trigger failed: {run_id}")

            state, _ = poll_until_done(run_id, tracker, category)
            log_history(category, job_id, run_id, state)
            if state != "SUCCESS":
                raise Exception(f"{category} ended with {state}")

            st.session_state.btn_status[btn_key] = "success"
            tracker.done()

    except Exception as e:
        st.session_state.btn_status[btn_key] = "failed"
        tracker.fail(str(e)[:120])
        log_history(action.get("category", action["kind"]), 0, "-", f"ERROR: {e}")

    finally:
        st.session_state.pending_action = None
        # Small pause so the final state (success / failure) is visible
        time.sleep(1.5)
        tracker.clear()
        st.rerun()


# =========================================================================
# 13. JOB EXECUTION HISTORY
# =========================================================================
st.divider()
st.subheader("Job Execution History")

if st.session_state.job_history:
    hist = pd.DataFrame(st.session_state.job_history)
    st.dataframe(hist, use_container_width=True, hide_index=True)
else:
    st.caption("No jobs run yet.")
