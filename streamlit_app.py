# ============================================================
# streamlit_app.py — STM Document Summarizer UI
# ============================================================
# Run:  streamlit run streamlit_app.py
# ============================================================

import streamlit as st
import time
import json
from pathlib import Path
from databricks_utils import (
    dbfs_upload, dbfs_delete, submit_run, poll_until_done, dbfs_read
)
from config import (
    SUPPORTED_EXTENSIONS, MAX_FILE_SIZE_MB,
    DBFS_INPUT_DIR, DBFS_OUTPUT_DIR
)

# ── Page Config ───────────────────────────────────────────
st.set_page_config(
    page_title="STM Document Summarizer",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    /* Page background */
    .stApp {
        background: linear-gradient(135deg, #0f1117 0%, #1a1f2e 50%, #0f1117 100%);
    }

    /* Header */
    .hero-title {
        font-family: 'DM Serif Display', serif;
        font-size: 2.8rem;
        background: linear-gradient(90deg, #60efff, #0061ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .hero-subtitle {
        color: #8892a4;
        font-size: 1rem;
        font-weight: 300;
        letter-spacing: 0.05em;
    }

    /* Upload zone */
    .upload-zone {
        border: 2px dashed #2d3748;
        border-radius: 16px;
        padding: 2.5rem;
        text-align: center;
        background: rgba(255,255,255,0.02);
        transition: border-color 0.3s;
    }
    .upload-zone:hover { border-color: #60efff; }

    /* Step cards */
    .step-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    .step-icon { font-size: 1.4rem; min-width: 2rem; text-align: center; }
    .step-label { color: #c9d1e0; font-size: 0.9rem; font-weight: 500; }
    .step-active  { border-color: #60efff44; background: rgba(96,239,255,0.04); }
    .step-done    { border-color: #38ef7d44; background: rgba(56,239,125,0.04); }
    .step-error   { border-color: #ff6b6b44; background: rgba(255,107,107,0.04); }

    /* Summary output */
    .summary-box {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(96,239,255,0.2);
        border-radius: 16px;
        padding: 2rem;
        margin-top: 1.5rem;
        line-height: 1.85;
        color: #dce3ee;
        font-size: 0.97rem;
    }
    .summary-header {
        font-family: 'DM Serif Display', serif;
        color: #60efff;
        font-size: 1.3rem;
        margin-bottom: 1rem;
        border-bottom: 1px solid rgba(96,239,255,0.15);
        padding-bottom: 0.6rem;
    }
    .meta-chip {
        display: inline-block;
        background: rgba(96,239,255,0.1);
        color: #60efff;
        border-radius: 20px;
        padding: 0.25rem 0.75rem;
        font-size: 0.78rem;
        margin-right: 0.5rem;
        margin-bottom: 0.5rem;
        border: 1px solid rgba(96,239,255,0.2);
    }

    /* Sidebar */
    .sidebar-section {
        background: rgba(255,255,255,0.04);
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        border: 1px solid rgba(255,255,255,0.07);
    }
    .sidebar-title {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #8892a4;
        margin-bottom: 0.6rem;
    }

    /* Streamlit overrides */
    .stButton > button {
        background: linear-gradient(90deg, #0061ff, #60efff);
        color: #0f1117;
        font-weight: 600;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 2rem;
        font-size: 1rem;
        width: 100%;
        transition: opacity 0.2s;
    }
    .stButton > button:hover { opacity: 0.88; }

    div[data-testid="stFileUploader"] {
        border-radius: 12px;
    }
    .stProgress > div > div { background: linear-gradient(90deg, #0061ff, #60efff); }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────
with st.sidebar:
    st.markdown('<p class="hero-title" style="font-size:1.6rem">⚙️ Settings</p>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<p class="sidebar-title">🔗 Databricks</p>', unsafe_allow_html=True)

    from config import DATABRICKS_HOST, DATABRICKS_TOKEN, NOTEBOOK_PATH
    st.code(DATABRICKS_HOST.replace("https://", ""), language=None)
    token_masked = DATABRICKS_TOKEN[:6] + "..." if len(DATABRICKS_TOKEN) > 6 else "Not set"
    st.caption(f"Token: `{token_masked}`")
    st.caption(f"Notebook: `{NOTEBOOK_PATH}`")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<p class="sidebar-title">📄 Supported Formats</p>', unsafe_allow_html=True)
    for ext in SUPPORTED_EXTENSIONS:
        st.markdown(f"• `{ext}`")
    st.caption(f"Max size: **{MAX_FILE_SIZE_MB} MB**")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<p class="sidebar-title">🔄 Pipeline Steps</p>', unsafe_allow_html=True)
    st.caption("1. Upload → DBFS")
    st.caption("2. Trigger Databricks Notebook")
    st.caption("3. Extract & Clean Text")
    st.caption("4. Call LLM Endpoint")
    st.caption("5. Fetch & Display Summary")
    st.markdown("</div>", unsafe_allow_html=True)


# ── Main Page ─────────────────────────────────────────────
st.markdown('<h1 class="hero-title">🧬 STM Document Summarizer</h1>', unsafe_allow_html=True)
st.markdown('<p class="hero-subtitle">Scientific · Technical · Medical document analysis powered by Databricks</p>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

col_upload, col_pipeline = st.columns([1.6, 1], gap="large")

with col_upload:
    st.markdown("### 📂 Upload Document")
    uploaded_file = st.file_uploader(
        label="Drop your STM document here",
        type=[ext.lstrip(".") for ext in SUPPORTED_EXTENSIONS],
        help=f"Supported: {', '.join(SUPPORTED_EXTENSIONS)} · Max {MAX_FILE_SIZE_MB}MB"
    )

    if uploaded_file:
        file_size_mb = uploaded_file.size / (1024 * 1024)
        st.markdown(f"""
        <div style="display:flex; gap:0.5rem; flex-wrap:wrap; margin:0.5rem 0 1rem 0">
            <span class="meta-chip">📄 {uploaded_file.name}</span>
            <span class="meta-chip">💾 {file_size_mb:.2f} MB</span>
            <span class="meta-chip">🏷️ {Path(uploaded_file.name).suffix.upper()}</span>
        </div>
        """, unsafe_allow_html=True)

        if file_size_mb > MAX_FILE_SIZE_MB:
            st.error(f"⚠️ File exceeds {MAX_FILE_SIZE_MB}MB limit.")
            st.stop()

        run_btn = st.button("🚀 Summarize Document", type="primary")
    else:
        st.info("👆 Upload a PDF, DOCX, or TXT document to begin.")
        run_btn = False

with col_pipeline:
    st.markdown("### 🔄 Pipeline Status")

    # Pipeline steps display
    if "pipeline_steps" not in st.session_state:
        st.session_state.pipeline_steps = {
            "upload":    ("⬜", "idle"),
            "trigger":   ("⬜", "idle"),
            "extract":   ("⬜", "idle"),
            "summarize": ("⬜", "idle"),
            "fetch":     ("⬜", "idle"),
        }

    step_labels = {
        "upload":    "Upload file to DBFS",
        "trigger":   "Trigger Databricks Notebook",
        "extract":   "Extract & clean text",
        "summarize": "Generate summary (LLM)",
        "fetch":     "Fetch result",
    }
    step_icons = {
        "idle":    ("⬜", ""),
        "active":  ("🔵", "step-active"),
        "done":    ("✅", "step-done"),
        "error":   ("❌", "step-error"),
    }

    pipeline_placeholder = st.empty()

    def render_pipeline():
        html = ""
        for key, label in step_labels.items():
            state = st.session_state.pipeline_steps[key][1]
            icon, css = step_icons.get(state, step_icons["idle"])
            html += f"""
            <div class="step-card {css}">
                <span class="step-icon">{icon}</span>
                <span class="step-label">{label}</span>
            </div>"""
        pipeline_placeholder.markdown(html, unsafe_allow_html=True)

    render_pipeline()


def set_step(step: str, state: str):
    st.session_state.pipeline_steps[step] = (step, state)
    render_pipeline()


# ── Run Pipeline ──────────────────────────────────────────
if run_btn and uploaded_file:
    filename     = uploaded_file.name
    file_bytes   = uploaded_file.read()
    output_path  = f"{DBFS_OUTPUT_DIR}/{filename}_summary.json"

    # Progress bar
    progress = st.progress(0, text="Starting pipeline…")
    summary_placeholder = st.empty()

    try:
        # ── Step 1: Upload to DBFS ──────────────────────────
        set_step("upload", "active")
        progress.progress(10, text="📤 Uploading file to Databricks DBFS…")

        input_dbfs_path = dbfs_upload(file_bytes, filename)
        set_step("upload", "done")
        progress.progress(25, text="✅ File uploaded to DBFS")

        # ── Step 2: Trigger Notebook ────────────────────────
        set_step("trigger", "active")
        progress.progress(30, text="🚀 Submitting Databricks notebook run…")

        run_id = submit_run(input_dbfs_path, output_path, filename)
        set_step("trigger", "done")
        progress.progress(40, text=f"⚙️ Run ID: {run_id} — Waiting for cluster…")

        # ── Step 3 & 4: Poll (extract + summarize) ──────────
        set_step("extract", "active")
        poll_count = [0]

        def on_progress(status):
            poll_count[0] += 1
            lc = status["life_cycle_state"]
            msg = status.get("state_message", "")

            pct = min(40 + poll_count[0] * 2, 85)

            if lc == "PENDING":
                progress.progress(pct, text="🟡 Cluster starting up… (may take 3–5 min on Community Edition)")
            elif lc == "RUNNING":
                set_step("extract", "done")
                set_step("summarize", "active")
                progress.progress(pct, text=f"🔵 Notebook running… extracting & summarizing…")
            elif msg:
                progress.progress(pct, text=f"ℹ️ {msg}")

        poll_until_done(run_id, progress_cb=on_progress)

        set_step("summarize", "done")

        # ── Step 5: Fetch Result ─────────────────────────────
        set_step("fetch", "active")
        progress.progress(90, text="📥 Fetching summary from DBFS…")

        result = dbfs_read(output_path)
        set_step("fetch", "done")
        progress.progress(100, text="✅ Complete!")
        time.sleep(0.5)
        progress.empty()

        # ── Display Summary ──────────────────────────────────
        summary     = result.get("summary", "No summary returned.")
        word_count  = result.get("word_count", "—")
        char_count  = result.get("char_count", "—")
        pages       = result.get("pages", "—")
        file_type   = result.get("file_type", Path(filename).suffix.upper())
        keywords    = result.get("keywords", [])
        chunks_used = result.get("chunks_used", 1)

        kw_html = "".join([f'<span class="meta-chip">#{kw}</span>' for kw in keywords[:10]])

        summary_placeholder.markdown(f"""
        <div class="summary-box">
            <div class="summary-header">📋 Document Summary</div>
            <div style="margin-bottom:1rem">
                <span class="meta-chip">📄 {file_type}</span>
                <span class="meta-chip">📝 {word_count:,} words</span>
                <span class="meta-chip">📄 {pages} pages</span>
                <span class="meta-chip">🧩 {chunks_used} chunk(s)</span>
            </div>
            <p style="margin:0 0 1.2rem 0">{summary}</p>
            {'<div><p style="color:#8892a4;font-size:0.82rem;margin-bottom:0.4rem">KEY TOPICS</p>' + kw_html + '</div>' if keywords else ''}
        </div>
        """, unsafe_allow_html=True)

        # Download button
        st.download_button(
            label="⬇️ Download Summary as JSON",
            data=json.dumps(result, indent=2),
            file_name=f"{Path(filename).stem}_summary.json",
            mime="application/json"
        )

        # Cleanup DBFS
        try:
            dbfs_delete(input_dbfs_path)
            dbfs_delete(output_path)
        except Exception:
            pass  # Non-critical

    except Exception as e:
        for step in st.session_state.pipeline_steps:
            if st.session_state.pipeline_steps[step][1] == "active":
                set_step(step, "error")
        progress.empty()
        st.error(f"❌ Pipeline failed: {str(e)}")
        st.markdown("""
        **Troubleshooting tips:**
        - Verify your `DATABRICKS_TOKEN` in `config.py` is valid
        - Ensure the notebook path exists in your Databricks workspace
        - Community Edition clusters can take **3–8 minutes** to start
        - Check the Databricks run page for detailed logs
        """)


# ── Footer ────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<p style="color:#4a5568;font-size:0.8rem;text-align:center">'
    'STM Document Summarizer · Powered by Databricks Community Edition · '
    'HuggingFace BART</p>',
    unsafe_allow_html=True
)
