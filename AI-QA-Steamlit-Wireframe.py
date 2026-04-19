import streamlit as st

# -------------------------------
# Page Configuration
# -------------------------------
st.set_page_config(
    page_title="End-to-End AI QA",
    layout="wide"
)

# -------------------------------
# Header (Plain Text Only)
# -------------------------------
st.title("End‑to‑End AI QA for Ingestion Pipelines")

# -------------------------------
# Main Layout
# -------------------------------
left_col, right_col = st.columns([3.5, 1.5], gap="medium")

# ============================================================================
# LEFT PANEL
# ============================================================================
with left_col:

    # ===============================
    # INGESTION CONFIGURATION
    # ===============================
    with st.container(border=True):
        st.subheader("Ingestion Configuration")

        st.selectbox(
            "Select STM Location",
            ["Select", "STM_Location_1", "STM_Location_2"],
            key="stm_location"
        )

        st.text_input(
            "Files Location (parquet, csv, etc)",
            placeholder="Enter file or folder path",
            key="files_location"
        )

        st.button("Summary", key="summary_btn")

    # ===============================
    # QUALITY ASSURANCE
    # ===============================
    with st.container(border=True):
        st.subheader("Quality Assurance")

        st.button(
            "Run All Validations",
            key="run_all_validations",
            use_container_width=True
        )

        st.markdown("**Structure**")
        col1, col2 = st.columns(2)

        with col1:
            st.button(
                "Test Case Generator",
                key="structure_generator",
                use_container_width=True
            )

        with col2:
            st.button(
                "Test Case Validation",
                key="structure_validation",
                use_container_width=True
            )

        st.markdown("**SCD**")
        col3, col4 = st.columns(2)

        with col3:
            st.button(
                "Test Case Generator",
                key="scd_generator",
                use_container_width=True
            )

        with col4:
            st.button(
                "Test Case Validation",
                key="scd_validation",
                use_container_width=True
            )

# ============================================================================
# RIGHT PANEL – SUMMARY VIEWER
# ============================================================================
with right_col:

    with st.container(border=True):
        st.subheader("Summary Viewer")

        st.markdown("**Pipeline Name**")
        st.write("STM Name")

        st.markdown("**Source**")
        st.write("Schema Name · Table Name")

        st.markdown("**Target**")
        st.write("Schema Name · Table Name")

        st.markdown("**Curated**")
        st.write("Schema Name · Table Name")

        st.markdown("**SCD Type**")
        st.write("Type 1 / Type 2")

        st.markdown("**Load Type**")
        st.write("Full / Incremental")
