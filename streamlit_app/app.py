import os
import streamlit as st
import pandas as pd
from databricks import sql

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MH-CET 2025 College Predictor",
    page_icon="🎓",
    layout="wide"
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1f4e79;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #555;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Databricks connection (auto-auth via Databricks Apps env vars) ────────────
@st.cache_resource
def get_connection():
    host  = os.environ.get("DATABRICKS_HOST", "").replace("https://", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    return sql.connect(
        server_hostname = host,
        http_path       = "/sql/protocolv1/o/2464733314746848/0426-134721-vfee0nbj",
        access_token    = token
    )

@st.cache_data(ttl=3600)
def get_branches():
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT branch_name "
        "FROM rankrangers_project_data.gold.mhcet_cutoffs "
        "ORDER BY branch_name"
    )
    return [row[0] for row in cursor.fetchall()]

@st.cache_data(ttl=3600)
def get_categories():
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT clean_category "
        "FROM rankrangers_project_data.gold.mhcet_cutoffs "
        "ORDER BY clean_category"
    )
    return [row[0] for row in cursor.fetchall()]

def query_colleges(branch, category, gender, score):
    conn   = get_connection()
    cursor = conn.cursor()
    # Sanitise inputs — no user-supplied strings in SQL
    cursor.execute("""
        SELECT
            institute_name,
            ROUND(cap1_cutoff, 2) AS cap1_cutoff,
            ROUND(cap2_cutoff, 2) AS cap2_cutoff,
            ROUND(cap3_cutoff, 2) AS cap3_cutoff,
            ROUND(cap4_cutoff, 2) AS cap4_cutoff,
            CASE
                WHEN cap1_cutoff <= ? THEN 'CAP-I'
                WHEN cap2_cutoff <= ? THEN 'CAP-II'
                WHEN cap3_cutoff <= ? THEN 'CAP-III'
                WHEN cap4_cutoff <= ? THEN 'CAP-IV'
                ELSE 'Unlikely'
            END AS likely_round,
            total_seats_filled
        FROM rankrangers_project_data.gold.mhcet_cutoffs
        WHERE
            clean_category = ?
            AND seat_gender IN (?, 'ANY')
            AND branch_name  = ?
            AND cap4_cutoff <= ?
        ORDER BY cap1_cutoff DESC
    """, [score, score, score, score, category, gender, branch, score])
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">🎓 MH-CET 2025 College Predictor</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Find colleges within your reach based on your CET score, category and branch</div>', unsafe_allow_html=True)

# ── Input Panel ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns([3, 2, 1.5, 1.5])

with col1:
    branches = get_branches()
    branch   = st.selectbox(
        "Branch",
        branches,
        index=branches.index("Computer Science and Engineering")
               if "Computer Science and Engineering" in branches else 0
    )

with col2:
    categories = get_categories()
    category   = st.selectbox("Category", categories)

with col3:
    gender_label = st.radio("Gender", ["Male", "Female"], horizontal=True)
    gender       = "M" if gender_label == "Male" else "F"

with col4:
    score = st.number_input(
        "MHT-CET Score",
        min_value=0.0, max_value=100.0,
        value=85.0, step=0.01, format="%.2f"
    )

search = st.button("🔍 Find Colleges", type="primary", use_container_width=True)

# ── Results ───────────────────────────────────────────────────────────────────
if search:
    with st.spinner("Searching..."):
        df = query_colleges(branch, category, gender, score)

    if df.empty:
        st.warning("No colleges found. Try adjusting your score or category.")
    else:
        # Summary metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Colleges Found",       len(df))
        m2.metric("Likely in CAP-I",      len(df[df["likely_round"] == "CAP-I"]))
        m3.metric("Likely in CAP-II",     len(df[df["likely_round"] == "CAP-II"]))
        m4.metric("Likely in CAP-III/IV", len(df[df["likely_round"].isin(["CAP-III","CAP-IV"])]))

        st.markdown("---")

        ROUND_EMOJI = {
            "CAP-I":   "🟢 CAP-I",
            "CAP-II":  "🟡 CAP-II",
            "CAP-III": "🟠 CAP-III",
            "CAP-IV":  "🔴 CAP-IV",
            "Unlikely":"⚫ Unlikely"
        }

        df_display = df.copy()
        df_display["likely_round"] = df_display["likely_round"].map(ROUND_EMOJI)
        for col in ["cap1_cutoff","cap2_cutoff","cap3_cutoff","cap4_cutoff"]:
            df_display[col] = df_display[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "—"
            )

        df_display = df_display.rename(columns={
            "institute_name":    "College",
            "cap1_cutoff":       "CAP-I Cutoff",
            "cap2_cutoff":       "CAP-II Cutoff",
            "cap3_cutoff":       "CAP-III Cutoff",
            "cap4_cutoff":       "CAP-IV Cutoff",
            "likely_round":      "Likely Round",
            "total_seats_filled":"Seats Filled"
        })

        st.dataframe(
            df_display[[
                "College","CAP-I Cutoff","CAP-II Cutoff",
                "CAP-III Cutoff","CAP-IV Cutoff","Likely Round","Seats Filled"
            ]],
            use_container_width=True,
            hide_index=True,
            height=500
        )

        st.download_button(
            label="⬇️ Download Results as CSV",
            data=df.to_csv(index=False),
            file_name=f"mhcet_{branch[:20]}_{category}_{gender}_{score}.csv",
            mime="text/csv"
        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#aaa;font-size:0.8rem'>"
    "Data: Maharashtra CET 2025 CAP Allotment Lists | "
    "Cutoffs based on actual allotments across all 4 CAP rounds"
    "</div>",
    unsafe_allow_html=True
)
