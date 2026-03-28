"""High-Alti PL Dashboard — Main entry point."""

import sys
from pathlib import Path

# Add app directory to path so imports work when run from any cwd
APP_DIR = Path(__file__).parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import streamlit as st

st.set_page_config(
    page_title="ハイアルチ PL管理",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize database on first run
from database import init_db
init_db()

# Custom CSS
st.markdown("""
<style>
    /* Hide Deploy button only */
    [data-testid="stAppDeployButton"] {display: none;}
    footer {visibility: hidden;}

    /* Print: hide sidebar and header, full width */
    @media print {
        [data-testid="stSidebar"] {display: none !important;}
        [data-testid="stSidebarCollapsedControl"] {display: none !important;}
        header {display: none !important;}
        .stMainBlockContainer {max-width: 100% !important; padding: 0 !important;}
        [data-testid="stToolbar"] {display: none !important;}
    }

    /* KPI cards */
    [data-testid="stMetric"] {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.85rem;
        color: #6c757d;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 700;
    }

    /* Sidebar */
    .stSidebar [data-testid="stSidebarContent"] {
        padding-top: 1rem;
    }

    /* Table styling */
    .stDataFrame {
        font-size: 0.9rem;
    }

    /* Header */
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1a1a2e;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 0.95rem;
        color: #6c757d;
        margin-bottom: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar logo
import base64
from pathlib import Path

logo_path = Path(__file__).parent / "assets" / "logo.png"
if logo_path.exists():
    logo_b64 = base64.b64encode(logo_path.read_bytes()).decode()
    st.sidebar.markdown(
        f'<div style="text-align:center;padding:8px 0 12px;">'
        f'<img src="data:image/png;base64,{logo_b64}" style="width:160px;border-radius:8px;">'
        f'</div>',
        unsafe_allow_html=True,
    )
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "ページ選択",
    ["ダッシュボード", "アップロード", "設定"],
    index=0,
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<small>High-Alti PL Dashboard v1.0</small>",
    unsafe_allow_html=True,
)

# Route to page
if page == "ダッシュボード":
    from views.dashboard import render
    render()
elif page == "アップロード":
    from views.upload import render
    render()
elif page == "設定":
    from views.settings import render
    render()
