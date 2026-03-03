import streamlit as st

from utils.auth import require_login
from components.header import render_header
from views.home_view import render_home
from views.portfolio_view import render_portfolio
from views.covered_calls_view import render_covered_calls
from views.config_view import render_config
from views.decision_desk_view import render_decision_desk

st.set_page_config(
    page_title="Breezy F&O",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={},
)

# ── Validate backend secrets ───────────────────────────────────────────────────
missing = [k for k in ("APP_TOKEN", "BASE_URL") if k not in st.secrets]
if missing:
    st.error(
        f"Missing secret(s): **{', '.join(missing)}**\n\n"
        "Add them to `.streamlit/secrets.toml` (local) or "
        "Streamlit Cloud → Settings → Secrets."
    )
    st.stop()

# ── Auth (sidebar_logout=False — logout is in the header) ─────────────────────
require_login(sidebar_logout=False)

# ── Branded header (also injects CSS to hide the sidebar) ─────────────────────
render_header()

# ── Top navigation tabs ────────────────────────────────────────────────────────
tab_home, tab_portfolio, tab_cc, tab_dd, tab_config = st.tabs([
    "🏠 Home",
    "💼 Portfolio",
    "🎯 Covered Call Analyser",
    "🏦 Decision Desk",
    "⚙️ Configuration",
])

with tab_home:
    render_home()

with tab_portfolio:
    render_portfolio()

with tab_cc:
    render_covered_calls()

with tab_dd:
    render_decision_desk()

with tab_config:
    render_config()
