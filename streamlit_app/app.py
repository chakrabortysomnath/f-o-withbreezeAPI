import streamlit as st
from utils.auth import require_login

st.set_page_config(
    page_title="Breeze Options Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Validate backend secrets early ────────────────────────────────────────────
missing = [k for k in ("APP_TOKEN", "BASE_URL") if k not in st.secrets]
if missing:
    st.error(
        f"Missing secret(s): **{', '.join(missing)}**\n\n"
        "Add them to `.streamlit/secrets.toml` (local) or "
        "the Streamlit Cloud dashboard under *Settings → Secrets*."
    )
    st.stop()

require_login()

# ── Home page ──────────────────────────────────────────────────────────────────
st.title("📊 Breeze Options Dashboard")
st.caption("Live F&O data from Breeze Connect via your Render backend.")

st.markdown("""
| Page | What it does |
|---|---|
| 📈 **Quote Fetcher** | Fetch live prices for any stock, future, or option contract |
| 📊 **Option Chain** | Load a full option chain with covered-call P&L metrics |
| ⚙️ **Config** | Add or edit symbols, lot sizes and yfinance ticker mappings |
""")

st.divider()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Quick health check")
    if st.button("Ping backend"):
        import requests
        url = st.secrets["BASE_URL"].rstrip("/") + "/health"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                st.success(f"Backend is up ✓  ({url})")
            else:
                st.warning(f"HTTP {r.status_code}")
        except Exception as e:
            st.error(f"Could not reach backend: {e}")

with col2:
    st.subheader("Backend URL")
    st.code(st.secrets["BASE_URL"], language=None)
