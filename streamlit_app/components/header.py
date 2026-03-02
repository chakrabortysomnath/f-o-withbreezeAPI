from __future__ import annotations

import streamlit as st

_BANNER_CSS = """
<style>
.breezy-header {
    background: linear-gradient(135deg, #0f2027 0%, #203a43 55%, #2c5364 100%);
    padding: 18px 24px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 6px;
    min-height: 72px;
}
.breezy-brand { display: flex; align-items: center; gap: 14px; }
.breezy-icon  { font-size: 36px; line-height: 1; }
.breezy-title { margin: 0; color: #ffffff; font-size: 22px; font-weight: 700; }
.breezy-sub   { margin: 0; color: #90b8cc; font-size: 12px; margin-top: 2px; }
.breezy-user  { text-align: right; }
.breezy-user-label { margin: 0; color: #90b8cc; font-size: 11px; }
.breezy-user-name  { margin: 0; color: #ffffff; font-size: 15px; font-weight: 600; }
</style>
"""

_HIDE_SIDEBAR_CSS = """
<style>
[data-testid="stSidebar"]        { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }
#MainMenu                         { visibility: hidden; }
footer                            { visibility: hidden; }
</style>
"""


def render_header() -> None:
    """
    Renders the full-width branded header and injects CSS to hide the sidebar.
    Call once at the top of app.py, after require_login().
    """
    username = st.secrets.get("AUTH_USERNAME", "User")

    st.markdown(_HIDE_SIDEBAR_CSS, unsafe_allow_html=True)
    st.markdown(_BANNER_CSS, unsafe_allow_html=True)

    col_brand, col_user = st.columns([5, 1])

    with col_brand:
        st.markdown(
            f"""
            <div class="breezy-header">
                <div class="breezy-brand">
                    <span class="breezy-icon">📊</span>
                    <div>
                        <p class="breezy-title">Breezy F&amp;O Dashboard</p>
                        <p class="breezy-sub">
                            NSE Live Quotes &nbsp;·&nbsp; Holdings &nbsp;·&nbsp;
                            F&amp;O Analytics &nbsp;·&nbsp; Covered Calls
                        </p>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_user:
        st.markdown(
            f"""
            <div class="breezy-header" style="justify-content:flex-end;">
                <div class="breezy-user">
                    <p class="breezy-user-label">Logged in as</p>
                    <p class="breezy-user-name">👤 {username}</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🚪 Logout", use_container_width=True, key="hdr_logout"):
            st.session_state.clear()
            st.rerun()
