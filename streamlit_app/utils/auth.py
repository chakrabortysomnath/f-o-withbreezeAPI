import hashlib
import time
import streamlit as st

_REQUIRED_SECRETS = ("AUTH_USERNAME", "AUTH_PASSWORD_HASH", "AUTH_SALT")


def _hash(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _check_credentials(username: str, password: str) -> bool:
    expected_user = st.secrets.get("AUTH_USERNAME", "")
    expected_hash = st.secrets.get("AUTH_PASSWORD_HASH", "")
    salt = st.secrets.get("AUTH_SALT", "")
    if not (expected_user and expected_hash and salt):
        return False
    return username == expected_user and _hash(password, salt) == expected_hash


def require_login() -> None:
    """
    Call at the top of every page (after set_page_config).

    - If the user is not authenticated: renders a centered login form and
      calls st.stop() so nothing else on the page is rendered.
    - If the user is authenticated: adds a Logout button to the sidebar
      and returns immediately so the page renders normally.
    """
    # ── Check auth secrets are configured ─────────────────────────────────────
    missing = [k for k in _REQUIRED_SECRETS if k not in st.secrets]
    if missing:
        st.error(
            f"Auth secrets not configured: **{', '.join(missing)}**\n\n"
            "Run `python generate_password_hash.py` and add the output to "
            "`.streamlit/secrets.toml` or Streamlit Cloud → Settings → Secrets."
        )
        st.stop()

    # ── Already logged in ──────────────────────────────────────────────────────
    if st.session_state.get("authenticated"):
        with st.sidebar:
            st.divider()
            if st.button("🚪 Logout", use_container_width=True):
                st.session_state.clear()
                st.rerun()
        return

    # ── Login form ─────────────────────────────────────────────────────────────
    _, centre, _ = st.columns([1, 1.2, 1])
    with centre:
        st.markdown("## 🔒 Breeze Options Dashboard")
        st.caption("Please log in to continue.")

        with st.form("login_form", clear_on_submit=True):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button(
                "Log in", type="primary", use_container_width=True
            )

        if submitted:
            if _check_credentials(username, password):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                time.sleep(0.5)  # slow brute-force
                st.error("Invalid username or password.")

    st.stop()
