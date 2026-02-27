import hashlib
import secrets as _secrets
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


def _show_setup_form() -> None:
    """Shown when AUTH_* secrets are not yet configured."""
    _, centre, _ = st.columns([1, 1.2, 1])
    with centre:
        st.markdown("## ⚙️ First-time Setup")
        st.info(
            "Auth credentials are not yet configured.  \n"
            "Generate them here, then paste the three lines into  \n"
            "**Streamlit Cloud → App Settings → Secrets** and reload the app.",
            icon="ℹ️",
        )

        with st.form("setup_form"):
            username = st.text_input("Choose a username")
            password = st.text_input("Choose a password", type="password")
            confirm  = st.text_input("Confirm password",  type="password")
            submitted = st.form_submit_button(
                "Generate credentials", type="primary", use_container_width=True
            )

        if submitted:
            if not username or not password:
                st.error("Username and password cannot be empty.")
            elif password != confirm:
                st.error("Passwords do not match.")
            else:
                salt          = _secrets.token_hex(32)
                password_hash = _hash(password, salt)

                st.success("Credentials generated — copy the block below into your Streamlit secrets.")
                st.code(
                    f'AUTH_USERNAME      = "{username}"\n'
                    f'AUTH_SALT          = "{salt}"\n'
                    f'AUTH_PASSWORD_HASH = "{password_hash}"',
                    language="toml",
                )
                st.info(
                    "After saving the secrets in Streamlit Cloud, reload the app to activate login.",
                    icon="🔄",
                )


def require_login() -> None:
    """
    Call at the top of every page (after set_page_config).

    - Auth secrets missing  → shows first-time setup form, then st.stop()
    - Not authenticated     → shows login form, then st.stop()
    - Authenticated         → adds Logout to the sidebar and returns
    """
    # ── First-time setup ───────────────────────────────────────────────────────
    missing = [k for k in _REQUIRED_SECRETS if k not in st.secrets]
    if missing:
        _show_setup_form()
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
