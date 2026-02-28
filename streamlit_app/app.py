import requests
import streamlit as st
from utils.auth import require_login

st.set_page_config(
    page_title="Breezy F&O",
    page_icon="📊",
    layout="wide",
    menu_items={},
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
| 💼 **Holdings** | View all demat stock holdings with P&L summary |
| ⚙️ **Config** | Add or edit symbols, lot sizes and yfinance ticker mappings |
""")

st.divider()

# ── System Health ──────────────────────────────────────────────────────────────
st.subheader("System Health")

col_btn, col_url = st.columns([2, 3])
with col_btn:
    run_check = st.button("Run health check", use_container_width=True)
with col_url:
    st.caption("Backend URL")
    st.code(st.secrets["BASE_URL"], language=None)

if run_check:
    base_url = st.secrets["BASE_URL"].rstrip("/")
    token = st.secrets["APP_TOKEN"]

    layers: dict = {}

    with st.spinner("Checking all layers…"):

        # ── Layer 1: Streamlit (self) ──────────────────────────────────────────
        layers["streamlit"] = {
            "label": "Streamlit",
            "ok": True,
            "detail": "UI layer is running",
            "extra": "",
        }

        # ── Layer 2: Render web service (backend reachability) ────────────────
        try:
            r = requests.get(f"{base_url}/health", timeout=10)
            ok = r.status_code == 200
            layers["render"] = {
                "label": "Render Web Service",
                "ok": ok,
                "detail": f"HTTP {r.status_code} — backend is {'up' if ok else 'returning an error'}",
                "extra": "",
            }
        except Exception as exc:
            layers["render"] = {
                "label": "Render Web Service",
                "ok": False,
                "detail": str(exc),
                "extra": "",
            }

        # ── Layers 3 & 4: static IP + Breeze (from /health/detailed) ─────────
        if layers["render"]["ok"]:
            try:
                r = requests.get(
                    f"{base_url}/health/detailed",
                    headers={"X-APP-TOKEN": token},
                    timeout=20,
                )
                if r.status_code == 200:
                    detail_layers = r.json().get("layers", {})

                    ip_info = detail_layers.get("static_ip", {})
                    ip_label = ip_info.get("ip", "")
                    layers["static_ip"] = {
                        "label": "Static / Egress IP",
                        "ok": ip_info.get("ok", False),
                        "detail": ip_info.get("detail", "No data"),
                        "extra": ip_label,
                    }

                    breeze_info = detail_layers.get("breeze_api", {})
                    layers["breeze_api"] = {
                        "label": "Breeze API",
                        "ok": breeze_info.get("ok", False),
                        "detail": breeze_info.get("detail", "No data"),
                        "extra": "",
                    }
                else:
                    msg = f"HTTP {r.status_code} from /health/detailed"
                    for key, label in (
                        ("static_ip", "Static / Egress IP"),
                        ("breeze_api", "Breeze API"),
                    ):
                        layers[key] = {
                            "label": label,
                            "ok": False,
                            "detail": msg,
                            "extra": "",
                        }
            except Exception as exc:
                err = str(exc)
                for key, label in (
                    ("static_ip", "Static / Egress IP"),
                    ("breeze_api", "Breeze API"),
                ):
                    layers[key] = {
                        "label": label,
                        "ok": False,
                        "detail": err,
                        "extra": "",
                    }
        else:
            for key, label in (
                ("static_ip", "Static / Egress IP"),
                ("breeze_api", "Breeze API"),
            ):
                layers[key] = {
                    "label": label,
                    "ok": False,
                    "detail": "Backend unreachable — skipped",
                    "extra": "",
                }

    # ── Render status cards ────────────────────────────────────────────────────
    cols = st.columns(4)
    for col, key in zip(cols, ("streamlit", "render", "static_ip", "breeze_api")):
        layer = layers[key]
        icon = "🟢" if layer["ok"] else "🔴"
        with col:
            st.markdown(f"**{icon} {layer['label']}**")
            if layer["ok"]:
                st.success(layer["detail"])
            else:
                st.error(layer["detail"])
            if layer.get("extra"):
                st.caption(f"IP: `{layer['extra']}`")

    # ── Overall summary ────────────────────────────────────────────────────────
    all_ok = all(v["ok"] for v in layers.values())
    if all_ok:
        st.success("All systems operational.")
    else:
        failed = [v["label"] for v in layers.values() if not v["ok"]]
        st.warning(f"Issue detected in: {', '.join(failed)}")
