from __future__ import annotations

import datetime
import streamlit as st
import yfinance as yf
import pandas as pd
import requests

from utils.api import fetch_quote
from utils.config import load_config, get_symbols


# ── Cached data helpers ────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _nifty_history(period: str = "3mo") -> pd.DataFrame:
    return yf.Ticker("^NSEI").history(period=period, interval="1d")


@st.cache_data(ttl=300)
def _nifty_fast_info() -> dict:
    fi = yf.Ticker("^NSEI").fast_info
    return {
        "last_price":   getattr(fi, "last_price",         None),
        "open":         getattr(fi, "open",                None),
        "day_high":     getattr(fi, "day_high",            None),
        "day_low":      getattr(fi, "day_low",             None),
        "prev_close":   getattr(fi, "previous_close",      None),
        "52w_high":     getattr(fi, "fifty_two_week_high", None),
        "52w_low":      getattr(fi, "fifty_two_week_low",  None),
        "volume":       getattr(fi, "three_month_average_volume", None),
    }


@st.cache_data(ttl=1800)
def _nifty_news() -> list[dict]:
    try:
        return yf.Ticker("^NSEI").news or []
    except Exception:
        return []


def _fmt(v, prefix="₹", decimals=2):
    if v is None:
        return "—"
    try:
        return f"{prefix}{float(v):,.{decimals}f}"
    except Exception:
        return "—"


# ── Main render ────────────────────────────────────────────────────────────────

def render_home() -> None:
    load_config()

    # ── Nifty 50 market overview ───────────────────────────────────────────────
    st.subheader("📈 Nifty 50 — Today")

    info = _nifty_fast_info()
    price   = info.get("last_price")
    prev    = info.get("prev_close")
    chg     = (price - prev) if (price and prev) else None
    chg_pct = (chg / prev * 100) if (chg and prev) else None
    delta_str = f"{chg:+.2f} ({chg_pct:+.2f}%)" if chg is not None else None

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Nifty 50",  _fmt(price, ""), delta=delta_str)
    m2.metric("Open",      _fmt(info.get("open"), ""))
    m3.metric("Day High",  _fmt(info.get("day_high"), ""))
    m4.metric("Day Low",   _fmt(info.get("day_low"), ""))
    m5.metric("52W High",  _fmt(info.get("52w_high"), ""))
    m6.metric("52W Low",   _fmt(info.get("52w_low"), ""))

    # ── Nifty chart ────────────────────────────────────────────────────────────
    chart_col, news_col = st.columns([3, 2])

    with chart_col:
        period = st.selectbox(
            "Chart period", ["1mo", "3mo", "6mo", "1y", "3y"],
            index=1, key="home_chart_period",
        )
        hist = _nifty_history(period)
        if not hist.empty:
            import plotly.graph_objects as go
            fig = go.Figure(data=[go.Candlestick(
                x=hist.index,
                open=hist["Open"], high=hist["High"],
                low=hist["Low"],   close=hist["Close"],
                increasing_line_color="#26a69a",
                decreasing_line_color="#ef5350",
            )])
            fig.update_layout(
                title="Nifty 50",
                xaxis_title=None,
                yaxis_title="Index",
                xaxis_rangeslider_visible=False,
                height=350,
                margin=dict(l=0, r=0, t=30, b=0),
                template="plotly_dark",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chart data unavailable.")

    # ── News feed ──────────────────────────────────────────────────────────────
    with news_col:
        st.markdown("#### 📰 Nifty 50 News")
        news = _nifty_news()
        if news:
            for item in news[:10]:
                title = item.get("title", "")
                link  = item.get("link", "")
                pub   = item.get("publisher", "")
                ts    = item.get("providerPublishTime")
                date_str = ""
                if ts:
                    try:
                        date_str = datetime.datetime.fromtimestamp(ts).strftime("%d %b %H:%M")
                    except Exception:
                        pass
                st.markdown(
                    f"**[{title}]({link})**  \n"
                    f"<span style='color:grey;font-size:12px'>{pub} · {date_str}</span>",
                    unsafe_allow_html=True,
                )
                st.divider()
        else:
            st.info("No news available at the moment.")

    # ── Quote Search ───────────────────────────────────────────────────────────
    st.divider()
    st.subheader("🔍 Get Quote")

    qcol1, qcol2, qcol3 = st.columns([1, 2, 1])

    with qcol1:
        exchange = st.selectbox("Exchange", ["NSE", "NFO", "BFO"], key="hq_exchange")
        is_fno = exchange in ("NFO", "BFO")
        product_type = st.selectbox(
            "Product Type",
            ["cash", "futures", "options"] if is_fno else ["cash"],
            key="hq_product",
        )

    with qcol2:
        symbol = st.selectbox("Symbol", get_symbols(), key="hq_symbol")

    with qcol3:
        expiry_date = ""
        right = None
        if is_fno:
            expiry_raw = st.date_input(
                "Expiry Date",
                value=None,
                min_value=datetime.date.today(),
                key="hq_expiry",
            )
            expiry_date = expiry_raw.strftime("%d-%b-%Y") if expiry_raw else ""
        if product_type == "options":
            right = st.selectbox("Right", ["call", "put"], key="hq_right")

    if st.button("🔄 Fetch Quote", type="primary", key="hq_btn"):
        with st.spinner(f"Fetching {exchange}:{symbol}…"):
            try:
                q = fetch_quote(
                    exchange_code=exchange,
                    stock_code=symbol,
                    product_type=product_type,
                    expiry_date=expiry_date or None,
                    strike_price=None,
                    right=right,
                )

                def _f(v):
                    try:
                        return float(v) if v not in (None, "") else None
                    except Exception:
                        return None

                ltp  = _f(q.get("ltp"))
                prev_close = _f(q.get("prev_close"))
                delta = f"{ltp - prev_close:+.2f}" if (ltp and prev_close) else None

                r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns(5)
                r1c1.metric("LTP",        _fmt(ltp, ""), delta=delta)
                r1c2.metric("Open",       _fmt(_f(q.get("open")), ""))
                r1c3.metric("High",       _fmt(_f(q.get("high")), ""))
                r1c4.metric("Low",        _fmt(_f(q.get("low")), ""))
                r1c5.metric("Prev Close", _fmt(prev_close, ""))

                r2c1, r2c2, r2c3, r2c4, r2c5 = st.columns(5)
                r2c1.metric("Bid",     _fmt(_f(q.get("bid_price")), ""))
                r2c2.metric("Ask",     _fmt(_f(q.get("ask_price")), ""))
                r2c3.metric("Bid Qty", q.get("bid_qty") or "—")
                r2c4.metric("Ask Qty", q.get("ask_qty") or "—")
                vol = q.get("volume")
                r2c5.metric("Volume",  f"{int(float(vol)):,}" if vol not in (None, "") else "—")

            except Exception as e:
                st.error(f"Error: {e}")

    # ── System health (collapsed) ──────────────────────────────────────────────
    with st.expander("🔧 System Health Check"):
        if st.button("Run Health Check", key="hq_health"):
            base_url = st.secrets["BASE_URL"].rstrip("/")
            token    = st.secrets["APP_TOKEN"]
            layers: dict = {"streamlit": {"label": "Streamlit", "ok": True, "detail": "UI layer running", "extra": ""}}

            try:
                r = requests.get(f"{base_url}/health", timeout=10)
                ok = r.status_code == 200
                layers["render"] = {"label": "Render", "ok": ok, "detail": f"HTTP {r.status_code}", "extra": ""}
            except Exception as exc:
                layers["render"] = {"label": "Render", "ok": False, "detail": str(exc), "extra": ""}

            if layers["render"]["ok"]:
                try:
                    r = requests.get(f"{base_url}/health/detailed", headers={"X-APP-TOKEN": token}, timeout=20)
                    if r.status_code == 200:
                        dl = r.json().get("layers", {})
                        ip = dl.get("static_ip", {})
                        layers["static_ip"] = {"label": "Static IP", "ok": ip.get("ok", False), "detail": ip.get("detail", ""), "extra": ip.get("ip", "")}
                        br = dl.get("breeze_api", {})
                        layers["breeze_api"] = {"label": "Breeze API", "ok": br.get("ok", False), "detail": br.get("detail", ""), "extra": ""}
                    else:
                        for k, lbl in (("static_ip", "Static IP"), ("breeze_api", "Breeze API")):
                            layers[k] = {"label": lbl, "ok": False, "detail": f"HTTP {r.status_code}", "extra": ""}
                except Exception as exc:
                    for k, lbl in (("static_ip", "Static IP"), ("breeze_api", "Breeze API")):
                        layers[k] = {"label": lbl, "ok": False, "detail": str(exc), "extra": ""}
            else:
                for k, lbl in (("static_ip", "Static IP"), ("breeze_api", "Breeze API")):
                    layers[k] = {"label": lbl, "ok": False, "detail": "Backend unreachable", "extra": ""}

            cols = st.columns(4)
            for col, key in zip(cols, ("streamlit", "render", "static_ip", "breeze_api")):
                lyr = layers.get(key, {})
                icon = "🟢" if lyr.get("ok") else "🔴"
                with col:
                    st.markdown(f"**{icon} {lyr['label']}**")
                    (st.success if lyr.get("ok") else st.error)(lyr.get("detail", ""))
                    if lyr.get("extra"):
                        st.caption(f"IP: `{lyr['extra']}`")
