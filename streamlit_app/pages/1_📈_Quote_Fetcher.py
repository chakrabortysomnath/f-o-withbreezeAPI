import datetime
import streamlit as st
import pandas as pd
from utils.api import fetch_quote, fetch_option_chain
from utils.config import load_config, get_symbols

st.set_page_config(page_title="Quote Fetcher", page_icon="📈", layout="wide")
st.title("📈 Quote Fetcher")
st.caption("Fetch a live price for any stock, future, or option contract.")

load_config()


def _to_f(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_int(v):
    try:
        return int(float(v)) if v not in (None, "") else 0
    except (TypeError, ValueError):
        return 0



# ── Inputs ─────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    exchange = st.selectbox("Exchange", ["NSE", "NFO", "BFO"])
    symbol = st.selectbox("Symbol", get_symbols())
    is_fno = exchange in ("NFO", "BFO")
    product_type = st.selectbox(
        "Product Type",
        ["cash", "futures", "options"] if is_fno else ["cash"],
    )

is_options = product_type == "options"

with col2:
    expiry_date = ""
    right = None

    if is_fno:
        expiry_raw = st.date_input(
            "Expiry Date",
            value=None,
            min_value=datetime.date.today(),
            format="DD/MM/YYYY",
            help="Select the option expiry date.",
        )
        expiry_date = expiry_raw.strftime("%d-%b-%Y") if expiry_raw else ""

    if is_options:
        right = st.selectbox("Right", ["call", "put"])


# ── Auto-fetch liquid strikes (NFO/BFO options only) ───────────────────────────
strike_price = None

if is_fno and is_options:
    trigger_key = (exchange, symbol, expiry_date.strip(), right)
    last_key = st.session_state.get("q_auto_fetch_key")
    all_ready = bool(symbol) and bool(right) and bool(expiry_date)

    # Any change to the trigger fields → clear stale data
    if trigger_key != last_key:
        for k in ("q_strikes", "q_spot", "q_fetch_error", "q_auto_fetch_key"):
            st.session_state.pop(k, None)

    # Trigger auto-fetch when all three fields are populated and no fetch for this key yet
    if all_ready and st.session_state.get("q_auto_fetch_key") is None:
        with st.spinner(
            f"Loading liquid strikes — {symbol} {right.upper()} {expiry_date.strip()}…"
        ):
            try:
                data = fetch_option_chain(exchange, symbol, right, expiry_date.strip())
                rows = data.get("rows", [])
                # Keep only rows where both bid_qty AND ask_qty are non-zero
                liquid = [
                    r for r in rows
                    if _to_int(r.get("bid_qty")) > 0 and _to_int(r.get("ask_qty")) > 0
                ]
                strikes = sorted({
                    float(r["strike_price"])
                    for r in liquid
                    if r.get("strike_price") is not None
                })
                st.session_state["q_strikes"] = strikes
                st.session_state["q_spot"] = data.get("spot_price")
                st.session_state["q_auto_fetch_key"] = trigger_key
            except Exception as e:
                st.session_state["q_fetch_error"] = str(e)
                st.session_state["q_auto_fetch_key"] = trigger_key  # prevent retry loop

    # ── Strike picker ───────────────────────────────────────────────────────────
    if st.session_state.get("q_fetch_error"):
        st.error(f"Could not load strikes: {st.session_state['q_fetch_error']}")
    elif not all_ready:
        st.caption("Select an **Expiry Date** and **Right** — strikes load automatically.")

    strikes_list = st.session_state.get("q_strikes", [])
    spot_val = st.session_state.get("q_spot")

    if strikes_list:
        spot_label = f"Spot ₹{float(spot_val):,.2f}  ·  " if spot_val else ""
        st.caption(f"{spot_label}{len(strikes_list)} liquid strikes (non-zero bid & ask qty)")

        # Default selection to ATM strike
        default_idx = 0
        if spot_val:
            diffs = [abs(s - float(spot_val)) for s in strikes_list]
            default_idx = diffs.index(min(diffs))

        strike_price = st.selectbox("Strike Price", strikes_list, index=default_idx)

    elif st.session_state.get("q_auto_fetch_key") and not st.session_state.get("q_fetch_error"):
        st.warning("No liquid strikes found — all returned strikes have zero bid or ask quantity.")


# ── Fetch Quote button ─────────────────────────────────────────────────────────
st.divider()

# For NFO/BFO options: require a strike selection before showing the button
if is_fno and is_options and strike_price is None:
    strikes_list = st.session_state.get("q_strikes", [])
    if not strikes_list and not st.session_state.get("q_fetch_error"):
        st.caption("Select Symbol, Expiry Date, and Right above — the Fetch Quote button appears once a strike is selected.")
    st.stop()

fetch_btn = st.button("🔄 Fetch Quote", type="primary", use_container_width=True)

if not fetch_btn:
    st.stop()

# ── Quote display ───────────────────────────────────────────────────────────────
with st.spinner(f"Fetching {exchange}:{symbol}…"):
    try:
        q = fetch_quote(
            exchange_code=exchange,
            stock_code=symbol,
            product_type=product_type,
            expiry_date=expiry_date or None,
            strike_price=str(strike_price) if strike_price is not None else None,
            right=right,
        )

        ltp = _to_f(q.get("ltp"))
        prev = _to_f(q.get("prev_close"))
        delta = f"{ltp - prev:+.2f}" if ltp is not None and prev is not None else None

        st.success(f"Quote fetched — {exchange}:{symbol}")

        # ── Metric row 1 ───────────────────────────────────────────────────────
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("LTP",        f"₹{ltp:,.2f}" if ltp is not None else "—", delta=delta)
        m2.metric("Open",       f"₹{_to_f(q.get('open')):,.2f}" if _to_f(q.get("open")) is not None else "—")
        m3.metric("High",       f"₹{_to_f(q.get('high')):,.2f}" if _to_f(q.get("high")) is not None else "—")
        m4.metric("Low",        f"₹{_to_f(q.get('low')):,.2f}" if _to_f(q.get("low")) is not None else "—")
        m5.metric("Prev Close", f"₹{prev:,.2f}" if prev is not None else "—")

        # ── Metric row 2 ───────────────────────────────────────────────────────
        m6, m7, m8, m9, m10 = st.columns(5)
        bid = _to_f(q.get("bid_price"))
        ask = _to_f(q.get("ask_price"))
        vol = q.get("volume")
        spot = _to_f(q.get("spot_price"))

        m6.metric("Bid",     f"₹{bid:,.2f}" if bid is not None else "—")
        m7.metric("Ask",     f"₹{ask:,.2f}" if ask is not None else "—")
        m8.metric("Bid Qty", q.get("bid_qty") or "—")
        m9.metric("Ask Qty", q.get("ask_qty") or "—")
        m10.metric("Volume", f"{int(float(vol)):,}" if vol not in (None, "") else "—")

        if is_options and spot is not None:
            st.info(f"Spot Price: ₹{spot:,.2f}")

        with st.expander("Full quote payload"):
            st.dataframe(
                pd.DataFrame({"Field": list(q.keys()), "Value": list(q.values())}),
                use_container_width=True,
                hide_index=True,
            )

    except Exception as e:
        st.error(f"Error: {e}")
