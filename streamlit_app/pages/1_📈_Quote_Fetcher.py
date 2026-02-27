import streamlit as st
import pandas as pd
from utils.api import fetch_quote, fetch_option_strikes
from utils.config import load_config, get_symbols, get_symbol_info

st.set_page_config(page_title="Quote Fetcher", page_icon="📈", layout="wide")
st.title("📈 Quote Fetcher")
st.caption("Fetch a live price for any stock, future, or option contract.")

load_config()  # ensure session_state is initialised

# ── Inputs ─────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    exchange = st.selectbox("Exchange", ["NSE", "NFO", "BFO"])
    symbol = st.selectbox("Symbol", get_symbols())
    product_type = st.selectbox("Product Type", ["cash", "futures", "options"])

is_fno = exchange in ("NFO", "BFO")
is_options = product_type == "options"

with col2:
    expiry_date = ""
    if is_fno:
        expiry_date = st.text_input(
            "Expiry Date",
            placeholder="dd-Mon-yyyy  e.g. 27-Mar-2026",
            help="Must match the Breeze format exactly, e.g. 27-Mar-2026",
        )

    right = None
    if is_options:
        right = st.selectbox("Right", ["call", "put"])

        st.markdown("**Strike Price**")
        load_strikes_btn = st.button("Load Strikes ↓", help="Fetch available strikes from Breeze")

        if load_strikes_btn:
            if not expiry_date or not right:
                st.warning("Set Expiry Date and Right before loading strikes.")
            else:
                with st.spinner("Loading strikes…"):
                    try:
                        strikes, spot = fetch_option_strikes(exchange, symbol, expiry_date, right)
                        st.session_state["q_strikes"] = strikes
                        st.session_state["q_spot"] = spot
                        label = f"{len(strikes)} strikes loaded"
                        if spot:
                            label += f" | Spot ₹{float(spot):,.2f}"
                        st.success(label)
                    except Exception as e:
                        st.error(f"Failed to load strikes: {e}")

strikes_list = st.session_state.get("q_strikes", [])
strike_price = None
if is_options and strikes_list:
    spot_val = st.session_state.get("q_spot")
    if spot_val:
        st.caption(f"Spot: ₹{float(spot_val):,.2f}")
    # Default selection to ATM (nearest strike to spot)
    default_idx = 0
    if spot_val:
        diffs = [abs(float(s) - float(spot_val)) for s in strikes_list]
        default_idx = diffs.index(min(diffs))
    strike_price = st.selectbox("Strike Price", strikes_list, index=default_idx)

# ── Fetch ──────────────────────────────────────────────────────────────────────
st.divider()
fetch_btn = st.button("🔄 Fetch Quote", type="primary", use_container_width=True)

if fetch_btn:
    if not symbol:
        st.warning("Select a symbol first.")
    else:
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

                def to_float(v):
                    try:
                        return float(v) if v not in (None, "") else None
                    except (TypeError, ValueError):
                        return None

                ltp = to_float(q.get("ltp"))
                prev = to_float(q.get("prev_close"))
                delta = f"{ltp - prev:+.2f}" if ltp is not None and prev is not None else None

                st.success(f"Quote fetched — {exchange}:{symbol}")

                # ── Metric row 1 ───────────────────────────────────────────
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("LTP", f"₹{ltp:,.2f}" if ltp is not None else "—", delta=delta)
                m2.metric("Open", f"₹{to_float(q.get('open')):,.2f}" if to_float(q.get("open")) is not None else "—")
                m3.metric("High", f"₹{to_float(q.get('high')):,.2f}" if to_float(q.get("high")) is not None else "—")
                m4.metric("Low", f"₹{to_float(q.get('low')):,.2f}" if to_float(q.get("low")) is not None else "—")
                m5.metric("Prev Close", f"₹{prev:,.2f}" if prev is not None else "—")

                # ── Metric row 2 ───────────────────────────────────────────
                m6, m7, m8, m9, m10 = st.columns(5)
                bid = to_float(q.get("bid_price"))
                ask = to_float(q.get("ask_price"))
                vol = q.get("volume")
                spot = to_float(q.get("spot_price"))

                m6.metric("Bid", f"₹{bid:,.2f}" if bid is not None else "—")
                m7.metric("Ask", f"₹{ask:,.2f}" if ask is not None else "—")
                m8.metric("Bid Qty", q.get("bid_qty") or "—")
                m9.metric("Ask Qty", q.get("ask_qty") or "—")
                m10.metric("Volume", f"{int(float(vol)):,}" if vol not in (None, "") else "—")

                if is_options and spot is not None:
                    st.info(f"Spot Price: ₹{spot:,.2f}")

                with st.expander("Full quote payload"):
                    st.dataframe(
                        pd.DataFrame({"Field": q.keys(), "Value": q.values()}),
                        use_container_width=True,
                        hide_index=True,
                    )

            except Exception as e:
                st.error(f"Error: {e}")
