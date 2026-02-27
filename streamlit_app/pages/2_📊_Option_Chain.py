import streamlit as st
import pandas as pd
from utils.api import fetch_option_chain
from utils.chart import render_candlestick
from utils.config import load_config, get_symbols, get_symbol_info

st.set_page_config(page_title="Option Chain", page_icon="📊", layout="wide")
st.title("📊 Option Chain Compare")
st.caption("Covered-call P&L analysis for a single expiry.")

load_config()


def _to_f(v) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def build_chain_df(rows: list, spot, lot_size: int, avg_cost: float) -> pd.DataFrame:
    spot_f = _to_f(spot)
    out = []
    for r in rows:
        strike = _to_f(r.get("strike_price"))
        premium = _to_f(r.get("ltp"))
        bid = _to_f(r.get("bid"))
        ask = _to_f(r.get("ask"))

        spread = (ask - bid) if ask is not None and bid is not None else None
        mid = ((ask + bid) / 2) if ask is not None and bid is not None else None
        spread_pct = (spread / mid) if spread is not None and mid and mid > 0 else None

        intrinsic = max(spot_f - strike, 0) if spot_f is not None and strike is not None else None
        time_val = (premium - intrinsic) if premium is not None and intrinsic is not None else None

        breakeven = (avg_cost - premium) if avg_cost and premium is not None else None
        premium_cash = (premium * lot_size) if premium is not None else None
        max_profit = (
            (strike - avg_cost + premium) * lot_size
            if strike is not None and premium is not None and avg_cost
            else None
        )
        return_pct = (
            (strike - avg_cost + premium) / avg_cost
            if strike is not None and premium is not None and avg_cost
            else None
        )

        out.append(
            {
                "Strike": strike,
                "Spot": spot_f,
                "Premium (LTP)": premium,
                "Bid": bid,
                "Ask": ask,
                "Bid Qty": _to_f(r.get("bid_qty")),
                "Ask Qty": _to_f(r.get("ask_qty")),
                "Spread (₹)": spread,
                "Spread %": spread_pct,
                "Intrinsic (₹/shr)": intrinsic,
                "Time Value (₹/shr)": time_val,
                "Breakeven (₹)": breakeven,
                "Premium Cash (₹)": premium_cash,
                "Max Profit @ Strike (₹)": max_profit,
                "Return to Strike %": return_pct,
            }
        )
    return pd.DataFrame(out)


# ── Inputs ─────────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    exchange = st.selectbox("Exchange", ["NFO", "BFO"])
    symbol = st.selectbox("Symbol", get_symbols())

with col2:
    right = st.selectbox("Right", ["call", "put"])
    expiry_date = st.text_input(
        "Expiry Date",
        placeholder="dd-Mon-yyyy  e.g. 27-Mar-2026",
        help="Must match the Breeze format exactly.",
    )

sym_info = get_symbol_info(symbol)

with col3:
    lot_size = st.number_input(
        "Lot Size",
        min_value=1,
        value=sym_info["lot_size"] if sym_info else 1,
        step=1,
        help="Auto-filled from Config. Override if needed.",
    )
    avg_cost = st.number_input(
        "Avg Cost Price (₹/share)",
        min_value=0.0,
        step=0.5,
        format="%.2f",
        help="Your average acquisition cost per share. Used for breakeven and max-profit calculations.",
    )

load_btn = st.button("📥 Load Option Chain", type="primary", use_container_width=True)

if load_btn:
    errors = []
    if not expiry_date:
        errors.append("Expiry Date is required.")
    if avg_cost <= 0:
        errors.append("Average Cost Price must be > 0.")
    if errors:
        for e in errors:
            st.warning(e)
    else:
        with st.spinner(f"Loading {symbol} {right.upper()} chain for {expiry_date}…"):
            try:
                data = fetch_option_chain(exchange, symbol, right, expiry_date)
                st.session_state["cc_data"] = data
                st.session_state["cc_lot"] = int(lot_size)
                st.session_state["cc_avg_cost"] = avg_cost
                st.session_state["cc_sym_info"] = sym_info
                spot = data.get("spot_price")
                msg = f"Loaded {data['rows_count']} strikes"
                if spot:
                    msg += f" | Spot ₹{float(spot):,.2f}"
                st.success(msg)
            except Exception as e:
                st.error(f"Error: {e}")

# ── Results ────────────────────────────────────────────────────────────────────
if "cc_data" not in st.session_state:
    st.stop()

data = st.session_state["cc_data"]
lot_size = st.session_state["cc_lot"]
avg_cost = st.session_state["cc_avg_cost"]
sym_info = st.session_state.get("cc_sym_info")
spot = data.get("spot_price")

st.divider()

# Summary strip
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Symbol", data["symbol"])
m2.metric("Right", data["right"].upper())
m3.metric("Expiry", data["expiry_date"])
m4.metric("Spot Price", f"₹{float(spot):,.2f}" if spot else "—")
m5.metric("Avg Cost", f"₹{avg_cost:,.2f}")

# ── Chain table ────────────────────────────────────────────────────────────────
st.subheader("Option Chain")

df = build_chain_df(data["rows"], spot, lot_size, avg_cost)

# Row-level ITM / ATM / OTM colouring
def _row_color(row):
    strike = row.get("Strike")
    spot_f = _to_f(spot)
    if strike is None or spot_f is None:
        return [""] * len(row)
    diff = abs(strike - spot_f)
    if diff <= 50:  # ATM band (adjust as needed)
        return ["background-color: #FFF8E1; color: #5D4037"] * len(row)
    elif strike < spot_f:  # ITM for calls
        return ["background-color: #FDECEC; color: #B71C1C"] * len(row)
    else:  # OTM for calls
        return ["background-color: #EAF7EA; color: #1B5E20"] * len(row)


inr_cols = [
    "Strike", "Spot", "Premium (LTP)", "Bid", "Ask",
    "Spread (₹)", "Intrinsic (₹/shr)", "Time Value (₹/shr)",
    "Breakeven (₹)", "Premium Cash (₹)", "Max Profit @ Strike (₹)",
]
pct_cols = ["Spread %", "Return to Strike %"]
qty_cols = ["Bid Qty", "Ask Qty"]

fmt = {c: "₹{:,.2f}" for c in inr_cols}
fmt.update({c: "{:.2%}" for c in pct_cols})
fmt.update({c: "{:,.0f}" for c in qty_cols})

styled_df = df.style.apply(_row_color, axis=1).format(fmt, na_rep="—")

st.dataframe(styled_df, use_container_width=True, height=520)

col_dl, col_info = st.columns([1, 3])
with col_dl:
    st.download_button(
        "⬇️ Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{data['symbol']}_{data['right']}_{data['expiry_date'].replace('-', '')}.csv",
        mime="text/csv",
    )
with col_info:
    st.caption(
        "🟡 ATM (within ±50)  |  🔴 ITM  |  🟢 OTM  "
        "*(for calls — reverse for puts)*"
    )

# ── Candlestick chart ──────────────────────────────────────────────────────────
st.divider()
yf_ticker = sym_info.get("yf_ticker") if sym_info else None
nse_label = sym_info.get("nse_symbol", data["symbol"]) if sym_info else data["symbol"]
st.subheader(f"📈 {nse_label} — Underlying (Last 90 Days)")
render_candlestick(yf_ticker, title=f"{nse_label} — Last 90 Days")
