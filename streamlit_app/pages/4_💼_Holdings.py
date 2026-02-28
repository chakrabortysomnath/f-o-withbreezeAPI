import streamlit as st
import pandas as pd
from utils.api import fetch_holdings
from utils.auth import require_login

st.set_page_config(page_title="Holdings", page_icon="💼", layout="wide")
require_login()
st.title("💼 Stock Holdings")
st.caption("All demat holdings in your Breeze / ICICIDirect account.")

if st.button("🔄 Load Holdings", type="primary", use_container_width=False):
    with st.spinner("Fetching holdings from Breeze…"):
        try:
            st.session_state["holdings_data"] = fetch_holdings()
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

    if not st.session_state.get("holdings_data"):
        st.warning("No holdings found in the account.")
        st.stop()

if "holdings_data" in st.session_state:
    holdings = st.session_state["holdings_data"]

    # ── Exchange filter ────────────────────────────────────────────────────────
    exchanges = sorted({h.get("exchange_code", "") for h in holdings if h.get("exchange_code")})
    selected_exchanges = st.multiselect(
        "Filter by Exchange",
        options=exchanges,
        default=exchanges,
    )
    filtered = [h for h in holdings if h.get("exchange_code") in selected_exchanges]

    if not filtered:
        st.warning("No holdings match the selected exchange(s).")
        st.stop()

    # ── Build display dataframe ────────────────────────────────────────────────
    def _f(v, decimals=2):
        try:
            return round(float(v), decimals) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    rows = []
    total_book = 0.0
    total_market = 0.0
    total_pnl = 0.0

    for h in filtered:
        book = _f(h.get("book_value")) or 0.0
        mkt  = _f(h.get("market_value")) or 0.0
        pnl  = _f(h.get("pnl")) or 0.0
        total_book   += book
        total_market += mkt
        total_pnl    += pnl

        pnl_pct = _f(h.get("pnl_percent"))
        rows.append({
            "Symbol":       h.get("stock_code", ""),
            "Exchange":     h.get("exchange_code", ""),
            "Qty":          h.get("quantity"),
            "Avg Cost (₹)": _f(h.get("average_cost")),
            "LTP (₹)":      _f(h.get("ltp")),
            "Book Value (₹)":   book if book else None,
            "Market Value (₹)": mkt  if mkt  else None,
            "P&L (₹)":     pnl  if pnl  else None,
            "P&L %":        pnl_pct,
        })

    df = pd.DataFrame(rows)

    # ── Summary metrics ────────────────────────────────────────────────────────
    pnl_color = "normal" if total_pnl >= 0 else "inverse"
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Holdings", len(filtered))
    m2.metric("Book Value",   f"₹{total_book:,.2f}")
    m3.metric("Market Value", f"₹{total_market:,.2f}")
    m4.metric(
        "Unrealised P&L",
        f"₹{total_pnl:,.2f}",
        delta=f"{(total_pnl / total_book * 100):.2f}%" if total_book else None,
    )

    st.divider()

    # ── Colour P&L column ──────────────────────────────────────────────────────
    def _colour_pnl(val):
        if val is None:
            return ""
        colour = "green" if val >= 0 else "red"
        return f"color: {colour}"

    styled = (
        df.style
        .applymap(_colour_pnl, subset=["P&L (₹)", "P&L %"])
        .format(
            {
                "Avg Cost (₹)":      lambda v: f"₹{v:,.2f}" if v is not None else "—",
                "LTP (₹)":           lambda v: f"₹{v:,.2f}" if v is not None else "—",
                "Book Value (₹)":    lambda v: f"₹{v:,.2f}" if v is not None else "—",
                "Market Value (₹)":  lambda v: f"₹{v:,.2f}" if v is not None else "—",
                "P&L (₹)":           lambda v: f"₹{v:,.2f}" if v is not None else "—",
                "P&L %":             lambda v: f"{v:.2f}%"   if v is not None else "—",
            },
            na_rep="—",
        )
    )

    st.dataframe(styled, use_container_width=True, hide_index=True)

    with st.expander("Raw data"):
        st.json(filtered)
else:
    st.info("Click **Load Holdings** to fetch your current portfolio from Breeze.")
