import streamlit as st
import pandas as pd
from utils.api import fetch_holdings
from utils.auth import require_login

st.set_page_config(page_title="My Stock Holdings", page_icon="💼", layout="wide")
require_login()
st.title("💼 My Stock Holdings")
st.caption("Demat holdings from your Breeze / ICICIDirect account across selected exchanges.")

EXCHANGES = ["NSE", "BSE", "NFO", "BFO", "MCX", "NCDEX"]

# ── Exchange selection (before fetch) ──────────────────────────────────────────
selected_exchanges = st.multiselect(
    "Select Exchanges to Load",
    options=EXCHANGES,
    default=["NSE", "BSE"],
    help="Holdings will be fetched separately for each selected exchange and aggregated.",
)

col_btn, _ = st.columns([1, 5])
with col_btn:
    load = st.button("🔄 Load Holdings", type="primary", disabled=not selected_exchanges)

if load:
    if not selected_exchanges:
        st.warning("Please select at least one exchange.")
        st.stop()
    with st.spinner(f"Fetching holdings for {', '.join(selected_exchanges)}…"):
        try:
            holdings, exchange_errors = fetch_holdings(selected_exchanges)
            st.session_state["holdings_data"] = holdings
            st.session_state["holdings_errors"] = exchange_errors
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

    if not holdings:
        st.warning("No holdings found for the selected exchange(s).")
        st.stop()

if "holdings_data" in st.session_state:
    holdings = st.session_state["holdings_data"]
    exchange_errors = st.session_state.get("holdings_errors", {})

    # ── Show any per-exchange errors ───────────────────────────────────────────
    if exchange_errors:
        with st.expander(f"⚠️ {len(exchange_errors)} exchange(s) returned no data", expanded=False):
            st.json(exchange_errors)

    # ── Numeric helper ─────────────────────────────────────────────────────────
    def _f(v, decimals=2):
        try:
            return round(float(v), decimals) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    # ── Build display dataframe ────────────────────────────────────────────────
    rows = []
    total_book = 0.0
    total_market = 0.0
    total_pnl = 0.0

    for h in holdings:
        book = _f(h.get("book_value")) or 0.0
        mkt  = _f(h.get("market_value")) or 0.0
        pnl  = _f(h.get("pnl")) or 0.0
        total_book   += book
        total_market += mkt
        total_pnl    += pnl

        pnl_pct = _f(h.get("pnl_percent"))
        rows.append({
            "Symbol":           h.get("stock_code", ""),
            "Exchange":         h.get("exchange_code", ""),
            "Qty":              h.get("quantity"),
            "Avg Cost (₹)":     _f(h.get("average_cost")),
            "LTP (₹)":          _f(h.get("ltp")),
            "Book Value (₹)":   book if book else None,
            "Market Value (₹)": mkt  if mkt  else None,
            "P&L (₹)":          pnl  if pnl  else None,
            "P&L %":            pnl_pct,
        })

    df = pd.DataFrame(rows)

    # ── Summary metrics ────────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Holdings", len(holdings))
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
        return f"color: {'green' if val >= 0 else 'red'}"

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
        st.json(holdings)
else:
    st.info("Select one or more exchanges above, then click **Load Holdings**.")
