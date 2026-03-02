from __future__ import annotations

import streamlit as st
import pandas as pd
import yfinance as yf

from utils.api import fetch_holdings
from utils.config import load_config, get_symbol_info


# ── yfinance enrichment (cached 1 h) ──────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _enrich_batch(yf_tickers: tuple[str, ...]) -> dict[str, dict]:
    """
    Returns {yf_ticker: {sector, 52w_high, 52w_low}} for each ticker.
    Uses yf.download for 52w data (fast, one network call) and individual
    .info for sector (cached, slower but done once per hour).
    """
    result: dict[str, dict] = {t: {"sector": "—", "52w_high": None, "52w_low": None} for t in yf_tickers}
    valid = [t for t in yf_tickers if t and not t.startswith("^")]
    if not valid:
        return result

    # 52-week high/low via bulk download
    try:
        hist = yf.download(valid, period="1y", progress=False, auto_adjust=True)
        if not hist.empty:
            if len(valid) == 1:
                result[valid[0]]["52w_high"] = round(float(hist["High"].max()), 2)
                result[valid[0]]["52w_low"]  = round(float(hist["Low"].min()),  2)
            else:
                for sym in valid:
                    try:
                        result[sym]["52w_high"] = round(float(hist["High"][sym].dropna().max()), 2)
                        result[sym]["52w_low"]  = round(float(hist["Low"][sym].dropna().min()),  2)
                    except Exception:
                        pass
    except Exception:
        pass

    # Sector via individual ticker.info (one call per ticker)
    for sym in valid:
        try:
            info = yf.Ticker(sym).info
            result[sym]["sector"] = info.get("sector") or info.get("industry") or "—"
        except Exception:
            pass

    return result


# ── Helpers ────────────────────────────────────────────────────────────────────

def _f(v, decimals: int = 2):
    try:
        return round(float(v), decimals) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _build_table(holdings: list[dict], enrichment: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for h in holdings:
        stock   = h.get("stock_code", "")
        sym_cfg = get_symbol_info(stock) or {}
        yf_tk   = sym_cfg.get("yf_ticker", "")
        enrich  = enrichment.get(yf_tk, {})

        avg     = _f(h.get("average_cost"))
        ltp     = _f(h.get("ltp"))
        qty     = _f(h.get("quantity"), 0)
        book    = _f(h.get("book_value"))
        mkt     = _f(h.get("market_value"))

        # Derived metrics
        unreal_pnl = round(mkt - book, 2) if (mkt is not None and book is not None) else None
        # Unrealised P&L % as % of Market Value (as requested)
        unreal_pnl_pct = round(unreal_pnl / mkt * 100, 2) if (unreal_pnl is not None and mkt and mkt != 0) else None
        # Simple return % vs avg cost (labelled IRR %)
        irr_pct = round((ltp - avg) / avg * 100, 2) if (ltp and avg and avg != 0) else None

        rows.append({
            "Symbol":              stock,
            "Exchange":            h.get("exchange_code", ""),
            "Sector":              enrich.get("sector", "—"),
            "Qty":                 int(qty) if qty is not None else None,
            "Avg Cost (₹)":        avg,
            "LTP (₹)":             ltp,
            "Book Value (₹)":      book,
            "Market Value (₹)":    mkt,
            "Unrealised P&L (₹)":  unreal_pnl,
            "Unrealised P&L %":    unreal_pnl_pct,
            "IRR %":               irr_pct,
            "52W High (₹)":        enrich.get("52w_high"),
            "52W Low (₹)":         enrich.get("52w_low"),
        })
    return pd.DataFrame(rows)


def _colour_pnl(val):
    if val is None:
        return ""
    return f"color: {'#26a69a' if val >= 0 else '#ef5350'}"


def _display_exchange_table(df_ex: pd.DataFrame) -> None:
    if df_ex.empty:
        st.info("No holdings for this exchange.")
        return

    numeric_fmt = {
        "Avg Cost (₹)":       lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "LTP (₹)":            lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "Book Value (₹)":     lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "Market Value (₹)":   lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "Unrealised P&L (₹)": lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "Unrealised P&L %":   lambda v: f"{v:.2f}%"   if v is not None else "—",
        "IRR %":              lambda v: f"{v:.2f}%"   if v is not None else "—",
        "52W High (₹)":       lambda v: f"₹{v:,.2f}" if v is not None else "—",
        "52W Low (₹)":        lambda v: f"₹{v:,.2f}" if v is not None else "—",
    }
    pnl_cols = ["Unrealised P&L (₹)", "Unrealised P&L %", "IRR %"]

    styled = (
        df_ex.style
        .applymap(_colour_pnl, subset=[c for c in pnl_cols if c in df_ex.columns])
        .format(numeric_fmt, na_rep="—")
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


# ── Main render ────────────────────────────────────────────────────────────────

def render_portfolio() -> None:
    load_config()
    st.subheader("💼 My Holdings")

    # ── Controls ───────────────────────────────────────────────────────────────
    ctrl_left, ctrl_right = st.columns([3, 1])
    with ctrl_left:
        selected_exchanges = st.multiselect(
            "Exchanges",
            ["NSE", "BSE", "NFO", "BFO", "MCX", "NCDEX"],
            default=["NSE", "BSE"],
            key="pf_exchanges",
        )
    with ctrl_right:
        enrich_yf = st.checkbox(
            "Enrich: sector & 52W data",
            value=True,
            key="pf_enrich",
            help="Fetch sector and 52-week high/low from Yahoo Finance (cached 1 h)",
        )

    btn_col, _ = st.columns([1, 5])
    with btn_col:
        reload = st.button("🔄 Reload Holdings", key="pf_reload")

    # ── Auto-load on first visit or explicit reload ────────────────────────────
    first_visit = "pf_holdings" not in st.session_state
    if first_visit or reload:
        if not selected_exchanges:
            st.warning("Select at least one exchange.")
            return
        with st.spinner(f"Fetching holdings for {', '.join(selected_exchanges)}…"):
            try:
                holdings, errors = fetch_holdings(selected_exchanges)
                st.session_state["pf_holdings"] = holdings
                st.session_state["pf_errors"]   = errors
            except Exception as e:
                st.error(f"Error fetching holdings: {e}")
                return

    holdings      = st.session_state.get("pf_holdings", [])
    exchange_errs = st.session_state.get("pf_errors", {})

    if not holdings:
        st.info("No holdings found. Try selecting different exchanges and reloading.")
        return

    if exchange_errs:
        with st.expander(f"⚠️ {len(exchange_errs)} exchange(s) returned no data"):
            st.json(exchange_errs)

    # ── yfinance enrichment ────────────────────────────────────────────────────
    enrichment: dict[str, dict] = {}
    if enrich_yf:
        yf_tickers = tuple(
            (get_symbol_info(h.get("stock_code", "")) or {}).get("yf_ticker", "")
            for h in holdings
        )
        unique_tickers = tuple(t for t in set(yf_tickers) if t)
        if unique_tickers:
            with st.spinner("Enriching with Yahoo Finance data…"):
                enrichment = _enrich_batch(unique_tickers)

    # ── Build master table ─────────────────────────────────────────────────────
    df = _build_table(holdings, enrichment)

    # ── Summary metrics ────────────────────────────────────────────────────────
    total_book  = df["Book Value (₹)"].sum()
    total_mkt   = df["Market Value (₹)"].sum()
    total_upnl  = df["Unrealised P&L (₹)"].sum()
    upnl_pct    = (total_upnl / total_mkt * 100) if total_mkt else None

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Holdings",       len(holdings))
    m2.metric("Book Value",     f"₹{total_book:,.0f}")
    m3.metric("Market Value",   f"₹{total_mkt:,.0f}")
    m4.metric("Unrealised P&L", f"₹{total_upnl:,.0f}",
              delta=f"{upnl_pct:.2f}% of mkt" if upnl_pct is not None else None)
    m5.metric("Stocks Held",    df["Symbol"].nunique())

    st.divider()

    # ── Tabs per exchange ──────────────────────────────────────────────────────
    exchanges_in_data = sorted(df["Exchange"].dropna().unique())

    if not exchanges_in_data:
        st.info("No exchange data to display.")
        return

    if len(exchanges_in_data) == 1:
        _display_exchange_table(df.drop(columns=["Exchange"]))
    else:
        tabs = st.tabs([f"🏦 {ex}" for ex in exchanges_in_data] + ["📋 All"])
        for tab, ex in zip(tabs[:-1], exchanges_in_data):
            with tab:
                ex_df = df[df["Exchange"] == ex].drop(columns=["Exchange"]).reset_index(drop=True)
                _display_exchange_table(ex_df)
        with tabs[-1]:
            _display_exchange_table(df.reset_index(drop=True))

    with st.expander("Raw API data"):
        st.json(holdings)
