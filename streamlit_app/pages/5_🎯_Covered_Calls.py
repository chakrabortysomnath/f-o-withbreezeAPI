from __future__ import annotations

import streamlit as st
import pandas as pd
from utils.api import fetch_holdings, scan_covered_calls, get_covered_call_advice
from utils.auth import require_login

st.set_page_config(page_title="Covered Call Scanner", page_icon="🎯", layout="wide")
require_login()
st.title("🎯 Covered Call Scanner — Nifty 50")
st.caption(
    "Scans all 50 Nifty stocks against your holdings. "
    "Shows call-writing opportunities where you hold a full lot and advises on "
    "accumulating positions where you don't."
)

# ── Sidebar — scan parameters ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Scan Parameters")
    expiry_date = st.text_input(
        "Expiry Date",
        value="",
        placeholder="e.g. 27-Mar-2026",
        help="NSE option expiry in DD-Mon-YYYY format",
    )
    otm_max = st.slider("Max OTM %", min_value=0, max_value=20, value=10, step=1,
                        help="Only show strikes up to this % above spot")
    min_yield = st.slider("Min Premium Yield %", min_value=0.1, max_value=3.0, value=0.3, step=0.1,
                          help="Minimum premium as % of average cost (filters thin premiums)")
    st.divider()
    risk_tolerance = st.selectbox("Risk Tolerance (for Claude)", ["conservative", "moderate", "aggressive"],
                                  index=1)
    income_goal = st.number_input("Monthly Income Goal (% of portfolio)", min_value=0.1, max_value=5.0,
                                  value=1.0, step=0.1)

# ── Step 1: Load holdings ──────────────────────────────────────────────────────
st.subheader("Step 1 — Load Holdings")
exchanges_sel = st.multiselect(
    "Exchanges to load holdings from",
    ["NSE", "BSE", "NFO"],
    default=["NSE"],
)

col_load, col_skip = st.columns([1, 5])
with col_load:
    if st.button("Load Holdings", use_container_width=True):
        with st.spinner("Fetching holdings…"):
            try:
                h, errs = fetch_holdings(exchanges_sel)
                st.session_state["cc_holdings"] = h
                if errs:
                    st.warning(f"Some exchanges returned no data: {list(errs.keys())}")
            except Exception as e:
                st.error(f"Could not load holdings: {e}")

with col_skip:
    if st.button("Skip (scan without holdings)", use_container_width=False):
        st.session_state["cc_holdings"] = []

holdings = st.session_state.get("cc_holdings")
if holdings is None:
    st.info("Load your holdings above, or click **Skip** to scan without a portfolio context.")
    st.stop()

if holdings:
    st.success(f"Loaded {len(holdings)} holding(s).")
else:
    st.info("No holdings loaded — scan will show accumulation advice for all Nifty 50 stocks.")

# ── Step 2: Run scan ───────────────────────────────────────────────────────────
st.subheader("Step 2 — Scan Nifty 50")

if not expiry_date:
    st.warning("Enter an expiry date in the sidebar before scanning.")
    st.stop()

if st.button("🔍 Scan All Nifty 50", type="primary", use_container_width=False):
    with st.spinner("Scanning 50 stocks… (fetching option chains for your positions)"):
        try:
            result = scan_covered_calls(
                expiry_date=expiry_date,
                holdings=holdings,
                otm_max_pct=float(otm_max),
                min_premium_yield_pct=float(min_yield),
            )
            st.session_state["cc_scan"] = result
            st.session_state.pop("cc_advice", None)  # clear stale advice
        except Exception as e:
            st.error(f"Scan failed: {e}")
            st.stop()

scan = st.session_state.get("cc_scan")
if not scan:
    st.info("Click **Scan All Nifty 50** to run the analysis.")
    st.stop()

results = scan["results"]

# ── Summary metrics ────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Stocks", scan["total_symbols"])
m2.metric("✅ Can Write Calls", scan["can_write"])
m3.metric("⚡ Building Position", scan["partial_position"])
m4.metric("⬜ No Position", scan["no_position"])

st.divider()

# ── Results in three tabs ──────────────────────────────────────────────────────
tab_write, tab_build, tab_none = st.tabs([
    f"✅ Ready to Write ({scan['can_write']})",
    f"⚡ Building Position ({scan['partial_position']})",
    f"⬜ No Position ({scan['no_position']})",
])

# Helper: format a float with ₹ or % or dash
def _fmt(v, prefix="", suffix="", decimals=2):
    if v is None:
        return "—"
    return f"{prefix}{v:,.{decimals}f}{suffix}"


# ── Tab 1: Ready to Write ──────────────────────────────────────────────────────
with tab_write:
    write_results = [r for r in results if r["can_write_calls"]]
    if not write_results:
        st.info("No stocks with a full lot in the selected exchanges.")
    else:
        for r in write_results:
            best = r["opportunities"][0] if r["opportunities"] else None
            header_cols = st.columns([3, 1, 1, 1, 1, 1])
            header_cols[0].markdown(f"### {r['stock_code']}")
            header_cols[1].metric("Lots Held", r["lots_held"])
            header_cols[2].metric("Spot", _fmt(r["spot"], "₹"))
            header_cols[3].metric("Avg Cost", _fmt(r["avg_cost"], "₹"))
            if best:
                header_cols[4].metric("Best Ann. Yield", _fmt(best.get("annualised_yield_pct"), suffix="%"))
                header_cols[5].metric("Premium / Lot", _fmt(best.get("premium_cash_per_lot"), "₹", decimals=0))

            if r["opportunities"]:
                df = pd.DataFrame(r["opportunities"])
                df = df.rename(columns={
                    "strike": "Strike",
                    "otm_pct": "OTM %",
                    "premium_bid": "Bid (₹)",
                    "premium_ltp": "LTP (₹)",
                    "premium_yield_pct": "Yield %",
                    "annualised_yield_pct": "Ann. Yield %",
                    "dte": "DTE",
                    "breakeven": "Breakeven (₹)",
                    "max_profit_pct": "Max Profit %",
                    "premium_cash_per_lot": "Cash/Lot (₹)",
                    "total_premium_cash": "Total Cash (₹)",
                })
                display_cols = [
                    "Strike", "OTM %", "Bid (₹)", "LTP (₹)", "Yield %",
                    "Ann. Yield %", "DTE", "Breakeven (₹)", "Max Profit %",
                    "Cash/Lot (₹)", "Total Cash (₹)",
                ]
                st.dataframe(
                    df[[c for c in display_cols if c in df.columns]],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.warning(f"No liquid call strikes found for {r['stock_code']} within the OTM filter.")

            if r.get("accumulation_advice"):
                st.caption(f"ℹ️ {r['accumulation_advice']['message']} (next lot)")

            st.divider()


# ── Tab 2: Building Position ───────────────────────────────────────────────────
with tab_build:
    build_results = [r for r in results if not r["can_write_calls"] and r["held_qty"] > 0]
    if not build_results:
        st.info("No partial positions found.")
    else:
        rows = []
        for r in build_results:
            adv = r.get("accumulation_advice") or {}
            rows.append({
                "Stock": r["stock_code"],
                "Held": r["held_qty"],
                "Lot Size": r["lot_size"],
                "Shares Needed": adv.get("shares_needed", r["shares_short"]),
                "Approx Cost (₹)": adv.get("approx_cost"),
                "Avg Cost (₹)": r.get("avg_cost"),
                "Current LTP (₹)": r.get("spot"),
            })
        df_build = pd.DataFrame(rows)
        st.dataframe(
            df_build.style.format({
                "Approx Cost (₹)": lambda v: f"₹{v:,.0f}" if v else "—",
                "Avg Cost (₹)": lambda v: f"₹{v:,.2f}" if v else "—",
                "Current LTP (₹)": lambda v: f"₹{v:,.2f}" if v else "—",
            }, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(
            "Buy the **Shares Needed** to complete 1 lot, then return here and re-scan to see "
            "covered call opportunities."
        )


# ── Tab 3: No Position ─────────────────────────────────────────────────────────
with tab_none:
    none_results = [r for r in results if r["held_qty"] == 0]
    if not none_results:
        st.info("You hold at least some shares in every Nifty 50 stock.")
    else:
        rows = []
        for r in none_results:
            rows.append({
                "Stock": r["stock_code"],
                "Lot Size": r["lot_size"],
                "Shares to Buy": r["lot_size"],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.caption(
            "These stocks have no position. The lot size shows how many shares you need "
            "to buy before you can write covered calls. "
            "Ask Claude (below) which ones are worth building first."
        )


# ── Step 3: Claude analysis ────────────────────────────────────────────────────
st.subheader("Step 3 — Claude Analysis")
st.caption("Claude will rank the best covered call trades and identify the best accumulation targets.")

if st.button("🤖 Get Claude's Recommendations", type="primary"):
    with st.spinner("Calling Claude (claude-sonnet-4-6)…"):
        try:
            advice_resp = get_covered_call_advice(
                scan_results=results,
                expiry_date=expiry_date,
                risk_tolerance=risk_tolerance,
                income_goal_pct=income_goal,
            )
            st.session_state["cc_advice"] = advice_resp.get("advice", {})
        except Exception as e:
            st.error(f"Claude call failed: {e}")
            st.stop()

advice = st.session_state.get("cc_advice")
if advice:
    if "raw_response" in advice:
        st.warning("Claude returned an unstructured response:")
        st.text(advice["raw_response"])
    else:
        # Market commentary
        if advice.get("market_commentary"):
            st.info(f"**Market Commentary:** {advice['market_commentary']}")

        # Caution flags
        if advice.get("caution_flags"):
            for flag in advice["caution_flags"]:
                st.warning(f"⚠️ {flag}")

        # Top picks
        picks = advice.get("top_picks", [])
        if picks:
            st.markdown("#### Top Covered Call Picks")
            for i, p in enumerate(picks, 1):
                conf_colour = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(
                    str(p.get("confidence", "")).lower(), "⚪"
                )
                with st.expander(
                    f"{conf_colour} #{i}  {p.get('action', p.get('stock', ''))}  "
                    f"| Ann. Yield: {_fmt(p.get('annualised_yield_pct'), suffix='%')}",
                    expanded=(i == 1),
                ):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Premium / Lot (₹)", _fmt(p.get("premium_per_lot"), decimals=0))
                    c2.metric("Total Premium (₹)", _fmt(p.get("total_premium"), decimals=0))
                    c3.metric("Breakeven (₹)", _fmt(p.get("breakeven")))
                    c4.metric("Max Profit %", _fmt(p.get("max_profit_pct"), suffix="%"))
                    st.markdown(f"**Rationale:** {p.get('rationale', '—')}")
                    st.markdown(f"**Risk:** {p.get('risk_note', '—')}")

        # Accumulation priorities
        accum = advice.get("accumulation_priorities", [])
        if accum:
            st.markdown("#### Accumulation Priorities")
            rows = []
            for a in accum:
                rows.append({
                    "Stock": a.get("stock"),
                    "Shares Needed": a.get("shares_needed"),
                    "Approx Cost (₹)": a.get("approx_cost"),
                    "Rationale": a.get("rationale"),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with st.expander("Raw JSON from Claude"):
        st.json(advice)
