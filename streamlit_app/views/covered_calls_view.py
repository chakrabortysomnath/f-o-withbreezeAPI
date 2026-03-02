from __future__ import annotations

import datetime
import streamlit as st
import pandas as pd

from utils.api import fetch_holdings, scan_covered_calls, get_covered_call_advice


def _fmt(v, prefix="", suffix="", decimals=2):
    if v is None:
        return "—"
    return f"{prefix}{v:,.{decimals}f}{suffix}"


def render_covered_calls() -> None:
    st.subheader("🎯 Covered Call Scanner — Nifty 50")
    st.caption(
        "Scans all 50 Nifty stocks against your holdings. "
        "Shows call-writing opportunities where you hold a full lot and advises on "
        "accumulating positions where you don't."
    )

    # ── Two-column layout: main content left, controls right ──────────────────
    col_main, col_ctrl = st.columns([3, 1])

    # ── Right-hand control panel ───────────────────────────────────────────────
    with col_ctrl:
        st.markdown("#### Scan Parameters")

        expiry_dt = st.date_input(
            "Expiry Date",
            value=datetime.date.today(),
            min_value=datetime.date.today(),
            help="Select the NSE option expiry date",
            key="cc_expiry_dt",
        )
        expiry_date = expiry_dt.strftime("%d-%b-%Y") if expiry_dt else ""

        with st.expander("⚙️ Advanced Settings", expanded=True):
            otm_max = st.slider(
                "Max OTM %", min_value=0, max_value=20, value=10, step=1,
                help="Only show call strikes up to this % above spot price",
                key="cc_otm_max",
            )
            min_yield = st.slider(
                "Min Premium Yield %", min_value=0.1, max_value=3.0, value=0.3, step=0.1,
                help="Minimum premium as % of average holding cost (filters thin premiums)",
                key="cc_min_yield",
            )
            risk_tolerance = st.selectbox(
                "Risk Tolerance (Claude)",
                ["conservative", "moderate", "aggressive"],
                index=1,
                help="Guides Claude's strike selection in the analysis step",
                key="cc_risk",
            )
            income_goal = st.number_input(
                "Monthly Income Goal %",
                min_value=0.1, max_value=5.0, value=1.0, step=0.1,
                help="Monthly income target as % of portfolio value (for Claude context)",
                key="cc_income_goal",
            )
            st.divider()
            bypass_claude = st.checkbox(
                "Bypass Claude Analysis",
                value=True,
                help="When checked, Step 3 is skipped and no Anthropic API credits are used.",
                key="cc_bypass",
            )

    # ── Main content ───────────────────────────────────────────────────────────
    with col_main:

        # ── Step 1: Load holdings ──────────────────────────────────────────────
        st.subheader("Step 1 — Load Holdings")
        exchanges_sel = st.multiselect(
            "Exchanges to load holdings from",
            ["NSE", "BSE", "NFO"],
            default=["NSE"],
            key="cc_exchanges",
        )

        btn_load, btn_skip, _ = st.columns([1, 2, 4])
        with btn_load:
            if st.button("Load Holdings", use_container_width=True, key="cc_load_btn"):
                with st.spinner("Fetching holdings…"):
                    try:
                        h, errs = fetch_holdings(exchanges_sel)
                        st.session_state["cc_holdings"] = h
                        if errs:
                            st.warning(f"Some exchanges returned no data: {list(errs.keys())}")
                    except Exception as e:
                        st.error(f"Could not load holdings: {e}")

        with btn_skip:
            if st.button("Skip — scan without holdings", key="cc_skip_btn"):
                st.session_state["cc_holdings"] = []

        holdings = st.session_state.get("cc_holdings")
        if holdings is None:
            st.info("Load your holdings above, or click **Skip** to scan without a portfolio context.")
            return

        if holdings:
            st.success(f"Loaded {len(holdings)} holding(s).")
        else:
            st.info("No holdings loaded — scan will show accumulation advice for all Nifty 50 stocks.")

        # ── Step 2: Run scan ───────────────────────────────────────────────────
        st.subheader("Step 2 — Scan Nifty 50")

        if not expiry_date:
            st.warning("Select an expiry date on the right before scanning.")
            return

        if st.button("🔍 Scan All Nifty 50", type="primary", key="cc_scan_btn"):
            with st.spinner("Scanning 50 stocks… (fetching option chains for your positions)"):
                try:
                    result = scan_covered_calls(
                        expiry_date=expiry_date,
                        holdings=holdings,
                        otm_max_pct=float(otm_max),
                        min_premium_yield_pct=float(min_yield),
                    )
                    st.session_state["cc_scan"] = result
                    st.session_state.pop("cc_advice", None)
                except Exception as e:
                    st.error(f"Scan failed: {e}")
                    return

        scan = st.session_state.get("cc_scan")
        if not scan:
            st.info("Click **Scan All Nifty 50** to run the analysis.")
            return

        results = scan["results"]

        # ── Summary metrics ────────────────────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Stocks",       scan["total_symbols"])
        m2.metric("✅ Can Write Calls",  scan["can_write"])
        m3.metric("⚡ Building Position", scan["partial_position"])
        m4.metric("⬜ No Position",       scan["no_position"])

        st.divider()

        # ── Results in three tabs ──────────────────────────────────────────────
        tab_write, tab_build, tab_none = st.tabs([
            f"✅ Ready to Write ({scan['can_write']})",
            f"⚡ Building Position ({scan['partial_position']})",
            f"⬜ No Position ({scan['no_position']})",
        ])

        # ── Tab 1: Ready to Write ──────────────────────────────────────────────
        with tab_write:
            write_results = [r for r in results if r["can_write_calls"]]
            if not write_results:
                st.info("No stocks with a full lot found.")
            else:
                for r in write_results:
                    best = r["opportunities"][0] if r["opportunities"] else None
                    hc = st.columns([3, 1, 1, 1, 1, 1])
                    hc[0].markdown(f"### {r['stock_code']}")
                    hc[1].metric("Lots Held", r["lots_held"])
                    hc[2].metric("Spot",      _fmt(r["spot"],     "₹"))
                    hc[3].metric("Avg Cost",  _fmt(r["avg_cost"], "₹"))
                    if best:
                        hc[4].metric("Best Ann. Yield", _fmt(best.get("annualised_yield_pct"), suffix="%"))
                        hc[5].metric("Premium / Lot",   _fmt(best.get("premium_cash_per_lot"), "₹", decimals=0))

                    if r["opportunities"]:
                        df = pd.DataFrame(r["opportunities"]).rename(columns={
                            "strike": "Strike", "otm_pct": "OTM %",
                            "premium_bid": "Bid (₹)", "premium_ltp": "LTP (₹)",
                            "premium_yield_pct": "Yield %",
                            "annualised_yield_pct": "Ann. Yield %", "dte": "DTE",
                            "breakeven": "Breakeven (₹)", "max_profit_pct": "Max Profit %",
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
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.warning(f"No liquid call strikes found for {r['stock_code']} within the OTM filter.")

                    if r.get("accumulation_advice"):
                        st.caption(f"ℹ️ {r['accumulation_advice']['message']} (next lot)")
                    st.divider()

        # ── Tab 2: Building Position ───────────────────────────────────────────
        with tab_build:
            build_results = [r for r in results if not r["can_write_calls"] and r["held_qty"] > 0]
            if not build_results:
                st.info("No partial positions found.")
            else:
                rows = []
                for r in build_results:
                    adv = r.get("accumulation_advice") or {}
                    rows.append({
                        "Stock":            r["stock_code"],
                        "Held":             r["held_qty"],
                        "Lot Size":         r["lot_size"],
                        "Shares Needed":    adv.get("shares_needed", r["shares_short"]),
                        "Approx Cost (₹)":  adv.get("approx_cost"),
                        "Avg Cost (₹)":     r.get("avg_cost"),
                        "Current LTP (₹)":  r.get("spot"),
                    })
                df_build = pd.DataFrame(rows)
                st.dataframe(
                    df_build.style.format({
                        "Approx Cost (₹)": lambda v: f"₹{v:,.0f}" if v else "—",
                        "Avg Cost (₹)":    lambda v: f"₹{v:,.2f}" if v else "—",
                        "Current LTP (₹)": lambda v: f"₹{v:,.2f}" if v else "—",
                    }, na_rep="—"),
                    use_container_width=True, hide_index=True,
                )
                st.caption("Buy the **Shares Needed** to complete 1 lot, then re-scan.")

        # ── Tab 3: No Position ─────────────────────────────────────────────────
        with tab_none:
            none_results = [r for r in results if r["held_qty"] == 0]
            if not none_results:
                st.info("You hold at least some shares in every Nifty 50 stock.")
            else:
                rows = []
                for r in none_results:
                    rows.append({"Stock": r["stock_code"], "Lot Size": r["lot_size"], "Shares to Buy": r["lot_size"]})
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                st.caption("Ask Claude below which stocks are best to build first.")

        # ── Step 3: Claude analysis ────────────────────────────────────────────
        st.subheader("Step 3 — Claude Analysis")

        if bypass_claude:
            st.info(
                "Claude analysis is **bypassed**. "
                "Uncheck **Bypass Claude Analysis** in ⚙️ Advanced Settings to enable."
            )
        else:
            st.caption("Claude will rank the best covered call trades and identify accumulation priorities.")

            if st.button("🤖 Get Claude's Recommendations", type="primary", key="cc_claude_btn"):
                st.session_state["cc_confirm_pending"] = True

            if st.session_state.get("cc_confirm_pending"):
                st.warning(
                    "This will call the **Anthropic Claude API** and consume API credits "
                    "(approx. ₹2–3 per call). Proceed?"
                )
                col_yes, col_no, _ = st.columns([1, 1, 5])
                with col_yes:
                    if st.button("✅ Yes, call Claude", use_container_width=True, key="cc_yes_btn"):
                        st.session_state["cc_confirm_pending"] = False
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
                with col_no:
                    if st.button("❌ Cancel", use_container_width=True, key="cc_no_btn"):
                        st.session_state["cc_confirm_pending"] = False
                        st.rerun()

        advice = st.session_state.get("cc_advice")
        if advice:
            if "raw_response" in advice:
                st.warning("Claude returned an unstructured response:")
                st.text(advice["raw_response"])
            else:
                if advice.get("market_commentary"):
                    st.info(f"**Market Commentary:** {advice['market_commentary']}")
                for flag in advice.get("caution_flags", []):
                    st.warning(f"⚠️ {flag}")

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
                            c3.metric("Breakeven (₹)",     _fmt(p.get("breakeven")))
                            c4.metric("Max Profit %",      _fmt(p.get("max_profit_pct"), suffix="%"))
                            st.markdown(f"**Rationale:** {p.get('rationale', '—')}")
                            st.markdown(f"**Risk:** {p.get('risk_note', '—')}")

                accum = advice.get("accumulation_priorities", [])
                if accum:
                    st.markdown("#### Accumulation Priorities")
                    rows = [{"Stock": a.get("stock"), "Shares Needed": a.get("shares_needed"),
                              "Approx Cost (₹)": a.get("approx_cost"), "Rationale": a.get("rationale")}
                             for a in accum]
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            with st.expander("Raw JSON from Claude"):
                st.json(advice)
