"""
decision_desk_view.py — 12-tile Nifty 50 covered call decision dashboard.

Layout
------
render_decision_desk()
├── col_ctrl [1]:  expiry, filters, holdings source, fetch button
└── col_main [3]:  summary metrics + 12-tile grid (3×4) + drill-down expander
"""
from __future__ import annotations

import calendar
import datetime
import math

import pandas as pd
import streamlit as st

from utils.api import scan_decision_desk, fetch_holdings
from utils.config import get_dd_config, load_config
from utils.chart import fetch_hv30
from views.cc_metrics import compute_opportunity_metrics, add_composite_score


# ── Expiry helpers ────────────────────────────────────────────────────────────

def _last_thursday_of_month(year: int, month: int) -> datetime.date:
    last = calendar.monthrange(year, month)[1]
    d = datetime.date(year, month, last)
    while d.weekday() != 3:   # 3 = Thursday
        d -= datetime.timedelta(days=1)
    return d


def _compute_expiries(switch_days: int) -> tuple[datetime.date, datetime.date]:
    """Return (primary_expiry, next_expiry) based on today and switch_days threshold."""
    today = datetime.date.today()
    y, m  = today.year, today.month
    curr  = _last_thursday_of_month(y, m)
    nm    = m + 1 if m < 12 else 1
    ny    = y if m < 12 else y + 1
    nxt   = _last_thursday_of_month(ny, nm)
    if (curr - today).days <= switch_days:
        # Too close to current expiry — advance to next and the one after
        nm2   = nm + 1 if nm < 12 else 1
        ny2   = ny if nm < 12 else ny + 1
        return nxt, _last_thursday_of_month(ny2, nm2)
    return curr, nxt


# ── Formatting helpers ────────────────────────────────────────────────────────

def _fmt_inr(v, decimals: int = 0) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    try:
        return f"₹{float(v):,.{decimals}f}"
    except Exception:
        return "—"


def _fmt_pct(v, decimals: int = 2) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    try:
        return f"{float(v):.{decimals}f}%"
    except Exception:
        return "—"


# ── Tile renderer ─────────────────────────────────────────────────────────────

def _render_tile(title: str, df_top: pd.DataFrame, display_cols: list[str]) -> None:
    with st.container(border=True):
        st.markdown(f"**{title}**")
        if df_top.empty:
            st.caption("No data — adjust filters or load holdings.")
            return
        # Only keep columns that actually exist in the dataframe
        valid_cols = [c for c in display_cols if c in df_top.columns]
        st.dataframe(
            df_top[valid_cols].head(st.session_state.get("dd_top_n", 10)),
            hide_index=True,
            use_container_width=True,
        )


# ── Main render ───────────────────────────────────────────────────────────────

def render_decision_desk() -> None:
    st.subheader("🏦 Decision Desk")
    st.caption(
        "12-tile ranked view of covered call opportunities across all 50 Nifty stocks. "
        "Metrics include transaction costs, downside cushion, volatility, and composite scoring."
    )

    dd_cfg     = get_dd_config()
    switch_days = dd_cfg.get("expiry_switch_days", 7)
    top_n       = dd_cfg.get("tile_top_n", 10)
    st.session_state["dd_top_n"] = top_n

    # ── Two-column layout ──────────────────────────────────────────────────────
    col_main, col_ctrl = st.columns([3, 1])

    # ═══════════════════════════════════════════════════════════════════════════
    # RIGHT: Control panel
    # ═══════════════════════════════════════════════════════════════════════════
    with col_ctrl:
        st.markdown("#### Controls")

        # Expiry
        expiry_mode = st.selectbox(
            "Expiry Mode",
            ["auto", "current month", "next month", "custom"],
            index=0,
            key="dd_expiry_mode",
            help="'auto' switches away from the current month when ≤ switch_days remain.",
        )

        primary_dt, next_dt = _compute_expiries(switch_days)

        if expiry_mode == "auto":
            pass   # already set above
        elif expiry_mode == "current month":
            today = datetime.date.today()
            primary_dt = _last_thursday_of_month(today.year, today.month)
            nm = today.month + 1 if today.month < 12 else 1
            ny = today.year if today.month < 12 else today.year + 1
            next_dt = _last_thursday_of_month(ny, nm)
        elif expiry_mode == "next month":
            today = datetime.date.today()
            nm = today.month + 1 if today.month < 12 else 1
            ny = today.year if today.month < 12 else today.year + 1
            primary_dt = _last_thursday_of_month(ny, nm)
            nm2 = nm + 1 if nm < 12 else 1
            ny2 = ny if nm < 12 else ny + 1
            next_dt = _last_thursday_of_month(ny2, nm2)
        else:  # custom
            primary_dt = st.date_input(
                "Primary Expiry",
                value=primary_dt,
                min_value=datetime.date.today(),
                key="dd_custom_expiry",
            )

        primary_str = primary_dt.strftime("%d-%b-%Y")
        next_str    = next_dt.strftime("%d-%b-%Y")
        dte         = (primary_dt - datetime.date.today()).days

        st.caption(f"Primary: **{primary_str}** · DTE: **{dte}**")
        st.caption(f"Next: {next_str}")

        if dte <= switch_days:
            st.warning(
                f"⚠️ Only **{dte}** day(s) to expiry. "
                f"Consider switching to **{next_str}**."
            )

        st.divider()

        # ── Filters ────────────────────────────────────────────────────────
        st.markdown("#### Filters")

        all_sectors = sorted({
            s.get("sector", "—")
            for s in load_config().get("symbols", [])
            if s.get("sector") and s.get("sector") != "Index"
        })

        sector_filter = st.multiselect(
            "Sectors",
            all_sectors,
            default=[],
            key="dd_sector_filter",
            help="Leave blank to show all sectors.",
        )

        holding_status = st.radio(
            "Holding Status",
            ["All", "Held Only", "Not Held", "Recovery"],
            index=0,
            key="dd_holding_status",
        )

        liquid_only = st.checkbox(
            "Liquid Only",
            value=True,
            key="dd_liquid_only",
            help="Exclude strikes with wide spreads or low volume.",
        )

        gross_net = st.radio(
            "Show Yields",
            ["Net (post-costs)", "Gross"],
            index=0,
            key="dd_gross_net",
        )
        use_net = gross_net.startswith("Net")

        st.divider()

        # ── Holdings source ────────────────────────────────────────────────
        st.markdown("#### Holdings")

        use_portfolio_holdings = st.checkbox(
            "Use Portfolio tab holdings",
            value=True,
            key="dd_use_pf_holdings",
            help="Reuse the holdings already loaded in the Portfolio tab.",
        )

        if not use_portfolio_holdings:
            exchanges_sel = st.multiselect(
                "Exchanges",
                ["NSE", "BSE", "NFO"],
                default=["NSE"],
                key="dd_exchanges",
            )
            if st.button("Load Holdings", key="dd_load_holdings_btn"):
                with st.spinner("Fetching holdings…"):
                    try:
                        h, _ = fetch_holdings(exchanges_sel)
                        st.session_state["dd_holdings"] = h
                    except Exception as exc:
                        st.error(f"Could not load holdings: {exc}")

        if use_portfolio_holdings:
            holdings = st.session_state.get("pf_holdings", [])
            if not holdings:
                st.caption("No Portfolio holdings loaded yet. Load them in the Portfolio tab first.")
        else:
            holdings = st.session_state.get("dd_holdings", [])

        st.divider()

        # ── Fetch button ───────────────────────────────────────────────────
        if st.button(
            "🔄 Fetch All 50 Chains",
            type="primary",
            use_container_width=True,
            key="dd_fetch_btn",
        ):
            with st.spinner("Fetching 50 option chains (~60 s)…"):
                try:
                    result = scan_decision_desk(
                        expiry_date=primary_str,
                        holdings=holdings,
                    )
                    st.session_state["dd_raw_data"]  = result
                    st.session_state["dd_fetch_ts"]  = datetime.datetime.now().strftime("%H:%M:%S")
                    st.session_state["dd_fetch_dte"] = dte
                    # Invalidate downstream caches
                    st.session_state.pop("dd_hv30_map",    None)
                    st.session_state.pop("dd_hv30_done",   None)
                    st.session_state.pop("dd_metrics_df",  None)
                except Exception as exc:
                    st.error(f"Fetch failed: {exc}")

        if "dd_fetch_ts" in st.session_state:
            st.caption(f"Last fetched: {st.session_state['dd_fetch_ts']}")

    # ═══════════════════════════════════════════════════════════════════════════
    # LEFT: Main content
    # ═══════════════════════════════════════════════════════════════════════════
    with col_main:
        raw = st.session_state.get("dd_raw_data")
        if not raw:
            st.info(
                "Configure filters on the right, then click **🔄 Fetch All 50 Chains** to load data."
            )
            return

        results      = raw.get("results", [])
        fetch_dte    = st.session_state.get("dd_fetch_dte", dte)

        # ── Chain error summary ────────────────────────────────────────────
        errors = [(r["stock_code"], r["chain_error"]) for r in results if r.get("chain_error")]
        if errors:
            with st.expander(f"⚠️ {len(errors)} symbol(s) had chain errors", expanded=False):
                st.dataframe(
                    pd.DataFrame(errors, columns=["Symbol", "Error"]),
                    hide_index=True, use_container_width=True,
                )

        # ── HV enrichment (runs once after each fetch) ─────────────────────
        if not st.session_state.get("dd_hv30_done"):
            sym_cfg_map: dict[str, str] = {
                s["nfo_symbol"]: s.get("yf_ticker", "")
                for s in load_config().get("symbols", [])
            }
            hv30_map: dict[str, float | None] = {}
            prog = st.progress(0, text="Enriching with 30-day volatility data…")
            n = len(results)
            for i, r in enumerate(results):
                sc  = r["stock_code"]
                tkr = sym_cfg_map.get(sc, "")
                hv30_map[sc] = fetch_hv30(tkr) if tkr else None
                prog.progress((i + 1) / n, text=f"HV: {sc} ({i+1}/{n})")
            prog.empty()
            st.session_state["dd_hv30_map"]  = hv30_map
            st.session_state["dd_hv30_done"] = True

        hv30_map = st.session_state.get("dd_hv30_map", {})

        # ── Build sector map for metrics engine ───────────────────────────
        sector_map = {
            s["nfo_symbol"]: s.get("sector", "—")
            for s in load_config().get("symbols", [])
        }
        dd_cfg_with_sector = {**dd_cfg, "_sector_map": sector_map}

        # ── Compute / retrieve metrics DataFrame ───────────────────────────
        if "dd_metrics_df" not in st.session_state:
            metrics_df = compute_opportunity_metrics(
                raw_results=results,
                dd_config=dd_cfg_with_sector,
                hv30_map=hv30_map,
                dte=fetch_dte,
            )
            st.session_state["dd_metrics_df"] = metrics_df
        else:
            metrics_df = st.session_state["dd_metrics_df"]

        if metrics_df.empty:
            st.warning("No opportunities found in the fetched data.")
            return

        # ── Apply filters ──────────────────────────────────────────────────
        fdf = metrics_df.copy()

        if sector_filter:
            fdf = fdf[fdf["sector"].isin(sector_filter)]

        if holding_status == "Held Only":
            fdf = fdf[fdf["is_held"] == True]
        elif holding_status == "Not Held":
            fdf = fdf[fdf["is_held"] == False]
        elif holding_status == "Recovery":
            fdf = fdf[fdf["is_recovery"] == True]

        if liquid_only:
            fdf = fdf[fdf["illiquid_flag"] == False]

        # Apply composite score on filtered set
        if not fdf.empty:
            fdf = add_composite_score(fdf, dd_cfg.get("composite_weights", {}))

        yield_col     = "net_annualised_yield_pct"  if use_net else "gross_annualised_yield_pct"
        premium_col   = "net_premium_per_lot"       if use_net else "gross_premium_per_lot"
        yield_label   = "Net Ann.Yield%" if use_net else "Gross Ann.Yield%"
        premium_label = "Net Prem/Lot"  if use_net else "Gross Prem/Lot"

        # ── Summary metrics ────────────────────────────────────────────────
        total_opps  = len(fdf)
        held_stocks = fdf[fdf["is_held"] == True]["stock_code"].nunique()
        recovery_ct = fdf[fdf["is_recovery"] == True]["stock_code"].nunique()
        best_yield  = fdf[yield_col].max() if yield_col in fdf.columns and not fdf.empty else None

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Opportunities",    total_opps)
        m2.metric("Held Stocks",      held_stocks)
        m3.metric("Recovery Stocks",  recovery_ct)
        m4.metric(
            "Best Ann. Yield",
            f"{best_yield:.1f}%" if best_yield is not None else "—",
        )

        st.divider()

        if fdf.empty:
            st.info("No opportunities match the current filters.")
            return

        # ── 12-Tile grid (3 rows × 4 columns) ──────────────────────────────
        def _top(sub: pd.DataFrame, sort_col: str, asc: bool = False) -> pd.DataFrame:
            if sub.empty or sort_col not in sub.columns:
                return pd.DataFrame()
            return sub.dropna(subset=[sort_col]).sort_values(sort_col, ascending=asc)

        base_held     = fdf[fdf["is_held"] == True]
        base_recovery = fdf[fdf["is_recovery"] == True]
        base_not_held = fdf[fdf["is_held"] == False]

        # ── Row 1 ──────────────────────────────────────────────────────────
        r1c1, r1c2, r1c3, r1c4 = st.columns(4)

        with r1c1:
            _render_tile(
                "💰 Highest Premium Income",
                _top(fdf, "net_premium_per_lot"),
                ["stock_code", "strike", "otm_pct", "net_premium_per_lot", "lot_size"],
            )

        with r1c2:
            _render_tile(
                "📈 Highest Yield on Cost",
                _top(fdf, yield_col),
                ["stock_code", "strike", "otm_pct", yield_col, "dte"],
            )

        with r1c3:
            _render_tile(
                "🔄 Best Annualised Yield",
                _top(fdf, "gross_annualised_yield_pct"),
                ["stock_code", "strike", "otm_pct", "gross_annualised_yield_pct", "net_annualised_yield_pct"],
            )

        with r1c4:
            _render_tile(
                "🏆 Best Return If Assigned",
                _top(base_held, "total_return_if_assigned_pct"),
                ["stock_code", "strike", "avg_cost", "total_return_if_assigned_pct", "assigned_profit_inr"],
            )

        # ── Row 2 ──────────────────────────────────────────────────────────
        r2c1, r2c2, r2c3, r2c4 = st.columns(4)

        with r2c1:
            _render_tile(
                "✅ Best Return If Expires",
                _top(fdf, "return_if_expires_pct"),
                ["stock_code", "strike", "otm_pct", "return_if_expires_pct", "net_premium_per_lot"],
            )

        with r2c2:
            _render_tile(
                "🏗️ Lowest Build-Up Cost",
                _top(base_not_held, "buildup_cost_per_lot", asc=True),
                ["stock_code", "spot", "lot_size", "buildup_cost_per_lot", yield_col],
            )

        with r2c3:
            best_exit_df = base_held[base_held["lots_held"] >= 1] if not base_held.empty else pd.DataFrame()
            _render_tile(
                "🚪 Best Exit Calls (Held)",
                _top(best_exit_df, "assigned_profit_inr"),
                ["stock_code", "strike", "avg_cost", "spot", "assigned_profit_inr", "lots_held"],
            )

        with r2c4:
            _render_tile(
                "🔄 Recovery Calls",
                _top(base_recovery, "otm_pct"),
                ["stock_code", "avg_cost", "spot", "recovery_gap_pct", "strike", "net_premium_per_lot"],
            )

        # ── Row 3 ──────────────────────────────────────────────────────────
        r3c1, r3c2, r3c3, r3c4 = st.columns(4)

        with r3c1:
            _render_tile(
                "🛡️ Best Downside Cushion",
                _top(base_held, "downside_cushion_pct"),
                ["stock_code", "strike", "breakeven", "downside_cushion_pct", "otm_pct"],
            )

        with r3c2:
            thin_df = fdf[fdf["thin_cushion_flag"] == True] if not fdf.empty else pd.DataFrame()
            _render_tile(
                "⚠️ Thin Cushion Alert",
                _top(thin_df, "hv30_cushion_ratio", asc=True),
                ["stock_code", "strike", "hv30", "expected_move_pct_dte", "downside_cushion_pct", "hv30_cushion_ratio"],
            )

        with r3c3:
            illiq_df = metrics_df[metrics_df["illiquid_flag"] == True].copy() if not metrics_df.empty else pd.DataFrame()
            if not illiq_df.empty and sector_filter:
                illiq_df = illiq_df[illiq_df["sector"].isin(sector_filter)]
            _render_tile(
                "💧 Illiquid Options",
                _top(illiq_df, "gross_premium_per_lot"),
                ["stock_code", "strike", "gross_premium_per_lot", "spread_pct", "bid_qty", "ask_qty"],
            )

        with r3c4:
            _render_tile(
                "⭐ Composite Score",
                _top(fdf, "composite_score"),
                ["stock_code", "strike", "composite_score", "composite_score_label", yield_col, "downside_cushion_pct"],
            )

        # ── Drill-Down ─────────────────────────────────────────────────────
        st.divider()
        with st.expander("🔍 Opportunity Drill-Down", expanded=False):
            if fdf.empty:
                st.info("No opportunities to drill down on.")
            else:
                all_symbols = sorted(fdf["stock_code"].unique())
                drill_stock = st.selectbox(
                    "Select Stock", all_symbols, key="dd_drill_stock"
                )
                stock_df = fdf[fdf["stock_code"] == drill_stock]

                if not stock_df.empty:
                    strikes = sorted(stock_df["strike"].unique())
                    drill_strike = st.selectbox(
                        "Select Strike", strikes, key="dd_drill_strike"
                    )
                    row = stock_df[stock_df["strike"] == drill_strike]

                    if not row.empty:
                        r = row.iloc[0]

                        st.markdown(f"#### {drill_stock} — Strike {drill_strike}")
                        d1, d2, d3, d4 = st.columns(4)
                        d1.metric("Spot",                    _fmt_inr(r.get("spot"), 2))
                        d2.metric("Avg Cost",                _fmt_inr(r.get("avg_cost"), 2))
                        d3.metric("OTM %",                   _fmt_pct(r.get("otm_pct")))
                        d4.metric("DTE",                     r.get("dte", "—"))

                        d5, d6, d7, d8 = st.columns(4)
                        d5.metric("Gross Premium/Lot",       _fmt_inr(r.get("gross_premium_per_lot")))
                        d6.metric("Net Premium/Lot",         _fmt_inr(r.get("net_premium_per_lot")))
                        d7.metric("Transaction Cost/Lot",    _fmt_inr(r.get("total_txn_cost_per_lot")))
                        d8.metric("Net All Lots",            _fmt_inr(r.get("net_premium_all_lots")))

                        d9, d10, d11, d12 = st.columns(4)
                        d9.metric("Gross Ann. Yield",        _fmt_pct(r.get("gross_annualised_yield_pct")))
                        d10.metric("Net Ann. Yield",         _fmt_pct(r.get("net_annualised_yield_pct")))
                        d11.metric("Return If Expires",      _fmt_pct(r.get("return_if_expires_pct")))
                        d12.metric("Return If Assigned",     _fmt_pct(r.get("total_return_if_assigned_pct")))

                        d13, d14, d15, d16 = st.columns(4)
                        d13.metric("Breakeven",              _fmt_inr(r.get("breakeven"), 2))
                        d14.metric("Downside Cushion",       _fmt_pct(r.get("downside_cushion_pct")))
                        d15.metric("HV30",                   _fmt_pct(r.get("hv30")))
                        d16.metric("HV30 Cushion Ratio",
                                   f"{r.get('hv30_cushion_ratio'):.2f}" if r.get("hv30_cushion_ratio") is not None else "—")

                        d17, d18, d19, d20 = st.columns(4)
                        d17.metric("Composite Score",        f"{r.get('composite_score', '—')}")
                        d18.metric("Grade",                  r.get("composite_score_label", "—"))
                        d19.metric("Liquidity Score",        f"{r.get('liquidity_score', '—'):.2f}" if r.get("liquidity_score") is not None else "—")
                        d20.metric("Illiquid?",              "Yes" if r.get("illiquid_flag") else "No")

                    st.markdown("##### All Strikes for this Stock")
                    st.dataframe(
                        stock_df.sort_values("strike")[
                            ["strike", "otm_pct", "gross_premium_per_lot", "net_premium_per_lot",
                             "gross_annualised_yield_pct", "net_annualised_yield_pct",
                             "breakeven", "downside_cushion_pct", "liquidity_score",
                             "hv30_cushion_ratio", "composite_score", "illiquid_flag"]
                        ],
                        hide_index=True, use_container_width=True,
                    )
