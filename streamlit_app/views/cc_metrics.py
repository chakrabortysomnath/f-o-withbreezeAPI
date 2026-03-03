"""
cc_metrics.py — Decision Desk metrics engine.

Pure Python / pandas — NO Streamlit imports.
Transforms raw /scan/decision-desk results into a flat DataFrame
with ~44 derived columns per (stock, strike) opportunity row.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd


# ── Safe conversions ──────────────────────────────────────────────────────────

def _f(v: Any) -> float | None:
    """Convert a value to float, returning None on failure."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    """Convert a value to int, returning None on failure."""
    f = _f(v)
    return int(f) if f is not None else None


# ── Composite score helper ────────────────────────────────────────────────────

def _minmax(series: pd.Series, invert: bool = False) -> pd.Series:
    """Min-max normalise a series to [0, 1]. Returns 0.5 if all values are equal."""
    mn, mx = series.min(), series.max()
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series(0.5, index=series.index)
    normed = (series - mn) / (mx - mn)
    return 1.0 - normed if invert else normed


def add_composite_score(df: pd.DataFrame, weights: dict) -> pd.DataFrame:
    """
    Add composite_score (0-100), composite_rank (int), and composite_score_label
    columns to the DataFrame.  Operates on the current filtered set so rankings
    are relative to what's visible.

    weights keys: annualised_yield_pct, downside_cushion_pct, net_premium_per_lot,
                  otm_pct, liquidity_score, hv30_cushion_ratio
    """
    df = df.copy()

    def _col_or_zero(col: str) -> pd.Series:
        if col in df.columns:
            return df[col].fillna(0.0)
        return pd.Series(0.0, index=df.index)

    # Higher is better for these
    norm_yield    = _minmax(_col_or_zero("net_annualised_yield_pct"))
    norm_cushion  = _minmax(_col_or_zero("downside_cushion_pct"))
    norm_premium  = _minmax(_col_or_zero("net_premium_per_lot"))
    norm_otm      = _minmax(_col_or_zero("otm_pct"))       # higher OTM = safer
    norm_liq      = _minmax(_col_or_zero("liquidity_score"))
    norm_hv_ratio = _minmax(_col_or_zero("hv30_cushion_ratio"))

    w = weights
    score = (
        w.get("annualised_yield_pct", 0.25)  * norm_yield   +
        w.get("downside_cushion_pct", 0.20)  * norm_cushion +
        w.get("net_premium_per_lot",  0.20)  * norm_premium +
        w.get("otm_pct",              0.15)  * norm_otm     +
        w.get("liquidity_score",      0.10)  * norm_liq     +
        w.get("hv30_cushion_ratio",   0.10)  * norm_hv_ratio
    ) * 100.0

    df["composite_score"] = score.round(2)
    df["composite_rank"]  = df["composite_score"].rank(ascending=False, method="min").astype(int)
    df["composite_score_label"] = df["composite_score"].apply(
        lambda s: "A" if s > 75 else ("B" if s > 50 else ("C" if s > 25 else "D"))
    )
    return df


# ── Main metrics engine ───────────────────────────────────────────────────────

def compute_opportunity_metrics(
    raw_results: list[dict],
    dd_config: dict,
    hv30_map: dict[str, float | None],
    dte: int,
) -> pd.DataFrame:
    """
    Flatten raw /scan/decision-desk results into one row per (stock, strike).

    Parameters
    ----------
    raw_results : list[dict]
        The ``results`` list from the backend response.
    dd_config : dict
        The ``decision_desk`` block from config.json.
    hv30_map : dict[str, float | None]
        {stock_code: hv30_pct} pre-fetched from yfinance.
    dte : int
        Days to expiry (used for annualised yield and expected-move calculations).

    Returns
    -------
    pd.DataFrame with ~44 columns, one row per opportunity.
    Returns an empty DataFrame if no opportunities are found.
    """
    if not raw_results or dte is None or dte <= 0:
        return pd.DataFrame()

    TC  = dd_config.get("transaction_costs", {})
    LIQ = dd_config.get("liquidity", {})
    atm_band = dd_config.get("atm_band_pct", 2.0)

    rows: list[dict] = []

    # Build sector map from config symbols (passed in dd_config or fall back to empty)
    sector_map: dict[str, str] = dd_config.get("_sector_map", {})

    for stock in raw_results:
        stock_code  = stock.get("stock_code", "")
        lot_size    = _i(stock.get("lot_size")) or 1
        held_qty    = _f(stock.get("held_qty")) or 0.0
        lots_held   = _i(stock.get("lots_held")) or 0
        avg_cost    = _f(stock.get("avg_cost"))
        spot_stock  = _f(stock.get("spot"))
        is_held     = bool(stock.get("is_held", held_qty > 0))
        sector      = sector_map.get(stock_code, stock.get("sector", "—"))
        hv30        = hv30_map.get(stock_code)

        for chain_row in stock.get("chain_rows", []):
            bid       = _f(chain_row.get("best_bid_price"))
            ask       = _f(chain_row.get("best_offer_price"))
            ltp_opt   = _f(chain_row.get("ltp"))
            strike    = _f(chain_row.get("strike_price"))
            bid_qty   = _i(chain_row.get("best_bid_quantity")) or 0
            ask_qty   = _i(chain_row.get("best_offer_quantity")) or 0
            volume    = _i(chain_row.get("total_quantity_traded")) or 0
            spot_row  = _f(chain_row.get("spot_price"))

            # Use stock-level spot if chain row doesn't have one
            spot = spot_row or spot_stock
            if not spot:
                continue

            # Skip rows with no actionable premium
            if bid is None or bid <= 0 or ask is None or strike is None:
                continue

            P = bid  # conservative: sell at bid

            # ── Option basics ──────────────────────────────────────────────
            otm_pct    = round((strike - spot) / spot * 100, 3) if spot else None
            spread_inr = round(ask - bid, 4)
            spread_pct = round((ask - bid) / ltp_opt * 100, 3) if (ltp_opt and ltp_opt > 0) else None

            # ── Transaction costs ──────────────────────────────────────────
            brok = P * lot_size * TC.get("brokerage_pct_of_premium", 0.03) / 100
            stt  = P * lot_size * TC.get("stt_pct_of_premium", 0.05) / 100
            othr = P * lot_size * (
                TC.get("exchange_txn_pct", 0.00053) +
                TC.get("sebi_pct", 0.000001) +
                TC.get("stamp_duty_pct", 0.003)
            ) / 100
            gst  = brok * TC.get("gst_pct_of_brokerage", 18.0) / 100
            total_txn = brok + stt + othr + gst

            # ── Net premiums ───────────────────────────────────────────────
            gross_per_lot = round(P * lot_size, 2)
            net_per_lot   = round(gross_per_lot - total_txn, 2)
            net_all_lots  = round(net_per_lot * lots_held, 2) if lots_held > 0 else None

            # ── Yields (cost_basis = avg_cost if held, else spot) ──────────
            cost_basis = avg_cost if avg_cost else spot
            gross_yield    = round(gross_per_lot / (cost_basis * lot_size) * 100, 4) if cost_basis else None
            net_yield      = round(net_per_lot   / (cost_basis * lot_size) * 100, 4) if cost_basis else None
            gross_ann      = round(gross_yield * 365 / dte, 3) if gross_yield is not None else None
            net_ann        = round(net_yield   * 365 / dte, 3) if net_yield   is not None else None

            # ── Scenario outcomes (only when avg_cost known) ───────────────
            breakeven = assigned_profit = total_return_if_assigned = None
            return_if_expires = net_yield   # same as net yield for "expires worthless"
            breakeven_drop_pct = None
            if avg_cost:
                be         = avg_cost - net_per_lot / lot_size
                breakeven  = round(be, 2)
                breakeven_drop_pct = round((spot - be) / spot * 100, 3) if spot else None
                total_ret  = (strike - avg_cost + net_per_lot / lot_size) / avg_cost * 100
                total_return_if_assigned = round(total_ret, 3)
                if lots_held > 0:
                    assigned_profit = round(
                        (strike - avg_cost + net_per_lot / lot_size) * lot_size * lots_held, 2
                    )

            # ── Downside cushion & volatility ──────────────────────────────
            downside_cushion_pct = breakeven_drop_pct  # alias

            replacement_value   = round(spot * lot_size, 2)
            prem_pct_replacement = round(gross_per_lot / replacement_value * 100, 3) if replacement_value else None

            expected_move_pct = None
            expected_move_inr = None
            hv30_cushion_ratio = None
            thin_cushion_flag  = False
            if hv30 and dte:
                em_pct = hv30 / math.sqrt(365.0 / dte)
                expected_move_pct = round(em_pct, 3)
                expected_move_inr = round(spot * em_pct / 100, 2) if spot else None
                if downside_cushion_pct is not None and em_pct > 0:
                    hv30_cushion_ratio = round(downside_cushion_pct / em_pct, 4)
                    thin_cushion_flag  = hv30_cushion_ratio < 1.0

            # ── Capital & liquidity ────────────────────────────────────────
            buildup_cost = 0.0 if is_held else round(spot * lot_size, 2)
            liq_score    = round(min(bid_qty, ask_qty) / lot_size, 4) if lot_size else 0.0
            illiquid     = (
                bid_qty < LIQ.get("min_bid_qty", 1) or
                ask_qty < LIQ.get("min_ask_qty", 1) or
                volume  < LIQ.get("min_volume", 100) or
                (spread_pct is not None and spread_pct > LIQ.get("max_spread_pct_of_ltp", 10.0))
            )

            # ── Position flags ─────────────────────────────────────────────
            is_recovery      = is_held and avg_cost is not None and spot < avg_cost
            recovery_gap_pct = round((avg_cost - spot) / avg_cost * 100, 3) if is_recovery else None
            is_atm           = abs(otm_pct) <= atm_band if otm_pct is not None else False
            assigned_below   = bool(is_held and avg_cost and strike < avg_cost)

            rows.append({
                # Identifiers
                "stock_code":                   stock_code,
                "sector":                       sector,
                "lot_size":                     lot_size,
                "held_qty":                     held_qty,
                "lots_held":                    lots_held,
                "avg_cost":                     avg_cost,
                "spot":                         spot,
                "strike":                       strike,
                "dte":                          dte,
                "hv30":                         hv30,
                # Option pricing basics
                "premium_bid":                  P,
                "premium_ask":                  ask,
                "premium_ltp":                  ltp_opt,
                "spread_inr":                   spread_inr,
                "spread_pct":                   spread_pct,
                "otm_pct":                      otm_pct,
                "bid_qty":                      bid_qty,
                "ask_qty":                      ask_qty,
                "volume":                       volume,
                # Transaction costs
                "brokerage_per_lot":            round(brok, 2),
                "stt_per_lot":                  round(stt, 2),
                "other_txn_per_lot":            round(othr, 2),
                "gst_per_lot":                  round(gst, 2),
                "total_txn_cost_per_lot":       round(total_txn, 2),
                # Net premiums
                "gross_premium_per_lot":        gross_per_lot,
                "net_premium_per_lot":          net_per_lot,
                "net_premium_all_lots":         net_all_lots,
                # Yields
                "gross_premium_yield_pct":      gross_yield,
                "net_premium_yield_pct":        net_yield,
                "gross_annualised_yield_pct":   gross_ann,
                "net_annualised_yield_pct":     net_ann,
                # Scenarios
                "return_if_expires_pct":        return_if_expires,
                "breakeven":                    breakeven,
                "breakeven_drop_pct":           breakeven_drop_pct,
                "total_return_if_assigned_pct": total_return_if_assigned,
                "assigned_profit_inr":          assigned_profit,
                # Downside & volatility
                "downside_cushion_pct":         downside_cushion_pct,
                "replacement_value_per_lot":    replacement_value,
                "premium_pct_of_replacement":   prem_pct_replacement,
                "expected_move_pct_dte":        expected_move_pct,
                "expected_move_inr":            expected_move_inr,
                "hv30_cushion_ratio":           hv30_cushion_ratio,
                "thin_cushion_flag":            thin_cushion_flag,
                # Capital & liquidity
                "buildup_cost_per_lot":         buildup_cost,
                "liquidity_score":              liq_score,
                "illiquid_flag":                illiquid,
                # Position flags
                "is_held":                      is_held,
                "is_recovery":                  is_recovery,
                "recovery_gap_pct":             recovery_gap_pct,
                "is_atm":                       is_atm,
                "assigned_below_cost":          assigned_below,
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)
