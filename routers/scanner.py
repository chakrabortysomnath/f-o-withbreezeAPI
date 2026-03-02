from __future__ import annotations

import logging
from datetime import datetime
from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from auth import require_auth
from breeze_client import get_breeze, fetch_option_chain_rows
from utils import cors_preflight_response, safe_float

router = APIRouter()
logger = logging.getLogger(__name__)

# Nifty 50 universe — lot sizes are revised quarterly by NSE; update as needed
NIFTY50 = [
    {"stock_code": "ADANIENT",   "lot_size": 25},
    {"stock_code": "ADANIPORTS", "lot_size": 200},
    {"stock_code": "APOLLOHOSP", "lot_size": 25},
    {"stock_code": "ASIANPAINT", "lot_size": 200},
    {"stock_code": "AXISBANK",   "lot_size": 625},
    {"stock_code": "BAJAJ-AUTO", "lot_size": 75},
    {"stock_code": "BAJAJFINSV", "lot_size": 125},
    {"stock_code": "BAJFINANCE", "lot_size": 125},
    {"stock_code": "BHARTIARTL", "lot_size": 500},
    {"stock_code": "BPCL",       "lot_size": 1800},
    {"stock_code": "CIPLA",      "lot_size": 650},
    {"stock_code": "COALINDIA",  "lot_size": 1400},
    {"stock_code": "DRREDDY",    "lot_size": 50},
    {"stock_code": "EICHERMOT",  "lot_size": 25},
    {"stock_code": "ETERNAL",    "lot_size": 2000},
    {"stock_code": "GRASIM",     "lot_size": 175},
    {"stock_code": "HCLTECH",    "lot_size": 350},
    {"stock_code": "HDFCBANK",   "lot_size": 550},
    {"stock_code": "HDFCLIFE",   "lot_size": 1000},
    {"stock_code": "HEROMOTOCO", "lot_size": 100},
    {"stock_code": "HINDALCO",   "lot_size": 700},
    {"stock_code": "HINDUNILVR", "lot_size": 300},
    {"stock_code": "ICICIBANK",  "lot_size": 700},
    {"stock_code": "INDUSINDBK", "lot_size": 400},
    {"stock_code": "INFY",       "lot_size": 300},
    {"stock_code": "ITC",        "lot_size": 1600},
    {"stock_code": "JSWSTEEL",   "lot_size": 600},
    {"stock_code": "KOTAKBANK",  "lot_size": 400},
    {"stock_code": "LT",         "lot_size": 150},
    {"stock_code": "LTIM",       "lot_size": 75},
    {"stock_code": "M&M",        "lot_size": 175},
    {"stock_code": "MARUTI",     "lot_size": 25},
    {"stock_code": "NESTLEIND",  "lot_size": 50},
    {"stock_code": "NTPC",       "lot_size": 2250},
    {"stock_code": "ONGC",       "lot_size": 1925},
    {"stock_code": "POWERGRID",  "lot_size": 2700},
    {"stock_code": "RELIANCE",   "lot_size": 250},
    {"stock_code": "SBILIFE",    "lot_size": 375},
    {"stock_code": "SBIN",       "lot_size": 1500},
    {"stock_code": "SHRIRAMFIN", "lot_size": 150},
    {"stock_code": "SUNPHARMA",  "lot_size": 350},
    {"stock_code": "TATACONSUM", "lot_size": 425},
    {"stock_code": "TATAMOTORS", "lot_size": 550},
    {"stock_code": "TATASTEEL",  "lot_size": 5500},
    {"stock_code": "TCS",        "lot_size": 150},
    {"stock_code": "TECHM",      "lot_size": 300},
    {"stock_code": "TITAN",      "lot_size": 175},
    {"stock_code": "TRENT",      "lot_size": 125},
    {"stock_code": "ULTRACEMCO", "lot_size": 50},
    {"stock_code": "WIPRO",      "lot_size": 1500},
]


class HoldingInput(BaseModel):
    stock_code: str
    quantity: float
    average_cost: float | None = None
    ltp: float | None = None


class ScanRequest(BaseModel):
    expiry_date: str                     # e.g. "27-Mar-2026"
    holdings: list[HoldingInput] = []
    otm_max_pct: float = 10.0           # only strikes up to this % OTM
    min_premium_yield_pct: float = 0.3  # min premium / avg_cost % (skips thin premiums)


def _dte(expiry_date: str) -> int | None:
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            exp = datetime.strptime(expiry_date, fmt)
            return max(1, (exp - datetime.now()).days + 1)
        except ValueError:
            continue
    return None


@router.options("/scan/covered-calls")
def options_scan(request: Request):
    return cors_preflight_response(request)


@router.post("/scan/covered-calls")
def scan_covered_calls(
    req: ScanRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
):
    """
    Scan all Nifty 50 stocks for covered call opportunities.

    For each stock the response includes:
    - Lot status (lots held, shares needed to complete the next lot)
    - Accumulation advice for partial / no positions
    - Ranked call option opportunities for stocks with >= 1 full lot
      (option chain is only fetched for stocks where calls can be written)
    """
    require_auth(x_app_token)
    breeze = get_breeze()
    dte = _dte(req.expiry_date)

    holdings_map: dict[str, HoldingInput] = {
        h.stock_code.upper(): h for h in req.holdings
    }

    results = []

    for sym in NIFTY50:
        stock_code = sym["stock_code"]
        lot_size = sym["lot_size"]

        holding = holdings_map.get(stock_code)
        held_qty = holding.quantity if holding else 0.0
        avg_cost = holding.average_cost if holding else None
        spot = holding.ltp if holding else None

        lots_held = int(held_qty // lot_size)
        remainder = held_qty % lot_size
        shares_short = int(lot_size - remainder) if remainder > 0 else (lot_size if lots_held == 0 else 0)
        can_write = lots_held >= 1

        entry: dict = {
            "stock_code": stock_code,
            "lot_size": lot_size,
            "held_qty": held_qty,
            "lots_held": lots_held,
            "shares_short": shares_short,
            "avg_cost": avg_cost,
            "spot": spot,
            "can_write_calls": can_write,
            "accumulation_advice": None,
            "opportunities": [],
        }

        if shares_short > 0:
            cost_est = round(shares_short * spot, 0) if spot else None
            entry["accumulation_advice"] = {
                "shares_needed": shares_short,
                "approx_cost": cost_est,
                "message": (
                    f"Need {shares_short} more share(s) to complete 1 lot"
                    + (f" — approx ₹{cost_est:,.0f}" if cost_est else "")
                ),
            }

        if not can_write:
            results.append(entry)
            continue

        # ── Fetch option chain ─────────────────────────────────────────────
        rows, _, _ = fetch_option_chain_rows(
            breeze=breeze,
            stock_code=stock_code,
            exchange_code="NFO",
            expiry_date=req.expiry_date,
            right="call",
        )
        logger.info("SCAN  %s  chain_rows=%d", stock_code, len(rows))

        # Derive spot from chain if not from holdings
        if not spot:
            for r in rows:
                s = safe_float(r.get("spot_price"))
                if s:
                    spot = s
                    break
            entry["spot"] = spot

        for r in rows:
            strike = safe_float(r.get("strike_price"))
            bid = safe_float(r.get("best_bid_price"))
            ask = safe_float(r.get("best_offer_price"))
            ltp_opt = safe_float(r.get("ltp"))
            bid_qty = r.get("best_bid_quantity")
            ask_qty = r.get("best_offer_quantity")

            if not (strike and bid and ask and ltp_opt):
                continue
            if not (bid_qty and ask_qty):
                continue  # illiquid

            premium = bid  # conservative: what you actually receive selling at bid

            otm_pct = ((strike - spot) / spot * 100) if spot else None
            if otm_pct is not None and (otm_pct < -2.0 or otm_pct > req.otm_max_pct):
                continue  # skip deep ITM and excessively OTM strikes

            premium_yield = (premium / avg_cost * 100) if avg_cost else None
            if premium_yield is not None and premium_yield < req.min_premium_yield_pct:
                continue

            ann_yield = (premium_yield * 365 / dte) if (premium_yield and dte) else None
            breakeven = (avg_cost - premium) if avg_cost else None
            max_profit_pct = (
                (strike - avg_cost + premium) / avg_cost * 100
            ) if avg_cost else None

            entry["opportunities"].append({
                "strike": strike,
                "premium_bid": bid,
                "premium_ask": ask,
                "premium_ltp": ltp_opt,
                "otm_pct": round(otm_pct, 2) if otm_pct is not None else None,
                "premium_yield_pct": round(premium_yield, 3) if premium_yield is not None else None,
                "annualised_yield_pct": round(ann_yield, 2) if ann_yield is not None else None,
                "dte": dte,
                "breakeven": round(breakeven, 2) if breakeven is not None else None,
                "max_profit_pct": round(max_profit_pct, 2) if max_profit_pct is not None else None,
                "premium_cash_per_lot": round(premium * lot_size, 2),
                "total_premium_cash": round(premium * lot_size * lots_held, 2),
            })

        entry["opportunities"].sort(
            key=lambda x: x.get("annualised_yield_pct") or 0,
            reverse=True,
        )
        results.append(entry)

    results.sort(
        key=lambda x: (
            0 if x["can_write_calls"] else (1 if x["held_qty"] > 0 else 2),
            -(x["opportunities"][0]["annualised_yield_pct"] or 0) if x["opportunities"] else 0,
        )
    )

    return {
        "status": "ok",
        "expiry_date": req.expiry_date,
        "total_symbols": len(results),
        "can_write": sum(1 for r in results if r["can_write_calls"]),
        "partial_position": sum(1 for r in results if not r["can_write_calls"] and r["held_qty"] > 0),
        "no_position": sum(1 for r in results if r["held_qty"] == 0),
        "results": results,
    }
