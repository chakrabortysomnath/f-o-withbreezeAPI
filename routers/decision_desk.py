from __future__ import annotations

import logging
import time
from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from auth import require_auth
from breeze_client import get_breeze, fetch_option_chain_rows
from utils import cors_preflight_response, safe_float
from routers.scanner import NIFTY50, HoldingInput, _dte

router = APIRouter()
logger = logging.getLogger(__name__)


class DDScanRequest(BaseModel):
    expiry_date: str
    holdings: list[HoldingInput] = []
    fetch_all: bool = True


@router.options("/scan/decision-desk")
def options_dd(request: Request):
    return cors_preflight_response(request)


@router.post("/scan/decision-desk")
def scan_decision_desk(
    req: DDScanRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
):
    """
    Fetch raw option chain data for all 50 Nifty stocks.

    Unlike /scan/covered-calls, this endpoint:
    - Fetches chains for ALL 50 stocks regardless of holdings
    - Returns raw, unfiltered chain rows (no metrics, no thresholds)
    - Continues on per-symbol errors (chain_error field)

    All derived metrics are computed in the Streamlit layer.
    """
    require_auth(x_app_token)
    breeze = get_breeze()
    dte = _dte(req.expiry_date)

    holdings_map: dict[str, HoldingInput] = {
        h.stock_code.upper(): h for h in req.holdings
    }

    results = []
    symbols_with_data = 0

    for sym in NIFTY50:
        stock_code = sym["stock_code"]
        lot_size = sym["lot_size"]

        holding = holdings_map.get(stock_code)
        held_qty = holding.quantity if holding else 0.0
        avg_cost = holding.average_cost if holding else None
        spot_from_holding = holding.ltp if holding else None

        lots_held = int(held_qty // lot_size)
        remainder = held_qty % lot_size
        shares_short = (
            int(lot_size - remainder) if remainder > 0
            else (lot_size if lots_held == 0 else 0)
        )

        entry: dict = {
            "stock_code": stock_code,
            "lot_size": lot_size,
            "held_qty": held_qty,
            "lots_held": lots_held,
            "shares_short": shares_short,
            "avg_cost": avg_cost,
            "spot": spot_from_holding,
            "is_held": held_qty > 0,
            "is_recovery": False,
            "chain_rows": [],
            "chain_error": None,
        }

        t0 = time.perf_counter()
        try:
            rows, _, _ = fetch_option_chain_rows(
                breeze=breeze,
                stock_code=stock_code,
                exchange_code="NFO",
                expiry_date=req.expiry_date,
                right="call",
            )

            # Derive spot from chain if not from holdings
            spot = spot_from_holding
            if not spot:
                for r in rows:
                    s = safe_float(r.get("spot_price"))
                    if s:
                        spot = s
                        break
                entry["spot"] = spot

            entry["is_recovery"] = (
                avg_cost is not None and spot is not None and spot < avg_cost
            )

            if rows:
                symbols_with_data += 1
                entry["chain_rows"] = rows
            else:
                entry["chain_error"] = "No chain data returned"

        except Exception as exc:
            entry["chain_error"] = str(exc)
            logger.error("DD  %s  error: %s", stock_code, exc)

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            "DD  %s  rows=%d  error=%s  ms=%d",
            stock_code,
            len(entry["chain_rows"]),
            entry["chain_error"] or "none",
            elapsed_ms,
        )
        results.append(entry)

    return {
        "status": "ok",
        "expiry_date": req.expiry_date,
        "dte": dte,
        "total_symbols": len(results),
        "symbols_with_chain_data": symbols_with_data,
        "results": results,
    }
