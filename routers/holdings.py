import logging

from fastapi import APIRouter, Header, Query, Request
from auth import require_auth
from breeze_client import get_breeze
from utils import cors_preflight_response

router = APIRouter()
logger = logging.getLogger(__name__)

SUPPORTED_EXCHANGES = ["NSE", "BSE", "NFO", "BFO", "MCX", "NCDEX"]


@router.options("/holdings")
def options_holdings(request: Request):
    return cors_preflight_response(request)


@router.get("/holdings")
def get_holdings(
    exchange_code: list[str] = Query(default=["NSE", "BSE"]),
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
):
    """
    Return portfolio (demat) holdings for one or more exchanges.

    Pass ?exchange_code=NSE&exchange_code=BSE (repeatable) to select which
    exchanges to query.  The handler calls Breeze once per exchange, then
    aggregates the results before returning.

    Each row includes: stock_code, exchange_code, quantity, average_cost,
    book_value, ltp, market_value, pnl, pnl_percent, product_type,
    portfolio_name, raw.
    """
    require_auth(x_app_token)
    breeze = get_breeze()

    invalid = [e for e in exchange_code if e not in SUPPORTED_EXCHANGES]
    if invalid:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported exchange code(s): {invalid}. Must be one of {SUPPORTED_EXCHANGES}",
        )

    holdings = []
    errors = {}

       for exch in exchange_code:
        resp = breeze.get_portfolio_holdings(exchange_code=exch)
        status = resp.get("Status")
        logger.info("HOLDINGS  exchange=%s raw_status=%s", exch, status)

        rows = resp.get("Success") or []
        if not rows:
            errors[exch] = resp
            continue

        for r in rows:
            # Normalize types
            def to_float(x):
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return None

            qty = to_float(r.get("quantity"))
            ltp = to_float(r.get("ltp") or r.get("current_market_price"))
            avg_price = to_float(r.get("average_cost") or r.get("average_price"))

            # Derived fields
            book_value = qty * avg_price if qty is not None and avg_price is not None else None

            # Breeze sometimes gives open_position_value, otherwise fall back
            open_position_value = to_float(r.get("open_position_value"))
            market_value = (
                open_position_value
                if open_position_value is not None
                else (qty * ltp if qty is not None and ltp is not None else None)
            )

            realized = to_float(r.get("realized_profit"))
            unrealized = to_float(r.get("unrealized_profit"))
            pnl = None
            if realized is not None or unrealized is not None:
                pnl = (realized or 0.0) + (unrealized or 0.0)

            pnl_percent = None
            if pnl is not None and book_value not in (None, 0):
                pnl_percent = pnl / book_value * 100.0

            holdings.append({
                "stock_code":     r.get("stock_code"),
                "exchange_code":  r.get("exchange_code") or exch,
                "quantity":       qty,
                "average_cost":   avg_price,
                "book_value":     book_value,
                "ltp":            ltp,
                "market_value":   market_value,
                "pnl":            pnl,
                "pnl_percent":    pnl_percent,
                "product_type":   r.get("product_type"),
                # Breeze doesn't really have portfolio_name in this API; keep None or derive elsewhere
                "portfolio_name": r.get("portfolio_name"),
                "raw":            r,
            })

    result = {
        "status": "ok",
        "count": len(holdings),
        "exchanges_queried": exchange_code,
        "holdings": holdings,
    }
    if errors:
        result["exchange_errors"] = errors

    return result
