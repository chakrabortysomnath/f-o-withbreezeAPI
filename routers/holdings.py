import logging

from fastapi import APIRouter, Header, Request
from auth import require_auth
from breeze_client import get_breeze
from utils import cors_preflight_response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.options("/holdings")
def options_holdings(request: Request):
    return cors_preflight_response(request)


@router.get("/holdings")
def get_holdings(x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    """
    Return all portfolio (demat) holdings for the authenticated Breeze account.

    Each row in the response represents one position and includes the stock
    symbol, exchange, quantity held, average cost, book value, current market
    price, market value, and P&L.
    """
    require_auth(x_app_token)
    breeze = get_breeze()

    resp = breeze.get_portfolio_holdings()
    logger.info("HOLDINGS  raw_status=%s", resp.get("Status"))

    rows = resp.get("Success") or []
    if not rows:
        return {"status": "error", "error": resp}

    holdings = []
    for r in rows:
        holdings.append({
            "stock_code": r.get("stock_code"),
            "exchange_code": r.get("exchange_code"),
            "quantity": r.get("quantity"),
            "average_cost": r.get("average_cost"),
            "book_value": r.get("book_value"),
            "ltp": r.get("ltp") or r.get("current_market_price"),
            "market_value": r.get("market_value"),
            "pnl": r.get("pnl") or r.get("profit_loss"),
            "pnl_percent": r.get("pnl_percentage") or r.get("profit_loss_percentage"),
            "product_type": r.get("product_type"),
            "portfolio_name": r.get("portfolio_name"),
            "raw": r,
        })

    return {
        "status": "ok",
        "count": len(holdings),
        "holdings": holdings,
    }
