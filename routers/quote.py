from fastapi import APIRouter, Header, Request
from auth import require_auth
from breeze_client import get_breeze
from models import QuoteRequest
from utils import cors_preflight_response

router = APIRouter()


@router.options("/quote")
def options_quote(request: Request):
    return cors_preflight_response(request)


@router.post("/quote")
def quote(req: QuoteRequest, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    breeze = get_breeze()

    params = {
        "stock_code": req.stock_code.strip().upper(),
        "exchange_code": req.exchange_code.strip().upper(),
        "product_type": (req.product_type or "cash").strip().lower(),
    }

    if req.expiry_date:
        params["expiry_date"] = req.expiry_date
    if req.strike_price:
        params["strike_price"] = str(req.strike_price)
    if req.right:
        params["right"] = req.right

    resp = breeze.get_quotes(**params)

    rows = resp.get("Success") or []
    if not rows:
        return {
            "status": "error",
            "error": resp.get("Error") or resp,
            "attempted": {k: params[k] for k in ("stock_code", "exchange_code", "product_type")},
        }

    r = rows[0]

    quote_data = {
        "exchange": req.exchange_code,
        "symbol": req.stock_code,
        "ltp": r.get("ltp") or r.get("LTP") or r.get("last_traded_price"),
        "open": r.get("open") or r.get("OPEN"),
        "high": r.get("high") or r.get("HIGH"),
        "low": r.get("low") or r.get("LOW"),
        "prev_close": r.get("previous_close") or r.get("prev_close") or r.get("CLOSE"),
        "volume": r.get("volume") or r.get("VOLUME"),
        "ltt": r.get("ltt") or r.get("LTT") or r.get("last_traded_time"),
        "bid_price": r.get("best_bid_price"),
        "bid_qty": r.get("best_bid_quantity"),
        "ask_price": r.get("best_offer_price"),
        "ask_qty": r.get("best_offer_quantity"),
        "ltp_percent_change": r.get("ltp_percent_change"),
        "upper_circuit": r.get("upper_circuit"),
        "lower_circuit": r.get("lower_circuit"),
        "total_qty_traded": r.get("total_quantity_traded"),
        "spot_price": r.get("spot_price"),
        "expiry_date": r.get("expiry_date"),
        "strike_price": r.get("strike_price"),
        "right": r.get("right"),
    }

    return {
        "status": "ok",
        "quote": quote_data,
        "raw": r,
        "raw_keys": sorted(list(r.keys()))
    }
