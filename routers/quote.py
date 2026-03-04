import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Header, Request
from auth import require_auth
from breeze_client import get_breeze
from models import QuoteRequest
from utils import cors_preflight_response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.options("/quote")
def options_quote(request: Request):
    return cors_preflight_response(request)


def _historical_quote(breeze, stock_code: str, exchange_code: str, product_type: str,
                      expiry_date: str = "", right: str = "", strike_price: str = "") -> dict | None:
    """
    Fallback quote via get_historical_data_v2 (1-day candles, last 7 days).
    Returns a normalised quote_data dict, or None if unavailable.
    Uses a completely different Breeze endpoint (BREEZE_NEW_URL) that is
    reliable for NSE/BSE cash equities even when get_quotes() fails.
    """
    now = datetime.utcnow()
    from_date = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00.000Z")
    to_date   = now.strftime("%Y-%m-%dT23:59:59.000Z")

    kwargs = dict(
        interval="1day",
        from_date=from_date,
        to_date=to_date,
        stock_code=stock_code,
        exchange_code=exchange_code,
        product_type=product_type,
    )
    if expiry_date:
        kwargs["expiry_date"] = expiry_date
    if right and right != "others":
        kwargs["right"] = right
    if strike_price:
        kwargs["strike_price"] = strike_price

    try:
        resp = breeze.get_historical_data_v2(**kwargs)
        rows = resp.get("Success") or []
        if not rows:
            logger.warning("HIST_FALLBACK  no data  stock=%s  resp=%s", stock_code, resp)
            return None

        last = rows[-1]
        prev = rows[-2] if len(rows) >= 2 else None

        def _f(v):
            try:
                return float(v) if v not in (None, "") else None
            except (TypeError, ValueError):
                return None

        return {
            "exchange":            exchange_code,
            "symbol":              stock_code,
            "ltp":                 _f(last.get("close")),
            "open":                _f(last.get("open")),
            "high":                _f(last.get("high")),
            "low":                 _f(last.get("low")),
            "prev_close":          _f(prev.get("close")) if prev else None,
            "volume":              _f(last.get("volume")),
            "ltt":                 last.get("datetime"),
            "bid_price":           None,
            "bid_qty":             None,
            "ask_price":           None,
            "ask_qty":             None,
            "ltp_percent_change":  None,
            "upper_circuit":       None,
            "lower_circuit":       None,
            "total_qty_traded":    _f(last.get("volume")),
            "spot_price":          None,
            "expiry_date":         last.get("expiry_date"),
            "strike_price":        last.get("strike_price"),
            "right":               last.get("right"),
            "_source":             "historical_data_v2",
        }
    except Exception as exc:
        logger.error("HIST_FALLBACK  error  stock=%s  err=%s", stock_code, exc)
        return None


@router.post("/quote")
def quote(req: QuoteRequest, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    breeze = get_breeze()

    product_type = (req.product_type or "cash").strip().lower()

    # Breeze get_quotes() requires ALL parameters to be passed explicitly.
    # For cash equities, right must be "others" (not blank, not omitted).
    if product_type == "cash":
        right = "others"
    else:
        right = req.right or ""

    params = {
        "stock_code":   req.stock_code.strip().upper(),
        "exchange_code": req.exchange_code.strip().upper(),
        "product_type": product_type,
        "expiry_date":  req.expiry_date or "",
        "strike_price": str(req.strike_price) if req.strike_price else "",
        "right":        right,
    }

    resp = breeze.get_quotes(**params)
    rows = resp.get("Success") or []

    logger.info("QUOTE  get_quotes  stock=%s  exch=%s  pt=%s  rows=%d  err=%s",
                params["stock_code"], params["exchange_code"], params["product_type"],
                len(rows), resp.get("Error") or "none")

    if not rows:
        # get_quotes() is unreliable for NSE/BSE cash; fall back to historical data
        logger.info("QUOTE  falling back to get_historical_data_v2  stock=%s", params["stock_code"])
        quote_data = _historical_quote(
            breeze,
            stock_code=params["stock_code"],
            exchange_code=params["exchange_code"],
            product_type=product_type,
            expiry_date=params["expiry_date"],
            right=params["right"],
            strike_price=params["strike_price"],
        )
        if quote_data:
            return {"status": "ok", "quote": quote_data, "raw": {}, "raw_keys": []}

        return {
            "status": "error",
            "error": resp.get("Error") or resp,
            "attempted": {k: params[k] for k in ("stock_code", "exchange_code", "product_type", "right", "expiry_date")},
        }

    r = rows[0]

    quote_data = {
        "exchange":           req.exchange_code,
        "symbol":             req.stock_code,
        "ltp":                r.get("ltp") or r.get("LTP") or r.get("last_traded_price"),
        "open":               r.get("open") or r.get("OPEN"),
        "high":               r.get("high") or r.get("HIGH"),
        "low":                r.get("low") or r.get("LOW"),
        "prev_close":         r.get("previous_close") or r.get("prev_close") or r.get("CLOSE"),
        "volume":             r.get("volume") or r.get("VOLUME"),
        "ltt":                r.get("ltt") or r.get("LTT") or r.get("last_traded_time"),
        "bid_price":          r.get("best_bid_price"),
        "bid_qty":            r.get("best_bid_quantity"),
        "ask_price":          r.get("best_offer_price"),
        "ask_qty":            r.get("best_offer_quantity"),
        "ltp_percent_change": r.get("ltp_percent_change"),
        "upper_circuit":      r.get("upper_circuit"),
        "lower_circuit":      r.get("lower_circuit"),
        "total_qty_traded":   r.get("total_quantity_traded"),
        "spot_price":         r.get("spot_price"),
        "expiry_date":        r.get("expiry_date"),
        "strike_price":       r.get("strike_price"),
        "right":              r.get("right"),
    }

    return {
        "status": "ok",
        "quote": quote_data,
        "raw": r,
        "raw_keys": sorted(list(r.keys())),
    }
