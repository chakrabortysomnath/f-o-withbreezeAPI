from fastapi import APIRouter, Header, HTTPException, Request
from auth import require_auth
from breeze_client import get_breeze, fetch_option_chain_rows
from models import StrikeListRequest, ChainCompareRequest
from utils import cors_preflight_response, require_right, safe_float

router = APIRouter()


@router.options("/option_strikes")
def options_option_strikes(request: Request):
    return cors_preflight_response(request)


@router.post("/option_strikes")
def option_strikes(
    req: StrikeListRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")
):
    require_auth(x_app_token)
    breeze = get_breeze()

    right = require_right(req.right)

    rows, attempted, last_resp = fetch_option_chain_rows(
        breeze=breeze,
        stock_code=req.stock_code.strip().upper(),
        exchange_code=req.exchange_code.strip().upper(),
        expiry_date=req.expiry_date,
        right=right
    )

    if not rows:
        return {
            "status": "error",
            "error": last_resp,
            "attempted_right_values": attempted
        }

    strikes = sorted({
        float(r.get("strike_price"))
        for r in rows
        if r.get("strike_price") is not None and str(r.get("strike_price")).strip() != ""
    })

    spot = None
    for r in rows:
        s = r.get("spot_price")
        if s is not None and str(s).strip() != "":
            try:
                spot = float(s)
                break
            except Exception:
                pass

    return {
        "status": "ok",
        "exchange": req.exchange_code.upper(),
        "symbol": req.stock_code.upper(),
        "expiry_date": req.expiry_date,
        "right": attempted[-1],
        "spot_price": spot,
        "count": len(strikes),
        "strikes": strikes
    }


@router.options("/option_chain_compare")
def options_option_chain_compare(request: Request):
    return cors_preflight_response(request)


@router.post("/option_chain_compare")
def option_chain_compare(
    req: ChainCompareRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")
):
    require_auth(x_app_token)
    breeze = get_breeze()

    exchange_code = req.exchange_code.strip().upper()
    stock_code = req.stock_code.strip().upper()
    right = require_right(req.right)
    expiry_date = str(req.expiry_date or "").strip()

    if exchange_code not in ("NFO", "BFO"):
        raise HTTPException(status_code=400, detail="exchange_code must be NFO or BFO for option chain")

    if not expiry_date:
        raise HTTPException(status_code=400, detail="expiry_date is required")

    rows, attempted, raw_resp = fetch_option_chain_rows(
        breeze=breeze,
        stock_code=stock_code,
        exchange_code=exchange_code,
        expiry_date=expiry_date,
        right=right
    )

    if not rows:
        return {
            "status": "error",
            "error": "No option chain rows returned",
            "attempted_right_values": attempted,
            "debug_error": raw_resp
        }

    spot_price = None
    for r in rows:
        s = safe_float(r.get("spot_price"))
        if s is not None:
            spot_price = s
            break

    out_rows = []
    for r in rows:
        strike = safe_float(r.get("strike_price"))
        ltp = safe_float(r.get("ltp"))
        bid = safe_float(r.get("best_bid_price"))
        ask = safe_float(r.get("best_offer_price"))

        if strike is None:
            continue
        if not (ltp and bid and ask):
            continue

        out_rows.append({
            "strike_price": strike,
            "premium": ltp,
            "ltp": ltp,
            "bid": bid,
            "ask": ask,
            "bid_qty": r.get("best_bid_quantity"),
            "ask_qty": r.get("best_offer_quantity"),
            "ltt": r.get("ltt"),
            "volume": r.get("total_quantity_traded"),
        })

    out_rows.sort(key=lambda x: x["strike_price"])

    return {
        "status": "ok",
        "exchange": exchange_code,
        "symbol": stock_code,
        "right": right,
        "expiry_date": expiry_date,
        "spot_price": spot_price,
        "rows_count": len(out_rows),
        "rows": out_rows,
        "attempted_right_values": attempted
    }
