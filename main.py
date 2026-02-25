import os
import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from breeze_connect import BreezeConnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from fastapi import Response, Request
from fastapi import Header



app = FastAPI(title="Breeze Tiny Endpoint")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Simple shared-secret protection
APP_TOKEN = os.environ.get("APP_TOKEN", "")

# Breeze credentials (we’ll set these in Render later as environment variables)
BREEZE_API_KEY = os.environ.get("BREEZE_API_KEY", "")
BREEZE_API_SECRET = os.environ.get("BREEZE_API_SECRET", "")
BREEZE_SESSION_TOKEN = os.environ.get("BREEZE_SESSION_TOKEN", "")


class StrikeListRequest(BaseModel):
    exchange_code: str          # "NFO"
    stock_code: str             # e.g. "TCS"
    expiry_date: str            # e.g. "30-Mar-2026" or Breeze-supported format
    right: str                  # "call" / "put"
    product_type: Optional[str] = "options"


class QuoteRequest(BaseModel):
    exchange_code: str  # e.g. "NSE"
    stock_code: str     # e.g. "TCS"
    product_type: Optional[str] = None   # "cash", "futures", "options"
    expiry_date: Optional[str] = None     # e.g. "27-Mar-2026"
    strike_price: Optional[str] = None    # e.g. "22500"
    right: Optional[str] = None           # "call" or "put"

class ChainCompareRequest(BaseModel):
    exchange_code: str          # "NFO"
    stock_code: str             # e.g. "TCS"
    right: str                  # "call" / "put"
    expiry_date: str            # single expiry


def require_auth(x_app_token: str | None):
    if not APP_TOKEN:
        raise HTTPException(status_code=500, detail="APP_TOKEN not set on server")
    if x_app_token != APP_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _safe_float(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _normalize_right_for_chain(user_right: str):
    r = (user_right or "").strip().lower()
    if r not in ("call", "put"):
        raise HTTPException(status_code=400, detail="right must be 'call' or 'put'")
    # Breeze may accept 'call' or 'Call' depending on endpoint behavior; try both where needed
    return r


def _fetch_option_chain_rows_for_expiry(breeze, stock_code: str, exchange_code: str, expiry_date: str, right: str):
    """
    Returns tuple: (rows, attempted_right_values)
    Tries both lowercase and Capitalized right to handle Breeze variations.
    """
    attempted = []
    last_rows = []
    last_resp = None

    for right_val in [right, right.capitalize()]:
        attempted.append(right_val)
        resp = breeze.get_option_chain_quotes(
            stock_code=stock_code,
            exchange_code=exchange_code,
            product_type="options",
            right=right_val,
            expiry_date=expiry_date
        )
        last_resp = resp
        rows = resp.get("Success") or []
        if rows:
            return rows, attempted, resp

    return last_rows, attempted, last_resp


def _nearest_strike_index(sorted_strikes: list[float], spot: float):
    if not sorted_strikes or spot is None:
        return None
    best_i = 0
    best_diff = abs(sorted_strikes[0] - spot)
    for i in range(1, len(sorted_strikes)):
        d = abs(sorted_strikes[i] - spot)
        if d < best_diff:
            best_diff = d
            best_i = i
    return best_i

def get_breeze():
    if not (BREEZE_API_KEY and BREEZE_API_SECRET and BREEZE_SESSION_TOKEN):
        raise HTTPException(status_code=500, detail="Breeze env vars not set")
    breeze = BreezeConnect(api_key=BREEZE_API_KEY)
    breeze.generate_session(api_secret=BREEZE_API_SECRET, session_token=BREEZE_SESSION_TOKEN)
    return breeze


@app.post("/echo")
def echo(payload: dict, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    return {"status": "ok", "received": payload}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/version")
def version():
    return {"version": "cors-options-quote-v1"}

@app.options("/quote")
def options_quote(request: Request):
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-APP-TOKEN",
            "Access-Control-Max-Age": "86400",
        },
    )


@app.get("/egress_ip")
def egress_ip(x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    r = requests.get("https://api.ipify.org?format=json", timeout=10)
    return r.json()


@app.post("/quote")
def quote(req: QuoteRequest, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    breeze = get_breeze()

    params = {
        "stock_code": req.stock_code.strip().upper(),
        "exchange_code": req.exchange_code.strip().upper(),
        "product_type": (req.product_type or "cash").strip().lower(),
    }

    # Add F&O fields only when present
    if req.expiry_date:
        params["expiry_date"] = req.expiry_date
    if req.strike_price:
        params["strike_price"] = str(req.strike_price)
    if req.right:
        params["right"] = req.right

    resp = breeze.get_quotes(**params)

    rows = resp.get("Success") or []
    if not rows:
        return {"status": "error", "error": resp}

    r = rows[0]  # first row

    # Return a stable, flat schema for Google Sheets
    quote = {
        "exchange": req.exchange_code,
        "symbol": req.stock_code,

        # These keys depend on Breeze payload; we'll map what exists safely
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
        "quote": quote,
        "raw": r,
        "raw_keys": sorted(list(r.keys()))
    }

@app.options("/option_strikes")
def option_strikes(request: Request):
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-APP-TOKEN",
            "Access-Control-Max-Age": "86400",
        },
    )

@app.post("/option_strikes")
def option_strikes(
    req: StrikeListRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")
):
    require_auth(x_app_token)
    breeze = get_breeze()

    right_in = (req.right or "").strip().lower()
    if right_in not in ("call", "put"):
        raise HTTPException(status_code=400, detail="right must be 'call' or 'put'")

    attempted = []
    last_resp = None

    for right_val in [right_in, right_in.capitalize()]:
        attempted.append(right_val)

        resp = breeze.get_option_chain_quotes(
            stock_code=req.stock_code.strip().upper(),
            exchange_code=req.exchange_code.strip().upper(),
            product_type="options",
            right=right_val,
            expiry_date=req.expiry_date
        )
        last_resp = resp

        rows = resp.get("Success") or []
        if rows:
            strikes = sorted({
                float(r.get("strike_price"))
                for r in rows
                if r.get("strike_price") is not None and str(r.get("strike_price")).strip() != ""
            })

            # Try to extract spot price from any row
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
                "right": right_val,
                "spot_price": spot,
                "count": len(strikes),
                "strikes": strikes
            }

    return {
        "status": "error",
        "error": last_resp,
        "attempted_right_values": attempted
    }

@app.options("/option_chain_compare")
def option_chain_compare(request: Request):
    return Response(
        status_code=204,
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-APP-TOKEN",
            "Access-Control-Max-Age": "86400",
        },
    )

@app.post("/option_chain_compare")
def option_chain_compare(
    req: ChainCompareRequest,
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")
):
    require_auth(x_app_token)
    breeze = get_breeze()

    exchange_code = req.exchange_code.strip().upper()
    stock_code = req.stock_code.strip().upper()
    right = _normalize_right_for_chain(req.right)
    expiry_date = str(req.expiry_date or "").strip()

    if exchange_code not in ("NFO", "BFO"):
        raise HTTPException(status_code=400, detail="exchange_code must be NFO or BFO for option chain")

    if not expiry_date:
        raise HTTPException(status_code=400, detail="expiry_date is required")

    rows, attempted, raw_resp = _fetch_option_chain_rows_for_expiry(
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

    # Extract spot
    spot_price = None
    for r in rows:
        s = _safe_float(r.get("spot_price"))
        if s is not None:
            spot_price = s
            break

    # Filter and map rows (only non-zero ltp/bid/ask)
    out_rows = []
    for r in rows:
        strike = _safe_float(r.get("strike_price"))
        ltp = _safe_float(r.get("ltp"))
        bid = _safe_float(r.get("best_bid_price"))
        ask = _safe_float(r.get("best_offer_price"))

        if strike is None:
            continue

        # Keep only rows where all three are non-zero
        if not (ltp and bid and ask):
            continue

        out_rows.append({
            "strike_price": strike,
            "premium": ltp,   # Premium = LTP
            "ltp": ltp,
            "bid": bid,
            "ask": ask,
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
