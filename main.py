import os
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from breeze_connect import BreezeConnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional



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


class QuoteRequest(BaseModel):
    exchange_code: str  # e.g. "NSE"
    stock_code: str     # e.g. "TCS"
    product_type: Optional[str] = None   # "cash", "futures", "options"
    expiry_date: Optional[str] = None     # e.g. "27-Mar-2026"
    strike_price: Optional[str] = None    # e.g. "22500"
    right: Optional[str] = None           # "call" or "put"


def require_auth(x_app_token: str | None):
    if not APP_TOKEN:
        raise HTTPException(status_code=500, detail="APP_TOKEN not set on server")
    if x_app_token != APP_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

def get_breeze():
    if not (BREEZE_API_KEY and BREEZE_API_SECRET and BREEZE_SESSION_TOKEN):
        raise HTTPException(status_code=500, detail="Breeze env vars not set")
    breeze = BreezeConnect(api_key=BREEZE_API_KEY)
    breeze.generate_session(api_secret=BREEZE_API_SECRET, session_token=BREEZE_SESSION_TOKEN)
    return breeze

@app.get("/health")
def health():
    return {"ok": True}


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



