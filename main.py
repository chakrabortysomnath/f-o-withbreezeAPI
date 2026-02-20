import os

import io 
import gzip
import csv
import time
import requests

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


# --- NSE F&O contract file lot size lookup ---
_nse_lot_cache = {
    "data": None,         # dict: symbol -> lot_size
    "loaded_at": 0,       # epoch seconds
    "source_url": None
}

def _nse_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/",
    })
    return s

def _get_latest_nse_contract_url():
    """
    Tries to discover the latest NSE_FO_contract_ddmmyyyy.csv.gz file from NSE All Reports page.
    Falls back to env var NSE_FO_CONTRACT_URL if set.
    """
    forced = os.environ.get("NSE_FO_CONTRACT_URL", "").strip()
    if forced:
        return forced

    reports_url = "https://www.nseindia.com/all-reports-derivatives"
    sess = _nse_session()

    # NSE sometimes requires homepage hit first for cookies
    try:
        sess.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass

    r = sess.get(reports_url, timeout=15)
    r.raise_for_status()
    html = r.text

    # Find latest filename like NSE_FO_contract_17022026.csv.gz
    import re
    matches = re.findall(r"NSE_FO_contract_\d{8}\.csv\.gz", html)
    if not matches:
        raise RuntimeError("Could not find NSE_FO_contract file name on NSE reports page")

    # Pick the latest by date in filename
    latest_name = sorted(matches)[-1]
    return f"https://nsearchives.nseindia.com/content/fo/{latest_name}"

def _normalize_symbol(sym: str) -> str:
    return (sym or "").strip().upper()

def _extract_lot_size_map_from_contract_csv(content_bytes: bytes):
    """
    Reads NSE_FO_contract_ddmmyyyy.csv.gz and builds symbol->lot_size map.
    Uses header heuristics because column names may vary slightly.
    """
    with gzip.GzipFile(fileobj=io.BytesIO(content_bytes)) as gz:
        raw = gz.read()

    # decode safely
    text = raw.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    rows = list(reader)
    if not rows:
        raise RuntimeError("NSE contract file parsed but contains no rows")

    headers = [h.strip() for h in (reader.fieldnames or []) if h]
    lower = {h.lower(): h for h in headers}

    # Heuristic column matching
    symbol_col = None
    lot_col = None
    instr_col = None

    # Common symbol column names
    for candidate in [
        "symbol", "underlying", "underlying_symbol", "tradingsymbol", "security"
    ]:
        if candidate in lower:
            symbol_col = lower[candidate]
            break

    # Common lot-size column names
    for candidate in [
        "market lot", "market_lot", "lot_size", "lotsize", "qty freeze lot", "quantity freeze"
    ]:
        if candidate in lower:
            lot_col = lower[candidate]
            break

    # Instrument type (optional filter)
    for candidate in ["instrument", "instrumenttype", "inst type", "series"]:
        if candidate in lower:
            instr_col = lower[candidate]
            break

    # If not found, try fuzzy search
    if not symbol_col:
        for h in headers:
            if "symbol" in h.lower() or "underly" in h.lower():
                symbol_col = h
                break
    if not lot_col:
        for h in headers:
            hl = h.lower()
            if "lot" in hl and ("size" in hl or "market" in hl):
                lot_col = h
                break

    if not symbol_col or not lot_col:
        raise RuntimeError(f"Could not detect symbol/lot columns in NSE contract file. Headers: {headers}")

    lot_map = {}

    for row in rows:
        sym = _normalize_symbol(str(row.get(symbol_col, "")))
        if not sym:
            continue

        # Optional instrument filtering: prefer FUT/OPT records if mixed file
        if instr_col:
            inst = str(row.get(instr_col, "")).upper()
            if inst and not any(x in inst for x in ["OPT", "FUT"]):
                # skip unrelated rows if clearly non-derivative
                continue

        raw_lot = str(row.get(lot_col, "")).strip().replace(",", "")
        if not raw_lot:
            continue

        try:
            lot = int(float(raw_lot))
        except Exception:
            continue

        # keep first valid lot (usually same across rows for a symbol)
        if sym not in lot_map:
            lot_map[sym] = lot

    if not lot_map:
        raise RuntimeError("Parsed NSE contract file but no lot sizes found")

    return lot_map

def _load_nse_lot_sizes(force_refresh: bool = False):
    # cache for 6 hours
    ttl_seconds = 6 * 60 * 60
    now = time.time()

    if (not force_refresh and
        _nse_lot_cache["data"] is not None and
        (now - _nse_lot_cache["loaded_at"] < ttl_seconds)):
        return _nse_lot_cache["data"]

    url = _get_latest_nse_contract_url()
    sess = _nse_session()
    try:
        sess.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass

    r = sess.get(url, timeout=20)
    r.raise_for_status()

    lot_map = _extract_lot_size_map_from_contract_csv(r.content)

    _nse_lot_cache["data"] = lot_map
    _nse_lot_cache["loaded_at"] = now
    _nse_lot_cache["source_url"] = url
    return lot_map

def get_nse_lot_size(symbol: str):
    sym = _normalize_symbol(symbol)
    if not sym:
        return None

    lot_map = _load_nse_lot_sizes(force_refresh=False)
    return lot_map.get(sym)

@app.get("/lot_size/{symbol}")
def lot_size_lookup(symbol: str, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    try:
        lot = get_nse_lot_size(symbol)
        return {
            "status": "ok",
            "symbol": symbol.upper(),
            "lot_size": lot,
            "source_url": _nse_lot_cache.get("source_url")
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

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

    lot_size = None
    try:
        if req.exchange_code.upper() == "NFO":
            lot_size = get_nse_lot_size(req.stock_code)
    except Exception:
        # don't fail quote if NSE lot lookup fails
        lot_size = None

    return {
        "status": "ok",
        "quote": quote,
        "meta": {"lot_size": lot_size},
        "raw": r,
        "raw_keys": sorted(list(r.keys())) 
    }


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

