import os
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from breeze_connect import BreezeConnect
from fastapi.middleware.cors import CORSMiddleware


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

class SearchRequest(BaseModel):
    exchange_code: str  # "NSE"
    query: str          # partial text, e.g. "reliance"

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

@app.get("/debug/breeze_methods")
def breeze_methods(x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    breeze = get_breeze()

    # show likely search-related methods
    candidates = [m for m in dir(breeze) if any(k in m.lower() for k in ["search", "name", "scrip", "instrument", "symbol"])]
    candidates.sort()
    return {"status": "ok", "methods": candidates}


@app.post("/quote")
def quote(req: QuoteRequest, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
    require_auth(x_app_token)
    breeze = get_breeze()

    resp = breeze.get_quotes(
        stock_code=req.stock_code,
        exchange_code=req.exchange_code,
        product_type="cash"
    )

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
    }
    return {"status": "ok", "quote": quote, "raw": r}


    @app.post("/search")
    def search(req: SearchRequest, x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN")):
        require_auth(x_app_token)
        breeze = get_breeze()

        # Breeze SDK supports instrument search; exact name may be get_names / search_scrips depending on SDK version.
        # We'll try get_names first.
        try:
            resp = breeze.get_names(exchange_code=req.exchange_code, stock_code=req.query)
        except Exception as e:
            return {"status": "error", "error": str(e)}

        return {"status": "ok", "data": resp}

