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

    # Cash market quote (NSE/BSE)
    resp = breeze.get_quotes(
        stock_code=req.stock_code,
        exchange_code=req.exchange_code,
        product_type="cash"
    )

    return {"status": "ok", "data": resp}

