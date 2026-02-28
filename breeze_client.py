import os
from fastapi import HTTPException
from breeze_connect import BreezeConnect

BREEZE_API_KEY = os.environ.get("BREEZE_API_KEY", "")
BREEZE_API_SECRET = os.environ.get("BREEZE_API_SECRET", "")
BREEZE_SESSION_TOKEN = os.environ.get("BREEZE_SESSION_TOKEN", "")


def get_breeze():
    """Initialize and return an authenticated Breeze API client."""
    if not (BREEZE_API_KEY and BREEZE_API_SECRET and BREEZE_SESSION_TOKEN):
        raise HTTPException(status_code=500, detail="Breeze env vars not set")
    breeze = BreezeConnect(api_key=BREEZE_API_KEY)
    breeze.generate_session(api_secret=BREEZE_API_SECRET, session_token=BREEZE_SESSION_TOKEN)
    return breeze


def fetch_option_chain_rows(breeze, stock_code: str, exchange_code: str, expiry_date: str, right: str):
    """
    Fetch option chain rows from Breeze API.
    Tries both lowercase and capitalized right values to handle Breeze variations.
    Returns (rows, attempted_right_values, raw_response).
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
