from pydantic import BaseModel
from typing import Optional


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
