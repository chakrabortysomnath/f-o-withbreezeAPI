from __future__ import annotations

import requests
import streamlit as st


def _base_url() -> str:
    return st.secrets["BASE_URL"].rstrip("/")


def _headers() -> dict:
    return {
        "X-APP-TOKEN": st.secrets["APP_TOKEN"],
        "Content-Type": "application/json",
    }


def _post(path: str, payload: dict, timeout: int = 30) -> dict:
    url = f"{_base_url()}{path}"
    resp = requests.post(url, json=payload, headers=_headers(), timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        raise ValueError(f"HTTP {resp.status_code}: non-JSON response from server")
    if resp.status_code != 200 or data.get("status") != "ok":
        raise ValueError(data.get("error", f"HTTP {resp.status_code}"))
    return data


def fetch_quote(
    exchange_code: str,
    stock_code: str,
    product_type: str = "cash",
    expiry_date: str | None = None,
    strike_price: str | None = None,
    right: str | None = None,
) -> dict:
    payload: dict = {
        "exchange_code": exchange_code,
        "stock_code": stock_code,
        "product_type": product_type,
    }
    if expiry_date:
        payload["expiry_date"] = expiry_date
    if strike_price:
        payload["strike_price"] = str(strike_price)
    if right:
        payload["right"] = right
    return _post("/quote", payload)["quote"]


def fetch_option_strikes(
    exchange_code: str,
    stock_code: str,
    expiry_date: str,
    right: str,
) -> tuple[list, float | None]:
    payload = {
        "exchange_code": exchange_code,
        "stock_code": stock_code,
        "expiry_date": expiry_date,
        "right": right,
    }
    data = _post("/option_strikes", payload)
    return data["strikes"], data.get("spot_price")


def fetch_option_chain(
    exchange_code: str,
    stock_code: str,
    right: str,
    expiry_date: str,
) -> dict:
    payload = {
        "exchange_code": exchange_code,
        "stock_code": stock_code,
        "right": right,
        "expiry_date": expiry_date,
    }
    return _post("/option_chain_compare", payload)


def scan_covered_calls(
    expiry_date: str,
    holdings: list[dict],
    otm_max_pct: float = 10.0,
    min_premium_yield_pct: float = 0.3,
) -> dict:
    """
    Call /scan/covered-calls and return the full response dict.
    holdings should be the list returned by fetch_holdings().
    """
    payload = {
        "expiry_date": expiry_date,
        "holdings": [
            {
                "stock_code": h.get("stock_code", ""),
                "quantity": h.get("quantity") or 0,
                "average_cost": h.get("average_cost"),
                "ltp": h.get("ltp"),
            }
            for h in holdings
        ],
        "otm_max_pct": otm_max_pct,
        "min_premium_yield_pct": min_premium_yield_pct,
    }
    return _post("/scan/covered-calls", payload)


def scan_decision_desk(
    expiry_date: str,
    holdings: list[dict],
) -> dict:
    """
    Call /scan/decision-desk and return the full raw response dict.
    Fetches option chains for all 50 Nifty stocks. Holdings provide
    position context (avg_cost, held_qty, ltp).
    Timeout is 180 s to accommodate serial chain fetching for 50 stocks.
    """
    payload = {
        "expiry_date": expiry_date,
        "holdings": [
            {
                "stock_code": h.get("stock_code", ""),
                "quantity": h.get("quantity") or 0,
                "average_cost": h.get("average_cost"),
                "ltp": h.get("ltp"),
            }
            for h in holdings
        ],
        "fetch_all": True,
    }
    return _post("/scan/decision-desk", payload, timeout=180)


def get_covered_call_advice(
    scan_results: list[dict],
    expiry_date: str,
    risk_tolerance: str = "moderate",
    income_goal_pct: float = 1.0,
) -> dict:
    """Call /agent/covered-call-advice and return the advice dict."""
    payload = {
        "scan_results": scan_results,
        "expiry_date": expiry_date,
        "risk_tolerance": risk_tolerance,
        "income_goal_pct": income_goal_pct,
    }
    return _post("/agent/covered-call-advice", payload, timeout=120)


def fetch_holdings(exchange_codes: list[str] | None = None) -> tuple[list[dict], dict]:
    """
    Fetch holdings for the given exchange codes.

    Returns (holdings_list, exchange_errors) where exchange_errors is a dict
    of {exchange: raw_error_response} for any exchange that returned no data.
    """
    params = [("exchange_code", e) for e in (exchange_codes or ["NSE", "BSE"])]
    url = f"{_base_url()}/holdings"
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    try:
        data = resp.json()
    except Exception:
        raise ValueError(f"HTTP {resp.status_code}: non-JSON response from server")
    if resp.status_code != 200 or data.get("status") != "ok":
        raise ValueError(data.get("error", f"HTTP {resp.status_code}"))
    return data["holdings"], data.get("exchange_errors", {})
