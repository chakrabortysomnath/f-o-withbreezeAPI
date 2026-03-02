import requests
import streamlit as st


def _base_url() -> str:
    return st.secrets["BASE_URL"].rstrip("/")


def _headers() -> dict:
    return {
        "X-APP-TOKEN": st.secrets["APP_TOKEN"],
        "Content-Type": "application/json",
    }


def _post(path: str, payload: dict) -> dict:
    url = f"{_base_url()}{path}"
    resp = requests.post(url, json=payload, headers=_headers(), timeout=30)
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
