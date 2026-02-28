import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

TEST_TOKEN = "test-secret-token"

SAMPLE_CHAIN_ROWS = [
    {
        "strike_price": "22000",
        "ltp": "120.5",
        "best_bid_price": "120.0",
        "best_offer_price": "121.0",
        "best_bid_quantity": "100",
        "best_offer_quantity": "80",
        "ltt": "15:30:00",
        "total_quantity_traded": "5000",
        "spot_price": "22150.0",
    },
    {
        "strike_price": "22500",
        "ltp": "45.0",
        "best_bid_price": "44.5",
        "best_offer_price": "45.5",
        "best_bid_quantity": "200",
        "best_offer_quantity": "150",
        "ltt": "15:30:00",
        "total_quantity_traded": "12000",
        "spot_price": "22150.0",
    },
    {
        # Row with no ltp — should be filtered out
        "strike_price": "23000",
        "ltp": None,
        "best_bid_price": None,
        "best_offer_price": None,
        "spot_price": "22150.0",
    },
]


@pytest.fixture
def client():
    with patch("auth.APP_TOKEN", TEST_TOKEN):
        from main import app
        yield TestClient(app)


@pytest.fixture
def auth_headers():
    return {"X-APP-TOKEN": TEST_TOKEN}


# ──────────────────────────────────────────────────────────────────────────────
# /option_strikes
# ──────────────────────────────────────────────────────────────────────────────

def test_options_strikes_preflight_returns_204(client):
    response = client.options("/option_strikes")
    assert response.status_code == 204


def test_options_strikes_preflight_cors_headers(client):
    response = client.options("/option_strikes", headers={"Origin": "http://myapp.com"})
    assert "access-control-allow-origin" in response.headers


def test_option_strikes_success(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_strikes",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "expiry_date": "27-Mar-2026",
                "right": "call",
            },
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["symbol"] == "NIFTY"
    assert data["exchange"] == "NFO"
    assert isinstance(data["strikes"], list)
    assert data["count"] == len(data["strikes"])
    assert data["spot_price"] == 22150.0


def test_option_strikes_returns_sorted_strikes(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_strikes",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "expiry_date": "27-Mar-2026",
                "right": "call",
            },
            headers=auth_headers,
        )

    strikes = response.json()["strikes"]
    assert strikes == sorted(strikes)


def test_option_strikes_invalid_right_returns_400(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/option_strikes",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "expiry_date": "27-Mar-2026",
                "right": "straddle",
            },
            headers=auth_headers,
        )

    assert response.status_code == 400


def test_option_strikes_returns_error_when_no_rows(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=([], ["call", "Call"], {"Error": "none"})):

        response = client.post(
            "/option_strikes",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "expiry_date": "27-Mar-2026",
                "right": "call",
            },
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "error"


def test_option_strikes_requires_auth(client):
    response = client.post(
        "/option_strikes",
        json={"exchange_code": "NFO", "stock_code": "NIFTY", "expiry_date": "27-Mar-2026", "right": "call"},
    )
    assert response.status_code == 401


# ──────────────────────────────────────────────────────────────────────────────
# /option_chain_compare
# ──────────────────────────────────────────────────────────────────────────────

def test_options_chain_compare_preflight_returns_204(client):
    response = client.options("/option_chain_compare")
    assert response.status_code == 204


def test_option_chain_compare_success(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "right": "call",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["exchange"] == "NFO"
    assert data["symbol"] == "NIFTY"
    assert data["spot_price"] == 22150.0
    assert isinstance(data["rows"], list)


def test_option_chain_compare_filters_zero_ltp_rows(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "right": "call",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    rows = response.json()["rows"]
    # SAMPLE_CHAIN_ROWS has 3 rows; the third has ltp=None so should be filtered
    assert len(rows) == 2
    for row in rows:
        assert row["ltp"] is not None
        assert row["bid"] is not None
        assert row["ask"] is not None


def test_option_chain_compare_rows_sorted_by_strike(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "right": "call",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    strikes = [r["strike_price"] for r in response.json()["rows"]]
    assert strikes == sorted(strikes)


def test_option_chain_compare_rejects_non_fno_exchange(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NSE",
                "stock_code": "TCS",
                "right": "call",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    assert response.status_code == 400


def test_option_chain_compare_invalid_right_returns_400(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "right": "straddle",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    assert response.status_code == 400


def test_option_chain_compare_returns_error_when_no_rows(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=([], ["call", "Call"], {})):

        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "right": "call",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "error"


def test_option_chain_compare_requires_auth(client):
    response = client.post(
        "/option_chain_compare",
        json={"exchange_code": "NFO", "stock_code": "NIFTY", "right": "call", "expiry_date": "27-Mar-2026"},
    )
    assert response.status_code == 401


def test_option_chain_compare_bfo_exchange_allowed(client, auth_headers):
    mock_breeze = MagicMock()
    with patch("routers.options.get_breeze", return_value=mock_breeze), \
         patch("routers.options.fetch_option_chain_rows",
               return_value=(SAMPLE_CHAIN_ROWS, ["call"], {})):

        response = client.post(
            "/option_chain_compare",
            json={
                "exchange_code": "BFO",
                "stock_code": "SENSEX",
                "right": "put",
                "expiry_date": "27-Mar-2026",
            },
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
