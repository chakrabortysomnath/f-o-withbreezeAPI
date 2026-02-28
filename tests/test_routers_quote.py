import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

TEST_TOKEN = "test-secret-token"

SAMPLE_ROW = {
    "ltp": "150.5",
    "open": "148.0",
    "high": "152.0",
    "low": "147.5",
    "previous_close": "149.0",
    "volume": "10000",
    "ltt": "15:30:00",
    "best_bid_price": "150.4",
    "best_bid_quantity": "50",
    "best_offer_price": "150.6",
    "best_offer_quantity": "30",
    "ltp_percent_change": "0.5",
    "upper_circuit": "165.0",
    "lower_circuit": "135.0",
    "total_quantity_traded": "50000",
    "spot_price": "150.5",
    "expiry_date": None,
    "strike_price": None,
    "right": None,
}


@pytest.fixture
def client():
    with patch("auth.APP_TOKEN", TEST_TOKEN):
        from main import app
        yield TestClient(app)


@pytest.fixture
def auth_headers():
    return {"X-APP-TOKEN": TEST_TOKEN}


@pytest.fixture
def mock_breeze():
    breeze = MagicMock()
    breeze.get_quotes.return_value = {"Success": [SAMPLE_ROW]}
    return breeze


def test_options_quote_returns_204(client):
    response = client.options("/quote")
    assert response.status_code == 204


def test_options_quote_cors_headers(client):
    response = client.options("/quote", headers={"Origin": "http://myapp.com"})
    assert "access-control-allow-origin" in response.headers


def test_quote_success(client, auth_headers, mock_breeze):
    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/quote",
            json={"exchange_code": "NSE", "stock_code": "TCS"},
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "quote" in data
    assert data["quote"]["ltp"] == "150.5"
    assert data["quote"]["exchange"] == "NSE"
    assert data["quote"]["symbol"] == "TCS"


def test_quote_normalizes_stock_code_to_uppercase(client, auth_headers, mock_breeze):
    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        client.post(
            "/quote",
            json={"exchange_code": "nse", "stock_code": "tcs"},
            headers=auth_headers,
        )

    call_kwargs = mock_breeze.get_quotes.call_args.kwargs
    assert call_kwargs["stock_code"] == "TCS"
    assert call_kwargs["exchange_code"] == "NSE"


def test_quote_defaults_product_type_to_cash(client, auth_headers, mock_breeze):
    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        client.post(
            "/quote",
            json={"exchange_code": "NSE", "stock_code": "TCS"},
            headers=auth_headers,
        )

    call_kwargs = mock_breeze.get_quotes.call_args.kwargs
    assert call_kwargs["product_type"] == "cash"


def test_quote_passes_optional_fno_fields(client, auth_headers, mock_breeze):
    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        client.post(
            "/quote",
            json={
                "exchange_code": "NFO",
                "stock_code": "NIFTY",
                "product_type": "options",
                "expiry_date": "27-Mar-2026",
                "strike_price": "22500",
                "right": "call",
            },
            headers=auth_headers,
        )

    call_kwargs = mock_breeze.get_quotes.call_args.kwargs
    assert call_kwargs["expiry_date"] == "27-Mar-2026"
    assert call_kwargs["strike_price"] == "22500"
    assert call_kwargs["right"] == "call"


def test_quote_returns_error_when_no_rows(client, auth_headers):
    mock_breeze = MagicMock()
    mock_breeze.get_quotes.return_value = {"Success": []}

    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/quote",
            json={"exchange_code": "NSE", "stock_code": "TCS"},
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "error"


def test_quote_returns_error_on_breeze_failure(client, auth_headers):
    mock_breeze = MagicMock()
    mock_breeze.get_quotes.return_value = {"Error": "something went wrong"}

    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/quote",
            json={"exchange_code": "NSE", "stock_code": "TCS"},
            headers=auth_headers,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "error"


def test_quote_requires_auth(client):
    response = client.post("/quote", json={"exchange_code": "NSE", "stock_code": "TCS"})
    assert response.status_code == 401


def test_quote_wrong_token(client):
    response = client.post(
        "/quote",
        json={"exchange_code": "NSE", "stock_code": "TCS"},
        headers={"X-APP-TOKEN": "bad-token"},
    )
    assert response.status_code == 401


def test_quote_missing_required_fields(client, auth_headers):
    response = client.post("/quote", json={"exchange_code": "NSE"}, headers=auth_headers)
    assert response.status_code == 422


def test_quote_includes_raw_keys(client, auth_headers, mock_breeze):
    with patch("routers.quote.get_breeze", return_value=mock_breeze):
        response = client.post(
            "/quote",
            json={"exchange_code": "NSE", "stock_code": "TCS"},
            headers=auth_headers,
        )

    data = response.json()
    assert "raw_keys" in data
    assert isinstance(data["raw_keys"], list)
