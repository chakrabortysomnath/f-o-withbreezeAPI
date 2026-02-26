import pytest
from pydantic import ValidationError

from models import StrikeListRequest, QuoteRequest, ChainCompareRequest


class TestStrikeListRequest:
    def test_valid_minimal(self):
        req = StrikeListRequest(
            exchange_code="NFO",
            stock_code="TCS",
            expiry_date="30-Mar-2026",
            right="call",
        )
        assert req.exchange_code == "NFO"
        assert req.stock_code == "TCS"
        assert req.right == "call"
        assert req.product_type == "options"  # default

    def test_custom_product_type(self):
        req = StrikeListRequest(
            exchange_code="NFO",
            stock_code="TCS",
            expiry_date="30-Mar-2026",
            right="put",
            product_type="futures",
        )
        assert req.product_type == "futures"

    def test_missing_exchange_code_raises(self):
        with pytest.raises(ValidationError):
            StrikeListRequest(stock_code="TCS", expiry_date="30-Mar-2026", right="call")

    def test_missing_stock_code_raises(self):
        with pytest.raises(ValidationError):
            StrikeListRequest(exchange_code="NFO", expiry_date="30-Mar-2026", right="call")

    def test_missing_expiry_raises(self):
        with pytest.raises(ValidationError):
            StrikeListRequest(exchange_code="NFO", stock_code="TCS", right="call")

    def test_missing_right_raises(self):
        with pytest.raises(ValidationError):
            StrikeListRequest(exchange_code="NFO", stock_code="TCS", expiry_date="30-Mar-2026")


class TestQuoteRequest:
    def test_minimal_required_fields(self):
        req = QuoteRequest(exchange_code="NSE", stock_code="TCS")
        assert req.exchange_code == "NSE"
        assert req.stock_code == "TCS"
        assert req.product_type is None
        assert req.expiry_date is None
        assert req.strike_price is None
        assert req.right is None

    def test_full_options_request(self):
        req = QuoteRequest(
            exchange_code="NFO",
            stock_code="NIFTY",
            product_type="options",
            expiry_date="27-Mar-2026",
            strike_price="22500",
            right="call",
        )
        assert req.product_type == "options"
        assert req.strike_price == "22500"
        assert req.right == "call"

    def test_missing_exchange_code_raises(self):
        with pytest.raises(ValidationError):
            QuoteRequest(stock_code="TCS")

    def test_missing_stock_code_raises(self):
        with pytest.raises(ValidationError):
            QuoteRequest(exchange_code="NSE")


class TestChainCompareRequest:
    def test_valid(self):
        req = ChainCompareRequest(
            exchange_code="NFO",
            stock_code="BANKNIFTY",
            right="put",
            expiry_date="30-Mar-2026",
        )
        assert req.right == "put"
        assert req.stock_code == "BANKNIFTY"

    def test_missing_right_raises(self):
        with pytest.raises(ValidationError):
            ChainCompareRequest(
                exchange_code="NFO",
                stock_code="TCS",
                expiry_date="30-Mar-2026",
            )

    def test_missing_expiry_raises(self):
        with pytest.raises(ValidationError):
            ChainCompareRequest(exchange_code="NFO", stock_code="TCS", right="call")
