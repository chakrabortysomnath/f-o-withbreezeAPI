import pytest
from unittest.mock import MagicMock
from fastapi import HTTPException

from utils import safe_float, require_right, nearest_strike_index, cors_preflight_response


class TestSafeFloat:
    def test_none_returns_none(self):
        assert safe_float(None) is None

    def test_empty_string_returns_none(self):
        assert safe_float("") is None

    def test_whitespace_returns_none(self):
        assert safe_float("   ") is None

    def test_valid_string_float(self):
        assert safe_float("3.14") == 3.14

    def test_valid_integer(self):
        assert safe_float(42) == 42.0

    def test_valid_float(self):
        assert safe_float(1.5) == 1.5

    def test_invalid_string_returns_none(self):
        assert safe_float("abc") is None

    def test_zero_returns_zero(self):
        assert safe_float("0") == 0.0

    def test_negative_value(self):
        assert safe_float("-100.5") == -100.5


class TestRequireRight:
    def test_call_lowercase(self):
        assert require_right("call") == "call"

    def test_put_lowercase(self):
        assert require_right("put") == "put"

    def test_call_uppercase(self):
        assert require_right("CALL") == "call"

    def test_put_uppercase(self):
        assert require_right("PUT") == "put"

    def test_call_mixed_case(self):
        assert require_right("Call") == "call"

    def test_call_with_whitespace(self):
        assert require_right("  call  ") == "call"

    def test_invalid_value_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            require_right("invalid")
        assert exc.value.status_code == 400

    def test_empty_string_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            require_right("")
        assert exc.value.status_code == 400

    def test_none_raises_400(self):
        with pytest.raises(HTTPException):
            require_right(None)


class TestNearestStrikeIndex:
    def test_empty_list_returns_none(self):
        assert nearest_strike_index([], 100.0) is None

    def test_none_spot_returns_none(self):
        assert nearest_strike_index([100.0, 200.0], None) is None

    def test_exact_match_first(self):
        assert nearest_strike_index([100.0, 200.0, 300.0], 100.0) == 0

    def test_exact_match_middle(self):
        assert nearest_strike_index([100.0, 200.0, 300.0], 200.0) == 1

    def test_exact_match_last(self):
        assert nearest_strike_index([100.0, 200.0, 300.0], 300.0) == 2

    def test_nearest_to_lower(self):
        assert nearest_strike_index([100.0, 200.0, 300.0], 130.0) == 0

    def test_nearest_to_upper(self):
        assert nearest_strike_index([100.0, 200.0, 300.0], 170.0) == 1

    def test_single_element_always_returns_zero(self):
        assert nearest_strike_index([500.0], 1.0) == 0


class TestCorsPreflightResponse:
    def _make_request(self, origin="http://example.com"):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = origin
        return mock_request

    def test_status_code_is_204(self):
        response = cors_preflight_response(self._make_request())
        assert response.status_code == 204

    def test_origin_header_echoed(self):
        response = cors_preflight_response(self._make_request("http://myapp.com"))
        assert response.headers["access-control-allow-origin"] == "http://myapp.com"

    def test_allowed_methods_header(self):
        response = cors_preflight_response(self._make_request())
        assert "POST" in response.headers["access-control-allow-methods"]

    def test_allowed_headers_includes_token(self):
        response = cors_preflight_response(self._make_request())
        assert "X-APP-TOKEN" in response.headers["access-control-allow-headers"]

    def test_max_age_header_present(self):
        response = cors_preflight_response(self._make_request())
        assert response.headers["access-control-max-age"] == "86400"
