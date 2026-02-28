import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

import breeze_client


class TestGetBreeze:
    def test_raises_500_when_all_env_vars_missing(self):
        with patch.object(breeze_client, "BREEZE_API_KEY", ""), \
             patch.object(breeze_client, "BREEZE_API_SECRET", ""), \
             patch.object(breeze_client, "BREEZE_SESSION_TOKEN", ""):
            with pytest.raises(HTTPException) as exc:
                breeze_client.get_breeze()
        assert exc.value.status_code == 500

    def test_raises_500_when_api_key_missing(self):
        with patch.object(breeze_client, "BREEZE_API_KEY", ""), \
             patch.object(breeze_client, "BREEZE_API_SECRET", "secret"), \
             patch.object(breeze_client, "BREEZE_SESSION_TOKEN", "token"):
            with pytest.raises(HTTPException) as exc:
                breeze_client.get_breeze()
        assert exc.value.status_code == 500

    def test_raises_500_when_session_token_missing(self):
        with patch.object(breeze_client, "BREEZE_API_KEY", "key"), \
             patch.object(breeze_client, "BREEZE_API_SECRET", "secret"), \
             patch.object(breeze_client, "BREEZE_SESSION_TOKEN", ""):
            with pytest.raises(HTTPException) as exc:
                breeze_client.get_breeze()
        assert exc.value.status_code == 500

    def test_returns_initialized_breeze_instance(self):
        with patch.object(breeze_client, "BREEZE_API_KEY", "key"), \
             patch.object(breeze_client, "BREEZE_API_SECRET", "secret"), \
             patch.object(breeze_client, "BREEZE_SESSION_TOKEN", "token"), \
             patch("breeze_client.BreezeConnect") as mock_bc:
            mock_instance = MagicMock()
            mock_bc.return_value = mock_instance

            result = breeze_client.get_breeze()

        assert result == mock_instance
        mock_bc.assert_called_once_with(api_key="key")
        mock_instance.generate_session.assert_called_once_with(
            api_secret="secret", session_token="token"
        )


class TestFetchOptionChainRows:
    def _make_breeze(self, side_effects):
        mock_breeze = MagicMock()
        mock_breeze.get_option_chain_quotes.side_effect = side_effects
        return mock_breeze

    def test_returns_rows_on_first_attempt(self):
        rows = [{"strike_price": "100", "ltp": "5.0"}]
        breeze = self._make_breeze([{"Success": rows}])

        result_rows, attempted, _ = breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "call"
        )

        assert result_rows == rows
        assert attempted == ["call"]
        assert breeze.get_option_chain_quotes.call_count == 1

    def test_falls_back_to_capitalized_right(self):
        rows = [{"strike_price": "200", "ltp": "8.0"}]
        breeze = self._make_breeze([
            {"Success": []},   # "call" fails
            {"Success": rows}  # "Call" succeeds
        ])

        result_rows, attempted, _ = breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "call"
        )

        assert result_rows == rows
        assert attempted == ["call", "Call"]
        assert breeze.get_option_chain_quotes.call_count == 2

    def test_returns_empty_when_both_attempts_fail(self):
        breeze = self._make_breeze([
            {"Success": []},
            {"Success": []}
        ])

        result_rows, attempted, last_resp = breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "put"
        )

        assert result_rows == []
        assert attempted == ["put", "Put"]

    def test_handles_missing_success_key(self):
        breeze = self._make_breeze([
            {"Error": "some error"},
            {"Error": "some error"}
        ])

        result_rows, attempted, _ = breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "call"
        )

        assert result_rows == []

    def test_passes_correct_params_to_api(self):
        rows = [{"strike_price": "100"}]
        breeze = self._make_breeze([{"Success": rows}])

        breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "put"
        )

        breeze.get_option_chain_quotes.assert_called_once_with(
            stock_code="TCS",
            exchange_code="NFO",
            product_type="options",
            right="put",
            expiry_date="30-Mar-2026",
        )

    def test_put_capitalized_fallback(self):
        rows = [{"strike_price": "300"}]
        breeze = self._make_breeze([
            {"Success": []},
            {"Success": rows}
        ])

        _, attempted, _ = breeze_client.fetch_option_chain_rows(
            breeze, "TCS", "NFO", "30-Mar-2026", "put"
        )

        assert attempted == ["put", "Put"]
