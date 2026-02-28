"""
Tests for POST /session endpoint.
"""
import pytest
from unittest.mock import patch, MagicMock


def test_session_options_preflight(client):
    resp = client.options("/session", headers={"Origin": "http://localhost"})
    assert resp.status_code == 204


def test_session_requires_auth(client):
    resp = client.post("/session", json={"session_token": "tok123"})
    assert resp.status_code == 401


def test_session_missing_breeze_env(client, auth_headers):
    with patch("routers.session.BREEZE_API_KEY", ""), \
         patch("routers.session.BREEZE_API_SECRET", ""):
        resp = client.post("/session", json={"session_token": "tok123"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "error"
    assert "not set" in body["detail"]


def test_session_success(client, auth_headers):
    mock_breeze = MagicMock()
    mock_breeze.generate_session.return_value = {
        "Status": 200,
        "Success": {"idirect_userid": "CUST001"},
    }
    mock_breeze_cls = MagicMock(return_value=mock_breeze)

    with patch("routers.session.BREEZE_API_KEY", "key123"), \
         patch("routers.session.BREEZE_API_SECRET", "secret123"), \
         patch("routers.session.BreezeConnect", mock_breeze_cls):
        resp = client.post("/session", json={"session_token": "tok123"}, headers=auth_headers)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["customer_id"] == "CUST001"
    assert body["session_token"] == "tok123"
    mock_breeze_cls.assert_called_once_with(api_key="key123")
    mock_breeze.generate_session.assert_called_once_with(
        api_secret="secret123", session_token="tok123"
    )


def test_session_failure_from_breeze(client, auth_headers):
    mock_breeze = MagicMock()
    mock_breeze.generate_session.return_value = {
        "Status": 400,
        "Error": "Invalid session token",
    }
    mock_breeze_cls = MagicMock(return_value=mock_breeze)

    with patch("routers.session.BREEZE_API_KEY", "key123"), \
         patch("routers.session.BREEZE_API_SECRET", "secret123"), \
         patch("routers.session.BreezeConnect", mock_breeze_cls):
        resp = client.post("/session", json={"session_token": "bad-tok"}, headers=auth_headers)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "error"
    assert body["detail"] == "Failed to generate session"


def test_session_strips_whitespace_from_token(client, auth_headers):
    mock_breeze = MagicMock()
    mock_breeze.generate_session.return_value = {
        "Status": 200,
        "Success": {"idirect_userid": "CUST002"},
    }
    mock_breeze_cls = MagicMock(return_value=mock_breeze)

    with patch("routers.session.BREEZE_API_KEY", "key123"), \
         patch("routers.session.BREEZE_API_SECRET", "secret123"), \
         patch("routers.session.BreezeConnect", mock_breeze_cls):
        resp = client.post("/session", json={"session_token": "  tok456  "}, headers=auth_headers)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["session_token"] == "tok456"
    mock_breeze.generate_session.assert_called_once_with(
        api_secret="secret123", session_token="tok456"
    )
