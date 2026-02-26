import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

TEST_TOKEN = "test-secret-token"


@pytest.fixture
def client():
    with patch("auth.APP_TOKEN", TEST_TOKEN):
        from main import app
        yield TestClient(app)


@pytest.fixture
def auth_headers():
    return {"X-APP-TOKEN": TEST_TOKEN}


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_version_returns_version_string(client):
    response = client.get("/version")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data


def test_echo_returns_payload(client, auth_headers):
    payload = {"foo": "bar", "num": 42}
    response = client.post("/echo", json=payload, headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["received"] == payload


def test_echo_requires_auth(client):
    response = client.post("/echo", json={"x": 1})
    assert response.status_code == 401


def test_echo_wrong_token(client):
    response = client.post("/echo", json={}, headers={"X-APP-TOKEN": "wrong"})
    assert response.status_code == 401


def test_egress_ip_returns_ip(client, auth_headers):
    mock_response = MagicMock()
    mock_response.json.return_value = {"ip": "1.2.3.4"}

    with patch("routers.health.requests.get", return_value=mock_response):
        response = client.get("/egress_ip", headers=auth_headers)

    assert response.status_code == 200
    assert response.json() == {"ip": "1.2.3.4"}


def test_egress_ip_requires_auth(client):
    response = client.get("/egress_ip")
    assert response.status_code == 401
