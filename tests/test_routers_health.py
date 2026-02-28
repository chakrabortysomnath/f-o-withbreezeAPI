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


# ── /health/detailed tests ─────────────────────────────────────────────────────

def test_health_detailed_requires_auth(client):
    response = client.get("/health/detailed")
    assert response.status_code == 401


def test_health_detailed_wrong_token(client):
    response = client.get("/health/detailed", headers={"X-APP-TOKEN": "bad"})
    assert response.status_code == 401


def test_health_detailed_returns_all_layers(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch("routers.health.BreezeConnect", return_value=mock_breeze),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    assert response.status_code == 200
    data = response.json()
    assert "ok" in data
    assert "layers" in data
    layers = data["layers"]
    assert "backend" in layers
    assert "static_ip" in layers
    assert "breeze_api" in layers


def test_health_detailed_backend_layer_always_ok(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "5.6.7.8"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch("routers.health.BreezeConnect", return_value=mock_breeze),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    assert response.json()["layers"]["backend"]["ok"] is True


def test_health_detailed_static_ip_ok(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch("routers.health.BreezeConnect", return_value=mock_breeze),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    ip_layer = response.json()["layers"]["static_ip"]
    assert ip_layer["ok"] is True
    assert ip_layer["ip"] == "1.2.3.4"


def test_health_detailed_static_ip_failure(client, auth_headers):
    mock_breeze = MagicMock()
    with (
        patch("routers.health.requests.get", side_effect=Exception("timeout")),
        patch.dict(
            "os.environ",
            {
                "BREEZE_API_KEY": "k",
                "BREEZE_API_SECRET": "s",
                "BREEZE_SESSION_TOKEN": "t",
            },
        ),
        patch("routers.health.BreezeConnect", return_value=mock_breeze),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    ip_layer = response.json()["layers"]["static_ip"]
    assert ip_layer["ok"] is False
    assert "timeout" in ip_layer["detail"]


def test_health_detailed_breeze_missing_env_vars(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch.dict(
            "os.environ",
            {"BREEZE_API_KEY": "", "BREEZE_API_SECRET": "", "BREEZE_SESSION_TOKEN": ""},
        ),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    breeze_layer = response.json()["layers"]["breeze_api"]
    assert breeze_layer["ok"] is False
    assert "Missing env vars" in breeze_layer["detail"]


def test_health_detailed_breeze_session_ok(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze_instance = MagicMock()
    mock_breeze_class = MagicMock(return_value=mock_breeze_instance)

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch.dict(
            "os.environ",
            {
                "BREEZE_API_KEY": "key123",
                "BREEZE_API_SECRET": "sec123",
                "BREEZE_SESSION_TOKEN": "tok123",
            },
        ),
        patch("routers.health.BreezeConnect", mock_breeze_class),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    breeze_layer = response.json()["layers"]["breeze_api"]
    assert breeze_layer["ok"] is True
    mock_breeze_instance.generate_session.assert_called_once_with(
        api_secret="sec123", session_token="tok123"
    )


def test_health_detailed_breeze_session_error(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze_instance = MagicMock()
    mock_breeze_instance.generate_session.side_effect = Exception("Session expired")
    mock_breeze_class = MagicMock(return_value=mock_breeze_instance)

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch.dict(
            "os.environ",
            {
                "BREEZE_API_KEY": "key123",
                "BREEZE_API_SECRET": "sec123",
                "BREEZE_SESSION_TOKEN": "tok123",
            },
        ),
        patch("routers.health.BreezeConnect", mock_breeze_class),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    breeze_layer = response.json()["layers"]["breeze_api"]
    assert breeze_layer["ok"] is False
    assert "Session expired" in breeze_layer["detail"]


def test_health_detailed_overall_ok_when_all_pass(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    mock_breeze = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch.dict(
            "os.environ",
            {
                "BREEZE_API_KEY": "k",
                "BREEZE_API_SECRET": "s",
                "BREEZE_SESSION_TOKEN": "t",
            },
        ),
        patch("routers.health.BreezeConnect", return_value=mock_breeze),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    assert response.json()["ok"] is True


def test_health_detailed_overall_not_ok_when_breeze_fails(client, auth_headers):
    mock_ip_resp = MagicMock()
    mock_ip_resp.json.return_value = {"ip": "1.2.3.4"}
    mock_ip_resp.raise_for_status = MagicMock()

    with (
        patch("routers.health.requests.get", return_value=mock_ip_resp),
        patch.dict(
            "os.environ",
            {"BREEZE_API_KEY": "", "BREEZE_API_SECRET": "", "BREEZE_SESSION_TOKEN": ""},
        ),
    ):
        response = client.get("/health/detailed", headers=auth_headers)

    assert response.json()["ok"] is False
