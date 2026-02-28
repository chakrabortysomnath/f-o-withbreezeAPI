import pytest
from unittest.mock import patch
from fastapi import HTTPException

import auth


def test_require_auth_raises_500_when_app_token_not_set():
    with patch.object(auth, "APP_TOKEN", ""):
        with pytest.raises(HTTPException) as exc:
            auth.require_auth("any-token")
    assert exc.value.status_code == 500
    assert "APP_TOKEN" in exc.value.detail


def test_require_auth_raises_401_on_wrong_token():
    with patch.object(auth, "APP_TOKEN", "correct-secret"):
        with pytest.raises(HTTPException) as exc:
            auth.require_auth("wrong-token")
    assert exc.value.status_code == 401


def test_require_auth_raises_401_on_none_token():
    with patch.object(auth, "APP_TOKEN", "correct-secret"):
        with pytest.raises(HTTPException) as exc:
            auth.require_auth(None)
    assert exc.value.status_code == 401


def test_require_auth_passes_with_correct_token():
    with patch.object(auth, "APP_TOKEN", "correct-secret"):
        auth.require_auth("correct-secret")  # must not raise
