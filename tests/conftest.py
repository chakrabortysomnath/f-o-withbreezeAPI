"""
Shared fixtures and test configuration.
"""
import sys
from unittest.mock import MagicMock

# breeze_connect is not installable in the test environment; stub it out so
# all modules that import it can be collected and tested with mocks.
sys.modules.setdefault("breeze_connect", MagicMock())

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

TEST_TOKEN = "test-secret-token"


@pytest.fixture
def client():
    """TestClient with APP_TOKEN patched in for auth."""
    with patch("auth.APP_TOKEN", TEST_TOKEN):
        from main import app
        yield TestClient(app)


@pytest.fixture
def auth_headers():
    """Valid auth headers for authenticated requests."""
    return {"X-APP-TOKEN": TEST_TOKEN}
