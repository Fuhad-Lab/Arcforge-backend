"""Tests for sandbox routes using httpx AsyncClient + app override.

These tests mock the Daytona SDK at the client level so they can run
without a live Daytona instance.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_daytona():
    """Return a mock Daytona client with a configured sandbox."""
    sandbox = MagicMock()
    sandbox.id = "sb-test-123"
    sandbox.name = "test-sandbox"
    sandbox.state = "Running"
    sandbox.language = "python"
    sandbox.image = None
    sandbox.metadata = {"owner": "test"}
    sandbox.created_at = None
    sandbox.error = None

    resources = MagicMock()
    resources.cpu = 2.0
    resources.memory = "4Gi"
    resources.disk = "10Gi"
    sandbox.resources = resources

    sandbox.start.return_value = None
    sandbox.stop.return_value = None
    sandbox.execute.return_value = MagicMock(
        exit_code=0, stdout="hello\n", stderr="", timed_out=False
    )
    sandbox.upload_file.return_value = None
    sandbox.download_file.return_value = b"file content"

    client = MagicMock()
    client.create.return_value = sandbox
    client.get.return_value = sandbox
    client.list.return_value = [sandbox]
    client.delete.return_value = None

    return client


@pytest.fixture()
def app(mock_daytona):
    """Create a test app with mocked Daytona client."""
    with patch("app.daytona_client._client", mock_daytona):
        application = create_app()
        yield application


@pytest.fixture()
async def client(app):
    """Async HTTP client bound to the test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# Health tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_health(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "version" in data


# ---------------------------------------------------------------------------
# Sandbox CRUD tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_sandbox(client: AsyncClient):
    resp = await client.post(
        "/api/v1/sandboxes",
        json={
            "method": "snapshot",
            "language": "python",
            "name": "test-sandbox",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["id"] == "sb-test-123"
    assert data["state"] == "Running"


@pytest.mark.anyio
async def test_list_sandboxes(client: AsyncClient):
    resp = await client.get("/api/v1/sandboxes")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["items"]) == 1


@pytest.mark.anyio
async def test_get_sandbox(client: AsyncClient):
    resp = await client.get("/api/v1/sandboxes/sb-test-123")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "sb-test-123"


@pytest.mark.anyio
async def test_start_sandbox(client: AsyncClient):
    resp = await client.post("/api/v1/sandboxes/sb-test-123/start")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_stop_sandbox(client: AsyncClient):
    resp = await client.post("/api/v1/sandboxes/sb-test-123/stop")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_delete_sandbox(client: AsyncClient):
    resp = await client.delete("/api/v1/sandboxes/sb-test-123")
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Code execution tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_execute_code(client: AsyncClient):
    resp = await client.post(
        "/api/v1/sandboxes/sb-test-123/code",
        json={"code": "print('hello')", "timeout_ms": 10000},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["exit_code"] == 0
    assert "hello" in data["stdout"]


# ---------------------------------------------------------------------------
# File I/O tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_upload_file(client: AsyncClient):
    resp = await client.post(
        "/api/v1/sandboxes/sb-test-123/files/upload?path=/tmp/",
        files={"file": ("test.py", b"print(42)", "text/plain")},
    )
    assert resp.status_code == 204


@pytest.mark.anyio
async def test_read_file(client: AsyncClient):
    resp = await client.get(
        "/api/v1/sandboxes/sb-test-123/files/read?path=/tmp/test.py",
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["content"] == "file content"
