"""Proxy test fixtures.

Each test gets its own in-memory SQLite DB (engine + session) and a
fresh FastAPI app wired to it.  The upstream HTTP client is replaced
with an ``httpx.MockTransport`` whose handler the test can override.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import AsyncIterator, Callable

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Proxy app modules live at the project root (no ``src/`` layout).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest_asyncio.fixture
async def engine():
    from database import Base
    import models  # noqa: F401

    e = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with e.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield e
    await e.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def upstream_handler():
    """Default handler returns an empty-but-valid response.  Override per-test
    by re-assigning ``handler.handler`` to a callable."""
    class Container:
        handler: Callable[[httpx.Request], httpx.Response] = staticmethod(
            lambda req: httpx.Response(200, json={"success": True, "results": [],
                                                  "unique_results": 0,
                                                  "time_taken": 0.0,
                                                  "performance_metrics": {},
                                                  "browser_metrics": {}})
        )
    return Container()


@pytest_asyncio.fixture
async def client(monkeypatch, engine, session_factory, upstream_handler) -> AsyncIterator[AsyncClient]:
    """HTTP client driving the FastAPI app."""
    import database as db_module
    monkeypatch.setattr(db_module, "engine", engine)
    monkeypatch.setattr(db_module, "async_session", session_factory)

    # Replace the upstream httpx.AsyncClient with one bound to a MockTransport
    from main import app
    # Prevent real startup (which would try to reach host.docker.internal:8000)
    def _handler(req: httpx.Request) -> httpx.Response:
        return upstream_handler.handler(req)

    real_mock = httpx.AsyncClient(
        base_url="http://upstream",
        transport=httpx.MockTransport(_handler),
        timeout=30.0,
    )

    class _TupleReturningClient:
        """Adapter so tests see the same (response, upstream) interface as production."""
        def __init__(self, inner):
            self._inner = inner

        async def get(self, path, *, params=None):
            resp = await self._inner.get(path, params=params)
            return (resp, "http://mock-upstream")

        async def aclose(self):
            await self._inner.aclose()

    mock_upstream = _TupleReturningClient(real_mock)

    class _NoopImageWorker:
        queue = None
        async def start(self): pass
        async def stop(self): pass
        def enqueue(self, *a, **k): pass
        async def create_pending_records(self, *a, **k): pass

    app.state.upstream_client = mock_upstream
    app.state.image_worker = _NoopImageWorker()

    # Bypass the lifespan (which would try to connect to the real upstream)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testproxy") as ac:
        yield ac
    await real_mock.aclose()
