"""Duplicate pending image rows must not abort the download.

The worker used ``scalar_one_or_none`` for (listing, url, pending). A
second search inserted another pending row for the same URL, and the
download then logged ``Multiple rows were found when one or none was
required`` and left the file unsaved.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from image_worker import ImageJob, ImageWorker
from models import Image, Listing, ListingVersion, new_uuid


class _Response:
    def __init__(self, content: bytes, status_error: Exception | None = None):
        self.content = content
        self.headers = {"content-type": "image/jpeg"}
        self._error = status_error

    def raise_for_status(self):
        if self._error:
            raise self._error


class _Client:
    def __init__(self, response: _Response):
        self.response = response
        self.calls = 0

    async def get(self, url: str):
        self.calls += 1
        return self.response


async def _listing(session_factory) -> tuple[str, str]:
    listing_id = new_uuid()
    version_id = new_uuid()
    async with session_factory() as session:
        session.add(Listing(id=listing_id, adid="ad-1"))
        session.add(ListingVersion(id=version_id, listing_id=listing_id, title="t"))
        await session.commit()
    return listing_id, version_id


@pytest.mark.asyncio
async def test_second_pending_insert_is_skipped(session_factory):
    listing_id, version_id = await _listing(session_factory)
    worker = ImageWorker()
    url = "https://img.example/a.jpg"
    async with session_factory() as session:
        await worker.create_pending_records(session, listing_id, version_id, [url])
        await worker.create_pending_records(session, listing_id, version_id, [url])
        await session.commit()
        rows = (await session.scalars(select(Image).where(Image.original_url == url))).all()
    assert len(rows) == 1
    assert rows[0].status == "pending"


@pytest.mark.asyncio
async def test_duplicate_pending_rows_download_once(monkeypatch, session_factory, tmp_path):
    import image_worker as image_worker_module

    monkeypatch.setattr(image_worker_module, "async_session", session_factory)
    listing_id, version_id = await _listing(session_factory)
    url = "https://img.example/dup.jpg"
    async with session_factory() as session:
        session.add(Image(
            id=new_uuid(), listing_id=listing_id, version_id=version_id,
            original_url=url, status="pending",
        ))
        session.add(Image(
            id=new_uuid(), listing_id=listing_id, version_id=version_id,
            original_url=url, status="pending",
        ))
        await session.commit()

    client = _Client(_Response(b"jpeg-bytes"))
    worker = ImageWorker()
    worker.client = client
    worker.storage_path = tmp_path
    await worker._download(ImageJob("ad-1", listing_id, version_id, url))

    assert client.calls == 1
    async with session_factory() as session:
        rows = (await session.scalars(select(Image).where(Image.original_url == url))).all()
    assert len(rows) == 2
    assert {row.status for row in rows} == {"downloaded"}
    assert len({row.local_path for row in rows}) == 1
    saved = tmp_path / "ad-1" / Path(rows[0].local_path).name
    assert saved.read_bytes() == b"jpeg-bytes"
