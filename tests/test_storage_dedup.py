"""Hash-based dedup tests for the proxy's storage layer.

Spec (from the proxy README):
- Same ``adid`` + same content → no new version, ``last_seen_at`` updated.
- Same ``adid`` + different content → new ``listing_versions`` row,
  ``listings.current_version_id`` advanced.
- The volatile ``views`` field is *excluded* from the hash so a view-count
  bump alone does NOT create a new version.
"""
import asyncio
import sys
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import storage
from models import Listing, ListingVersion


def _search_result(adid: str, title: str = "T", price: str = "100", description: str = "D") -> dict:
    return {
        "adid": adid, "title": title, "price": price,
        "url": f"https://ka.de/x/{adid}", "description": description,
    }


class TestFirstInsert:
    async def test_creates_listing_and_version(self, session_factory):
        async with session_factory() as s:
            listing_id, version_id, is_new, image_urls = await storage.store_listing(
                s, _search_result("a1"), source="search"
            )
            await s.commit()
            assert listing_id is not None
            assert version_id is not None
            assert is_new is True

        async with session_factory() as s:
            listing = (await s.execute(select(Listing).where(Listing.adid == "a1"))).scalar_one()
            assert listing.current_version_id == version_id
            versions = (await s.execute(select(ListingVersion).where(ListingVersion.listing_id == listing.id))).scalars().all()
            assert len(versions) == 1


class TestUnchangedContent:
    async def test_second_call_updates_last_seen_only(self, session_factory):
        async with session_factory() as s:
            await storage.store_listing(s, _search_result("a2"), source="search")
            await s.commit()
        async with session_factory() as s:
            listing = (await s.execute(select(Listing).where(Listing.adid == "a2"))).scalar_one()
            v0 = listing.current_version_id
            first_seen = listing.last_seen_at

        async with session_factory() as s:
            listing_id, version_id, is_new, _ = await storage.store_listing(
                s, _search_result("a2"), source="search"
            )
            await s.commit()
            assert is_new is False, "identical content must not create a new version"
            assert version_id is None

        async with session_factory() as s:
            refreshed = (await s.execute(select(Listing).where(Listing.adid == "a2"))).scalar_one()
            assert refreshed.current_version_id == v0, "current_version_id must not change"
            assert refreshed.last_seen_at >= first_seen
            versions = (await s.execute(select(ListingVersion).where(ListingVersion.listing_id == refreshed.id))).scalars().all()
            assert len(versions) == 1, "exactly one version should exist"


class TestChangedContent:
    async def test_price_change_creates_new_version(self, session_factory):
        async with session_factory() as s:
            await storage.store_listing(s, _search_result("a3", price="100"), source="search")
            await s.commit()
        async with session_factory() as s:
            lid, vid, is_new, _ = await storage.store_listing(
                s, _search_result("a3", price="80"), source="search"
            )
            await s.commit()
            assert is_new is True
            assert vid is not None

        async with session_factory() as s:
            listing = (await s.execute(select(Listing).where(Listing.adid == "a3"))).scalar_one()
            versions = (await s.execute(select(ListingVersion).where(ListingVersion.listing_id == listing.id))).scalars().all()
            assert len(versions) == 2
            assert listing.current_version_id == versions[-1].id or listing.current_version_id == versions[0].id
            # current_version_id should point at the newest (by hash)
            current = next(v for v in versions if v.id == listing.current_version_id)
            assert current.price_amount == "80"


class TestConcurrentInsertRace:
    async def test_parallel_inserts_same_adid_do_not_raise(self, session_factory):
        """Concurrent cache misses for the same adid must not 500 on UNIQUE."""
        detail = {
            "id": "race1",
            "title": "Moped",
            "description": "Fast",
            "url": "https://ka.de/x/race1",
            "price": {"amount": "200", "currency": "€", "negotiable": False},
            "views": "1",
            "images": ["https://img.example/1.jpg"],
            "details": {},
            "features": [],
            "seller": {},
            "extra_info": {},
        }

        async def _store():
            async with session_factory() as s:
                result = await storage.store_listing(s, detail, source="detail")
                await s.commit()
                return result

        results = await asyncio.gather(*[_store() for _ in range(8)])
        for listing_id, _version_id, _is_new, _urls in results:
            assert listing_id is not None

        async with session_factory() as s:
            listings = (await s.execute(select(Listing).where(Listing.adid == "race1"))).scalars().all()
            assert len(listings) == 1


class TestViewsExcludedFromHash:
    async def test_views_bump_alone_does_not_create_version(self, session_factory):
        """``views`` changes every hour — including it in the hash would
        pointlessly create new versions.  Spec excludes it."""
        detail_1 = {
            "id": "d1", "title": "Moped", "description": "Fast",
            "url": "https://ka.de/x/d1",
            "price": {"amount": "200", "currency": "€", "negotiable": False},
            "views": "10",
            "images": [], "details": {}, "features": [], "seller": {}, "extra_info": {},
        }
        detail_2 = {**detail_1, "views": "100"}

        async with session_factory() as s:
            await storage.store_listing(s, detail_1, source="detail")
            await s.commit()
        async with session_factory() as s:
            lid, vid, is_new, _ = await storage.store_listing(s, detail_2, source="detail")
            await s.commit()
            assert is_new is False, "views bump alone must not trigger a new version"

        async with session_factory() as s:
            listing = (await s.execute(select(Listing).where(Listing.adid == "d1"))).scalar_one()
            versions = (await s.execute(select(ListingVersion).where(ListingVersion.listing_id == listing.id))).scalars().all()
            assert len(versions) == 1
