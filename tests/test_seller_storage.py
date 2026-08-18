"""Seller archive: first-class seller rows, listing FK, cache-once profile."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import storage
from models import Listing, Seller, SellerVersion


def _listing_detail(adid: str, seller: dict) -> dict:
    return {
        "id": adid,
        "title": f"Ad {adid}",
        "status": "active",
        "price": {"amount": "10", "currency": "€", "negotiable": False},
        "location": {"zip": "10115", "city": "Berlin", "state": "BE"},
        "description": "d",
        "images": [],
        "details": {},
        "features": [],
        "seller": seller,
        "extra_info": {},
    }


def _sidebar_seller(user_id: str = "1", name: str = "André") -> dict:
    return {
        "id": user_id,
        "user_id": user_id,
        "name": name,
        "type": "private",
        "since": "01.09.2009",
        "badges": [],
        "url": f"https://www.kleinanzeigen.de/s-bestandsliste.html?userId={user_id}",
        "shop_url": None,
        "response_time": None,
        "response_time_hours": None,
        "followers": None,
        "ads_online": None,
        "ads_total": None,
    }


def _profile_seller(user_id: str = "1") -> dict:
    s = _sidebar_seller(user_id)
    s["badges"] = ["TOP Zufriedenheit"]
    s["response_time"] = "Antwortet in der Regel innerhalb von 3 Stunden"
    s["response_time_hours"] = 3
    s["followers"] = 27
    return s


class TestStoreSeller:
    async def test_first_profile_creates_seller_and_version(self, session_factory):
        async with session_factory() as s:
            sid, vid, is_new = await storage.store_seller(
                s, _profile_seller(), source="profile"
            )
            await s.commit()
            assert sid is not None
            assert vid is not None
            assert is_new is True

        async with session_factory() as s:
            row = (await s.execute(select(Seller).where(Seller.user_id == "1"))).scalar_one()
            assert row.has_profile is True
            versions = (
                await s.execute(select(SellerVersion).where(SellerVersion.seller_id == row.id))
            ).scalars().all()
            assert len(versions) == 1
            assert versions[0].followers == 27

    async def test_identical_profile_does_not_version(self, session_factory):
        async with session_factory() as s:
            await storage.store_seller(s, _profile_seller(), source="profile")
            await s.commit()
        async with session_factory() as s:
            sid, vid, is_new = await storage.store_seller(
                s, _profile_seller(), source="profile"
            )
            await s.commit()
            assert is_new is False
            assert vid is None

        async with session_factory() as s:
            versions = (await s.execute(select(SellerVersion))).scalars().all()
            assert len(versions) == 1

    async def test_listing_sidebar_does_not_overwrite_profile(self, session_factory):
        async with session_factory() as s:
            await storage.store_seller(s, _profile_seller(), source="profile")
            await s.commit()
        async with session_factory() as s:
            await storage.store_seller(s, _sidebar_seller(name="Changed"), source="listing")
            await s.commit()

        async with session_factory() as s:
            row = (await s.execute(select(Seller).where(Seller.user_id == "1"))).scalar_one()
            assert row.has_profile is True
            ver = (
                await s.execute(
                    select(SellerVersion).where(SellerVersion.id == row.current_version_id)
                )
            ).scalar_one()
            assert ver.name == "André"
            assert ver.followers == 27
            versions = (await s.execute(select(SellerVersion))).scalars().all()
            assert len(versions) == 1


class TestListingSellerLink:
    async def test_detail_store_links_listing_to_seller(self, session_factory):
        async with session_factory() as s:
            lid, _, _, _ = await storage.store_listing(
                s, _listing_detail("ad-1", _sidebar_seller()), source="detail"
            )
            await s.commit()

        async with session_factory() as s:
            listing = (await s.execute(select(Listing).where(Listing.adid == "ad-1"))).scalar_one()
            assert listing.seller_id is not None
            seller = await s.get(Seller, listing.seller_id)
            assert seller.user_id == "1"
            assert seller.has_profile is False

    async def test_cached_seller_none_until_profile(self, session_factory):
        async with session_factory() as s:
            await storage.store_seller(s, _sidebar_seller(), source="listing")
            await s.commit()
            cached = await storage.get_cached_seller(s, "1")
            assert cached is None

        async with session_factory() as s:
            await storage.store_seller(s, _profile_seller(), source="profile")
            await s.commit()
            cached = await storage.get_cached_seller(s, "1")
            assert cached is not None
            assert cached["followers"] == 27
            assert cached["id"] == "1"
