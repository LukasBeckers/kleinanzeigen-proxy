"""One-shot cache invalidator: drop ListingVersion rows whose ``image_urls``
JSON array has at most one element.

Why:
    Pre-fix the upstream scraper (``ebay-kleinanzeigen-api``) was capturing
    only the cover image per listing — every cached detail came in with
    ``image_urls = ["<cover>"]``.  After the scraper fix lands and the
    scraper container is rebuilt, the proxy keeps serving the stale
    single-URL arrays for any adid already in the cache (no TTL).  Run
    this script after deploying the fix so the next ``/inserate-detailed
    -cached`` access re-fetches the full gallery upstream.

Usage:
    # Inside the running proxy container (the DB path matches config.py):
    docker compose exec kleinanzeigen-proxy \\
        python scripts/invalidate_single_image_cache.py

    # Dry-run first to see how many rows would be deleted:
    docker compose exec kleinanzeigen-proxy \\
        python scripts/invalidate_single_image_cache.py --dry-run

Idempotent: re-running after the fix is harmless once the cache has been
re-populated (no rows will match because every freshly-fetched entry has
multiple images).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Run from the project root so ``database`` / ``models`` resolve when
# the script lives one level down under ``scripts/``.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import delete, func, select  # noqa: E402

from database import async_session  # noqa: E402
from models import ListingVersion  # noqa: E402


def _array_len(image_urls_json: str | None) -> int:
    """Length of the stored JSON array; defensive against legacy NULL /
    malformed entries (treated as length 0)."""
    if not image_urls_json:
        return 0
    try:
        v = json.loads(image_urls_json)
        return len(v) if isinstance(v, list) else 0
    except (json.JSONDecodeError, TypeError):
        return 0


async def main(dry_run: bool) -> None:
    async with async_session() as session:
        # Two-pass approach (SQLite ``json_array_length`` is available but
        # not consistently exposed through aiosqlite; doing the count in
        # Python keeps the script portable).
        result = await session.execute(select(ListingVersion))
        rows = result.scalars().all()
        targets = [r for r in rows if _array_len(r.image_urls) <= 1]

        total = await session.scalar(select(func.count()).select_from(ListingVersion))
        print(f"ListingVersion rows total: {total}")
        print(f"Targets (image_urls array length ≤ 1): {len(targets)}")

        if not targets:
            print("Nothing to invalidate.")
            return

        if dry_run:
            print("Dry-run; no rows deleted.")
            return

        target_ids = [r.id for r in targets]
        # Batch the delete so SQLite doesn't choke on huge IN clauses.
        BATCH = 500
        deleted = 0
        for i in range(0, len(target_ids), BATCH):
            batch = target_ids[i:i + BATCH]
            await session.execute(
                delete(ListingVersion).where(ListingVersion.id.in_(batch))
            )
            deleted += len(batch)
        await session.commit()
        print(f"Deleted {deleted} ListingVersion rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be deleted without committing")
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
