import asyncio
import logging
import mimetypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import async_session
from models import Image, new_uuid

logger = logging.getLogger(__name__)


@dataclass
class ImageJob:
    adid: str
    listing_id: str
    version_id: str
    original_url: str


class ImageWorker:
    def __init__(self):
        self.queue: asyncio.Queue[ImageJob | None] = asyncio.Queue()
        self.storage_path = Path(settings.image_storage_path)
        self.concurrency = settings.image_download_concurrency
        self.client: httpx.AsyncClient | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)

        for i in range(self.concurrency):
            task = asyncio.create_task(self._worker(i))
            self._tasks.append(task)

        # Re-enqueue pending images from DB
        await self._recover_pending()

        logger.info(f"ImageWorker started with {self.concurrency} workers")

    async def stop(self):
        for _ in self._tasks:
            await self.queue.put(None)
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.client:
            await self.client.aclose()
        logger.info("ImageWorker stopped")

    async def _recover_pending(self):
        async with async_session() as session:
            result = await session.execute(
                select(Image).where(Image.status == "pending")
            )
            pending = result.scalars().all()
            for img in pending:
                # We need the adid from the listing
                from models import Listing
                listing_result = await session.execute(
                    select(Listing.adid).where(Listing.id == img.listing_id)
                )
                adid = listing_result.scalar_one_or_none()
                if adid:
                    self.queue.put_nowait(ImageJob(
                        adid=adid,
                        listing_id=img.listing_id,
                        version_id=img.version_id,
                        original_url=img.original_url,
                    ))
            if pending:
                logger.info(f"Re-enqueued {len(pending)} pending image downloads")

    def enqueue(self, adid: str, listing_id: str, version_id: str, urls: list[str]):
        for url in urls:
            self.queue.put_nowait(ImageJob(adid, listing_id, version_id, url))

    async def create_pending_records(
        self, session: AsyncSession, listing_id: str, version_id: str, urls: list[str]
    ) -> None:
        """Create pending image records in DB before enqueuing downloads."""
        for url in urls:
            # Check if this image already exists for this listing
            result = await session.execute(
                select(Image).where(
                    Image.listing_id == listing_id,
                    Image.original_url == url,
                    Image.status == "downloaded",
                )
            )
            existing = result.scalar_one_or_none()
            if existing:
                continue

            image = Image(
                id=new_uuid(),
                listing_id=listing_id,
                version_id=version_id,
                original_url=url,
                status="pending",
            )
            session.add(image)

    async def _worker(self, worker_id: int):
        while True:
            job = await self.queue.get()
            if job is None:
                self.queue.task_done()
                break
            try:
                await self._download(job)
            except Exception as e:
                logger.error(f"Worker {worker_id} error downloading {job.original_url}: {e}")
            finally:
                self.queue.task_done()

    async def _download(self, job: ImageJob):
        async with async_session() as session:
            # Find the pending record
            result = await session.execute(
                select(Image).where(
                    Image.listing_id == job.listing_id,
                    Image.original_url == job.original_url,
                    Image.status == "pending",
                )
            )
            image = result.scalar_one_or_none()
            if not image:
                return

            try:
                response = await self.client.get(job.original_url)
                response.raise_for_status()

                content_type = response.headers.get("content-type", "image/jpeg")
                ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) or ".jpg"

                # Store under /data/images/{adid}/{image_uuid}{ext}
                listing_dir = self.storage_path / job.adid
                listing_dir.mkdir(parents=True, exist_ok=True)

                filename = f"{image.id}{ext}"
                filepath = listing_dir / filename
                filepath.write_bytes(response.content)

                image.local_path = f"{job.adid}/{filename}"
                image.downloaded_at = datetime.now(timezone.utc)
                image.file_size = len(response.content)
                image.content_type = content_type.split(";")[0].strip()
                image.status = "downloaded"

                await session.commit()
                logger.info(f"Downloaded image {job.original_url} -> {image.local_path}")

            except Exception as e:
                image.status = "failed"
                await session.commit()
                logger.error(f"Failed to download {job.original_url}: {e}")
