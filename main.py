import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import settings
from database import init_db
from image_worker import ImageWorker
from routers import inserate, inserat, inserate_detailed
from upstream import LoadBalancedClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    urls = settings.upstream_urls
    logger.info(f"Connecting to upstream API(s): {', '.join(urls)}")

    await init_db()
    logger.info("Database initialized")

    app.state.upstream_client = LoadBalancedClient(urls, timeout=300.0)

    app.state.image_worker = ImageWorker()
    await app.state.image_worker.start()

    yield

    # Shutdown
    await app.state.image_worker.stop()
    await app.state.upstream_client.aclose()
    logger.info("Shutdown complete")


app = FastAPI(title="Kleinanzeigen Proxy", version="1.0.0", lifespan=lifespan)

app.include_router(inserate.router)
app.include_router(inserat.router)
app.include_router(inserate_detailed.router)


@app.get("/")
async def root():
    return {
        "service": "kleinanzeigen-proxy",
        "upstreams": settings.upstream_urls,
        "endpoints": ["/inserate", "/inserat/{id}", "/inserate-detailed", "/inserate-detailed-cached"],
    }
