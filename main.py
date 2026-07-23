import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

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
        "endpoints": [
            "/inserate",
            "/inserat/{id}",
            "/inserate-detailed",
            "/inserate-detailed-cached",
            "/upstream-stats",
            "/upstream-seed",
        ],
    }


@app.get("/upstream-stats")
async def upstream_stats(request: Request):
    """Sliding-window success/failure counts and current pick probabilities.

    Used by the kleinanzeigen-hunter admin panel to visualize load-balancer
    health per downstream scraper worker.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    return client.stats()


class UpstreamSeedBody(BaseModel):
    url: str = Field(..., min_length=1, description="Exact upstream base URL")
    probability: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Target pick probability in 5% steps (0, 0.05, …, 1.0)",
    )


@app.post("/upstream-seed")
async def upstream_seed(body: UpstreamSeedBody, request: Request):
    """Reseed one upstream's sliding window to a target pick probability.

    Fills the window with successes/failures so success-weighted selection
    assigns approximately ``probability`` to this worker. Used by the hunter
    admin panel (5% step control) to re-introduce a recovered scraper without
    waiting for a long failure history to age out.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        return client.seed_pick_probability(body.url, body.probability)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
