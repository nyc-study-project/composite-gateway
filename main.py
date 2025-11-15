from __future__ import annotations

import os
import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Body, Response
from httpx import AsyncClient, HTTPStatusError, RequestError

COMPOSITE_SERVICE_NAME = "Composite Gateway"

USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8001")
SPOT_SERVICE_URL = os.getenv("SPOT_SERVICE_URL", "http://localhost:8002")
REVIEWS_SERVICE_URL = os.getenv("REVIEWS_SERVICE_URL", "http://localhost:8003")

DEFAULT_TIMEOUT = 5.0  # seconds

app = FastAPI(
    title="Composite Gateway Service",
    description=(
        "Aggregates Spot, User, and Reviews microservices into a unified API.\n\n"
        "Key features:\n"
        "- /composite/spots/{id}/full → spot + reviews + users (parallel fan-out)\n"
        "- FK validation in POST /api/reviews\n"
        "- Async geocode 202 flow via POST /api/spots/{id}/geocode + GET /api/tasks/{task_id}\n"
    ),
    version="1.0.0",
)


# ---------- Helpers ----------

async def _get_json(client: AsyncClient, url: str):
    """Helper: GET a URL and return JSON or raise for HTTP error."""
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------- Basic endpoints ----------

@app.get("/health", tags=["internal"])
async def health():
    return {
        "status": "ok",
        "service": COMPOSITE_SERVICE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", tags=["internal"])
async def root():
    return {
        "message": "Welcome to the Composite Gateway API. See /docs for OpenAPI UI."
    }


# ---------- Composite endpoint (threaded fan-out) ----------

@app.get("/composite/spots/{spot_id}/full", tags=["composite"])
async def get_spot_full(spot_id: str):
    """
    Composite endpoint that aggregates data from all three microservices.

    - Spot service:    GET {SPOT_SERVICE_URL}/studyspots/{spot_id}
    - Reviews service: GET {REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}
    - User service:    GET {USER_SERVICE_URL}/users  (example index; can be refined later)
    """
    async with AsyncClient() as client:
        try:
            spot_task = _get_json(client, f"{SPOT_SERVICE_URL}/studyspots/{spot_id}")
            reviews_task = _get_json(
                client, f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}"
            )
            users_task = _get_json(client, f"{USER_SERVICE_URL}/users")
            spot, reviews, users = await asyncio.gather(
                spot_task, reviews_task, users_task
            )
        except HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.text,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Downstream request error: {e}",
            )

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
    }


# ---------- Façade / proxy endpoints ----------

@app.get("/api/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot(spot_id: str, response: Response):
    """
    Façade that proxies GET studyspot by ID to the Spot service.
    Also forwards the ETag header so clients can do conditional requests.
    """
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag
    return resp.json()


@app.get("/api/reviews", tags=["proxy"])
async def proxy_list_reviews(spot_id: Optional[str] = None, response: Response = None):
    """
    Façade for listing reviews.

    Optional query:
      - /api/reviews?spot_id={id} to filter by studyspot.
    """
    query = f"?spot_id={spot_id}" if spot_id else ""
    async with AsyncClient() as client:
        resp = await client.get(
            f"{REVIEWS_SERVICE_URL}/reviews{query}", timeout=DEFAULT_TIMEOUT
        )

    if response is not None:
        response.status_code = resp.status_code
    return resp.json()


@app.get("/api/users/{user_id}", tags=["proxy"])
async def proxy_get_user(user_id: str, response: Response):
    """Façade that proxies GET user by ID to the User service."""
    async with AsyncClient() as client:
        resp = await client.get(
            f"{USER_SERVICE_URL}/users/{user_id}", timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    return resp.json()


# ---------- FK-validated review creation ----------

@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(payload: dict = Body(...), response: Response = None):
    """
    Create a review, but first validate that the referenced spot_id and user_id exist.

    Steps:
      1. Check payload has spot_id and user_id.
      2. GET Spot and User from their respective services.
      3. If both exist, forward POST to reviews service.
    """
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")

    if not spot_id or not user_id:
        raise HTTPException(
            status_code=400, detail="spot_id and user_id are required"
        )

    async with AsyncClient() as client:
        # parallel FK validation
        spot_resp, user_resp = await asyncio.gather(
            client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT),
            client.get(f"{USER_SERVICE_URL}/users/{user_id}", timeout=DEFAULT_TIMEOUT),
        )

        if spot_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Spot not found")
        if user_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found")

        if spot_resp.status_code >= 400:
            raise HTTPException(
                status_code=spot_resp.status_code,
                detail=spot_resp.text,
            )
        if user_resp.status_code >= 400:
            raise HTTPException(
                status_code=user_resp.status_code,
                detail=user_resp.text,
            )

        # If both exist, create the review
        create_resp = await client.post(
            f"{REVIEWS_SERVICE_URL}/reviews", json=payload, timeout=DEFAULT_TIMEOUT
        )

    if response is not None:
        response.status_code = create_resp.status_code
    # assume JSON from reviews service
    return create_resp.json()


# ---------- Async geocode 202 + polling ----------

@app.post("/api/spots/{spot_id}/geocode", tags=["async"])
async def start_geocode_job(spot_id: str, response: Response):
    """
    Start an asynchronous geocoding job for a studyspot.

    Proxies to:
      POST {SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode

    If Spot service returns:
      { "message": "...", "job_id": "..." } with 202 + Location: /jobs/{job_id}

    This endpoint:
      - returns 202
      - rewrites Location to /api/tasks/{job_id}
    """
    async with AsyncClient() as client:
        resp = await client.post(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode",
            timeout=DEFAULT_TIMEOUT,
        )

    # bubble up status (should be 202 on success)
    response.status_code = resp.status_code

    job_json = {}
    try:
        job_json = resp.json()
    except ValueError:
        # non-JSON body — just surface raw text
        return {"detail": f"Unexpected response from Spot service: {resp.text}"}

    job_id = job_json.get("job_id") or job_json.get("id")

    if job_id:
        # Rewrite Location so clients only talk to composite
        response.headers["Location"] = f"/api/tasks/{job_id}"

    return job_json


@app.get("/api/tasks/{task_id}", tags=["async"])
async def get_task_status(task_id: str, response: Response):
    """
    Poll the status of an async geocode job.

    Proxies to:
      GET {SPOT_SERVICE_URL}/jobs/{task_id}
    """
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/jobs/{task_id}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    return resp.json()


# ---------- Local dev entrypoint ----------

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FASTAPIPORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
