from __future__ import annotations

import os
import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Body, Response
from fastapi.middleware.cors import CORSMiddleware
from httpx import AsyncClient, HTTPStatusError, RequestError

# ---------- Config ----------

COMPOSITE_SERVICE_NAME = "Composite Gateway"

USER_SERVICE_URL    = "http://34.139.134.144:8002"
SPOT_SERVICE_URL    = "https://spot-management-642518168067.us-east1.run.app"
REVIEWS_SERVICE_URL = "https://reviews-api-c73xxvyjwq-ue.a.run.app"

DEFAULT_TIMEOUT = 5.0  # seconds

# ---------- FastAPI app ----------

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

# ---------- CORS ----------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # change to your frontend URL in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------

async def _get_json(client: AsyncClient, url: str):
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


async def _safe_get_reviews(client: AsyncClient, url: str):
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)

    if resp.status_code == 404:
        return []

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
    return {"message": "Welcome to the Composite Gateway API. See /docs."}

# ---------- Composite endpoint ----------
@app.get("/composite/spots/{spot_id}/full", tags=["composite"])
async def get_spot_full(spot_id: str):
    async with AsyncClient() as client:
        try:
            spot_task = _get_json(client, f"{SPOT_SERVICE_URL}/studyspots/{spot_id}")
            reviews_task = _safe_get_reviews(
                client,
                f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}",
            )
            users_task = _get_json(client, f"{USER_SERVICE_URL}/users")

            spot_raw, reviews_raw, users_raw = await asyncio.gather(
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
                detail=f"Downstream error: {e}",
            )

    # ----- Normalize shapes so the frontend gets what it expects -----

    # Spot: unwrap { "data": { ... } } → { ... }
    spot = spot_raw.get("data", spot_raw) if isinstance(spot_raw, dict) else spot_raw

    # Reviews: unwrap { "data": [ ... ] } → [ ... ]
    if isinstance(reviews_raw, dict):
        reviews = reviews_raw.get("data", [])
    elif isinstance(reviews_raw, list):
        reviews = reviews_raw
    else:
        reviews = []

    # Users: if user service also wraps in data, unwrap similarly
    if isinstance(users_raw, dict):
        users = users_raw.get("data", [])
    elif isinstance(users_raw, list):
        users = users_raw
    else:
        users = []

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
    }

# ---------- Proxy endpoints ----------

@app.get("/api/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot(spot_id: str, response: Response):
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}",
            timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    return resp.json()


@app.get("/api/reviews", tags=["proxy"])
async def proxy_list_reviews(
    spot_id: Optional[str] = None,
    response: Response = None,
):
    query = f"?spot_id={spot_id}" if spot_id else ""

    async with AsyncClient() as client:
        resp = await client.get(
            f"{REVIEWS_SERVICE_URL}/reviews{query}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    return resp.json()


@app.get("/api/users/{user_id}", tags=["proxy"])
async def proxy_get_user(user_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/users/{user_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"User service error: {e}")

    response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}

# ---------- Review w/ FK validation ----------

# ---------- Review w/ FK validation ----------
@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(
    payload: dict = Body(...),
    response: Response = None,
):
    """
    Composite FK-validation for creating a review.

    - Accepts: {spot_id, user_id, rating, comment}
    - Validates that spot + user exist via Spot/User services
    - Translates to Reviews API shape and calls:
        POST /review/{spotId}/user/{userId}
      with body: {postDate, review}
    """
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")

    if not spot_id or not user_id:
        raise HTTPException(status_code=400, detail="spot_id and user_id required")

    async with AsyncClient() as client:
        # 1) Parallel FK checks
        spot_resp, user_resp = await asyncio.gather(
            client.get(
                f"{SPOT_SERVICE_URL}/studyspots/{spot_id}",
                timeout=DEFAULT_TIMEOUT,
            ),
            client.get(
                f"{USER_SERVICE_URL}/users/{user_id}",
                timeout=DEFAULT_TIMEOUT,
            ),
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

        # 2) Map composite payload -> Reviews microservice schema
        review_payload = {
            # If caller doesn’t send postDate, use "now"
            "postDate": payload.get("postDate")
            or datetime.now(timezone.utc).isoformat(),
            # Use comment as the review text
            "review": payload.get("comment") or payload.get("review") or "",
        }

        # 3) Call the real Reviews endpoint:
        #    POST /review/{spotId}/user/{userId}
        create_resp = await client.post(
            f"{REVIEWS_SERVICE_URL}/review/{spot_id}/user/{user_id}",
            json=review_payload,
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = create_resp.status_code
    try:
        return create_resp.json()
    except ValueError:
        # fallback if reviews service ever returns non-JSON
        return {"detail": create_resp.text}

# ---------- Async geocode endpoints ----------

@app.post("/api/spots/{spot_id}/geocode", tags=["async"])
async def start_geocode_job(spot_id: str, response: Response):
    async with AsyncClient() as client:
        resp = await client.post(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code

    try:
        job_json = resp.json()
    except ValueError:
        return {"detail": f"Unexpected response: {resp.text}"}

    job_id = job_json.get("job_id") or job_json.get("id")
    if job_id:
        response.headers["Location"] = f"/api/tasks/{job_id}"

    return job_json


@app.get("/api/tasks/{task_id}", tags=["async"])
async def get_task_status(task_id: str, response: Response):
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/jobs/{task_id}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    return resp.json()

# ---------- Local entrypoint ----------

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("FASTAPIPORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
