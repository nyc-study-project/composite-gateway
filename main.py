from __future__ import annotations

import os
import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Body, Response
from fastapi.middleware.cors import CORSMiddleware
from httpx import AsyncClient, HTTPStatusError, RequestError

from google.cloud import pubsub_v1

# ---------- Config ----------

COMPOSITE_SERVICE_NAME = "Composite Gateway"

USER_SERVICE_URL = "http://34.139.134.144:8002"
SPOT_SERVICE_URL = "https://spot-management-642518168067.us-east1.run.app"
REVIEWS_SERVICE_URL = "https://reviews-api-c73xxvyjwq-ue.a.run.app"

DEFAULT_TIMEOUT = 5.0  # seconds

GCP_PROJECT_ID = "study-spot-nyc"
PUBSUB_TOPIC_ID = "composite-events"

# ---------- Pub/Sub setup ----------

publisher = pubsub_v1.PublisherClient()
TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)


def emit_event(event_type: str, data: dict) -> None:
    """
    Fire-and-forget helper to publish a small JSON event to Pub/Sub.

    This is intentionally simple: we don't wait on the future or handle retries,
    since for the project we just need to demonstrate event emission.
    """
    envelope = {
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    message_bytes = json.dumps(envelope).encode("utf-8")
    try:
        publisher.publish(TOPIC_PATH, message_bytes)
    except Exception as exc:  # noqa: BLE001
        # We *never* fail the main request just because Pub/Sub failed.
        print(f"[WARN] Failed to publish event to Pub/Sub: {exc}")


# ---------- FastAPI app ----------

app = FastAPI(
    title="Composite Gateway Service",
    description=(
        "Aggregates Spot, User, and Reviews microservices into a unified API.\n\n"
        "Key features:\n"
        "- /composite/spots/{id}/full → spot + reviews + users (parallel fan-out)\n"
        "- FK validation in POST /api/reviews (composite write)\n"
        "- Async geocode 202 flow via POST /api/spots/{id}/geocode + GET /api/tasks/{task_id}\n"
        "- Pub/Sub events on review creation (Cloud Run function listener)\n"
    ),
    version="1.0.0",
)

# ---------- CORS ----------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod, lock this to your frontend origin
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
    """
    GET reviews for a spot.
    If reviews service returns 404, treat as "no reviews yet" → [].
    """
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
    """
    Fan-out read:

    - Spot service:    GET {SPOT_SERVICE_URL}/studyspots/{spot_id}
    - Reviews service: GET {REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}
    - User service:    GET {USER_SERVICE_URL}/users
    """
    async with AsyncClient() as client:
        try:
            spot_task = _get_json(client, f"{SPOT_SERVICE_URL}/studyspots/{spot_id}")
            reviews_task = _safe_get_reviews(
                client, f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}"
            )
            users_task = _get_json(client, f"{USER_SERVICE_URL}/users")

            spot_raw, reviews, users = await asyncio.gather(
                spot_task, reviews_task, users_task
            )

        except HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"Downstream error: {e}")

    # Spot service wraps the object under "data"
    spot = spot_raw.get("data", spot_raw)

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
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


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

    if response is not None:
        response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


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


# ---------- Review w/ FK validation + Pub/Sub event ----------


@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(
    payload: dict = Body(...),
    response: Response = None,
):
    """
    Composite FK-validation for creating a review.

    - Accepts: {spot_id, user_id, rating, comment}
    - Validates that spot + user exist (spot + user services)
    - Translates to Reviews API schema:
        POST /review/{spotId}/user/{userId}
      with body: {postDate, review}
    - Emits Pub/Sub event "review_created" for Cloud Run listener.
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
            "postDate": payload.get("postDate")
            or datetime.now(timezone.utc).isoformat(),
            "review": payload.get("comment") or payload.get("review") or "",
        }

        # 3) Call the real Reviews endpoint:
        #    POST /review/{spotId}/user/{userId}
        create_resp = await client.post(
            f"{REVIEWS_SERVICE_URL}/review/{spot_id}/user/{user_id}",
            json=review_payload,
            timeout=DEFAULT_TIMEOUT,
        )

    if response is not None:
        response.status_code = create_resp.status_code

    # 4) Emit Pub/Sub event (fire-and-forget; don't break request if it fails)
    emit_event(
        "review_created",
        {
            "spot_id": spot_id,
            "user_id": user_id,
            "rating": payload.get("rating"),
            "comment": payload.get("comment"),
        },
    )

    try:
        return create_resp.json()
    except ValueError:
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

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/auth/callback/google", tags=["proxy"])
async def google_oauth_callback(request: Request, response: Response):
    """
    Proxy endpoint that forwards the Google OAuth callback request to the User Management service.
    This will be used to handle the redirect URI issue when the User Management service is on a VM with an IP.
    """
    user_management_url = "http://34.139.134.144:8002/auth/callback/google"  # your User Management service callback URL
    async with httpx.AsyncClient() as client:
        resp = await client.get(user_management_url, params=request.query_params)
        response.status_code = resp.status_code
        response.headers.update(resp.headers)
        return resp.text


# ---------- Local entrypoint (for dev) ----------

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FASTAPIPORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
