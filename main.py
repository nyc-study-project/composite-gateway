from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException

COMPOSITE_SERVICE_NAME = "Composite Gateway"
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8001")
SPOT_SERVICE_URL = os.getenv("SPOT_SERVICE_URL", "http://localhost:8002")
REVIEWS_SERVICE_URL = os.getenv("REVIEWS_SERVICE_URL", "http://localhost:8003")

app = FastAPI(
    title="Composite Gateway Service",
    description="Aggregates Spot, User, and Reviews microservices into a unified API.",
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": COMPOSITE_SERVICE_NAME,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Composite Gateway API. See /docs for OpenAPI UI."
    }


@app.get("/composite/spots/{spot_id}/full")
async def get_spot_full(spot_id: str):
    """
    Placeholder composite endpoint.
    Later, this should:
      - Fetch spot details from Spot service
      - Fetch reviews + ratings from Reviews service
      - Optionally fetch user info
    For now, we just return a stubbed payload so the route exists and CI can test it.
    """
    return {
        "spot_id": spot_id,
        "spot": {"placeholder": True},
        "reviews": [],
        "rating": None,
        "note": "Composite aggregation logic to be implemented in Sprint 2.",
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FASTAPIPORT", 8004))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

