import os
import sys

# Ensure composite-gateway project root is on sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import respx
import httpx
from fastapi.testclient import TestClient

from main import (
    app,
    COMPOSITE_SERVICE_NAME,
    SPOT_SERVICE_URL,
    REVIEWS_SERVICE_URL,
    USER_SERVICE_URL,
)

client = TestClient(app)


def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "Welcome" in data["message"]
    assert "Composite Gateway API" in data["message"]


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == COMPOSITE_SERVICE_NAME
    assert "timestamp" in data


def test_composite_stub():
    """
    Sanity check that the composite endpoint works when downstream services succeed.
    """
    spot_id = "test-spot-123"

    with respx.mock:
        respx.get(f"{SPOT_SERVICE_URL}/spots/{spot_id}").mock(
            return_value=httpx.Response(
                200, json={"id": spot_id, "name": "Test Spot"}
            )
        )
        respx.get(f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}").mock(
            return_value=httpx.Response(
                200,
                json=[{"id": "r1", "rating": 5, "spot_id": spot_id}],
            )
        )
        respx.get(f"{USER_SERVICE_URL}/users").mock(
            return_value=httpx.Response(200, json=[{"id": "u1"}])
        )

        response = client.get(f"/composite/spots/{spot_id}/full")

    assert response.status_code == 200
    data = response.json()
    assert data["spot"]["id"] == spot_id
    assert isinstance(data["reviews"], list)
    assert isinstance(data["users"], list)
