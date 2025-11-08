import os
import sys

# Ensure composite-gateway project root is on sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "Welcome" in data["message"]


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "Composite Gateway"


def test_composite_stub():
    spot_id = "test-spot-123"
    response = client.get(f"/composite/spots/{spot_id}/full")
    assert response.status_code == 200
    data = response.json()
    assert data["spot_id"] == spot_id
    # ensure the stubbed note is present
    assert "Composite aggregation logic" in data["note"]


