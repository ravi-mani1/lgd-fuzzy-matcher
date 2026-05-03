"""Tests for api.py — FastAPI endpoints."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi.testclient import TestClient
from api import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


class TestHealth:
    def test_health(self, client: TestClient):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestStats:
    def test_stats(self, client: TestClient):
        r = client.get("/stats")
        assert r.status_code == 200
        data = r.json()
        assert "states" in data
        assert "districts" in data


class TestMatch:
    def test_single_record(self, client: TestClient):
        r = client.post("/match", json={
            "records": [{"state_name_raw": "Delhi", "district_name_raw": "New Delhi"}]
        })
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 1
        assert data["results"][0]["match_status"] in ("EXACT", "HIGH_CONFIDENCE")

    def test_empty_records(self, client: TestClient):
        r = client.post("/match", json={"records": []})
        assert r.status_code == 400

    def test_multiple_records(self, client: TestClient):
        r = client.post("/match", json={
            "records": [
                {"state_name_raw": "UP", "district_name_raw": "varansi"},
                {"state_name_raw": "Maharashtra", "district_name_raw": "Bombay"},
            ]
        })
        assert r.status_code == 200
        assert r.json()["total"] == 2


class TestMatchCsv:
    def test_valid_csv(self, client: TestClient):
        csv_content = b"state_name_raw,district_name_raw\nDelhi,New Delhi\nUP,varansi\n"
        r = client.post("/match-csv", files={"file": ("test.csv", csv_content, "text/csv")})
        assert r.status_code == 200
        assert r.json()["total"] == 2

    def test_missing_columns(self, client: TestClient):
        csv_content = b"state,district\nDelhi,New Delhi\n"
        r = client.post("/match-csv", files={"file": ("test.csv", csv_content, "text/csv")})
        assert r.status_code == 422
