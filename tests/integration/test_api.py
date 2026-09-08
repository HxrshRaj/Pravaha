"""API-level integration: auth, RBAC, producer/schema CRUD, event query,
analytics, and the AI investigation flow end-to-end with the mock provider."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture()
async def client(_infra, migrated):
    import httpx

    from pravaha.api.bootstrap import ensure_bootstrap_admin
    from pravaha.api.main import create_app

    await ensure_bootstrap_admin()
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _admin_token(client) -> str:
    r = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@pravaha.local", "password": "admin12345"},
    )
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


async def test_auth_and_rbac(client):
    # no token -> 401 structured error
    r = await client.get("/api/v1/producers")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "UNAUTHORIZED"

    tok = await _admin_token(client)
    h = {"Authorization": f"Bearer {tok}"}
    r = await client.get("/api/v1/auth/me", headers=h)
    assert r.json()["role"] == "ADMIN"

    # create a VIEWER and confirm it cannot create producers
    r = await client.post(
        "/api/v1/auth/users",
        headers=h,
        json={"email": "viewer@x.com", "password": "viewer12345", "role": "VIEWER"},
    )
    assert r.status_code == 201
    rv = await client.post(
        "/api/v1/auth/login", json={"email": "viewer@x.com", "password": "viewer12345"}
    )
    vtok = rv.json()["access_token"]
    r = await client.post(
        "/api/v1/producers",
        headers={"Authorization": f"Bearer {vtok}"},
        json={"name": "nope"},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "FORBIDDEN"


async def test_producer_schema_and_ingest_via_api(client):
    tok = await _admin_token(client)
    h = {"Authorization": f"Bearer {tok}"}

    r = await client.post(
        "/api/v1/producers",
        headers=h,
        json={"name": "api-demo", "allowed_event_types": [], "rate_limit_per_min": 1000000},
    )
    assert r.status_code == 201, r.text
    api_key = r.json()["api_key"]
    assert api_key.startswith("pvh_")

    r = await client.post(
        "/api/v1/schemas",
        headers=h,
        json={
            "event_type": "order.created",
            "json_schema": {
                "type": "object",
                "required": ["order_id", "amount"],
                "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}},
            },
        },
    )
    assert r.status_code == 201, r.text

    # valid event
    r = await client.post(
        "/api/v1/events",
        headers={"X-API-Key": api_key},
        json={"event_type": "order.created", "payload": {"order_id": "o1", "amount": 12.5}},
    )
    assert r.status_code == 202, r.text
    assert r.json()["valid"] is True

    # invalid event -> 422 structured
    r = await client.post(
        "/api/v1/events",
        headers={"X-API-Key": api_key},
        json={"event_type": "order.created", "payload": {"order_id": "o2"}},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] in ("VALIDATION_FAILED", "SCHEMA_VALIDATION_FAILED")


async def test_ai_investigation_flow_with_mock(client):
    """Seed an anomaly row, run an investigation, assert grounded conclusion."""
    from datetime import UTC, datetime, timedelta

    from pravaha.ai.investigator import create_investigation, run_investigation
    from pravaha.db import session_scope
    from pravaha.models import Aggregation, AnomalyRecord

    now = datetime.now(UTC).replace(second=0, microsecond=0)
    async with session_scope() as s:
        for i in range(20):
            s.add(
                Aggregation(
                    metric="payment_failure_rate",
                    bucket_start=now - timedelta(minutes=20 - i),
                    group_key="_all",
                    value=0.5 if i > 16 else 0.06,
                    count=100,
                )
            )
        anomaly = AnomalyRecord(
            metric="payment_failure_rate",
            group_key="_all",
            detected_at=now,
            window_start=now - timedelta(minutes=1),
            window_end=now,
            observed_value=0.52,
            expected_value=0.06,
            deviation=0.46,
            severity="HIGH",
            algorithm="ewma",
            confidence=0.8,
            evidence={"z_score": 12.0},
            dedup_key=f"pfr|_all|HIGH|{int(now.timestamp()//600)}",
        )
        s.add(anomaly)
        await s.flush()
        anomaly_id = anomaly.id

    inv_id = await create_investigation(anomaly_id, trigger="test")
    result = await run_investigation(inv_id)
    assert result["status"] == "COMPLETED", result
    assert result["root_cause"] != "inconclusive"
    assert result["evidence_refs"], "conclusion has no grounded evidence refs"
    assert not result["hallucinated_refs"]

    tok = await _admin_token(client)
    r = await client.get(
        f"/api/v1/ai/investigations/{inv_id}", headers={"Authorization": f"Bearer {tok}"}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "COMPLETED"
    assert len(body["hypotheses"]) >= 3
    assert body["usage"]["total_tokens"] >= 0
