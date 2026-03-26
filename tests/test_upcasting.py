from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from ledger.audit import run_integrity_check
from ledger.event_store import EventStore, InMemoryEventStore
from ledger.upcasters import UpcasterRegistry


async def _ensure_schema(db_url: str) -> None:
    sql = Path("schema.sql").read_text(encoding="utf-8")
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_upcasting_v1_to_v2_is_read_time_only_inmemory():
    upcasters = UpcasterRegistry()
    store = InMemoryEventStore(upcaster_registry=upcasters)

    payload_v1 = {
        "application_id": "A1",
        "session_id": "S1",
        "decision": {
            "risk_tier": "LOW",
            "recommended_limit_usd": "1000",
            "confidence": 0.8,
            "rationale": "ok",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_version": "m",
        "model_deployment_id": "d",
        "input_data_hash": "h",
        "analysis_duration_ms": 1,
        "completed_at": "2026-01-01T00:00:00+00:00",
    }
    await store.append(
        "credit-A1",
        [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": payload_v1}],
        expected_version=-1,
    )

    raw = store._streams["credit-A1"][0]  # internal stored row (v1)
    assert int(raw["event_version"]) == 1
    assert "model_versions" not in raw["payload"]

    loaded = await store.load_stream("credit-A1", from_position=0)
    assert loaded[0]["event_version"] == 2
    assert "model_versions" in loaded[0]["payload"]
    assert "regulatory_basis" in loaded[0]["payload"]


@pytest.mark.asyncio
async def test_upcasting_v1_to_v2_loads_as_v2_but_db_payload_remains_v1(db_url):
    try:
        await _ensure_schema(db_url)
    except Exception:
        pytest.skip("Postgres not available for upcasting immutability test")

    upcasters = UpcasterRegistry()
    store = EventStore(db_url, upcaster_registry=upcasters)
    await store.connect()
    stream_id = "credit-UPCAST-IMMUTABLE"
    try:
        payload_v1 = {
            "application_id": "UPCAST-IMMUTABLE",
            "session_id": "S1",
            "decision": {
                "risk_tier": "LOW",
                "recommended_limit_usd": "1000",
                "confidence": 0.8,
                "rationale": "ok",
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            "model_version": "m",
            "model_deployment_id": "d",
            "input_data_hash": "h",
            "analysis_duration_ms": 1,
            "completed_at": "2026-01-01T00:00:00+00:00",
        }
        await store.append(
            stream_id,
            [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": payload_v1}],
            expected_version=-1,
        )

        async with store._pool.acquire() as conn:  # type: ignore[union-attr]
            raw = await conn.fetchrow(
                "SELECT event_version, payload FROM events WHERE stream_id=$1 ORDER BY stream_position ASC LIMIT 1",
                stream_id,
            )
        assert raw is not None
        assert int(raw["event_version"]) == 1
        assert "model_versions" not in dict(raw["payload"])

        loaded = await store.load_stream(stream_id, from_position=0)
        assert int(loaded[0]["event_version"]) == 2
        assert "model_versions" in dict(loaded[0]["payload"])
    finally:
        async with store._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute("DELETE FROM events WHERE stream_id=$1", stream_id)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", stream_id)
        await store.close()


@pytest.mark.asyncio
async def test_integrity_check_detects_db_tamper(db_url):
    try:
        await _ensure_schema(db_url)
    except Exception:
        pytest.skip("Postgres not available for tamper test")

    store = EventStore(db_url, upcaster_registry=UpcasterRegistry())
    await store.connect()
    stream_id = "loan-TAMPER-001"
    try:
        await store.append(
            stream_id,
            [{"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {"application_id": "TAMPER-001"}}],
            expected_version=-1,
        )
        await store.append(
            stream_id,
            [{"event_type": "DocumentUploadRequested", "event_version": 1, "payload": {"application_id": "TAMPER-001"}}],
            expected_version=0,
        )

        # Mutate the payload directly in DB (simulated tampering).
        async with store._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                "UPDATE events SET payload = payload || '{\"tampered\": true}'::jsonb WHERE stream_id=$1 AND stream_position=1",
                stream_id,
            )

        res = await run_integrity_check(
            store,
            entity_type="loan",
            entity_id="TAMPER-001",
            stream_id=stream_id,
            correlation_id="TAMPER-001",
            causation_id="test",
        )
        assert res.tamper_detected is True
        assert res.chain_valid is False
    finally:
        async with store._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute("DELETE FROM events WHERE stream_id IN ($1, $2)", stream_id, "audit-TAMPER-001")
            await conn.execute("DELETE FROM event_streams WHERE stream_id IN ($1, $2)", stream_id, "audit-TAMPER-001")
        await store.close()
