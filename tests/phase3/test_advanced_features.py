import pytest

from ledger.audit import verify_stream_hash_chain
from ledger.event_store import InMemoryEventStore
from ledger.temporal import load_stream_as_of, parse_as_of
from ledger.upcasters import UpcasterRegistry
from ledger.what_if import branch_in_memory


@pytest.mark.asyncio
async def test_audit_chain_verification_detects_tampering():
    store = InMemoryEventStore()
    await store.append("s-1", [{"event_type": "E1", "event_version": 1, "payload": {"a": 1}}], expected_version=-1)
    await store.append("s-1", [{"event_type": "E2", "event_version": 1, "payload": {"b": 2}}], expected_version=0)

    events = await store.load_stream("s-1", from_position=0)
    ok, errors = verify_stream_hash_chain(events)
    assert ok
    assert errors == []

    # Tamper in-memory copy and verify fails.
    tampered = [dict(e) for e in events]
    tampered[1] = dict(tampered[1])
    tampered[1]["payload"] = {**tampered[1]["payload"], "b": 999}
    ok2, errors2 = verify_stream_hash_chain(tampered)
    assert not ok2
    assert any("event_hash mismatch" in er.reason for er in errors2)


@pytest.mark.asyncio
async def test_upcasting_v1_to_v2_is_read_time_only():
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
async def test_what_if_branch_does_not_mutate_base():
    base = InMemoryEventStore()
    await base.append("loan-X", [{"event_type": "E1", "event_version": 1, "payload": {"x": 1}}], expected_version=-1)
    await base.append("loan-X", [{"event_type": "E2", "event_version": 1, "payload": {"x": 2}}], expected_version=0)

    branch = await branch_in_memory(base_store=base, branch_id="b1", up_to_global_position=0)
    assert await base.stream_version("loan-X") == 1
    assert await branch.store.stream_version("loan-X") == 0

    await branch.store.append("loan-X", [{"event_type": "WHATIF", "event_version": 1, "payload": {"x": 99}}], expected_version=0)
    assert await base.stream_version("loan-X") == 1
    assert await branch.store.stream_version("loan-X") == 1


@pytest.mark.asyncio
async def test_temporal_query_as_of_filters_by_recorded_at():
    store = InMemoryEventStore()
    await store.append("s", [{"event_type": "E1", "event_version": 1, "payload": {}}], expected_version=-1)
    await store.append("s", [{"event_type": "E2", "event_version": 1, "payload": {}}], expected_version=0)
    events = await store.load_stream("s", from_position=0)
    cutoff = events[0]["recorded_at"]
    as_of = cutoff.isoformat()
    filtered = await load_stream_as_of(store, "s", as_of=parse_as_of(as_of))
    assert [e["event_type"] for e in filtered] == ["E1"]

