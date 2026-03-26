import asyncio

import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_double_decision_occ_collision_expected_version_3_one_winner():
    """
    Mandatory OCC test:
    - two concurrent writers on the same stream
    - same expected_version=3
    - exactly one succeeds
    """
    store = InMemoryEventStore()
    stream_id = "loan-test-occ-ev3"

    # Bring stream to version 3 (0-based positions 0..3 => 4 events)
    for i in range(4):
        ver = await store.stream_version(stream_id)
        await store.append(stream_id, [_ev("ApplicationSubmitted", i=i)], expected_version=ver)
    assert await store.stream_version(stream_id) == 3

    results = await asyncio.gather(
        store.append(stream_id, [_ev("DecisionRequested")], expected_version=3),
        store.append(stream_id, [_ev("DecisionRequested")], expected_version=3),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, Exception)]

    assert len(successes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], OptimisticConcurrencyError)
    err: OptimisticConcurrencyError = errors[0]  # type: ignore[assignment]
    assert err.stream_id == stream_id
    assert err.expected == 3
    assert err.actual == 4  # winner advanced stream to v4

    assert await store.stream_version(stream_id) == 4
    stream_events = await store.load_stream(stream_id, from_position=0)
    assert len(stream_events) == 5
