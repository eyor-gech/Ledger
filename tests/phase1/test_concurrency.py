"""
tests/phase1/test_concurrency.py

Extra OCC and ordering tests for InMemoryEventStore.
"""

import asyncio

import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_many_concurrent_appends_one_winner():
    store = InMemoryEventStore()
    await store.append("s", [_ev("ApplicationSubmitted")], expected_version=-1)

    successes = 0
    failures = 0

    async def attempt():
        nonlocal successes, failures
        try:
            await store.append("s", [_ev("ApplicationSubmitted")], expected_version=0)
            successes += 1
        except OptimisticConcurrencyError:
            failures += 1

    await asyncio.gather(*[attempt() for _ in range(25)])
    assert successes == 1
    assert failures == 24
    assert await store.stream_version("s") == 1


@pytest.mark.asyncio
async def test_two_truly_concurrent_appends_one_failure_has_structured_occ():
    store = InMemoryEventStore()
    await store.append("s", [_ev("ApplicationSubmitted")], expected_version=-1)

    results = await asyncio.gather(
        store.append("s", [_ev("ApplicationSubmitted")], expected_version=0),
        store.append("s", [_ev("ApplicationSubmitted")], expected_version=0),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, Exception)]
    assert len(successes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], OptimisticConcurrencyError)
    err = errors[0]
    assert err.stream_id == "s"
    assert err.expected == 0
    assert err.actual in {1}  # the winning append moved stream to v1
    assert await store.stream_version("s") == 1


@pytest.mark.asyncio
async def test_per_stream_ordering_is_sequential():
    store = InMemoryEventStore()
    for i in range(50):
        ver = await store.stream_version("stream-a")
        await store.append("stream-a", [_ev("ApplicationSubmitted", i=i)], expected_version=ver)

    events = await store.load_stream("stream-a")
    assert [e["stream_position"] for e in events] == list(range(50))


@pytest.mark.asyncio
async def test_global_positions_monotonic_across_streams():
    store = InMemoryEventStore()
    await store.append("a", [_ev("ApplicationSubmitted"), _ev("ApplicationSubmitted")], expected_version=-1)
    await store.append("b", [_ev("ApplicationSubmitted")], expected_version=-1)
    await store.append("a", [_ev("ApplicationSubmitted")], expected_version=1)

    all_events = [e async for e in store.load_all(from_position=0)]
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)
    assert positions == list(range(len(all_events)))
