"""
tests/test_event_store.py

PostgreSQL-backed EventStore integration smoke tests.

Notes:
- Stream positions are 0-based in this repo: first event is stream_position=0.
- These tests use unique stream ids and clean up their own data.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from dotenv import load_dotenv

from ledger.event_store import EventStore, OptimisticConcurrencyError


env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
DB_URL = os.getenv("DATABASE_URL")


@pytest.fixture
async def store():
    if not DB_URL:
        pytest.skip("DATABASE_URL not configured")
    s = EventStore(DB_URL)
    await s.connect()
    yield s
    # Clean up test streams after each test (idempotent).
    async with s._pool.acquire() as conn:  # type: ignore[union-attr]
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-%'")
    await s.close()


def _event(etype: str, n: int = 1):
    return [
        {"event_type": etype, "event_version": 1, "payload": {"seq": i, "test": True}}
        for i in range(n)
    ]


def _new_stream_id(prefix: str = "test") -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


@pytest.mark.asyncio
async def test_append_new_stream(store: EventStore):
    stream_id = _new_stream_id("test-new")
    positions = await store.append(stream_id, _event("TestEvent"), expected_version=-1)
    assert positions == [0]
    assert await store.stream_version(stream_id) == 0


@pytest.mark.asyncio
async def test_append_existing_stream(store: EventStore):
    stream_id = _new_stream_id("test-exist")
    await store.append(stream_id, _event("TestEvent"), expected_version=-1)
    positions = await store.append(stream_id, _event("TestEvent2"), expected_version=0)
    assert positions == [1]
    assert await store.stream_version(stream_id) == 1


@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store: EventStore):
    stream_id = _new_stream_id("test-occ")
    await store.append(stream_id, _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append(stream_id, _event("E"), expected_version=99)
    assert exc.value.stream_id == stream_id
    assert exc.value.expected == 99
    assert exc.value.actual == 0


@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store: EventStore):
    stream_id = _new_stream_id("test-concurrent")
    await store.append(stream_id, _event("Init"), expected_version=-1)

    results = await asyncio.gather(
        store.append(stream_id, _event("A"), expected_version=0),
        store.append(stream_id, _event("B"), expected_version=0),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1
    assert len(errors) == 1
    assert await store.stream_version(stream_id) == 1


@pytest.mark.asyncio
async def test_load_stream_ordered(store: EventStore):
    stream_id = _new_stream_id("test-load")
    await store.append(stream_id, _event("E", 3), expected_version=-1)
    events = await store.load_stream(stream_id)
    assert len(events) == 3
    positions = [int(e["stream_position"]) for e in events]
    assert positions == [0, 1, 2]


@pytest.mark.asyncio
async def test_stream_version(store: EventStore):
    stream_id = _new_stream_id("test-ver")
    await store.append(stream_id, _event("E", 4), expected_version=-1)
    assert await store.stream_version(stream_id) == 3


@pytest.mark.asyncio
async def test_stream_version_nonexistent(store: EventStore):
    assert await store.stream_version(_new_stream_id("test-does-not-exist")) == -1


@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store: EventStore):
    a = _new_stream_id("test-global-A")
    b = _new_stream_id("test-global-B")
    await store.append(a, _event("E", 2), expected_version=-1)
    await store.append(b, _event("E", 2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_position=0)]
    positions = [int(e["global_position"]) for e in all_events]
    assert positions == sorted(positions)
