# manual_test_event_store_pg.py
import asyncio
import pytest
import os
import uuid
from pathlib import Path
from dotenv import load_dotenv
from ledger.event_store import EventStore, OptimisticConcurrencyError

# Load .env automatically
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)
DB_URL = os.getenv("DATABASE_URL")

# -------------------- FIXTURE --------------------
@pytest.fixture
async def store():
    store = EventStore(DB_URL)
    await store.connect()
    yield store
    # Clean up test streams after each test
    async with store._pool.acquire() as conn:
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-%'")
    await store.close()


# -------------------- HELPERS --------------------
def _event(etype, n=1):
    """Create list of events with incremental seq numbers."""
    return [{"event_type": etype, "event_version": 1, "payload": {"seq": i, "test": True}} for i in range(n)]

def _new_stream_id(prefix="test"):
    """Generate a unique stream ID for each test run."""
    return f"{prefix}-{uuid.uuid4().hex}"


# -------------------- TESTS --------------------
@pytest.mark.asyncio
async def test_append_new_stream(store):
    stream_id = _new_stream_id("new")
    positions = await store.append(stream_id, _event("TestEvent"), expected_version=-1)
    assert positions == [0]

@pytest.mark.asyncio
async def test_append_existing_stream(store):
    stream_id = _new_stream_id("exist")
    # Append first event
    positions1 = await store.append(stream_id, _event("TestEvent"), expected_version=-1)
    assert positions1 == [0]
    # Append second event
    positions2 = await store.append(stream_id, _event("TestEvent2"), expected_version=0)
    assert positions2 == [1]

@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store):
    stream_id = _new_stream_id("occ")
    # Append first event
    positions = await store.append(stream_id, _event("E"), expected_version=-1)
    assert positions == [0]
    # Attempt append with wrong expected version
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append(stream_id, _event("E"), expected_version=99)
    assert exc.value.expected == 99
    assert exc.value.actual == 0

@pytest.mark.asyncio
async def test_stream_version_nonexistent(store):
    stream_id = _new_stream_id("nonexistent")
    version = await store.stream_version(stream_id)
    assert version == -1

@pytest.mark.asyncio
async def test_stream_version_increments(store):
    stream_id = _new_stream_id("ver")
    await store.append(stream_id, _event("E", 3), expected_version=-1)
    version = await store.stream_version(stream_id)
    assert version == 2  # 0-based positions: last position is 2 for 3 events

@pytest.mark.asyncio
async def test_load_stream_ordered(store):
    stream_id = _new_stream_id("load")
    await store.append(stream_id, _event("E", 3), expected_version=-1)
    events = await store.load_stream(stream_id)
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)

@pytest.mark.asyncio
async def test_load_all_global_order(store):
    # Create two streams
    s1 = _new_stream_id("globalA")
    s2 = _new_stream_id("globalB")
    await store.append(s1, _event("E", 2), expected_version=-1)
    await store.append(s2, _event("E", 2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_position=0)]
    global_positions = [e["global_position"] for e in all_events]
    assert global_positions == sorted(global_positions)
