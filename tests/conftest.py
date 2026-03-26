"""
tests/conftest.py — shared fixtures
"""
import pytest, sys, os, random
from pathlib import Path
from faker import Faker
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

random.seed(42); Faker.seed(42)

load_dotenv()

@pytest.fixture(scope="session")
def db_url():
    url = (
        os.environ.get("TEST_DB_URL")
        or os.environ.get("DATABASE_URL")
    )

    if not url:
        pytest.skip("No database URL provided")

    # HARD BLOCK: Prevent production accidents
    forbidden_keywords = ["prod", "live", "main", "actual", "billing"]
    if any(k in url.lower() for k in forbidden_keywords) or "test" not in url.lower():
        print(f"\n CRITICAL SAFETY VIOLATION: Refusing to run on {url}")
        sys.exit(1)

    return url

@pytest.fixture
def sample_companies():
    from datagen.company_generator import generate_companies
    return generate_companies(10)

@pytest.fixture
def event_store_class():
    """Returns the EventStore class. Swap for real once implemented."""
    from ledger.event_store import EventStore
    return EventStore
